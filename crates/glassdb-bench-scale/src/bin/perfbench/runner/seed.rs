//! The storage of a cell, and the seed that writes its pools.

use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use glassdb::{Collection, CollectionPath, Database, Error as GError, GcStats, RestructurerStats};
use glassdb_backend::Backend;
use glassdb_bench_scale::backend_breakdown::{BackendBreakdownHandle, wrap};
use glassdb_bench_scale::run::shutdown_databases_until;
use glassdb_concurr::rt;
use indexmap::IndexMap;
use serde::Serialize;

use super::super::backend::{Factory, ForkableBackend};
use super::model::{CellSpec, DatabaseOptions, PoolSpec, SeedSpec};

/// The keys that one seed transaction writes.
const SEED_BATCH: usize = 64;

// The full GCs run while no worker runs, so they can use many more checks than
// background GC. A full GC checks one LIST page of 1,000 records at a time, and
// each check waits mostly for backend latency, so the limit lets a whole page
// run at the same time.
const FULL_GC_PARALLELISM: NonZeroUsize = NonZeroUsize::new(1024).unwrap();

/// The values that decide what a seed writes. Cells with equal keys share one
/// seed.
#[derive(Debug, Clone, PartialEq)]
pub(crate) struct SeedKey {
    pools: IndexMap<String, PoolSpec>,
    collections: usize,
    seed: SeedSpec,
}

/// A seeded database name over one storage.
pub(crate) struct Storage {
    pub(crate) backend: Arc<dyn Backend>,
    /// Counts backend operations by object role.
    pub(crate) breakdown: BackendBreakdownHandle,
    pub(crate) database: String,
    /// The storage, when it can fork.
    forkable: Option<ForkableBackend>,
    pub(crate) seed: SeedReport,
}

/// The outcome of a seed.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct SeedReport {
    pub(crate) splits: u64,
    pub(crate) settle_wall_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) full_gc: Option<Collected>,
}

/// The transaction records that full GCs deleted, and the time they took.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct Collected {
    pub(crate) deleted: u64,
    pub(crate) wall_ms: u64,
}

/// Provides each cell of a run with the storage that it starts from.
pub(crate) struct Seeds<'a> {
    factory: &'a Factory,
    name_prefix: String,
    drain_timeout: Duration,
    /// The last seed on storage that can fork, with its key.
    cached: Option<(SeedKey, Storage)>,
    count: usize,
}

impl SeedKey {
    pub(crate) fn of(cell: &CellSpec) -> Self {
        Self {
            pools: cell.pools.clone(),
            collections: cell.seed_collections,
            seed: cell.seed.clone(),
        }
    }
}

impl Storage {
    /// Returns a copy of this storage, when it can fork.
    pub(crate) fn fork(&self) -> Option<Storage> {
        self.forkable.as_ref().map(|storage| {
            let copy = storage.fork();
            let (backend, breakdown) = wrap(copy.backend());
            Storage {
                backend,
                breakdown,
                database: self.database.clone(),
                forkable: Some(copy),
                seed: self.seed,
            }
        })
    }

    /// Opens one database instance with `options`.
    pub(crate) async fn open(&self, options: &DatabaseOptions) -> Result<Database, Box<dyn Error>> {
        let builder = Database::builder(&self.database, self.backend.clone());
        Ok(options.apply(builder)?.open().await?)
    }
}

impl<'a> Seeds<'a> {
    pub(crate) fn new(factory: &'a Factory, name_prefix: String, drain_timeout: Duration) -> Self {
        Self {
            factory,
            name_prefix,
            drain_timeout,
            cached: None,
            count: 0,
        }
    }

    /// Returns new storage with the seed of `cell`. On storage that can fork,
    /// cells with the same seed key get copies of one seed.
    pub(crate) async fn storage_for(&mut self, cell: &CellSpec) -> Result<Storage, Box<dyn Error>> {
        let key = SeedKey::of(cell);
        if let Some((cached, storage)) = &self.cached
            && *cached == key
            && let Some(copy) = storage.fork()
        {
            return Ok(copy);
        }
        self.cached = None;
        self.count += 1;
        let database = format!("{}s{}", self.name_prefix, self.count);
        let forkable = self.factory.forkable();
        let raw = match &forkable {
            Some(storage) => storage.backend(),
            None => self.factory.backend(),
        };
        let (backend, breakdown) = wrap(raw);
        let report = seed(&backend, &database, cell, self.drain_timeout).await?;
        let storage = Storage {
            backend,
            breakdown,
            database,
            forkable,
            seed: report,
        };
        match storage.fork() {
            Some(copy) => {
                self.cached = Some((key, storage));
                Ok(copy)
            }
            None => Ok(storage),
        }
    }
}

/// Returns the collection path of collection `index` of `pool`.
pub(crate) fn collection_path(pool: &str, index: usize) -> String {
    format!("{pool}-{index}")
}

/// Opens the collections of every pool of `cell` in `db`, in the order of the
/// pools, then of the collections.
pub(crate) async fn open_collections(
    db: &Database,
    cell: &CellSpec,
) -> Result<IndexMap<String, Arc<[Collection]>>, GError> {
    let mut all = IndexMap::new();
    for pool in cell.pools.keys() {
        let collections = open_collections_of(db, pool, cell.collections).await?;
        all.insert(pool.clone(), Arc::from(collections));
    }
    Ok(all)
}

/// Opens the first `count` collections of `pool` in `db`.
pub(crate) async fn open_collections_of(
    db: &Database,
    pool: &str,
    count: usize,
) -> Result<Vec<Collection>, GError> {
    let mut collections = Vec::with_capacity(count);
    for index in 0..count {
        let path = CollectionPath::new(collection_path(pool, index).as_bytes())?;
        collections.push(db.open_collection(&path).await?);
    }
    Ok(collections)
}

/// Writes the pools of `cell` in a new database, and waits until its splits,
/// merges, and garbage collection stop.
async fn seed(
    backend: &Arc<dyn Backend>,
    database: &str,
    cell: &CellSpec,
    drain_timeout: Duration,
) -> Result<SeedReport, Box<dyn Error>> {
    let spec = &cell.seed;
    let db = spec
        .database
        .apply(Database::builder(database, backend.clone()))?
        .open()
        .await?;
    let written = write_pools(&db, cell).await;
    if let Err(error) = written {
        // The run fails either way. The seeding error stays the reported one.
        let _ = shutdown(&db, drain_timeout).await;
        return Err(error.into());
    }
    let started = Instant::now();
    let mut report = SeedReport {
        splits: 0,
        settle_wall_ms: 0,
        full_gc: None,
    };
    let mut settled = true;
    if !spec.quiet_period.is_zero() {
        let databases = std::slice::from_ref(&db);
        settled = wait_for_quiet(
            || structural_changes(databases),
            &AtomicBool::new(false),
            spec.quiet_period,
            spec.timeout,
        )
        .await;
        // Garbage collection also changes the stored state, so every copy would
        // otherwise carry work that its cell does while it measures.
        {
            let collected = collect_garbage(&db).await;
            match collected {
                Ok(collected) => {
                    crate::progress::log!(
                        "  full GC took {}ms: {} transaction records deleted",
                        collected.wall_ms,
                        collected.deleted
                    );
                    report.full_gc = Some(collected);
                }
                Err(error) => {
                    let _ = shutdown(&db, drain_timeout).await;
                    return Err(error.into());
                }
            }
        }
    }
    report.splits = db.stats().restructurer.splits;
    report.settle_wall_ms = millis(started.elapsed());
    shutdown(&db, drain_timeout).await?;
    if !settled {
        return Err(format!(
            "setup splits and merges did not stay quiet for {:?} within {:?} (completed splits={})",
            spec.quiet_period, spec.timeout, report.splits
        )
        .into());
    }
    crate::progress::log!(
        "  setup settled after {}ms: {} completed splits",
        report.settle_wall_ms,
        report.splits
    );
    Ok(report)
}

async fn write_pools(db: &Database, cell: &CellSpec) -> Result<(), GError> {
    for (name, pool) in &cell.pools {
        let keys = pool.keys.unwrap_or(0);
        for index in 0..cell.seed_collections {
            let path = collection_path(name, index);
            let collection = db
                .root_collection()
                .create_collection_if_absent(path.as_bytes())
                .await?;
            if !pool.seeded {
                continue;
            }
            let value = vec![0x5a; pool.value_bytes];
            let mut start = 0;
            while start < keys {
                let end = (start + SEED_BATCH).min(keys);
                let batch: Vec<Vec<u8>> = (start..end).map(|index| pool.key(index)).collect();
                let collection = &collection;
                let (batch, value) = (&batch, &value);
                db.tx(|tx| async move {
                    for key in batch {
                        tx.write(collection, key, value)?;
                    }
                    Ok(())
                })
                .await?;
                start = end;
            }
        }
    }
    Ok(())
}

pub(crate) async fn shutdown(db: &Database, drain_timeout: Duration) -> Result<(), GError> {
    shutdown_databases_until(
        std::slice::from_ref(db),
        tokio::time::Instant::now() + drain_timeout,
    )
    .await
}

/// Returns the completed splits and merges of `databases`.
pub(crate) fn structural_changes(databases: &[Database]) -> u64 {
    let stats = restructurer_stats(databases);
    stats.splits + stats.merges
}

/// Returns the sum of the restructurer counters of `databases`.
pub(crate) fn restructurer_stats(databases: &[Database]) -> RestructurerStats {
    databases
        .iter()
        .fold(RestructurerStats::default(), |mut total, database| {
            total += database.stats().restructurer;
            total
        })
}

/// Returns the sum of the GC counters of `databases`.
pub(crate) fn gc_stats(databases: &[Database]) -> GcStats {
    databases
        .iter()
        .fold(GcStats::default(), |mut total, database| {
            total += database.stats().gc;
            total
        })
}

/// Waits until `changes` stops moving for a full quiet period, or until
/// `timeout` or `stop`, and reports whether the quiet period was reached.
pub(crate) async fn wait_for_quiet(
    changes: impl Fn() -> u64,
    stop: &AtomicBool,
    quiet: Duration,
    timeout: Duration,
) -> bool {
    let started = Instant::now();
    let mut last = changes();
    let mut unchanged_since = started;
    let poll = (quiet / 4).clamp(Duration::from_millis(20), Duration::from_millis(250));
    loop {
        let now = Instant::now();
        let current = changes();
        if current != last {
            last = current;
            unchanged_since = now;
        }
        let is_quiet = now.duration_since(unchanged_since) >= quiet;
        if is_quiet
            || now.duration_since(started) >= timeout
            || stop.load(std::sync::atomic::Ordering::Relaxed)
        {
            return is_quiet;
        }
        tokio::time::sleep(poll).await;
    }
}

/// The limit of the full GCs after a seed or a warm-up. The records of the last
/// safety horizon of a load wait for it, so it is longer than the pending
/// timeout of the protocol timing.
const GC_TIMEOUT: Duration = Duration::from_secs(600);

/// Runs full GCs on `database` until no garbage that a full GC can find is
/// left, or until [`GC_TIMEOUT`].
///
/// The records of the last safety horizon are deferred, and a check that only
/// reclaims some effects of a record can delete it the next time.
pub(crate) async fn collect_garbage(database: &Database) -> Result<Collected, GError> {
    let timeout = GC_TIMEOUT;
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut deleted = 0;
    loop {
        let report = database.collect_garbage(FULL_GC_PARALLELISM).await?;
        deleted += report.deleted;
        if report.deferred == 0 && report.reclaimed_effects == 0 {
            return Ok(Collected {
                deleted,
                wall_ms: millis(started.elapsed()),
            });
        }
        let wait = report.retry_after.unwrap_or_default();
        if tokio::time::Instant::now() + wait >= deadline {
            return Err(GError::internal(format!(
                "garbage collection did not finish within {timeout:?}: {report:?}"
            )));
        }
        rt::sleep(wait).await;
    }
}

pub(crate) fn millis(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}
