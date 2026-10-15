use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use glassdb::{Collection, CollectionPath, Database, Error as GError, RestructurerStats, Stats};
use glassdb_backend::Backend;
use glassdb_bench_scale::run::shutdown_databases_until;
use glassdb_concurr::rt;
use tokio::runtime::Handle;

use super::result;
use super::{key_bytes, value};

const COLLECTION_PREFIX: &str = "mix";
// The full GCs run while no worker runs, so they can use many more checks than
// background GC. The limit keeps the backend client within its connections.
const FULL_GC_PARALLELISM: NonZeroUsize = NonZeroUsize::new(256).unwrap();

/// Inputs that determine how storage is seeded and settled.
pub(super) struct SeedConfig {
    pub(super) collections: usize,
    pub(super) pool_size: usize,
    pub(super) quiet_period: Duration,
    pub(super) timeout: Duration,
    pub(super) gc_timeout: Duration,
    pub(super) drain_timeout: Duration,
}

/// A database with seeded collections whose structural changes are complete.
pub(super) struct Seed {
    backend: Arc<dyn Backend>,
    database_name: String,
    settlement: Settlement,
}

impl Seed {
    /// Seeds the collections of a new database and waits until its splits,
    /// merges, and garbage collection stop.
    pub(super) fn create(
        handle: &Handle,
        backend: Arc<dyn Backend>,
        database_name: String,
        config: &SeedConfig,
    ) -> Result<Self, Box<dyn Error>> {
        let settlement = seed_and_settle(handle, backend.clone(), &database_name, config)?;
        Ok(Self {
            backend,
            database_name,
            settlement,
        })
    }

    /// Returns the same seeded database over `backend`, which must hold a
    /// copy of this seed's storage.
    pub(super) fn with_backend(&self, backend: Arc<dyn Backend>) -> Self {
        Self {
            backend,
            database_name: self.database_name.clone(),
            settlement: self.settlement,
        }
    }

    /// Opens one measurement database instance for each of the first `databases`
    /// collections, with handles to all of these collections.
    pub(super) fn open_cell(
        &self,
        handle: &Handle,
        databases: usize,
    ) -> Result<PreparedCell, Box<dyn Error>> {
        let paths = collection_paths(databases)?;
        let databases: Vec<_> = (0..databases)
            .map(|_| open_db(handle, &self.database_name, self.backend.clone()))
            .collect();
        let collections = handle.block_on(open_collections(&databases, &paths))?;
        Ok(PreparedCell {
            databases,
            collections,
            settlement: self.settlement,
        })
    }
}

/// A seeded cell whose measurement clients have opened every target collection.
pub(super) struct PreparedCell {
    databases: Vec<Database>,
    collections: Vec<Arc<[Collection]>>,
    settlement: Settlement,
}

impl PreparedCell {
    /// Returns the measurement clients in their stable home-collection order.
    pub(super) fn databases(&self) -> &[Database] {
        &self.databases
    }

    /// Returns the handles of each database instance to every target
    /// collection.
    pub(super) fn collections(&self) -> &[Arc<[Collection]>] {
        &self.collections
    }

    /// Waits while the workload runs for at least `minimum` of model time,
    /// then until the splits and merges of the database instances stop for
    /// `quiet`, or until `timeout`, or until `stop` is set.
    ///
    /// The topology policy decides from windows of measurements, so a load
    /// causes no split until it has run for some windows. Without the
    /// minimum, a short `quiet` would end the warm-up before that.
    pub(super) async fn warm_up(
        &self,
        stop: &AtomicBool,
        minimum: Duration,
        quiet: Duration,
        timeout: Duration,
    ) -> Warmup {
        let started = Instant::now();
        let start = restructurer_stats(&self.databases);
        tokio::select! {
            () = rt::sleep(minimum) => {}
            () = wait_for_stop(stop) => {}
        }
        let remaining = timeout.saturating_sub(started.elapsed());
        let settled = wait_for_quiet(
            || structural_changes(&self.databases),
            stop,
            quiet,
            remaining,
        )
        .await;
        let end = restructurer_stats(&self.databases);
        Warmup {
            splits: end.splits - start.splits,
            merges: end.merges - start.merges,
            elapsed: started.elapsed(),
            quiet: settled,
        }
    }

    /// Deletes the garbage of the warm-up while the workers wait, so that the
    /// measured interval does not do that GC work.
    pub(super) async fn collect_garbage(&self, timeout: Duration) -> Result<Collected, GError> {
        collect_garbage_until_clean(&self.databases[0], timeout).await
    }

    /// Starts the measured phase by capturing the counter baselines of the
    /// database instances.
    pub(super) fn begin_measurement(self) -> ActiveCell {
        let baselines = self.databases.iter().map(Database::stats).collect();
        ActiveCell {
            databases: self.databases,
            settlement: self.settlement,
            baselines,
        }
    }
}

/// The splits, merges, and duration of a cell's warm-up.
#[derive(Clone, Copy)]
pub(super) struct Warmup {
    pub(super) splits: u64,
    pub(super) merges: u64,
    pub(super) elapsed: Duration,
    /// Whether the changes stopped before the timeout.
    pub(super) quiet: bool,
}

/// A prepared cell with measurement counter baselines captured.
pub(super) struct ActiveCell {
    databases: Vec<Database>,
    settlement: Settlement,
    baselines: Vec<Stats>,
}

impl ActiveCell {
    /// Shuts down measurement clients and returns their settled counter deltas.
    ///
    /// Shutdown always runs before errors are returned. A worker error retains
    /// precedence over a simultaneous shutdown timeout.
    pub(super) fn teardown(
        &self,
        handle: &Handle,
        deadline: tokio::time::Instant,
        workers: Result<(), GError>,
    ) -> Result<CompletedCell, Box<dyn Error>> {
        let shutdown = handle.block_on(shutdown_databases_until(&self.databases, deadline));
        workers?;
        shutdown?;
        let deltas = self
            .databases
            .iter()
            .enumerate()
            .map(|(index, database)| database.stats() - self.baselines[index])
            .collect();
        Ok(CompletedCell {
            databases: self.databases.len(),
            setup_splits: self.settlement.splits,
            split_settle_elapsed: self.settlement.elapsed,
            deltas,
        })
    }
}

/// Setup and counter outcomes needed to report a completed cell.
pub(super) struct CompletedCell {
    pub(super) databases: usize,
    pub(super) setup_splits: u64,
    pub(super) split_settle_elapsed: Duration,
    pub(super) deltas: Vec<Stats>,
}

fn collection_paths(count: usize) -> Result<Vec<CollectionPath>, GError> {
    (0..count)
        .map(|index| CollectionPath::new(format!("{COLLECTION_PREFIX}-{index}").as_bytes()))
        .collect()
}

fn open_db(handle: &Handle, name: &str, backend: Arc<dyn Backend>) -> Database {
    handle
        .block_on(Database::open(name, backend))
        .expect("open db")
}

async fn open_collections(
    databases: &[Database],
    paths: &[CollectionPath],
) -> Result<Vec<Arc<[Collection]>>, GError> {
    let mut all = Vec::with_capacity(databases.len());
    for database in databases {
        let mut collections = Vec::with_capacity(paths.len());
        for path in paths {
            collections.push(database.open_collection(path).await?);
        }
        all.push(Arc::from(collections));
    }
    Ok(all)
}

/// Returns the completed splits and merges of `databases`.
fn structural_changes(databases: &[Database]) -> u64 {
    let stats = restructurer_stats(databases);
    stats.splits + stats.merges
}

/// Returns the sum of the restructurer counters of `databases`.
fn restructurer_stats(databases: &[Database]) -> RestructurerStats {
    databases
        .iter()
        .fold(RestructurerStats::default(), |mut total, database| {
            total += database.stats().restructurer;
            total
        })
}

/// Waits until `stop` is set.
async fn wait_for_stop(stop: &AtomicBool) {
    while !stop.load(Ordering::Relaxed) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// The transaction records that full GCs deleted, and the time they took.
pub(super) struct Collected {
    pub(super) deleted: u64,
    pub(super) elapsed: Duration,
}

/// Runs full GCs on `database` until no garbage that a full GC can find is
/// left, or until `timeout`.
///
/// The records of the last safety horizon are deferred, and a check that only
/// reclaims some effects of a record can delete it the next time.
async fn collect_garbage_until_clean(
    database: &Database,
    timeout: Duration,
) -> Result<Collected, GError> {
    let started = Instant::now();
    let deadline = tokio::time::Instant::now() + timeout;
    let mut deleted = 0;
    loop {
        let report = database.collect_garbage(FULL_GC_PARALLELISM).await?;
        deleted += report.deleted;
        if report.deferred == 0 && report.reclaimed_effects == 0 {
            return Ok(Collected {
                deleted,
                elapsed: started.elapsed(),
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

#[derive(Clone, Copy)]
struct Settlement {
    splits: u64,
    elapsed: Duration,
}

struct QuietTracker {
    changes: u64,
    unchanged_since: Instant,
}

impl QuietTracker {
    fn new(changes: u64, now: Instant) -> Self {
        Self {
            changes,
            unchanged_since: now,
        }
    }

    fn observe(&mut self, changes: u64, now: Instant) {
        if changes != self.changes {
            self.changes = changes;
            self.unchanged_since = now;
        }
    }

    fn is_quiet(&self, now: Instant, quiet: Duration) -> bool {
        now.duration_since(self.unchanged_since) >= quiet
    }
}

fn deadline_expired(started: Instant, now: Instant, timeout: Duration) -> bool {
    now.duration_since(started) >= timeout
}

/// Waits until `changes` stops moving for a full quiet period, or until
/// `timeout` or `stop`, and reports whether the quiet period was reached.
async fn wait_for_quiet(
    changes: impl Fn() -> u64,
    stop: &AtomicBool,
    quiet: Duration,
    timeout: Duration,
) -> bool {
    let started = Instant::now();
    let mut tracker = QuietTracker::new(changes(), started);
    let poll = (quiet / 4).clamp(Duration::from_millis(20), Duration::from_millis(250));
    loop {
        let now = Instant::now();
        tracker.observe(changes(), now);
        let is_quiet = tracker.is_quiet(now, quiet);
        if is_quiet || deadline_expired(started, now, timeout) || stop.load(Ordering::Relaxed) {
            return is_quiet;
        }
        tokio::time::sleep(poll).await;
    }
}

/// Seeds every collection and lets its complete split cascade and garbage
/// collection finish.
fn seed_and_settle(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    database_name: &str,
    config: &SeedConfig,
) -> Result<Settlement, Box<dyn Error>> {
    let database = open_db(handle, database_name, backend);
    let paths = collection_paths(config.collections)?;
    let seeded = handle.block_on(async {
        for path in &paths {
            let name = path
                .segments()
                .next()
                .expect("mixed benchmark collection paths are non-empty");
            let collection = database
                .root_collection()
                .create_collection_if_absent(name)
                .await?;
            let mut index = 0;
            while index < config.pool_size {
                let end = (index + 100).min(config.pool_size);
                let batch: Vec<Vec<u8>> = (index..end).map(key_bytes).collect();
                let collection = &collection;
                let batch = &batch;
                database
                    .tx(|transaction| async move {
                        for key in batch {
                            transaction.write(collection, key, &value())?;
                        }
                        Ok(())
                    })
                    .await?;
                index = end;
            }
        }
        Ok::<(), GError>(())
    });
    if let Err(error) = seeded {
        // The run fails either way. The seeding error stays the reported one.
        let _ = handle.block_on(shutdown_databases_until(
            std::slice::from_ref(&database),
            tokio::time::Instant::now() + config.drain_timeout,
        ));
        return Err(error.into());
    }
    let databases = std::slice::from_ref(&database);
    let started = Instant::now();
    let settled = handle.block_on(wait_for_quiet(
        || structural_changes(databases),
        &AtomicBool::new(false),
        config.quiet_period,
        config.timeout,
    ));
    // Garbage collection also changes the stored state, so every fork would
    // otherwise carry work that its cell does while it measures.
    let collected = handle.block_on(collect_garbage_until_clean(&database, config.gc_timeout));
    let settlement = Settlement {
        splits: database.stats().restructurer.splits,
        elapsed: started.elapsed(),
    };
    handle.block_on(shutdown_databases_until(
        databases,
        tokio::time::Instant::now() + config.drain_timeout,
    ))?;
    if !settled {
        return Err(format!(
            "setup splits and merges did not stay quiet for {:?} within {:?} \
             (completed splits={})",
            config.quiet_period, config.timeout, settlement.splits
        )
        .into());
    }
    let collected = collected?;
    eprintln!("{}", result::garbage_collected(&collected));
    eprintln!(
        "{}",
        result::setup_settled(settlement.elapsed, settlement.splits, config.quiet_period)
    );
    Ok(settlement)
}

#[cfg(test)]
mod tests {
    use glassdb::backend::memory::MemoryBackend;

    use super::*;

    #[test]
    fn quiet_period_resets_and_deadline_is_inclusive() {
        let start = Instant::now();
        let quiet = Duration::from_secs(2);
        let mut tracker = QuietTracker::new(3, start);

        assert!(!tracker.is_quiet(start + Duration::from_secs(1), quiet));
        tracker.observe(4, start + Duration::from_millis(1500));
        assert!(!tracker.is_quiet(start + Duration::from_secs(3), quiet));
        tracker.observe(4, start + Duration::from_millis(3200));
        assert!(tracker.is_quiet(start + Duration::from_millis(3500), quiet));
        assert!(!deadline_expired(
            start,
            start + Duration::from_millis(2999),
            Duration::from_secs(3)
        ));
        assert!(deadline_expired(
            start,
            start + Duration::from_secs(3),
            Duration::from_secs(3)
        ));
    }

    #[test]
    fn cells_open_independent_forks_of_one_seed() -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let handle = runtime.handle();
        let storage = Arc::new(MemoryBackend::new());
        let seed = Seed::create(
            handle,
            storage.clone(),
            "mixedsetuptest".to_string(),
            &SeedConfig {
                collections: 2,
                pool_size: 3,
                quiet_period: Duration::from_millis(20),
                timeout: Duration::from_secs(1),
                gc_timeout: Duration::from_secs(120),
                drain_timeout: Duration::from_secs(1),
            },
        )?;

        // A cell can use fewer collections than the seed has.
        let cell = seed
            .with_backend(Arc::new(storage.fork()))
            .open_cell(handle, 1)?;
        assert_eq!(cell.databases().len(), 1);
        assert_eq!(cell.collections()[0].len(), 1);
        let collection = cell.collections()[0][0].clone();
        let keys: Vec<_> = handle.block_on(collection.iter_keys())?.collect();
        assert_eq!(keys, [key_bytes(0), key_bytes(1), key_bytes(2)]);

        // A write in one cell is not visible to the next cell.
        handle.block_on(collection.write(&key_bytes(0), b"cell"))?;
        let next = seed
            .with_backend(Arc::new(storage.fork()))
            .open_cell(handle, 2)?;
        assert_eq!(next.collections()[1].len(), 2);
        let value = handle.block_on(next.collections()[0][0].read(&key_bytes(0)))?;
        assert_eq!(value, Some(super::value()));

        let probe = cell.databases()[0].clone();
        let completed = cell.begin_measurement().teardown(
            handle,
            tokio::time::Instant::now() + Duration::from_secs(1),
            Ok(()),
        )?;
        assert_eq!(completed.databases, 1);
        assert!(matches!(
            handle.block_on(probe.root_collection().read(b"after-shutdown")),
            Err(GError::ShuttingDown)
        ));
        next.begin_measurement().teardown(
            handle,
            tokio::time::Instant::now() + Duration::from_secs(1),
            Ok(()),
        )?;
        Ok(())
    }
}
