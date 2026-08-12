use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant};

use glassdb::{Collection, CollectionPath, Database, Error as GError, Stats};
use glassdb_backend::Backend;
use glassdb_bench_scale::run::shutdown_databases_until;
use tokio::runtime::Handle;

use super::result;
use super::{key_bytes, value};

const COLLECTION_PREFIX: &str = "mix";

/// Inputs that determine a cell's storage setup and cleanup bounds.
pub(super) struct CellConfig {
    pub(super) databases: usize,
    pub(super) pool_size: usize,
    pub(super) split_quiet: Duration,
    pub(super) split_settle_timeout: Duration,
    pub(super) drain_timeout: Duration,
}

/// A seeded cell whose measurement clients have opened every target collection.
pub(super) struct PreparedCell {
    databases: Vec<Database>,
    collections: Vec<Arc<[Collection]>>,
    settlement: SplitSettlement,
}

impl PreparedCell {
    /// Returns the measurement clients in their stable home-collection order.
    pub(super) fn databases(&self) -> &[Database] {
        &self.databases
    }

    /// Starts the measured phase by capturing client counter baselines.
    pub(super) fn begin_measurement(self) -> ActiveCell {
        let baselines = self.databases.iter().map(Database::stats).collect();
        ActiveCell {
            databases: self.databases,
            collections: self.collections,
            settlement: self.settlement,
            baselines,
        }
    }
}

/// A prepared cell with measurement counter baselines captured.
pub(super) struct ActiveCell {
    databases: Vec<Database>,
    collections: Vec<Arc<[Collection]>>,
    settlement: SplitSettlement,
    baselines: Vec<Stats>,
}

impl ActiveCell {
    /// Returns the clients whose counters are part of the measured interval.
    pub(super) fn databases(&self) -> &[Database] {
        &self.databases
    }

    /// Returns each client's handles to every target collection.
    pub(super) fn collections(&self) -> &[Arc<[Collection]>] {
        &self.collections
    }

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
            setup_splits: self.settlement.completed,
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

/// Seeds storage, waits for structural settlement, and opens measurement clients.
pub(super) fn prepare_cell(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    database_name: &str,
    config: CellConfig,
) -> Result<PreparedCell, Box<dyn Error>> {
    let collection_paths = (0..config.databases)
        .map(|index| CollectionPath::new(format!("{COLLECTION_PREFIX}-{index}").as_bytes()))
        .collect::<Result<Vec<_>, _>>()?;
    let settlement = seed_and_settle(
        handle,
        backend.clone(),
        database_name,
        &collection_paths,
        &config,
    )?;
    let databases: Vec<_> = (0..config.databases)
        .map(|_| open_db(handle, database_name, backend.clone()))
        .collect();
    let collections = handle.block_on(open_collections(&databases, &collection_paths))?;
    Ok(PreparedCell {
        databases,
        collections,
        settlement,
    })
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

#[derive(Clone, Copy)]
struct SplitSettlement {
    completed: u64,
    elapsed: Duration,
}

struct SplitQuietTracker {
    completed: u64,
    unchanged_since: Instant,
}

impl SplitQuietTracker {
    fn new(completed: u64, now: Instant) -> Self {
        Self {
            completed,
            unchanged_since: now,
        }
    }

    fn observe(&mut self, completed: u64, now: Instant) {
        if completed != self.completed {
            self.completed = completed;
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

/// Waits until completed splits stop moving for a full quiet period.
async fn wait_for_split_quiet(
    database: &Database,
    quiet: Duration,
    timeout: Duration,
) -> Result<SplitSettlement, Box<dyn Error>> {
    let started = Instant::now();
    let mut tracker = SplitQuietTracker::new(database.stats().splitter.completed, started);
    let poll = (quiet / 4).clamp(Duration::from_millis(20), Duration::from_millis(250));
    loop {
        let now = Instant::now();
        tracker.observe(database.stats().splitter.completed, now);
        if tracker.is_quiet(now, quiet) {
            return Ok(SplitSettlement {
                completed: tracker.completed,
                elapsed: started.elapsed(),
            });
        }
        if deadline_expired(started, Instant::now(), timeout) {
            return Err(format!(
                "setup splits did not stay quiet for {quiet:?} within {timeout:?} \
                 (completed={})",
                tracker.completed
            )
            .into());
        }
        tokio::time::sleep(poll).await;
    }
}

/// Seeds every collection and lets its complete split cascade finish.
fn seed_and_settle(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    database_name: &str,
    paths: &[CollectionPath],
    config: &CellConfig,
) -> Result<SplitSettlement, Box<dyn Error>> {
    let database = open_db(handle, database_name, backend);
    handle.block_on(async {
        for path in paths {
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
    })?;
    let settlement = handle.block_on(wait_for_split_quiet(
        &database,
        config.split_quiet,
        config.split_settle_timeout,
    ))?;
    eprintln!(
        "{}",
        result::setup_settled(settlement.elapsed, settlement.completed, config.split_quiet)
    );
    handle.block_on(shutdown_databases_until(
        std::slice::from_ref(&database),
        tokio::time::Instant::now() + config.drain_timeout,
    ))?;
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
        let mut tracker = SplitQuietTracker::new(3, start);

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
    fn setup_seeds_each_collection_and_teardown_closes_clients() -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let handle = runtime.handle();
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let prepared = prepare_cell(
            handle,
            backend,
            "mixedsetuptest",
            CellConfig {
                databases: 2,
                pool_size: 3,
                split_quiet: Duration::from_millis(20),
                split_settle_timeout: Duration::from_secs(1),
                drain_timeout: Duration::from_secs(1),
            },
        )?;

        assert_eq!(prepared.databases().len(), 2);
        assert!(
            prepared
                .collections
                .iter()
                .all(|collections| collections.len() == 2)
        );
        for collection in prepared.collections[0].iter() {
            let keys: Vec<_> = handle.block_on(collection.iter_keys())?.collect();
            assert_eq!(keys, [key_bytes(0), key_bytes(1), key_bytes(2)]);
        }

        let probe = prepared.databases()[0].clone();
        let active = prepared.begin_measurement();
        let completed = active.teardown(
            handle,
            tokio::time::Instant::now() + Duration::from_secs(1),
            Ok(()),
        )?;
        assert_eq!(completed.databases, 2);
        assert!(matches!(
            handle.block_on(probe.root_collection().read(b"after-shutdown")),
            Err(GError::ShuttingDown)
        ));
        Ok(())
    }
}
