use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use glassdb::{Collection, CollectionPath, Database, Error as GError, NodeSizePolicy, Stats};
use glassdb_backend::Backend;
use glassdb_bench_scale::run::{SplitSettlement, shutdown_databases_until, wait_for_split_quiet};
use tokio::runtime::Handle;

use super::super::policy::{PolicySpec, ShadowTally};
use super::result;
use super::{key_bytes, value};

const COLLECTION_PREFIX: &str = "mix";

/// Inputs that determine a cell's storage setup and cleanup bounds.
pub(super) struct CellConfig {
    pub(super) databases: usize,
    pub(super) policy: PolicySpec,
    /// Policies that decide on the windows of the measurement clients
    /// without changing the tree.
    pub(super) shadows: Vec<PolicySpec>,
    /// Where the measurement clients record their windows and the shadow
    /// decisions, if the cell reports them.
    pub(super) tally: Option<Arc<ShadowTally>>,
    pub(super) pool_size: usize,
    /// The leaf entry limit of the setup Database, or none for the engine
    /// soft caps.
    pub(super) seed_leaf_entries: Option<usize>,
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

    /// Returns each client's handles to every target collection.
    pub(super) fn collections(&self) -> &[Arc<[Collection]>] {
        &self.collections
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
        .map(|index| {
            let builder = Database::builder(database_name, backend.clone());
            let builder = match &config.tally {
                Some(tally) => config
                    .policy
                    .apply_shadowed(builder, &config.shadows, tally, index),
                None => config.policy.apply(builder),
            };
            handle.block_on(builder.open()).expect("open db")
        })
        .collect();
    let collections = handle.block_on(open_collections(&databases, &collection_paths))?;
    Ok(PreparedCell {
        databases,
        collections,
        settlement,
    })
}

/// Opens the setup Database, with leaves of at most `leaf_entries` and no
/// underfull merges, if set.
fn open_setup_db(
    handle: &Handle,
    name: &str,
    backend: Arc<dyn Backend>,
    leaf_entries: Option<usize>,
) -> Result<Database, Box<dyn Error>> {
    let mut builder = Database::builder(name, backend);
    if let Some(entries) = leaf_entries {
        builder = builder.node_size_policy(
            NodeSizePolicy::builder()
                .leaf_max_entries(entries)
                .leaf_min_entries(0)
                .index_min_children(0)
                .build()?,
        );
    }
    Ok(handle.block_on(builder.open())?)
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

/// Seeds every collection and lets its complete split cascade finish.
fn seed_and_settle(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    database_name: &str,
    paths: &[CollectionPath],
    config: &CellConfig,
) -> Result<SplitSettlement, Box<dyn Error>> {
    let database = open_setup_db(handle, database_name, backend, config.seed_leaf_entries)?;
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
                policy: PolicySpec::Engine,
                shadows: Vec::new(),
                tally: None,
                pool_size: 3,
                seed_leaf_entries: None,
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

    #[test]
    fn shadow_policies_count_changes_on_a_seeded_tree_without_making_them()
    -> Result<(), Box<dyn Error>> {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let handle = runtime.handle();
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let tally = Arc::new(ShadowTally::new([PolicySpec::Size.label()], true));
        let prepared = prepare_cell(
            handle,
            backend,
            "mixedshadowtest",
            CellConfig {
                databases: 1,
                policy: PolicySpec::Fixed,
                shadows: vec![PolicySpec::Size],
                tally: Some(tally.clone()),
                pool_size: 4,
                seed_leaf_entries: Some(3),
                split_quiet: Duration::from_millis(200),
                split_settle_timeout: Duration::from_secs(5),
                drain_timeout: Duration::from_secs(1),
            },
        )?;
        let active = prepared.begin_measurement();
        tally.take();
        // A window is one second of real time, so the margin for a slow
        // machine is more than one window.
        let collection = &active.collections()[0][0];
        let stop = tokio::time::Instant::now() + Duration::from_millis(2500);
        let written = handle.block_on(async {
            while tokio::time::Instant::now() < stop {
                active.databases()[0]
                    .tx(|transaction| async move {
                        transaction.write(collection, &key_bytes(0), &value())
                    })
                    .await?;
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Ok::<(), GError>(())
        });
        let shadow = serde_json::to_value(tally.take())?;
        let completed = active.teardown(
            handle,
            tokio::time::Instant::now() + Duration::from_secs(1),
            written,
        )?;

        // The seed limit of 3 splits the 4 keys into leaves of 2, which are
        // under the minimum live entries of the default node size policy.
        assert_eq!(completed.setup_splits, 1);
        assert!(shadow["windows"].as_u64() >= Some(1), "{shadow}");
        assert!(shadow["leafWindows"].as_u64() >= Some(1), "{shadow}");
        assert_eq!(shadow["policies"][0]["policy"], "size", "{shadow}");
        assert!(
            shadow["policies"][0]["merges"].as_u64() >= Some(1),
            "{shadow}"
        );
        assert_eq!(
            shadow["trace"].as_array().map(|trace| trace.len() as u64),
            shadow["windows"].as_u64(),
            "{shadow}"
        );
        assert_eq!(shadow["trace"][0]["leaves"][0]["entries"], 2, "{shadow}");
        assert_eq!(completed.deltas[0].restructurer.merges, 0);
        Ok(())
    }
}
