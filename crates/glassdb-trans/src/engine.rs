//! The transaction engine and its runtime graph.

use std::num::NonZeroUsize;
use std::sync::Arc;
use std::time::Duration;

use glassdb_backend::{Backend, BackendError, BackendStats, StatsBackend};
use glassdb_concurr::{Background, DedupKeySnapshot, RetryConfig};
use glassdb_data::{
    CollectionAddress, CollectionId, DatabaseId, DbRoot, LogicalKey, ObjectPath, TxId,
};
use glassdb_storage::transaction::TLogger;
use glassdb_storage::{
    CacheStats, CachedStore, CollectionRecord, CollectionStore, InlinePolicy, LeafBody, Node,
    NodeStore, PersistentCache, PersistentCacheConfig, PersistentCacheMedia, Requirement,
    SplitPolicy, StorageError, StructuralIntentStore, Timeline, TreeRouter,
};

use crate::access::{AccessSet, ScanMutation, ScanRange};
use crate::algo::{Algo, BodyDecision, DirectCommitStats, Handle};
use crate::collection_catalog::CollectionCatalog;
use crate::collection_commit::{CollectionCommit, CollectionReservations};
use crate::collection_coordination::CollectionStateResolver;
use crate::collections::{CatalogAccesses, CollectionLifecycle, DirectorySnapshot};
use crate::error::TransError;
use crate::gc::{Gc, GcDiagnostics, GcHints, GcStats};
use crate::key_resolver::{KeyResolver, ScanResult};
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::{LeafCoordinator, LeafCoordinatorStats};
use crate::monitor::{Monitor, MonitorStats, ProtocolTiming};
use crate::reader::{ReadOutcome, Reader};
use crate::tlocker::{Locker, LockerStats};
use crate::tree_rebalancer::{TreeRebalancer, TreeRebalancerStats};

/// Balances backend traffic and memory use for a default production client.
const DEFAULT_CACHE_SIZE: usize = 512 * 1024 * 1024;
const DEFAULT_TRANSACTION_LEAF_PARALLELISM: NonZeroUsize = NonZeroUsize::new(16).unwrap();

#[derive(Clone)]
struct PersistentCacheSetup {
    config: PersistentCacheConfig,
    media: Option<PersistentCacheMedia>,
}

/// Coherent configuration for opening a transaction engine.
#[derive(Clone)]
pub struct EngineConfig {
    cache_size: usize,
    persistent_cache: Option<PersistentCacheSetup>,
    retry: RetryConfig,
    split_policy: SplitPolicy,
    inline_policy: InlinePolicy,
    protocol_timing: ProtocolTiming,
    transaction_leaf_parallelism: NonZeroUsize,
}

impl EngineConfig {
    /// Sets the decoded-object cache capacity.
    pub fn set_cache_size(&mut self, bytes: usize) {
        self.cache_size = bytes;
    }

    /// Enables the persistent encoded-body cache.
    pub fn set_persistent_cache(
        &mut self,
        config: PersistentCacheConfig,
        media: Option<PersistentCacheMedia>,
    ) {
        self.persistent_cache = Some(PersistentCacheSetup { config, media });
    }

    /// Sets the initial coordination retry delay.
    pub fn set_retry_initial_interval(&mut self, interval: Duration) {
        self.retry.initial_interval = interval;
    }

    /// Sets the maximum coordination retry delay.
    pub fn set_retry_max_interval(&mut self, interval: Duration) {
        self.retry.max_interval = interval;
    }

    /// Sets the shared tree-splitting policy.
    pub fn set_split_policy(&mut self, policy: SplitPolicy) {
        self.split_policy = policy;
    }

    /// Sets the direct-commit inline-value policy.
    pub fn set_inline_policy(&mut self, policy: InlinePolicy) {
        self.inline_policy = policy;
    }

    /// Sets transaction-liveness timing.
    pub fn set_protocol_timing(&mut self, timing: ProtocolTiming) {
        self.protocol_timing = timing;
    }

    /// Sets how many leaf operations one transaction can run in parallel in each bounded phase.
    pub fn set_transaction_leaf_parallelism(&mut self, parallelism: NonZeroUsize) {
        self.transaction_leaf_parallelism = parallelism;
    }
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            cache_size: DEFAULT_CACHE_SIZE,
            persistent_cache: None,
            retry: RetryConfig::default(),
            split_policy: SplitPolicy::default(),
            inline_policy: InlinePolicy::default(),
            protocol_timing: ProtocolTiming::default(),
            transaction_leaf_parallelism: DEFAULT_TRANSACTION_LEAF_PARALLELISM,
        }
    }
}

/// An opaque transaction attempt owned by [`Engine`].
pub struct EngineTransaction(Handle);

impl EngineTransaction {
    /// Returns the attempt's transaction identity.
    pub fn id(&self) -> &TxId {
        self.0.id()
    }

    /// Returns collection-ID reservations owned by the current identity.
    pub fn collection_reservations(&self) -> CollectionReservations {
        self.0.collection_reservations()
    }
}

/// Runtime counters collected from one engine snapshot.
pub struct EngineStats {
    /// Backend object operations.
    pub backend: BackendStats,
    /// Decoded and persistent cache activity.
    pub cache: CacheStats,
    /// Transaction final-status cache activity.
    pub monitor: MonitorStats,
    /// Distributed-locker activity.
    pub locker: LockerStats,
    /// Shared leaf-coordinator activity.
    pub coordinator: LeafCoordinatorStats,
    /// Logless direct-commit coverage.
    pub direct_commit: DirectCommitStats,
    /// Background tree rebalancing activity.
    pub tree_rebalancer: TreeRebalancerStats,
    /// Garbage collection activity.
    pub gc: GcStats,
}

/// Live coordination state collected from one engine snapshot.
pub struct EngineDiagnostics {
    /// Local GC queues and scan scope.
    pub gc: GcDiagnostics,
    /// Per-object deduplication state inside the leaf coordinator.
    pub coordinator_dedup: Vec<DedupKeySnapshot>,
}

/// Owns and mediates access to one database's transaction runtime.
pub struct Engine {
    backend: Arc<StatsBackend>,
    objects: CachedStore,
    monitor: Monitor,
    reader: Reader,
    resolver: KeyResolver,
    collection_catalog: CollectionCatalog,
    algo: Algo,
    coord: LeafCoordinator,
    locker: Locker,
    tree_rebalancer: TreeRebalancer,
    gc: Gc,
    // Subsystems hold weak references so this sole strong owner breaks task
    // capture cycles when the engine is dropped.
    background: Arc<Background>,
}

impl Engine {
    /// Opens and starts the transaction runtime for an initialized database.
    pub async fn open(
        name: &str,
        database_id: DatabaseId,
        backend: Arc<StatsBackend>,
        config: EngineConfig,
    ) -> Result<Self, StorageError> {
        let dormant = DormantEngine::open(name, database_id, backend, config).await?;
        Ok(dormant.start())
    }

    /// Creates or verifies the permanent collection objects before metadata is published.
    pub async fn prepare_permanent_collection<B>(
        backend: &B,
        name: &str,
    ) -> Result<(), StorageError>
    where
        B: Backend + ?Sized,
    {
        let collection = CollectionAddress::root(name);
        let record_path = ObjectPath::CollectionRecord {
            collection: collection.clone(),
        }
        .to_string();
        Self::ensure_collection_record(backend, &record_path).await?;
        let root_path = ObjectPath::TreeRoot { collection }.to_string();
        Self::ensure_tree_root(backend, &root_path).await
    }

    /// Reads one logical key with the requested staleness allowance.
    pub async fn read(
        &self,
        key: &LogicalKey,
        max_stale: Duration,
    ) -> Result<ReadOutcome, StorageError> {
        self.reader.read(key, max_stale).await
    }

    /// Scans one logical key range and returns its validation evidence.
    pub async fn scan(
        &self,
        collection: &CollectionAddress,
        range: &ScanRange,
        overlay: &[ScanMutation],
    ) -> Result<ScanResult, StorageError> {
        self.resolver
            .scan_keys(collection, range, overlay, None, None)
            .await
    }

    /// Loads a logical snapshot of a collection's direct-child directory.
    pub async fn collection_snapshot(
        &self,
        parent: &CollectionAddress,
    ) -> Result<DirectorySnapshot, TransError> {
        self.collection_catalog.snapshot(parent).await
    }

    /// Starts a transaction attempt from its collected logical accesses.
    pub fn begin_transaction(
        &self,
        accesses: AccessSet,
        catalog_accesses: CatalogAccesses,
    ) -> EngineTransaction {
        EngineTransaction(self.algo.begin(accesses, catalog_accesses))
    }

    /// Replaces the logical accesses of an uncommitted transaction attempt.
    pub fn reset_transaction(
        &self,
        tx: &mut EngineTransaction,
        accesses: AccessSet,
        catalog_accesses: CatalogAccesses,
    ) {
        self.algo
            .reset_with_collections(&mut tx.0, accesses, catalog_accesses);
    }

    /// Validates read-only accesses and reports whether the body must run again.
    pub async fn validate_reads(
        &self,
        tx: &mut EngineTransaction,
    ) -> Result<BodyDecision, TransError> {
        self.algo.validate_reads(&mut tx.0).await
    }

    /// Commits an attempt or reports that the body must run again.
    pub async fn commit(&self, tx: &mut EngineTransaction) -> Result<BodyDecision, TransError> {
        self.algo.commit(&mut tx.0).await
    }

    /// Finalizes a transaction attempt, aborting it when necessary.
    pub async fn end(&self, tx: &mut EngineTransaction) -> Result<(), TransError> {
        self.algo.end(&mut tx.0).await
    }

    /// Gracefully drains background work and closes engine storage.
    pub async fn shutdown(&self) {
        self.background.shutdown().await;
        self.coord.close().await;
        self.objects.shutdown().await;
    }

    /// Returns and resets all runtime component counters.
    pub fn stats_and_reset(&self) -> EngineStats {
        EngineStats {
            backend: self.backend.stats_and_reset(),
            cache: self.objects.cache_stats_and_reset(),
            monitor: self.monitor.stats_and_reset(),
            locker: self.locker.stats_and_reset(),
            coordinator: self.coord.stats_and_reset(),
            direct_commit: self.algo.direct_commit_stats_and_reset(),
            tree_rebalancer: self.tree_rebalancer.stats_and_reset(),
            gc: self.gc.stats_and_reset(),
        }
    }

    /// Returns the engine's live coordination diagnostics.
    pub fn diagnostics(&self) -> EngineDiagnostics {
        EngineDiagnostics {
            gc: self.gc.diagnostics(),
            coordinator_dedup: self.coord.dedup_snapshot(),
        }
    }

    async fn ensure_collection_record<B>(backend: &B, path: &str) -> Result<(), StorageError>
    where
        B: Backend + ?Sized,
    {
        match backend
            .write_if_not_exists(path, CollectionRecord::new().encode())
            .await
        {
            Ok(_) => Ok(()),
            Err(BackendError::Precondition) => {
                let stored = backend.read(path).await?;
                CollectionRecord::decode(&stored.contents)?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }

    async fn ensure_tree_root<B>(backend: &B, path: &str) -> Result<(), StorageError>
    where
        B: Backend + ?Sized,
    {
        match backend
            .write_if_not_exists(path, Node::leaf(LeafBody::new()).encode())
            .await
        {
            Ok(_) => Ok(()),
            Err(BackendError::Precondition) => {
                let stored = backend.read(path).await?;
                Node::decode(&stored.contents)?;
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    }
}

/// Shared storage, time, monitoring, and task ownership for an engine graph.
///
/// Focused unit fixtures use this foundation and add only the collaborator that
/// they exercise. Production construction extends it into the complete graph.
#[derive(Clone)]
struct AssemblyFoundation {
    backend: Arc<StatsBackend>,
    objects: CachedStore,
    records: CollectionStore,
    nodes: NodeStore,
    structural_intents: StructuralIntentStore,
    timeline: Timeline,
    tlogger: TLogger,
    background: Arc<Background>,
    monitor: Monitor,
}

impl AssemblyFoundation {
    fn new(
        backend: Arc<StatsBackend>,
        persistent: Option<PersistentCache>,
        timeline: Timeline,
        db_root: DbRoot,
        config: &EngineConfig,
    ) -> Self {
        let dyn_backend: Arc<dyn Backend> = backend.clone();
        let objects =
            CachedStore::new(dyn_backend, config.cache_size, timeline.clone(), persistent);
        let records = CollectionStore::new(objects.clone());
        let nodes = NodeStore::new(objects.clone(), config.transaction_leaf_parallelism);
        let structural_intents = StructuralIntentStore::new(objects.clone());
        let tlogger = TLogger::new(objects.clone(), db_root);
        let background = Arc::new(Background::new());
        let monitor = Monitor::with_config(
            tlogger.clone(),
            timeline.clone(),
            Arc::downgrade(&background),
            config.retry,
            config.protocol_timing,
        );
        Self {
            backend,
            objects,
            records,
            nodes,
            structural_intents,
            timeline,
            tlogger,
            background,
            monitor,
        }
    }
}

/// Direct runtime access for focused unit fixtures.
#[cfg(test)]
#[derive(Clone)]
pub(crate) struct AssemblyFixture {
    foundation: AssemblyFoundation,
    pub(crate) objects: CachedStore,
    pub(crate) records: CollectionStore,
    pub(crate) nodes: NodeStore,
    pub(crate) structural_intents: StructuralIntentStore,
    pub(crate) timeline: Timeline,
    pub(crate) tlogger: TLogger,
    pub(crate) background: Arc<Background>,
    pub(crate) monitor: Monitor,
}

#[cfg(test)]
impl AssemblyFixture {
    /// Creates a dormant test foundation over the supplied backend.
    pub(crate) fn new(backend: Arc<dyn Backend>, db_root: DbRoot, config: &EngineConfig) -> Self {
        let foundation = AssemblyFoundation::new(
            Arc::new(StatsBackend::new(backend)),
            None,
            Timeline::new(),
            db_root,
            config,
        );
        Self {
            objects: foundation.objects.clone(),
            records: foundation.records.clone(),
            nodes: foundation.nodes.clone(),
            structural_intents: foundation.structural_intents.clone(),
            timeline: foundation.timeline.clone(),
            tlogger: foundation.tlogger.clone(),
            background: foundation.background.clone(),
            monitor: foundation.monitor.clone(),
            foundation,
        }
    }

    /// Creates a monitor that follows a focused fixture's task owner.
    pub(crate) fn monitor_for(
        &self,
        background: &Arc<Background>,
        retry: RetryConfig,
        protocol_timing: ProtocolTiming,
    ) -> Monitor {
        Monitor::with_config(
            self.tlogger.clone(),
            self.timeline.clone(),
            Arc::downgrade(background),
            retry,
            protocol_timing,
        )
    }
}

/// A complete engine whose maintenance tasks have not started.
struct DormantEngine {
    engine: Engine,
}

impl DormantEngine {
    /// Opens storage and constructs the transaction runtime without starting work.
    async fn open(
        name: &str,
        database_id: DatabaseId,
        backend: Arc<StatsBackend>,
        config: EngineConfig,
    ) -> Result<Self, StorageError> {
        let db_root = DbRoot::try_from(name)
            .map_err(|error| StorageError::with_source("validating database root", error))?;
        let (persistent, timeline) = match config.persistent_cache.clone() {
            Some(setup) => {
                let opened =
                    PersistentCache::open(setup.config, name, database_id, setup.media).await;
                // Persistent evidence must establish the next timeline before
                // any database object can be observed.
                let timeline = Timeline::starting_after(opened.last_sequence_point);
                (Some(opened.cache), timeline)
            }
            None => (None, Timeline::new()),
        };
        let foundation =
            AssemblyFoundation::new(backend, persistent, timeline, db_root.clone(), &config);
        verify_permanent_collection(&db_root, &foundation).await?;
        Ok(Self::from_foundation(foundation, db_root, config, true))
    }

    /// Starts maintenance work and returns the live engine.
    fn start(self) -> Engine {
        self.engine.gc.start(&self.engine.background);
        self.engine.tree_rebalancer.start();
        self.engine
    }

    fn from_foundation(
        foundation: AssemblyFoundation,
        db_root: DbRoot,
        config: EngineConfig,
        managed_retirement: bool,
    ) -> Self {
        let EngineConfig {
            retry,
            split_policy,
            inline_policy,
            transaction_leaf_parallelism,
            ..
        } = config;
        let AssemblyFoundation {
            backend,
            objects,
            records,
            nodes,
            structural_intents,
            timeline,
            tlogger,
            background,
            monitor,
        } = foundation;
        let background_weak = Arc::downgrade(&background);
        let collection_state = CollectionStateResolver::new(
            records.clone(),
            tlogger.clone(),
            timeline.clone(),
            monitor.clone(),
            retry,
        );
        let collection_catalog = CollectionCatalog::new(collection_state.clone());
        let key_state = KeyStateResolver::new(monitor.clone());
        let router = TreeRouter::new(
            nodes.clone(),
            timeline.clone(),
            transaction_leaf_parallelism,
        );
        let resolver = KeyResolver::new(
            router.clone(),
            key_state.clone(),
            transaction_leaf_parallelism,
        );
        let reader = Reader::new(resolver.clone(), timeline.clone(), retry);
        let cleanup_hints = GcHints::default();
        let (coord, tree_rebalancer) = TreeRebalancer::with_coordinator(
            background_weak.clone(),
            records.clone(),
            nodes.clone(),
            structural_intents.clone(),
            timeline.clone(),
            monitor.clone(),
            key_state,
            retry,
            db_root,
            split_policy,
            inline_policy,
            cleanup_hints.clone(),
        );
        let locker = Locker::new(
            coord.clone(),
            TreeRouter::new(
                nodes.clone(),
                timeline.clone(),
                transaction_leaf_parallelism,
            ),
            collection_state,
            monitor.clone(),
            retry,
            transaction_leaf_parallelism,
        );
        let collection_lifecycle = CollectionLifecycle::new(
            records,
            nodes.clone(),
            monitor.clone(),
            retry,
            Arc::new(tree_rebalancer.clone()),
        );
        let gc = Gc::new(
            tlogger.clone(),
            nodes.clone(),
            structural_intents,
            timeline.clone(),
            locker.clone(),
            collection_lifecycle.clone(),
            monitor.protocol_timing(),
            cleanup_hints.clone(),
        );
        let collection_commit = CollectionCommit::new(
            collection_catalog.clone(),
            collection_lifecycle,
            monitor.clone(),
            split_policy,
        );
        let algo = Algo::new(
            nodes,
            timeline,
            retry,
            locker.clone(),
            coord.clone(),
            monitor.clone(),
            collection_commit,
            cleanup_hints,
            managed_retirement.then_some(background_weak),
            router,
            resolver.clone(),
            split_policy,
            inline_policy,
            tree_rebalancer.hint_sink(),
        );
        let engine = Engine {
            backend,
            objects,
            monitor,
            reader,
            resolver,
            collection_catalog,
            algo,
            coord,
            locker,
            tree_rebalancer,
            gc,
            background,
        };
        Self { engine }
    }
}

/// Direct handles from a dormant complete engine for Algo tests.
#[cfg(test)]
pub(crate) struct EngineFixture {
    pub(crate) algo: Algo,
    pub(crate) locker: Locker,
    _engine: DormantEngine,
}

/// Extends a focused foundation into the complete dormant engine.
#[cfg(test)]
pub(crate) fn engine_fixture(
    fixture: &AssemblyFixture,
    db_root: DbRoot,
    config: EngineConfig,
    managed_retirement: bool,
) -> EngineFixture {
    let dormant = DormantEngine::from_foundation(
        fixture.foundation.clone(),
        db_root,
        config,
        managed_retirement,
    );
    EngineFixture {
        algo: dormant.engine.algo.clone(),
        locker: dormant.engine.locker.clone(),
        _engine: dormant,
    }
}

async fn verify_permanent_collection(
    db_root: &DbRoot,
    foundation: &AssemblyFoundation,
) -> Result<(), StorageError> {
    let collection = CollectionAddress::from_db_root(db_root.clone(), CollectionId::root());
    let requirement = Requirement::after(foundation.timeline.currentness_barrier());
    match foundation
        .records
        .load_record(&collection, requirement)
        .await
    {
        Ok(_) => {}
        Err(StorageError::NotFound) => {
            return Err(StorageError::other(
                "initialized database is missing its permanent collection record",
            ));
        }
        Err(error) => return Err(error),
    }
    match foundation.nodes.load_root(&collection, requirement).await {
        Ok(_) => Ok(()),
        Err(StorageError::NotFound) => Err(StorageError::other(
            "initialized database is missing its permanent tree root",
        )),
        Err(error) => Err(error),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture};
    use glassdb_concurr::rt;
    use glassdb_data::NodeToken;
    use glassdb_storage::{CurrentState, IndexNode, LeafEntry};

    use super::*;
    use crate::access::{ReadAccess, WriteAccess};

    struct Workload {
        engine: Engine,
        backend: Arc<StatsBackend>,
        hooks: Arc<HookBackend>,
        collection: CollectionAddress,
        paths: Vec<ObjectPath>,
    }

    impl Workload {
        async fn open(inline: InlinePolicy, leaves: &[&[(&[u8], usize)]]) -> Self {
            let hooks = HookBackend::new(Arc::new(MemoryBackend::new()));
            let backend = Arc::new(StatsBackend::new(hooks.clone()));
            let collection = CollectionAddress::root("adaptation");
            Engine::prepare_permanent_collection(backend.as_ref(), collection.db_root())
                .await
                .unwrap();
            let tokens: Vec<_> = (0..leaves.len())
                .map(|index| NodeToken::from_bytes([index as u8 + 1; 16]))
                .collect();
            let paths: Vec<_> = tokens
                .iter()
                .map(|token| ObjectPath::Node {
                    collection: collection.clone(),
                    token: token.clone(),
                })
                .collect();
            for (index, entries) in leaves.iter().enumerate() {
                let node = Node::leaf(LeafBody::from_entries(entries.iter().map(
                    |(key, bytes)| {
                        LeafEntry::new(key.to_vec()).with_current(CurrentState::Inline {
                            writer: TxId::with_priority(1, b"seed"),
                            value: Arc::from(vec![0; *bytes]),
                        })
                    },
                )))
                .with_high_key(leaves.get(index + 1).map(|entries| entries[0].0.to_vec()))
                .with_right_sibling(tokens.get(index + 1).map(ToString::to_string));
                backend
                    .write_if_not_exists(&paths[index].to_string(), node.encode())
                    .await
                    .unwrap();
            }
            let root_path = ObjectPath::TreeRoot {
                collection: collection.clone(),
            }
            .to_string();
            let version = backend.read(&root_path).await.unwrap().version;
            let root = Node::index(IndexNode::from_children(tokens.iter().enumerate().map(
                |(index, token)| {
                    let separator = if index == 0 {
                        Vec::new()
                    } else {
                        leaves[index][0].0.to_vec()
                    };
                    (separator, token.to_string())
                },
            )));
            backend
                .write_if(&root_path, root.encode(), &version)
                .await
                .unwrap();
            let mut config = EngineConfig::default();
            config.set_inline_policy(inline);
            let engine = Engine::open(
                collection.db_root(),
                DatabaseId::from_bytes([1; 16]),
                backend.clone(),
                config,
            )
            .await
            .unwrap();
            rt::sleep(Duration::from_millis(1)).await;
            Self {
                engine,
                backend,
                hooks,
                collection,
                paths,
            }
        }

        async fn rmw(&self, keys: &[&[u8]], output_bytes: usize) -> Result<(), TransError> {
            let mut tx = self
                .engine
                .begin_transaction(AccessSet::default(), CatalogAccesses::default());
            loop {
                let mut reads = Vec::new();
                let mut writes = Vec::new();
                for raw in keys {
                    let key = LogicalKey::new(self.collection.clone(), raw);
                    let (value, evidence) =
                        self.engine.read(&key, Duration::MAX).await?.into_parts();
                    let value = value.ok_or_else(|| TransError::other("workload key is absent"))?;
                    let counter = decode_counter(&value.value)?;
                    let mut next = vec![0; output_bytes];
                    next[..8].copy_from_slice(&(counter + 1).to_le_bytes());
                    reads.push(ReadAccess::new(key.clone(), evidence));
                    writes.push(WriteAccess::put(key, Arc::from(next)));
                }
                self.engine.reset_transaction(
                    &mut tx,
                    AccessSet::new(reads, writes, Vec::new()),
                    CatalogAccesses::default(),
                );
                if self.engine.commit(&mut tx).await? == BodyDecision::ReturnOutcome {
                    break;
                }
            }
            self.engine.end(&mut tx).await?;
            // Let finite write-back and immediate GC hints finish so later
            // request counts do not include cleanup from these commits.
            rt::sleep(Duration::from_millis(1)).await;
            Ok(())
        }

        async fn source(&self, index: usize) -> Node {
            Node::decode(
                &self
                    .backend
                    .read(&self.paths[index].to_string())
                    .await
                    .unwrap()
                    .contents,
            )
            .unwrap()
        }

        async fn counter(&self, key: &[u8]) -> u64 {
            let (value, _) = self
                .engine
                .read(
                    &LogicalKey::new(self.collection.clone(), key),
                    Duration::MAX,
                )
                .await
                .unwrap()
                .into_parts();
            decode_counter(&value.unwrap().value).unwrap()
        }
    }

    fn decode_counter(value: &[u8]) -> Result<u64, TransError> {
        let bytes = value
            .get(..8)
            .ok_or_else(|| TransError::other("workload value has no counter"))?;
        let bytes = bytes
            .try_into()
            .map_err(|_| TransError::other("invalid workload counter"))?;
        Ok(u64::from_le_bytes(bytes))
    }

    fn requests(stats: BackendStats) -> u64 {
        stats.obj_reads + stats.obj_writes + stats.obj_lists
    }

    #[tokio::test(start_paused = true)]
    async fn concurrent_missed_direct_commits_can_merge_and_restore_the_direct_path() {
        let workload = Workload::open(
            InlinePolicy::default(),
            &[&[(b"a", 8), (b"b", 8)], &[(b"m", 8), (b"n", 8)]],
        )
        .await;
        workload.engine.stats_and_reset();
        for _ in 0..16 {
            let (a, b) = tokio::join!(
                workload.rmw(&[b"a", b"m"], 8),
                workload.rmw(&[b"b", b"n"], 8)
            );
            a.unwrap();
            b.unwrap();
        }
        let before = workload.engine.stats_and_reset();
        assert_eq!(before.direct_commit.landed, 0);
        assert_eq!(before.tree_rebalancer.merge_hints, 32);
        assert_eq!(before.tree_rebalancer.merges_completed, 0);
        rt::sleep(Duration::from_secs(31)).await;
        rt::sleep(Duration::from_millis(1)).await;
        assert_eq!(
            workload
                .engine
                .stats_and_reset()
                .tree_rebalancer
                .merges_completed,
            1
        );
        assert_eq!(workload.source(0).await.as_leaf().unwrap().len(), 4);
        let right = workload.source(1).await;
        let target = ObjectPath::Node {
            collection: workload.collection.clone(),
            token: NodeToken::try_from(right.forwarding_target().unwrap()).unwrap(),
        };
        assert_eq!(target, workload.paths[0]);
        workload.engine.stats_and_reset();
        for _ in 0..32 {
            workload.rmw(&[b"a", b"m"], 8).await.unwrap();
        }
        let after = workload.engine.stats_and_reset();
        assert_eq!(after.direct_commit.landed, 32);
        assert_eq!(
            requests(after.backend),
            32,
            "each warm commit still uses one request"
        );
        assert!(requests(before.backend) > requests(after.backend));
        for (key, expected) in [(b"a", 48), (b"m", 48), (b"b", 16), (b"n", 16)] {
            assert_eq!(workload.counter(key).await, expected);
        }
        workload.engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn left_direct_commit_can_win_before_merge_publication() {
        let workload = Workload::open(InlinePolicy::default(), &[&[(b"a", 8)], &[(b"m", 8)]]).await;
        for _ in 0..32 {
            workload.rmw(&[b"a", b"m"], 8).await.unwrap();
        }
        let entered = Arc::new(tokio::sync::Notify::new());
        let resume = Arc::new(tokio::sync::Notify::new());
        let publications = Arc::new(AtomicUsize::new(0));
        workload.hooks.set_before({
            let left = workload.paths[0].to_string();
            let right = workload.paths[1].to_string();
            let entered = entered.clone();
            let resume = resume.clone();
            let publications = publications.clone();
            move |operation| {
                let publication = matches!(operation,
                    BackendOp::WriteIf { path, value, .. } if *path == left
                        && Node::decode(value).is_ok_and(|node| node.structural_gate().intent().is_some())
                );
                if publication {
                    publications.fetch_add(1, Ordering::SeqCst);
                }
                let pause = matches!(operation,
                    BackendOp::WriteIf { path, value, .. } if *path == right
                        && Node::decode(value).is_ok_and(|node| node.structural_gate().intent().is_some())
                );
                let entered = entered.clone();
                let resume = resume.clone();
                let future: HookFuture = Box::pin(async move {
                    if pause {
                        entered.notify_one();
                        resume.notified().await;
                    }
                    Ok(())
                });
                future
            }
        });
        rt::sleep(Duration::from_secs(31)).await;
        tokio::time::timeout(Duration::from_secs(5), entered.notified())
            .await
            .expect("hinted merge must reach publication");
        assert!(
            workload
                .source(0)
                .await
                .structural_gate()
                .holder()
                .is_none()
        );
        assert!(
            workload
                .source(1)
                .await
                .structural_gate()
                .holder()
                .is_some()
        );
        assert_eq!(workload.counter(b"m").await, 32);
        workload.engine.stats_and_reset();

        tokio::time::timeout(Duration::from_secs(5), workload.rmw(&[b"a"], 8))
            .await
            .expect("left writes must not wait for merge publication")
            .unwrap();
        assert_eq!(workload.engine.stats_and_reset().direct_commit.landed, 1);
        resume.notify_waiters();
        rt::sleep(Duration::from_millis(1)).await;
        assert_eq!(publications.load(Ordering::SeqCst), 1);
        workload.hooks.clear_before();

        for index in [0, 1] {
            let node = workload.source(index).await;
            assert_eq!(node.as_leaf().unwrap().len(), 1);
            assert!(node.structural_gate().holder().is_none());
        }
        assert_eq!(workload.counter(b"a").await, 33);
        assert_eq!(workload.counter(b"m").await, 32);
        assert_eq!(
            workload
                .engine
                .stats_and_reset()
                .tree_rebalancer
                .merges_completed,
            0
        );
        workload.engine.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn merge_hints_leave_nonadjacent_or_inline_full_leaves_separate() {
        for (inline, leaves, keys) in [
            (
                InlinePolicy::default(),
                vec![vec![(b"a".as_slice(), 8)], vec![(b"m", 8)], vec![(b"z", 8)]],
                [b"a".as_slice(), b"z".as_slice()],
            ),
            (
                InlinePolicy {
                    max_value_bytes: 100,
                    max_leaf_bytes: 200,
                },
                vec![
                    vec![(b"a".as_slice(), 8), (b"b", 80)],
                    vec![(b"m", 8), (b"n", 80)],
                ],
                [b"a".as_slice(), b"m".as_slice()],
            ),
        ] {
            let refs: Vec<_> = leaves.iter().map(Vec::as_slice).collect();
            let workload = Workload::open(inline, &refs).await;
            for _ in 0..32 {
                workload.rmw(&keys, 8).await.unwrap();
            }
            rt::sleep(Duration::from_secs(31)).await;
            rt::sleep(Duration::from_millis(1)).await;
            let stats = workload.engine.stats_and_reset();
            assert_eq!(stats.tree_rebalancer.merge_candidates, 1);
            assert_eq!(stats.tree_rebalancer.merges_completed, 0);
            assert!(workload.source(0).await.forwarding_target().is_none());
            for key in keys {
                assert_eq!(workload.counter(key).await, 32);
            }
            workload.engine.shutdown().await;
        }
    }
}
