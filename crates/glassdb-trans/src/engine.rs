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
use crate::collection_commit::CollectionCommit;
use crate::collection_coordination::CollectionStateResolver;
use crate::collections::{CatalogAccesses, CollectionLifecycle, DirectorySnapshot};
use crate::error::TransError;
use crate::gc::{Gc, TxCleanupHints};
use crate::key_resolver::{KeyResolver, ScanResult};
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::{LeafCoordinator, LeafCoordinatorStats};
use crate::monitor::{Monitor, ProtocolTiming};
use crate::reader::{ReadOutcome, Reader};
use crate::split::{Splitter, SplitterStats};
use crate::tlocker::{Locker, LockerStats};

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
}

/// Runtime counters collected from one engine snapshot.
pub struct EngineStats {
    /// Backend object operations.
    pub backend: BackendStats,
    /// Decoded and persistent cache activity.
    pub cache: CacheStats,
    /// Distributed-locker activity.
    pub locker: LockerStats,
    /// Shared leaf-coordinator activity.
    pub coordinator: LeafCoordinatorStats,
    /// Logless direct-commit coverage.
    pub direct_commit: DirectCommitStats,
    /// Background tree-split activity.
    pub splitter: SplitterStats,
}

/// Live coordination state collected from one engine snapshot.
pub struct EngineDiagnostics {
    /// Per-object deduplication state inside the leaf coordinator.
    pub coordinator_dedup: Vec<DedupKeySnapshot>,
}

/// Owns and mediates access to one database's transaction runtime.
pub struct Engine {
    backend: Arc<StatsBackend>,
    objects: CachedStore,
    reader: Reader,
    resolver: KeyResolver,
    collection_catalog: CollectionCatalog,
    algo: Algo,
    coord: LeafCoordinator,
    locker: Locker,
    splitter: Splitter,
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
            locker: self.locker.stats_and_reset(),
            coordinator: self.coord.stats_and_reset(),
            direct_commit: self.algo.direct_commit_stats_and_reset(),
            splitter: self.splitter.stats_and_reset(),
        }
    }

    /// Returns the engine's live coordination diagnostics.
    pub fn diagnostics(&self) -> EngineDiagnostics {
        EngineDiagnostics {
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
    gc: Gc,
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
        self.gc.start();
        self.engine.splitter.start();
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
        let collection_state =
            CollectionStateResolver::new(records.clone(), tlogger.clone(), monitor.clone(), retry);
        let collection_catalog = CollectionCatalog::new(collection_state.clone());
        let key_state = KeyStateResolver::new(monitor.clone());
        let router = TreeRouter::new(nodes.clone(), transaction_leaf_parallelism);
        let resolver = KeyResolver::new(
            router.clone(),
            key_state.clone(),
            transaction_leaf_parallelism,
        );
        let reader = Reader::new(resolver.clone(), timeline.clone(), retry);
        let cleanup_hints = TxCleanupHints::default();
        let (coord, splitter) = Splitter::with_coordinator(
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
            TreeRouter::new(nodes.clone(), transaction_leaf_parallelism),
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
            Arc::new(splitter.clone()),
        );
        let gc = Gc::new(
            background_weak.clone(),
            tlogger,
            nodes.clone(),
            structural_intents,
            timeline.clone(),
            locker.clone(),
            collection_lifecycle.clone(),
            monitor.clone(),
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
            monitor,
            collection_commit,
            cleanup_hints,
            managed_retirement.then_some(background_weak),
            router,
            resolver.clone(),
            split_policy,
            inline_policy,
            splitter.hint_sink(),
        );
        let engine = Engine {
            backend,
            objects,
            reader,
            resolver,
            collection_catalog,
            algo,
            coord,
            locker,
            splitter,
            background,
        };
        Self { engine, gc }
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
    let requirement = Requirement::AtLeast(foundation.timeline.now());
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
