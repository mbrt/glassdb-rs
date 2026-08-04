//! The transaction-engine façade and its runtime assembly.

use std::sync::Arc;
use std::time::Duration;

use glassdb_backend::{Backend, BackendError, BackendStats, StatsBackend};
use glassdb_concurr::{Background, DedupKeySnapshot, RetryConfig};
use glassdb_data::{CollectionAddress, DatabaseId, KeyRef, TxId, paths};
use glassdb_storage::{
    CacheStats, CachedStore, CollectionRecord, CollectionStore, InlinePolicy, Node,
    PersistentCache, PersistentCacheConfig, PersistentCacheMedia, Requirement, Shard, ShardStore,
    SplitPolicy, StorageError, TLogger, Timeline, TreeRouter,
};

use crate::access::{Data, ScanMutation, ScanRange};
use crate::algo::{Algo, DirectCommitStats, Handle};
use crate::collection_catalog::CollectionCatalog;
use crate::collection_commit::CollectionCommit;
use crate::collection_coordination::CollectionStateResolver;
use crate::collections::{CollectionData, CollectionLifecycle, DirectorySnapshot};
use crate::error::TransError;
use crate::gc::Gc;
use crate::key_resolver::{KeyResolver, ScanResult};
use crate::key_state_resolver::KeyStateResolver;
use crate::monitor::{Monitor, ProtocolTiming};
use crate::reader::{ReadOutcome, Reader};
use crate::shard_coord::{ShardCoordinator, ShardCoordinatorStats};
use crate::split::{Splitter, SplitterStats};
use crate::tlocker::{Locker, LockerStats, TxLockSnapshot};

/// Balances backend traffic and memory use for a default production client.
const DEFAULT_CACHE_SIZE: usize = 512 * 1024 * 1024;

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
        }
    }
}

/// An opaque transaction attempt owned by [`Engine`].
pub struct EngineTransaction(Handle);

impl EngineTransaction {
    /// Returns the attempt's transaction ID.
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
    /// Shared shard-coordinator activity.
    pub coordinator: ShardCoordinatorStats,
    /// Logless direct-commit coverage.
    pub direct_commit: DirectCommitStats,
    /// Background tree-split activity.
    pub splitter: SplitterStats,
}

/// Live coordination state collected from one engine snapshot.
pub struct EngineDiagnostics {
    /// Per-object deduplication state inside the shard coordinator.
    pub coordinator_dedup: Vec<DedupKeySnapshot>,
    /// Transactions with locally tracked locks.
    pub transactions: Vec<TxLockSnapshot>,
}

/// Owns and mediates access to one database's transaction runtime.
pub struct Engine {
    backend: Arc<StatsBackend>,
    objects: CachedStore,
    reader: Reader,
    resolver: KeyResolver,
    collection_catalog: CollectionCatalog,
    algo: Algo,
    coord: ShardCoordinator,
    locker: Locker,
    splitter: Splitter,
    // Subsystems hold weak references so dropping this sole strong owner breaks
    // spawned-task capture cycles.
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
        let EngineConfig {
            cache_size,
            persistent_cache,
            retry,
            split_policy,
            inline_policy,
            protocol_timing,
        } = config;
        let dyn_backend: Arc<dyn Backend> = backend.clone();
        let (persistent, timeline) = match persistent_cache {
            Some(setup) => {
                let opened =
                    PersistentCache::open(setup.config, name, database_id, setup.media).await;
                // The persistent cache carries sequence points across restarts,
                // preventing stale cached objects from appearing fresh.
                let timeline = Timeline::starting_after(opened.last_sequence_point);
                (Some(opened.cache), timeline)
            }
            None => (None, Timeline::new()),
        };
        let objects = CachedStore::new(dyn_backend, cache_size, timeline.clone(), persistent);
        let records = CollectionStore::new(objects.clone());
        let shards = ShardStore::new(objects.clone());
        Self::verify_permanent_collection(name, &records, &shards, &timeline).await?;

        let tlogger = TLogger::new(objects.clone(), name);
        let background = Arc::new(Background::new());
        let background_weak = Arc::downgrade(&background);
        let monitor = Monitor::with_config(
            tlogger.clone(),
            timeline.clone(),
            background_weak.clone(),
            retry,
            protocol_timing,
        );
        let collection_state =
            CollectionStateResolver::new(records.clone(), tlogger.clone(), monitor.clone(), retry);
        let collection_catalog = CollectionCatalog::new(collection_state.clone());
        let key_state = KeyStateResolver::new(monitor.clone());
        let resolver = KeyResolver::new(TreeRouter::new(shards.clone()), key_state.clone());
        let reader = Reader::new(resolver.clone(), timeline.clone(), retry);
        let (coord, splitter) = Splitter::with_coordinator(
            background_weak.clone(),
            records.clone(),
            shards.clone(),
            timeline.clone(),
            monitor.clone(),
            key_state,
            retry,
            name,
            split_policy,
            inline_policy,
        );
        let locker = Locker::new(
            coord.clone(),
            TreeRouter::new(shards.clone()),
            collection_state,
            monitor.clone(),
            retry,
        );
        let collection_lifecycle = CollectionLifecycle::new(
            records,
            shards.clone(),
            monitor.clone(),
            retry,
            Arc::new(splitter.clone()),
        );
        let gc = Gc::new(
            background_weak.clone(),
            tlogger,
            shards.clone(),
            timeline.clone(),
            locker.clone(),
            collection_lifecycle.clone(),
            monitor.clone(),
        );
        gc.start();
        splitter.start();
        let collection_commit = CollectionCommit::new(
            collection_catalog.clone(),
            collection_lifecycle,
            monitor.clone(),
            split_policy,
        );
        let algo = Algo::new(
            shards,
            timeline,
            locker.clone(),
            coord.clone(),
            monitor,
            collection_commit,
            gc,
            Some(background_weak),
            resolver.clone(),
            split_policy,
            inline_policy,
            splitter.hint_sink(),
        );

        Ok(Self {
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
        })
    }

    /// Creates or verifies the permanent collection objects before metadata is published.
    pub async fn prepare_permanent_collection<B>(
        backend: &B,
        name: &str,
    ) -> Result<(), StorageError>
    where
        B: Backend + ?Sized,
    {
        let prefix = CollectionAddress::root(name).physical_prefix();
        Self::ensure_collection_record(backend, &paths::collection_record(&prefix)).await?;
        Self::ensure_tree_root(backend, &paths::tree_root(&prefix)).await
    }

    /// Reads one logical key with the requested staleness allowance.
    pub async fn read(
        &self,
        key: &KeyRef,
        max_stale: Duration,
    ) -> Result<ReadOutcome, StorageError> {
        self.reader.read(key, max_stale).await
    }

    /// Scans one logical key range and returns its validation evidence.
    pub async fn scan_keys(
        &self,
        collection: &CollectionAddress,
        range: &ScanRange,
        overlay: &[ScanMutation],
        own_lock_holder: Option<&TxId>,
        cap: Option<&[u8]>,
    ) -> Result<ScanResult, StorageError> {
        self.resolver
            .scan_keys(collection, range, overlay, own_lock_holder, cap)
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
        data: Data,
        collection_data: CollectionData,
    ) -> EngineTransaction {
        EngineTransaction(self.algo.begin(data, collection_data))
    }

    /// Replaces the logical accesses of an uncommitted transaction attempt.
    pub fn reset_transaction(
        &self,
        tx: &mut EngineTransaction,
        data: Data,
        collection_data: CollectionData,
    ) {
        self.algo
            .reset_with_collections(&mut tx.0, data, collection_data);
    }

    /// Restarts a wounded attempt with a fresh identity and preserved priority.
    pub fn rebegin_transaction(&self, tx: EngineTransaction) -> EngineTransaction {
        EngineTransaction(self.algo.rebegin(tx.0))
    }

    /// Validates the read-only accesses of a transaction attempt.
    pub async fn validate_reads(&self, tx: &mut EngineTransaction) -> Result<(), TransError> {
        self.algo.validate_reads(&mut tx.0).await
    }

    /// Commits a transaction attempt.
    pub async fn commit(&self, tx: &mut EngineTransaction) -> Result<(), TransError> {
        self.algo.commit(&mut tx.0).await
    }

    /// Finalizes a transaction attempt, aborting it when necessary.
    pub async fn end(&self, tx: &mut EngineTransaction) -> Result<(), TransError> {
        self.algo.end(&mut tx.0).await
    }

    /// Schedules cancellation cleanup for an abandoned transaction identity.
    pub fn async_abort(&self, tx_id: &TxId) {
        self.algo.async_abort(tx_id);
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
            transactions: self.locker.tx_locks_snapshot(),
        }
    }

    async fn verify_permanent_collection(
        name: &str,
        records: &CollectionStore,
        shards: &ShardStore,
        timeline: &Timeline,
    ) -> Result<(), StorageError> {
        let prefix = CollectionAddress::root(name).physical_prefix();
        let requirement = Requirement::AtLeast(timeline.now());
        match records.load_record(&prefix, requirement).await {
            Ok(_) => {}
            Err(StorageError::NotFound) => {
                return Err(StorageError::other(
                    "initialized database is missing its permanent collection record",
                ));
            }
            Err(error) => return Err(error),
        }
        match shards.load_root(&prefix, requirement).await {
            Ok(_) => Ok(()),
            Err(StorageError::NotFound) => Err(StorageError::other(
                "initialized database is missing its permanent tree root",
            )),
            Err(error) => Err(error),
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
            .write_if_not_exists(path, Node::leaf(Shard::new()).encode())
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
