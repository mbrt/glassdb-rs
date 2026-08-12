//! The database entry point. Ported from the Go `db.go`: opening a database,
//! the transaction retry loop, collections, and stats.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_backend::Backend;
use glassdb_concurr::rt;
use glassdb_data::{DatabaseId, TxId};
use glassdb_storage::{InlinePolicy, PersistentCacheConfig, PersistentCacheMedia, SplitPolicy};
use glassdb_trans::{
    CollectionData, Data, Engine, EngineConfig, EngineTransaction, ProtocolTiming, TransError,
};
use tokio::sync::Notify;

use crate::collection::{Collection, CollectionPath};
use crate::diagnostics::Diagnostics;
use crate::error::Error;
use crate::stats::{Stats, TransactionStats};
use crate::tx::Transaction;
use crate::version::check_or_create_db_meta;

/// Builds and opens a [`Database`], tweaking optional settings before opening.
///
/// Start from [`Database::builder`], chain any setters, then call
/// [`DatabaseBuilder::open`]. For the default configuration, [`Database::open`] is a
/// shorthand.
#[derive(Clone)]
pub struct DatabaseBuilder {
    name: String,
    backend: Arc<dyn Backend>,
    engine_config: EngineConfig,
}

impl DatabaseBuilder {
    /// Sets the number of bytes dedicated to caching objects and metadata.
    /// Setting this too small may impact performance, as more backend calls are
    /// necessary.
    pub fn cache_size(mut self, bytes: usize) -> Self {
        self.engine_config.set_cache_size(bytes);
        self
    }

    /// Enables the best-effort persistent encoded-body cache.
    ///
    /// The cache identity is derived automatically from the database name and
    /// its persistent ID. Production capacities must be at least 131 MiB.
    pub fn persistent_cache(self, config: PersistentCacheConfig) -> Self {
        self.configure_persistent_cache(config, None)
    }

    /// Sets the delay before the first retry of a transient
    /// transaction-coordination operation (polling a peer transaction's commit
    /// status, writing a transaction's final log, or reacquiring locks under the
    /// same identity after exhausted shard contention). The delay grows
    /// exponentially up to [`DatabaseBuilder::retry_max_interval`].
    pub fn retry_initial_interval(mut self, interval: Duration) -> Self {
        self.engine_config.set_retry_initial_interval(interval);
        self
    }

    /// Sets the upper bound on the per-retry delay for transient
    /// transaction-coordination and same-identity lock-acquisition operations.
    pub fn retry_max_interval(mut self, interval: Duration) -> Self {
        self.engine_config.set_retry_max_interval(interval);
        self
    }

    /// Overrides the node sizing policy, including split triggers and hard cap.
    /// Every client of one database should use the same policy because splits
    /// durably reshape shared topology.
    pub fn split_policy(mut self, policy: SplitPolicy) -> Self {
        self.engine_config.set_split_policy(policy);
        self
    }

    /// Overrides the budgets for logless direct commits whose authoritative
    /// value is stored in the leaf (ADR-051, ADR-054). Values outside the
    /// budgets take the regular logged protocol. Every client of one database
    /// should use the same policy because aggregate-pressure misses can request
    /// durable tree splits (ADR-056).
    pub fn inline_policy(mut self, policy: InlinePolicy) -> Self {
        self.engine_config.set_inline_policy(policy);
        self
    }

    /// Overrides transaction-liveness timing, including the pending lease and
    /// cross-client clock-skew allowance. The configured skew must bound every
    /// client using this database so a live transaction is never reclaimed.
    pub fn protocol_timing(mut self, timing: ProtocolTiming) -> Self {
        self.engine_config.set_protocol_timing(timing);
        self
    }

    /// Opens the database, validating the name and creating its metadata if
    /// needed.
    pub async fn open(self) -> Result<Database, Error> {
        let DatabaseBuilder {
            name,
            backend: b,
            engine_config,
        } = self;

        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(Error::InvalidInput(format!(
                "name must be alphanumeric, got {name:?}"
            )));
        }
        engine_config
            .validate()
            .map_err(|error| Error::InvalidInput(error.to_string()))?;
        let backend = Arc::new(glassdb_backend::StatsBackend::new(b));
        let database_id = check_or_create_db_meta(&backend, &name).await?;
        let engine = Engine::open(&name, database_id, backend, engine_config)
            .await
            .map_err(Error::from_read)?;

        let inner = Arc::new(DbInner {
            name,
            database_id,
            engine,
            stats: Mutex::new(Stats::default()),
            operations: OperationLifecycle::new(),
        });
        Ok(Database { inner })
    }

    /// Configures the persistent cache with an optional explicit media.
    pub(crate) fn configure_persistent_cache(
        mut self,
        config: PersistentCacheConfig,
        media: Option<PersistentCacheMedia>,
    ) -> Self {
        self.engine_config.set_persistent_cache(config, media);
        self
    }

    fn new(name: impl Into<String>, backend: Arc<dyn Backend>) -> Self {
        DatabaseBuilder {
            name: name.into(),
            backend,
            engine_config: EngineConfig::default(),
        }
    }
}

pub(crate) struct DbInner {
    pub(crate) name: String,
    pub(crate) database_id: DatabaseId,
    pub(crate) engine: Engine,
    stats: Mutex<Stats>,
    // Admission and drain cover every public asynchronous operation, including
    // the few APIs that do not run through a transaction.
    operations: OperationLifecycle,
}

/// An open GlassDB database instance.
#[derive(Clone)]
pub struct Database {
    inner: Arc<DbInner>,
}

impl Database {
    /// Starts building a database with the given name and backend. Chain setters
    /// on the returned [`DatabaseBuilder`], then call [`DatabaseBuilder::open`].
    ///
    /// `b` may be any concrete backend (`MemoryBackend::new()`, etc.) or a
    /// pre-erased `Arc<dyn Backend>` (covered by the crate's `impl Backend for
    /// Arc<B>` blanket).
    pub fn builder<B>(name: impl Into<String>, b: B) -> DatabaseBuilder
    where
        B: Backend + 'static,
    {
        DatabaseBuilder::new(name, Arc::new(b))
    }

    /// Opens a database with the given name using default options. Shorthand for
    /// `Database::builder(name, b).open()`.
    pub async fn open<B>(name: &str, b: B) -> Result<Database, Error>
    where
        B: Backend + 'static,
    {
        Database::builder(name, b).open().await
    }

    /// Gracefully shuts the database down: refuses new public asynchronous
    /// operations (they return [`Error::ShuttingDown`]) and awaits admitted
    /// operations and background protocol work. Post-commit key write-back is
    /// drained as a finite pass; a leaf blocked by a live structural holder is
    /// deferred to lazy recovery rather than waited on.
    /// Idempotent; safe to call from multiple [`Database`] clones concurrently.
    ///
    /// Dropping the last [`Database`] still aborts background work, but
    /// `shutdown` additionally waits for those tasks to stop. It cannot wait for
    /// a backend mutation whose future was previously abandoned by cancellation.
    pub async fn shutdown(&self) {
        self.inner.operations.shutdown().await;
        self.inner.engine.shutdown().await;
    }

    /// Returns the permanent, key-bearing root collection.
    pub fn root_collection(&self) -> Collection {
        Collection::new_root(self.inner.clone())
    }

    /// Resolves a logical path to its currently bound collection incarnation.
    ///
    /// A string names one top-level collection; use [`CollectionPath`] for a
    /// nested path.
    pub async fn open_collection<P>(&self, path: P) -> Result<Collection, Error>
    where
        P: TryInto<CollectionPath>,
        P::Error: Into<Error>,
    {
        let path = path.try_into().map_err(Into::into)?;
        self.inner
            .tx(move |tx| open_path_in_transaction(tx, path.clone()))
            .await
    }

    /// Reports whether every component of a logical collection path is bound.
    ///
    /// A string names one top-level collection; use [`CollectionPath`] for a
    /// nested path.
    pub async fn collection_exists<P>(&self, path: P) -> Result<bool, Error>
    where
        P: TryInto<CollectionPath>,
        P::Error: Into<Error>,
    {
        let path = path.try_into().map_err(Into::into)?;
        self.inner
            .tx(move |tx| path_exists_in_transaction(tx, path.clone()))
            .await
    }

    /// Strictly creates the final component of `path`.
    ///
    /// Every ancestor must already exist. A string names one top-level
    /// collection; use [`CollectionPath`] for a nested path.
    pub async fn create_collection<P>(&self, path: P) -> Result<Collection, Error>
    where
        P: TryInto<CollectionPath>,
        P::Error: Into<Error>,
    {
        let path = path.try_into().map_err(Into::into)?;
        self.inner
            .tx(move |tx| create_path_in_transaction(tx, path.clone(), PathCreateMode::Strict))
            .await
    }

    /// Returns the final component of `path`, creating it when absent.
    ///
    /// Every ancestor must already exist. A string names one top-level
    /// collection; use [`CollectionPath`] for a nested path.
    pub async fn create_collection_if_absent<P>(&self, path: P) -> Result<Collection, Error>
    where
        P: TryInto<CollectionPath>,
        P::Error: Into<Error>,
    {
        let path = path.try_into().map_err(Into::into)?;
        self.inner
            .tx(move |tx| create_path_in_transaction(tx, path.clone(), PathCreateMode::IfAbsent))
            .await
    }

    /// Executes `f` within a serializable transaction, retrying on conflicts.
    /// The value returned by `f` on a successful commit is returned to the
    /// caller.
    ///
    /// `f` receives the [`Transaction`] handle by value and returns a future, so the
    /// transaction future is `Send` and can be `tokio::spawn`-ed. Write the body
    /// as `|tx| async move { ... }`. The framework owns the retry loop and may
    /// invoke `f` multiple times, so `f` must be `FnMut`.
    ///
    /// # Body errors
    ///
    /// When `f` returns an error, the attempt's writes are discarded and its
    /// reads are validated. If those reads were inconsistent, `f` is invoked
    /// again; otherwise the original error is returned. Conditions derived from
    /// transaction reads must therefore return an error, for example with
    /// [`crate::ensure_tx!`], rather than assert or panic. Panics bypass read
    /// validation.
    ///
    /// # Cancellation
    ///
    /// This future is durability-safe to cancel: dropping it cannot produce a
    /// partial logical commit. It dispatches, but does not guarantee rollback,
    /// because a transaction-log commit or logless value CAS already dispatched
    /// when the future is dropped may still commit even though the caller
    /// receives no result.
    pub async fn tx<T, F, Fut>(&self, f: F) -> Result<T, Error>
    where
        F: FnMut(Transaction) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        self.inner.tx(f).await
    }

    /// Retrieves cumulative foreground and background performance stats.
    ///
    /// Counters only increase; subtract snapshots for intervals. Collection is
    /// not an atomic cut across concurrently active engine components.
    pub fn stats(&self) -> Stats {
        let engine = self.inner.engine.stats_and_reset();
        let delta = Stats {
            backend: engine.backend,
            cache: engine.cache,
            locker: engine.locker,
            coordinator: engine.coordinator,
            direct_commit: engine.direct_commit,
            splitter: engine.splitter,
            ..Default::default()
        };
        let mut stats = self.inner.stats.lock().unwrap();
        *stats += delta;
        *stats
    }

    /// Returns a snapshot of the shard coordinator's and locker's live state,
    /// intended for operators investigating hangs or unexpected contention. See
    /// [`crate::diagnostics`] for the data shape and how to enable the
    /// complementary `tracing` events.
    ///
    /// Pull-only and zero cost unless called: each shard's lock is taken
    /// briefly while collecting counts, then released.
    pub fn diagnostics(&self) -> Diagnostics {
        let engine = self.inner.engine.diagnostics();
        Diagnostics {
            coordinator_dedup: engine.coordinator_dedup,
            transactions: engine.transactions,
        }
    }
}

enum PathCreateMode {
    Strict,
    IfAbsent,
}

/// Resolves `path` through one serializable transaction.
async fn open_path_in_transaction(
    tx: Transaction,
    path: CollectionPath,
) -> Result<Collection, Error> {
    tx.open_collection_path(&path).await
}

/// Checks every binding in `path` through one serializable transaction.
async fn path_exists_in_transaction(tx: Transaction, path: CollectionPath) -> Result<bool, Error> {
    tx.collection_path_exists(&path).await
}

/// Creates the final binding in `path` through one serializable transaction.
async fn create_path_in_transaction(
    tx: Transaction,
    path: CollectionPath,
    mode: PathCreateMode,
) -> Result<Collection, Error> {
    let mut segments = path.segments();
    let name = segments
        .next_back()
        .expect("CollectionPath always has one segment");
    let mut parent = tx.root_collection();
    for segment in segments {
        parent = tx.open_collection(&parent, segment).await?;
    }
    match mode {
        PathCreateMode::Strict => tx.create_collection(&parent, name).await,
        PathCreateMode::IfAbsent => Ok(tx.create_collection_if_absent(&parent, name).await?.0),
    }
}

struct OperationLifecycle {
    state: Mutex<OperationState>,
    drained: Notify,
}

struct OperationState {
    shutting_down: bool,
    active: usize,
}

impl OperationLifecycle {
    fn new() -> Self {
        Self {
            state: Mutex::new(OperationState {
                shutting_down: false,
                active: 0,
            }),
            drained: Notify::new(),
        }
    }

    fn admit(&self) -> Result<OperationGuard<'_>, Error> {
        let mut state = self.state.lock().unwrap();
        if state.shutting_down {
            return Err(Error::ShuttingDown);
        }
        state.active += 1;
        Ok(OperationGuard { lifecycle: self })
    }

    async fn shutdown(&self) {
        loop {
            let notified = self.drained.notified();
            {
                let mut state = self.state.lock().unwrap();
                state.shutting_down = true;
                if state.active == 0 {
                    return;
                }
            }
            notified.await;
        }
    }
}

pub(crate) struct OperationGuard<'a> {
    lifecycle: &'a OperationLifecycle,
}

impl Drop for OperationGuard<'_> {
    fn drop(&mut self) {
        let mut state = self.lifecycle.state.lock().unwrap();
        state.active -= 1;
        if state.active == 0 {
            self.lifecycle.drained.notify_waiters();
        }
    }
}

impl DbInner {
    /// Admits one public asynchronous operation or rejects it once shutdown has
    /// begun.
    pub(crate) fn admit_operation(&self) -> Result<OperationGuard<'_>, Error> {
        self.operations.admit()
    }

    pub(crate) async fn tx<T, F, Fut>(self: &Arc<Self>, f: F) -> Result<T, Error>
    where
        F: FnMut(Transaction) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let _guard = self.admit_operation()?;

        let mut stats = TransactionStats {
            completed: 1,
            ..Default::default()
        };
        let begin = rt::Instant::now();
        let res = self.tx_impl(f, &mut stats).await;
        stats.elapsed = begin.elapsed();
        self.update_transaction_stats(stats);
        res
    }

    fn update_transaction_stats(&self, transaction: TransactionStats) {
        let mut stats = self.stats.lock().unwrap();
        stats.transactions += transaction;
    }

    async fn tx_impl<T, F, Fut>(
        self: &Arc<Self>,
        mut f: F,
        stats: &mut TransactionStats,
    ) -> Result<T, Error>
    where
        F: FnMut(Transaction) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let tx = Transaction::new(self.clone());
        let mut driver = AttemptDriver::new(&self.engine);

        let result: Result<T, Error> = loop {
            // Hand a fresh handle to the user closure (which consumes it); `tx`
            // retains access to the same shared state to collect accesses and
            // reset between retries.
            let fn_res = f(tx.handle()).await;
            if tx.aborted() {
                break Err(Error::Aborted);
            }

            // Collect the accesses produced by the user function.
            let (access, collection_access) = tx.collect_accesses();
            let metrics = tx.metrics();
            stats.reads += access.reads.len() as u64;
            stats.cache_hits += metrics.cache_hits;
            stats.writes += access.writes.len() as u64;

            let restart_after_wound = if fn_res.is_ok() {
                driver.install_accesses(access, collection_access);
                match driver.commit().await {
                    Ok(()) => break fn_res,
                    Err(TransError::Wounded) => true,
                    Err(TransError::Retry) => false,
                    Err(e) => break Err(e.into()),
                }
            } else {
                // The user function returned an error. It might be the result
                // of a spurious read, so validate only the reads.
                match driver.validate_body_error(access, collection_access).await {
                    Err(TransError::Retry) => false,
                    Err(TransError::Wounded) => true,
                    _ => break fn_res,
                }
            };

            if restart_after_wound {
                // A higher-priority transaction aborted us. Release whatever
                // we held and restart with a fresh id that preserves our
                // priority, so we are not starved on the retry.
                driver.restart_after_wound().await;
            }
            tx.reset();
            stats.retries += 1;
        };

        let end_result = driver.finish().await;
        if let Err(e) = end_result
            && result.is_ok()
        {
            return Err(e.into());
        }
        result
    }
}

/// Drives the engine-side transitions for one public transaction.
struct AttemptDriver<'a> {
    engine: &'a Engine,
    resources: Option<AttemptResources<'a>>,
}

impl<'a> AttemptDriver<'a> {
    fn new(engine: &'a Engine) -> Self {
        Self {
            engine,
            resources: None,
        }
    }

    /// Installs the accesses collected from the latest closure execution.
    fn install_accesses(&mut self, access: Data, collection_access: CollectionData) {
        match self.resources.as_mut() {
            Some(resources) => {
                self.engine
                    .reset_transaction(&mut resources.handle, access, collection_access)
            }
            None => {
                let handle = self.engine.begin_transaction(access, collection_access);
                self.resources = Some(AttemptResources::new(self.engine, handle));
            }
        }
    }

    /// Validates the reads that led the transaction body to return an error.
    async fn validate_body_error(
        &mut self,
        mut access: Data,
        collection_access: CollectionData,
    ) -> Result<(), TransError> {
        access.writes.clear();
        self.install_accesses(access, collection_access.into_read_only());
        let resources = self
            .resources
            .as_mut()
            .expect("body-error validation installs attempt resources");
        self.engine.validate_reads(&mut resources.handle).await
    }

    /// Restarts an attempt that a higher-priority transaction wounded.
    async fn restart_after_wound(&mut self) {
        let resources = self
            .resources
            .as_mut()
            .expect("a wound is reported only for an active attempt");
        let _ = self.engine.end(&mut resources.handle).await;
        let resources = self
            .resources
            .take()
            .expect("the ended attempt remains active until it is renewed");
        self.resources = Some(resources.rebegin());
    }

    /// Commits the accesses installed for the latest closure execution.
    async fn commit(&mut self) -> Result<(), TransError> {
        let resources = self
            .resources
            .as_mut()
            .expect("commit follows access installation");
        self.engine.commit(&mut resources.handle).await
    }

    /// Finalizes any active engine attempt.
    async fn finish(mut self) -> Result<(), TransError> {
        let Some(resources) = self.resources.as_mut() else {
            return Ok(());
        };
        let result = self.engine.end(&mut resources.handle).await;
        resources.abort_guard.disarm();
        result
    }
}

/// Engine state for one active attempt and its cancellation safety net.
///
/// Storing this as one optional value ensures the handle and armed guard exist
/// together or not at all.
struct AttemptResources<'a> {
    handle: EngineTransaction,
    abort_guard: TransactionAbortGuard<'a>,
}

impl<'a> AttemptResources<'a> {
    fn new(engine: &'a Engine, handle: EngineTransaction) -> Self {
        let tx_id = handle.id().clone();
        Self {
            handle,
            abort_guard: TransactionAbortGuard::new(engine, tx_id),
        }
    }

    fn rebegin(mut self) -> Self {
        let engine = self.abort_guard.engine;
        self.abort_guard.disarm();
        let handle = engine.rebegin_transaction(self.handle);
        Self::new(engine, handle)
    }
}

/// RAII safety net for [`DbInner::tx_impl`]: if the surrounding future is
/// dropped between attempt begin and end, the guard schedules recovery for the
/// currently armed transaction id. Before terminal dispatch this publishes a
/// pinned wound so peers need not wait for the lock lease; after dispatch it
/// preserves the possibly committed outcome.
///
/// Whether the armed id actually needs an abort is the engine's decision, not
/// the guard's: an attempt that never took a logged identity is
/// invisible to peers and must not be given an abort-side object it never had.
struct TransactionAbortGuard<'a> {
    engine: &'a Engine,
    armed: Option<TxId>,
}

impl<'a> TransactionAbortGuard<'a> {
    fn new(engine: &'a Engine, tx_id: TxId) -> Self {
        Self {
            engine,
            armed: Some(tx_id),
        }
    }

    /// Disarms the guard once the attempt has ended so `Drop` is a no-op.
    fn disarm(&mut self) {
        self.armed = None;
    }
}

impl Drop for TransactionAbortGuard<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.armed.take() {
            self.engine.async_abort(&id);
        }
    }
}

#[cfg(test)]
mod tests {
    use glassdb_backend::memory::MemoryBackend;

    use super::*;

    #[tokio::test]
    async fn wound_restart_renews_the_attempt_and_remains_finishable() {
        let db = Database::open("attempts", MemoryBackend::new())
            .await
            .unwrap();
        let mut driver = AttemptDriver::new(&db.inner.engine);
        driver.install_accesses(Data::default(), CollectionData::default());

        let original_id = driver
            .resources
            .as_ref()
            .expect("access installation starts an attempt")
            .handle
            .id()
            .clone();
        driver.restart_after_wound().await;
        let renewed_id = driver
            .resources
            .as_ref()
            .expect("wound restart keeps an active attempt")
            .handle
            .id()
            .clone();

        assert_ne!(renewed_id, original_id);
        driver.finish().await.unwrap();
        db.shutdown().await;
    }
}
