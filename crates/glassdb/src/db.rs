//! The database entry point. Ported from the Go `db.go`: opening a database,
//! the transaction retry loop, collections, and stats.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_backend::Backend;
use glassdb_concurr::rt;
use glassdb_data::DatabaseId;
use glassdb_storage::{InlinePolicy, PersistentCacheConfig, PersistentCacheMedia, SplitPolicy};
use glassdb_trans::{
    AccessSet, BodyOutcome, CollectionData, Engine, EngineConfig, EngineTransaction,
    ProtocolTiming, TransError,
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
    /// Managed abandonment retirement is part of the drain and can wait
    /// indefinitely on storage or protocol recovery. Cancelling this future is
    /// safe; a later call resumes the shutdown.
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
    /// [`crate::ensure_tx!`], rather than assert or panic.
    ///
    /// # Panics
    ///
    /// A panic from `f` propagates with its original payload. It is neither
    /// read-validated nor retried, even when the execution observed an
    /// inconsistent snapshot. Uncommitted database changes stay unpublished.
    /// If an earlier execution left an engine attempt active for a retained
    /// retry, unwinding synchronously hands that attempt to managed recovery;
    /// physical lock and prepared-object reclamation may finish later.
    ///
    /// # Cancellation
    ///
    /// This future is durability-safe to cancel: dropping it cannot produce a
    /// partial logical commit. Active attempt resources are handed to managed
    /// recovery before the future is destroyed. Cancellation does not guarantee
    /// rollback, because a transaction-log commit or logless value CAS already
    /// dispatched when the future is dropped may still commit even though the
    /// caller receives no result.
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

    /// Returns a snapshot of the shard coordinator's live state, intended for
    /// operators investigating hangs or unexpected contention. See
    /// [`crate::diagnostics`] for the data shape and how to enable the
    /// complementary `tracing` events.
    pub fn diagnostics(&self) -> Diagnostics {
        let engine = self.inner.engine.diagnostics();
        Diagnostics {
            coordinator_dedup: engine.coordinator_dedup,
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
            let (accesses, collection_access) = tx.collect_accesses();
            let metrics = tx.metrics();
            stats.reads += accesses.read_count() as u64;
            stats.cache_hits += metrics.cache_hits;
            stats.writes += accesses.write_count() as u64;

            if fn_res.is_ok() {
                driver.install_accesses(accesses, collection_access);
                match driver.commit().await {
                    Ok(BodyOutcome::Complete) => break fn_res,
                    Ok(BodyOutcome::ReplayBody) => {}
                    Err(e) => break Err(e.into()),
                }
            } else {
                // The user function returned an error. It might be the result
                // of a spurious read, so validate only the reads. The error may
                // escape only once validation has certified that snapshot: an
                // attempt that could not finish proves nothing about the reads
                // behind the error, so the caller learns about the failed
                // validation instead.
                match driver
                    .validate_body_error(accesses, collection_access)
                    .await
                {
                    Ok(BodyOutcome::ReplayBody) => {}
                    Ok(BodyOutcome::Complete) => break fn_res,
                    Err(e) => break Err(Error::from_read_validation(e)),
                }
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
    handle: Option<EngineTransaction>,
}

impl<'a> AttemptDriver<'a> {
    fn new(engine: &'a Engine) -> Self {
        Self {
            engine,
            handle: None,
        }
    }

    /// Installs the accesses collected from the latest closure execution.
    fn install_accesses(&mut self, accesses: AccessSet, collection_access: CollectionData) {
        match self.handle.as_mut() {
            Some(handle) => self
                .engine
                .reset_transaction(handle, accesses, collection_access),
            None => {
                self.handle = Some(self.engine.begin_transaction(accesses, collection_access));
            }
        }
    }

    /// Validates the reads that led the transaction body to return an error.
    async fn validate_body_error(
        &mut self,
        accesses: AccessSet,
        collection_access: CollectionData,
    ) -> Result<BodyOutcome, TransError> {
        self.install_accesses(
            accesses.into_read_only(),
            collection_access.into_read_only(),
        );
        let handle = self
            .handle
            .as_mut()
            .expect("body-error validation installs an attempt");
        self.engine.validate_reads(handle).await
    }

    /// Commits the accesses installed for the latest closure execution.
    async fn commit(&mut self) -> Result<BodyOutcome, TransError> {
        let handle = self
            .handle
            .as_mut()
            .expect("commit follows access installation");
        self.engine.commit(handle).await
    }

    /// Finalizes any active engine attempt.
    async fn finish(mut self) -> Result<(), TransError> {
        let Some(handle) = self.handle.as_mut() else {
            return Ok(());
        };
        self.engine.end(handle).await
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture};
    use glassdb_backend::{Backend, BackendError};

    use super::*;

    const SERIAL_TRANSITION_CONFLICTS: usize = 3 * 50;

    fn fail_root_conflicts(backend: &HookBackend, failures: usize) -> Arc<AtomicUsize> {
        let remaining = Arc::new(AtomicUsize::new(failures));
        let hook_remaining = remaining.clone();
        backend.set_before(move |operation| {
            let fail = matches!(
                operation,
                BackendOp::WriteIf { path, .. } if path.ends_with("/_r")
            ) && hook_remaining
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |count| {
                    count.checked_sub(1)
                })
                .is_ok();
            let result = if fail {
                Err(BackendError::Precondition)
            } else {
                Ok(())
            };
            let future: HookFuture = Box::pin(async move { result });
            future
        });
        remaining
    }

    #[tokio::test(start_paused = true)]
    async fn serial_renewal_does_not_replay_a_point_transaction_body() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()) as Arc<dyn Backend>);
        let db = Database::builder("serialpoint", backend.clone())
            .inline_policy(InlinePolicy::none())
            .open()
            .await
            .unwrap();
        let remaining = fail_root_conflicts(&backend, SERIAL_TRANSITION_CONFLICTS);
        let bodies = Arc::new(AtomicUsize::new(0));

        db.tx({
            let bodies = bodies.clone();
            move |tx| {
                bodies.fetch_add(1, Ordering::SeqCst);
                async move {
                    let root = tx.root_collection();
                    tx.write(&root, b"key", b"value")
                }
            }
        })
        .await
        .unwrap();

        assert_eq!(remaining.load(Ordering::SeqCst), 0);
        assert_eq!(bodies.load(Ordering::SeqCst), 1);
        db.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn serial_renewal_replays_a_collection_change_body() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()) as Arc<dyn Backend>);
        let db = Database::builder("serialcollection", backend.clone())
            .inline_policy(InlinePolicy::none())
            .open()
            .await
            .unwrap();
        let remaining = fail_root_conflicts(&backend, SERIAL_TRANSITION_CONFLICTS);
        let bodies = Arc::new(AtomicUsize::new(0));

        let child = db
            .tx({
                let bodies = bodies.clone();
                move |tx| {
                    bodies.fetch_add(1, Ordering::SeqCst);
                    async move {
                        let root = tx.root_collection();
                        let child = tx.create_collection(&root, b"child").await?;
                        tx.write(&root, b"key", b"value")?;
                        Ok(child)
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(remaining.load(Ordering::SeqCst), 0);
        assert_eq!(bodies.load(Ordering::SeqCst), 2);
        assert_eq!(child.name(), Some(b"child".as_slice()));
        db.shutdown().await;
    }
}
