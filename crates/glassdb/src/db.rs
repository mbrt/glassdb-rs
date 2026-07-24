//! The database entry point. Ported from the Go `db.go`: opening a database,
//! the transaction retry loop, collections, and stats.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use glassdb_backend::{Backend, StatsBackend};
use glassdb_concurr::{Background, Clock, RetryConfig};
use glassdb_data::{CollectionPath, TxId};
use glassdb_storage::{
    CachedStore, Directory, PersistentCache, PersistentCacheConfig, ShardStore, SplitPolicy,
    TLogger, Timeline,
};
use glassdb_trans::{
    Algo, Gc, Locker, Monitor, ProtocolTiming, Resolver, ShardCoordinator, Splitter, TransError,
};
use tokio::sync::Notify;

use crate::collection::Collection;
use crate::diagnostics::Diagnostics;
use crate::error::Error;
use crate::stats::Stats;
use crate::tx::Transaction;
use crate::version::check_or_create_db_meta;

/// Default cache size: 512 MiB, a reasonable middle ground for production.
const DEFAULT_CACHE_SIZE: usize = 512 * 1024 * 1024;

/// Fixed wall-clock anchor used when `deterministic_time` is set: 2023-11-14
/// 22:13:20 UTC. The exact value is irrelevant; it only needs to be constant so
/// replays are byte-identical.
const DETERMINISTIC_EPOCH_SECS: u64 = 1_700_000_000;

/// Builds and opens a [`Database`], tweaking optional settings before opening.
///
/// Start from [`Database::builder`], chain any setters, then call
/// [`DatabaseBuilder::open`]. For the default configuration, [`Database::open`] is a
/// shorthand.
#[derive(Clone)]
pub struct DatabaseBuilder {
    name: String,
    backend: Arc<dyn Backend>,
    cache_size: usize,
    persistent_cache: Option<PersistentCacheConfig>,
    deterministic_time: bool,
    retry: RetryConfig,
    split_policy: SplitPolicy,
    protocol_timing: ProtocolTiming,
}

impl DatabaseBuilder {
    /// Sets the number of bytes dedicated to caching objects and metadata.
    /// Setting this too small may impact performance, as more backend calls are
    /// necessary.
    pub fn cache_size(mut self, bytes: usize) -> Self {
        self.cache_size = bytes;
        self
    }

    /// Enables the best-effort persistent encoded-body cache.
    ///
    /// The cache identity is derived automatically from the database name and
    /// its persistent ID. Production capacities must be at least 131 MiB.
    pub fn persistent_cache(mut self, config: PersistentCacheConfig) -> Self {
        self.persistent_cache = Some(config);
        self
    }

    /// Sets the delay before the first retry of a transient
    /// transaction-coordination operation (polling a peer transaction's commit
    /// status, or writing a transaction's final log). The delay grows
    /// exponentially up to [`DatabaseBuilder::retry_max_interval`].
    pub fn retry_initial_interval(mut self, interval: Duration) -> Self {
        self.retry.initial_interval = interval;
        self
    }

    /// Sets the upper bound on the per-retry delay for transient
    /// transaction-coordination operations.
    pub fn retry_max_interval(mut self, interval: Duration) -> Self {
        self.retry.max_interval = interval;
        self
    }

    /// When true, wall-clock reads are anchored to a fixed base plus the
    /// runtime's (mockable/virtual) elapsed time instead of the real system
    /// clock. Combined with the simulation executor this makes transaction-id
    /// timestamps — and thus the transaction-log object keys derived from them —
    /// a deterministic function of the simulation seed. Intended for the
    /// deterministic fuzzer; leave false in production.
    pub fn deterministic_time(mut self, enabled: bool) -> Self {
        self.deterministic_time = enabled;
        self
    }

    /// Overrides the node sizing policy, including split triggers and hard cap.
    pub fn split_policy(mut self, policy: SplitPolicy) -> Self {
        self.split_policy = policy;
        self
    }

    /// Overrides transaction-liveness timing, including the pending lease and
    /// cross-client clock-skew allowance. The configured skew must bound every
    /// client using this database so a live transaction is never reclaimed.
    pub fn protocol_timing(mut self, timing: ProtocolTiming) -> Self {
        self.protocol_timing = timing;
        self
    }

    /// Opens the database, validating the name and creating its metadata if
    /// needed.
    pub async fn open(self) -> Result<Database, Error> {
        let DatabaseBuilder {
            name,
            backend: b,
            cache_size,
            persistent_cache,
            deterministic_time,
            retry,
            split_policy,
            protocol_timing,
        } = self;

        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(Error::InvalidInput(format!(
                "name must be alphanumeric, got {name:?}"
            )));
        }
        let backend = Arc::new(StatsBackend::new(b));
        let database_id = check_or_create_db_meta(&backend, &name).await?;
        let dyn_backend: Arc<dyn Backend> = backend.clone();
        let (persistent, last_sequence_point) = match persistent_cache {
            Some(config) => {
                let opened = PersistentCache::open(config, &name, database_id).await;
                (Some(opened.cache), opened.last_sequence_point)
            }
            None => (None, None),
        };
        let timeline = Timeline::starting_after(last_sequence_point);
        let objects = CachedStore::new(dyn_backend, cache_size, timeline.clone(), persistent);
        let shards = ShardStore::new(objects.clone());
        let tl = TLogger::new(objects.clone(), &name);
        let bg = Arc::new(Background::new());
        let clock = if deterministic_time {
            Clock::anchored_at(UNIX_EPOCH + Duration::from_secs(DETERMINISTIC_EPOCH_SECS))
        } else {
            Clock::real()
        };
        // Subsystems hold `Weak<Background>`. `DbInner::background` (set below)
        // is the sole strong owner; this prevents spawned-task captures (which
        // close over `Monitor`/`Gc`/`Algo` clones) from forming a cycle that
        // would keep `Background` alive forever.
        let bg_weak = Arc::downgrade(&bg);
        let tmon = Monitor::with_config(
            tl.clone(),
            timeline.clone(),
            bg_weak.clone(),
            clock.clone(),
            retry,
            protocol_timing,
        );
        let resolver = Resolver::new(shards.clone(), tmon.clone());
        let dir = Directory::new(shards.clone());
        // Build the coordinator and splitter as a co-wired pair over one shared
        // candidate feed: leaf writes report split candidates into the feed,
        // while leaf splits acquire through the same coordinator.
        let (coord, splitter) = Splitter::with_coordinator(
            bg_weak.clone(),
            shards.clone(),
            timeline.clone(),
            tmon.clone(),
            clock.clone(),
            retry,
            &name,
            split_policy,
        );
        let locker = Locker::new(coord.clone(), dir, tmon.clone(), retry);
        let gc = Gc::new(
            bg_weak.clone(),
            tl,
            shards.clone(),
            timeline.clone(),
            locker.clone(),
            tmon.clone(),
            clock.clone(),
        );
        gc.start();
        splitter.start();
        let algo = Algo::new(
            shards.clone(),
            timeline.clone(),
            locker.clone(),
            coord.clone(),
            tmon.clone(),
            clock,
            gc,
            Some(bg_weak),
            resolver,
            split_policy,
        );

        let inner = Arc::new(DbInner {
            name,
            backend,
            objects,
            shards,
            timeline,
            tmon,
            algo,
            coord,
            locker,
            splitter,
            retry,
            stats: Mutex::new(Stats::default()),
            operations: OperationLifecycle::new(),
            background: bg,
        });
        Ok(Database { inner })
    }

    fn new(name: impl Into<String>, backend: Arc<dyn Backend>) -> Self {
        DatabaseBuilder {
            name: name.into(),
            backend,
            cache_size: DEFAULT_CACHE_SIZE,
            persistent_cache: None,
            deterministic_time: false,
            retry: RetryConfig::default(),
            split_policy: SplitPolicy::default(),
            protocol_timing: ProtocolTiming::default(),
        }
    }
}

pub(crate) struct DbInner {
    pub(crate) name: String,
    pub(crate) backend: Arc<StatsBackend>,
    pub(crate) objects: CachedStore,
    pub(crate) shards: ShardStore,
    pub(crate) timeline: Timeline,
    pub(crate) tmon: Monitor,
    pub(crate) algo: Algo,
    pub(crate) coord: ShardCoordinator,
    pub(crate) locker: Locker,
    pub(crate) splitter: Splitter,
    pub(crate) retry: RetryConfig,
    stats: Mutex<Stats>,
    // Admission and drain cover every public asynchronous operation, including
    // the few APIs that do not run through a transaction.
    operations: OperationLifecycle,
    // Sole strong owner of the background task manager. Subsystems (Monitor,
    // Gc, Algo) hold `Weak<Background>`s so that captured clones inside
    // spawned tasks do not form a cycle that would prevent `Background::drop`
    // from firing. When this struct drops, the `Arc` count reaches zero,
    // `Background::drop` aborts every spawned task, and the captured clones
    // unwind. `Database::shutdown` uses the same owner to wait for tasks that opted
    // into clean-shutdown draining.
    background: Arc<Background>,
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
    /// operations and background protocol work. Idempotent; safe to call from
    /// multiple [`Database`] clones concurrently.
    ///
    /// Dropping the last [`Database`] still aborts background work, but
    /// `shutdown` additionally waits for those tasks to stop. It cannot wait for
    /// a backend mutation whose future was previously abandoned by cancellation.
    pub async fn shutdown(&self) {
        self.inner.operations.shutdown().await;
        // Drain background write-backs and async aborts first: a backgrounded
        // write-back submits through the locker's dedup, so it must complete
        // before the dedup is closed.
        self.inner.background.shutdown().await;
        self.inner.coord.close().await;
        self.inner.objects.shutdown().await;
    }

    /// Returns a top-level collection with the given name.
    pub fn collection(&self, name: &[u8]) -> Collection {
        self.inner
            .open_collection(CollectionPath::new(self.inner.name.as_str(), name))
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
    /// # Cancellation
    ///
    /// This future is durability-safe to cancel: dropping it mid-flight is
    /// equivalent to a crash and is recovered by the commit protocol, so it
    /// never corrupts data or leaves a half-applied transaction. Cancel by
    /// dropping the surrounding future — e.g. via `tokio::time::timeout`,
    /// `select!`, or `JoinHandle::abort`. The cancelled attempt's transaction
    /// log entry is asynchronously marked aborted from `Transaction`'s `Drop`, so
    /// peer transactions observe the release immediately; the lock-lease
    /// timeout is only the backstop for when the abort write itself fails.
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
        let bstats = self.inner.backend.stats_and_reset();
        let lock_calls = self.inner.locker.lock_calls_and_reset() as u64;
        let coord = self.inner.coord.stats_and_reset();
        let split = self.inner.splitter.stats_and_reset();
        let cache = self.inner.objects.cache_stats_and_reset();
        let mut s = self.inner.stats.lock().unwrap();
        s.add_backend(&bstats);
        s.add_protocol(lock_calls, coord, split);
        s.add_cache(cache);
        *s
    }

    /// Returns a snapshot of the shard coordinator's and locker's live state,
    /// intended for operators investigating hangs or unexpected contention. See
    /// [`crate::diagnostics`] for the data shape and how to enable the
    /// complementary `tracing` events.
    ///
    /// Pull-only and zero cost unless called: each shard's lock is taken
    /// briefly while collecting counts, then released.
    pub fn diagnostics(&self) -> Diagnostics {
        Diagnostics {
            coordinator_dedup: self.inner.coord.dedup_snapshot(),
            transactions: self.inner.locker.tx_locks_snapshot(),
        }
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
    pub(crate) fn open_collection(self: &Arc<Self>, path: CollectionPath) -> Collection {
        Collection::new(path, self.clone())
    }

    /// Admits one public asynchronous operation or rejects it once shutdown has
    /// begun.
    pub(crate) fn admit_operation(&self) -> Result<OperationGuard<'_>, Error> {
        self.operations.admit()
    }

    pub(crate) async fn tx<T, F, Fut>(&self, f: F) -> Result<T, Error>
    where
        F: FnMut(Transaction) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let _guard = self.admit_operation()?;

        let mut stats = Stats {
            tx_n: 1,
            ..Default::default()
        };
        let begin = std::time::Instant::now();
        let res = self.tx_impl(f, &mut stats).await;
        stats.tx_time = begin.elapsed();
        self.update_stats(&stats);
        res
    }

    fn update_stats(&self, s: &Stats) {
        let mut stats = self.stats.lock().unwrap();
        stats.add(s);
    }

    async fn tx_impl<T, F, Fut>(&self, mut f: F, stats: &mut Stats) -> Result<T, Error>
    where
        F: FnMut(Transaction) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let tx = Transaction::new(
            self.shards.clone(),
            self.timeline.clone(),
            self.tmon.clone(),
            self.retry,
        );
        let mut handle = None;
        // RAII safety net: if this future is dropped between `algo.begin` and
        // `algo.end` (e.g. by `tokio::time::timeout` or `JoinHandle::abort`),
        // the guard's `Drop` runs `algo.async_abort` so the engine-side tx is
        // marked aborted promptly instead of lingering until lease expiry.
        // Updated to the current tx id after every `begin`/`rebegin`; cleared
        // once `end` has run.
        let mut abort_guard = TransactionAbortGuard::new(&self.algo);

        let result: Result<T, Error> = loop {
            // Hand a fresh handle to the user closure (which consumes it); `tx`
            // retains access to the same shared state to collect accesses and
            // reset between retries.
            let fn_res = f(tx.handle()).await;
            if tx.aborted() {
                break Err(Error::Aborted);
            }

            // Collect the accesses produced by the user function.
            let access = tx.collect_accesses();
            let metrics = tx.metrics();
            stats.tx_reads += access.reads.len() as u64;
            stats.tx_cache_hits += metrics.cache_hits;
            stats.tx_writes += access.writes.len() as u64;

            let value = match fn_res {
                Ok(v) => {
                    // Hand the full access (reads and writes) to the handle. The
                    // handle owns the data from here on; the wound path below
                    // recovers it from the handle, so no separate clone is kept.
                    match handle.as_mut() {
                        None => {
                            let h = self.algo.begin(access);
                            abort_guard.arm(h.id().clone());
                            handle = Some(h);
                        }
                        Some(h) => self.algo.reset(h, access),
                    }
                    v
                }
                Err(ferr) => {
                    // The user function returned an error. It might be the
                    // result of a spurious read, so validate only the reads.
                    let mut ro = access;
                    ro.writes.clear();
                    match handle.as_mut() {
                        None => {
                            let h = self.algo.begin(ro);
                            abort_guard.arm(h.id().clone());
                            handle = Some(h);
                        }
                        Some(h) => self.algo.reset(h, ro),
                    }
                    let h = handle.as_mut().unwrap();
                    match self.algo.validate_reads(h).await {
                        Err(TransError::Retry) => {
                            tx.reset();
                            stats.tx_retries += 1;
                            continue;
                        }
                        Err(TransError::Wounded) => {
                            if let Some(h) = handle.as_mut() {
                                let _ = self.algo.end(h).await;
                            }
                            let old = handle.take().unwrap();
                            let new = self.algo.rebegin(old);
                            abort_guard.arm(new.id().clone());
                            handle = Some(new);
                            tx.reset();
                            stats.tx_retries += 1;
                            continue;
                        }
                        _ => break Err(ferr),
                    }
                }
            };

            // Try to commit.
            let commit_res = {
                let h = handle.as_mut().unwrap();
                self.algo.commit(h).await
            };
            match commit_res {
                Ok(()) => break Ok(value),
                Err(TransError::Wounded) => {
                    // A higher-priority transaction aborted us. Release whatever
                    // we held and restart with a fresh id that preserves our
                    // priority, so we are not starved on the retry.
                    if let Some(h) = handle.as_mut() {
                        let _ = self.algo.end(h).await;
                    }
                    let old = handle.take().unwrap();
                    let new = self.algo.rebegin(old);
                    // Refresh the cancellation safety net with the new id so a
                    // drop after the rebegin aborts the retry's tx, not the
                    // (already-ended) original.
                    abort_guard.arm(new.id().clone());
                    handle = Some(new);
                    tx.reset();
                    stats.tx_retries += 1;
                    continue;
                }
                Err(TransError::Retry) => {
                    tx.reset();
                    stats.tx_retries += 1;
                    continue;
                }
                Err(e) => break Err(e.into()),
            }
        };

        // Always finalize the handle (a committed handle is a no-op). The
        // safety-net guard is disarmed either way so its `Drop` does not fire
        // a redundant async abort for an already-finalized tx.
        let end_result = if let Some(h) = handle.as_mut() {
            self.algo.end(h).await
        } else {
            Ok(())
        };
        abort_guard.disarm();
        if let Err(e) = end_result
            && result.is_ok()
        {
            return Err(e.into());
        }
        result
    }
}

/// RAII safety net for [`DbInner::tx_impl`]: if the surrounding future is
/// dropped between `algo.begin` and `algo.end`, the guard's `Drop` runs
/// [`Algo::async_abort`] for the currently-armed transaction id, so peer
/// transactions see the abort marker quickly instead of waiting for the
/// lock-lease timeout.
struct TransactionAbortGuard<'a> {
    algo: &'a Algo,
    armed: Option<TxId>,
}

impl<'a> TransactionAbortGuard<'a> {
    fn new(algo: &'a Algo) -> Self {
        Self { algo, armed: None }
    }

    /// Arms the guard for `tx_id`. Replaces any prior id (e.g. after a wound
    /// retry that gets a fresh id via `algo.rebegin`).
    fn arm(&mut self, tx_id: TxId) {
        self.armed = Some(tx_id);
    }

    /// Disarms the guard: called once `algo.end` ran so `Drop` is a no-op.
    fn disarm(&mut self) {
        self.armed = None;
    }
}

impl Drop for TransactionAbortGuard<'_> {
    fn drop(&mut self) {
        if let Some(id) = self.armed.take() {
            self.algo.async_abort(&id);
        }
    }
}
