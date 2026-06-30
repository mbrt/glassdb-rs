//! The database entry point. Ported from the Go `db.go`: opening a database,
//! the transaction retry loop, collections, and stats.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use glassdb_backend::{Backend, StatsBackend};
use glassdb_concurr::{Background, Clock, RetryConfig};
use glassdb_data::{TxId, paths};
use glassdb_storage::{Global, Local, ShardStore, TLogger};
use glassdb_trans::{Algo, Gc, Locker, Monitor, Reader, TransError};
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
    deterministic_time: bool,
    retry: RetryConfig,
}

impl DatabaseBuilder {
    fn new(name: impl Into<String>, backend: Arc<dyn Backend>) -> Self {
        DatabaseBuilder {
            name: name.into(),
            backend,
            cache_size: DEFAULT_CACHE_SIZE,
            deterministic_time: false,
            retry: RetryConfig::default(),
        }
    }

    /// Sets the number of bytes dedicated to caching objects and metadata.
    /// Setting this too small may impact performance, as more backend calls are
    /// necessary.
    pub fn cache_size(mut self, bytes: usize) -> Self {
        self.cache_size = bytes;
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

    /// Opens the database, validating the name and creating its metadata if
    /// needed.
    pub async fn open(self) -> Result<Database, Error> {
        let DatabaseBuilder {
            name,
            backend: b,
            cache_size,
            deterministic_time,
            retry,
        } = self;

        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(Error::InvalidInput(format!(
                "name must be alphanumeric, got {name:?}"
            )));
        }
        check_or_create_db_meta(&b, &name).await?;

        let backend = Arc::new(StatsBackend::new(b));
        let dyn_backend: Arc<dyn Backend> = backend.clone();
        let local = Local::new(cache_size);
        let global = Global::new(dyn_backend, local.clone());
        // One shared, uncached shard/root coordination store over the backend.
        let shards = ShardStore::new(backend.clone());
        let tl = TLogger::new(global.clone(), local.clone(), &name);
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
        let tmon = Monitor::with_config(local.clone(), tl.clone(), bg_weak.clone(), clock, retry);
        let reader = Reader::new(local.clone(), shards.clone(), tmon.clone(), retry);
        let locker = Locker::new(shards.clone(), tmon.clone(), retry);
        let gc = Gc::new(bg_weak.clone(), tl);
        gc.start();
        let algo = Algo::new(
            global.clone(),
            local.clone(),
            locker,
            tmon.clone(),
            gc,
            Some(bg_weak),
            reader,
        );

        let inner = Arc::new(DbInner {
            name,
            backend,
            shards,
            local,
            tmon,
            algo,
            retry,
            stats: Mutex::new(Stats::default()),
            shutting_down: AtomicBool::new(false),
            tx_in_flight: AtomicUsize::new(0),
            tx_drained: Notify::new(),
            background: bg,
        });
        Ok(Database { inner })
    }
}

pub(crate) struct DbInner {
    pub(crate) name: String,
    pub(crate) backend: Arc<StatsBackend>,
    pub(crate) shards: ShardStore,
    pub(crate) local: Local,
    pub(crate) tmon: Monitor,
    pub(crate) algo: Algo,
    pub(crate) retry: RetryConfig,
    stats: Mutex<Stats>,
    // Graceful-shutdown bookkeeping. `shutting_down` flips first so any
    // subsequent `Database::tx` call rejects with `Error::ShuttingDown`. In-flight
    // transactions increment `tx_in_flight` while their future runs; the
    // decrement notifies `tx_drained` so `Database::shutdown` can await drain.
    shutting_down: AtomicBool,
    tx_in_flight: AtomicUsize,
    tx_drained: Notify,
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

    /// Gracefully shuts the database down: refuses any new [`Database::tx`] calls
    /// (they return [`Error::ShuttingDown`]) and awaits every in-flight
    /// transaction future to complete. Idempotent; safe to call from multiple
    /// `Database` clones concurrently.
    ///
    /// Background loops (GC, transaction-log refresh, async lock cleanup) and
    /// best-effort tasks are torn down via [`Drop`] when the last [`Database`] clone
    /// is dropped; calling `shutdown` is *not* required for cleanup to happen.
    /// Clean-shutdown background tasks, such as async aborts scheduled by a
    /// cancelled transaction, are awaited before `shutdown` returns.
    pub async fn shutdown(&self) {
        self.inner.shutting_down.store(true, Ordering::SeqCst);
        loop {
            // Acquire the wait future BEFORE re-checking the counter so a
            // racing in-flight drop's notify cannot be missed.
            let notified = self.inner.tx_drained.notified();
            if self.inner.tx_in_flight.load(Ordering::SeqCst) == 0 {
                break;
            }
            notified.await;
        }
        // Drain spawned dedup owner tasks so callers observing `shutdown` to
        // return synchronize with their full release.
        self.inner.algo.close().await;
        self.inner.background.shutdown().await;
    }

    /// Returns a top-level collection with the given name.
    pub fn collection(&self, name: &[u8]) -> Collection {
        let p = paths::from_collection(&self.inner.name, name);
        self.inner.open_collection(p)
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

    /// Retrieves ongoing performance stats. Only updated when transactions
    /// close. Counters only increase; subtract snapshots for intervals.
    pub fn stats(&self) -> Stats {
        let bstats = self.inner.backend.stats_and_reset();
        let mut s = self.inner.stats.lock().unwrap();
        s.add_backend(&bstats);
        *s
    }

    /// Returns a snapshot of the lock coordinator's live state, intended for
    /// operators investigating hangs or unexpected contention. See
    /// [`crate::diagnostics`] for the data shape and how to enable the
    /// complementary `tracing` events.
    ///
    /// Pull-only and zero cost unless called: each shard's lock is taken
    /// briefly while collecting counts, then released.
    pub fn diagnostics(&self) -> Diagnostics {
        let locker = self.inner.algo.locker();
        Diagnostics {
            locker_dedup: locker.dedup_snapshot(),
            transactions: locker.tx_locks_snapshot(),
        }
    }
}

/// RAII counter for in-flight user transactions. The increment happens on
/// construction (so `Database::shutdown` sees the new tx) and the decrement on drop
/// (so a cancelled transaction future still releases its slot). When the
/// counter hits zero, [`DbInner::tx_drained`] is notified, releasing any
/// `shutdown` waiter.
struct InFlightGuard<'a> {
    inner: &'a DbInner,
}

impl<'a> InFlightGuard<'a> {
    fn new(inner: &'a DbInner) -> Self {
        inner.tx_in_flight.fetch_add(1, Ordering::SeqCst);
        Self { inner }
    }
}

impl Drop for InFlightGuard<'_> {
    fn drop(&mut self) {
        if self.inner.tx_in_flight.fetch_sub(1, Ordering::SeqCst) == 1 {
            // notify_waiters wakes every current waiter (`shutdown` uses
            // exactly one) and remains correct even if multiple shutdowns
            // race.
            self.inner.tx_drained.notify_waiters();
        }
    }
}

impl DbInner {
    pub(crate) fn open_collection(self: &Arc<Self>, prefix: String) -> Collection {
        Collection::new(prefix, self.clone())
    }

    fn update_stats(&self, s: &Stats) {
        let mut stats = self.stats.lock().unwrap();
        stats.add(s);
    }

    pub(crate) async fn tx<T, F, Fut>(&self, f: F) -> Result<T, Error>
    where
        F: FnMut(Transaction) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        if self.shutting_down.load(Ordering::SeqCst) {
            return Err(Error::ShuttingDown);
        }
        // RAII: increment now and decrement on drop, so a cancelled (dropped)
        // future still notifies the shutdown waiter.
        let _guard = InFlightGuard::new(self);

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

    async fn tx_impl<T, F, Fut>(&self, mut f: F, stats: &mut Stats) -> Result<T, Error>
    where
        F: FnMut(Transaction) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        let tx = Transaction::new(
            self.shards.clone(),
            self.local.clone(),
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
            stats.tx_reads += access.reads.len() as u64;
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
