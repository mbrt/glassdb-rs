//! The database entry point. Ported from the Go `db.go`: opening a database,
//! the transaction retry loop, collections, and stats.

use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, UNIX_EPOCH};

use glassdb_backend::{Backend, StatsBackend};
use glassdb_concurr::{Background, CancelToken, Clock, RetryConfig};
use glassdb_data::paths;
use glassdb_storage::{Global, Local, TLogger};
use glassdb_trans::{Algo, Gc, Locker, Monitor};

use crate::collection::Collection;
use crate::diagnostics::Diagnostics;
use crate::error::Error;
use crate::stats::Stats;
use crate::tx::Tx;
use crate::version::check_or_create_db_meta;

/// Default cache size: 512 MiB, a reasonable middle ground for production.
const DEFAULT_CACHE_SIZE: usize = 512 * 1024 * 1024;

/// Fixed wall-clock anchor used when `deterministic_time` is set: 2023-11-14
/// 22:13:20 UTC. The exact value is irrelevant; it only needs to be constant so
/// replays are byte-identical.
const DETERMINISTIC_EPOCH_SECS: u64 = 1_700_000_000;

/// Builds and opens a [`DB`], tweaking optional settings before opening.
///
/// Start from [`DB::builder`], chain any setters, then call
/// [`DbBuilder::open`]. For the default configuration, [`DB::open`] is a
/// shorthand.
#[derive(Clone)]
pub struct DbBuilder {
    name: String,
    backend: Arc<dyn Backend>,
    cache_size: usize,
    deterministic_time: bool,
    retry: RetryConfig,
}

impl DbBuilder {
    fn new(name: impl Into<String>, backend: Arc<dyn Backend>) -> Self {
        DbBuilder {
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
    /// exponentially up to [`DbBuilder::retry_max_interval`].
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
    pub async fn open(self) -> Result<DB, Error> {
        let DbBuilder {
            name,
            backend: b,
            cache_size,
            deterministic_time,
            retry,
        } = self;

        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(Error::Other(format!(
                "name must be alphanumeric, got {name:?}"
            )));
        }
        check_or_create_db_meta(&b, &name).await?;

        let backend = Arc::new(StatsBackend::new(b));
        let dyn_backend: Arc<dyn Backend> = backend.clone();
        let local = Local::new(cache_size);
        let global = Global::new(dyn_backend, local.clone());
        let tl = TLogger::new(global.clone(), local.clone(), &name);
        let bg = Arc::new(Background::new());
        let clock = if deterministic_time {
            Clock::anchored_at(UNIX_EPOCH + Duration::from_secs(DETERMINISTIC_EPOCH_SECS))
        } else {
            Clock::real()
        };
        let tmon = Monitor::with_config(local.clone(), tl.clone(), bg.clone(), clock, retry);
        let locker = Locker::new(local.clone(), global.clone(), tmon.clone());
        let gc = Gc::new(bg.clone(), tl);
        gc.start();
        let algo = Algo::new(
            global.clone(),
            local.clone(),
            locker,
            tmon.clone(),
            gc,
            Some(bg.clone()),
        );

        let inner = Arc::new(DbInner {
            name,
            backend,
            local,
            global,
            background: bg,
            tmon,
            algo,
            stats: Mutex::new(Stats::default()),
        });
        Ok(DB { inner })
    }
}

pub(crate) struct DbInner {
    pub(crate) name: String,
    pub(crate) backend: Arc<StatsBackend>,
    pub(crate) local: Local,
    pub(crate) global: Global,
    background: Arc<Background>,
    pub(crate) tmon: Monitor,
    pub(crate) algo: Algo,
    stats: Mutex<Stats>,
}

/// An open GlassDB database instance.
#[derive(Clone)]
pub struct DB {
    inner: Arc<DbInner>,
}

impl DB {
    /// Starts building a database with the given name and backend. Chain setters
    /// on the returned [`DbBuilder`], then call [`DbBuilder::open`].
    pub fn builder(name: impl Into<String>, b: Arc<dyn Backend>) -> DbBuilder {
        DbBuilder::new(name, b)
    }

    /// Opens a database with the given name using default options. Shorthand for
    /// `DB::builder(name, b).open()`.
    pub async fn open(name: &str, b: Arc<dyn Backend>) -> Result<DB, Error> {
        DB::builder(name, b).open().await
    }

    /// Releases resources associated with the database.
    pub async fn close(&self) {
        self.inner.algo.close().await;
        self.inner.background.close().await;
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
    /// `f` receives the [`Tx`] handle by value and returns a future, so the
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
    /// `select!`, or `JoinHandle::abort`. On-storage locks held by an
    /// abandoned attempt are reclaimed by the lock-lease timeout.
    pub async fn tx<T, F, Fut>(&self, f: F) -> Result<T, Error>
    where
        F: FnMut(Tx) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        self.inner.tx(f).await
    }

    /// Retrieves ongoing performance stats. Only updated when transactions
    /// close. Counters only increase; use [`Stats::sub`] for intervals.
    pub fn stats(&self) -> Stats {
        let mut s = self.inner.stats.lock().unwrap();
        let bstats = self.inner.backend.stats_and_reset();
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
        F: FnMut(Tx) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
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
        F: FnMut(Tx) -> Fut + Send,
        Fut: Future<Output = Result<T, Error>> + Send,
        T: Send,
    {
        // External cancellation is by dropping this future; internally the
        // engine threads a fresh, uncancelled `CancelToken` solely so subroutines
        // that race against it (validation, lock waits) keep working unchanged.
        let ctx = CancelToken::new();
        let tx = Tx::new(
            ctx.clone(),
            self.global.clone(),
            self.local.clone(),
            self.tmon.clone(),
        );
        let mut handle = None;

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
            stats.tx_reads += access.reads.len() as i64;
            stats.tx_writes += access.writes.len() as i64;

            let value = match fn_res {
                Ok(v) => {
                    // Hand the full access (reads and writes) to the handle. The
                    // handle owns the data from here on; the wound path below
                    // recovers it from the handle, so no separate clone is kept.
                    match handle.as_mut() {
                        None => handle = Some(self.algo.begin(&ctx, access)),
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
                        None => handle = Some(self.algo.begin(&ctx, ro)),
                        Some(h) => self.algo.reset(h, ro),
                    }
                    let h = handle.as_mut().unwrap();
                    match self.algo.validate_reads(&ctx, h).await {
                        Err(e) if e.is_retry() => {
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
                self.algo.commit(&ctx, h).await
            };
            match commit_res {
                Ok(()) => break Ok(value),
                Err(e) if e.is_wounded() => {
                    // A higher-priority transaction aborted us. Release whatever
                    // we were holding and restart with a fresh ID that preserves
                    // our priority, so we are not starved on the retry.
                    if let Some(h) = handle.as_mut() {
                        let _ = self.algo.end(&ctx, h).await;
                    }
                    let old = handle.take().unwrap();
                    handle = Some(self.algo.rebegin(old));
                    tx.reset();
                    stats.tx_retries += 1;
                    continue;
                }
                Err(e) if e.is_retry() => {
                    tx.reset();
                    stats.tx_retries += 1;
                    continue;
                }
                Err(e) => break Err(e.into()),
            }
        };

        // Always finalize the handle (a committed handle is a no-op).
        if let Some(h) = handle.as_mut()
            && let Err(e) = self.algo.end(&ctx, h).await
            && result.is_ok()
        {
            return Err(e.into());
        }
        result
    }
}
