//! The database entry point. Ported from the Go `db.go`: opening a database,
//! the transaction retry loop, collections, and stats.

use std::sync::{Arc, Mutex};

use glassdb_backend::{Backend, StatsBackend};
use glassdb_concurr::{Background, Ctx};
use glassdb_data::paths;
use glassdb_storage::{Global, Local, TLogger};
use glassdb_trans::{Algo, Gc, Locker, Monitor};

use crate::collection::Collection;
use crate::error::Error;
use crate::stats::Stats;
use crate::tx::Tx;
use crate::version::check_or_create_db_meta;

/// Tweakable options for a [`DB`].
#[derive(Debug, Clone)]
pub struct Options {
    /// Number of bytes dedicated to caching objects and metadata. Setting this
    /// too small may impact performance, as more backend calls are necessary.
    pub cache_size: usize,
}

impl Default for Options {
    fn default() -> Self {
        // 512 MiB, a reasonable middle ground for production.
        Options {
            cache_size: 512 * 1024 * 1024,
        }
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
    /// Opens a database with the given name using default options.
    pub async fn open(ctx: &Ctx, name: &str, b: Arc<dyn Backend>) -> Result<DB, Error> {
        DB::open_with(ctx, name, b, Options::default()).await
    }

    /// Opens a database with the given name and custom options.
    pub async fn open_with(
        ctx: &Ctx,
        name: &str,
        b: Arc<dyn Backend>,
        opts: Options,
    ) -> Result<DB, Error> {
        if name.is_empty() || !name.chars().all(|c| c.is_ascii_alphanumeric()) {
            return Err(Error::Other(format!(
                "name must be alphanumeric, got {name:?}"
            )));
        }
        check_or_create_db_meta(ctx, &b, name).await?;

        let backend = Arc::new(StatsBackend::new(b));
        let dyn_backend: Arc<dyn Backend> = backend.clone();
        let local = Local::new(opts.cache_size);
        let global = Global::new(dyn_backend, local.clone());
        let tl = TLogger::new(global.clone(), local.clone(), name);
        let bg = Arc::new(Background::new());
        let tmon = Monitor::new(local.clone(), tl.clone(), bg.clone());
        let locker = Locker::new(local.clone(), global.clone(), tmon.clone());
        let gc = Gc::new(bg.clone(), tl);
        gc.start(ctx);
        let algo = Algo::new(
            global.clone(),
            local.clone(),
            locker,
            tmon.clone(),
            gc,
            Some(bg.clone()),
        );

        let inner = Arc::new(DbInner {
            name: name.to_string(),
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

    /// Releases resources associated with the database.
    pub async fn close(&self) {
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
    pub async fn tx<T, F>(&self, ctx: &Ctx, f: F) -> Result<T, Error>
    where
        F: AsyncFnMut(&mut Tx) -> Result<T, Error>,
    {
        self.inner.tx(ctx, f).await
    }

    /// Retrieves ongoing performance stats. Only updated when transactions
    /// close. Counters only increase; use [`Stats::sub`] for intervals.
    pub fn stats(&self) -> Stats {
        let mut s = self.inner.stats.lock().unwrap();
        let bstats = self.inner.backend.stats_and_reset();
        s.add_backend(&bstats);
        *s
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

    pub(crate) async fn tx<T, F>(&self, ctx: &Ctx, f: F) -> Result<T, Error>
    where
        F: AsyncFnMut(&mut Tx) -> Result<T, Error>,
    {
        let mut stats = Stats {
            tx_n: 1,
            ..Default::default()
        };
        let begin = std::time::Instant::now();
        let res = self.tx_impl(ctx, f, &mut stats).await;
        stats.tx_time = begin.elapsed();
        self.update_stats(&stats);
        res
    }

    async fn tx_impl<T, F>(&self, ctx: &Ctx, mut f: F, stats: &mut Stats) -> Result<T, Error>
    where
        F: AsyncFnMut(&mut Tx) -> Result<T, Error>,
    {
        let mut tx = Tx::new(
            ctx.clone(),
            self.global.clone(),
            self.local.clone(),
            self.tmon.clone(),
        );
        let mut handle = None;

        let result: Result<T, Error> = loop {
            if ctx.is_cancelled() {
                break Err(Error::Cancelled);
            }
            let fn_res = f(&mut tx).await;
            if tx.aborted() {
                break Err(Error::Aborted);
            }

            // Collect the accesses produced by the user function.
            let access = tx.collect_accesses();
            stats.tx_reads += access.reads.len() as i64;
            stats.tx_writes += access.writes.len() as i64;
            match handle.as_mut() {
                None => handle = Some(self.algo.begin(ctx, access.clone())),
                Some(h) => self.algo.reset(h, access.clone()),
            }

            let value = match fn_res {
                Ok(v) => v,
                Err(ferr) => {
                    // The user function returned an error. It might be the
                    // result of a spurious read, so validate only the reads.
                    let mut ro = access;
                    ro.writes.clear();
                    let h = handle.as_mut().unwrap();
                    self.algo.reset(h, ro);
                    match self.algo.validate_reads(ctx, h).await {
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
            let h = handle.as_mut().unwrap();
            match self.algo.commit(ctx, h).await {
                Ok(()) => break Ok(value),
                Err(e) if e.is_retry() => {
                    tx.reset();
                    stats.tx_retries += 1;
                    continue;
                }
                Err(e) => break Err(e.into()),
            }
        };

        // Always finalize the handle (a committed handle is a no-op).
        if let Some(h) = handle.as_mut() {
            if let Err(e) = self.algo.end(ctx, h).await {
                if result.is_ok() {
                    return Err(e.into());
                }
            }
        }
        result
    }
}
