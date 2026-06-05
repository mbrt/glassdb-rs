//! Distributed locking over global storage, built on the dedup state machine.
//! Ported from the Go `internal/trans/tlocker.go`.
//!
//! `Locker` exposes lock/unlock operations per key. Concurrent requests for the
//! same key are merged or queued through [`glassdb_concurr::Dedup`]; a per-key
//! worker drives the conditional writes, waits for blocking transactions to
//! finish, and retries on stale metadata.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_backend::{self as backend, BackendError};
use glassdb_concurr::{
    await_signal, shard::Sharded, Controller, Ctx, Dedup, DedupError, DedupWorker, MergeRequest,
};
use glassdb_data::{set_diff, set_union, TxId, TxIdSet};
use glassdb_storage::{
    compute_lock_update, tags_lock_info, Global, Local, LockInfo, LockRequest, LockType,
    Locker as StorageLocker, PathLock, StorageError, TValue, TxPathState,
};
use tokio::sync::Semaphore;

use crate::error::TransError;
use crate::monitor::Monitor;
use crate::reader::Reader;

/// Refresh metadata now and then, to avoid getting stuck retrying for something
/// that no longer makes sense.
const META_MAX_STALENESS: Duration = Duration::from_secs(10);
/// Periodically poll while waiting for transactions to finish.
const WAIT_POLL_DURATION: Duration = Duration::from_secs(5);

#[derive(Clone, Copy, PartialEq, Eq)]
enum TxState {
    Unknown,
    Existing,
}

#[derive(Default)]
struct Stats {
    n_calls: AtomicU64,
    n_hits: AtomicU64,
    n_retries: AtomicU64,
}

/// Counters for lock operations performed by a [`Locker`].
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LockStats {
    pub calls: usize,
    pub hits: usize,
    pub retries: usize,
}

/// A lock/unlock request that can merge with another for the same key.
#[derive(Clone)]
struct LockReq(LockRequest);

impl MergeRequest for LockReq {
    fn merge(&self, other: &Self) -> Option<Self> {
        merge_requests(self.clone(), other.clone())
    }
    fn can_reorder(&self) -> bool {
        self.0.typ == LockType::None
    }
}

fn merge_requests(mut r1: LockReq, mut r2: LockReq) -> Option<LockReq> {
    if r1.0.typ == LockType::None {
        // Keep r1 as the one dictating the lock type; unlocks stay in r2.
        std::mem::swap(&mut r1, &mut r2);
    }
    match (r1.0.typ, r2.0.typ) {
        (LockType::Create, _) | (_, LockType::Create) => return None,
        (_, LockType::None) => {}
        (LockType::Read, LockType::Read) => {}
        _ => return None,
    }
    let lockers = set_union(
        &TxIdSet::from_ids(r1.0.lockers.clone()),
        &TxIdSet::from_ids(r2.0.lockers.clone()),
    )
    .into_iter()
    .collect();
    let unlockers = set_union(
        &TxIdSet::from_ids(r1.0.unlockers.clone()),
        &TxIdSet::from_ids(r2.0.unlockers.clone()),
    )
    .into_iter()
    .collect();
    Some(LockReq(LockRequest {
        typ: r1.0.typ,
        lockers,
        unlockers,
    }))
}

#[derive(Default)]
struct LockOpResult {
    locked_for: Vec<TxId>,
    unlocked_for: Vec<TxId>,
    wait_for_tx: Vec<TxId>,
    wound_tx: Vec<TxId>,
}

struct LockData {
    info: LockInfo,
    version: backend::Version,
}

/// The shared, lock-independent pieces used by the dedup worker.
#[derive(Clone)]
struct LockerCore {
    local: Local,
    global: Global,
    tmon: Monitor,
}

impl LockerCore {
    fn reader(&self) -> Reader {
        Reader::new(self.local.clone(), self.global.clone(), self.tmon.clone())
    }

    async fn do_lock_op(
        &self,
        ctx: &Ctx,
        key: &str,
        req: &LockRequest,
    ) -> Result<LockOpResult, StorageError> {
        let ldata = self.fetch_lock_info(ctx, key).await?;
        let txs = self.fetch_lockers_state(ctx, key, &ldata.info).await?;
        let ops = compute_lock_update(ldata.info, req, &txs)?;
        if !ops.wound.is_empty() {
            // Lower-priority holders are in the way. Abort them and retry.
            return Ok(LockOpResult {
                wound_tx: ops.wound,
                ..Default::default()
            });
        }
        if !ops.wait_for.is_empty() {
            return Ok(LockOpResult {
                wait_for_tx: ops.wait_for,
                ..Default::default()
            });
        }
        if ops.has_update {
            let locker = StorageLocker::new(self.global.clone());
            locker
                .update_lock(ctx, key, &ldata.version, &ops.update)
                .await?;
        }
        Ok(LockOpResult {
            locked_for: ops.locked_for,
            unlocked_for: ops.unlocked_for,
            ..Default::default()
        })
    }

    async fn fetch_lock_info(&self, ctx: &Ctx, key: &str) -> Result<LockData, StorageError> {
        let meta = match self
            .reader()
            .get_metadata(ctx, key, META_MAX_STALENESS)
            .await
        {
            Ok(m) => m,
            Err(e) if e.is_not_found() => {
                return Ok(LockData {
                    info: LockInfo {
                        typ: LockType::None,
                        ..Default::default()
                    },
                    version: backend::Version::default(),
                });
            }
            Err(e) => return Err(e),
        };
        let info = tags_lock_info(&meta.tags)?;
        info.valid()?;
        Ok(LockData {
            info,
            version: meta.version,
        })
    }

    async fn fetch_lockers_state(
        &self,
        ctx: &Ctx,
        key: &str,
        info: &LockInfo,
    ) -> Result<Vec<TxPathState>, StorageError> {
        if info.typ == LockType::Create || info.typ == LockType::Write {
            let tx = info.locked_by[0].clone();
            let tv = self
                .tmon
                .committed_value(ctx, key, &tx)
                .await
                .map_err(|_| {
                    StorageError::Other(format!("getting committed value from tx {tx}"))
                })?;
            return Ok(vec![TxPathState {
                tx,
                status: tv.status,
                value: tv.value,
            }]);
        }

        // For read locks, the tx status is enough; avoid fetching values.
        let mut txs = Vec::new();
        for tx in &info.locked_by {
            let status = self
                .tmon
                .tx_status(ctx, tx)
                .await
                .map_err(|_| StorageError::Other(format!("getting tx status for {tx}")))?;
            txs.push(TxPathState {
                tx: tx.clone(),
                status,
                value: TValue::default(),
            });
        }
        Ok(txs)
    }
}

struct LockerWorker {
    core: LockerCore,
    stats: Arc<Stats>,
}

#[async_trait]
impl DedupWorker<LockReq, StorageError> for LockerWorker {
    async fn work(
        &self,
        ctx: &Ctx,
        key: &str,
        contr: &Controller<LockReq, StorageError>,
    ) -> Result<(), StorageError> {
        let mut counter: u64 = 0;
        let mut wait_sem: Option<Arc<Semaphore>> = None;

        let result = loop {
            counter += 1;
            let req = contr.request(key).0;

            let lock_res = match self.core.do_lock_op(ctx, key, &req).await {
                Ok(r) => r,
                Err(e) => {
                    if e.is_precondition() && req.typ != LockType::Create {
                        // The lock info was outdated; force a reload and retry.
                        let _ = self.core.global.get_metadata(ctx, key).await;
                        continue;
                    }
                    // For create, there's nothing more we can do.
                    break Err(e);
                }
            };
            if !lock_res.wound_tx.is_empty() {
                if let Err(e) = self.wound_txs(ctx, &lock_res.wound_tx).await {
                    break Err(e);
                }
                // Retry now that the lower-priority holders are aborted.
                continue;
            }
            if !lock_res.wait_for_tx.is_empty() {
                if let Err(e) = self
                    .wait_for_tx(ctx, key, &lock_res.wait_for_tx, &mut wait_sem, contr)
                    .await
                {
                    break Err(e);
                }
                continue;
            }
            if !is_complete(&req, &lock_res) {
                continue;
            }
            break Ok(());
        };

        if counter > 1 {
            self.stats
                .n_retries
                .fetch_add(counter - 1, Ordering::Relaxed);
        }
        result
    }
}

impl LockerWorker {
    /// Aborts the given lower-priority holders so the requester can take over
    /// their locks under the wound-wait rule.
    async fn wound_txs(&self, ctx: &Ctx, txs: &[TxId]) -> Result<(), StorageError> {
        for tx in txs {
            self.core
                .tmon
                .wound_tx(ctx, tx)
                .await
                .map_err(trans_to_storage)?;
        }
        Ok(())
    }

    async fn wait_for_tx(
        &self,
        ctx: &Ctx,
        key: &str,
        txs: &[TxId],
        wait_sem: &mut Option<Arc<Semaphore>>,
        contr: &Controller<LockReq, StorageError>,
    ) -> Result<(), StorageError> {
        for tx in txs {
            let status = self
                .core
                .tmon
                .tx_status(ctx, tx)
                .await
                .map_err(trans_to_storage)?;
            if status.is_final() {
                continue;
            }
            if wait_sem.is_none() {
                *wait_sem = Some(contr.on_next_do(key));
            }
            let sem = wait_sem.as_ref().unwrap().clone();
            let wait_rx = self.core.tmon.wait_for_tx(ctx, tx);

            tokio::select! {
                biased;
                _ = ctx.cancelled() => return Err(BackendError::Cancelled.into()),
                _ = wait_rx => {}
                _ = await_signal(&sem) => {}
                _ = tokio::time::sleep(WAIT_POLL_DURATION) => {}
            }
            break;
        }
        Ok(())
    }
}

fn trans_to_storage(e: TransError) -> StorageError {
    match e {
        TransError::Storage(s) => s,
        TransError::Cancelled => StorageError::Backend(BackendError::Cancelled),
        other => StorageError::Other(other.to_string()),
    }
}

fn is_complete(req: &LockRequest, res: &LockOpResult) -> bool {
    let to_lock = TxIdSet::from_ids(req.lockers.clone());
    let to_unlock = TxIdSet::from_ids(req.unlockers.clone());
    let locked = TxIdSet::from_ids(res.locked_for.clone());
    let unlocked = TxIdSet::from_ids(res.unlocked_for.clone());
    set_diff(&to_lock, &locked).is_empty() && set_diff(&to_unlock, &unlocked).is_empty()
}

/// Acquires and releases distributed locks on storage objects, hiding waits and
/// retries from callers.
#[derive(Clone)]
pub struct Locker {
    inner: Arc<LockerState>,
}

/// One independent partition of the per-transaction locks, keyed by tid bytes.
type LockerShard = Mutex<HashMap<Vec<u8>, HashMap<String, LockType>>>;

struct LockerState {
    tmon: Monitor,
    tlocks: Sharded<LockerShard>,
    stats: Arc<Stats>,
    dedup: Dedup<LockReq, StorageError, LockerWorker>,
}

impl Locker {
    /// Creates a locker over local/global storage and the transaction monitor.
    pub fn new(local: Local, global: Global, tmon: Monitor) -> Self {
        let core = LockerCore {
            local,
            global,
            tmon: tmon.clone(),
        };
        let stats = Arc::new(Stats::default());
        let worker = LockerWorker {
            core,
            stats: stats.clone(),
        };
        let dedup = Dedup::new(worker);
        Locker {
            inner: Arc::new(LockerState {
                tmon,
                tlocks: Sharded::new(|_| Mutex::new(HashMap::new())),
                stats,
                dedup,
            }),
        }
    }

    /// Acquires a read lock on `key` for the transaction.
    pub async fn lock_read(&self, ctx: &Ctx, key: &str, tid: &TxId) -> Result<(), TransError> {
        self.push_request(ctx, key, LockType::Read, tid).await
    }

    /// Acquires a write lock on `key` for the transaction.
    pub async fn lock_write(&self, ctx: &Ctx, key: &str, tid: &TxId) -> Result<(), TransError> {
        self.push_request(ctx, key, LockType::Write, tid).await
    }

    /// Acquires a create lock on `key` (first-time creation) for the transaction.
    pub async fn lock_create(&self, ctx: &Ctx, key: &str, tid: &TxId) -> Result<(), TransError> {
        self.push_request(ctx, key, LockType::Create, tid).await
    }

    /// Releases the lock held by the transaction on `key`.
    pub async fn unlock(&self, ctx: &Ctx, key: &str, tid: &TxId) -> Result<(), TransError> {
        self.push_request(ctx, key, LockType::None, tid).await
    }

    /// Returns the lock type currently held by `tid` on `key`.
    pub fn lock_type(&self, key: &str, tid: &TxId) -> LockType {
        let tlocks = self.inner.tlocks.for_key(tid.as_bytes()).lock().unwrap();
        tlocks
            .get(tid.as_bytes())
            .and_then(|m| m.get(key))
            .copied()
            .unwrap_or(LockType::None)
    }

    /// Returns all paths currently locked by `tid`.
    pub fn locked_paths(&self, tid: &TxId) -> Vec<PathLock> {
        let tlocks = self.inner.tlocks.for_key(tid.as_bytes()).lock().unwrap();
        match tlocks.get(tid.as_bytes()) {
            None => Vec::new(),
            Some(m) => {
                let mut out: Vec<PathLock> = m
                    .iter()
                    .map(|(p, t)| PathLock {
                        path: p.clone(),
                        typ: *t,
                    })
                    .collect();
                // Stable order so the transaction log's lock list is
                // independent of `HashMap` iteration (needed for byte-for-byte
                // deterministic replays; harmless in production).
                out.sort_by(|a, b| a.path.cmp(&b.path));
                out
            }
        }
    }

    /// Returns and resets the accumulated lock statistics.
    pub fn stats_and_reset(&self) -> LockStats {
        LockStats {
            calls: self.inner.stats.n_calls.swap(0, Ordering::Relaxed) as usize,
            hits: self.inner.stats.n_hits.swap(0, Ordering::Relaxed) as usize,
            retries: self.inner.stats.n_retries.swap(0, Ordering::Relaxed) as usize,
        }
    }

    async fn push_request(
        &self,
        ctx: &Ctx,
        key: &str,
        lt: LockType,
        tid: &TxId,
    ) -> Result<(), TransError> {
        self.inner.stats.n_calls.fetch_add(1, Ordering::Relaxed);
        let (txs, nproc) = self.needs_processing(key, tid, lt);
        if !nproc {
            self.inner.stats.n_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if txs == TxState::Unknown {
            // We'll need refresh logs to keep the locks alive from now on.
            self.inner.tmon.start_refresh_tx(ctx, tid);
        }

        let (lockers, unlockers) = if lt == LockType::None {
            (Vec::new(), vec![tid.clone()])
        } else {
            (vec![tid.clone()], Vec::new())
        };
        let req = LockReq(LockRequest {
            typ: lt,
            lockers,
            unlockers,
        });

        let (err, lock_updated, final_lt): (Result<(), TransError>, bool, LockType) =
            match self.inner.dedup.run(ctx, key, req).await {
                Ok(()) => (Ok(()), true, lt),
                Err(DedupError::Work(e)) => {
                    if e.is_precondition() {
                        (Err(TransError::Storage((*e).clone())), false, lt)
                    } else {
                        // On any other error (incl. timeout) we don't know the
                        // outcome. Be conservative and mark the lock unknown.
                        (
                            Err(TransError::Storage((*e).clone())),
                            true,
                            LockType::Unknown,
                        )
                    }
                }
                Err(DedupError::Cancelled) => (Err(TransError::Cancelled), true, LockType::Unknown),
            };

        if lock_updated {
            self.update_tx_locks(key, tid, final_lt);
        }
        err
    }

    fn needs_processing(&self, key: &str, tid: &TxId, lt: LockType) -> (TxState, bool) {
        let tlocks = self.inner.tlocks.for_key(tid.as_bytes()).lock().unwrap();
        let txl = tlocks.get(tid.as_bytes());
        let st = if txl.is_some() {
            TxState::Existing
        } else {
            TxState::Unknown
        };
        if txl.is_none() && lt == LockType::None {
            // We don't know the transaction, so there's nothing to unlock.
            return (st, false);
        }
        let glt = txl.and_then(|m| m.get(key)).copied();
        if glt.is_none() && lt == LockType::None {
            return (st, false);
        }
        (st, glt != Some(lt))
    }

    fn update_tx_locks(&self, key: &str, tid: &TxId, lt: LockType) {
        let mut tlocks = self.inner.tlocks.for_key(tid.as_bytes()).lock().unwrap();
        if lt == LockType::None {
            if let Some(m) = tlocks.get_mut(tid.as_bytes()) {
                m.remove(key);
                if m.is_empty() {
                    tlocks.remove(tid.as_bytes());
                }
            }
            return;
        }
        tlocks
            .entry(tid.as_bytes().to_vec())
            .or_default()
            .insert(key.to_string(), lt);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_backend::{memory::MemoryBackend, Backend, Tags};
    use glassdb_concurr::Background;
    use glassdb_data::paths;
    use glassdb_storage::{TLogger, TxCommitStatus, TxLog, TxWrite};
    use std::sync::Arc;

    struct TlCtx {
        global: Global,
        backend: Arc<dyn Backend>,
        monitor: Monitor,
    }

    fn new_test_locker(b: Arc<dyn Backend>) -> (Locker, TlCtx) {
        let local = Local::new(1024);
        let global = Global::new(b.clone(), local.clone());
        let tl = TLogger::new(global.clone(), local.clone(), "test");
        let bg = Arc::new(Background::new());
        let mon = Monitor::new(local.clone(), tl, bg);
        let locker = Locker::new(local, global.clone(), mon.clone());
        (
            locker,
            TlCtx {
                global,
                backend: b,
                monitor: mon,
            },
        )
    }

    fn init_tl_test() -> (Locker, TlCtx) {
        new_test_locker(Arc::new(MemoryBackend::new()))
    }

    // Builds a deterministic, valid transaction ID. A smaller `order` yields an
    // older (higher-priority) transaction under the wound-wait rule; the name
    // only affects the prefix, never the priority.
    fn mk_tid(order: u64, name: &str) -> TxId {
        TxId::with_priority(order * 1_000_000_000, name.as_bytes())
    }

    async fn assert_lock_info(g: &Global, key: &str, typ: LockType, mut locked_by: Vec<TxId>) {
        let ctx = Ctx::background();
        let meta = g.get_metadata(&ctx, key).await.unwrap();
        let mut info = tags_lock_info(&meta.tags).unwrap();
        info.locked_by.sort_by_key(|t| t.to_string());
        locked_by.sort_by_key(|t| t.to_string());
        assert_eq!(info.typ, typ, "lock type mismatch for {key}");
        assert_eq!(info.locked_by, locked_by, "lockers mismatch for {key}");
    }

    #[tokio::test]
    async fn lock_create() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx = TxId::new_random();
        tctx.monitor.begin_tx(&tx);

        // Lock + unlock without commit, repeated.
        for _ in 0..3 {
            locker.lock_create(&ctx, &key, &tx).await.unwrap();
            assert_lock_info(&tctx.global, &key, LockType::Create, vec![tx.clone()]).await;

            locker.unlock(&ctx, &key, &tx).await.unwrap();
            let err = tctx.global.get_metadata(&ctx, &key).await.unwrap_err();
            assert!(err.is_not_found());
        }

        // Lock, commit, unlock writes the value.
        locker.lock_create(&ctx, &key, &tx).await.unwrap();
        let value = b"val".to_vec();
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            path: key.clone(),
            value: value.clone(),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        tctx.monitor.commit_tx(&ctx, tl).await.unwrap();
        locker.unlock(&ctx, &key, &tx).await.unwrap();

        let gr = tctx.global.read(&ctx, &key).await.unwrap();
        assert_eq!(gr.value, value);
    }

    #[tokio::test]
    async fn lock_create_fail() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx = TxId::new_random();
        tctx.monitor.begin_tx(&tx);

        tctx.global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        let err = locker.lock_create(&ctx, &key, &tx).await.unwrap_err();
        assert!(err.is_precondition(), "expected precondition, got {err:?}");
    }

    #[tokio::test]
    async fn unlock_after_create_timeout() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx = TxId::new_random();
        tctx.monitor.begin_tx(&tx);

        let (cctx, cancel) = Ctx::with_cancel();
        cancel.cancel();
        let err = locker.lock_create(&cctx, &key, &tx).await.unwrap_err();
        assert!(err.is_cancelled(), "expected cancelled, got {err:?}");

        let err = tctx.global.get_metadata(&ctx, &key).await.unwrap_err();
        assert!(err.is_not_found());

        locker.unlock(&ctx, &key, &tx).await.unwrap();
    }

    #[tokio::test]
    async fn lock_read_write() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx = TxId::new_random();
        tctx.monitor.begin_tx(&tx);

        tctx.global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&ctx, &key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![tx.clone()]).await;

        locker.unlock(&ctx, &key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::None, vec![]).await;

        locker.lock_write(&ctx, &key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![tx.clone()]).await;

        locker.unlock(&ctx, &key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::None, vec![]).await;
    }

    #[tokio::test]
    async fn lock_multiple_r() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx1 = TxId::new_random();
        let tx2 = TxId::new_random();
        tctx.monitor.begin_tx(&tx1);
        tctx.monitor.begin_tx(&tx2);

        tctx.global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&ctx, &key, &tx1).await.unwrap();
        locker.lock_read(&ctx, &key, &tx2).await.unwrap();
        assert_lock_info(
            &tctx.global,
            &key,
            LockType::Read,
            vec![tx1.clone(), tx2.clone()],
        )
        .await;

        // Lock again with the same tx is a no-op.
        locker.lock_read(&ctx, &key, &tx1).await.unwrap();
        assert_lock_info(
            &tctx.global,
            &key,
            LockType::Read,
            vec![tx1.clone(), tx2.clone()],
        )
        .await;

        locker.unlock(&ctx, &key, &tx1).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![tx2.clone()]).await;

        locker.unlock(&ctx, &key, &tx2).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::None, vec![]).await;
    }

    #[tokio::test]
    async fn lock_read_after_delete() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");

        tctx.global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        let txw = TxId::from_bytes(b"txw".to_vec());
        tctx.monitor.begin_tx(&txw);
        locker.lock_write(&ctx, &key, &txw).await.unwrap();
        let mut tl = TxLog::new(txw.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            path: key.clone(),
            value: Vec::new(),
            deleted: true,
            prev_writer: TxId::default(),
        }];
        tctx.monitor.commit_tx(&ctx, tl).await.unwrap();

        let txr = TxId::from_bytes(b"txr".to_vec());
        tctx.monitor.begin_tx(&txr);
        let err = locker.lock_read(&ctx, &key, &txr).await.unwrap_err();
        assert!(err.is_not_found(), "expected not-found, got {err:?}");
    }

    #[tokio::test]
    async fn lock_upgrade() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx = TxId::new_random();
        tctx.monitor.begin_tx(&tx);

        tctx.global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&ctx, &key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![tx.clone()]).await;

        locker.lock_write(&ctx, &key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![tx.clone()]).await;
    }

    #[tokio::test]
    async fn wait_for_tx() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let locker = Arc::new(locker);
        let key = paths::from_key("example", b"key");
        let txr = TxId::from_bytes(b"txr".to_vec());
        let txw = TxId::from_bytes(b"txw".to_vec());
        tctx.monitor.begin_tx(&txr);
        tctx.monitor.begin_tx(&txw);

        tctx.global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&ctx, &key, &txr).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![txr.clone()]).await;

        // Unlock the read just after the write starts waiting.
        let unlock_task = {
            let l = locker.clone();
            let k = key.clone();
            let t = txr.clone();
            let c = ctx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                l.unlock(&c, &k, &t).await.unwrap();
            })
        };
        locker.lock_write(&ctx, &key, &txw).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![txw.clone()]).await;

        // Abort the write tx after a new read starts waiting.
        let abort_task = {
            let mon = tctx.monitor.clone();
            let t = txw.clone();
            let c = ctx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                mon.abort_tx(&c, &t).await.unwrap();
            })
        };
        // Make sure the unlock and abort have finished before relocking with the
        // same tx id (concurrent lock/unlock of the same key+tx is unsafe).
        unlock_task.await.unwrap();
        abort_task.await.unwrap();

        locker.lock_read(&ctx, &key, &txr).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![txr.clone()]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn queue_up() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let locker = Arc::new(locker);
        let key = paths::from_key("example", b"key");
        let txw = TxId::from_bytes(b"txw".to_vec());
        tctx.monitor.begin_tx(&txw);

        tctx.global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_write(&ctx, &key, &txw).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![txw.clone()]).await;

        // Three transactions try to read-lock in parallel.
        let mut handles = Vec::new();
        let mut txrs = Vec::new();
        for i in 0..3 {
            let tx = TxId::from_bytes(format!("txr{i}").into_bytes());
            txrs.push(tx.clone());
            let l = locker.clone();
            let k = key.clone();
            let mon = tctx.monitor.clone();
            let c = ctx.clone();
            handles.push(tokio::spawn(async move {
                mon.begin_tx(&tx);
                l.lock_read(&c, &k, &tx).await
            }));
        }

        // A write lock joins the queue a little later.
        let txw0 = TxId::from_bytes(b"txw0".to_vec());
        let writer = {
            let l = locker.clone();
            let k = key.clone();
            let mon = tctx.monitor.clone();
            let c = ctx.clone();
            let t = txw0.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                mon.begin_tx(&t);
                l.lock_write(&c, &k, &t).await
            })
        };

        // Unlock the original write once the reads are waiting.
        tokio::time::sleep(Duration::from_millis(50)).await;
        locker.unlock(&ctx, &key, &txw).await.unwrap();

        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_lock_info(&tctx.global, &key, LockType::Read, txrs.clone()).await;

        // Abort all readers; the queued writer should then acquire the lock.
        for tx in &txrs {
            tctx.monitor.abort_tx(&ctx, tx).await.unwrap();
        }
        writer.await.unwrap().unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![txw0.clone()]).await;
    }

    #[tokio::test]
    async fn lock_upgrade_wait() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let locker = Arc::new(locker);
        let key = paths::from_key("example", b"key");
        // Equal priority so the write upgrade waits for the other reader to
        // release instead of wounding it.
        let tx = mk_tid(1, "tx");
        let txr = mk_tid(1, "txr");
        tctx.monitor.begin_tx(&tx);
        tctx.monitor.begin_tx(&txr);

        tctx.global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&ctx, &key, &tx).await.unwrap();
        locker.lock_read(&ctx, &key, &txr).await.unwrap();
        assert_lock_info(
            &tctx.global,
            &key,
            LockType::Read,
            vec![tx.clone(), txr.clone()],
        )
        .await;

        {
            let l = locker.clone();
            let k = key.clone();
            let t = txr.clone();
            let c = ctx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                l.unlock(&c, &k, &t).await.unwrap();
            });
        }
        locker.lock_write(&ctx, &key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![tx.clone()]).await;
    }

    #[tokio::test]
    async fn lock_read_remote() {
        let ctx = Ctx::background();
        let (locker1, tctx1) = init_tl_test();
        let (locker2, tctx2) = new_test_locker(tctx1.backend.clone());

        let key = paths::from_key("example", b"key");
        tctx1
            .global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        // Make locker1 see stale metadata by caching it first.
        tctx1.global.get_metadata(&ctx, &key).await.unwrap();

        let tx2 = TxId::new_random();
        tctx2.monitor.begin_tx(&tx2);
        locker2.lock_read(&ctx, &key, &tx2).await.unwrap();
        assert_lock_info(&tctx2.global, &key, LockType::Read, vec![tx2.clone()]).await;

        let tx1 = TxId::new_random();
        tctx1.monitor.begin_tx(&tx1);
        locker1.lock_read(&ctx, &key, &tx1).await.unwrap();
        assert_lock_info(
            &tctx1.global,
            &key,
            LockType::Read,
            vec![tx1.clone(), tx2.clone()],
        )
        .await;

        // There must have been a retry due to stale metadata.
        let stats = locker1.stats_and_reset();
        assert_eq!(stats.retries, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn wait_remote() {
        let ctx = Ctx::background();
        let (locker1, tctx1) = init_tl_test();
        let (locker2, tctx2) = new_test_locker(tctx1.backend.clone());
        let locker2 = Arc::new(locker2);

        let key = paths::from_key("example", b"key");
        tctx1
            .global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        // Equal priority so the second writer waits for the holder to release
        // instead of wounding it.
        let tx2 = mk_tid(1, "tx2");
        tctx2.monitor.begin_tx(&tx2);
        locker2.lock_write(&ctx, &key, &tx2).await.unwrap();
        assert_lock_info(&tctx2.global, &key, LockType::Write, vec![tx2.clone()]).await;

        {
            let l = locker2.clone();
            let k = key.clone();
            let t = tx2.clone();
            let c = ctx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                l.unlock(&c, &k, &t).await.unwrap();
            });
        }

        let tx1 = mk_tid(1, "tx1");
        tctx1.monitor.begin_tx(&tx1);
        locker1.lock_write(&ctx, &key, &tx1).await.unwrap();
        assert_lock_info(&tctx1.global, &key, LockType::Write, vec![tx1.clone()]).await;

        // Commit tx2 after locker1 holds the write lock.
        {
            let mon = tctx2.monitor.clone();
            let k = key.clone();
            let t = tx2.clone();
            let c = ctx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut tl = TxLog::new(t.clone(), TxCommitStatus::Ok);
                tl.writes = vec![TxWrite {
                    path: k,
                    value: b"foo".to_vec(),
                    deleted: false,
                    prev_writer: TxId::default(),
                }];
                let _ = mon.commit_tx(&c, tl).await;
            });
        }

        locker1.lock_write(&ctx, &key, &tx1).await.unwrap();
        assert_lock_info(&tctx1.global, &key, LockType::Write, vec![tx1.clone()]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn wound_younger_holder() {
        let ctx = Ctx::background();
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");

        tctx.global
            .write(&ctx, &key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        // A younger transaction holds the write lock.
        let tx_young = mk_tid(2, "young");
        tctx.monitor.begin_tx(&tx_young);
        locker.lock_write(&ctx, &key, &tx_young).await.unwrap();

        // An older (higher-priority) transaction wants the same lock. Under the
        // wound-wait rule it aborts the younger holder and takes the lock
        // without waiting.
        let tx_old = mk_tid(1, "old");
        tctx.monitor.begin_tx(&tx_old);
        locker.lock_write(&ctx, &key, &tx_old).await.unwrap();

        assert_lock_info(&tctx.global, &key, LockType::Write, vec![tx_old.clone()]).await;

        // The younger transaction was wounded (aborted).
        let status = tctx.monitor.tx_status(&ctx, &tx_young).await.unwrap();
        assert_eq!(status, TxCommitStatus::Aborted);
    }
}
