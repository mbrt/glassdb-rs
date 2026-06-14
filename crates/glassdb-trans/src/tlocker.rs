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
use glassdb_backend::{self as backend};
use glassdb_concurr::{
    BatchHandle, Dedup, DedupError, DedupKeySnapshot, MergeRequest, Worker, rt, shard::Sharded,
};
use glassdb_data::{TxId, TxIdSet, set_diff, set_union};
use glassdb_storage::{
    Global, Local, LockInfo, LockRequest, LockType, LockUpdate, Locker as StorageLocker, PathLock,
    StorageError, TValue, TxPathState, compute_lock_update, tags_lock_info,
};

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

/// Diagnostic snapshot of one transaction's locally-tracked held locks.
///
/// Returned by [`Locker::tx_locks_snapshot`] for operators investigating hangs
/// or partial-lock deadlocks. The locks list is sorted by path for stable
/// display.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxLockSnapshot {
    pub tx_id: TxId,
    pub locks: Vec<PathLock>,
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

    async fn do_lock_op(&self, key: &str, req: &LockRequest) -> Result<LockOpResult, StorageError> {
        // A pure create request is self-checking: `write_if_not_exists` fails
        // with a precondition error if the object already exists, so the
        // preliminary metadata read (and lockers-state fetch) that the general
        // path performs is redundant. `compute_lock_update` ignores the current
        // lock state for a create anyway (it never waits or wounds), so go
        // straight to the conditional create and save a metadata round-trip per
        // created key. On a precondition error the caller falls back to a write
        // lock, exactly as it would have with the metadata read.
        if req.typ == LockType::Create && req.unlockers.is_empty() {
            let locker = StorageLocker::new(self.global.clone());
            let update = LockUpdate {
                typ: LockType::Create,
                lockers: req.lockers.clone(),
                ..Default::default()
            };
            locker
                .update_lock(key, &backend::Version::default(), &update)
                .await?;
            return Ok(LockOpResult {
                locked_for: req.lockers.clone(),
                ..Default::default()
            });
        }

        let ldata = self.fetch_lock_info(key).await?;
        let txs = self.fetch_lockers_state(key, &ldata.info).await?;
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
        if let Some(update) = ops.update {
            let locker = StorageLocker::new(self.global.clone());
            locker.update_lock(key, &ldata.version, &update).await?;
        }
        Ok(LockOpResult {
            locked_for: ops.locked_for,
            unlocked_for: ops.unlocked_for,
            ..Default::default()
        })
    }

    async fn fetch_lock_info(&self, key: &str) -> Result<LockData, StorageError> {
        let meta = match self.reader().get_metadata(key, META_MAX_STALENESS).await {
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
            version: meta.version.clone(),
        })
    }

    async fn fetch_lockers_state(
        &self,
        key: &str,
        info: &LockInfo,
    ) -> Result<Vec<TxPathState>, StorageError> {
        if info.typ == LockType::Create || info.typ == LockType::Write {
            let tx = info.locked_by[0].clone();
            let tv = self.tmon.committed_value(key, &tx).await.map_err(|_| {
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
                .tx_status(tx)
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
impl Worker<LockReq, StorageError> for LockerWorker {
    async fn run(
        &self,
        key: &str,
        batch: &BatchHandle<LockReq, StorageError>,
    ) -> Result<(), StorageError> {
        let mut counter: u64 = 0;

        let result = loop {
            counter += 1;
            let req = batch.merged().0;

            let lock_res = match self.core.do_lock_op(key, &req).await {
                Ok(r) => r,
                Err(e) => {
                    // Two recoverable cases reload metadata and try again:
                    //   - a precondition on a non-create means our cached lock
                    //     info was stale (someone else changed it);
                    //   - an in-doubt outcome means the conditional lock write
                    //     may or may not have landed. Acquiring a lock is
                    //     pre-commit and idempotent: a fresh read reveals
                    //     whether it took, and re-applying is harmless. So we
                    //     retry the lock operation itself rather than
                    //     surfacing the uncertainty — only the commit point
                    //     must surface in-doubt (ADR-009), where a blind retry
                    //     could double-apply a durable write.
                    let stale = e.is_precondition() && req.typ != LockType::Create;
                    if stale || e.is_unavailable() {
                        let _ = self.core.global.get_metadata(key).await;
                        continue;
                    }
                    // For create, there's nothing more we can do.
                    break Err(e);
                }
            };
            if !lock_res.wound_tx.is_empty() {
                if let Err(e) = self.wound_txs(&lock_res.wound_tx).await {
                    break Err(e);
                }
                // Retry now that the lower-priority holders are aborted.
                continue;
            }
            if !lock_res.wait_for_tx.is_empty() {
                if let Err(e) = self.wait_for_tx(&lock_res.wait_for_tx, batch).await {
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
    async fn wound_txs(&self, txs: &[TxId]) -> Result<(), StorageError> {
        for tx in txs {
            self.core
                .tmon
                .wound_tx(tx)
                .await
                .map_err(trans_to_storage)?;
        }
        Ok(())
    }

    async fn wait_for_tx(
        &self,
        txs: &[TxId],
        batch: &BatchHandle<LockReq, StorageError>,
    ) -> Result<(), StorageError> {
        for tx in txs {
            let status = self
                .core
                .tmon
                .tx_status(tx)
                .await
                .map_err(trans_to_storage)?;
            if status.is_final() {
                continue;
            }
            let wait_fut = self.core.tmon.wait_for_tx(tx);

            tokio::select! {
                biased;
                _ = wait_fut => {}
                _ = batch.changed() => {}
                _ = rt::sleep(WAIT_POLL_DURATION) => {}
            }
            break;
        }
        Ok(())
    }
}

/// RAII guard that marks a lock as `Unknown` in the locker's per-tx state if
/// the `push_request` future is dropped before `dedup.run` returns. The driver
/// hand-off in `Dedup` may still complete the storage update in a spawned
/// owner; recording the lock as unknown forces a subsequent serial validate to
/// `unlock_all` before re-acquiring, keeping `tlocks` and storage consistent
/// without threading a cancellation token through every locker call.
struct PushGuard {
    locker: Arc<LockerState>,
    key: String,
    tid: TxId,
    armed: bool,
}

impl Drop for PushGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut tlocks = self
            .locker
            .tlocks
            .for_key(self.tid.as_bytes())
            .lock()
            .unwrap();
        tlocks
            .entry(self.tid.clone())
            .or_default()
            .insert(self.key.clone(), LockType::Unknown);
    }
}

fn trans_to_storage(e: TransError) -> StorageError {
    match e {
        TransError::Storage(s) => s,
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

/// One independent partition of the per-transaction locks.
type LockerShard = Mutex<HashMap<TxId, HashMap<String, LockType>>>;

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
    pub async fn lock_read(&self, key: &str, tid: &TxId) -> Result<(), TransError> {
        self.push_request(key, LockType::Read, tid).await
    }

    /// Acquires a write lock on `key` for the transaction.
    pub async fn lock_write(&self, key: &str, tid: &TxId) -> Result<(), TransError> {
        self.push_request(key, LockType::Write, tid).await
    }

    /// Acquires a create lock on `key` (first-time creation) for the transaction.
    pub async fn lock_create(&self, key: &str, tid: &TxId) -> Result<(), TransError> {
        self.push_request(key, LockType::Create, tid).await
    }

    /// Releases the lock held by the transaction on `key`.
    pub async fn unlock(&self, key: &str, tid: &TxId) -> Result<(), TransError> {
        self.push_request(key, LockType::None, tid).await
    }

    /// Returns the lock type currently held by `tid` on `key`.
    pub fn lock_type(&self, key: &str, tid: &TxId) -> LockType {
        let tlocks = self.inner.tlocks.for_key(tid.as_bytes()).lock().unwrap();
        tlocks
            .get(tid)
            .and_then(|m| m.get(key))
            .copied()
            .unwrap_or(LockType::None)
    }

    /// Reports whether `tid` currently holds any lock.
    pub fn has_locks(&self, tid: &TxId) -> bool {
        let tlocks = self.inner.tlocks.for_key(tid.as_bytes()).lock().unwrap();
        tlocks.get(tid).is_some_and(|m| !m.is_empty())
    }

    /// Returns all paths currently locked by `tid`.
    pub fn locked_paths(&self, tid: &TxId) -> Vec<PathLock> {
        let tlocks = self.inner.tlocks.for_key(tid.as_bytes()).lock().unwrap();
        match tlocks.get(tid) {
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

    /// Cancels in-flight lock work and awaits any spawned dedup owner tasks, so
    /// none leak when the database shuts down.
    pub async fn close(&self) {
        self.inner.dedup.close().await;
    }

    /// Returns and resets the accumulated lock statistics.
    pub fn stats_and_reset(&self) -> LockStats {
        LockStats {
            calls: self.inner.stats.n_calls.swap(0, Ordering::Relaxed) as usize,
            hits: self.inner.stats.n_hits.swap(0, Ordering::Relaxed) as usize,
            retries: self.inner.stats.n_retries.swap(0, Ordering::Relaxed) as usize,
        }
    }

    /// Returns a snapshot of the per-key dedup state used to coordinate lock
    /// requests. Pull-only and zero cost unless called.
    pub fn dedup_snapshot(&self) -> Vec<DedupKeySnapshot> {
        self.inner.dedup.snapshot()
    }

    /// Returns one entry per transaction that currently holds any local-cache
    /// lock, with the held paths sorted by path. Pull-only and zero cost unless
    /// called. Output is sorted by transaction id for stable display.
    pub fn tx_locks_snapshot(&self) -> Vec<TxLockSnapshot> {
        let mut out = Vec::new();
        self.inner.tlocks.each(|shard| {
            let m = shard.lock().unwrap();
            for (tx_id, locks) in m.iter() {
                if locks.is_empty() {
                    continue;
                }
                let mut paths: Vec<PathLock> = locks
                    .iter()
                    .map(|(p, t)| PathLock {
                        path: p.clone(),
                        typ: *t,
                    })
                    .collect();
                paths.sort_by(|a, b| a.path.cmp(&b.path));
                out.push(TxLockSnapshot {
                    tx_id: tx_id.clone(),
                    locks: paths,
                });
            }
        });
        out.sort_by(|a, b| a.tx_id.cmp(&b.tx_id));
        out
    }

    async fn push_request(&self, key: &str, lt: LockType, tid: &TxId) -> Result<(), TransError> {
        self.inner.stats.n_calls.fetch_add(1, Ordering::Relaxed);
        let (txs, nproc) = self.needs_processing(key, tid, lt);
        if !nproc {
            self.inner.stats.n_hits.fetch_add(1, Ordering::Relaxed);
            return Ok(());
        }
        if txs == TxState::Unknown {
            // We'll need refresh logs to keep the locks alive from now on.
            self.inner.tmon.start_refresh_tx(tid);
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

        // If our future is dropped mid-`dedup.run` (e.g. the caller hit a
        // deadlock watchdog and dropped the validate work), we don't know
        // whether storage was actually mutated. Be conservative and mark the
        // lock `Unknown` in our per-tx state so a subsequent serial validate
        // observes the discrepancy and unlocks before reacquiring. On normal
        // completion the guard is disarmed and the real outcome is recorded
        // below.
        let mut guard = PushGuard {
            locker: Arc::clone(&self.inner),
            key: key.to_string(),
            tid: tid.clone(),
            armed: true,
        };

        let res = self.inner.dedup.run(key, req).await;
        guard.armed = false;

        let (err, lock_updated, final_lt): (Result<(), TransError>, bool, LockType) = match res {
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
            Err(DedupError::Cancelled) => (
                Err(TransError::Other("dedup shutdown".into())),
                true,
                LockType::Unknown,
            ),
        };

        if lock_updated {
            self.update_tx_locks(key, tid, final_lt);
        }
        err
    }

    fn needs_processing(&self, key: &str, tid: &TxId, lt: LockType) -> (TxState, bool) {
        let tlocks = self.inner.tlocks.for_key(tid.as_bytes()).lock().unwrap();
        let txl = tlocks.get(tid);
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
            if let Some(m) = tlocks.get_mut(tid) {
                m.remove(key);
                if m.is_empty() {
                    tlocks.remove(tid);
                }
            }
            tracing::trace!(
                target: "glassdb::locker",
                tx = %tid,
                key,
                lock_type = %LockType::None,
                "lock_released",
            );
            return;
        }
        tlocks
            .entry(tid.clone())
            .or_default()
            .insert(key.to_string(), lt);
        tracing::trace!(
            target: "glassdb::locker",
            tx = %tid,
            key,
            lock_type = %lt,
            "lock_acquired",
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_backend::{Backend, Tags, memory::MemoryBackend};
    use glassdb_concurr::Background;
    use glassdb_data::paths;
    use glassdb_storage::{TLogger, TxCommitStatus, TxLog, TxWrite};
    use std::sync::Arc;

    struct TlCtx {
        global: Global,
        backend: Arc<dyn Backend>,
        monitor: Monitor,
        // Strong owner so spawning still works during the test.
        _bg: Arc<Background>,
    }

    fn new_test_locker(b: Arc<dyn Backend>) -> (Locker, TlCtx) {
        let local = Local::new(1024);
        let global = Global::new(b.clone(), local.clone());
        let tl = TLogger::new(global.clone(), local.clone(), "test");
        let bg = Arc::new(Background::new());
        let mon = Monitor::new(local.clone(), tl, Arc::downgrade(&bg));
        let locker = Locker::new(local, global.clone(), mon.clone());
        (
            locker,
            TlCtx {
                global,
                backend: b,
                monitor: mon,
                _bg: bg,
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
        let meta = g.get_metadata(key).await.unwrap();
        let mut info = tags_lock_info(&meta.tags).unwrap();
        info.locked_by.sort_by_key(|t| t.to_string());
        locked_by.sort_by_key(|t| t.to_string());
        assert_eq!(info.typ, typ, "lock type mismatch for {key}");
        assert_eq!(info.locked_by, locked_by, "lockers mismatch for {key}");
    }

    #[tokio::test]
    async fn lock_create() {
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx = TxId::new_random();
        tctx.monitor.begin_tx(&tx);

        // Lock + unlock without commit, repeated.
        for _ in 0..3 {
            locker.lock_create(&key, &tx).await.unwrap();
            assert_lock_info(&tctx.global, &key, LockType::Create, vec![tx.clone()]).await;

            locker.unlock(&key, &tx).await.unwrap();
            let err = tctx.global.get_metadata(&key).await.unwrap_err();
            assert!(err.is_not_found());
        }

        // Lock, commit, unlock writes the value.
        locker.lock_create(&key, &tx).await.unwrap();
        let value = b"val".to_vec();
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            path: key.clone(),
            value: value.clone(),
            deleted: false,
            prev_writer: TxId::default(),
        }];
        tctx.monitor.commit_tx(tl).await.unwrap();
        locker.unlock(&key, &tx).await.unwrap();

        let gr = tctx.global.read(&key).await.unwrap();
        assert_eq!(gr.value, value);
    }

    #[tokio::test]
    async fn lock_create_fail() {
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx = TxId::new_random();
        tctx.monitor.begin_tx(&tx);

        tctx.global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        let err = locker.lock_create(&key, &tx).await.unwrap_err();
        assert!(err.is_precondition(), "expected precondition, got {err:?}");
    }

    #[tokio::test]
    async fn lock_read_write() {
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx = TxId::new_random();
        tctx.monitor.begin_tx(&tx);

        tctx.global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![tx.clone()]).await;

        locker.unlock(&key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::None, vec![]).await;

        locker.lock_write(&key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![tx.clone()]).await;

        locker.unlock(&key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::None, vec![]).await;
    }

    #[tokio::test]
    async fn lock_multiple_r() {
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx1 = TxId::new_random();
        let tx2 = TxId::new_random();
        tctx.monitor.begin_tx(&tx1);
        tctx.monitor.begin_tx(&tx2);

        tctx.global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&key, &tx1).await.unwrap();
        locker.lock_read(&key, &tx2).await.unwrap();
        assert_lock_info(
            &tctx.global,
            &key,
            LockType::Read,
            vec![tx1.clone(), tx2.clone()],
        )
        .await;

        // Lock again with the same tx is a no-op.
        locker.lock_read(&key, &tx1).await.unwrap();
        assert_lock_info(
            &tctx.global,
            &key,
            LockType::Read,
            vec![tx1.clone(), tx2.clone()],
        )
        .await;

        locker.unlock(&key, &tx1).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![tx2.clone()]).await;

        locker.unlock(&key, &tx2).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::None, vec![]).await;
    }

    #[tokio::test]
    async fn lock_read_after_delete() {
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");

        tctx.global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        let txw = TxId::from_bytes(b"txw".to_vec());
        tctx.monitor.begin_tx(&txw);
        locker.lock_write(&key, &txw).await.unwrap();
        let mut tl = TxLog::new(txw.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWrite {
            path: key.clone(),
            value: Vec::new(),
            deleted: true,
            prev_writer: TxId::default(),
        }];
        tctx.monitor.commit_tx(tl).await.unwrap();

        let txr = TxId::from_bytes(b"txr".to_vec());
        tctx.monitor.begin_tx(&txr);
        let err = locker.lock_read(&key, &txr).await.unwrap_err();
        assert!(err.is_not_found(), "expected not-found, got {err:?}");
    }

    #[tokio::test]
    async fn lock_upgrade() {
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");
        let tx = TxId::new_random();
        tctx.monitor.begin_tx(&tx);

        tctx.global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![tx.clone()]).await;

        locker.lock_write(&key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![tx.clone()]).await;
    }

    #[tokio::test]
    async fn wait_for_tx() {
        let (locker, tctx) = init_tl_test();
        let locker = Arc::new(locker);
        let key = paths::from_key("example", b"key");
        let txr = TxId::from_bytes(b"txr".to_vec());
        let txw = TxId::from_bytes(b"txw".to_vec());
        tctx.monitor.begin_tx(&txr);
        tctx.monitor.begin_tx(&txw);

        tctx.global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&key, &txr).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![txr.clone()]).await;

        // Unlock the read just after the write starts waiting.
        let unlock_task = {
            let l = locker.clone();
            let k = key.clone();
            let t = txr.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                l.unlock(&k, &t).await.unwrap();
            })
        };
        locker.lock_write(&key, &txw).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![txw.clone()]).await;

        // Abort the write tx after a new read starts waiting.
        let abort_task = {
            let mon = tctx.monitor.clone();
            let t = txw.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                mon.abort_tx(&t).await.unwrap();
            })
        };
        // Make sure the unlock and abort have finished before relocking with the
        // same tx id (concurrent lock/unlock of the same key+tx is unsafe).
        unlock_task.await.unwrap();
        abort_task.await.unwrap();

        locker.lock_read(&key, &txr).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Read, vec![txr.clone()]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn queue_up() {
        let (locker, tctx) = init_tl_test();
        let locker = Arc::new(locker);
        let key = paths::from_key("example", b"key");
        let txw = TxId::from_bytes(b"txw".to_vec());
        tctx.monitor.begin_tx(&txw);

        tctx.global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_write(&key, &txw).await.unwrap();
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
            handles.push(tokio::spawn(async move {
                mon.begin_tx(&tx);
                l.lock_read(&k, &tx).await
            }));
        }

        // A write lock joins the queue a little later.
        let txw0 = TxId::from_bytes(b"txw0".to_vec());
        let writer = {
            let l = locker.clone();
            let k = key.clone();
            let mon = tctx.monitor.clone();
            let t = txw0.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                mon.begin_tx(&t);
                l.lock_write(&k, &t).await
            })
        };

        // Unlock the original write once the reads are waiting.
        tokio::time::sleep(Duration::from_millis(50)).await;
        locker.unlock(&key, &txw).await.unwrap();

        for h in handles {
            h.await.unwrap().unwrap();
        }
        assert_lock_info(&tctx.global, &key, LockType::Read, txrs.clone()).await;

        // Abort all readers; the queued writer should then acquire the lock.
        for tx in &txrs {
            tctx.monitor.abort_tx(tx).await.unwrap();
        }
        writer.await.unwrap().unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![txw0.clone()]).await;
    }

    #[tokio::test]
    async fn lock_upgrade_wait() {
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
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        locker.lock_read(&key, &tx).await.unwrap();
        locker.lock_read(&key, &txr).await.unwrap();
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
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                l.unlock(&k, &t).await.unwrap();
            });
        }
        locker.lock_write(&key, &tx).await.unwrap();
        assert_lock_info(&tctx.global, &key, LockType::Write, vec![tx.clone()]).await;
    }

    #[tokio::test]
    async fn lock_read_remote() {
        let (locker1, tctx1) = init_tl_test();
        let (locker2, tctx2) = new_test_locker(tctx1.backend.clone());

        let key = paths::from_key("example", b"key");
        tctx1
            .global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        // Make locker1 see stale metadata by caching it first.
        tctx1.global.get_metadata(&key).await.unwrap();

        let tx2 = TxId::new_random();
        tctx2.monitor.begin_tx(&tx2);
        locker2.lock_read(&key, &tx2).await.unwrap();
        assert_lock_info(&tctx2.global, &key, LockType::Read, vec![tx2.clone()]).await;

        let tx1 = TxId::new_random();
        tctx1.monitor.begin_tx(&tx1);
        locker1.lock_read(&key, &tx1).await.unwrap();
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
        let (locker1, tctx1) = init_tl_test();
        let (locker2, tctx2) = new_test_locker(tctx1.backend.clone());
        let locker2 = Arc::new(locker2);

        let key = paths::from_key("example", b"key");
        tctx1
            .global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        // Equal priority so the second writer waits for the holder to release
        // instead of wounding it.
        let tx2 = mk_tid(1, "tx2");
        tctx2.monitor.begin_tx(&tx2);
        locker2.lock_write(&key, &tx2).await.unwrap();
        assert_lock_info(&tctx2.global, &key, LockType::Write, vec![tx2.clone()]).await;

        {
            let l = locker2.clone();
            let k = key.clone();
            let t = tx2.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                l.unlock(&k, &t).await.unwrap();
            });
        }

        let tx1 = mk_tid(1, "tx1");
        tctx1.monitor.begin_tx(&tx1);
        locker1.lock_write(&key, &tx1).await.unwrap();
        assert_lock_info(&tctx1.global, &key, LockType::Write, vec![tx1.clone()]).await;

        // Commit tx2 after locker1 holds the write lock.
        {
            let mon = tctx2.monitor.clone();
            let k = key.clone();
            let t = tx2.clone();
            tokio::spawn(async move {
                tokio::time::sleep(Duration::from_millis(50)).await;
                let mut tl = TxLog::new(t.clone(), TxCommitStatus::Ok);
                tl.writes = vec![TxWrite {
                    path: k,
                    value: b"foo".to_vec(),
                    deleted: false,
                    prev_writer: TxId::default(),
                }];
                let _ = mon.commit_tx(tl).await;
            });
        }

        locker1.lock_write(&key, &tx1).await.unwrap();
        assert_lock_info(&tctx1.global, &key, LockType::Write, vec![tx1.clone()]).await;
    }

    #[tokio::test(start_paused = true)]
    async fn wound_younger_holder() {
        let (locker, tctx) = init_tl_test();
        let key = paths::from_key("example", b"key");

        tctx.global
            .write(&key, b"x".to_vec(), Tags::new())
            .await
            .unwrap();

        // A younger transaction holds the write lock.
        let tx_young = mk_tid(2, "young");
        tctx.monitor.begin_tx(&tx_young);
        locker.lock_write(&key, &tx_young).await.unwrap();

        // An older (higher-priority) transaction wants the same lock. Under the
        // wound-wait rule it aborts the younger holder and takes the lock
        // without waiting.
        let tx_old = mk_tid(1, "old");
        tctx.monitor.begin_tx(&tx_old);
        locker.lock_write(&key, &tx_old).await.unwrap();

        assert_lock_info(&tctx.global, &key, LockType::Write, vec![tx_old.clone()]).await;

        // The younger transaction was wounded (aborted).
        let status = tctx.monitor.tx_status(&tx_young).await.unwrap();
        assert_eq!(status, TxCommitStatus::Aborted);
    }

    // Diagnostic snapshot reflects each tx's held locks (sorted by path) and is
    // empty after every lock is released.
    #[tokio::test]
    async fn tx_locks_snapshot_lists_held_locks_per_tx() {
        let (locker, tctx) = init_tl_test();
        let key_a = paths::from_key("coll", b"a");
        let key_b = paths::from_key("coll", b"b");
        let key_c = paths::from_key("coll", b"c");

        // Pre-create the keys so reads can take a non-Create lock.
        for k in [&key_a, &key_b, &key_c] {
            tctx.global
                .write(k, b"x".to_vec(), Tags::new())
                .await
                .unwrap();
        }

        let tx1 = mk_tid(1, "tx1");
        let tx2 = mk_tid(2, "tx2");
        tctx.monitor.begin_tx(&tx1);
        tctx.monitor.begin_tx(&tx2);

        // tx1 holds a write on b and a read on a; tx2 holds a read on c.
        locker.lock_write(&key_b, &tx1).await.unwrap();
        locker.lock_read(&key_a, &tx1).await.unwrap();
        locker.lock_read(&key_c, &tx2).await.unwrap();

        let snap = locker.tx_locks_snapshot();
        assert_eq!(snap.len(), 2, "expected two txs: {snap:?}");
        let s1 = snap.iter().find(|s| s.tx_id == tx1).expect("tx1 missing");
        let s2 = snap.iter().find(|s| s.tx_id == tx2).expect("tx2 missing");
        // Locks are sorted by path within each tx.
        assert_eq!(
            s1.locks,
            vec![
                PathLock {
                    path: key_a.clone(),
                    typ: LockType::Read,
                },
                PathLock {
                    path: key_b.clone(),
                    typ: LockType::Write,
                },
            ],
        );
        assert_eq!(
            s2.locks,
            vec![PathLock {
                path: key_c.clone(),
                typ: LockType::Read,
            }],
        );

        // Releasing every lock empties the snapshot.
        locker.unlock(&key_a, &tx1).await.unwrap();
        locker.unlock(&key_b, &tx1).await.unwrap();
        locker.unlock(&key_c, &tx2).await.unwrap();
        assert!(locker.tx_locks_snapshot().is_empty());
    }
}
