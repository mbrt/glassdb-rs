//! Transaction lifecycle monitor. Ported from the Go `internal/trans/monitor.go`.
//!
//! Tracks local and remote transaction state, refreshes pending logs to keep
//! locks alive, aborts expired remote transactions, and lets callers wait for a
//! transaction to finalize.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use glassdb_backend as backend;
use glassdb_concurr::{Background, Clock, RetryConfig, rt, shard::Sharded};
use glassdb_data::TxId;
use glassdb_storage::{
    Local, MAX_STALENESS, StorageError, TLogger, TValue, TxCommitStatus, TxLog, Version,
};
use tokio::sync::oneshot;

use crate::error::TransError;

const PENDING_TX_TIMEOUT: Duration = Duration::from_secs(15);
const MAX_CLOCK_SKEW: Duration = Duration::from_secs(30);

fn refresh_timeout() -> Duration {
    // refreshMultiplier = 0.5
    PENDING_TX_TIMEOUT / 2
}

fn is_expired(last_refresh: SystemTime, now: SystemTime) -> bool {
    // Go: now.Sub(lastRefresh.Add(maxClockSkew)) > pendingTxTimeout
    match now.duration_since(last_refresh + MAX_CLOCK_SKEW) {
        Ok(d) => d > PENDING_TX_TIMEOUT,
        Err(_) => false,
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum RefreshState {
    NotStarted,
    Running,
    Stopped,
}

struct TxStatusEntry {
    status: TxCommitStatus,
    last_version: backend::Version,
    refresh_state: RefreshState,
}

struct WaitRequest {
    tx: oneshot::Sender<WaitTxResult>,
}

#[derive(Default)]
struct State {
    local_tx: HashMap<TxId, TxStatusEntry>,
    waiters: HashMap<TxId, Vec<WaitRequest>>,
    unknown_tx: HashMap<TxId, SystemTime>,
}

struct Inner {
    local: Local,
    tl: TLogger,
    // Weak so a `Monitor` clone captured inside a spawned task does not keep
    // the [`Background`] alive across DB shutdown. The single strong owner
    // is `DbInner::background`.
    background: Weak<Background>,
    clock: Clock,
    retry: RetryConfig,
    // The transaction-tracking maps are partitioned into independent shards
    // keyed by tid. Grouping the three maps under one lock per shard keeps
    // their cross-map updates (e.g. removing a tx and notifying its waiters)
    // atomic for a given transaction.
    shards: Sharded<Mutex<State>>,
}

/// Tracks the lifecycle of transactions: commit, abort, status queries, and
/// asynchronous waits.
#[derive(Clone)]
pub struct Monitor {
    inner: Arc<Inner>,
}

/// A transaction's commit status for a specific key, plus the value written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct KeyCommitStatus {
    pub status: TxCommitStatus,
    pub value: TValue,
}

/// The outcome of waiting for a transaction to complete.
#[derive(Debug, Clone, Default)]
pub struct WaitTxResult {
    pub status: TxCommitStatus,
    pub err: Option<TransError>,
}

impl Monitor {
    /// Creates a monitor using the real wall-clock and default retry timing.
    pub fn new(local: Local, tl: TLogger, background: Weak<Background>) -> Self {
        Self::with_config(local, tl, background, Clock::real(), RetryConfig::default())
    }

    /// Creates a monitor with a custom clock (used in tests for deterministic
    /// expiry/refresh timing) and retry-backoff configuration. The retry config
    /// tunes the backoff used when polling a peer transaction's commit status
    /// and when writing a transaction's final log.
    pub fn with_config(
        local: Local,
        tl: TLogger,
        background: Weak<Background>,
        clock: Clock,
        retry: RetryConfig,
    ) -> Self {
        Monitor {
            inner: Arc::new(Inner {
                local,
                tl,
                background,
                clock,
                retry,
                shards: Sharded::new(|_| Mutex::new(State::default())),
            }),
        }
    }

    /// Returns the shard lock responsible for `tid`.
    fn shard_for(&self, tid: &TxId) -> &Mutex<State> {
        self.inner.shards.for_key(tid.as_bytes())
    }

    /// Returns the current wall-clock time according to the monitor's clock.
    /// Used by the transaction engine to derive a transaction's priority.
    pub(crate) fn clock_now(&self) -> SystemTime {
        self.inner.clock.now()
    }

    /// Registers a new pending local transaction.
    pub fn begin_tx(&self, tid: &TxId) {
        let mut st = self.shard_for(tid).lock().unwrap();
        st.local_tx.insert(
            tid.clone(),
            TxStatusEntry {
                status: TxCommitStatus::Pending,
                last_version: backend::Version::default(),
                refresh_state: RefreshState::NotStarted,
            },
        );
    }

    /// Starts a background task that periodically refreshes the pending log so
    /// the transaction is not considered expired. The task is aborted when its
    /// [`Background`] is dropped.
    pub fn start_refresh_tx(&self, tid: &TxId) {
        let need_start = {
            let mut st = self.shard_for(tid).lock().unwrap();
            match st.local_tx.get_mut(tid) {
                Some(e) if e.refresh_state == RefreshState::NotStarted => {
                    e.refresh_state = RefreshState::Running;
                    true
                }
                _ => false,
            }
        };
        if !need_start {
            return;
        }
        // The captured `Monitor` clone only holds a `Weak<Background>`, so it
        // does not keep `Background` alive past DB shutdown. If `Background`
        // is already gone the refresh is silently skipped.
        let Some(bg) = self.inner.background.upgrade() else {
            return;
        };
        let m = self.clone();
        let tid = tid.clone();
        bg.spawn(async move {
            m.refresh_pending(tid).await;
        });
    }

    /// Marks the transaction committed, writing the final log (if it held
    /// locks), updating local storage, and notifying waiters.
    pub async fn commit_tx(&self, mut tl: TxLog) -> Result<(), TransError> {
        self.stop_tx_refresh(&tl.id);

        // Optimization: if nothing was locked (RO or single-W tx), avoid writing
        // the transaction log.
        if !tl.locks.is_empty() {
            tl.status = TxCommitStatus::Ok;
            // `context` preserves the `AlreadyFinalized` sentinel so the commit
            // path can recognize a wound (the log was already aborted out from
            // under us), as well as any classification of an escaping error.
            // In-doubt outcomes are normally retried inside `set_final_log`
            // because the log is keyed by tx id and the write is idempotent.
            self.set_final_log(&tl)
                .await
                .map_err(|e| e.context("writing tx log"))?;
        } else if tl.writes.len() > 1 {
            return Err(TransError::other(format!(
                "got {} writes with no locks; this is a bug",
                tl.writes.len()
            )));
        }

        let version = Version {
            b: backend::Version::default(),
            writer: tl.id.clone(),
        };
        for entry in &tl.writes {
            if entry.deleted {
                self.inner.local.mark_deleted(&entry.path, version.clone());
            } else {
                self.inner
                    .local
                    .write(&entry.path, entry.value.clone(), version.clone());
            }
        }

        let mut st = self.shard_for(&tl.id).lock().unwrap();
        st.local_tx.remove(&tl.id);
        notify_waiters(
            &mut st,
            &tl.id,
            WaitTxResult {
                status: TxCommitStatus::Ok,
                err: None,
            },
        );
        Ok(())
    }

    /// Marks the transaction aborted, writing the final log and notifying
    /// waiters. The local state is cleared even if writing the log fails.
    pub async fn abort_tx(&self, tid: &TxId) -> Result<(), TransError> {
        self.stop_tx_refresh(tid);

        let res = self
            .set_final_log(&TxLog::new(tid.clone(), TxCommitStatus::Aborted))
            .await;

        let mut st = self.shard_for(tid).lock().unwrap();
        st.local_tx.remove(tid);
        notify_waiters(
            &mut st,
            tid,
            WaitTxResult {
                status: TxCommitStatus::Aborted,
                err: None,
            },
        );
        res
    }

    /// Forces the given transaction into the aborted state so that a
    /// higher-priority transaction can take over its locks under the wound-wait
    /// rule. It is idempotent and safe on transactions that already finished: a
    /// committed transaction is left untouched (its locks are released through
    /// the normal flow), and an already-aborted one is a no-op.
    ///
    /// The abort is made durable via a conditional write on the transaction log,
    /// so it is observed both by the local victim (its commit will fail) and by
    /// other clients holding the same lock.
    pub async fn wound_tx(&self, tid: &TxId) -> Result<(), TransError> {
        let cs = self.inner.tl.commit_status(tid).await.map_err(|e| {
            TransError::Storage(e.context(format!("reading status of wound target {tid}")))
        })?;
        if cs.status.is_final() {
            // Already committed or aborted: nothing left to wound.
            self.mark_local_aborted(tid, cs.status);
            return Ok(());
        }

        // Force the transaction to aborted, CAS-ing over its current log version
        // (or creating an aborted log if it has none yet).
        let status = self.try_abort_remote_tx(tid, &cs.version).await?;
        self.mark_local_aborted(tid, status);
        Ok(())
    }

    /// Reflects a durable abort in the in-memory state when the wounded
    /// transaction is local, so the victim and any waiters unwind promptly.
    fn mark_local_aborted(&self, tid: &TxId, status: TxCommitStatus) {
        if status != TxCommitStatus::Aborted {
            return;
        }
        self.stop_tx_refresh(tid);

        let mut st = self.shard_for(tid).lock().unwrap();
        st.local_tx.remove(tid);
        notify_waiters(
            &mut st,
            tid,
            WaitTxResult {
                status: TxCommitStatus::Aborted,
                err: None,
            },
        );
    }

    /// Returns the commit status, checking locally first then remote storage.
    pub async fn tx_status(&self, tid: &TxId) -> Result<TxCommitStatus, TransError> {
        {
            let st = self.shard_for(tid).lock().unwrap();
            if let Some(e) = st.local_tx.get(tid) {
                return Ok(e.status);
            }
        }
        self.fetch_remote_tx_status(tid).await
    }

    /// Waits asynchronously for the transaction to finalize. The returned
    /// future yields exactly one result; dropping it cancels the wait.
    pub fn wait_for_tx(
        &self,
        tid: &TxId,
    ) -> impl std::future::Future<Output = WaitTxResult> + Send + use<> {
        let rx = self.wait_for_tx_rx(tid);
        async move { rx.await.unwrap_or_default() }
    }

    fn wait_for_tx_rx(&self, tid: &TxId) -> oneshot::Receiver<WaitTxResult> {
        let (tx, rx) = oneshot::channel();

        let mut st = self.shard_for(tid).lock().unwrap();
        let entry = st.local_tx.get(tid);
        let is_local = entry.is_some();
        let status = entry.map(|e| e.status).unwrap_or(TxCommitStatus::Unknown);

        // Matches Go precedence: (isLocal && OK) || Aborted.
        if (is_local && status == TxCommitStatus::Ok) || status == TxCommitStatus::Aborted {
            let _ = tx.send(WaitTxResult { status, err: None });
            return rx;
        }

        if let Some(ws) = st.waiters.get_mut(tid) {
            ws.push(WaitRequest { tx });
            return rx;
        }

        if is_local {
            // Local transition: no worker needed; we'll be notified by
            // commit_tx/abort_tx.
            st.waiters.insert(tid.clone(), vec![WaitRequest { tx }]);
            return rx;
        }

        // Remote transaction: spawn a poller. Waiter liveness is checked
        // between polls so the poller exits promptly once every caller has
        // dropped its `wait_for_tx` future.
        st.waiters.insert(tid.clone(), vec![WaitRequest { tx }]);
        drop(st);

        let m = self.clone();
        let tid = tid.clone();
        // Detached poller: it terminates either when the tx finalizes (final
        // status or a fetch error) or when every caller has dropped its
        // `wait_for_tx` future.
        rt::spawn(async move {
            let (status, err) = m.poll_tx_status_with_liveness(&tid).await;
            let res = WaitTxResult { status, err };
            let mut st = m.shard_for(&tid).lock().unwrap();
            notify_waiters(&mut st, &tid, res);
        });

        rx
    }

    /// Returns the committed value a transaction wrote for `key`, reading from
    /// local storage or the transaction log.
    pub async fn committed_value(
        &self,
        key: &str,
        tid: &TxId,
    ) -> Result<KeyCommitStatus, TransError> {
        if let Some(lr) = self.inner.local.read(key, MAX_STALENESS)
            && lr.version.writer == *tid
        {
            return Ok(KeyCommitStatus {
                status: TxCommitStatus::Ok,
                value: TValue {
                    value: lr.value,
                    deleted: lr.deleted,
                    not_written: false,
                },
            });
        }

        let status = self.tx_status(tid).await?;
        if status != TxCommitStatus::Ok {
            return Ok(KeyCommitStatus {
                status,
                value: TValue::default(),
            });
        }

        let tl = self
            .inner
            .tl
            .get(tid)
            .await
            .map_err(|e| TransError::Storage(e.context(format!("getting TID {tid}"))))?;
        for entry in &tl.writes {
            if entry.path == key {
                return Ok(KeyCommitStatus {
                    status: TxCommitStatus::Ok,
                    value: TValue {
                        value: entry.value.clone(),
                        deleted: entry.deleted,
                        not_written: false,
                    },
                });
            }
        }
        Ok(KeyCommitStatus {
            status: TxCommitStatus::Ok,
            value: TValue {
                not_written: true,
                ..Default::default()
            },
        })
    }

    async fn fetch_remote_tx_status(&self, tid: &TxId) -> Result<TxCommitStatus, TransError> {
        let status = self.inner.tl.commit_status(tid).await?;
        match status.status {
            TxCommitStatus::Unknown => self.handle_unknown_tx(tid).await,
            TxCommitStatus::Pending => {
                if is_expired(status.last_update, self.inner.clock.now()) {
                    self.try_abort_remote_tx(tid, &status.version).await
                } else {
                    Ok(TxCommitStatus::Pending)
                }
            }
            s => Ok(s),
        }
    }

    async fn handle_unknown_tx(&self, tid: &TxId) -> Result<TxCommitStatus, TransError> {
        let now = self.inner.clock.now();
        let first_check = {
            let mut st = self.shard_for(tid).lock().unwrap();
            match st.unknown_tx.get(tid) {
                Some(fc) => *fc,
                None => {
                    st.unknown_tx.insert(tid.clone(), now);
                    return Ok(TxCommitStatus::Pending);
                }
            }
        };

        if is_expired(first_check, now) {
            let res = self
                .try_abort_remote_tx(tid, &backend::Version::default())
                .await;
            if res.is_ok() {
                self.shard_for(tid).lock().unwrap().unknown_tx.remove(tid);
            }
            return res;
        }
        Ok(TxCommitStatus::Pending)
    }

    async fn try_abort_remote_tx(
        &self,
        tid: &TxId,
        expected: &backend::Version,
    ) -> Result<TxCommitStatus, TransError> {
        let tlog = TxLog::new(tid.clone(), TxCommitStatus::Aborted);
        let mut expected = expected.clone();
        let mut backoff = self.inner.retry.backoff();
        loop {
            let r = if expected.is_unset() {
                self.inner.tl.set(&tlog).await
            } else {
                self.inner.tl.set_if(&tlog, &expected).await
            };
            match r {
                Ok(_) => return Ok(TxCommitStatus::Aborted),
                Err(StorageError::Precondition) => {
                    // The version moved under us (a commit, a pending-log
                    // refresh, or another wounder). Report whatever status is
                    // now durable.
                    let st = self.inner.tl.commit_status(tid).await?;
                    return Ok(st.status);
                }
                // In-doubt: the abort write may or may not have landed. Just
                // like `set_final_log`, forcing a not-yet-final log to
                // `aborted` is idempotent and convergent, so it is always safe
                // to retry (ADR-009). This is what keeps a lost ack on a wound
                // (or on an expired-tx abort) from escaping the locker as a
                // `failed locking` error: a pre-commit outcome must be
                // recovered in place, never surfaced to the caller. Re-read to
                // decide: a final status resolves it (our own landed abort, a
                // peer's, or a commit that won the race); a still-pending
                // status means retry the CAS over the refreshed version.
                Err(StorageError::Unavailable(_)) => {
                    let st = self.inner.tl.commit_status(tid).await?;
                    if st.status.is_final() {
                        return Ok(st.status);
                    }
                    expected = st.version;
                }
                Err(e) => return Err(e.into()),
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    /// Polls the remote tx status until it finalizes, a fetch fails, or every
    /// caller has dropped its `wait_for_tx` future (signalled by closed
    /// `oneshot::Sender`s in the waiters list). The latter is the future-drop
    /// equivalent of the per-call cancellation contexts the Go original used.
    async fn poll_tx_status_with_liveness(
        &self,
        tid: &TxId,
    ) -> (TxCommitStatus, Option<TransError>) {
        let mut backoff = self.inner.retry.backoff();
        loop {
            let s = match self.fetch_remote_tx_status(tid).await {
                Err(e) => return (TxCommitStatus::Unknown, Some(e)),
                Ok(s) => s,
            };
            if s.is_final() {
                return (s, None);
            }
            let alive = {
                let mut st = self.shard_for(tid).lock().unwrap();
                match st.waiters.get_mut(tid) {
                    Some(ws) => {
                        ws.retain(|w| !w.tx.is_closed());
                        if ws.is_empty() {
                            st.waiters.remove(tid);
                            false
                        } else {
                            true
                        }
                    }
                    None => false,
                }
            };
            if !alive {
                return (
                    s,
                    Some(TransError::other("no live waiters; abandoning poll")),
                );
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn set_final_log(&self, tlog: &TxLog) -> Result<(), TransError> {
        let tid = &tlog.id;
        if tid.is_unset() {
            return Err(TransError::other("missing required tlog ID"));
        }
        let mut last_v = {
            let st = self.shard_for(tid).lock().unwrap();
            st.local_tx
                .get(tid)
                .map(|e| e.last_version.clone())
                .unwrap_or_default()
        };

        let mut backoff = self.inner.retry.backoff();
        loop {
            let r = if last_v.is_unset() {
                self.inner.tl.set(tlog).await
            } else {
                self.inner.tl.set_if(tlog, &last_v).await
            };
            match r {
                Ok(_) => return Ok(()),
                Err(StorageError::Precondition) => {
                    // The version moved under us. Possible races: our own
                    // `refresh_pending` advancing the pending log, a wound
                    // from another client writing `aborted`, or our own
                    // previously-landed write (e.g. an `Unavailable` retry
                    // below). Re-read and decide:
                    //   - Status still `Pending`: it's a non-final race; it is
                    //     always safe to refresh `last_v` and retry.
                    //   - Status matches what we are writing: either us (only
                    //     we write `committed` for our own tx id) or a wound
                    //     that converged to the same outcome we wanted (only
                    //     possible for `aborted`). Either way the desired
                    //     final state is durable -> success.
                    //   - Status final but mismatched (we wanted `committed`,
                    //     found `aborted`): a wound landed first -> surface as
                    //     `AlreadyFinalized` so the commit path treats it as a
                    //     wound.
                    let st = self.inner.tl.commit_status(tid).await?;
                    if st.status == tlog.status {
                        return Ok(());
                    }
                    if st.status.is_final() {
                        return Err(TransError::AlreadyFinalized);
                    }
                    last_v = st.version;
                }
                // In-doubt outcome: the log write may or may not have landed.
                // It is always safe to retry as long as the log status was not
                // final: a not-yet-final log can only become final by a write
                // that converges on our intent (us or a wound to `aborted`),
                // and the precondition branch above resolves the matching /
                // mismatched final outcomes correctly.
                Err(StorageError::Unavailable(_)) => {}
                Err(e) => return Err(e.into()),
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    fn should_refresh(&self, tid: &TxId) -> bool {
        let st = self.shard_for(tid).lock().unwrap();
        matches!(
            st.local_tx.get(tid),
            Some(e) if e.refresh_state == RefreshState::Running
        )
    }

    fn stop_tx_refresh(&self, tid: &TxId) -> bool {
        let mut st = self.shard_for(tid).lock().unwrap();
        match st.local_tx.get_mut(tid) {
            Some(e) if e.refresh_state == RefreshState::Running => {
                e.refresh_state = RefreshState::Stopped;
                true
            }
            _ => false,
        }
    }

    async fn refresh_pending(&self, tid: TxId) {
        if !self.should_refresh(&tid) {
            return;
        }
        let mut last_version = backend::Version::default();

        loop {
            rt::sleep(refresh_timeout()).await;
            if !self.should_refresh(&tid) {
                return;
            }

            let start = self.inner.clock.now();
            let mut tl = TxLog::new(tid.clone(), TxCommitStatus::Pending);
            tl.timestamp = Some(start);

            let r = if last_version.is_unset() {
                self.inner.tl.set(&tl).await
            } else {
                self.inner.tl.set_if(&tl, &last_version).await
            };
            match r {
                Ok(v) => {
                    last_version = v;
                    let mut st = self.shard_for(&tid).lock().unwrap();
                    if let Some(e) = st.local_tx.get_mut(&tid) {
                        e.last_version = last_version.clone();
                    }
                }
                Err(_) => return,
            }
        }
    }
}

fn notify_waiters(st: &mut State, tid: &TxId, res: WaitTxResult) {
    if let Some(ws) = st.waiters.remove(tid) {
        for w in ws {
            // `send` silently fails if the receiver has been dropped,
            // which is the new "waiter cancelled" signal.
            let _ = w.tx.send(res.clone());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_backend::{Backend, Tags, memory::MemoryBackend};
    use glassdb_data::paths;
    use glassdb_storage::{Global, LockType, PathLock, TxWrite};

    struct TestCtx {
        tl: TLogger,
        global: Global,
        // The strong `Arc<Background>` lives here so refresh tasks can be
        // spawned for the duration of the test; the `Monitor` only stores a
        // `Weak`.
        _bg: Arc<Background>,
    }

    fn new_test_monitor(b: Arc<dyn Backend>) -> (Monitor, TestCtx) {
        new_test_monitor_clock(b, Clock::real())
    }

    fn new_test_monitor_clock(b: Arc<dyn Backend>, clock: Clock) -> (Monitor, TestCtx) {
        let local = Local::new(1024);
        let global = Global::new(b, local.clone());
        let tl = TLogger::new(global.clone(), local.clone(), "test");
        let bg = Arc::new(Background::new());
        let mon = Monitor::with_config(
            local,
            tl.clone(),
            Arc::downgrade(&bg),
            clock,
            RetryConfig::default(),
        );
        (
            mon,
            TestCtx {
                tl,
                global,
                _bg: bg,
            },
        )
    }

    #[tokio::test]
    async fn status() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, _t1) = new_test_monitor(b.clone());
        let (mon2, _t2) = new_test_monitor(b.clone());
        let key = paths::from_key("example", b"key1");
        let tx = TxId::from_bytes(b"tx1".to_vec());
        mon1.begin_tx(&tx);

        assert_eq!(mon1.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);

        mon1.abort_tx(&tx).await.unwrap();
        assert_eq!(mon1.tx_status(&tx).await.unwrap(), TxCommitStatus::Aborted);
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Aborted);

        let tx = TxId::from_bytes(b"tx2".to_vec());
        mon1.begin_tx(&tx);
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        tl.locks = vec![PathLock {
            path: key,
            typ: LockType::Write,
        }];
        mon1.commit_tx(tl).await.unwrap();
        assert_eq!(mon1.tx_status(&tx).await.unwrap(), TxCommitStatus::Ok);
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Ok);
    }

    #[tokio::test]
    async fn committed_value() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, t1) = new_test_monitor(b.clone());
        let (mon2, _t2) = new_test_monitor(b.clone());
        let key = paths::from_key("example", b"key");

        t1.global
            .write(&key, Arc::from(&b"x"[..]), Tags::new())
            .await
            .unwrap();

        let tx = TxId::from_bytes(b"tx2".to_vec());
        mon1.begin_tx(&tx);
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWriteForTest::w(&key, b"val1")];
        tl.locks = vec![PathLock {
            path: key.clone(),
            typ: LockType::Write,
        }];
        mon1.commit_tx(tl).await.unwrap();

        let cs = mon1.committed_value(&key, &tx).await.unwrap();
        assert_eq!(cs.status, TxCommitStatus::Ok);
        assert_eq!(&*cs.value.value, b"val1");
        // From a remote monitor.
        let cs = mon2.committed_value(&key, &tx).await.unwrap();
        assert_eq!(cs.status, TxCommitStatus::Ok);
        assert_eq!(&*cs.value.value, b"val1");

        // A key the transaction didn't write.
        let key2 = paths::from_key("example", b"key2");
        let cs = mon2.committed_value(&key2, &tx).await.unwrap();
        assert_eq!(cs.status, TxCommitStatus::Ok);
        assert!(cs.value.not_written);
    }

    #[tokio::test]
    async fn wait_for_local_tx() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, _t1) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"tx1".to_vec());
        mon1.begin_tx(&tx);

        let ch1 = mon1.wait_for_tx(&tx);
        let ch2 = mon1.wait_for_tx(&tx);

        mon1.abort_tx(&tx).await.unwrap();
        assert_eq!(ch1.await.status, TxCommitStatus::Aborted);
        assert_eq!(ch2.await.status, TxCommitStatus::Aborted);
    }

    #[tokio::test]
    async fn wait_for_remote_tx() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, _t1) = new_test_monitor(b.clone());
        let (mon2, _t2) = new_test_monitor(b.clone());
        let tx = TxId::from_bytes(b"tx1".to_vec());
        mon1.begin_tx(&tx);

        let _ch1 = mon2.wait_for_tx(&tx);
        let ch2 = mon2.wait_for_tx(&tx);
        let ch3 = mon2.wait_for_tx(&tx);

        mon1.abort_tx(&tx).await.unwrap();

        assert_eq!(ch2.await.status, TxCommitStatus::Aborted);
        assert_eq!(ch3.await.status, TxCommitStatus::Aborted);
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_keeps_pending() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor_clock(b.clone(), Clock::anchored());
        let tx = TxId::from_bytes(b"tx1".to_vec());
        mon.begin_tx(&tx);
        mon.start_refresh_tx(&tx);

        // Advance well past the pending timeout. Refresh keeps it alive.
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

        let st = t.tl.commit_status(&tx).await.unwrap();
        assert_eq!(st.status, TxCommitStatus::Pending);

        // A separate monitor should still see it as pending (not expired).
        let (mon2, _t2) = new_test_monitor_clock(b, Clock::anchored());
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);

        mon.abort_tx(&tx).await.unwrap();
    }

    // Tiny helper to build a TxWrite in tests.
    struct TxWriteForTest;
    impl TxWriteForTest {
        fn w(path: &str, value: &[u8]) -> TxWrite {
            TxWrite {
                path: path.to_string(),
                value: Arc::from(value),
                deleted: false,
                prev_writer: TxId::default(),
            }
        }
    }
}
