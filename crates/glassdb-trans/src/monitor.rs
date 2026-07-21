//! Transaction lifecycle monitor. Ported from the Go `internal/trans/monitor.go`.
//!
//! Tracks local and remote transaction state, refreshes pending logs to keep
//! locks alive, aborts expired remote transactions, and lets callers wait for a
//! transaction to finalize.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use glassdb_concurr::{Background, Clock, RetryConfig, rt, shard::Sharded};
use glassdb_data::{KeyRef, TxId};
use glassdb_storage::{
    Observation, Requirement, SequencePoint, StorageError, TLogger, TValue, Timeline,
    TxCommitStatus, TxLock, TxLog, TxStatus,
};
use hashlink::LinkedHashMap;
use tokio::sync::oneshot;

use crate::error::TransError;

const FINAL_STATUS_CACHE_SIZE: usize = 16384;

/// Timing parameters for transaction liveness and recovery.
///
/// Production uses [`ProtocolTiming::default`]. Deterministic simulation uses
/// [`ProtocolTiming::simulation`] so lease-boundary interleavings are cheap to
/// explore while preserving the production ratios.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolTiming {
    pending_timeout: Duration,
    max_clock_skew: Duration,
}

impl ProtocolTiming {
    /// Creates a timing profile with an explicit pending-transaction timeout
    /// and maximum expected clock skew between database clients.
    ///
    /// `max_clock_skew` must conservatively bound the clocks of every client
    /// using the database; underestimating it can reclaim a live transaction.
    ///
    /// # Panics
    ///
    /// Panics when `pending_timeout` is zero.
    pub const fn new(pending_timeout: Duration, max_clock_skew: Duration) -> Self {
        assert!(
            !pending_timeout.is_zero(),
            "pending timeout must be non-zero"
        );
        Self {
            pending_timeout,
            max_clock_skew,
        }
    }

    /// Returns the shortened timing profile used by deterministic simulation.
    pub const fn simulation() -> Self {
        Self::new(Duration::from_millis(250), Duration::from_millis(500))
    }

    /// Returns the interval after which an unrefreshed transaction is stale.
    pub const fn pending_timeout(self) -> Duration {
        self.pending_timeout
    }

    /// Returns the allowance for timestamps written by another machine.
    pub const fn max_clock_skew(self) -> Duration {
        self.max_clock_skew
    }

    /// Applies the skew-padded absolute lease check used for foreign timestamps
    /// and GC retention horizons.
    pub(crate) fn is_expired(self, last_refresh: SystemTime, now: SystemTime) -> bool {
        // Go: now.Sub(lastRefresh.Add(maxClockSkew)) > pendingTxTimeout
        match now.duration_since(last_refresh + self.max_clock_skew) {
            Ok(d) => d > self.pending_timeout,
            Err(_) => false,
        }
    }

    fn refresh_interval(self) -> Duration {
        // refreshMultiplier = 0.5
        self.pending_timeout / 2
    }

    /// Applies the observer-relative check used when both endpoints come from
    /// the same local clock.
    fn is_expired_no_skew(self, first_seen: SystemTime, now: SystemTime) -> bool {
        match now.duration_since(first_seen) {
            Ok(d) => d > self.pending_timeout,
            Err(_) => false,
        }
    }
}

impl Default for ProtocolTiming {
    fn default() -> Self {
        Self::new(Duration::from_secs(15), Duration::from_secs(30))
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
    last_observation: Option<Observation<TxLog>>,
    refresh_state: RefreshState,
    // The lock set this transaction holds, recorded by the engine once it has
    // acquired its locks.
    locks: Vec<TxLock>,
}

#[derive(Clone, Copy)]
struct FinalStatus {
    status: TxCommitStatus,
    watermark: SequencePoint,
}

struct FinalStatusCache {
    capacity: usize,
    entries: LinkedHashMap<TxId, FinalStatus>,
}

impl FinalStatusCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            entries: LinkedHashMap::new(),
        }
    }

    fn get(&mut self, tid: &TxId) -> Option<FinalStatus> {
        self.entries.to_back(tid).copied()
    }

    fn insert(&mut self, tid: TxId, status: FinalStatus) {
        self.entries.insert(tid, status);
        while self.entries.len() > self.capacity {
            self.entries.pop_front();
        }
    }
}

struct WaitRequest {
    tx: oneshot::Sender<TxCommitStatus>,
}

/// Observer-relative liveness tracker for a watched remote pending transaction
/// (ADR-024). Remembers the last lease `timestamp` seen on the holder's object
/// and the observer-clock time it was first seen at that value; if the value
/// does not advance within the configured pending timeout of `observed_at` (no
/// skew, since both endpoints are the observer's own clock) the holder has
/// stopped making progress and is treated as dead.
struct PendingProgress {
    last_seen: SystemTime,
    observed_at: SystemTime,
}

#[derive(Default)]
struct State {
    local_tx: HashMap<TxId, TxStatusEntry>,
    waiters: HashMap<TxId, Vec<WaitRequest>>,
    unknown_tx: HashMap<TxId, SystemTime>,
    pending_progress: HashMap<TxId, PendingProgress>,
}

struct Inner {
    tl: TLogger,
    timeline: Timeline,
    final_status: Mutex<FinalStatusCache>,
    // Weak so a `Monitor` clone captured inside a spawned task does not keep
    // the [`Background`] alive across DB shutdown. The single strong owner
    // is `DbInner::background`.
    background: Weak<Background>,
    clock: Clock,
    retry: RetryConfig,
    timing: ProtocolTiming,
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
pub(crate) struct KeyCommitStatus {
    pub status: TxCommitStatus,
    pub value: TValue,
    pub cache_hit: bool,
}

impl Monitor {
    /// Creates a monitor using the real wall-clock and default retry timing.
    pub fn new(tl: TLogger, timeline: Timeline, background: Weak<Background>) -> Self {
        Self::with_config(
            tl,
            timeline,
            background,
            Clock::real(),
            RetryConfig::default(),
            ProtocolTiming::default(),
        )
    }

    /// Creates a monitor with a custom clock (used in tests for deterministic
    /// expiry/refresh timing), retry-backoff configuration, and transaction
    /// liveness timing. The retry config tunes the backoff used when polling a
    /// peer transaction's commit status and when writing a transaction's final
    /// log.
    pub fn with_config(
        tl: TLogger,
        timeline: Timeline,
        background: Weak<Background>,
        clock: Clock,
        retry: RetryConfig,
        timing: ProtocolTiming,
    ) -> Self {
        Monitor {
            inner: Arc::new(Inner {
                tl,
                timeline,
                final_status: Mutex::new(FinalStatusCache::new(FINAL_STATUS_CACHE_SIZE)),
                background,
                clock,
                retry,
                timing,
                shards: Sharded::new(|_| Mutex::new(State::default())),
            }),
        }
    }

    pub(crate) fn protocol_timing(&self) -> ProtocolTiming {
        self.inner.timing
    }

    /// Registers a new pending local transaction.
    pub(crate) fn begin_tx(&self, tid: &TxId) {
        let mut st = self.shard_for(tid).lock().unwrap();
        st.local_tx.insert(
            tid.clone(),
            TxStatusEntry {
                status: TxCommitStatus::Pending,
                last_observation: None,
                refresh_state: RefreshState::NotStarted,
                locks: Vec::new(),
            },
        );
    }

    /// Records the lock set a transaction currently holds, so the refresher can
    /// stamp it onto the pending transaction object (ADR-022). Overwrites any
    /// previously recorded set with the latest acquire; a no-op if the
    /// transaction is no longer tracked (already finalized).
    pub(crate) fn record_tx_locks(&self, tid: &TxId, locks: Vec<TxLock>) {
        let mut st = self.shard_for(tid).lock().unwrap();
        if let Some(e) = st.local_tx.get_mut(tid) {
            e.locks = locks;
        }
    }

    /// Starts a background task that periodically refreshes the pending log so
    /// the transaction is not considered expired. The task is aborted when its
    /// [`Background`] is dropped.
    pub(crate) fn start_refresh_tx(&self, tid: &TxId) {
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

    /// Marks the transaction committed, writing the final transaction object
    /// (if it produced any writes or held any locks), updating local storage,
    /// and notifying waiters.
    pub(crate) async fn commit_tx(&self, mut tl: TxLog) -> Result<(), TransError> {
        self.stop_tx_refresh(&tl.id);

        // In v2 the transaction object is the value store: it must be persisted
        // whenever the transaction has writes (the committed values readers
        // help-forward) or recorded lock intentions. A read-only transaction
        // carries neither, so it skips the write entirely — its in-memory
        // bookkeeping is simply cleared below. This is the create-or-flip commit
        // point: `set_final_log` creates the committed object when no pending
        // one was written (the short-transaction case where the lazy refresh
        // never fired), or CASes pending -> committed otherwise.
        if !tl.locks.is_empty() || !tl.writes.is_empty() {
            tl.status = TxCommitStatus::Ok;
            // `context` preserves the `AlreadyFinalized` sentinel so the commit
            // path can recognize a wound (the log was already aborted out from
            // under us), as well as any classification of an escaping error.
            // In-doubt outcomes are normally retried inside `set_final_log`
            // because the log is keyed by tx id and the write is idempotent.
            self.set_final_log(&tl)
                .await
                .map_err(|e| e.context("writing tx log"))?;
        }

        let mut st = self.shard_for(&tl.id).lock().unwrap();
        st.local_tx.remove(&tl.id);
        notify_waiters(&mut st, &tl.id, TxCommitStatus::Ok);
        Ok(())
    }

    /// Marks the transaction aborted, writing the final log and notifying
    /// waiters. The local state is cleared even if writing the log fails.
    pub(crate) async fn abort_tx(&self, tid: &TxId) -> Result<(), TransError> {
        self.stop_tx_refresh(tid);

        let mut log = TxLog::new(tid.clone(), TxCommitStatus::Aborted);
        if let Some(entry) = self.shard_for(tid).lock().unwrap().local_tx.get(tid) {
            log.locks = entry.locks.clone();
        }
        let res = self.set_final_log(&log).await;

        let mut st = self.shard_for(tid).lock().unwrap();
        st.local_tx.remove(tid);
        notify_waiters(&mut st, tid, TxCommitStatus::Aborted);
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
    pub(crate) async fn wound_tx(&self, tid: &TxId) -> Result<(), TransError> {
        // TODO: this smells of TOCTOU
        let cs = self
            .inner
            .tl
            .commit_status_at(tid, self.current_requirement())
            .await
            .map_err(|e| {
                TransError::Storage(e.context(format!("reading status of wound target {tid}")))
            })?;
        if cs.status.is_final() {
            // Already committed or aborted: nothing left to wound.
            self.mark_local_aborted(tid, cs.status);
            return Ok(());
        }

        // Force the transaction to aborted, CAS-ing over its current log version
        // (or creating an aborted log if it has none yet).
        let status = self.force_abort(tid, &cs.observation).await?;
        self.mark_local_aborted(tid, status);
        Ok(())
    }

    /// Returns the commit status, checking locally first then remote storage.
    pub(crate) async fn tx_status(&self, tid: &TxId) -> Result<TxCommitStatus, TransError> {
        {
            let st = self.shard_for(tid).lock().unwrap();
            if let Some(e) = st.local_tx.get(tid) {
                return Ok(e.status);
            }
        }
        if let Some(status) = self.cached_final_status(tid) {
            return Ok(status);
        }
        self.fetch_remote_tx_status(tid).await
    }

    /// Returns the commit status using a caller-provided observation bound.
    pub(crate) async fn tx_status_at(
        &self,
        tid: &TxId,
        requirement: Requirement,
    ) -> Result<TxCommitStatus, TransError> {
        Ok(self.tx_status_at_with_cache(tid, requirement).await?.0)
    }

    /// Returns whether `tid` is committed using transaction-state evidence no
    /// older than `at`.
    pub(crate) async fn committed_at(
        &self,
        tid: &TxId,
        at: SequencePoint,
    ) -> Result<bool, TransError> {
        Ok(self.tx_status_at(tid, Requirement::AtLeast(at)).await? == TxCommitStatus::Ok)
    }

    /// Returns status at the requested bound and whether resolving it reused
    /// cached transaction state.
    pub(crate) async fn tx_status_at_with_cache(
        &self,
        tid: &TxId,
        requirement: Requirement,
    ) -> Result<(TxCommitStatus, bool), TransError> {
        {
            let st = self.shard_for(tid).lock().unwrap();
            if let Some(e) = st.local_tx.get(tid) {
                return Ok((e.status, true));
            }
        }
        if let Some(status) = self.cached_final_status(tid) {
            return Ok((status, true));
        }
        let status = self.inner.tl.commit_status_at(tid, requirement).await?;
        let cache_hit = status.observation.cache_hit();
        Ok((self.resolve_remote_tx_status(tid, status).await?, cache_hit))
    }

    /// Waits asynchronously for the transaction to finalize, yielding its final
    /// commit status. The returned future yields exactly one value; dropping it
    /// cancels the wait. If the sender is dropped without finalizing, it yields
    /// [`TxCommitStatus::Unknown`] (the default) so the caller re-resolves.
    pub(crate) fn wait_for_tx(
        &self,
        tid: &TxId,
    ) -> impl std::future::Future<Output = TxCommitStatus> + Send + use<> {
        let rx = self.wait_for_tx_rx(tid);
        async move { rx.await.unwrap_or_default() }
    }

    /// Returns the committed value a transaction wrote for `key`, reading from
    /// local storage or the transaction log.
    pub(crate) async fn committed_value(
        &self,
        key: &KeyRef,
        tid: &TxId,
    ) -> Result<KeyCommitStatus, TransError> {
        self.committed_value_with_requirement(key, tid, None).await
    }

    /// Returns a committed value using a caller-provided observation bound.
    pub(crate) async fn committed_value_at(
        &self,
        key: &KeyRef,
        tid: &TxId,
        requirement: Requirement,
    ) -> Result<KeyCommitStatus, TransError> {
        self.committed_value_with_requirement(key, tid, Some(requirement))
            .await
    }

    /// Writes a transaction's final (committed or aborted) log object with
    /// create-if-absent + CAS and in-doubt robustness (ADR-009).
    pub(crate) async fn set_final_log(&self, tlog: &TxLog) -> Result<(), TransError> {
        let tid = &tlog.id;
        if tid.is_unset() {
            return Err(TransError::other("missing required tlog ID"));
        }
        let mut last_observation = {
            let st = self.shard_for(tid).lock().unwrap();
            st.local_tx
                .get(tid)
                .and_then(|entry| entry.last_observation.clone())
        };

        let mut backoff = self.inner.retry.backoff();
        loop {
            let r = match &last_observation {
                Some(observed) => self.inner.tl.set_if(tlog, observed).await,
                None => self.inner.tl.set(tlog).await,
            };
            match r {
                Ok(observed) => {
                    self.remember_final(tid, &observed);
                    return Ok(());
                }
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
                    let st = self
                        .inner
                        .tl
                        .commit_status_at(tid, self.current_requirement())
                        .await?;
                    if st.status == tlog.status {
                        self.remember_final(tid, &st.observation);
                        return Ok(());
                    }
                    if st.status.is_final() {
                        return Err(TransError::AlreadyFinalized);
                    }
                    last_observation = Some(st.observation);
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

    /// Force-aborts a specific pending version of a transaction, the ADR-022 GC
    /// reclaim of a dead pending object. It is the *same* official sequence a
    /// contended lease expiry uses ([`Monitor::force_abort`]): CAS `pending →
    /// aborted` over `expected`. If a live owner committed or refreshed first
    /// the CAS loses and the now-durable status is reported instead, so GC
    /// never drops a lock out from under a still-live owner. A final expected
    /// observation is returned unchanged without issuing a mutation.
    pub(crate) async fn force_abort(
        &self,
        tid: &TxId,
        expected: &Observation<TxLog>,
    ) -> Result<TxCommitStatus, TransError> {
        if let Some(current) = expected.value() {
            match current.status {
                status @ (TxCommitStatus::Ok | TxCommitStatus::Aborted) => {
                    self.remember_final(tid, expected);
                    return Ok(status);
                }
                TxCommitStatus::Pending => {}
                TxCommitStatus::Unknown => {
                    return Err(TransError::other(format!(
                        "transaction {tid} has an invalid persisted status"
                    )));
                }
            }
        }

        let mut tlog = TxLog::new(tid.clone(), TxCommitStatus::Aborted);
        if let Some(current) = expected.value() {
            tlog.writes = current.writes.clone();
            tlog.locks = current.locks.clone();
        }
        let mut expected = expected.clone();
        let mut backoff = self.inner.retry.backoff();
        loop {
            let r = if expected.is_absent() {
                self.inner.tl.set(&tlog).await
            } else {
                self.inner.tl.set_if(&tlog, &expected).await
            };
            match r {
                Ok(observed) => {
                    self.remember_final(tid, &observed);
                    return Ok(TxCommitStatus::Aborted);
                }
                Err(StorageError::Precondition) => {
                    // The version moved under us (a commit, a pending-log
                    // refresh, or another wounder). Report whatever status is
                    // now durable.
                    let st = self
                        .inner
                        .tl
                        .commit_status_at(tid, self.current_requirement())
                        .await?;
                    self.remember_final(tid, &st.observation);
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
                    let st = self
                        .inner
                        .tl
                        .commit_status_at(tid, self.current_requirement())
                        .await?;
                    if st.status.is_final() {
                        self.remember_final(tid, &st.observation);
                        return Ok(st.status);
                    }
                    expected = st.observation;
                }
                Err(e) => return Err(e.into()),
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn committed_value_with_requirement(
        &self,
        key: &KeyRef,
        tid: &TxId,
        requirement: Option<Requirement>,
    ) -> Result<KeyCommitStatus, TransError> {
        let status_cache_hit = {
            let local = self
                .shard_for(tid)
                .lock()
                .unwrap()
                .local_tx
                .contains_key(tid);
            local
                || self
                    .inner
                    .final_status
                    .lock()
                    .unwrap()
                    .entries
                    .contains_key(tid)
        };
        let status = match requirement {
            Some(requirement) => self.tx_status_at(tid, requirement).await?,
            None => self.tx_status(tid).await?,
        };
        if status != TxCommitStatus::Ok {
            return Ok(KeyCommitStatus {
                status,
                value: TValue::default(),
                cache_hit: false,
            });
        }

        let tl = self.final_log(tid, status, requirement).await?;
        let cache_hit = status_cache_hit && tl.cache_hit();
        let tl = tl
            .value()
            .ok_or_else(|| TransError::other(format!("missing final log for {tid}")))?;
        for entry in &tl.writes {
            if &entry.key == key {
                return Ok(KeyCommitStatus {
                    status: TxCommitStatus::Ok,
                    value: TValue {
                        value: entry.value.clone(),
                        deleted: entry.deleted,
                        not_written: false,
                    },
                    cache_hit,
                });
            }
        }
        Ok(KeyCommitStatus {
            status: TxCommitStatus::Ok,
            value: TValue {
                not_written: true,
                ..Default::default()
            },
            cache_hit,
        })
    }

    async fn final_log(
        &self,
        tid: &TxId,
        expected_status: TxCommitStatus,
        requirement: Option<Requirement>,
    ) -> Result<Observation<TxLog>, TransError> {
        let cached = self.inner.final_status.lock().unwrap().get(tid);
        let mut observed = match requirement {
            Some(requirement) => {
                let requirement = match cached {
                    Some(status) => requirement.stricter(Requirement::AtLeast(status.watermark)),
                    None => requirement,
                };
                self.inner.tl.get_at(tid, requirement).await
            }
            None => self.inner.tl.get_at(tid, self.current_requirement()).await,
        }
        .map_err(|error| TransError::Storage(error.context(format!("getting TID {tid}"))))?;
        if observed
            .value()
            .is_some_and(|log| log.status != expected_status)
        {
            observed = self
                .inner
                .tl
                .get_at(tid, self.current_requirement())
                .await
                .map_err(|error| {
                    TransError::Storage(error.context(format!("refreshing TID {tid}")))
                })?;
        }
        if !observed
            .value()
            .is_some_and(|log| log.status == expected_status)
        {
            return Err(TransError::other(format!(
                "terminal status and transaction object disagree for {tid}"
            )));
        }
        Ok(observed)
    }

    /// Starts a transaction-log poll after all status evidence already seen by
    /// this monitor. Unlike transaction validation, a remote-holder poll has no
    /// preceding CAS or validation barrier to reuse, so the monitor must create
    /// the lower bound itself.
    fn current_requirement(&self) -> Requirement {
        Requirement::AtLeast(self.inner.timeline.now())
    }

    fn cached_final_status(&self, tid: &TxId) -> Option<TxCommitStatus> {
        self.inner
            .final_status
            .lock()
            .unwrap()
            .get(tid)
            .map(|entry| entry.status)
    }

    fn remember_final(&self, tid: &TxId, observed: &Observation<TxLog>) {
        let Some(log) = observed.value() else {
            return;
        };
        if !log.status.is_final() {
            return;
        }
        self.inner.final_status.lock().unwrap().insert(
            tid.clone(),
            FinalStatus {
                status: log.status,
                watermark: observed.current_after(),
            },
        );
    }

    /// Returns the shard lock responsible for `tid`.
    fn shard_for(&self, tid: &TxId) -> &Mutex<State> {
        self.inner.shards.for_key(tid.as_bytes())
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
        notify_waiters(&mut st, tid, TxCommitStatus::Aborted);
    }

    fn wait_for_tx_rx(&self, tid: &TxId) -> oneshot::Receiver<TxCommitStatus> {
        let (tx, rx) = oneshot::channel();

        let mut st = self.shard_for(tid).lock().unwrap();
        let entry = st.local_tx.get(tid);
        let is_local = entry.is_some();
        let status = entry.map(|e| e.status).unwrap_or(TxCommitStatus::Unknown);

        // Matches Go precedence: (isLocal && OK) || Aborted.
        if (is_local && status == TxCommitStatus::Ok) || status == TxCommitStatus::Aborted {
            let _ = tx.send(status);
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
            let status = m.poll_tx_status_with_liveness(&tid).await;
            let mut st = m.shard_for(&tid).lock().unwrap();
            notify_waiters(&mut st, &tid, status);
        });

        rx
    }

    async fn fetch_remote_tx_status(&self, tid: &TxId) -> Result<TxCommitStatus, TransError> {
        let status = self
            .inner
            .tl
            .commit_status_at(tid, self.current_requirement())
            .await?;
        self.resolve_remote_tx_status(tid, status).await
    }

    async fn resolve_remote_tx_status(
        &self,
        tid: &TxId,
        status: TxStatus,
    ) -> Result<TxCommitStatus, TransError> {
        if status.status == TxCommitStatus::Unknown {
            return self.handle_unknown_tx(tid).await;
        }
        self.resolve_present_tx_status(tid, status).await
    }

    async fn resolve_present_tx_status(
        &self,
        tid: &TxId,
        status: TxStatus,
    ) -> Result<TxCommitStatus, TransError> {
        match status.status {
            TxCommitStatus::Pending => {
                let now = self.inner.clock.now();
                // Absolute lease check (foreign clock — skew applies): a holder
                // whose last refresh is already ancient is reclaimed at once.
                // Observer-relative progress check (one clock — no skew): a
                // holder that stops bumping its lease `timestamp` while a waiter
                // watches it is dead within the configured pending timeout
                // (ADR-024).
                if self.inner.timing.is_expired(status.last_update, now)
                    || self.pending_no_progress(tid, status.last_update, now)
                {
                    self.clear_pending_progress(tid);
                    self.force_abort(tid, &status.observation).await
                } else {
                    Ok(TxCommitStatus::Pending)
                }
            }
            s @ (TxCommitStatus::Ok | TxCommitStatus::Aborted) => {
                // Finalized: drop the observer-relative progress tracking.
                self.clear_pending_progress(tid);
                self.remember_final(tid, &status.observation);
                Ok(s)
            }
            TxCommitStatus::Unknown => Err(TransError::other(format!(
                "transaction {tid} has an invalid persisted status"
            ))),
        }
    }

    /// Observer-relative no-progress check (ADR-024). Records the lease
    /// `timestamp` seen on the holder's pending object and when (observer clock)
    /// it was first seen at that value. Returns `true` only if the value has not
    /// advanced within the pending timeout of that first sight — comparing two
    /// values from the *same* observer clock, so no clock-skew allowance is owed.
    fn pending_no_progress(&self, tid: &TxId, last_update: SystemTime, now: SystemTime) -> bool {
        let mut st = self.shard_for(tid).lock().unwrap();
        match st.pending_progress.get_mut(tid) {
            None => {
                st.pending_progress.insert(
                    tid.clone(),
                    PendingProgress {
                        last_seen: last_update,
                        observed_at: now,
                    },
                );
                false
            }
            Some(p) => {
                if p.last_seen != last_update {
                    // The lease advanced: progress. Reset the window.
                    p.last_seen = last_update;
                    p.observed_at = now;
                    false
                } else {
                    self.inner.timing.is_expired_no_skew(p.observed_at, now)
                }
            }
        }
    }

    fn clear_pending_progress(&self, tid: &TxId) {
        self.shard_for(tid)
            .lock()
            .unwrap()
            .pending_progress
            .remove(tid);
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

        // Observer-relative grace: the object must *appear* within
        // the pending timeout of first sight. Both endpoints are the observer's
        // own clock, so this owes no skew allowance (ADR-024 refines ADR-021,
        // which over-granted this window by reusing the skew-padded check).
        if self.inner.timing.is_expired_no_skew(first_check, now) {
            let status = self
                .inner
                .tl
                .commit_status_at(tid, self.current_requirement())
                .await?;
            let res = match status.status {
                TxCommitStatus::Unknown => self.force_abort(tid, &status.observation).await,
                // Appearance is progress. Re-enter ordinary status resolution
                // so a fresh pending lease remains live and a final object can
                // never be used as the expected side of an abort CAS.
                _ => self.resolve_present_tx_status(tid, status).await,
            };
            if res.is_ok() {
                self.shard_for(tid).lock().unwrap().unknown_tx.remove(tid);
            }
            return res;
        }
        Ok(TxCommitStatus::Pending)
    }

    /// Polls the remote tx status until it finalizes, a fetch fails, or every
    /// caller has dropped its `wait_for_tx` future (signalled by closed
    /// `oneshot::Sender`s in the waiters list). The latter is the future-drop
    /// equivalent of the per-call cancellation contexts the Go original used.
    ///
    /// Returns the last status seen: the final status on success, or
    /// [`TxCommitStatus::Unknown`] / the last pending status on a fetch error or
    /// abandoned poll. A waiter woken with a non-final status re-resolves the
    /// holder (re-issuing `tx_status`), so a transient fetch error is retried and
    /// a persistent one resurfaces there — the poll itself reports no error.
    async fn poll_tx_status_with_liveness(&self, tid: &TxId) -> TxCommitStatus {
        let mut backoff = self.inner.retry.backoff();
        loop {
            let s = match self.fetch_remote_tx_status(tid).await {
                Err(_) => return TxCommitStatus::Unknown,
                Ok(s) => s,
            };
            if s.is_final() {
                return s;
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
                return s;
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

    /// Background lease refresher (ADR-021/ADR-024). Under hold-and-wait a live
    /// transaction can block while holding locks for far longer than the
    /// configured pending timeout, so this loop keeps its lease fresh until the
    /// transaction commits or aborts (`should_refresh` flips). It is
    /// load-bearing: its **first** write *creates* the pending transaction
    /// object with create-if-absent semantics, materializing the lazily-created
    /// object (ADR-024); thereafter it CAS-bumps the `timestamp` over the
    /// object's version halfway through each pending interval.
    ///
    /// Create-if-absent is what keeps lazy materialization wound-safe: if an
    /// older peer already wounded this transaction (wrote an `aborted` object)
    /// before it materialized its own pending one, the create loses, the
    /// refresher observes the final status, stops, and the owner's commit fails
    /// — it can never resurrect itself over a wound. A later refresh CAS that
    /// finds the object `aborted` is the same wound signal once materialized.
    /// Transient backend failures (in-doubt, unavailable) are retried rather
    /// than abandoning the lease, since re-applying a pending refresh is
    /// idempotent and convergent (ADR-009).
    async fn refresh_pending(&self, tid: TxId) {
        if !self.should_refresh(&tid) {
            return;
        }
        let mut last_observation: Option<Observation<TxLog>> = None;

        loop {
            rt::sleep(self.inner.timing.refresh_interval()).await;
            if !self.should_refresh(&tid) {
                return;
            }

            let start = self.inner.clock.now();
            let mut tl = TxLog::new(tid.clone(), TxCommitStatus::Pending);
            tl.timestamp = Some(start);
            // Stamp the currently-held lock set (read synchronously before the
            // write) so the materialized pending object records its own
            // back-references for GC (ADR-022).
            tl.locks = self
                .shard_for(&tid)
                .lock()
                .unwrap()
                .local_tx
                .get(&tid)
                .map(|e| e.locks.clone())
                .unwrap_or_default();
            let r = if let Some(observed) = &last_observation {
                self.inner.tl.set_if(&tl, observed).await
            } else {
                // First materialization: create-if-absent so a pre-existing
                // `aborted` object (an older peer's wound) wins.
                self.inner.tl.set(&tl).await
            };
            match r {
                Ok(observed) => {
                    last_observation = Some(observed);
                    let mut st = self.shard_for(&tid).lock().unwrap();
                    if let Some(e) = st.local_tx.get_mut(&tid) {
                        e.last_observation = last_observation.clone();
                    }
                }
                // The create lost (object already exists) or the CAS version
                // moved under us. Re-read: a final status is a wound (or a race
                // we lost) — stop and let the owner observe it; a still-pending
                // status means we adopt its version and keep refreshing.
                Err(StorageError::Precondition) => {
                    match self
                        .inner
                        .tl
                        .commit_status_at(&tid, self.current_requirement())
                        .await
                    {
                        Ok(st) if st.status.is_final() => {
                            self.mark_local_aborted(&tid, st.status);
                            return;
                        }
                        Ok(st) => last_observation = Some(st.observation),
                        // Couldn't read it back; retry on the next cycle.
                        Err(_) => {}
                    }
                }
                // In-doubt or other transient failures: keep the lease alive by
                // retrying on the next cycle rather than abandoning a live
                // holder's locks to false reclamation.
                Err(_) => {}
            }
        }
    }
}

fn notify_waiters(st: &mut State, tid: &TxId, status: TxCommitStatus) {
    if let Some(ws) = st.waiters.remove(tid) {
        for w in ws {
            // `send` silently fails if the receiver has been dropped,
            // which is the new "waiter cancelled" signal.
            let _ = w.tx.send(status);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_backend::middleware::RecordingBackend;
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_data::CollectionPath;
    use glassdb_storage::{CachedStore, LockType, Timeline, TxWrite};

    #[test]
    fn protocol_timing_profiles_preserve_liveness_boundaries() {
        let production = ProtocolTiming::default();
        assert_eq!(production.pending_timeout(), Duration::from_secs(15));
        assert_eq!(production.max_clock_skew(), Duration::from_secs(30));
        assert_eq!(production.refresh_interval(), Duration::from_millis(7_500));

        let simulation = ProtocolTiming::simulation();
        assert_eq!(simulation.pending_timeout(), Duration::from_millis(250));
        assert_eq!(simulation.max_clock_skew(), Duration::from_millis(500));
        assert_eq!(simulation.refresh_interval(), Duration::from_millis(125));

        let refreshed = SystemTime::UNIX_EPOCH;
        let boundary = refreshed + simulation.pending_timeout() + simulation.max_clock_skew();
        assert!(!simulation.is_expired(refreshed, boundary));
        assert!(simulation.is_expired(refreshed, boundary + Duration::from_nanos(1)));
    }

    fn key_ref(key: &[u8]) -> KeyRef {
        KeyRef::new(CollectionPath::new("test", b"collection"), key)
    }

    struct TestCtx {
        tl: TLogger,
        // The clock the monitor was built with, so tests can stamp tx logs with
        // the monitor's own notion of "now".
        clock: Clock,
        // The strong `Arc<Background>` lives here so refresh tasks can be
        // spawned for the duration of the test; the `Monitor` only stores a
        // `Weak`.
        _bg: Arc<Background>,
    }

    fn new_test_monitor(b: Arc<dyn Backend>) -> (Monitor, TestCtx) {
        new_test_monitor_clock(b, Clock::real())
    }

    fn new_test_monitor_clock(b: Arc<dyn Backend>, clock: Clock) -> (Monitor, TestCtx) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(b, 1024, timeline.clone());
        let tl = TLogger::new(objects.clone(), "test");
        let bg = Arc::new(Background::new());
        let mon = Monitor::with_config(
            tl.clone(),
            timeline,
            Arc::downgrade(&bg),
            clock.clone(),
            RetryConfig::default(),
            ProtocolTiming::default(),
        );
        (mon, TestCtx { tl, clock, _bg: bg })
    }

    #[test]
    fn final_status_cache_is_count_bounded_and_lru() {
        let timeline = Timeline::new();
        let watermark = timeline.now();
        let mut cache = FinalStatusCache::new(2);
        let first = TxId::from_bytes(b"first".to_vec());
        let second = TxId::from_bytes(b"second".to_vec());
        let third = TxId::from_bytes(b"third".to_vec());
        let status = FinalStatus {
            status: TxCommitStatus::Ok,
            watermark,
        };

        cache.insert(first.clone(), status);
        cache.insert(second.clone(), status);
        assert!(cache.get(&first).is_some());
        cache.insert(third.clone(), status);

        assert!(cache.get(&second).is_none());
        assert!(cache.get(&first).is_some());
        assert!(cache.get(&third).is_some());
    }

    #[tokio::test]
    async fn status() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, _t1) = new_test_monitor(b.clone());
        let (mon2, _t2) = new_test_monitor(b.clone());
        let key = key_ref(b"key1");
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
        tl.locks = vec![TxLock::Entry {
            key,
            typ: LockType::Write,
        }];
        mon1.commit_tx(tl).await.unwrap();
        assert_eq!(mon1.tx_status(&tx).await.unwrap(), TxCommitStatus::Ok);
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Ok);
    }

    #[tokio::test]
    async fn committed_value() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, _t1) = new_test_monitor(b.clone());
        let (mon2, _t2) = new_test_monitor(b.clone());
        let key = key_ref(b"key");

        let tx = TxId::from_bytes(b"tx2".to_vec());
        mon1.begin_tx(&tx);
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        tl.writes = vec![TxWriteForTest::w(&key, b"val1")];
        tl.locks = vec![TxLock::Entry {
            key: key.clone(),
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
        let key2 = key_ref(b"key2");
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
        assert_eq!(ch1.await, TxCommitStatus::Aborted);
        assert_eq!(ch2.await, TxCommitStatus::Aborted);
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

        assert_eq!(ch2.await, TxCommitStatus::Aborted);
        assert_eq!(ch3.await, TxCommitStatus::Aborted);
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

        let st = t.tl.commit_status_at(&tx, Requirement::Any).await.unwrap();
        assert_eq!(st.status, TxCommitStatus::Pending);

        // A separate monitor should still see it as pending (not expired).
        let (mon2, _t2) = new_test_monitor_clock(b, Clock::anchored());
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);

        mon.abort_tx(&tx).await.unwrap();
    }

    // Regression (review 1.1 / ADR-022): the lazily-materialized pending object
    // the refresher writes must carry the transaction's recorded lock set, so a
    // dead pending transaction still describes its own back-references for GC to
    // prune. Recording locks before the refresher fires must land on the object.
    #[tokio::test(start_paused = true)]
    async fn refresh_records_locks() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor_clock(b.clone(), Clock::anchored());
        let tx = TxId::from_bytes(b"tx1".to_vec());
        let locks = vec![TxLock::Entry {
            key: key_ref(b"k"),
            typ: LockType::Write,
        }];
        mon.begin_tx(&tx);
        mon.record_tx_locks(&tx, locks.clone());
        mon.start_refresh_tx(&tx);

        // Advance past the refresh interval so the refresher materializes the
        // pending object.
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

        let tl = t.tl.get_at(&tx, Requirement::Any).await.unwrap();
        let tl = tl.value().unwrap();
        assert_eq!(tl.status, TxCommitStatus::Pending);
        assert_eq!(tl.locks, locks);

        mon.abort_tx(&tx).await.unwrap();
    }

    // ADR-024: a peer that repeatedly polls a *live* holder over a span far
    // beyond the pending timeout never reclaims it, because the refresher bumps
    // the lease timestamp halfway through each interval, so the observer always
    // sees progress (neither the absolute lease nor the relative no-progress
    // check fires).
    #[tokio::test(start_paused = true)]
    async fn live_holder_not_reclaimed_across_long_wait() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, _t) = new_test_monitor_clock(b.clone(), Clock::anchored());
        let (observer, _o) = new_test_monitor_clock(b.clone(), Clock::anchored());
        let tx = TxId::from_bytes(b"live".to_vec());
        mon.begin_tx(&tx);
        mon.start_refresh_tx(&tx);

        // 50s total, far past the 15s timeout, polled every 5s.
        for _ in 0..10 {
            tokio::time::sleep(Duration::from_secs(5)).await;
            assert_eq!(
                observer.tx_status(&tx).await.unwrap(),
                TxCommitStatus::Pending
            );
        }

        mon.abort_tx(&tx).await.unwrap();
        assert_eq!(
            observer.tx_status(&tx).await.unwrap(),
            TxCommitStatus::Aborted
        );
    }

    // ADR-024: a crashed holder whose pending object exists but stops being
    // refreshed is reclaimed within the pending timeout by the observer-relative
    // no-progress check — even though its absolute (skew-padded) lease is nowhere
    // near expiry — once a watcher has seen it make no progress for that long.
    #[tokio::test(start_paused = true)]
    async fn dead_holder_reclaimed_by_relative_progress() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor_clock(b.clone(), Clock::anchored());
        let tx = TxId::from_bytes(b"dead".to_vec());

        // A pending object stamped "now" that never refreshes (a crashed
        // holder). Its absolute lease includes both the pending timeout and
        // skew allowance, so only the relative check can reclaim it sooner.
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Pending);
        tl.timestamp = Some(t.clock.now());
        t.tl.set(&tl).await.unwrap();

        // First sight records the progress baseline; still pending.
        assert_eq!(mon.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);

        // No progress for longer than the timeout on the observer's own clock.
        tokio::time::sleep(mon.protocol_timing().pending_timeout() + Duration::from_secs(1)).await;

        // The stalled holder is reclaimed (aborted), well before its absolute
        // lease would have expired.
        assert_eq!(mon.tx_status(&tx).await.unwrap(), TxCommitStatus::Aborted);
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_recheck_preserves_a_concurrent_commit() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (observer, _o) = new_test_monitor_clock(b.clone(), Clock::anchored());
        let (_owner, owner) = new_test_monitor_clock(b.clone(), Clock::anchored());
        let tx = TxId::from_bytes(b"committed-during-unknown-grace".to_vec());

        assert_eq!(
            observer.tx_status(&tx).await.unwrap(),
            TxCommitStatus::Pending
        );
        tokio::time::sleep(observer.protocol_timing().pending_timeout() + Duration::from_secs(1))
            .await;

        owner
            .tl
            .set(&TxLog::new(tx.clone(), TxCommitStatus::Ok))
            .await
            .unwrap();

        assert_eq!(
            observer.handle_unknown_tx(&tx).await.unwrap(),
            TxCommitStatus::Ok
        );
        let (_verify, verify) = new_test_monitor(b);
        assert_eq!(
            verify
                .tl
                .commit_status_at(&tx, Requirement::Any)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Ok
        );
    }

    #[tokio::test]
    async fn force_abort_preserves_a_final_observation() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let b: Arc<dyn Backend> = Arc::new(backend);
        let (mon, t) = new_test_monitor(b.clone());
        let tx = TxId::from_bytes(b"already-committed".to_vec());
        let committed =
            t.tl.set(&TxLog::new(tx.clone(), TxCommitStatus::Ok))
                .await
                .unwrap();
        operations.lock().unwrap().clear();

        assert_eq!(
            mon.force_abort(&tx, &committed).await.unwrap(),
            TxCommitStatus::Ok
        );
        assert!(
            operations.lock().unwrap().is_empty(),
            "the final-observation fast path must issue no backend operation"
        );
        let (_verify, verify) = new_test_monitor(b);
        assert_eq!(
            verify
                .tl
                .commit_status_at(&tx, Requirement::Any)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Ok
        );
    }

    // Tiny helper to build a TxWrite in tests.
    struct TxWriteForTest;
    impl TxWriteForTest {
        fn w(key: &KeyRef, value: &[u8]) -> TxWrite {
            TxWrite {
                key: key.clone(),
                value: Arc::from(value),
                deleted: false,
                prev_writer: TxId::default(),
            }
        }
    }
}
