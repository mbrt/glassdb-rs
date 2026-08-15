//! Transaction lifecycle monitor. Ported from the Go `internal/trans/monitor.go`.
//!
//! Tracks local and remote transaction state, refreshes pending logs to keep
//! locks alive, aborts expired remote transactions, and lets callers wait for a
//! transaction to finalize.

use std::collections::{HashMap, hash_map::Entry};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use glassdb_concurr::{Background, Backoff, RetryConfig, rt, shard::Sharded};
use glassdb_data::{CollectionAddress, KeyRef, TxId};
use glassdb_storage::transaction::{
    TLogger, TxCollectionChange, TxCommitStatus, TxLifecycleRelation, TxLock, TxLog, TxRecordState,
    TxStatus,
};
use glassdb_storage::{Observation, Requirement, SequencePoint, StorageError, Timeline};
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
    recovery: TxRecoveryManifest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum OwnerOperations {
    #[default]
    Internal,
    Guarded {
        active: usize,
        unresolved: bool,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum OwnerAdmission {
    #[default]
    Open,
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum TerminalCommit {
    #[default]
    NotStarted,
    Started,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct OwnerLifecycle {
    operations: OwnerOperations,
    admission: OwnerAdmission,
    terminal_commit: TerminalCommit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerCloseReason {
    Preemption,
    OwnerAbort,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerRecordState {
    NotEngaged,
    Wounded,
    Other,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerRetirementProof {
    Internal,
    Guarded,
    Unavailable,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OwnerClosePlan {
    Transition(AbortTransition),
    PreserveCommit,
    AlreadyFinished,
}

impl OwnerLifecycle {
    fn begin_operation(&mut self) -> Result<(), TransError> {
        if self.admission == OwnerAdmission::Closed {
            return Err(TransError::Wounded);
        }
        match &mut self.operations {
            OwnerOperations::Internal => {
                self.operations = OwnerOperations::Guarded {
                    active: 1,
                    unresolved: false,
                };
            }
            OwnerOperations::Guarded { active, .. } => *active += 1,
        }
        Ok(())
    }

    fn finish_operation(&mut self, unresolved: bool) {
        let OwnerOperations::Guarded {
            active,
            unresolved: has_unresolved,
        } = &mut self.operations
        else {
            return;
        };
        debug_assert!(*active > 0, "owner operation finished more than once");
        *active = active.saturating_sub(1);
        *has_unresolved |= unresolved;
    }

    fn start_terminal_commit(&mut self) -> Result<(), TransError> {
        if self.admission == OwnerAdmission::Closed {
            return Err(TransError::AlreadyFinalized);
        }
        self.terminal_commit = TerminalCommit::Started;
        Ok(())
    }

    fn close(&mut self, reason: OwnerCloseReason, record: OwnerRecordState) -> OwnerClosePlan {
        self.admission = OwnerAdmission::Closed;
        if reason == OwnerCloseReason::OwnerAbort && record == OwnerRecordState::NotEngaged {
            return OwnerClosePlan::AlreadyFinished;
        }

        let retirement = match self.operations {
            OwnerOperations::Internal => OwnerRetirementProof::Internal,
            OwnerOperations::Guarded {
                active: 0,
                unresolved: false,
            } => OwnerRetirementProof::Guarded,
            OwnerOperations::Guarded { .. } => OwnerRetirementProof::Unavailable,
        };
        let terminal_commit_started = self.terminal_commit == TerminalCommit::Started;

        match reason {
            OwnerCloseReason::Preemption => {
                if record != OwnerRecordState::NotEngaged
                    && retirement == OwnerRetirementProof::Guarded
                    && (!terminal_commit_started || record == OwnerRecordState::Wounded)
                {
                    OwnerClosePlan::Transition(AbortTransition::Acknowledge)
                } else {
                    OwnerClosePlan::Transition(AbortTransition::EnsureWounded)
                }
            }
            OwnerCloseReason::OwnerAbort
                if terminal_commit_started && record != OwnerRecordState::Wounded =>
            {
                if retirement != OwnerRetirementProof::Unavailable {
                    OwnerClosePlan::PreserveCommit
                } else {
                    OwnerClosePlan::Transition(AbortTransition::WoundIfPresent)
                }
            }
            OwnerCloseReason::OwnerAbort => {
                if retirement != OwnerRetirementProof::Unavailable {
                    OwnerClosePlan::Transition(AbortTransition::Acknowledge)
                } else {
                    OwnerClosePlan::Transition(AbortTransition::EnsureWounded)
                }
            }
        }
    }

    fn has_active_operations(&self) -> bool {
        matches!(
            self.operations,
            OwnerOperations::Guarded { active, .. } if active > 0
        )
    }
}

struct OwnedTxRuntime {
    record: Option<TxStatusEntry>,
    lifecycle: OwnerLifecycle,
}

impl OwnedTxRuntime {
    fn new(record: Option<TxStatusEntry>) -> Self {
        Self {
            record,
            lifecycle: OwnerLifecycle::default(),
        }
    }

    fn close(&mut self, reason: OwnerCloseReason) -> OwnerClosePlan {
        let record = match self.record.as_mut() {
            Some(record) => {
                record.refresh_state = RefreshState::Stopped;
                if record.status == TxCommitStatus::Wounded {
                    OwnerRecordState::Wounded
                } else {
                    OwnerRecordState::Other
                }
            }
            None => OwnerRecordState::NotEngaged,
        };
        self.lifecycle.close(reason, record)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum RemoteLiveness {
    #[default]
    Unobserved,
    MissingSince {
        since: SystemTime,
    },
    PendingUnchanged {
        last_refresh: SystemTime,
        since: SystemTime,
    },
}

enum RemoteLivenessDecision {
    Live,
    Expired,
    Owned(TxStatusEvidence),
}

#[derive(Default)]
struct ForeignTxRuntime {
    liveness: RemoteLiveness,
}

enum TxRuntimeRole {
    Owned(OwnedTxRuntime),
    Foreign(ForeignTxRuntime),
}

struct TxRuntimeEntry {
    role: TxRuntimeRole,
    waiters: Vec<oneshot::Sender<()>>,
}

impl TxRuntimeEntry {
    fn owned(record: Option<TxStatusEntry>) -> Self {
        Self {
            role: TxRuntimeRole::Owned(OwnedTxRuntime::new(record)),
            waiters: Vec::new(),
        }
    }

    fn foreign() -> Self {
        Self {
            role: TxRuntimeRole::Foreign(ForeignTxRuntime::default()),
            waiters: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbortTransition {
    Acknowledge,
    EnsureWounded,
    WoundIfPresent,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum AbortObservationAction {
    Settled,
    Write(TxCommitStatus),
}

#[derive(Clone, Copy)]
enum CommitWriteFailure {
    Conflict,
    Ambiguous { started_at: rt::Instant },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitResolution {
    Committed,
    Retry,
    AlreadyFinalized,
    InDoubt,
}

struct PendingWrite {
    log: TxLog,
    expected: Option<Observation<TxLog>>,
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

#[derive(Default)]
struct State {
    transactions: HashMap<TxId, TxRuntimeEntry>,
}

struct Inner {
    tl: TLogger,
    timeline: Timeline,
    final_status: Mutex<FinalStatusCache>,
    // Weak so a `Monitor` clone captured inside a spawned task does not keep
    // the [`Background`] alive across DB shutdown. The single strong owner
    // is `DbInner::background`.
    background: Weak<Background>,
    retry: RetryConfig,
    timing: ProtocolTiming,
    // Runtime entries are partitioned into independent shards keyed by tid.
    // One lock keeps ownership, liveness, and waiter updates atomic for a given
    // transaction while the role enum prevents owned and foreign state from
    // coexisting.
    shards: Sharded<Mutex<State>>,
}

/// Tracks the lifecycle of transactions: commit, abort, status queries, and
/// asynchronous waits.
#[derive(Clone)]
pub struct Monitor {
    inner: Arc<Inner>,
}

/// Tracks one owner-side protocol execution so a concurrent local wound can
/// distinguish an acknowledged retirement from work that may still publish.
pub(crate) struct OwnerOperation {
    monitor: Monitor,
    tid: TxId,
    completed: bool,
}

impl OwnerOperation {
    /// Records that the owner operation returned normally, including with a
    /// classified error, so it cannot publish anything after this point.
    pub(crate) fn complete(mut self) {
        self.monitor.finish_owner_operation(&self.tid, false);
        self.completed = true;
    }
}

impl Drop for OwnerOperation {
    fn drop(&mut self) {
        if !self.completed {
            self.monitor.finish_owner_operation(&self.tid, true);
        }
    }
}

/// Outcome of aborting a transaction from its owning Database.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum OwnerAbortOutcome {
    /// The abort is acknowledged and eligible for ordinary GC.
    Acknowledged,
    /// The abort is terminal but pinned because owner work remains unresolved.
    Pinned,
    /// The transaction committed before owner-side closure won.
    Committed,
    /// A terminal commit was dispatched, so cleanup must preserve its outcome.
    CommitOutcomePreserved,
    /// The Monitor had already stopped tracking the transaction.
    AlreadyFinished,
}

/// Durable backreferences needed to recover a pending transaction after its
/// owner disappears.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TxRecoveryManifest {
    pub(crate) locks: Vec<TxLock>,
    pub(crate) collection_changes: Vec<TxCollectionChange>,
    pub(crate) prepared_collections: Vec<CollectionAddress>,
}

impl TxRecoveryManifest {
    /// Extracts the durable recovery backreferences from a transaction log.
    pub(crate) fn from_log(log: &TxLog) -> Self {
        Self {
            locks: log.locks.clone(),
            collection_changes: log.collection_changes.clone(),
            prepared_collections: log.prepared_collections.clone(),
        }
    }

    /// Applies the durable recovery backreferences to a transaction log.
    pub(crate) fn apply_to(self, log: &mut TxLog) {
        log.locks = self.locks;
        log.collection_changes = self.collection_changes;
        log.prepared_collections = self.prepared_collections;
    }
}

/// A transaction status known to be durable and terminal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum TxFinalStatus {
    Committed,
    Aborted,
}

/// A value written by a transaction, including whether it was deleted or absent.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TValue {
    pub value: Arc<[u8]>,
    pub deleted: bool,
    /// True when the transaction committed without writing this value.
    pub not_written: bool,
}

/// A transaction's commit status for a specific key, plus the value written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct KeyCommitStatus {
    pub status: TxCommitStatus,
    pub value: TValue,
    pub cache_hit: bool,
}

/// Transaction status together with the exact evidence used to resolve it.
struct TxStatusEvidence {
    state: TxRecordState,
    observation: Option<Observation<TxLog>>,
    cache_hit: bool,
}

impl TxStatusEvidence {
    fn local(record: Option<&TxStatusEntry>) -> Result<Self, TransError> {
        match record {
            Some(record) => Ok(Self {
                state: TxRecordState::try_from_status(Some(record.status))?,
                observation: record.last_observation.clone(),
                cache_hit: true,
            }),
            // An owner operation may precede entry into the logged protocol.
            // Like a freshly observed absent foreign record, it is not yet
            // terminal and is exposed as pending.
            None => Ok(Self {
                state: TxRecordState::Missing,
                observation: None,
                cache_hit: true,
            }),
        }
    }

    fn observed(status: TxStatus) -> Result<Self, TransError> {
        let state = TxRecordState::try_from_observation(&status.observation)?;
        let cache_hit = status.observation.cache_hit();
        Ok(Self {
            state,
            observation: Some(status.observation),
            cache_hit,
        })
    }

    fn cached_final(status: TxCommitStatus) -> Result<Self, TransError> {
        Ok(Self {
            state: TxRecordState::try_from_status(Some(status))?,
            observation: None,
            cache_hit: true,
        })
    }

    fn status(&self) -> TxCommitStatus {
        match self.state {
            // Missing is exposed as pending only during the observer-relative
            // appearance grace. Once the grace expires, resolution first pins
            // the identity as wounded and returns that exact observation.
            TxRecordState::Missing | TxRecordState::Pending => TxCommitStatus::Pending,
            TxRecordState::Wounded => TxCommitStatus::Wounded,
            TxRecordState::Committed => TxCommitStatus::Ok,
            TxRecordState::Aborted => TxCommitStatus::Aborted,
        }
    }
}

impl Monitor {
    /// Creates a monitor with retry-backoff and transaction-liveness timing.
    /// The retry config tunes the backoff used when polling a peer
    /// transaction's commit status and when writing a transaction's final log.
    pub fn with_config(
        tl: TLogger,
        timeline: Timeline,
        background: Weak<Background>,
        retry: RetryConfig,
        timing: ProtocolTiming,
    ) -> Self {
        Monitor {
            inner: Arc::new(Inner {
                tl,
                timeline,
                final_status: Mutex::new(FinalStatusCache::new(FINAL_STATUS_CACHE_SIZE)),
                background,
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
        self.register_tx(tid, TxRecoveryManifest::default());
    }

    /// Registers a transaction and makes its recovery manifest durable before
    /// returning.
    pub(crate) async fn begin_persisted_tx(
        &self,
        tid: &TxId,
        recovery: TxRecoveryManifest,
    ) -> Result<(), TransError> {
        self.register_tx(tid, recovery);
        self.persist_pending_tx(tid).await
    }

    /// Updates a pending transaction's recovery manifest and makes the result
    /// durable before returning.
    pub(crate) async fn update_pending_tx(
        &self,
        tid: &TxId,
        update: impl FnOnce(&mut TxRecoveryManifest) + Send,
    ) -> Result<(), TransError> {
        {
            let mut st = self.shard_for(tid).lock().unwrap();
            let entry = st
                .transactions
                .get_mut(tid)
                .ok_or_else(|| TransError::other("pending transaction was not begun"))?;
            let TxRuntimeRole::Owned(owned) = &mut entry.role else {
                return Err(TransError::other(
                    "pending transaction is tracked as foreign",
                ));
            };
            let record = owned
                .record
                .as_mut()
                .ok_or_else(|| TransError::other("pending transaction was not begun"))?;
            update(&mut record.recovery);
        }
        self.persist_pending_tx(tid).await
    }

    /// Opens one owner-side protocol execution for `tid`.
    ///
    /// The guard is deliberately registered before the transaction publishes a
    /// holder. If the future is dropped, its guard records an unresolved
    /// operation and any later wound must remain pinned.
    pub(crate) fn begin_owner_operation(&self, tid: &TxId) -> Result<OwnerOperation, TransError> {
        let mut st = self.shard_for(tid).lock().unwrap();
        let owned = match st.transactions.entry(tid.clone()) {
            Entry::Vacant(entry) => {
                let entry = entry.insert(TxRuntimeEntry::owned(None));
                let TxRuntimeRole::Owned(owned) = &mut entry.role else {
                    unreachable!();
                };
                owned
            }
            Entry::Occupied(entry) => match &mut entry.into_mut().role {
                TxRuntimeRole::Owned(owned) => owned,
                TxRuntimeRole::Foreign(_) => {
                    return Err(TransError::other(
                        "transaction identity is already tracked as foreign",
                    ));
                }
            },
        };
        owned.lifecycle.begin_operation()?;
        Ok(OwnerOperation {
            monitor: self.clone(),
            tid: tid.clone(),
            completed: false,
        })
    }

    /// Whether this client still tracks `tid` as one of its logged identities.
    /// A wounded identity remains tracked until its owner acknowledges it or
    /// cancellation recovery releases local ownership. Transactions that never
    /// engage the logged protocol are not tracked.
    pub(crate) fn is_tracked_local(&self, tid: &TxId) -> bool {
        self.shard_for(tid)
            .lock()
            .unwrap()
            .transactions
            .get(tid)
            .is_some_and(|entry| {
                matches!(&entry.role, TxRuntimeRole::Owned(owned) if owned.record.is_some())
            })
    }

    /// Records the lock set a transaction currently holds, so the refresher can
    /// stamp it onto the pending transaction object (ADR-022). Overwrites any
    /// previously recorded set with the latest acquire; a no-op if the
    /// transaction is no longer tracked (already finalized).
    pub(crate) fn record_tx_locks(&self, tid: &TxId, locks: Vec<TxLock>) {
        let mut st = self.shard_for(tid).lock().unwrap();
        if let Some(TxRuntimeEntry {
            role: TxRuntimeRole::Owned(owned),
            ..
        }) = st.transactions.get_mut(tid)
            && let Some(record) = owned.record.as_mut()
        {
            record.recovery.locks = locks;
        }
    }

    /// Starts a background task that periodically refreshes the pending log so
    /// the transaction is not considered expired. The task is aborted when its
    /// [`Background`] is dropped.
    pub(crate) fn start_refresh_tx(&self, tid: &TxId) {
        let need_start = {
            let mut st = self.shard_for(tid).lock().unwrap();
            match st.transactions.get_mut(tid) {
                Some(TxRuntimeEntry {
                    role: TxRuntimeRole::Owned(owned),
                    ..
                }) if owned.lifecycle.admission == OwnerAdmission::Open => {
                    match owned.record.as_mut() {
                        Some(record)
                            if record.status == TxCommitStatus::Pending
                                && record.refresh_state == RefreshState::NotStarted =>
                        {
                            record.refresh_state = RefreshState::Running;
                            true
                        }
                        _ => false,
                    }
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
    pub(crate) async fn commit_tx(&self, tl: TxLog) -> Result<(), TransError> {
        let tid = tl.id.clone();
        // In v2 the transaction object is the value store: it must be persisted
        // whenever the transaction has writes (the committed values readers
        // help-forward) or recorded lock intentions. A read-only transaction
        // carries neither, so it skips the write entirely — its in-memory
        // bookkeeping is simply cleared below. This is the create-or-flip commit
        // point: `persist_committed_log` creates the committed object when no
        // pending one was written (the short-transaction case where the lazy
        // refresh never fired), or CASes pending -> committed otherwise.
        if !tl.locks.is_empty()
            || !tl.writes.is_empty()
            || !tl.collection_changes.is_empty()
            || !tl.prepared_collections.is_empty()
        {
            // This handshake shares the owner-state lock with local wounding.
            // Either the wound closes admission first, or the commit is marked
            // as dispatched before a local task can claim safe retirement.
            self.start_terminal_commit(&tid)?;
            self.stop_tx_refresh(&tid);
            // `context` preserves the `AlreadyFinalized` sentinel so the commit
            // path can recognize an abort-side terminal winner, as well as any
            // classification of an escaping error.
            // In-doubt outcomes are normally retried inside
            // `persist_committed_log` because the log is keyed by tx id and
            // the write is idempotent.
            self.persist_committed_log(tl)
                .await
                .map_err(|error| error.context("writing tx log"))?;
        }

        self.finish_local_tx(&tid);
        Ok(())
    }

    /// Aborts a transaction owned by this Database, deriving the strongest safe
    /// durable state from tracked owner activity.
    ///
    /// Quiescent work may acknowledge `Aborted`; a dropped operation remains
    /// pinned as `Wounded`. An unresolved terminal commit preserves ADR-057
    /// ambiguity instead of manufacturing an abort-side object.
    pub(crate) async fn abort_owned_tx(&self, tid: &TxId) -> Result<OwnerAbortOutcome, TransError> {
        let transition = match self.owner_close_plan(tid) {
            OwnerClosePlan::Transition(transition) => transition,
            OwnerClosePlan::PreserveCommit => {
                return Ok(OwnerAbortOutcome::CommitOutcomePreserved);
            }
            OwnerClosePlan::AlreadyFinished => return Ok(OwnerAbortOutcome::AlreadyFinished),
        };
        let mut status = self
            .inner
            .tl
            .commit_status_at(tid, self.current_requirement())
            .await?;
        let mut backoff = self.inner.retry.backoff();
        loop {
            if transition == AbortTransition::WoundIfPresent && status.observation.is_absent() {
                return Err(in_doubt(format!(
                    "transaction {tid} owner operation was dropped after terminal commit dispatch"
                )));
            }
            status = self
                .advance_abort_transition(tid, &status.observation, transition)
                .await?;
            let outcome = match status.status {
                TxCommitStatus::Ok => Some(OwnerAbortOutcome::Committed),
                TxCommitStatus::Aborted => Some(OwnerAbortOutcome::Acknowledged),
                TxCommitStatus::Wounded if transition != AbortTransition::Acknowledge => {
                    Some(OwnerAbortOutcome::Pinned)
                }
                TxCommitStatus::Wounded | TxCommitStatus::Pending | TxCommitStatus::Unknown => None,
            };
            if let Some(outcome) = outcome {
                self.finish_local_tx(tid);
                return Ok(outcome);
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    /// Preempts a transaction under wound-wait, returning the durable terminal
    /// status that wins.
    ///
    /// A committed transaction is left untouched. A pending refresh does not
    /// defeat a wound: the refreshed observation is retried until either the
    /// abort lands or the owner commits.
    pub(crate) async fn preempt_tx(&self, tid: &TxId) -> Result<TxFinalStatus, TransError> {
        let transition = self.preemption_plan(tid);
        let mut status = self
            .inner
            .tl
            .commit_status_at(tid, self.current_requirement())
            .await
            .map_err(|e| {
                TransError::Storage(e.context(format!("reading status of wound target {tid}")))
            })?;
        let mut backoff = self.inner.retry.backoff();
        loop {
            status = self
                .advance_abort_transition(tid, &status.observation, transition)
                .await?;
            match status.status {
                TxCommitStatus::Ok => return Ok(TxFinalStatus::Committed),
                TxCommitStatus::Aborted => return Ok(TxFinalStatus::Aborted),
                TxCommitStatus::Wounded if transition != AbortTransition::Acknowledge => {
                    return Ok(TxFinalStatus::Aborted);
                }
                TxCommitStatus::Wounded | TxCommitStatus::Pending | TxCommitStatus::Unknown => {}
            }
            // Lease expiry must stop here because the refresh proves liveness.
            // Wound-wait is authorized by priority instead, so a refresh only
            // supplies the next CAS observation.
            rt::sleep(backoff.next_delay()).await;
        }
    }

    /// Returns the commit status, checking locally first then remote storage.
    pub(crate) async fn tx_status(&self, tid: &TxId) -> Result<TxCommitStatus, TransError> {
        Ok(self
            .resolve_tx_status_at(tid, self.current_requirement())
            .await?
            .status())
    }

    /// Returns the commit status using a caller-provided observation bound.
    pub(crate) async fn tx_status_at(
        &self,
        tid: &TxId,
        requirement: Requirement,
    ) -> Result<TxCommitStatus, TransError> {
        Ok(self.resolve_tx_status_at(tid, requirement).await?.status())
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

    /// Waits for and returns a transaction's durable final status.
    pub(crate) async fn await_tx_final(&self, tid: &TxId) -> Result<TxFinalStatus, TransError> {
        loop {
            // Resolve before registering a waiter so local state retains
            // precedence over a potentially newer final-status cache entry.
            match self.tx_status(tid).await? {
                TxCommitStatus::Ok => return Ok(TxFinalStatus::Committed),
                TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                    return Ok(TxFinalStatus::Aborted);
                }
                TxCommitStatus::Pending | TxCommitStatus::Unknown => {}
            }

            // Notifications and poll failures are only wake-up hints. Resolve
            // again before returning so an abandoned poll cannot decide status.
            self.wait_for_tx_change(tid).await;
        }
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

    /// Conditionally pins an exact foreign transaction observation as wounded.
    pub(crate) async fn try_wound_observed(
        &self,
        tid: &TxId,
        expected: &Observation<TxLog>,
    ) -> Result<TxStatus, TransError> {
        self.advance_abort_transition(tid, expected, AbortTransition::EnsureWounded)
            .await
    }

    /// Resolves status at the requested bound together with its exact evidence.
    async fn resolve_tx_status_at(
        &self,
        tid: &TxId,
        requirement: Requirement,
    ) -> Result<TxStatusEvidence, TransError> {
        if let Some(evidence) = self.owned_status_evidence(tid)? {
            return Ok(evidence);
        }
        if let Some(status) = self.cached_final_status(tid) {
            return TxStatusEvidence::cached_final(status);
        }
        let status = self.inner.tl.commit_status_at(tid, requirement).await?;
        self.resolve_remote_tx_status(tid, status).await
    }

    /// Persists the commit decision and resolves ambiguous outcomes while its
    /// durable record can still be read back (ADR-009, ADR-057).
    async fn persist_committed_log(&self, mut tlog: TxLog) -> Result<(), TransError> {
        let tid = &tlog.id;
        if tid.is_unset() {
            return Err(TransError::other("missing required tlog ID"));
        }
        tlog.status = TxCommitStatus::Ok;
        let mut expected = {
            let st = self.shard_for(tid).lock().unwrap();
            st.transactions
                .get(tid)
                .and_then(|entry| match &entry.role {
                    TxRuntimeRole::Owned(owned) => owned
                        .record
                        .as_ref()
                        .and_then(|record| record.last_observation.clone()),
                    TxRuntimeRole::Foreign(_) => None,
                })
        };

        let mut backoff = self.inner.retry.backoff();
        loop {
            let started_at = rt::Instant::now();
            let attempt = match &expected {
                Some(observed) => self.inner.tl.set_if(&tlog, observed).await,
                None => self.inner.tl.set(&tlog).await,
            };
            let failure = match attempt {
                Ok(observed) => {
                    self.record_durable_observation(tid, &observed);
                    return Ok(());
                }
                // A clean conflict proves this write did not land. An
                // unavailable result does not, so its read-back is bounded by
                // the record's reclamation horizon.
                Err(StorageError::Precondition) => CommitWriteFailure::Conflict,
                Err(StorageError::Unavailable(_)) => CommitWriteFailure::Ambiguous { started_at },
                Err(error) => return Err(error.into()),
            };
            let status = match failure {
                CommitWriteFailure::Conflict => {
                    self.read_tx_status_retrying_unavailable(tid, &mut backoff)
                        .await?
                }
                CommitWriteFailure::Ambiguous { started_at } => {
                    self.read_ambiguous_commit_before_reclaim(tid, &mut backoff, started_at)
                        .await?
                }
            };

            let record_state = TxRecordState::try_from_observation(&status.observation)?;
            match classify_commit_observation(failure, record_state)? {
                CommitResolution::Committed => {
                    self.record_durable_observation(tid, &status.observation);
                    return Ok(());
                }
                CommitResolution::AlreadyFinalized => {
                    self.record_durable_observation(tid, &status.observation);
                    return Err(TransError::AlreadyFinalized);
                }
                CommitResolution::InDoubt => {
                    return Err(in_doubt(format!(
                        "transaction {tid} record was reclaimed while its outcome was in doubt"
                    )));
                }
                CommitResolution::Retry => {
                    // Pending proves the preceding attempt did not land. The
                    // next write receives a fresh reclamation budget.
                    expected = Some(status.observation);
                    rt::sleep(backoff.next_delay()).await;
                }
            }
        }
    }

    /// Advances an abort-side lifecycle transition from an exact observation.
    async fn advance_abort_transition(
        &self,
        tid: &TxId,
        expected: &Observation<TxLog>,
        transition: AbortTransition,
    ) -> Result<TxStatus, TransError> {
        let mut expected = expected.clone();
        let mut backoff = self.inner.retry.backoff();
        loop {
            let current = TxStatus::from_observation(expected.clone());
            let record_state = TxRecordState::try_from_observation(&current.observation)?;
            let target = match classify_abort_observation(transition, record_state) {
                AbortObservationAction::Settled => {
                    self.record_durable_observation(tid, &current.observation);
                    return Ok(current);
                }
                AbortObservationAction::Write(target) => target,
            };
            let tlog = self.build_abort_log(tid, &expected, target);
            let r = if expected.is_absent() {
                self.inner.tl.set(&tlog).await
            } else {
                self.inner.tl.set_if(&tlog, &expected).await
            };
            match r {
                Ok(observed) => {
                    self.record_durable_observation(tid, &observed);
                    return Ok(TxStatus::from_observation(observed));
                }
                Err(StorageError::Precondition) => {
                    // The version moved under us (a commit, a pending-log
                    // refresh, or another abort). A clean conflict is not
                    // retried here: lease-based callers must treat a refresh as
                    // proof of liveness, while wound-wait decides separately to
                    // retry it.
                    let st = self
                        .read_tx_status_retrying_unavailable(tid, &mut backoff)
                        .await?;
                    self.record_durable_observation(tid, &st.observation);
                    return Ok(st);
                }
                // In-doubt: the abort write may or may not have landed. Forcing
                // a not-yet-final log to an abort-side state is idempotent and
                // convergent, so it is always safe to retry (ADR-009). This is
                // what keeps a lost ack on a wound (or on an expired-tx abort)
                // from escaping the locker as a `failed locking` error: a
                // pre-commit outcome must be recovered in place, never surfaced
                // to the caller. The observation table decides whether the
                // read-back is settled or should be retried.
                Err(StorageError::Unavailable(_)) => {
                    let st = self
                        .read_tx_status_retrying_unavailable(tid, &mut backoff)
                        .await?;
                    self.record_durable_observation(tid, &st.observation);
                    let record_state = TxRecordState::try_from_observation(&st.observation)?;
                    if classify_abort_observation(transition, record_state)
                        == AbortObservationAction::Settled
                    {
                        return Ok(st);
                    }
                    expected = st.observation;
                }
                Err(e) => return Err(e.into()),
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    /// Builds an abort-side transaction log that retains recovery ownership.
    fn build_abort_log(
        &self,
        tid: &TxId,
        expected: &Observation<TxLog>,
        target: TxCommitStatus,
    ) -> TxLog {
        debug_assert!(matches!(
            target,
            TxCommitStatus::Aborted | TxCommitStatus::Wounded
        ));
        let mut tlog = TxLog::new(tid.clone(), target);
        if let Some(current) = expected.value() {
            tlog.writes = current.writes.clone();
            TxRecoveryManifest::from_log(current).apply_to(&mut tlog);
        } else if let Some(TxRuntimeEntry {
            role: TxRuntimeRole::Owned(owned),
            ..
        }) = self.shard_for(tid).lock().unwrap().transactions.get(tid)
            && let Some(record) = owned.record.as_ref()
        {
            record.recovery.clone().apply_to(&mut tlog);
        }
        tlog
    }

    /// Reads back an ambiguous terminal write before its record can become
    /// reclaimable.
    ///
    /// GC reclaims a final transaction object once its lease horizon has
    /// elapsed since the timestamp that write stamped (ADR-022), and that
    /// timestamp is never earlier than the attempt that may have landed it.
    /// Measuring from the attempt and omitting the skew allowance (which is
    /// exactly what GC's own check adds to tolerate a foreign clock) therefore
    /// leaves at least GC's skew allowance after this recovery budget expires.
    async fn read_ambiguous_commit_before_reclaim(
        &self,
        tid: &TxId,
        backoff: &mut Backoff,
        attempt_started: rt::Instant,
    ) -> Result<TxStatus, TransError> {
        // The deadline starts with the write because time spent waiting for its
        // ambiguous response also consumes the record's retention horizon.
        let deadline = attempt_started + self.inner.timing.pending_timeout();
        loop {
            match self
                .inner
                .tl
                .commit_status_at(tid, self.current_requirement())
                .await
            {
                Ok(status) => return Ok(status),
                Err(StorageError::Unavailable(reason)) => {
                    let remaining = deadline.saturating_duration_since(rt::Instant::now());
                    if remaining.is_zero() {
                        return Err(in_doubt(reason));
                    }
                    rt::sleep(backoff.next_delay().min(remaining)).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    async fn read_tx_status_retrying_unavailable(
        &self,
        tid: &TxId,
        backoff: &mut Backoff,
    ) -> Result<TxStatus, TransError> {
        loop {
            match self
                .inner
                .tl
                .commit_status_at(tid, self.current_requirement())
                .await
            {
                Ok(status) => return Ok(status),
                Err(StorageError::Unavailable(_)) => {
                    rt::sleep(backoff.next_delay()).await;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn finish_owner_operation(&self, tid: &TxId, unresolved: bool) {
        let mut st = self.shard_for(tid).lock().unwrap();
        let Some(entry) = st.transactions.get_mut(tid) else {
            return;
        };
        let TxRuntimeRole::Owned(owned) = &mut entry.role else {
            return;
        };
        owned.lifecycle.finish_operation(unresolved);
        let remove = owned.record.is_none() && !owned.lifecycle.has_active_operations();
        if remove && let Some(mut entry) = st.transactions.remove(tid) {
            notify_waiters(&mut entry.waiters);
        }
    }

    /// Closes local admission and selects the strongest transition justified
    /// without waiting for owner work to finish.
    fn preemption_plan(&self, tid: &TxId) -> AbortTransition {
        let mut st = self.shard_for(tid).lock().unwrap();
        let Some(entry) = st.transactions.get_mut(tid) else {
            return AbortTransition::EnsureWounded;
        };
        let TxRuntimeRole::Owned(owned) = &mut entry.role else {
            return AbortTransition::EnsureWounded;
        };
        match owned.close(OwnerCloseReason::Preemption) {
            OwnerClosePlan::Transition(transition) => transition,
            OwnerClosePlan::PreserveCommit | OwnerClosePlan::AlreadyFinished => {
                debug_assert!(false, "preemption must select an abort transition");
                AbortTransition::EnsureWounded
            }
        }
    }

    /// Closes owner admission and derives the strongest safe abort transition
    /// from facts recorded by [`OwnerOperation`]. Untracked internal protocols
    /// call this only after their own mutation sequence has quiesced.
    fn owner_close_plan(&self, tid: &TxId) -> OwnerClosePlan {
        let mut st = self.shard_for(tid).lock().unwrap();
        let Some(entry) = st.transactions.get_mut(tid) else {
            return OwnerClosePlan::AlreadyFinished;
        };
        let TxRuntimeRole::Owned(owned) = &mut entry.role else {
            return OwnerClosePlan::AlreadyFinished;
        };
        owned.close(OwnerCloseReason::OwnerAbort)
    }

    fn start_terminal_commit(&self, tid: &TxId) -> Result<(), TransError> {
        let mut st = self.shard_for(tid).lock().unwrap();
        let owned = match st.transactions.entry(tid.clone()) {
            Entry::Vacant(entry) => {
                let entry = entry.insert(TxRuntimeEntry::owned(None));
                let TxRuntimeRole::Owned(owned) = &mut entry.role else {
                    unreachable!();
                };
                owned
            }
            Entry::Occupied(entry) => match &mut entry.into_mut().role {
                TxRuntimeRole::Owned(owned) => owned,
                TxRuntimeRole::Foreign(_) => return Err(TransError::AlreadyFinalized),
            },
        };
        if owned
            .record
            .as_ref()
            .is_some_and(|record| record.status != TxCommitStatus::Pending)
        {
            return Err(TransError::AlreadyFinalized);
        }
        owned.lifecycle.start_terminal_commit()
    }

    fn finish_local_tx(&self, tid: &TxId) {
        let mut st = self.shard_for(tid).lock().unwrap();
        let remove = st
            .transactions
            .get(tid)
            .is_some_and(|entry| matches!(entry.role, TxRuntimeRole::Owned(_)));
        if remove && let Some(mut entry) = st.transactions.remove(tid) {
            notify_waiters(&mut entry.waiters);
        }
    }

    fn register_tx(&self, tid: &TxId, recovery: TxRecoveryManifest) {
        let mut st = self.shard_for(tid).lock().unwrap();
        let record = TxStatusEntry {
            status: TxCommitStatus::Pending,
            last_observation: None,
            refresh_state: RefreshState::NotStarted,
            recovery,
        };
        match st.transactions.entry(tid.clone()) {
            Entry::Vacant(entry) => {
                entry.insert(TxRuntimeEntry::owned(Some(record)));
            }
            Entry::Occupied(entry) => match &mut entry.into_mut().role {
                TxRuntimeRole::Owned(owned) => owned.record = Some(record),
                TxRuntimeRole::Foreign(_) => {
                    panic!("cannot register a foreign transaction identity as owned")
                }
            },
        }
    }

    /// Builds the next pending-log mutation from current owner state.
    fn pending_write_snapshot(
        &self,
        tid: &TxId,
        require_running_refresh: bool,
    ) -> Result<Option<PendingWrite>, TransError> {
        let st = self.shard_for(tid).lock().unwrap();
        let entry = st
            .transactions
            .get(tid)
            .ok_or_else(|| TransError::other("pending transaction disappeared"))?;
        let TxRuntimeRole::Owned(owned) = &entry.role else {
            return Err(TransError::other(
                "pending transaction is tracked as foreign",
            ));
        };
        let record = owned
            .record
            .as_ref()
            .ok_or_else(|| TransError::other("pending transaction was not begun"))?;
        if record.status != TxCommitStatus::Pending
            || (require_running_refresh && record.refresh_state != RefreshState::Running)
        {
            return Ok(None);
        }

        let mut log = TxLog::new(tid.clone(), TxCommitStatus::Pending);
        log.timestamp = Some(rt::system_now());
        record.recovery.clone().apply_to(&mut log);
        Ok(Some(PendingWrite {
            log,
            expected: record.last_observation.clone(),
        }))
    }

    async fn write_pending(
        &self,
        write: &PendingWrite,
    ) -> Result<Observation<TxLog>, StorageError> {
        match &write.expected {
            Some(observed) => self.inner.tl.set_if(&write.log, observed).await,
            None => self.inner.tl.set(&write.log).await,
        }
    }

    async fn persist_pending_tx(&self, tid: &TxId) -> Result<(), TransError> {
        let mut backoff = self.inner.retry.backoff();
        loop {
            let write = self
                .pending_write_snapshot(tid, false)?
                .ok_or(TransError::AlreadyFinalized)?;
            match self.write_pending(&write).await {
                Ok(observed) => {
                    self.record_durable_observation(tid, &observed);
                    self.start_refresh_tx(tid);
                    return Ok(());
                }
                Err(StorageError::Precondition) => {
                    let status = self
                        .inner
                        .tl
                        .commit_status_at(tid, self.current_requirement())
                        .await?;
                    // An absent object was reclaimed rather than never written,
                    // and GC only deletes a final one. This write only ever
                    // stamps `pending`, so it cannot have been the reclaimed
                    // terminal state: the transaction is durably dead and must
                    // not be re-created over its own tombstone.
                    if status.status.is_final() || status.observation.is_absent() {
                        self.record_durable_observation(tid, &status.observation);
                        return Err(TransError::AlreadyFinalized);
                    }
                    self.record_durable_observation(tid, &status.observation);
                }
                Err(StorageError::Unavailable(_)) => {}
                Err(error) => return Err(error.into()),
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
        let evidence = match requirement {
            Some(requirement) => self.resolve_tx_status_at(tid, requirement).await?,
            None => {
                self.resolve_tx_status_at(tid, self.current_requirement())
                    .await?
            }
        };
        let status = evidence.status();
        if status != TxCommitStatus::Ok {
            return Ok(KeyCommitStatus {
                status,
                value: TValue::default(),
                cache_hit: evidence.cache_hit,
            });
        }

        let status_cache_hit = evidence.cache_hit;
        let tl = self
            .final_log(tid, status, requirement, evidence.observation)
            .await?;
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
        known: Option<Observation<TxLog>>,
    ) -> Result<Observation<TxLog>, TransError> {
        if let Some(observed) = known
            && observed
                .value()
                .is_some_and(|log| log.status == expected_status)
        {
            return Ok(observed);
        }
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

    fn owned_status_evidence(&self, tid: &TxId) -> Result<Option<TxStatusEvidence>, TransError> {
        let st = self.shard_for(tid).lock().unwrap();
        let Some(entry) = st.transactions.get(tid) else {
            return Ok(None);
        };
        match &entry.role {
            TxRuntimeRole::Owned(owned) => TxStatusEvidence::local(owned.record.as_ref()).map(Some),
            TxRuntimeRole::Foreign(_) => Ok(None),
        }
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
        if !log.status.is_immutable() {
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

    /// Records an exact durable observation in local owner state. Wounded is
    /// deliberately not put in the immutable final-status cache: its owner may
    /// still acknowledge it as aborted.
    fn record_durable_observation(&self, tid: &TxId, observed: &Observation<TxLog>) {
        let Some(log) = observed.value() else {
            return;
        };
        self.remember_final(tid, observed);

        let mut st = self.shard_for(tid).lock().unwrap();
        let foreign_final = log.status.is_final()
            && st
                .transactions
                .get(tid)
                .is_some_and(|entry| matches!(entry.role, TxRuntimeRole::Foreign(_)));
        if foreign_final {
            if let Some(mut entry) = st.transactions.remove(tid) {
                notify_waiters(&mut entry.waiters);
            }
            return;
        }
        let Some(TxRuntimeEntry {
            role: TxRuntimeRole::Owned(owned),
            waiters,
        }) = st.transactions.get_mut(tid)
        else {
            return;
        };
        let Some(record) = owned.record.as_mut() else {
            return;
        };
        record.status = log.status;
        record.last_observation = Some(observed.clone());
        if log.status.is_final() {
            record.refresh_state = RefreshState::Stopped;
            owned.lifecycle.admission = OwnerAdmission::Closed;
            notify_waiters(waiters);
        }
    }

    /// Returns the shard lock responsible for `tid`.
    fn shard_for(&self, tid: &TxId) -> &Mutex<State> {
        self.inner.shards.for_key(tid.as_bytes())
    }

    fn wait_for_tx_change(
        &self,
        tid: &TxId,
    ) -> impl std::future::Future<Output = ()> + Send + use<> {
        let rx = self.wait_for_tx_change_rx(tid);
        async move {
            let _ = rx.await;
        }
    }

    fn wait_for_tx_change_rx(&self, tid: &TxId) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();

        let mut st = self.shard_for(tid).lock().unwrap();
        let should_spawn = match st.transactions.entry(tid.clone()) {
            Entry::Vacant(entry) => {
                let entry = entry.insert(TxRuntimeEntry::foreign());
                entry.waiters.push(tx);
                true
            }
            Entry::Occupied(entry) => {
                let entry = entry.into_mut();
                match &entry.role {
                    TxRuntimeRole::Owned(owned)
                        if owned
                            .record
                            .as_ref()
                            .is_some_and(|record| record.status.is_final()) =>
                    {
                        let _ = tx.send(());
                        return rx;
                    }
                    TxRuntimeRole::Owned(_) => {
                        // Local transition: no worker is needed; commit or
                        // owner closure will notify these waiters.
                        entry.waiters.push(tx);
                        false
                    }
                    TxRuntimeRole::Foreign(_) => {
                        let should_spawn = entry.waiters.is_empty();
                        entry.waiters.push(tx);
                        should_spawn
                    }
                }
            }
        };
        drop(st);

        if !should_spawn {
            return rx;
        }

        let m = self.clone();
        let tid = tid.clone();
        // Detached poller: it terminates either when the tx finalizes (final
        // status or a fetch error) or when every caller has dropped its
        // `await_tx_final` future.
        rt::spawn(async move {
            m.poll_tx_status_with_liveness(&tid).await;
        });

        rx
    }

    async fn fetch_remote_tx_status(&self, tid: &TxId) -> Result<TxStatusEvidence, TransError> {
        self.resolve_tx_status_at(tid, self.current_requirement())
            .await
    }

    async fn resolve_remote_tx_status(
        &self,
        tid: &TxId,
        status: TxStatus,
    ) -> Result<TxStatusEvidence, TransError> {
        // The backend read is outside the runtime-state lock. A local owner may
        // have registered the identity while it was in flight; ownership takes
        // precedence over foreign liveness tracking.
        if let Some(evidence) = self.owned_status_evidence(tid)? {
            return Ok(evidence);
        }

        match TxRecordState::try_from_observation(&status.observation)? {
            TxRecordState::Missing => self.resolve_missing_tx(tid, status).await,
            TxRecordState::Pending => self.resolve_pending_tx(tid, status).await,
            TxRecordState::Wounded | TxRecordState::Committed | TxRecordState::Aborted => {
                self.record_durable_observation(tid, &status.observation);
                TxStatusEvidence::observed(status)
            }
        }
    }

    async fn resolve_pending_tx(
        &self,
        tid: &TxId,
        status: TxStatus,
    ) -> Result<TxStatusEvidence, TransError> {
        let now = rt::system_now();
        // Absolute lease check (foreign clock — skew applies) and relative
        // progress check (one observer clock — no skew) jointly decide whether
        // the holder must be pinned as wounded.
        let observation = if self.inner.timing.is_expired(status.last_update, now) {
            RemoteLivenessDecision::Expired
        } else {
            self.observe_remote_pending(tid, status.last_update, now)?
        };
        match observation {
            RemoteLivenessDecision::Owned(evidence) => Ok(evidence),
            RemoteLivenessDecision::Live => TxStatusEvidence::observed(status),
            RemoteLivenessDecision::Expired => {
                let wounded = self.try_wound_observed(tid, &status.observation).await?;
                TxStatusEvidence::observed(wounded)
            }
        }
    }

    /// Records a foreign pending lease and reports whether it stopped making
    /// progress for one observer-relative timeout.
    fn observe_remote_pending(
        &self,
        tid: &TxId,
        last_refresh: SystemTime,
        now: SystemTime,
    ) -> Result<RemoteLivenessDecision, TransError> {
        let mut st = self.shard_for(tid).lock().unwrap();
        let entry = st
            .transactions
            .entry(tid.clone())
            .or_insert_with(TxRuntimeEntry::foreign);
        let foreign = match &mut entry.role {
            TxRuntimeRole::Owned(owned) => {
                return TxStatusEvidence::local(owned.record.as_ref())
                    .map(RemoteLivenessDecision::Owned);
            }
            TxRuntimeRole::Foreign(foreign) => foreign,
        };
        let expired = match foreign.liveness {
            RemoteLiveness::PendingUnchanged {
                last_refresh: previous,
                since,
            } if previous == last_refresh => self.inner.timing.is_expired_no_skew(since, now),
            _ => {
                foreign.liveness = RemoteLiveness::PendingUnchanged {
                    last_refresh,
                    since: now,
                };
                false
            }
        };
        Ok(if expired {
            RemoteLivenessDecision::Expired
        } else {
            RemoteLivenessDecision::Live
        })
    }

    async fn resolve_missing_tx(
        &self,
        tid: &TxId,
        status: TxStatus,
    ) -> Result<TxStatusEvidence, TransError> {
        match self.observe_remote_missing(tid, rt::system_now())? {
            RemoteLivenessDecision::Owned(evidence) => return Ok(evidence),
            RemoteLivenessDecision::Live => return TxStatusEvidence::observed(status),
            RemoteLivenessDecision::Expired => {}
        }

        // The object must appear within one observer-relative timeout. Re-read
        // at the boundary so a concurrently created pending or final object is
        // never used as the expected side of an absence-based wound.
        let refreshed = self
            .inner
            .tl
            .commit_status_at(tid, self.current_requirement())
            .await?;
        if refreshed.observation.is_absent() {
            let wounded = self.try_wound_observed(tid, &refreshed.observation).await?;
            return TxStatusEvidence::observed(wounded);
        }
        match TxRecordState::try_from_observation(&refreshed.observation)? {
            TxRecordState::Pending => self.resolve_pending_tx(tid, refreshed).await,
            TxRecordState::Wounded | TxRecordState::Committed | TxRecordState::Aborted => {
                self.record_durable_observation(tid, &refreshed.observation);
                TxStatusEvidence::observed(refreshed)
            }
            TxRecordState::Missing => Err(TransError::other(
                "present transaction read normalized to a missing record",
            )),
        }
    }

    fn observe_remote_missing(
        &self,
        tid: &TxId,
        now: SystemTime,
    ) -> Result<RemoteLivenessDecision, TransError> {
        let mut st = self.shard_for(tid).lock().unwrap();
        let entry = st
            .transactions
            .entry(tid.clone())
            .or_insert_with(TxRuntimeEntry::foreign);
        let foreign = match &mut entry.role {
            TxRuntimeRole::Owned(owned) => {
                return TxStatusEvidence::local(owned.record.as_ref())
                    .map(RemoteLivenessDecision::Owned);
            }
            TxRuntimeRole::Foreign(foreign) => foreign,
        };
        let expired = match foreign.liveness {
            RemoteLiveness::MissingSince { since } => {
                self.inner.timing.is_expired_no_skew(since, now)
            }
            RemoteLiveness::Unobserved | RemoteLiveness::PendingUnchanged { .. } => {
                foreign.liveness = RemoteLiveness::MissingSince { since: now };
                false
            }
        };
        Ok(if expired {
            RemoteLivenessDecision::Expired
        } else {
            RemoteLivenessDecision::Live
        })
    }

    /// Polls the remote tx status until it finalizes, a fetch fails, or every
    /// caller has dropped its `await_tx_final` future (signalled by closed
    /// `oneshot::Sender`s in the waiters list). The latter is the future-drop
    /// equivalent of the per-call cancellation contexts the Go original used.
    ///
    /// [`Monitor::await_tx_final`] re-resolves after every wake, so the poller
    /// only needs to wake its waiters when it stops.
    async fn poll_tx_status_with_liveness(&self, tid: &TxId) {
        let mut backoff = self.inner.retry.backoff();
        let finalized = loop {
            let evidence = match self.fetch_remote_tx_status(tid).await {
                Err(_) => break false,
                Ok(evidence) => evidence,
            };
            if evidence.status().is_final() {
                break true;
            }
            if !self.retain_live_waiters(tid) {
                break false;
            }
            rt::sleep(backoff.next_delay()).await;
        };
        self.finish_status_poll(tid, finalized);
    }

    fn retain_live_waiters(&self, tid: &TxId) -> bool {
        let mut st = self.shard_for(tid).lock().unwrap();
        let Some(entry) = st.transactions.get_mut(tid) else {
            return false;
        };
        entry.waiters.retain(|waiter| !waiter.is_closed());
        !entry.waiters.is_empty()
    }

    fn finish_status_poll(&self, tid: &TxId, finalized: bool) {
        let mut st = self.shard_for(tid).lock().unwrap();
        let remove = st
            .transactions
            .get(tid)
            .is_some_and(|entry| finalized && matches!(entry.role, TxRuntimeRole::Foreign(_)));
        if remove {
            if let Some(mut entry) = st.transactions.remove(tid) {
                notify_waiters(&mut entry.waiters);
            }
            return;
        }
        if let Some(entry) = st.transactions.get_mut(tid) {
            notify_waiters(&mut entry.waiters);
        }
    }

    fn should_refresh(&self, tid: &TxId) -> bool {
        let st = self.shard_for(tid).lock().unwrap();
        matches!(
            st.transactions.get(tid),
            Some(TxRuntimeEntry {
                role: TxRuntimeRole::Owned(OwnedTxRuntime {
                    record: Some(record),
                    ..
                }),
                ..
            }) if record.status == TxCommitStatus::Pending
                && record.refresh_state == RefreshState::Running
        )
    }

    fn stop_tx_refresh(&self, tid: &TxId) -> bool {
        let mut st = self.shard_for(tid).lock().unwrap();
        match st.transactions.get_mut(tid) {
            Some(TxRuntimeEntry {
                role:
                    TxRuntimeRole::Owned(OwnedTxRuntime {
                        record: Some(record),
                        ..
                    }),
                ..
            }) if record.refresh_state == RefreshState::Running => {
                record.refresh_state = RefreshState::Stopped;
                true
            }
            _ => false,
        }
    }

    /// Background lease refresher (ADR-021/ADR-024). Under hold-and-wait a live
    /// transaction can block while holding locks for far longer than the
    /// configured pending timeout, so this loop keeps its lease fresh until the
    /// transaction commits or aborts (`should_refresh` flips). It is
    /// load-bearing: when no pending object has been observed, its first write
    /// creates one with create-if-absent semantics (ADR-024). If another owner
    /// path already persisted the object, the refresher reuses that exact
    /// observation and starts directly with a CAS. Every later refresh
    /// CAS-bumps the `timestamp` halfway through each pending interval.
    ///
    /// Create-if-absent is what keeps lazy materialization wound-safe: if an
    /// older peer already wounded this transaction (wrote a `wounded` object)
    /// before it materialized its own pending one, the create loses, the
    /// refresher observes the final status, stops, and the owner's commit fails
    /// — it can never resurrect itself over a wound. A later refresh CAS that
    /// finds `Wounded` or `Aborted` observes the same terminal decision.
    /// Transient backend failures (in-doubt, unavailable) are retried rather
    /// than abandoning the lease, since re-applying a pending refresh is
    /// idempotent and convergent (ADR-009).
    async fn refresh_pending(&self, tid: TxId) {
        if !self.should_refresh(&tid) {
            return;
        }

        loop {
            rt::sleep(self.inner.timing.refresh_interval()).await;
            let write = match self.pending_write_snapshot(&tid, true) {
                Ok(Some(write)) => write,
                Ok(None) | Err(_) => return,
            };
            match self.write_pending(&write).await {
                Ok(observed) => {
                    self.record_durable_observation(&tid, &observed);
                }
                // The create lost (object already exists) or the CAS version
                // moved under us. Re-read: a terminal status is a wound (or a race
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
                            self.record_durable_observation(&tid, &st.observation);
                            return;
                        }
                        // Reclaimed: the object went final and was collected,
                        // so this lease is over. There is nothing left to
                        // refresh and re-creating it would resurrect a dead
                        // transaction, so stop and let the owner's own commit
                        // establish the outcome.
                        Ok(st) if st.observation.is_absent() => return,
                        Ok(st) => self.record_durable_observation(&tid, &st.observation),
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

/// Selects the abort-side action justified by one durable observation.
fn classify_abort_observation(
    transition: AbortTransition,
    current: TxRecordState,
) -> AbortObservationAction {
    let (desired, target) = match transition {
        AbortTransition::Acknowledge => (TxRecordState::Aborted, TxCommitStatus::Aborted),
        AbortTransition::EnsureWounded | AbortTransition::WoundIfPresent => {
            (TxRecordState::Wounded, TxCommitStatus::Wounded)
        }
    };
    if transition == AbortTransition::WoundIfPresent && current == TxRecordState::Missing {
        return AbortObservationAction::Settled;
    }
    match current.relation_to(desired) {
        TxLifecycleRelation::Same | TxLifecycleRelation::Blocks => AbortObservationAction::Settled,
        TxLifecycleRelation::CanAdvance => AbortObservationAction::Write(target),
    }
}

/// Classifies what a durable read proves after a committed-log write did not
/// return success.
fn classify_commit_observation(
    failure: CommitWriteFailure,
    current: TxRecordState,
) -> Result<CommitResolution, TransError> {
    match (current.relation_to(TxRecordState::Committed), current) {
        (TxLifecycleRelation::Same, TxRecordState::Committed) => Ok(CommitResolution::Committed),
        (TxLifecycleRelation::Blocks, TxRecordState::Aborted | TxRecordState::Wounded) => {
            Ok(CommitResolution::AlreadyFinalized)
        }
        (TxLifecycleRelation::CanAdvance, TxRecordState::Pending) => Ok(CommitResolution::Retry),
        (TxLifecycleRelation::CanAdvance, TxRecordState::Missing) => Ok(match failure {
            CommitWriteFailure::Conflict => CommitResolution::AlreadyFinalized,
            CommitWriteFailure::Ambiguous { .. } => CommitResolution::InDoubt,
        }),
        _ => Err(TransError::other(
            "transaction lifecycle relation cannot resolve a commit",
        )),
    }
}

/// Builds the error that reports a transaction outcome as irreducibly unknown,
/// which the public surface classifies as `Error::InDoubt`.
fn in_doubt(reason: impl Into<String>) -> TransError {
    TransError::Storage(StorageError::Unavailable(reason.into()))
}

fn notify_waiters(waiters: &mut Vec<oneshot::Sender<()>>) {
    for waiter in waiters.drain(..) {
        // `send` silently fails if the receiver has been dropped, which is the
        // waiter-cancelled signal.
        let _ = waiter.send(());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture, RecordingBackend};
    use glassdb_backend::{Backend, BackendError, memory::MemoryBackend};
    use glassdb_data::{CollectionAddress, CollectionId, DbRoot};
    use glassdb_storage::transaction::{TxCollectionOp, TxWrite};
    use glassdb_storage::{CachedStore, LockType, Timeline};

    #[test]
    fn abort_observation_classification_matches_the_protocol() {
        let acknowledge = AbortTransition::Acknowledge;
        let ensure_wounded = AbortTransition::EnsureWounded;
        let wound_if_present = AbortTransition::WoundIfPresent;

        for transition in [acknowledge, ensure_wounded, wound_if_present] {
            assert_eq!(
                classify_abort_observation(transition, TxRecordState::Committed),
                AbortObservationAction::Settled
            );
            assert_eq!(
                classify_abort_observation(transition, TxRecordState::Aborted),
                AbortObservationAction::Settled
            );
        }

        assert_eq!(
            classify_abort_observation(acknowledge, TxRecordState::Wounded),
            AbortObservationAction::Write(TxCommitStatus::Aborted)
        );
        for transition in [ensure_wounded, wound_if_present] {
            assert_eq!(
                classify_abort_observation(transition, TxRecordState::Wounded),
                AbortObservationAction::Settled
            );
        }

        assert_eq!(
            classify_abort_observation(acknowledge, TxRecordState::Pending),
            AbortObservationAction::Write(TxCommitStatus::Aborted)
        );
        for transition in [ensure_wounded, wound_if_present] {
            assert_eq!(
                classify_abort_observation(transition, TxRecordState::Pending),
                AbortObservationAction::Write(TxCommitStatus::Wounded)
            );
        }

        assert_eq!(
            classify_abort_observation(acknowledge, TxRecordState::Missing),
            AbortObservationAction::Write(TxCommitStatus::Aborted)
        );
        assert_eq!(
            classify_abort_observation(ensure_wounded, TxRecordState::Missing),
            AbortObservationAction::Write(TxCommitStatus::Wounded)
        );
        assert_eq!(
            classify_abort_observation(wound_if_present, TxRecordState::Missing),
            AbortObservationAction::Settled
        );
    }

    #[test]
    fn commit_observation_classification_matches_the_protocol() {
        let conflict = CommitWriteFailure::Conflict;
        let ambiguous = CommitWriteFailure::Ambiguous {
            started_at: rt::Instant::now(),
        };

        for failure in [conflict, ambiguous] {
            assert_eq!(
                classify_commit_observation(failure, TxRecordState::Committed).unwrap(),
                CommitResolution::Committed
            );
            assert_eq!(
                classify_commit_observation(failure, TxRecordState::Aborted).unwrap(),
                CommitResolution::AlreadyFinalized
            );
            assert_eq!(
                classify_commit_observation(failure, TxRecordState::Wounded).unwrap(),
                CommitResolution::AlreadyFinalized
            );
            assert_eq!(
                classify_commit_observation(failure, TxRecordState::Pending).unwrap(),
                CommitResolution::Retry
            );
        }

        assert_eq!(
            classify_commit_observation(conflict, TxRecordState::Missing).unwrap(),
            CommitResolution::AlreadyFinalized
        );
        assert_eq!(
            classify_commit_observation(ambiguous, TxRecordState::Missing).unwrap(),
            CommitResolution::InDoubt
        );
    }

    #[test]
    fn owner_lifecycle_close_plans_match_retirement_proof() {
        let mut internal = OwnerLifecycle::default();
        assert_eq!(
            internal.close(OwnerCloseReason::OwnerAbort, OwnerRecordState::Other),
            OwnerClosePlan::Transition(AbortTransition::Acknowledge)
        );

        let mut unengaged_preemption = OwnerLifecycle::default();
        assert_eq!(
            unengaged_preemption.close(OwnerCloseReason::Preemption, OwnerRecordState::NotEngaged,),
            OwnerClosePlan::Transition(AbortTransition::EnsureWounded)
        );

        let mut quiescent = OwnerLifecycle::default();
        quiescent.begin_operation().unwrap();
        quiescent.finish_operation(false);
        assert_eq!(
            quiescent.close(OwnerCloseReason::Preemption, OwnerRecordState::Other),
            OwnerClosePlan::Transition(AbortTransition::Acknowledge)
        );

        let mut active = OwnerLifecycle::default();
        active.begin_operation().unwrap();
        assert_eq!(
            active.close(OwnerCloseReason::Preemption, OwnerRecordState::Other),
            OwnerClosePlan::Transition(AbortTransition::EnsureWounded)
        );

        let mut dropped = OwnerLifecycle::default();
        dropped.begin_operation().unwrap();
        dropped.finish_operation(true);
        assert_eq!(
            dropped.close(OwnerCloseReason::OwnerAbort, OwnerRecordState::Other),
            OwnerClosePlan::Transition(AbortTransition::EnsureWounded)
        );

        let mut dispatched = OwnerLifecycle::default();
        dispatched.begin_operation().unwrap();
        dispatched.finish_operation(false);
        dispatched.start_terminal_commit().unwrap();
        assert_eq!(
            dispatched.close(OwnerCloseReason::OwnerAbort, OwnerRecordState::Other),
            OwnerClosePlan::PreserveCommit
        );

        let mut unresolved_dispatch = OwnerLifecycle::default();
        unresolved_dispatch.begin_operation().unwrap();
        unresolved_dispatch.start_terminal_commit().unwrap();
        assert_eq!(
            unresolved_dispatch.close(OwnerCloseReason::OwnerAbort, OwnerRecordState::Other),
            OwnerClosePlan::Transition(AbortTransition::WoundIfPresent)
        );

        let mut observed_wound = OwnerLifecycle::default();
        observed_wound.begin_operation().unwrap();
        observed_wound.finish_operation(false);
        observed_wound.start_terminal_commit().unwrap();
        assert_eq!(
            observed_wound.close(OwnerCloseReason::OwnerAbort, OwnerRecordState::Wounded),
            OwnerClosePlan::Transition(AbortTransition::Acknowledge)
        );

        let mut never_engaged = OwnerLifecycle::default();
        assert_eq!(
            never_engaged.close(OwnerCloseReason::OwnerAbort, OwnerRecordState::NotEngaged,),
            OwnerClosePlan::AlreadyFinished
        );
    }

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

    #[test]
    fn recovery_manifest_round_trip_preserves_non_recovery_log_fields() {
        let id = TxId::from_bytes(b"manifest".to_vec());
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
        let parent = CollectionAddress::root("test");
        let created = collection_address(1);
        let manifest = TxRecoveryManifest {
            locks: vec![TxLock::Topology {
                collection: parent.clone(),
            }],
            collection_changes: vec![TxCollectionChange {
                parent,
                name: b"created".to_vec(),
                collection: created.clone(),
                op: TxCollectionOp::Create,
            }],
            prepared_collections: vec![created],
        };
        let writes = vec![TxWriteForTest::w(&key_ref(b"key"), b"value")];
        let mut log = TxLog::new(id.clone(), TxCommitStatus::Ok);
        log.timestamp = Some(timestamp);
        log.writes = writes.clone();

        manifest.clone().apply_to(&mut log);

        assert_eq!(TxRecoveryManifest::from_log(&log), manifest);
        assert_eq!(log.id, id);
        assert_eq!(log.status, TxCommitStatus::Ok);
        assert_eq!(log.timestamp, Some(timestamp));
        assert_eq!(log.writes, writes);
    }

    fn key_ref(key: &[u8]) -> KeyRef {
        KeyRef::new(CollectionAddress::root("test"), key)
    }

    fn collection_address(id: u8) -> CollectionAddress {
        CollectionAddress::new(
            "test",
            CollectionId::from_slice(&[id; 16]).expect("fixed ID has the required width"),
        )
    }

    struct TestCtx {
        tl: TLogger,
        // The strong `Arc<Background>` lives here so refresh tasks can be
        // spawned for the duration of the test; the `Monitor` only stores a
        // `Weak`.
        _bg: Arc<Background>,
    }

    fn new_test_monitor(b: Arc<dyn Backend>) -> (Monitor, TestCtx) {
        new_test_monitor_with_timing(b, ProtocolTiming::default())
    }

    fn new_test_monitor_with_timing(
        b: Arc<dyn Backend>,
        timing: ProtocolTiming,
    ) -> (Monitor, TestCtx) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(b, 1024, timeline.clone(), None);
        let tl = TLogger::new(objects.clone(), DbRoot::try_from("test").unwrap());
        let bg = Arc::new(Background::new());
        let mon = Monitor::with_config(
            tl.clone(),
            timeline,
            Arc::downgrade(&bg),
            RetryConfig::default(),
            timing,
        );
        (mon, TestCtx { tl, _bg: bg })
    }

    async fn wait_for_waiters(mon: &Monitor, tid: &TxId, count: usize) {
        for _ in 0..100 {
            let waiting = mon
                .shard_for(tid)
                .lock()
                .unwrap()
                .transactions
                .get(tid)
                .map_or(0, |entry| entry.waiters.len());
            if waiting == count {
                return;
            }
            rt::yield_now().await;
        }
        panic!("transaction did not register {count} waiters");
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
    async fn begin_persisted_tx_durably_records_manifest() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"persisted".to_vec());
        let parent = CollectionAddress::root("test");
        let created = collection_address(1);
        let recovery = TxRecoveryManifest {
            locks: vec![TxLock::Topology {
                collection: parent.clone(),
            }],
            collection_changes: vec![TxCollectionChange {
                parent,
                name: b"created".to_vec(),
                collection: created.clone(),
                op: TxCollectionOp::Create,
            }],
            prepared_collections: vec![created],
        };

        mon.begin_persisted_tx(&tx, recovery.clone()).await.unwrap();

        let log = t.tl.get_at(&tx, Requirement::Any).await.unwrap();
        let log = log.value().unwrap();
        assert_eq!(log.status, TxCommitStatus::Pending);
        assert_eq!(log.locks, recovery.locks);
        assert_eq!(log.collection_changes, recovery.collection_changes);
        assert_eq!(log.prepared_collections, recovery.prepared_collections);

        mon.abort_owned_tx(&tx).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn persisted_refresh_reuses_its_exact_observation() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let b: Arc<dyn Backend> = Arc::new(backend);
        let (mon, _t) = new_test_monitor_with_timing(b, ProtocolTiming::simulation());
        let tx = TxId::from_bytes(b"persisted-refresh".to_vec());

        mon.begin_persisted_tx(&tx, TxRecoveryManifest::default())
            .await
            .unwrap();
        operations.lock().unwrap().clear();

        // Let the spawned refresher begin its sleep before advancing virtual
        // time through exactly one refresh interval.
        rt::yield_now().await;
        tokio::time::sleep(ProtocolTiming::simulation().refresh_interval()).await;
        rt::yield_now().await;

        {
            let operations = operations.lock().unwrap();
            assert_eq!(operations.len(), 1, "one refresh should issue one CAS");
            assert_eq!(operations[0].op, "write_if");
        }

        mon.abort_owned_tx(&tx).await.unwrap();
    }

    #[tokio::test]
    async fn update_pending_tx_preserves_unmodified_backreferences() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"updated".to_vec());
        let lock = TxLock::Topology {
            collection: CollectionAddress::root("test"),
        };
        mon.begin_persisted_tx(
            &tx,
            TxRecoveryManifest {
                locks: vec![lock.clone()],
                ..TxRecoveryManifest::default()
            },
        )
        .await
        .unwrap();
        let created = collection_address(2);
        let change = TxCollectionChange {
            parent: CollectionAddress::root("test"),
            name: b"created".to_vec(),
            collection: created.clone(),
            op: TxCollectionOp::Create,
        };

        mon.update_pending_tx(&tx, {
            let change = change.clone();
            let created = created.clone();
            move |recovery| {
                recovery.collection_changes = vec![change];
                recovery.prepared_collections = vec![created];
            }
        })
        .await
        .unwrap();

        let log = t.tl.get_at(&tx, Requirement::Any).await.unwrap();
        let log = log.value().unwrap();
        assert_eq!(log.status, TxCommitStatus::Pending);
        assert_eq!(log.locks, vec![lock]);
        assert_eq!(log.collection_changes, vec![change]);
        assert_eq!(log.prepared_collections, vec![created]);

        mon.abort_owned_tx(&tx).await.unwrap();
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

        mon1.abort_owned_tx(&tx).await.unwrap();
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
    async fn losing_commit_observes_and_acknowledges_a_durable_wound() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (owner, _owner_ctx) = new_test_monitor(b.clone());
        let (wounder, _wounder_ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"commit-loser".to_vec());
        let lock = TxLock::Topology {
            collection: CollectionAddress::root("test"),
        };
        let recovery = TxRecoveryManifest {
            locks: vec![lock.clone()],
            ..TxRecoveryManifest::default()
        };
        owner.begin_persisted_tx(&tx, recovery).await.unwrap();

        assert_eq!(
            wounder.preempt_tx(&tx).await.unwrap(),
            TxFinalStatus::Aborted
        );

        let mut log = TxLog::new(tx.clone(), TxCommitStatus::Pending);
        log.locks.push(lock);
        assert!(matches!(
            owner.commit_tx(log).await,
            Err(TransError::AlreadyFinalized)
        ));
        assert_eq!(owner.tx_status(&tx).await.unwrap(), TxCommitStatus::Wounded);
        assert_eq!(
            owner.await_tx_final(&tx).await.unwrap(),
            TxFinalStatus::Aborted
        );
        assert_eq!(
            owner.abort_owned_tx(&tx).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
        assert_eq!(owner.tx_status(&tx).await.unwrap(), TxCommitStatus::Aborted);
    }

    // Regression: an unacknowledged wound is the anti-resurrection fence. It is
    // terminal to the owner but cannot be physically reclaimed before the owner
    // changes it to `Aborted`.
    #[tokio::test]
    async fn pinned_wound_cannot_be_reclaimed_or_resurrected() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (owner, _owner_ctx) = new_test_monitor(b.clone());
        let (wounder, wounder_ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"reclaimed-wound".to_vec());
        let lock = TxLock::Topology {
            collection: CollectionAddress::root("test"),
        };
        owner
            .begin_persisted_tx(
                &tx,
                TxRecoveryManifest {
                    locks: vec![lock.clone()],
                    ..TxRecoveryManifest::default()
                },
            )
            .await
            .unwrap();
        assert_eq!(
            wounder.preempt_tx(&tx).await.unwrap(),
            TxFinalStatus::Aborted
        );

        let wounded = wounder_ctx.tl.get_at(&tx, Requirement::Any).await.unwrap();
        assert!(matches!(
            wounder_ctx.tl.delete(&wounded).await,
            Err(StorageError::Precondition)
        ));

        let mut log = TxLog::new(tx.clone(), TxCommitStatus::Pending);
        log.locks.push(lock);
        assert!(matches!(
            owner.commit_tx(log).await,
            Err(TransError::AlreadyFinalized)
        ));
    }

    // Regression for the lazy-object suspension gap: a holder owner can stop
    // before its first Pending write. A foreign wound of the missing path must
    // remain present for arbitrarily long and defeat the owner's later create.
    #[tokio::test(start_paused = true)]
    async fn missing_foreign_wound_fences_a_late_owner_create() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (owner, _owner_ctx) = new_test_monitor(b.clone());
        let (wounder, wounder_ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"lazy-suspended-owner".to_vec());
        let lock = TxLock::Topology {
            collection: CollectionAddress::root("test"),
        };

        let owner_operation = owner.begin_owner_operation(&tx).unwrap();
        owner.begin_tx(&tx);
        assert_eq!(
            wounder.preempt_tx(&tx).await.unwrap(),
            TxFinalStatus::Aborted
        );
        assert_eq!(
            wounder_ctx
                .tl
                .commit_status_at(&tx, Requirement::Any)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Wounded
        );

        tokio::time::sleep(Duration::from_secs(24 * 60 * 60)).await;
        let mut log = TxLog::new(tx.clone(), TxCommitStatus::Pending);
        log.locks.push(lock);
        assert!(matches!(
            owner.commit_tx(log).await,
            Err(TransError::AlreadyFinalized)
        ));

        owner_operation.complete();
        owner.abort_owned_tx(&tx).await.unwrap();
        assert_eq!(owner.tx_status(&tx).await.unwrap(), TxCommitStatus::Aborted);
    }

    // Regression (ADR-057): a commit write that landed while its acknowledgement
    // was lost, and whose committed object GC then reclaimed, cannot establish
    // its own outcome from storage — the durable evidence is gone. That must
    // surface as the irreducible in-doubt outcome rather than an internal error,
    // and the record must not be re-created, which would claim a commit that a
    // peer's wound may equally have decided.
    #[tokio::test(start_paused = true)]
    async fn commit_reports_in_doubt_when_its_landed_write_was_reclaimed() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let b: Arc<dyn Backend> = backend.clone();
        let (owner, _owner_ctx) = new_test_monitor(b.clone());
        let (_collector, collector_ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"reclaimed-in-doubt".to_vec());
        let lock = TxLock::Topology {
            collection: CollectionAddress::root("test"),
        };
        owner
            .begin_persisted_tx(
                &tx,
                TxRecoveryManifest {
                    locks: vec![lock.clone()],
                    ..TxRecoveryManifest::default()
                },
            )
            .await
            .unwrap();

        let reclaim = Arc::new(Mutex::new(Some((collector_ctx.tl.clone(), tx.clone()))));
        backend.set_after(move |operation, _outcome| {
            let reclaim = is_commit_write(operation)
                .then(|| reclaim.lock().unwrap().take())
                .flatten();
            let future: HookFuture = Box::pin(async move {
                let Some((tl, tx)) = reclaim else {
                    return Ok(());
                };
                let committed = tl
                    .get_at(&tx, Requirement::Any)
                    .await
                    .expect("the commit write landed before its ack was lost");
                tl.delete(&committed)
                    .await
                    .expect("the committed object is reclaimable");
                Err(BackendError::Unavailable(
                    "injected lost ack (landed, ack lost)".into(),
                ))
            });
            future
        });

        let mut log = TxLog::new(tx.clone(), TxCommitStatus::Pending);
        log.locks.push(lock);
        let error = owner.commit_tx(log).await.unwrap_err();
        assert!(
            matches!(error, TransError::Storage(StorageError::Unavailable(_))),
            "expected an in-doubt outcome, got {error:?}"
        );
    }

    // Regression (ADR-057): retrying an in-doubt commit is only useful while the
    // record it would read is still there. GC may reclaim a landed write once
    // the lease horizon has elapsed since that write, so the commit loop must
    // give up within that horizon instead of retrying into a window where its
    // own record has been erased.
    //
    // The horizon must use the same runtime monotonic time as retry sleeps.
    // Measuring it with a raw wall clock would make the budget effectively
    // unbounded while this test advances paused runtime time.
    #[tokio::test(start_paused = true)]
    async fn commit_gives_up_in_doubt_within_the_reclaim_horizon() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let b: Arc<dyn Backend> = backend.clone();
        let (owner, _owner_ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"unconfirmable".to_vec());
        let lock = TxLock::Topology {
            collection: CollectionAddress::root("test"),
        };
        owner
            .begin_persisted_tx(
                &tx,
                TxRecoveryManifest {
                    locks: vec![lock.clone()],
                    ..TxRecoveryManifest::default()
                },
            )
            .await
            .unwrap();

        // The commit write's ack is lost and every later read fails, so the
        // owner can never confirm whether the write landed.
        backend.set_after(|operation, _outcome| {
            let outage = is_commit_write(operation)
                || matches!(
                    operation,
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                );
            let future: HookFuture = Box::pin(async move {
                if outage {
                    return Err(BackendError::Unavailable("injected outage".into()));
                }
                Ok(())
            });
            future
        });

        let mut log = TxLog::new(tx.clone(), TxCommitStatus::Pending);
        log.locks.push(lock);
        let started = rt::Instant::now();
        let error = owner.commit_tx(log).await.unwrap_err();
        let elapsed = started.elapsed();

        assert!(
            matches!(error, TransError::Storage(StorageError::Unavailable(_))),
            "expected an in-doubt outcome, got {error:?}"
        );
        // The recovery read is bounded directly, so it cannot run a whole
        // backoff interval past the horizon.
        let horizon = owner.protocol_timing().pending_timeout();
        assert!(
            elapsed <= horizon + Duration::from_millis(100),
            "gave up after {elapsed:?}, past the {horizon:?} reclaim horizon"
        );
        assert!(
            elapsed > horizon / 2,
            "gave up after {elapsed:?}, far short of the {horizon:?} reclaim horizon"
        );
    }

    /// Whether `operation` is the conditional write that flips a transaction
    /// object to `committed`, which is the commit point a lost acknowledgement
    /// makes ambiguous.
    fn is_commit_write(operation: &BackendOp<'_>) -> bool {
        matches!(
            operation,
            BackendOp::WriteIf { value, .. }
                if glassdb_storage::txobject::status(value)
                    .map(|status| status == TxCommitStatus::Ok)
                    .unwrap_or(false)
        )
    }

    #[tokio::test(start_paused = true)]
    async fn commit_retries_status_read_after_cas_conflict() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let b: Arc<dyn Backend> = backend.clone();
        let (owner, _owner_ctx) = new_test_monitor(b.clone());
        let (_racer, racer_ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"commit-read-retry".to_vec());
        let lock = TxLock::Topology {
            collection: CollectionAddress::root("test"),
        };
        owner
            .begin_persisted_tx(
                &tx,
                TxRecoveryManifest {
                    locks: vec![lock.clone()],
                    ..TxRecoveryManifest::default()
                },
            )
            .await
            .unwrap();
        let pending = racer_ctx.tl.get_at(&tx, Requirement::Any).await.unwrap();
        let mut refreshed = pending.value().unwrap().as_ref().clone();
        refreshed.timestamp = Some(rt::system_now());

        let refresh = Arc::new(Mutex::new(Some((racer_ctx.tl.clone(), refreshed, pending))));
        let fail_next_read = Arc::new(AtomicBool::new(false));
        let failed_reads = Arc::new(AtomicUsize::new(0));
        backend.set_before({
            let refresh = refresh.clone();
            let fail_next_read = fail_next_read.clone();
            let failed_reads = failed_reads.clone();
            move |operation| {
                let is_commit = matches!(
                    operation,
                    BackendOp::WriteIf { value, .. }
                        if glassdb_storage::txobject::status(value)
                            .map(|status| status == TxCommitStatus::Ok)
                            .unwrap_or(false)
                );
                let refresh = is_commit.then(|| refresh.lock().unwrap().take()).flatten();
                let fail_read = matches!(
                    operation,
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                ) && fail_next_read.swap(false, Ordering::SeqCst);
                let fail_next_read = fail_next_read.clone();
                let failed_reads = failed_reads.clone();
                let future: HookFuture = Box::pin(async move {
                    if let Some((tl, pending, expected)) = refresh {
                        tl.set_if(&pending, &expected)
                            .await
                            .expect("the competing pending refresh should win");
                        fail_next_read.store(true, Ordering::SeqCst);
                    }
                    if fail_read {
                        failed_reads.fetch_add(1, Ordering::SeqCst);
                        return Err(BackendError::Unavailable(
                            "injected status read failure".into(),
                        ));
                    }
                    Ok(())
                });
                future
            }
        });

        let mut log = TxLog::new(tx.clone(), TxCommitStatus::Pending);
        log.locks.push(lock);
        owner.commit_tx(log).await.unwrap();

        assert_eq!(failed_reads.load(Ordering::SeqCst), 1);
        assert_eq!(owner.tx_status(&tx).await.unwrap(), TxCommitStatus::Ok);
    }

    #[tokio::test]
    async fn preempt_tx_returns_the_status_that_won() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, _t) = new_test_monitor(b);
        let pending = TxId::from_bytes(b"pending".to_vec());
        mon.begin_tx(&pending);
        assert_eq!(
            mon.preempt_tx(&pending).await.unwrap(),
            TxFinalStatus::Aborted
        );

        let committed = TxId::from_bytes(b"committed".to_vec());
        mon.begin_tx(&committed);
        let mut log = TxLog::new(committed.clone(), TxCommitStatus::Ok);
        log.locks.push(TxLock::Entry {
            key: key_ref(b"key"),
            typ: LockType::Write,
        });
        mon.commit_tx(log).await.unwrap();
        assert_eq!(
            mon.preempt_tx(&committed).await.unwrap(),
            TxFinalStatus::Committed
        );
    }

    #[tokio::test]
    async fn quiescent_local_wound_writes_aborted_without_wounded() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let written = Arc::new(Mutex::new(Vec::new()));
        backend.set_before({
            let written = written.clone();
            move |operation| {
                let status = match operation {
                    BackendOp::WriteIf { value, .. }
                    | BackendOp::WriteIfNotExists { value, .. } => {
                        glassdb_storage::txobject::status(value).ok()
                    }
                    _ => None,
                };
                if let Some(status) = status {
                    written.lock().unwrap().push(status);
                }
                Box::pin(async { Ok(()) })
            }
        });
        let b: Arc<dyn Backend> = backend;
        let (mon, ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"local-quiescent".to_vec());

        let owner = mon.begin_owner_operation(&tx).unwrap();
        mon.begin_tx(&tx);
        owner.complete();

        assert_eq!(mon.preempt_tx(&tx).await.unwrap(), TxFinalStatus::Aborted);
        assert_eq!(
            written.lock().unwrap().as_slice(),
            [TxCommitStatus::Aborted]
        );
        assert_eq!(
            ctx.tl
                .commit_status_at(&tx, Requirement::Any)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Aborted
        );
    }

    #[tokio::test]
    async fn active_local_wound_is_pinned_before_owner_acknowledgement() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let written = Arc::new(Mutex::new(Vec::new()));
        backend.set_before({
            let written = written.clone();
            move |operation| {
                let status = match operation {
                    BackendOp::WriteIf { value, .. }
                    | BackendOp::WriteIfNotExists { value, .. } => {
                        glassdb_storage::txobject::status(value).ok()
                    }
                    _ => None,
                };
                if let Some(status) = status {
                    written.lock().unwrap().push(status);
                }
                Box::pin(async { Ok(()) })
            }
        });
        let b: Arc<dyn Backend> = backend;
        let (mon, ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"local-active".to_vec());

        let owner = mon.begin_owner_operation(&tx).unwrap();
        mon.begin_tx(&tx);
        assert_eq!(mon.preempt_tx(&tx).await.unwrap(), TxFinalStatus::Aborted);
        assert_eq!(mon.tx_status(&tx).await.unwrap(), TxCommitStatus::Wounded);
        assert_eq!(
            written.lock().unwrap().as_slice(),
            [TxCommitStatus::Wounded]
        );

        owner.complete();
        assert_eq!(
            mon.abort_owned_tx(&tx).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
        assert_eq!(
            written.lock().unwrap().as_slice(),
            [TxCommitStatus::Wounded, TxCommitStatus::Aborted]
        );
        assert_eq!(
            ctx.tl
                .commit_status_at(&tx, Requirement::Any)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Aborted
        );
    }

    #[tokio::test]
    async fn dropped_owner_operation_cannot_acknowledge_its_own_wound() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"local-unresolved".to_vec());

        let owner = mon.begin_owner_operation(&tx).unwrap();
        mon.begin_tx(&tx);
        drop(owner);

        assert_eq!(mon.preempt_tx(&tx).await.unwrap(), TxFinalStatus::Aborted);
        assert_eq!(
            mon.abort_owned_tx(&tx).await.unwrap(),
            OwnerAbortOutcome::Pinned
        );
        assert!(!mon.is_tracked_local(&tx));
        assert_eq!(
            ctx.tl
                .commit_status_at(&tx, Requirement::Any)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Wounded
        );
    }

    #[tokio::test]
    async fn cancellation_after_terminal_dispatch_does_not_invent_a_wound() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, ctx) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"terminal-dispatched".to_vec());

        let owner = mon.begin_owner_operation(&tx).unwrap();
        mon.begin_tx(&tx);
        mon.start_terminal_commit(&tx).unwrap();
        drop(owner);

        assert!(matches!(
            mon.abort_owned_tx(&tx).await,
            Err(TransError::Storage(StorageError::Unavailable(_)))
        ));
        assert!(matches!(
            ctx.tl.get_at(&tx, Requirement::Any).await,
            Err(StorageError::NotFound)
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn preempt_tx_retries_a_pending_refresh() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let b: Arc<dyn Backend> = backend.clone();
        let (mon, _wounder) = new_test_monitor(b.clone());
        let (_owner_mon, owner) = new_test_monitor(b.clone());
        let tx = TxId::from_bytes(b"refresh-before-wound".to_vec());
        let pending = owner
            .tl
            .set(&TxLog::new(tx.clone(), TxCommitStatus::Pending))
            .await
            .unwrap();

        let mut refreshed = TxLog::new(tx.clone(), TxCommitStatus::Pending);
        refreshed.locks.push(TxLock::Entry {
            key: key_ref(b"new-lock"),
            typ: LockType::Write,
        });
        let refresh = Arc::new(Mutex::new(Some((
            owner.tl.clone(),
            refreshed.clone(),
            pending,
        ))));
        let wound_writes = Arc::new(AtomicUsize::new(0));
        let fail_next_read = Arc::new(AtomicBool::new(false));
        let failed_reads = Arc::new(AtomicUsize::new(0));
        backend.set_before({
            let refresh = refresh.clone();
            let wound_writes = wound_writes.clone();
            let fail_next_read = fail_next_read.clone();
            let failed_reads = failed_reads.clone();
            move |operation| {
                let is_wound = matches!(
                    operation,
                    BackendOp::WriteIf { value, .. }
                        if glassdb_storage::txobject::status(value)
                            .map(|status| status == TxCommitStatus::Wounded)
                            .unwrap_or(false)
                );
                let refresh = if is_wound {
                    wound_writes.fetch_add(1, Ordering::SeqCst);
                    refresh.lock().unwrap().take()
                } else {
                    None
                };
                let fail_read = matches!(
                    operation,
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                ) && fail_next_read.swap(false, Ordering::SeqCst);
                let fail_next_read = fail_next_read.clone();
                let failed_reads = failed_reads.clone();
                let future: HookFuture = Box::pin(async move {
                    if let Some((tl, pending, expected)) = refresh {
                        tl.set_if(&pending, &expected)
                            .await
                            .expect("the competing pending refresh should win");
                        fail_next_read.store(true, Ordering::SeqCst);
                    }
                    if fail_read {
                        failed_reads.fetch_add(1, Ordering::SeqCst);
                        return Err(BackendError::Unavailable(
                            "injected status read failure".into(),
                        ));
                    }
                    Ok(())
                });
                future
            }
        });

        assert_eq!(mon.preempt_tx(&tx).await.unwrap(), TxFinalStatus::Aborted);
        assert_eq!(wound_writes.load(Ordering::SeqCst), 2);
        assert_eq!(failed_reads.load(Ordering::SeqCst), 1);
        let (_verify_mon, verify) = new_test_monitor(b);
        let final_log = verify.tl.get_at(&tx, Requirement::Any).await.unwrap();
        assert_eq!(final_log.value().unwrap().locks, refreshed.locks);
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
    async fn await_local_tx_final() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, _t1) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"tx1".to_vec());
        mon1.begin_tx(&tx);

        let ch1 = {
            let mon = mon1.clone();
            let tx = tx.clone();
            rt::spawn(async move { mon.await_tx_final(&tx).await })
        };
        let ch2 = {
            let mon = mon1.clone();
            let tx = tx.clone();
            rt::spawn(async move { mon.await_tx_final(&tx).await })
        };
        wait_for_waiters(&mon1, &tx, 2).await;

        mon1.abort_owned_tx(&tx).await.unwrap();
        assert_eq!(ch1.await.unwrap().unwrap(), TxFinalStatus::Aborted);
        assert_eq!(ch2.await.unwrap().unwrap(), TxFinalStatus::Aborted);
    }

    #[tokio::test]
    async fn await_remote_tx_final_coalesces_waiters() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, _t1) = new_test_monitor(b.clone());
        let (mon2, _t2) = new_test_monitor(b.clone());
        let tx = TxId::from_bytes(b"tx1".to_vec());
        mon1.begin_tx(&tx);

        let mut waits = Vec::new();
        for _ in 0..3 {
            let mon = mon2.clone();
            let tx = tx.clone();
            waits.push(rt::spawn(async move { mon.await_tx_final(&tx).await }));
        }
        wait_for_waiters(&mon2, &tx, 3).await;

        mon1.abort_owned_tx(&tx).await.unwrap();

        for wait in waits {
            assert_eq!(wait.await.unwrap().unwrap(), TxFinalStatus::Aborted);
        }
    }

    #[tokio::test]
    async fn await_final_uses_final_status_cache() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let b: Arc<dyn Backend> = Arc::new(backend);
        let (mon, _t) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"committed".to_vec());
        let key = key_ref(b"key");
        mon.begin_tx(&tx);
        let mut log = TxLog::new(tx.clone(), TxCommitStatus::Ok);
        log.locks.push(TxLock::Entry {
            key,
            typ: LockType::Write,
        });
        mon.commit_tx(log).await.unwrap();
        operations.lock().unwrap().clear();

        assert_eq!(
            mon.await_tx_final(&tx).await.unwrap(),
            TxFinalStatus::Committed
        );
        assert!(
            operations.lock().unwrap().is_empty(),
            "a cached final status must not spawn a remote poll"
        );
    }

    #[tokio::test]
    async fn await_final_treats_a_pinned_wound_as_aborted() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, _t) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"local-wound".to_vec());
        mon.begin_tx(&tx);

        let observed = mon
            .inner
            .tl
            .commit_status_at(&tx, Requirement::Any)
            .await
            .unwrap()
            .observation;
        assert_eq!(
            mon.try_wound_observed(&tx, &observed).await.unwrap().status,
            TxCommitStatus::Wounded
        );
        assert_eq!(mon.tx_status(&tx).await.unwrap(), TxCommitStatus::Wounded);
        assert_eq!(
            mon.await_tx_final(&tx).await.unwrap(),
            TxFinalStatus::Aborted
        );
    }

    #[tokio::test]
    async fn await_final_propagates_polling_errors() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        backend.set_before(|operation| {
            let fail = matches!(
                operation,
                BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
            );
            let future: HookFuture = Box::pin(async move {
                if fail {
                    Err(BackendError::Unavailable(
                        "injected transaction-status read failure".into(),
                    ))
                } else {
                    Ok(())
                }
            });
            future
        });
        let b: Arc<dyn Backend> = backend;
        let (mon, _t) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"remote".to_vec());

        assert!(matches!(
            mon.await_tx_final(&tx).await,
            Err(TransError::Storage(StorageError::Unavailable(_)))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn await_final_is_cancelled_when_dropped() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, _t) = new_test_monitor(b);
        let tx = TxId::from_bytes(b"pending".to_vec());
        mon.begin_tx(&tx);

        assert!(
            rt::timeout(Duration::from_millis(1), mon.await_tx_final(&tx),)
                .await
                .is_err()
        );
        assert!(
            mon.shard_for(&tx)
                .lock()
                .unwrap()
                .transactions
                .get(&tx)
                .is_some_and(|entry| entry.waiters.iter().all(|waiter| waiter.is_closed()))
        );

        mon.abort_owned_tx(&tx).await.unwrap();
        assert!(
            !mon.shard_for(&tx)
                .lock()
                .unwrap()
                .transactions
                .contains_key(&tx)
        );
    }

    #[tokio::test(start_paused = true)]
    async fn refresh_keeps_pending() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor(b.clone());
        let tx = TxId::from_bytes(b"tx1".to_vec());
        mon.begin_tx(&tx);
        mon.start_refresh_tx(&tx);

        // Advance well past the pending timeout. Refresh keeps it alive.
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

        let st = t.tl.commit_status_at(&tx, Requirement::Any).await.unwrap();
        assert_eq!(st.status, TxCommitStatus::Pending);

        // A separate monitor should still see it as pending (not expired).
        let (mon2, _t2) = new_test_monitor(b);
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);

        mon.abort_owned_tx(&tx).await.unwrap();
    }

    // Regression (review 1.1 / ADR-022): the lazily-materialized pending object
    // the refresher writes must carry the transaction's recorded lock set, so a
    // dead pending transaction still describes its own back-references for GC to
    // prune. Recording locks before the refresher fires must land on the object.
    #[tokio::test(start_paused = true)]
    async fn refresh_records_locks() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor(b.clone());
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

        mon.abort_owned_tx(&tx).await.unwrap();
    }

    // ADR-024: a peer that repeatedly polls a *live* holder over a span far
    // beyond the pending timeout never reclaims it, because the refresher bumps
    // the lease timestamp halfway through each interval, so the observer always
    // sees progress (neither the absolute lease nor the relative no-progress
    // check fires).
    #[tokio::test(start_paused = true)]
    async fn live_holder_not_reclaimed_across_long_wait() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, _t) = new_test_monitor(b.clone());
        let (observer, _o) = new_test_monitor(b.clone());
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

        mon.abort_owned_tx(&tx).await.unwrap();
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
        let timing = ProtocolTiming::new(Duration::from_nanos(1), Duration::from_secs(30));
        let (mon, t) = new_test_monitor_with_timing(b.clone(), timing);
        let tx = TxId::from_bytes(b"dead".to_vec());

        // A pending object stamped "now" that never refreshes (a crashed
        // holder). Its absolute lease includes both the pending timeout and
        // skew allowance, so only the relative check can reclaim it sooner.
        let mut tl = TxLog::new(tx.clone(), TxCommitStatus::Pending);
        tl.timestamp = Some(rt::system_now());
        t.tl.set(&tl).await.unwrap();

        // First sight records the progress baseline; still pending.
        assert_eq!(mon.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);

        // No progress for longer than the timeout on the observer's own clock.
        rt::yield_now().await;

        // The stalled holder is pinned as wounded, well before its absolute
        // lease would have expired.
        assert_eq!(mon.tx_status(&tx).await.unwrap(), TxCommitStatus::Wounded);
    }

    #[tokio::test(start_paused = true)]
    async fn unknown_recheck_preserves_a_concurrent_commit() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let timing = ProtocolTiming::new(Duration::from_nanos(1), Duration::ZERO);
        let (observer, _o) = new_test_monitor_with_timing(b.clone(), timing);
        let (_owner, owner) = new_test_monitor(b.clone());
        let tx = TxId::from_bytes(b"committed-during-unknown-grace".to_vec());

        assert_eq!(
            observer.tx_status(&tx).await.unwrap(),
            TxCommitStatus::Pending
        );
        rt::yield_now().await;

        owner
            .tl
            .set(&TxLog::new(tx.clone(), TxCommitStatus::Ok))
            .await
            .unwrap();

        assert_eq!(observer.tx_status(&tx).await.unwrap(), TxCommitStatus::Ok);
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
    async fn try_wound_observed_preserves_an_immutable_observation() {
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
            mon.try_wound_observed(&tx, &committed)
                .await
                .unwrap()
                .status,
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
