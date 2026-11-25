//! Transaction lifecycle monitor. Ported from the Go `internal/trans/monitor.go`.
//!
//! Tracks local and remote transaction state, refreshes pending records to keep
//! locks alive, aborts expired remote transactions, and lets callers wait for a
//! transaction to finalize.

use std::collections::{HashMap, hash_map::Entry};
use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::{Duration, SystemTime};

use glassdb_concurr::{Background, Backoff, RetrySchedule, rt, shard::Sharded};
use glassdb_data::{CollectionAddress, LeafRef, LogicalKey, TxId};
use glassdb_storage::transaction::{
    TxCollectionChange, TxCommitStatus, TxLifecycleRelation, TxLock, TxRecord, TxRecordState,
    TxRecordStore, TxStatus,
};
use glassdb_storage::{LockType, Observation, Requirement, StorageError, Timeline};
use hashlink::{LinkedHashMap, LinkedHashSet};
use tokio::sync::oneshot;

use crate::error::TransError;
use crate::gc::GcHints;

mod final_status;

use final_status::{FinalStatusCache, KnownFinal};

const FINAL_STATUS_CACHE_SIZE: usize = 16384;

/// How long the identities of the owner operations of this database instance
/// stay recent local identities (ADR-085). It covers the recency window of the
/// writers of leaf entries.
pub(crate) const LOCAL_RETENTION: Duration = Duration::from_secs(60);

/// The recent local identities that one database instance keeps. At a rate of
/// owner operations that fills it before [`LOCAL_RETENTION`], the oldest
/// identities stop being local early. Read-only identities fill it too, but no
/// leaf entry names them as its writer.
const MAX_RECENT_LOCAL: usize = 65_536;

/// The retired identities without a record that one database instance keeps.
/// When it is full, the oldest identity loses its retirement proof, and a
/// wound of it stays pinned.
const MAX_RETIRED_WITHOUT_RECORD: usize = 65_536;

/// The number of lock sets of identities that one database instance pinned as
/// wounded and keeps until GC releases them. A set is the lock set of an
/// identity that this instance owned and pinned, or the lock list of the
/// pending record of an identity that it wounded after its lease expired. That
/// list can be older than the lock set of its owner. The limit is on the count
/// of sets, not on their size. When it is full, the oldest set is dropped, and
/// its locks stay until a writer on the same key removes them.
const MAX_PINNED_LOCK_SETS: usize = 4_096;

/// Final-status cache activity and the liveness decisions of the monitor, for
/// one snapshot or accumulated interval.
///
/// Each final-status cache lookup counts once. Reads from local owner state
/// bypass this cache and do not count.
///
/// Each liveness decision that lets this database instance wound a foreign
/// identity counts once, also when the wound then finds the identity final.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MonitorStats {
    /// Final-status cache lookups that found a committed, wounded, or aborted
    /// status.
    pub hits: u64,
    /// Final-status cache lookups that found no status.
    pub misses: u64,
    /// Pending records whose lease expired by its timestamp, which includes
    /// the allowed clock skew.
    pub lease_expiries: u64,
    /// Pending records whose lease had not expired by its timestamp, but that
    /// this database instance saw unchanged for one pending timeout on its
    /// own clock.
    pub observed_lease_expiries: u64,
    /// Identities that this database instance saw with no transaction record
    /// for one pending timeout on its own clock.
    pub missing_record_expiries: u64,
}

impl AddAssign for MonitorStats {
    fn add_assign(&mut self, rhs: Self) {
        // Destructuring fails to compile when a field is added and not summed.
        let Self {
            hits,
            misses,
            lease_expiries,
            observed_lease_expiries,
            missing_record_expiries,
        } = rhs;
        self.hits += hits;
        self.misses += misses;
        self.lease_expiries += lease_expiries;
        self.observed_lease_expiries += observed_lease_expiries;
        self.missing_record_expiries += missing_record_expiries;
    }
}

impl Sub for MonitorStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self {
            hits: self.hits.saturating_sub(rhs.hits),
            misses: self.misses.saturating_sub(rhs.misses),
            lease_expiries: self.lease_expiries.saturating_sub(rhs.lease_expiries),
            observed_lease_expiries: self
                .observed_lease_expiries
                .saturating_sub(rhs.observed_lease_expiries),
            missing_record_expiries: self
                .missing_record_expiries
                .saturating_sub(rhs.missing_record_expiries),
        }
    }
}

/// Timing parameters for transaction liveness and recovery.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProtocolTiming {
    pending_timeout: Duration,
    max_clock_skew: Duration,
}

impl ProtocolTiming {
    /// Creates a timing profile with an explicit pending-transaction timeout
    /// and maximum expected clock skew between database instances.
    ///
    /// `max_clock_skew` must conservatively bound the clocks of every database
    /// instance using the database; underestimating it can reclaim a live transaction.
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

    /// Returns the timing profile used by deterministic simulation.
    ///
    /// The retention period is short, so that GC deletes transaction records
    /// while cached leaves, stale views, and late CASes still name them.
    /// Neither GC safety nor commit recovery depends on these values (ADR-079,
    /// ADR-080). The skew is not zero, so that the lease check on the foreign
    /// timestamp and the check on the observer's clock expire at different
    /// times, as they do in production.
    pub const fn simulation() -> Self {
        Self::new(Duration::from_millis(50), Duration::from_millis(50))
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
    /// and GC safety horizons.
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
    last_observation: Option<Observation<TxRecord>>,
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
enum CommitWrite {
    #[default]
    NotStarted,
    InFlight,
    Finished,
    /// Commit recovery proved that the commit did not land.
    Lost,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct OwnerLifecycle {
    operations: OwnerOperations,
    admission: OwnerAdmission,
    commit_write: CommitWrite,
    /// A create of the committed record ended in doubt, so it can still land
    /// on an empty path. No later write can clear this.
    commit_create_may_land: bool,
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
    BlindWounded,
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

    fn begin_commit_write(&mut self) -> Result<(), TransError> {
        if self.admission == OwnerAdmission::Closed {
            return Err(TransError::AlreadyFinalized);
        }
        self.commit_write = CommitWrite::InFlight;
        Ok(())
    }

    fn finish_commit_write(&mut self, create_may_land: bool, lost: bool) {
        debug_assert_eq!(
            self.commit_write,
            CommitWrite::InFlight,
            "commit write finished without a matching begin"
        );
        self.commit_write = if lost {
            CommitWrite::Lost
        } else {
            CommitWrite::Finished
        };
        self.commit_create_may_land |= create_may_land;
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
        let commit_write_started = self.commit_write != CommitWrite::NotStarted;
        let commit_write_fenced = match self.commit_write {
            CommitWrite::NotStarted => true,
            // GC can delete an acknowledged abort, and then a delayed create
            // lands on the empty path (ADR-059).
            _ if self.commit_create_may_land => false,
            CommitWrite::Lost => true,
            // A wound changes the revision that a conditional commit expects.
            // A create in flight is not fenced by a wound, but its owner
            // operation is still active or unresolved, so the retirement proof
            // already refuses an acknowledgement. A blind wound can follow a
            // landed commit whose record GC reclaimed, so only commit recovery
            // can prove that it fenced the commit (ADR-104).
            CommitWrite::InFlight | CommitWrite::Finished => record == OwnerRecordState::Wounded,
        };

        match reason {
            OwnerCloseReason::Preemption => {
                if record != OwnerRecordState::NotEngaged
                    && retirement == OwnerRetirementProof::Guarded
                    && commit_write_fenced
                {
                    OwnerClosePlan::Transition(AbortTransition::Acknowledge)
                } else {
                    OwnerClosePlan::Transition(AbortTransition::EnsureWounded)
                }
            }
            OwnerCloseReason::OwnerAbort
                if commit_write_started
                    && !matches!(
                        record,
                        OwnerRecordState::Wounded | OwnerRecordState::BlindWounded
                    ) =>
            {
                if retirement != OwnerRetirementProof::Unavailable {
                    OwnerClosePlan::PreserveCommit
                } else {
                    OwnerClosePlan::Transition(AbortTransition::WoundIfPresent)
                }
            }
            OwnerCloseReason::OwnerAbort => {
                if retirement != OwnerRetirementProof::Unavailable && commit_write_fenced {
                    OwnerClosePlan::Transition(AbortTransition::Acknowledge)
                } else {
                    OwnerClosePlan::Transition(AbortTransition::EnsureWounded)
                }
            }
        }
    }

    /// Reports whether the owner can prove that no operation of the identity
    /// is active or can still publish an effect.
    fn is_quiescent(&self) -> bool {
        matches!(
            self.operations,
            OwnerOperations::Internal
                | OwnerOperations::Guarded {
                    active: 0,
                    unresolved: false,
                }
        )
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

    /// Reports whether the identity sent no record write, holds no recovery
    /// manifest, and has a retirement proof. Then a later wound of it can be
    /// acknowledged after its owner state is gone (ADR-059).
    fn retires_without_record(&self) -> bool {
        let Some(record) = &self.record else {
            return false;
        };
        record.status == TxCommitStatus::Pending
            && record.last_observation.is_none()
            && record.refresh_state == RefreshState::NotStarted
            && record.recovery == TxRecoveryManifest::default()
            && self.lifecycle.commit_write == CommitWrite::NotStarted
            && self.lifecycle.is_quiescent()
    }

    fn close(&mut self, reason: OwnerCloseReason) -> OwnerClosePlan {
        let record = match self.record.as_mut() {
            Some(record) => {
                record.refresh_state = RefreshState::Stopped;
                match record.status {
                    TxCommitStatus::Wounded => OwnerRecordState::Wounded,
                    TxCommitStatus::BlindWounded => OwnerRecordState::BlindWounded,
                    _ => OwnerRecordState::Other,
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
    Rejected,
    InDoubt { started_at: rt::Instant },
}

/// A failure to persist the committed record.
struct CommitWriteError {
    error: TransError,
    /// A create of the record ended in doubt, so it can still land.
    create_may_land: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CommitResolution {
    Committed,
    /// The commit landed, and GC reclaimed its record.
    Reclaimed,
    Retry,
    AlreadyFinalized,
    InDoubt,
}

struct PendingWrite {
    record: TxRecord,
    expected: Option<Observation<TxRecord>>,
}

#[derive(Default)]
struct State {
    transactions: HashMap<TxId, TxRuntimeEntry>,
    // The identities of the recent owner operations, oldest first, with the
    // instant of their last operation (ADR-085).
    recent_local: LinkedHashMap<TxId, rt::Instant>,
    // The identities that retired without a record and with a retirement
    // proof, oldest first. GC acknowledges a later wound of one of them.
    retired_without_record: LinkedHashSet<TxId>,
    // The lock sets of the identities that this instance pinned as wounded,
    // oldest first. GC releases each set until one release completes.
    pinned_locks: LinkedHashMap<TxId, Vec<TxLock>>,
}

impl State {
    /// Makes `tid` the newest recent local identity of the shard, which keeps
    /// at most `capacity` of them.
    fn note_recent_local(&mut self, tid: TxId, capacity: usize) {
        let now = rt::Instant::now();
        self.recent_local.remove(&tid);
        self.recent_local.insert(tid, now);
        while let Some((_, at)) = self.recent_local.front()
            && (self.recent_local.len() > capacity
                || now.saturating_duration_since(*at) >= LOCAL_RETENTION)
        {
            self.recent_local.pop_front();
        }
    }

    /// Keeps the lock set of the pinned identity `tid`, in a shard that keeps
    /// at most `capacity` of them.
    fn note_pinned_locks(&mut self, tid: TxId, locks: Vec<TxLock>, capacity: usize) {
        self.pinned_locks.insert(tid, locks);
        while self.pinned_locks.len() > capacity {
            self.pinned_locks.pop_front();
        }
    }

    /// Keeps the retirement proof of `tid`, in a shard that keeps at most
    /// `capacity` of them.
    fn note_retired_without_record(&mut self, tid: TxId, capacity: usize) {
        self.retired_without_record.insert(tid);
        while self.retired_without_record.len() > capacity {
            self.retired_without_record.pop_front();
        }
    }
}

struct Inner {
    tx_records: TxRecordStore,
    timeline: Timeline,
    final_status: Mutex<FinalStatusCache>,
    // Weak so a `Monitor` clone captured inside a spawned task does not keep
    // the [`Background`] alive across DB shutdown. `Engine` is the single
    // strong owner.
    background: Weak<Background>,
    retry: RetrySchedule,
    timing: ProtocolTiming,
    // Reports the wounds whose lock sets GC must release.
    gc_hints: GcHints,
    // Runtime entries are partitioned into independent shards keyed by tid.
    // One lock keeps ownership, liveness, and waiter updates atomic for a given
    // transaction while the role enum prevents owned and foreign state from
    // coexisting.
    shards: Sharded<Mutex<State>>,
    expiries: ExpiryCounters,
}

/// The liveness decisions that [`MonitorStats`] reports.
#[derive(Default)]
struct ExpiryCounters {
    lease: AtomicU64,
    observed_lease: AtomicU64,
    missing_record: AtomicU64,
}

impl ExpiryCounters {
    fn take(&self) -> MonitorStats {
        MonitorStats {
            lease_expiries: self.lease.swap(0, Ordering::Relaxed),
            observed_lease_expiries: self.observed_lease.swap(0, Ordering::Relaxed),
            missing_record_expiries: self.missing_record.swap(0, Ordering::Relaxed),
            ..MonitorStats::default()
        }
    }
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
    /// The abort is final but pinned because owner work remains unresolved.
    Pinned,
    /// The transaction committed before owner-side retirement won.
    Committed,
    /// A commit write was dispatched, so the abort must preserve its outcome.
    CommitOutcomePreserved,
    /// The Monitor had already stopped tracking the transaction.
    AlreadyFinished,
}

/// Durable recovery manifest needed to recover a pending transaction after its
/// owner disappears.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct TxRecoveryManifest {
    pub(crate) locks: Vec<TxLock>,
    pub(crate) collection_changes: Vec<TxCollectionChange>,
    pub(crate) prepared_collections: Vec<CollectionAddress>,
}

impl TxRecoveryManifest {
    /// Extracts the durable recovery manifest from a transaction record.
    #[cfg(test)]
    pub(crate) fn from_record(record: &TxRecord) -> Self {
        Self {
            locks: record.locks.clone(),
            collection_changes: record.collection_changes.clone(),
            prepared_collections: record.prepared_collections.clone(),
        }
    }

    /// Applies the durable recovery manifest to a transaction record.
    pub(crate) fn apply_to(self, record: &mut TxRecord) {
        record.locks = self.locks;
        record.collection_changes = self.collection_changes;
        record.prepared_collections = self.prepared_collections;
    }

    /// Adds `locks` to the lock set of the identity (ADR-095). A lock on a
    /// target that the set already has replaces it only when the new lock is a
    /// write lock.
    fn add_locks(&mut self, locks: impl IntoIterator<Item = TxLock>) {
        let mut index: HashMap<LockTarget, usize> = self
            .locks
            .iter()
            .enumerate()
            .map(|(i, lock)| (LockTarget::of(lock), i))
            .collect();
        for lock in locks {
            match index.entry(LockTarget::of(&lock)) {
                Entry::Occupied(entry) => {
                    if lock.typ() == LockType::Write {
                        self.locks[*entry.get()] = lock;
                    }
                }
                Entry::Vacant(entry) => {
                    entry.insert(self.locks.len());
                    self.locks.push(lock);
                }
            }
        }
    }
}

/// The object that a recorded lock locks, without its lock type.
#[derive(PartialEq, Eq, Hash)]
enum LockTarget {
    Key(LogicalKey),
    Membership(LeafRef),
    Directory(CollectionAddress),
    TopologyParticipant(CollectionAddress),
}

impl LockTarget {
    fn of(lock: &TxLock) -> Self {
        match lock {
            TxLock::Key { key, .. } => Self::Key(key.clone()),
            TxLock::Membership { leaf, .. } => Self::Membership(leaf.clone()),
            TxLock::Directory { collection, .. } => Self::Directory(collection.clone()),
            TxLock::TopologyParticipant { collection } => {
                Self::TopologyParticipant(collection.clone())
            }
        }
    }
}

/// A transaction status known to be durable and final.
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
}

/// What this database instance knows about the value of a transaction for one
/// key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum CommittedValue {
    /// The status, and the value when the transaction committed.
    Known(KeyCommitStatus),
    /// A blind wound, or a committed record that GC reclaimed. The transaction
    /// holds no value in a current object, or in an observation that reaches
    /// `after`. An older observation can still name it where it held a value
    /// (ADR-104).
    NoValueAfter {
        status: TxCommitStatus,
        after: Requirement,
    },
}

/// The final record of a transaction, or the requirement after which GC
/// reclaimed it.
enum FinalRecord {
    Present(Observation<TxRecord>),
    Reclaimed(Requirement),
}

/// Transaction status together with the exact evidence used to resolve it.
struct TxStatusEvidence {
    /// The persisted status, or `None` for a missing record.
    stored: Option<TxCommitStatus>,
    observation: Option<Observation<TxRecord>>,
}

impl TxStatusEvidence {
    fn local(record: Option<&TxStatusEntry>) -> Result<Self, TransError> {
        match record {
            Some(record) => Self::new(Some(record.status), record.last_observation.clone()),
            // An owner operation may precede entry into the locked commit.
            // Like a freshly observed absent foreign record, it is not yet
            // final and is exposed as pending.
            None => Self::new(None, None),
        }
    }

    fn observed(status: TxStatus) -> Result<Self, TransError> {
        let stored = status.observation.value().map(|record| record.status);
        Self::new(stored, Some(status.observation))
    }

    fn cached_final(status: TxCommitStatus) -> Result<Self, TransError> {
        Self::new(Some(status), None)
    }

    fn new(
        stored: Option<TxCommitStatus>,
        observation: Option<Observation<TxRecord>>,
    ) -> Result<Self, TransError> {
        // Rejects `Unknown`, which is never a persisted status.
        TxRecordState::try_from_status(stored)?;
        Ok(Self {
            stored,
            observation,
        })
    }

    fn status(&self) -> TxCommitStatus {
        match self.stored {
            // Missing is exposed as pending only during the observer-relative
            // appearance grace. Once the grace expires, resolution first pins
            // the identity as wounded and returns that exact observation.
            None => TxCommitStatus::Pending,
            Some(status) => status,
        }
    }
}

impl Monitor {
    /// Creates a monitor with retry-backoff and transaction-liveness timing.
    /// The retry config tunes the backoff used when polling a peer
    /// transaction's commit status and when writing a transaction's final record.
    pub fn with_config(
        tx_records: TxRecordStore,
        timeline: Timeline,
        background: Weak<Background>,
        retry: RetrySchedule,
        timing: ProtocolTiming,
        gc_hints: GcHints,
    ) -> Self {
        Monitor {
            inner: Arc::new(Inner {
                tx_records,
                timeline,
                final_status: Mutex::new(FinalStatusCache::new(FINAL_STATUS_CACHE_SIZE)),
                background,
                retry,
                timing,
                gc_hints,
                shards: Sharded::new(|_| Mutex::new(State::default())),
                expiries: ExpiryCounters::default(),
            }),
        }
    }

    /// Returns final-status cache activity and liveness decisions since the
    /// previous sample.
    pub(crate) fn stats_and_reset(&self) -> MonitorStats {
        let mut stats = self.inner.final_status.lock().unwrap().take_stats();
        stats += self.inner.expiries.take();
        stats
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
        let capacity = (MAX_RECENT_LOCAL / self.inner.shards.len()).max(1);
        st.note_recent_local(*tid, capacity);
        let owned = match st.transactions.entry(*tid) {
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
            tid: *tid,
            completed: false,
        })
    }

    /// Reports whether the identity `tid` opened an owner operation in this
    /// database instance less than [`LOCAL_RETENTION`] ago (ADR-085).
    pub(crate) fn is_recent_local(&self, tid: &TxId) -> bool {
        self.shard_for(tid)
            .lock()
            .unwrap()
            .recent_local
            .get(tid)
            .is_some_and(|at| rt::Instant::now().saturating_duration_since(*at) < LOCAL_RETENTION)
    }

    /// Whether this database instance still tracks `tid` as one of its locked-commit identities.
    /// A wounded identity remains tracked until its owner acknowledges it or
    /// cancellation recovery releases local ownership. Transactions that never
    /// engage the locked commit are not tracked.
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

    /// Adds `locks` to the lock set of an owned identity and returns the whole
    /// set (ADR-095). The owner adds a lock before it sends the CAS that takes
    /// it, so every record that the owner writes lists the lock even if the CAS
    /// lands late. The set keeps the locks of every commit pass of the
    /// identity. Returns only `locks` if the transaction is no longer tracked.
    pub(crate) fn add_tx_locks(
        &self,
        tid: &TxId,
        locks: impl IntoIterator<Item = TxLock>,
    ) -> Vec<TxLock> {
        let mut st = self.shard_for(tid).lock().unwrap();
        let mut recovery = match st.transactions.get_mut(tid) {
            Some(TxRuntimeEntry {
                role: TxRuntimeRole::Owned(owned),
                ..
            }) => match owned.record.as_mut() {
                Some(record) => {
                    record.recovery.add_locks(locks);
                    return record.recovery.locks.clone();
                }
                None => TxRecoveryManifest::default(),
            },
            _ => TxRecoveryManifest::default(),
        };
        recovery.add_locks(locks);
        recovery.locks
    }

    /// Starts a background task that periodically refreshes the pending record so
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
        let tid = *tid;
        bg.spawn(async move {
            m.refresh_pending(tid).await;
        });
    }

    /// Marks the transaction committed, writing the final transaction record
    /// (if it produced any writes or held any locks), updating local storage,
    /// and notifying waiters.
    pub(crate) async fn commit_tx(&self, record: TxRecord) -> Result<(), TransError> {
        let tid = record.id;
        // In v2 the transaction record is the value store: it must be persisted
        // whenever the transaction has writes (the committed values readers
        // help-forward) or a recovery manifest. A read-only transaction
        // carries neither, so it skips the write entirely — its in-memory
        // bookkeeping is simply cleared below. This is the create-or-flip commit
        // point: `persist_committed_record` creates the committed record when no
        // pending one was written (the short-transaction case where the lazy
        // refresh never fired), or CASes pending -> committed otherwise.
        if !record.locks.is_empty()
            || !record.writes.is_empty()
            || !record.collection_changes.is_empty()
            || !record.prepared_collections.is_empty()
        {
            // This handshake shares the owner-state lock with local wounding.
            // Either the wound closes admission first, or the commit is marked
            // as dispatched before a local task can report safe retirement.
            self.begin_commit_write(&tid)?;
            self.stop_tx_refresh(&tid);
            let result = self.persist_committed_record(record).await;
            let create_may_land = result.as_ref().is_err_and(|error| error.create_may_land);
            let lost = result
                .as_ref()
                .is_err_and(|error| matches!(error.error, TransError::AlreadyFinalized));
            self.finish_commit_write(&tid, create_may_land, lost);
            // `context` preserves the `AlreadyFinalized` sentinel so the commit
            // path can recognize an abort-side final status, as well as any
            // classification of an escaping error.
            // In-doubt outcomes are normally retried inside
            // `persist_committed_record` because the record is keyed by transaction
            // identity and the write is idempotent.
            result.map_err(|error| error.error.context("writing transaction record"))?;
            self.finish_local_tx(&tid);
        } else {
            self.finish_tx_without_record(&tid);
        }
        Ok(())
    }

    /// Acknowledges the wound `observed` of an identity that retired in this
    /// database instance without a record. Returns the `Aborted` status, or
    /// `None` when this instance has no retirement proof for the identity.
    ///
    /// A peer can wound such an identity after it retired, through an old
    /// view of one of its holders. Its owner state is gone, so only this
    /// proof lets the wound be acknowledged (ADR-059).
    pub(crate) async fn acknowledge_retired_wound(
        &self,
        tid: &TxId,
        observed: &Observation<TxRecord>,
    ) -> Result<Option<TxStatus>, TransError> {
        let retired = self
            .shard_for(tid)
            .lock()
            .unwrap()
            .retired_without_record
            .contains(tid);
        if !retired {
            return Ok(None);
        }
        // The proof stays until eviction, so a later wound of the same
        // identity is acknowledged too.
        let (status, _) = self
            .advance_abort_transition(tid, observed, AbortTransition::Acknowledge)
            .await?;
        Ok((status.status == TxCommitStatus::Aborted).then_some(status))
    }

    /// Aborts a transaction owned by this Database, deriving the strongest safe
    /// durable state from tracked owner activity.
    ///
    /// Quiescent work may acknowledge `Aborted`; a dropped operation remains
    /// pinned as `Wounded`. An unresolved commit write stays in doubt
    /// (ADR-057) instead of manufacturing an abort-side record.
    pub(crate) async fn abort_owned_tx(&self, tid: &TxId) -> Result<OwnerAbortOutcome, TransError> {
        let transition = match self.owner_close_plan(tid) {
            OwnerClosePlan::Transition(transition) => transition,
            OwnerClosePlan::PreserveCommit => {
                // The owner has stopped, but its commit outcome is unknown.
                // Retaining local Pending state would keep waiters from using
                // durable recovery after the refresher has stopped.
                self.finish_local_tx(tid);
                return Ok(OwnerAbortOutcome::CommitOutcomePreserved);
            }
            OwnerClosePlan::AlreadyFinished => return Ok(OwnerAbortOutcome::AlreadyFinished),
        };
        let mut status = match self.owned_wound_observation(tid) {
            Some(observation) => TxStatus::from_observation(observation),
            None => {
                self.inner
                    .tx_records
                    .commit_status_at(tid, self.current_requirement())
                    .await?
            }
        };
        let mut backoff = self.inner.retry.backoff();
        loop {
            if transition == AbortTransition::WoundIfPresent && status.observation.is_absent() {
                return Err(in_doubt(format!(
                    "transaction {tid} owner operation was dropped after commit write dispatch"
                )));
            }
            status = self
                .advance_abort_transition(tid, &status.observation, transition)
                .await?
                .0;
            let outcome = match status.status {
                TxCommitStatus::Committed => Some(OwnerAbortOutcome::Committed),
                TxCommitStatus::Aborted => Some(OwnerAbortOutcome::Acknowledged),
                TxCommitStatus::Wounded | TxCommitStatus::BlindWounded
                    if transition != AbortTransition::Acknowledge =>
                {
                    Some(OwnerAbortOutcome::Pinned)
                }
                TxCommitStatus::Wounded
                | TxCommitStatus::BlindWounded
                | TxCommitStatus::Pending
                | TxCommitStatus::Unknown => None,
            };
            if let Some(outcome) = outcome {
                if outcome == OwnerAbortOutcome::Pinned {
                    self.finish_pinned_tx(tid);
                } else {
                    self.finish_local_tx(tid);
                }
                return Ok(outcome);
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    /// Preempts a transaction under wound-wait, returning the durable final
    /// status that wins.
    ///
    /// A committed transaction is left untouched. A pending refresh does not
    /// defeat a wound: after each rejected wound, the wound is sent again
    /// until either the abort lands or the owner commits.
    pub(crate) async fn preempt_tx(&self, tid: &TxId) -> Result<TxFinalStatus, TransError> {
        let transition = self.preemption_plan(tid);
        // Proof: conditional mutation (the wound expects the observed
        // revision, and a rejection reads the record again at a new barrier).
        // A final status never changes. A caller usually read the holder just
        // before, so the cache holds that observation.
        let mut status = self
            .inner
            .tx_records
            .commit_status_at(tid, Requirement::ANY)
            .await
            .map_err(|e| {
                TransError::Storage(e.context(format!("reading status of wound target {tid}")))
            })?;
        let mut backoff = self.inner.retry.backoff();
        loop {
            let settled = self
                .advance_abort_transition(tid, &status.observation, transition)
                .await?
                .0;
            match settled.status {
                TxCommitStatus::Committed => return Ok(TxFinalStatus::Committed),
                TxCommitStatus::Aborted => return Ok(TxFinalStatus::Aborted),
                // The caller removes the holder with a CAS on the current
                // object, so a blind wound holds no committed effect there.
                TxCommitStatus::Wounded | TxCommitStatus::BlindWounded
                    if transition != AbortTransition::Acknowledge =>
                {
                    return Ok(TxFinalStatus::Aborted);
                }
                TxCommitStatus::Wounded
                | TxCommitStatus::BlindWounded
                | TxCommitStatus::Pending
                | TxCommitStatus::Unknown => {}
            }
            // Lease expiry must stop here because the refresh proves liveness.
            // Wound-wait is authorized by priority instead, so the wound is
            // sent again. A live owner refreshes its record during the wait,
            // so the next wound expects a status read after the wait.
            rt::sleep(backoff.next_delay()).await;
            status = self
                .read_tx_status_retrying_unavailable(tid, &mut backoff)
                .await?;
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

    /// Returns the final status of `tid` if this database instance already
    /// knows it, without backend I/O. `None` does not mean that the identity
    /// is live.
    pub(crate) fn known_final_status(&self, tid: &TxId) -> Option<TxCommitStatus> {
        match self.owned_status_evidence(tid) {
            Ok(Some(evidence)) => {
                let status = evidence.status();
                return status.is_final().then_some(status);
            }
            Ok(None) => {}
            Err(_) => return None,
        }
        self.cached_final_status(tid)
    }

    /// Checks for a durable wound that local owner state may not yet reflect.
    pub(crate) async fn has_durable_wound(&self, tid: &TxId) -> Result<bool, TransError> {
        let status = self
            .inner
            .tx_records
            .commit_status_at(tid, self.current_requirement())
            .await?;
        // The owner asks before its commit write, so it has not committed and
        // a blind wound is a wound of this identity too.
        let wounded = matches!(
            status.status,
            TxCommitStatus::Wounded | TxCommitStatus::BlindWounded | TxCommitStatus::Aborted
        );
        if wounded {
            self.record_durable_observation(tid, &status.observation);
        }
        Ok(wounded)
    }

    /// Waits for and returns a transaction's durable final status.
    pub(crate) async fn await_tx_final(&self, tid: &TxId) -> Result<TxFinalStatus, TransError> {
        // A stale pending status only delays the return by one poll, and a
        // final status never changes. A caller usually read the holder just
        // before, so the cache holds that observation. The resolutions after a
        // wait are current.
        let mut requirement = Requirement::ANY;
        loop {
            // Resolve before registering a waiter so local state retains
            // precedence over a potentially newer final-status cache entry.
            match self.tx_status_at(tid, requirement).await? {
                TxCommitStatus::Committed => return Ok(TxFinalStatus::Committed),
                // A blind holder cannot commit any more, so a waiter can go on.
                // It decides from the current object, which holds no committed
                // effect of a blind-wounded holder.
                TxCommitStatus::Aborted
                | TxCommitStatus::Wounded
                | TxCommitStatus::BlindWounded => {
                    return Ok(TxFinalStatus::Aborted);
                }
                TxCommitStatus::Pending | TxCommitStatus::Unknown => {}
            }

            // Notifications and poll failures are only wake-up hints. Resolve
            // again before returning so a cancelled poll cannot decide status.
            self.wait_for_tx_change(tid).await;
            requirement = self.current_requirement();
        }
    }

    /// Returns the first requirement at which an observation shows no value
    /// of `tid`, after this instance saw a blind wound of `tid` or found its
    /// committed record reclaimed (ADR-104). If this instance lost it, returns
    /// a requirement after now: the next read of the record learns a newer one.
    pub(crate) fn no_value_requirement(&self, tid: &TxId) -> Requirement {
        self.inner
            .final_status
            .lock()
            .unwrap()
            .no_value_after(tid)
            .unwrap_or_else(|| self.current_requirement())
    }

    /// Returns what this instance knows about the value of `tid` for `key`,
    /// from a status read after the evidence that this instance has.
    pub(crate) async fn committed_value(
        &self,
        key: &LogicalKey,
        tid: &TxId,
    ) -> Result<CommittedValue, TransError> {
        self.committed_value_at(key, tid, self.current_requirement())
            .await
    }

    /// Returns what this instance knows about the value of `tid` for `key`.
    /// The status read reaches `requirement`.
    pub(crate) async fn committed_value_at(
        &self,
        key: &LogicalKey,
        tid: &TxId,
        requirement: Requirement,
    ) -> Result<CommittedValue, TransError> {
        let evidence = self.resolve_tx_status_at(tid, requirement).await?;
        let status = evidence.status();
        let known = |value| Ok(CommittedValue::Known(KeyCommitStatus { status, value }));
        if !matches!(
            status,
            TxCommitStatus::Committed | TxCommitStatus::BlindWounded
        ) {
            return known(TValue::default());
        }
        let after = self.no_value_after(tid, &evidence);
        if after.is_some() || status == TxCommitStatus::BlindWounded {
            // Without the requirement of a blind wound, the next read of the
            // record learns a newer one.
            let after = after.unwrap_or_else(|| self.current_requirement());
            return Ok(CommittedValue::NoValueAfter { status, after });
        }

        let observed = match self.final_record(tid, status, evidence.observation).await? {
            FinalRecord::Present(observed) => observed,
            FinalRecord::Reclaimed(after) => {
                return Ok(CommittedValue::NoValueAfter { status, after });
            }
        };
        let record = observed
            .value()
            .ok_or_else(|| TransError::other(format!("missing final record for {tid}")))?;
        let value = match record.writes.iter().find(|entry| &entry.key == key) {
            Some(entry) => TValue {
                value: entry.value.clone(),
                deleted: entry.deleted,
                not_written: false,
            },
            None => TValue {
                not_written: true,
                ..Default::default()
            },
        };
        known(value)
    }

    /// Conditionally pins an exact foreign transaction observation as wounded.
    /// The boolean reports a confirmed state change made by this call.
    async fn try_wound_observed(
        &self,
        tid: &TxId,
        expected: &Observation<TxRecord>,
    ) -> Result<(TxStatus, bool), TransError> {
        self.advance_abort_transition(tid, expected, AbortTransition::EnsureWounded)
            .await
    }

    /// Gives GC the lock list of the pending record `pending` that this
    /// instance wounded after its lease expired. The owner is probably gone
    /// and will not acknowledge. The list can be older than the lock set of
    /// the owner, so the release can miss locks (ADR-095).
    ///
    /// Only the instance whose wound write was confirmed gives the list. A
    /// wound write that was in doubt and landed reads back as an existing
    /// wound, so its release is lost.
    fn release_expired_locks(&self, tid: &TxId, pending: &Observation<TxRecord>) {
        let Some(record) = pending.value() else {
            return;
        };
        if record.locks.is_empty() {
            return;
        }
        let capacity = self.pinned_lock_capacity();
        self.shard_for(tid)
            .lock()
            .unwrap()
            .note_pinned_locks(*tid, record.locks.clone(), capacity);
        self.inner.gc_hints.schedule(*tid);
    }

    /// The number of pinned lock sets that one shard keeps.
    fn pinned_lock_capacity(&self) -> usize {
        (MAX_PINNED_LOCK_SETS / self.inner.shards.len()).max(1)
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
        let status = self
            .inner
            .tx_records
            .commit_status_at(tid, requirement)
            .await?;
        self.resolve_remote_tx_status(tid, status).await
    }

    /// Persists the commit decision and resolves in-doubt mutations while its
    /// durable record can still be read back (ADR-009, ADR-057).
    async fn persist_committed_record(&self, mut record: TxRecord) -> Result<(), CommitWriteError> {
        let tid = &record.id;
        record.status = TxCommitStatus::Committed;
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

        let mut create_may_land = false;
        // Whether this owner saw its own pending record. GC never deletes a
        // pending record, so a later empty path proves that this commit landed.
        let mut saw_pending = expected.as_ref().is_some_and(|observed| {
            observed
                .value()
                .is_some_and(|stored| stored.status == TxCommitStatus::Pending)
        });
        // The block keeps every early return inside it, so every error reports
        // a create that may still land.
        let result: Result<(), TransError> = async {
            let mut backoff = self.inner.retry.backoff();
            loop {
                let started_at = rt::Instant::now();
                let attempt = match &expected {
                    Some(observed) => self.inner.tx_records.set_if(&record, observed).await,
                    None => self.inner.tx_records.set(&record).await,
                };
                let failure = match attempt {
                    Ok(observed) => {
                        self.record_durable_observation(tid, &observed);
                        return Ok(());
                    }
                    // A clean rejection proves this write did not land. An
                    // unavailable result does not, so its read-back is bounded by
                    // the record's reclamation horizon.
                    Err(StorageError::Precondition) => CommitWriteFailure::Rejected,
                    Err(StorageError::Unavailable(_)) => {
                        create_may_land |= expected.is_none();
                        CommitWriteFailure::InDoubt { started_at }
                    }
                    Err(error) => return Err(error.into()),
                };
                let rejected = matches!(failure, CommitWriteFailure::Rejected);
                let status = match failure {
                    CommitWriteFailure::Rejected => {
                        self.read_tx_status_retrying_unavailable(tid, &mut backoff)
                            .await?
                    }
                    CommitWriteFailure::InDoubt { started_at } => {
                        self.read_in_doubt_commit_before_reclaim(tid, &mut backoff, started_at)
                            .await?
                    }
                };

                let record_state = TxRecordState::try_from_observation(&status.observation)?;
                let attempts = CommitAttempts {
                    saw_pending,
                    create_may_land,
                };
                match classify_commit_observation(record_state, attempts)? {
                    CommitResolution::Committed => {
                        self.record_durable_observation(tid, &status.observation);
                        return Ok(());
                    }
                    CommitResolution::Reclaimed => {
                        // The blind wound says less than this owner knows: the
                        // identity committed, and GC reclaimed its record.
                        let now = self.current_requirement();
                        self.inner
                            .final_status
                            .lock()
                            .unwrap()
                            .note_reclaimed(*tid, now);
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
                        // Pending proves only that the preceding attempt had not
                        // landed yet. If it lands later, the next write is rejected
                        // and its read-back decides the outcome.
                        expected = Some(status.observation);
                        saw_pending = true;
                        // Only this owner writes a pending record, and its
                        // lease refresh stopped before the first attempt. So a
                        // rejection lost to an earlier pending write of this
                        // owner: a refresh in flight, or an in-doubt write that
                        // landed late. No refresh follows, so write again at
                        // once. A backoff wait lets the lease expire, and a
                        // peer then wounds the transaction.
                        if !rejected {
                            rt::sleep(backoff.next_delay()).await;
                        }
                    }
                }
            }
        }
        .await;
        result.map_err(|error| CommitWriteError {
            error,
            create_may_land,
        })
    }

    /// Advances an abort-side lifecycle transition from an exact observation.
    async fn advance_abort_transition(
        &self,
        tid: &TxId,
        expected: &Observation<TxRecord>,
        transition: AbortTransition,
    ) -> Result<(TxStatus, bool), TransError> {
        let mut expected = expected.clone();
        let mut backoff = self.inner.retry.backoff();
        loop {
            let current = TxStatus::from_observation(expected.clone());
            let record_state = TxRecordState::try_from_observation(&current.observation)?;
            let target = match classify_abort_observation(transition, record_state) {
                AbortObservationAction::Settled => {
                    self.record_durable_observation(tid, &current.observation);
                    return Ok((current, false));
                }
                AbortObservationAction::Write(target) => target,
            };
            let abort_record = self.build_abort_record(tid, target);
            let r = if expected.is_absent() {
                self.inner.tx_records.set(&abort_record).await
            } else {
                self.inner.tx_records.set_if(&abort_record, &expected).await
            };
            match r {
                Ok(observed) => {
                    self.record_durable_observation(tid, &observed);
                    return Ok((TxStatus::from_observation(observed), true));
                }
                Err(StorageError::Precondition) => {
                    // The revision moved under us (a commit, a pending-record
                    // refresh, or another abort). A clean rejection is not
                    // retried here: lease-based callers must treat a refresh as
                    // proof of liveness, while wound-wait decides separately to
                    // retry it.
                    let st = self
                        .read_tx_status_retrying_unavailable(tid, &mut backoff)
                        .await?;
                    self.record_durable_observation(tid, &st.observation);
                    return Ok((st, false));
                }
                // In-doubt: the abort write may or may not have landed. Forcing
                // a not-yet-final record to an abort-side state is idempotent and
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
                        return Ok((st, false));
                    }
                    expected = st.observation;
                }
                Err(e) => return Err(e.into()),
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    /// Builds an abort-side transaction record. An `Aborted` record keeps the
    /// recovery manifest that GC needs to release the effects of the identity.
    fn build_abort_record(&self, tid: &TxId, target: TxCommitStatus) -> TxRecord {
        debug_assert!(matches!(
            target,
            TxCommitStatus::Aborted | TxCommitStatus::Wounded | TxCommitStatus::BlindWounded
        ));
        let mut abort_record = TxRecord::new(*tid, target);
        // Only the owner writes `Aborted`, and its own manifest is newer than
        // the stored one. GC reads nothing of a `Wounded` record past its
        // status, so a wound holds no manifest (ADR-095).
        if matches!(
            target,
            TxCommitStatus::Wounded | TxCommitStatus::BlindWounded
        ) {
            return abort_record;
        }
        if let Some(TxRuntimeEntry {
            role: TxRuntimeRole::Owned(owned),
            ..
        }) = self.shard_for(tid).lock().unwrap().transactions.get(tid)
            && let Some(record) = owned.record.as_ref()
        {
            record.recovery.clone().apply_to(&mut abort_record);
        }
        abort_record
    }

    /// Reads back an in-doubt commit write, giving up before its record usually
    /// becomes reclaimable.
    ///
    /// Correctness does not depend on this deadline: a missing record is always
    /// classified as in doubt. The deadline makes that outcome rare. GC
    /// reclaims a final transaction record only after its safety horizon has
    /// elapsed since the timestamp that write stamped (ADR-022), and that
    /// timestamp is never earlier than the attempt that may have landed it.
    /// Measuring from the attempt and omitting the skew allowance leaves at
    /// least GC's skew allowance, unless the owner pauses for longer.
    async fn read_in_doubt_commit_before_reclaim(
        &self,
        tid: &TxId,
        backoff: &mut Backoff,
        attempt_started: rt::Instant,
    ) -> Result<TxStatus, TransError> {
        // The deadline starts with the write because time spent waiting for its
        // in-doubt response also consumes the record's safety horizon.
        let remaining = self
            .inner
            .timing
            .pending_timeout()
            .saturating_sub(attempt_started.elapsed());
        if remaining.is_zero() {
            return Err(in_doubt("commit recovery deadline expired"));
        }
        tokio::select! {
            // A status that arrives at the deadline is too late to decide the outcome.
            biased;
            _ = rt::sleep(remaining) => Err(in_doubt("commit recovery deadline expired")),
            status = self.read_tx_status_retrying_unavailable(tid, backoff) => status,
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
                .tx_records
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

    /// Returns the durable observation of a wound on an owned transaction, when
    /// this Database already saw it.
    fn owned_wound_observation(&self, tid: &TxId) -> Option<Observation<TxRecord>> {
        let st = self.shard_for(tid).lock().unwrap();
        let TxRuntimeRole::Owned(owned) = &st.transactions.get(tid)?.role else {
            return None;
        };
        // Only the owner changes a `Wounded` record: other aborters settle on
        // it, a refresh or commit cannot replace it, and GC skips it
        // (ADR-071). So an old observation is still a correct CAS expectation,
        // and a stale one only fails the CAS and causes a read.
        owned
            .record
            .as_ref()?
            .last_observation
            .clone()
            .filter(|observation| {
                observation.value().is_some_and(|record| {
                    matches!(
                        record.status,
                        TxCommitStatus::Wounded | TxCommitStatus::BlindWounded
                    )
                })
            })
    }

    fn begin_commit_write(&self, tid: &TxId) -> Result<(), TransError> {
        let mut st = self.shard_for(tid).lock().unwrap();
        let owned = match st.transactions.entry(*tid) {
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
        owned.lifecycle.begin_commit_write()
    }

    fn finish_commit_write(&self, tid: &TxId, create_may_land: bool, lost: bool) {
        let mut st = self.shard_for(tid).lock().unwrap();
        if let Some(TxRuntimeEntry {
            role: TxRuntimeRole::Owned(owned),
            ..
        }) = st.transactions.get_mut(tid)
        {
            owned.lifecycle.finish_commit_write(create_may_land, lost);
        }
    }

    /// Returns the lock set that this instance kept when it pinned `tid` as
    /// wounded. The set stays until [`Self::forget_pinned_locks`], so a
    /// failed or cancelled release can run again.
    pub(crate) fn pinned_locks(&self, tid: &TxId) -> Option<Vec<TxLock>> {
        self.shard_for(tid)
            .lock()
            .unwrap()
            .pinned_locks
            .get(tid)
            .cloned()
    }

    /// Drops the lock set of the pinned identity `tid` after a complete
    /// release. An unresolved owner operation can still add a lock later. That
    /// lock stays until a writer on the same key removes it, and a reader
    /// drops it at once.
    pub(crate) fn forget_pinned_locks(&self, tid: &TxId) {
        self.shard_for(tid).lock().unwrap().pinned_locks.remove(tid);
    }

    /// Finishes a local identity that this instance pinned as wounded, and
    /// keeps its lock set for GC. The wound is durable, so a release of these
    /// locks cannot expose an effect of the identity (ADR-059).
    fn finish_pinned_tx(&self, tid: &TxId) {
        let mut st = self.shard_for(tid).lock().unwrap();
        let Some(TxRuntimeEntry {
            role: TxRuntimeRole::Owned(_),
            ..
        }) = st.transactions.get(tid)
        else {
            return;
        };
        let Some(mut entry) = st.transactions.remove(tid) else {
            return;
        };
        notify_waiters(&mut entry.waiters);
        let TxRuntimeRole::Owned(owned) = entry.role else {
            return;
        };
        let locks = owned
            .record
            .map(|record| record.recovery.locks)
            .unwrap_or_default();
        if !locks.is_empty() {
            st.note_pinned_locks(*tid, locks, self.pinned_lock_capacity());
        }
    }

    /// Finishes a local identity that wrote no record, and keeps its
    /// retirement proof when it has one.
    fn finish_tx_without_record(&self, tid: &TxId) {
        let mut st = self.shard_for(tid).lock().unwrap();
        let retires = match st.transactions.get(tid) {
            Some(TxRuntimeEntry {
                role: TxRuntimeRole::Owned(owned),
                ..
            }) => owned.retires_without_record(),
            _ => return,
        };
        if let Some(mut entry) = st.transactions.remove(tid) {
            notify_waiters(&mut entry.waiters);
        }
        if retires {
            let capacity = (MAX_RETIRED_WITHOUT_RECORD / self.inner.shards.len()).max(1);
            st.note_retired_without_record(*tid, capacity);
        }
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
        match st.transactions.entry(*tid) {
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

    /// Builds the next pending-record mutation from current owner state.
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

        let mut pending_record = TxRecord::new(*tid, TxCommitStatus::Pending);
        pending_record.timestamp = Some(rt::system_now());
        record.recovery.clone().apply_to(&mut pending_record);
        Ok(Some(PendingWrite {
            record: pending_record,
            expected: record.last_observation.clone(),
        }))
    }

    async fn write_pending(
        &self,
        write: &PendingWrite,
    ) -> Result<Observation<TxRecord>, StorageError> {
        match &write.expected {
            Some(observed) => self.inner.tx_records.set_if(&write.record, observed).await,
            None => self.inner.tx_records.set(&write.record).await,
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
                    // Proof for a pending record: conditional mutation. The
                    // rejection keeps a newer cached state, so this load finds
                    // the write that won without a backend read when it came
                    // through this instance. Proof for a final record: stable
                    // fact.
                    let mut status = self
                        .inner
                        .tx_records
                        .commit_status_at(tid, Requirement::ANY)
                        .await?;
                    if status.observation.is_absent() {
                        // Proof for a missing record: a read at the current
                        // requirement.
                        status = self
                            .inner
                            .tx_records
                            .commit_status_at(tid, self.current_requirement())
                            .await?;
                    }
                    // An absent object was reclaimed rather than never written,
                    // and GC only deletes a final one. This write only ever
                    // stamps `pending`, so it cannot have been the reclaimed
                    // final status: the transaction is durably dead and must
                    // not be re-created over its own tombstone.
                    if status.status.is_final() || status.observation.is_absent() {
                        self.record_durable_observation(tid, &status.observation);
                        return Err(TransError::AlreadyFinalized);
                    }
                    self.record_durable_observation(tid, &status.observation);
                    // Only this owner writes a pending record, so the write
                    // that won is not contention: write again at once. A
                    // backoff wait lets the next lease refresh start first,
                    // and the write can then lose every time.
                    continue;
                }
                Err(StorageError::Unavailable(_)) => {}
                Err(error) => return Err(error.into()),
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn final_record(
        &self,
        tid: &TxId,
        expected_status: TxCommitStatus,
        known: Option<Observation<TxRecord>>,
    ) -> Result<FinalRecord, TransError> {
        if let Some(observed) = known
            && observed
                .value()
                .is_some_and(|record| record.status == expected_status)
        {
            return Ok(FinalRecord::Present(observed));
        }
        match self
            .inner
            .tx_records
            .get_at(tid, self.current_requirement())
            .await
        {
            Ok(observed)
                if observed
                    .value()
                    .is_some_and(|record| record.status == expected_status) =>
            {
                Ok(FinalRecord::Present(observed))
            }
            // The status is permanent, but GC reclaimed the record: the record
            // is missing, or another instance wounded the identity at the empty
            // path. An observation that reaches the returned requirement shows
            // no value of the identity, also when it still names it as a stray
            // holder (ADR-104).
            Ok(_) | Err(StorageError::NotFound) => {
                let now = self.current_requirement();
                let reclaimed = self
                    .inner
                    .final_status
                    .lock()
                    .unwrap()
                    .note_reclaimed(*tid, now);
                Ok(FinalRecord::Reclaimed(reclaimed))
            }
            Err(error) => Err(TransError::Storage(
                error.context(format!("getting TID {tid}")),
            )),
        }
    }

    /// Starts a transaction-record poll after all status evidence already seen by
    /// this monitor. Unlike transaction validation, a remote-holder poll has no
    /// preceding CAS or validation barrier to reuse, so the monitor must create
    /// the lower bound itself.
    fn current_requirement(&self) -> Requirement {
        Requirement::after(self.inner.timeline.currentness_barrier())
    }

    fn owned_status_evidence(&self, tid: &TxId) -> Result<Option<TxStatusEvidence>, TransError> {
        let st = self.shard_for(tid).lock().unwrap();
        let Some(entry) = st.transactions.get(tid) else {
            return Ok(None);
        };
        match &entry.role {
            // A dispatched commit can take effect before its acknowledgement.
            // Readers must check durable status while that outcome is pending,
            // including when a peer has already published the writer in a leaf.
            TxRuntimeRole::Owned(owned)
                if owned.lifecycle.commit_write != CommitWrite::NotStarted
                    && owned
                        .record
                        .as_ref()
                        .is_none_or(|record| !record.status.is_final()) =>
            {
                Ok(None)
            }
            TxRuntimeRole::Owned(owned) => TxStatusEvidence::local(owned.record.as_ref()).map(Some),
            TxRuntimeRole::Foreign(_) => Ok(None),
        }
    }

    /// Returns the requirement that a leaf observation must reach to show no
    /// value of `tid`, for a blind wound or a reclaimed committed record.
    fn no_value_after(&self, tid: &TxId, evidence: &TxStatusEvidence) -> Option<Requirement> {
        if let Some(observed) = &evidence.observation {
            // A fresh observation recorded its requirement in the cache. The
            // cache can have evicted it since, and then this call records it.
            self.remember_final(tid, observed);
        }
        self.inner.final_status.lock().unwrap().no_value_after(tid)
    }

    fn cached_final_status(&self, tid: &TxId) -> Option<TxCommitStatus> {
        self.inner
            .final_status
            .lock()
            .unwrap()
            .get(tid)
            .map(KnownFinal::status)
    }

    fn remember_final(&self, tid: &TxId, observed: &Observation<TxRecord>) {
        let Some(stored) = observed.value() else {
            return;
        };
        let known = match stored.status {
            TxCommitStatus::BlindWounded => KnownFinal::blind_wound(self.current_requirement()),
            status => {
                let Some(known) = KnownFinal::observed(status) else {
                    return;
                };
                known
            }
        };
        self.inner.final_status.lock().unwrap().insert(*tid, known);
    }

    /// Records an exact durable observation in local owner state and in the
    /// final-status cache.
    fn record_durable_observation(&self, tid: &TxId, observed: &Observation<TxRecord>) {
        let Some(stored) = observed.value() else {
            return;
        };
        self.remember_final(tid, observed);

        let mut st = self.shard_for(tid).lock().unwrap();
        let foreign_final = stored.status.is_final()
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
        record.status = stored.status;
        record.last_observation = Some(observed.clone());
        if stored.status.is_final() {
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
        let should_spawn = match st.transactions.entry(*tid) {
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
        let tid = *tid;
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
            TxRecordState::Wounded
            | TxRecordState::BlindWounded
            | TxRecordState::Committed
            | TxRecordState::Aborted => {
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
            self.inner.expiries.lease.fetch_add(1, Ordering::Relaxed);
            RemoteLivenessDecision::Expired
        } else {
            let observation = self.observe_remote_pending(tid, status.last_update, now)?;
            if matches!(observation, RemoteLivenessDecision::Expired) {
                self.inner
                    .expiries
                    .observed_lease
                    .fetch_add(1, Ordering::Relaxed);
            }
            observation
        };
        match observation {
            RemoteLivenessDecision::Owned(evidence) => Ok(evidence),
            RemoteLivenessDecision::Live => TxStatusEvidence::observed(status),
            RemoteLivenessDecision::Expired => {
                let (wounded, changed) = self.try_wound_observed(tid, &status.observation).await?;
                if changed {
                    self.release_expired_locks(tid, &status.observation);
                }
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
            .entry(*tid)
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

        // The record must appear within one observer-relative timeout. Re-read
        // at the boundary so a concurrently created pending or final record is
        // never used as the expected side of an absence-based wound.
        let refreshed = self
            .inner
            .tx_records
            .commit_status_at(tid, self.current_requirement())
            .await?;
        if refreshed.observation.is_absent() {
            self.inner
                .expiries
                .missing_record
                .fetch_add(1, Ordering::Relaxed);
            let (wounded, _) = self.try_wound_observed(tid, &refreshed.observation).await?;
            return TxStatusEvidence::observed(wounded);
        }
        match TxRecordState::try_from_observation(&refreshed.observation)? {
            TxRecordState::Pending => self.resolve_pending_tx(tid, refreshed).await,
            TxRecordState::Wounded
            | TxRecordState::BlindWounded
            | TxRecordState::Committed
            | TxRecordState::Aborted => {
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
            .entry(*tid)
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
    /// only needs to wake its waiters when it stops. Each waiter resolved the
    /// status just before it registered, so the first fetch comes after one
    /// backoff.
    async fn poll_tx_status_with_liveness(&self, tid: &TxId) {
        let mut backoff = self.inner.retry.backoff();
        let finalized = loop {
            rt::sleep(backoff.next_delay()).await;
            if !self.retain_live_waiters(tid) {
                break false;
            }
            let evidence = match self.fetch_remote_tx_status(tid).await {
                Err(_) => break false,
                Ok(evidence) => evidence,
            };
            if evidence.status().is_final() {
                break true;
            }
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
    /// load-bearing: when no pending record has been observed, its first write
    /// creates one with create-if-absent semantics (ADR-024). If another owner
    /// path already persisted the record, the refresher reuses that exact
    /// observation and starts directly with a CAS. Every later refresh
    /// CAS-bumps the `timestamp` halfway through each pending interval.
    ///
    /// Create-if-absent is what keeps lazy materialization wound-safe: if an
    /// older peer already wounded this transaction (wrote a `wounded` record)
    /// before it materialized its own pending record, the create loses, the
    /// refresher observes the final status, stops, and the owner's commit fails
    /// — it can never resurrect itself over a wound. A later refresh CAS that
    /// finds `Wounded` or `Aborted` observes the same final decision.
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
                // The create lost (object already exists) or the revision
                // moved under us. Re-read: a final status is a wound (or a race
                // we lost) — stop and let the owner observe it; a still-pending
                // status means we adopt its revision and keep refreshing.
                Err(StorageError::Precondition) => {
                    match self
                        .inner
                        .tx_records
                        .commit_status_at(&tid, self.current_requirement())
                        .await
                    {
                        Ok(st) if st.status.is_final() => {
                            self.record_durable_observation(&tid, &st.observation);
                            return;
                        }
                        // Reclaimed: the record went final and was collected,
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
        // A wound that creates the record is blind (ADR-104).
        AbortTransition::EnsureWounded | AbortTransition::WoundIfPresent
            if current == TxRecordState::Missing =>
        {
            (TxRecordState::BlindWounded, TxCommitStatus::BlindWounded)
        }
        AbortTransition::EnsureWounded | AbortTransition::WoundIfPresent => {
            (TxRecordState::Wounded, TxCommitStatus::Wounded)
        }
    };
    if transition == AbortTransition::WoundIfPresent && current == TxRecordState::Missing {
        return AbortObservationAction::Settled;
    }
    match current.relation_to(desired) {
        // A blind wound and a wound over a pending record both settle a wound.
        TxLifecycleRelation::Same | TxLifecycleRelation::Blocks => AbortObservationAction::Settled,
        TxLifecycleRelation::CanAdvance => AbortObservationAction::Write(target),
        TxLifecycleRelation::Invalid => {
            unreachable!("the wound target follows from the observed state")
        }
    }
}

/// What the owner knows about its earlier attempts to write the committed
/// record.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CommitAttempts {
    /// The owner saw its own pending record.
    saw_pending: bool,
    /// An earlier create of the committed record was in doubt.
    create_may_land: bool,
}

/// Classifies what a durable read proves after a committed-record write did not
/// return success.
///
/// A missing record is in doubt even after a clean rejection: GC deleted a
/// final record, and during a commit the only final record that GC can delete
/// is this owner's own `Committed` record from an earlier attempt that was in
/// doubt. A peer writes a wound, which GC keeps. An acknowledgement as
/// `Aborted` needs proof that no owner operation is active, and a commit runs
/// inside one.
///
/// A blind wound landed at an empty path (ADR-104). If the owner saw its own
/// pending record, only the reclaimed `Committed` record of this owner can have
/// emptied the path. Otherwise the wound landed before the first create of the
/// committed record, unless a create was in doubt: then it is in doubt, as for a
/// missing record.
fn classify_commit_observation(
    current: TxRecordState,
    attempts: CommitAttempts,
) -> Result<CommitResolution, TransError> {
    match (current.relation_to(TxRecordState::Committed), current) {
        (TxLifecycleRelation::Same, TxRecordState::Committed) => Ok(CommitResolution::Committed),
        (TxLifecycleRelation::Blocks, TxRecordState::Aborted | TxRecordState::Wounded) => {
            Ok(CommitResolution::AlreadyFinalized)
        }
        (TxLifecycleRelation::Blocks, TxRecordState::BlindWounded) if attempts.saw_pending => {
            Ok(CommitResolution::Reclaimed)
        }
        (TxLifecycleRelation::Blocks, TxRecordState::BlindWounded) if attempts.create_may_land => {
            Ok(CommitResolution::InDoubt)
        }
        (TxLifecycleRelation::Blocks, TxRecordState::BlindWounded) => {
            Ok(CommitResolution::AlreadyFinalized)
        }
        (TxLifecycleRelation::CanAdvance, TxRecordState::Pending) => Ok(CommitResolution::Retry),
        (TxLifecycleRelation::CanAdvance, TxRecordState::Missing) => Ok(CommitResolution::InDoubt),
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
    use glassdb_data::{CollectionAddress, CollectionId, CollectionName, DbPrefix};
    use glassdb_storage::transaction::{TxCollectionOp, TxWrite};
    use glassdb_storage::{CachedStore, LockType, Timeline};

    fn tx_id(prefix: &[u8]) -> TxId {
        TxId::with_priority(0, prefix)
    }

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
            AbortObservationAction::Write(TxCommitStatus::BlindWounded)
        );
        assert_eq!(
            classify_abort_observation(wound_if_present, TxRecordState::Missing),
            AbortObservationAction::Settled
        );
    }

    #[test]
    fn commit_observation_classification_matches_the_protocol() {
        use CommitResolution::{AlreadyFinalized, Committed, InDoubt, Reclaimed, Retry};
        for saw_pending in [false, true] {
            for create_may_land in [false, true] {
                let attempts = CommitAttempts {
                    saw_pending,
                    create_may_land,
                };
                let classify = |state| classify_commit_observation(state, attempts).unwrap();
                assert_eq!(classify(TxRecordState::Committed), Committed);
                assert_eq!(classify(TxRecordState::Aborted), AlreadyFinalized);
                // A wound over a pending record fenced the commit.
                assert_eq!(classify(TxRecordState::Wounded), AlreadyFinalized);
                assert_eq!(classify(TxRecordState::Pending), Retry);
                // GC deleted a final record. It can be this owner's own commit
                // from an earlier attempt, even if the latest attempt was a
                // clean rejection.
                assert_eq!(classify(TxRecordState::Missing), InDoubt);
                // A blind wound landed at an empty path. GC never deletes a
                // pending record, so after one only the reclaimed commit of
                // this owner can have emptied the path. Without one, the wound
                // is as ambiguous as a missing record if a create was in doubt.
                let blind = if saw_pending {
                    Reclaimed
                } else if create_may_land {
                    InDoubt
                } else {
                    AlreadyFinalized
                };
                assert_eq!(classify(TxRecordState::BlindWounded), blind, "{attempts:?}");
            }
        }
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
        dispatched.begin_commit_write().unwrap();
        assert_eq!(
            dispatched.close(OwnerCloseReason::OwnerAbort, OwnerRecordState::Other),
            OwnerClosePlan::PreserveCommit
        );

        let mut unresolved_dispatch = OwnerLifecycle::default();
        unresolved_dispatch.begin_operation().unwrap();
        unresolved_dispatch.begin_commit_write().unwrap();
        assert_eq!(
            unresolved_dispatch.close(OwnerCloseReason::OwnerAbort, OwnerRecordState::Other),
            OwnerClosePlan::Transition(AbortTransition::WoundIfPresent)
        );

        let mut observed_wound = OwnerLifecycle::default();
        observed_wound.begin_operation().unwrap();
        observed_wound.finish_operation(false);
        observed_wound.begin_commit_write().unwrap();
        assert_eq!(
            observed_wound.close(OwnerCloseReason::OwnerAbort, OwnerRecordState::Wounded),
            OwnerClosePlan::Transition(AbortTransition::Acknowledge)
        );

        // A blind wound after a commit write proves a lost commit only when
        // commit recovery proved it (ADR-104).
        for (lost, plan) in [
            (
                false,
                OwnerClosePlan::Transition(AbortTransition::EnsureWounded),
            ),
            (
                true,
                OwnerClosePlan::Transition(AbortTransition::Acknowledge),
            ),
        ] {
            for reason in [OwnerCloseReason::OwnerAbort, OwnerCloseReason::Preemption] {
                let mut blind = OwnerLifecycle::default();
                blind.begin_operation().unwrap();
                blind.finish_operation(false);
                blind.begin_commit_write().unwrap();
                blind.finish_commit_write(false, lost);
                assert_eq!(
                    blind.close(reason, OwnerRecordState::BlindWounded),
                    plan,
                    "lost: {lost}, {reason:?}"
                );
            }
        }

        // A delayed create can land after GC deletes an acknowledged abort.
        for reason in [OwnerCloseReason::OwnerAbort, OwnerCloseReason::Preemption] {
            let mut create_in_doubt = OwnerLifecycle::default();
            create_in_doubt.begin_operation().unwrap();
            create_in_doubt.finish_operation(false);
            create_in_doubt.begin_commit_write().unwrap();
            create_in_doubt.finish_commit_write(true, false);
            assert_eq!(
                create_in_doubt.close(reason, OwnerRecordState::Wounded),
                OwnerClosePlan::Transition(AbortTransition::EnsureWounded)
            );
        }

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
        assert_eq!(simulation.pending_timeout(), Duration::from_millis(50));
        assert_eq!(simulation.max_clock_skew(), Duration::from_millis(50));
        assert_eq!(simulation.refresh_interval(), Duration::from_millis(25));

        let refreshed = SystemTime::UNIX_EPOCH;
        let boundary = refreshed + simulation.pending_timeout() + simulation.max_clock_skew();
        assert!(!simulation.is_expired(refreshed, boundary));
        assert!(simulation.is_expired(refreshed, boundary + Duration::from_nanos(1)));
    }

    #[test]
    fn recovery_manifest_round_trip_preserves_non_recovery_record_fields() {
        let id = tx_id(b"manifest");
        let timestamp = SystemTime::UNIX_EPOCH + Duration::from_secs(42);
        let parent = CollectionAddress::root("test");
        let created = collection_address(1);
        let manifest = TxRecoveryManifest {
            locks: vec![TxLock::TopologyParticipant {
                collection: parent.clone(),
            }],
            collection_changes: vec![TxCollectionChange {
                parent,
                name: CollectionName::new("created").unwrap(),
                collection: created.clone(),
                op: TxCollectionOp::Create,
            }],
            prepared_collections: vec![created],
        };
        let writes = vec![TxWriteForTest::w(&logical_key(b"key"), b"value")];
        let mut record = TxRecord::new(id, TxCommitStatus::Committed);
        record.timestamp = Some(timestamp);
        record.writes = writes.clone();

        manifest.clone().apply_to(&mut record);

        assert_eq!(TxRecoveryManifest::from_record(&record), manifest);
        assert_eq!(record.id, id);
        assert_eq!(record.status, TxCommitStatus::Committed);
        assert_eq!(record.timestamp, Some(timestamp));
        assert_eq!(record.writes, writes);
    }

    fn logical_key(key: &[u8]) -> LogicalKey {
        LogicalKey::new(CollectionAddress::root("test"), key)
    }

    fn collection_address(id: u8) -> CollectionAddress {
        CollectionAddress::new("test", CollectionId::from_bytes([id; 16]))
    }

    struct TestCtx {
        tx_records: TxRecordStore,
        // The strong `Arc<Background>` lives here so refresh tasks can be
        // spawned for the duration of the test; the `Monitor` only stores a
        // `Weak`.
        _bg: Arc<Background>,
    }

    // ADR-085: an identity that opened an owner operation is a recent local
    // identity for the retention, and then no longer.
    #[tokio::test(start_paused = true)]
    async fn local_identities_expire_after_their_retention() {
        let (mon, _ctx) = new_test_monitor(Arc::new(MemoryBackend::new()));
        let (local, other) = (tx_id(b"local"), tx_id(b"other"));

        mon.begin_owner_operation(&local).unwrap().complete();
        assert!(mon.is_recent_local(&local));
        assert!(!mon.is_recent_local(&other));

        tokio::time::advance(LOCAL_RETENTION).await;
        assert!(!mon.is_recent_local(&local));
    }

    fn new_test_monitor(b: Arc<dyn Backend>) -> (Monitor, TestCtx) {
        new_test_monitor_with_timing(b, ProtocolTiming::default())
    }

    fn new_test_monitor_with_timing(
        b: Arc<dyn Backend>,
        timing: ProtocolTiming,
    ) -> (Monitor, TestCtx) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(b, 1 << 20, timeline.clone(), None);
        let tx_records = TxRecordStore::new(objects.clone(), DbPrefix::try_from("test").unwrap());
        let bg = Arc::new(Background::new());
        let mon = Monitor::with_config(
            tx_records.clone(),
            timeline,
            Arc::downgrade(&bg),
            RetrySchedule::default(),
            timing,
            crate::gc::GcHints::default(),
        );
        (
            mon,
            TestCtx {
                tx_records,
                _bg: bg,
            },
        )
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

    #[tokio::test(start_paused = true)]
    async fn stats_count_final_status_lookups_across_clones() {
        let recorded = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let operations = recorded.log();
        let (writer, writer_ctx) = new_test_monitor(recorded.clone());
        let (reader, _reader_ctx) = new_test_monitor(recorded);
        let clone = reader.clone();

        for (id, status) in [
            (b"committed".as_slice(), TxCommitStatus::Committed),
            (b"aborted", TxCommitStatus::Aborted),
        ] {
            let tid = tx_id(id);
            writer_ctx
                .tx_records
                .set(&TxRecord::new(tid, status))
                .await
                .unwrap();
            assert_eq!(reader.tx_status(&tid).await.unwrap(), status);
            assert_eq!(
                clone.stats_and_reset(),
                MonitorStats {
                    hits: 0,
                    misses: 1,
                    ..Default::default()
                }
            );

            operations.lock().unwrap().clear();
            assert_eq!(clone.tx_status(&tid).await.unwrap(), status);
            assert_eq!(
                reader.stats_and_reset(),
                MonitorStats {
                    hits: 1,
                    misses: 0,
                    ..Default::default()
                }
            );
            assert!(operations.lock().unwrap().is_empty());
            assert_eq!(clone.stats_and_reset(), MonitorStats::default());
        }
        assert_eq!(writer.stats_and_reset(), MonitorStats::default());
    }

    #[tokio::test(start_paused = true)]
    async fn stats_exclude_local_owner_state_and_do_not_cache_pending_statuses() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (owner, owner_ctx) = new_test_monitor(backend.clone());
        let (reader, _reader_ctx) = new_test_monitor(backend);
        let tid = tx_id(b"local");
        owner.begin_tx(&tid);
        assert_eq!(
            owner.tx_status(&tid).await.unwrap(),
            TxCommitStatus::Pending
        );
        assert_eq!(owner.stats_and_reset(), MonitorStats::default());

        let pending = tx_id(b"pending");
        owner_ctx
            .tx_records
            .set(&TxRecord::new(pending, TxCommitStatus::Pending))
            .await
            .unwrap();
        for _ in 0..2 {
            assert_eq!(
                reader.tx_status(&pending).await.unwrap(),
                TxCommitStatus::Pending
            );
        }
        assert_eq!(
            reader.stats_and_reset(),
            MonitorStats {
                hits: 0,
                misses: 2,
                ..Default::default()
            }
        );
    }

    // Each check counts the decision that it alone makes. A record timestamp
    // far in the past expires the lease at once. A record timestamp far in the
    // future leaves the decision to the observer's clock, as a clock skew can.
    #[tokio::test]
    async fn stats_count_each_liveness_decision_by_its_check() {
        let timing = ProtocolTiming::new(Duration::from_millis(1), Duration::from_millis(1));
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (_writer, writer_ctx) = new_test_monitor_with_timing(backend.clone(), timing);
        let (reader, _reader_ctx) = new_test_monitor_with_timing(backend, timing);
        let stale = tx_id(b"stale");
        let ahead = tx_id(b"ahead");
        let missing = tx_id(b"missing");
        let now = rt::system_now();
        for (tid, timestamp) in [
            (stale, SystemTime::UNIX_EPOCH),
            (ahead, now + Duration::from_secs(60 * 60)),
        ] {
            let mut record = TxRecord::new(tid, TxCommitStatus::Pending);
            record.timestamp = Some(timestamp);
            writer_ctx.tx_records.set(&record).await.unwrap();
        }

        assert_eq!(
            reader.tx_status(&stale).await.unwrap(),
            TxCommitStatus::Wounded
        );
        for tid in [ahead, missing] {
            assert_eq!(
                reader.tx_status(&tid).await.unwrap(),
                TxCommitStatus::Pending
            );
        }
        tokio::time::sleep(timing.pending_timeout() * 5).await;
        for (tid, wounded) in [
            (ahead, TxCommitStatus::Wounded),
            (missing, TxCommitStatus::BlindWounded),
        ] {
            assert_eq!(reader.tx_status(&tid).await.unwrap(), wounded);
        }

        assert_eq!(
            reader.stats_and_reset(),
            MonitorStats {
                misses: 5,
                lease_expiries: 1,
                observed_lease_expiries: 1,
                missing_record_expiries: 1,
                ..Default::default()
            }
        );
    }

    // Lock resolution treats a wound as an abort, so a peer that saw it once
    // does not read the record again. The owner can still acknowledge it.
    #[tokio::test]
    async fn a_peer_caches_a_wound_until_it_sees_the_acknowledgement() {
        let recorded = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let operations = recorded.log();
        let (owner, _owner_ctx) = new_test_monitor(recorded.clone());
        let (peer, _peer_ctx) = new_test_monitor(recorded);
        let tid = tx_id(b"wounded");
        let operation = owner.begin_owner_operation(&tid).unwrap();
        owner.begin_tx(&tid);
        assert_eq!(peer.preempt_tx(&tid).await.unwrap(), TxFinalStatus::Aborted);
        operations.lock().unwrap().clear();

        assert_eq!(
            peer.known_final_status(&tid),
            Some(TxCommitStatus::BlindWounded)
        );
        assert_eq!(
            peer.tx_status(&tid).await.unwrap(),
            TxCommitStatus::BlindWounded
        );
        assert!(operations.lock().unwrap().is_empty());

        operation.complete();
        assert_eq!(
            owner.abort_owned_tx(&tid).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
        // A durable read of the acknowledged record replaces the cached wound.
        assert!(peer.has_durable_wound(&tid).await.unwrap());
        assert_eq!(peer.known_final_status(&tid), Some(TxCommitStatus::Aborted));
    }

    // The owner saw the wound when it checked its own status. The
    // acknowledgement uses that observation instead of reading it again, so it
    // costs one CAS.
    #[tokio::test]
    async fn owner_acknowledges_an_observed_wound_without_reading_it_again() {
        let recorded = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let operations = recorded.log();
        let (owner, _owner_ctx) = new_test_monitor(recorded.clone());
        let (peer, _peer_ctx) = new_test_monitor(recorded);
        let tid = tx_id(b"wounded");
        let operation = owner.begin_owner_operation(&tid).unwrap();
        owner.begin_tx(&tid);
        assert_eq!(peer.preempt_tx(&tid).await.unwrap(), TxFinalStatus::Aborted);
        assert!(owner.has_durable_wound(&tid).await.unwrap());
        operation.complete();
        operations.lock().unwrap().clear();

        assert_eq!(
            owner.abort_owned_tx(&tid).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
        let ops = operations
            .lock()
            .unwrap()
            .iter()
            .map(|op| op.op)
            .collect::<Vec<_>>();
        assert_eq!(ops, vec!["write_if"]);
        assert!(peer.has_durable_wound(&tid).await.unwrap());
        assert_eq!(peer.known_final_status(&tid), Some(TxCommitStatus::Aborted));
    }

    // An observed wound can be stale when the record changed after the owner
    // saw it. The failed CAS makes the owner read the record again, and the
    // abort settles on the current status.
    #[tokio::test]
    async fn owner_abort_recovers_from_a_stale_wound_observation() {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (owner, _owner_ctx) = new_test_monitor(backend.clone());
        let (peer, peer_ctx) = new_test_monitor(backend);
        let tid = tx_id(b"wounded");
        let operation = owner.begin_owner_operation(&tid).unwrap();
        owner.begin_tx(&tid);
        assert_eq!(peer.preempt_tx(&tid).await.unwrap(), TxFinalStatus::Aborted);
        assert!(owner.has_durable_wound(&tid).await.unwrap());
        operation.complete();
        let wounded = peer_ctx
            .tx_records
            .get_at(&tid, Requirement::ANY)
            .await
            .unwrap();
        let mut aborted = TxRecord::clone(wounded.value().unwrap());
        aborted.status = TxCommitStatus::Aborted;
        peer_ctx
            .tx_records
            .set_if(&aborted, &wounded)
            .await
            .unwrap();

        assert_eq!(
            owner.abort_owned_tx(&tid).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
        assert_eq!(
            owner.tx_status(&tid).await.unwrap(),
            TxCommitStatus::Aborted
        );
    }

    #[tokio::test]
    async fn begin_persisted_tx_durably_records_manifest() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor(b);
        let tx = tx_id(b"persisted");
        let parent = CollectionAddress::root("test");
        let created = collection_address(1);
        let recovery = TxRecoveryManifest {
            locks: vec![TxLock::TopologyParticipant {
                collection: parent.clone(),
            }],
            collection_changes: vec![TxCollectionChange {
                parent,
                name: CollectionName::new("created").unwrap(),
                collection: created.clone(),
                op: TxCollectionOp::Create,
            }],
            prepared_collections: vec![created],
        };

        mon.begin_persisted_tx(&tx, recovery.clone()).await.unwrap();

        let observed = t.tx_records.get_at(&tx, Requirement::ANY).await.unwrap();
        let stored = observed.value().unwrap();
        assert_eq!(stored.status, TxCommitStatus::Pending);
        assert_eq!(stored.locks, recovery.locks);
        assert_eq!(stored.collection_changes, recovery.collection_changes);
        assert_eq!(stored.prepared_collections, recovery.prepared_collections);

        mon.abort_owned_tx(&tx).await.unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn an_owner_abort_lists_locks_newer_than_the_pending_record() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor(b);
        let tx = tx_id(b"newer-locks");
        mon.begin_persisted_tx(&tx, TxRecoveryManifest::default())
            .await
            .unwrap();
        // Paused time keeps the refresher from writing the new locks.
        let lock = TxLock::Key {
            key: logical_key(b"k"),
            typ: LockType::Write,
        };
        mon.add_tx_locks(&tx, [lock.clone()]);

        mon.abort_owned_tx(&tx).await.unwrap();

        let observed = t.tx_records.get_at(&tx, Requirement::ANY).await.unwrap();
        let stored = observed.value().unwrap();
        assert_eq!(stored.status, TxCommitStatus::Aborted);
        assert_eq!(stored.locks, vec![lock]);
    }

    #[tokio::test]
    async fn the_lock_set_keeps_one_lock_for_each_target() {
        let (mon, _t) = new_test_monitor(Arc::new(MemoryBackend::new()));
        let tx = tx_id(b"lock-set");
        mon.begin_tx(&tx);
        let key = |typ| TxLock::Key {
            key: logical_key(b"k"),
            typ,
        };
        let other = TxLock::Key {
            key: logical_key(b"other"),
            typ: LockType::Read,
        };

        mon.add_tx_locks(&tx, [key(LockType::Read), other.clone()]);
        let locks = mon.add_tx_locks(&tx, [key(LockType::Write), key(LockType::Read)]);

        assert_eq!(locks, vec![key(LockType::Write), other]);
    }

    #[tokio::test(start_paused = true)]
    async fn persisted_refresh_reuses_its_exact_observation() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let b: Arc<dyn Backend> = Arc::new(backend);
        let (mon, _t) = new_test_monitor_with_timing(b, ProtocolTiming::simulation());
        let tx = tx_id(b"persisted-refresh");

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
    async fn update_pending_tx_preserves_unmodified_manifest_entries() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor(b);
        let tx = tx_id(b"updated");
        let lock = TxLock::TopologyParticipant {
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
            name: CollectionName::new("created").unwrap(),
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

        let observed = t.tx_records.get_at(&tx, Requirement::ANY).await.unwrap();
        let stored = observed.value().unwrap();
        assert_eq!(stored.status, TxCommitStatus::Pending);
        assert_eq!(stored.locks, vec![lock]);
        assert_eq!(stored.collection_changes, vec![change]);
        assert_eq!(stored.prepared_collections, vec![created]);

        mon.abort_owned_tx(&tx).await.unwrap();
    }

    /// A manifest write that starts while a lease refresh of the same owner is
    /// in flight expects the state that the refresh replaces. It is rejected
    /// once, and it writes again at once, before the next refresh can start.
    #[tokio::test(start_paused = true)]
    async fn a_manifest_write_rejected_by_the_lease_refresh_writes_again_at_once() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let (mon, t) = new_test_monitor(backend.clone());
        let mon = Arc::new(mon);
        let tx = tx_id(b"manifest-during-refresh");
        mon.begin_persisted_tx(&tx, TxRecoveryManifest::default())
            .await
            .unwrap();

        let is_pending_write = |operation: &BackendOp<'_>| {
            matches!(
                operation,
                BackendOp::WriteIf { value, .. }
                    if glassdb_storage::txrecord::status(value)
                        .is_ok_and(|status| status == TxCommitStatus::Pending)
            )
        };
        let refresh_entered = Arc::new(tokio::sync::Notify::new());
        let refresh_released = Arc::new(tokio::sync::Notify::new());
        let hold_next = Arc::new(AtomicBool::new(true));
        backend.set_before({
            let refresh_entered = refresh_entered.clone();
            let refresh_released = refresh_released.clone();
            move |operation| {
                if !is_pending_write(operation) || !hold_next.swap(false, Ordering::SeqCst) {
                    return Box::pin(async { Ok(()) });
                }
                refresh_entered.notify_one();
                let refresh_released = refresh_released.clone();
                Box::pin(async move {
                    refresh_released.notified().await;
                    Ok(())
                })
            }
        });
        let rejected = Arc::new(AtomicUsize::new(0));
        backend.set_after({
            let rejected = rejected.clone();
            move |operation, outcome| {
                if is_pending_write(operation) && !outcome.is_success() {
                    rejected.fetch_add(1, Ordering::SeqCst);
                }
                Box::pin(async { Ok(()) })
            }
        });

        refresh_entered.notified().await;
        let created = collection_address(2);
        let update = tokio::spawn({
            let mon = mon.clone();
            let created = created.clone();
            async move {
                mon.update_pending_tx(&tx, move |recovery| {
                    recovery.prepared_collections = vec![created];
                })
                .await
            }
        });
        // The paused clock advances only when every task waits, so the
        // manifest write waits behind the refresh by then.
        tokio::time::sleep(Duration::from_millis(1)).await;
        let released = tokio::time::Instant::now();
        refresh_released.notify_one();
        update.await.unwrap().unwrap();

        assert_eq!(rejected.load(Ordering::SeqCst), 1);
        assert_eq!(
            released.elapsed(),
            Duration::ZERO,
            "the write must not wait"
        );
        let observed = t.tx_records.get_at(&tx, Requirement::ANY).await.unwrap();
        assert_eq!(
            observed.value().unwrap().prepared_collections,
            vec![created]
        );
        mon.abort_owned_tx(&tx).await.unwrap();
    }

    /// A commit write that starts while a lease refresh of the same owner is
    /// in flight expects the state that the refresh replaces. It is rejected
    /// once, and it writes again at once, because no refresh keeps the lease
    /// during a wait.
    #[tokio::test(start_paused = true)]
    async fn a_commit_write_rejected_by_the_lease_refresh_writes_again_at_once() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let (mon, t) = new_test_monitor(backend.clone());
        let mon = Arc::new(mon);
        let tx = tx_id(b"commit-during-refresh");
        mon.begin_persisted_tx(&tx, TxRecoveryManifest::default())
            .await
            .unwrap();

        let is_record_write = |operation: &BackendOp<'_>| {
            matches!(
                operation,
                BackendOp::WriteIf { value, .. }
                    if glassdb_storage::txrecord::status(value).is_ok()
            )
        };
        let refresh_entered = Arc::new(tokio::sync::Notify::new());
        let refresh_released = Arc::new(tokio::sync::Notify::new());
        let hold_next = Arc::new(AtomicBool::new(true));
        backend.set_before({
            let refresh_entered = refresh_entered.clone();
            let refresh_released = refresh_released.clone();
            move |operation| {
                if !is_record_write(operation) || !hold_next.swap(false, Ordering::SeqCst) {
                    return Box::pin(async { Ok(()) });
                }
                refresh_entered.notify_one();
                let refresh_released = refresh_released.clone();
                Box::pin(async move {
                    refresh_released.notified().await;
                    Ok(())
                })
            }
        });
        let rejected = Arc::new(AtomicUsize::new(0));
        backend.set_after({
            let rejected = rejected.clone();
            move |operation, outcome| {
                if is_record_write(operation) && !outcome.is_success() {
                    rejected.fetch_add(1, Ordering::SeqCst);
                }
                Box::pin(async { Ok(()) })
            }
        });

        refresh_entered.notified().await;
        let commit = tokio::spawn({
            let mon = mon.clone();
            async move {
                let mut record = TxRecord::new(tx, TxCommitStatus::Committed);
                record.locks = vec![TxLock::Key {
                    key: logical_key(b"key"),
                    typ: LockType::Write,
                }];
                mon.commit_tx(record).await
            }
        });
        // The paused clock advances only when every task waits, so the commit
        // write waits behind the refresh by then.
        tokio::time::sleep(Duration::from_millis(1)).await;
        let released = tokio::time::Instant::now();
        refresh_released.notify_one();
        commit.await.unwrap().unwrap();

        assert_eq!(rejected.load(Ordering::SeqCst), 1);
        assert_eq!(
            released.elapsed(),
            Duration::ZERO,
            "the write must not wait"
        );
        assert_eq!(
            t.tx_records
                .commit_status_at(&tx, Requirement::ANY)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Committed
        );
    }

    #[tokio::test]
    async fn status() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, _t1) = new_test_monitor(b.clone());
        let (mon2, _t2) = new_test_monitor(b.clone());
        let key = logical_key(b"key1");
        let tx = tx_id(b"tx1");
        mon1.begin_tx(&tx);

        assert_eq!(mon1.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);

        mon1.abort_owned_tx(&tx).await.unwrap();
        assert_eq!(mon1.tx_status(&tx).await.unwrap(), TxCommitStatus::Aborted);
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Aborted);

        let tx = tx_id(b"tx2");
        mon1.begin_tx(&tx);
        let mut record = TxRecord::new(tx, TxCommitStatus::Committed);
        record.locks = vec![TxLock::Key {
            key,
            typ: LockType::Write,
        }];
        mon1.commit_tx(record).await.unwrap();
        assert_eq!(
            mon1.tx_status(&tx).await.unwrap(),
            TxCommitStatus::Committed
        );
        assert_eq!(
            mon2.tx_status(&tx).await.unwrap(),
            TxCommitStatus::Committed
        );
    }

    #[tokio::test]
    async fn losing_commit_observes_and_acknowledges_a_durable_wound() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (owner, _owner_ctx) = new_test_monitor(b.clone());
        let (wounder, _wounder_ctx) = new_test_monitor(b);
        let tx = tx_id(b"commit-loser");
        let lock = TxLock::TopologyParticipant {
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

        let mut record = TxRecord::new(tx, TxCommitStatus::Pending);
        record.locks.push(lock);
        assert!(matches!(
            owner.commit_tx(record).await,
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
    // final for the owner but cannot be physically reclaimed before the owner
    // changes it to `Aborted`.
    #[tokio::test]
    async fn pinned_wound_cannot_be_reclaimed_or_resurrected() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (owner, _owner_ctx) = new_test_monitor(b.clone());
        let (wounder, wounder_ctx) = new_test_monitor(b);
        let tx = tx_id(b"reclaimed-wound");
        let lock = TxLock::TopologyParticipant {
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

        let wounded = wounder_ctx
            .tx_records
            .get_at(&tx, Requirement::ANY)
            .await
            .unwrap();
        assert!(matches!(
            wounder_ctx.tx_records.delete(&wounded).await,
            Err(StorageError::Precondition)
        ));

        let mut record = TxRecord::new(tx, TxCommitStatus::Pending);
        record.locks.push(lock);
        assert!(matches!(
            owner.commit_tx(record).await,
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
        let tx = tx_id(b"lazy-suspended-owner");
        let lock = TxLock::TopologyParticipant {
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
                .tx_records
                .commit_status_at(&tx, Requirement::ANY)
                .await
                .unwrap()
                .status,
            TxCommitStatus::BlindWounded
        );

        tokio::time::sleep(Duration::from_secs(24 * 60 * 60)).await;
        let mut record = TxRecord::new(tx, TxCommitStatus::Pending);
        record.locks.push(lock);
        assert!(matches!(
            owner.commit_tx(record).await,
            Err(TransError::AlreadyFinalized)
        ));

        owner_operation.complete();
        owner.abort_owned_tx(&tx).await.unwrap();
        assert_eq!(owner.tx_status(&tx).await.unwrap(), TxCommitStatus::Aborted);
    }

    // Regression (ADR-057): a commit write that landed while its acknowledgement
    // was lost, and whose committed record GC then reclaimed, cannot establish
    // its own outcome from storage — the durable evidence is gone. That must
    // surface as the irreducible in-doubt outcome rather than an internal error,
    // and the record must not be re-created, which would state a commit that a
    // peer's wound may equally have decided.
    #[tokio::test(start_paused = true)]
    async fn commit_reports_in_doubt_when_its_landed_write_was_reclaimed() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let b: Arc<dyn Backend> = backend.clone();
        let (owner, _owner_ctx) = new_test_monitor(b.clone());
        let (_collector, collector_ctx) = new_test_monitor(b);
        let tx = tx_id(b"reclaimed-in-doubt");
        let lock = TxLock::TopologyParticipant {
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

        let reclaim = Arc::new(Mutex::new(Some((collector_ctx.tx_records.clone(), tx))));
        backend.set_after(move |operation, _outcome| {
            let reclaim = is_commit_write(operation)
                .then(|| reclaim.lock().unwrap().take())
                .flatten();
            let future: HookFuture = Box::pin(async move {
                let Some((tx_records, tx)) = reclaim else {
                    return Ok(());
                };
                let committed = tx_records
                    .get_at(&tx, Requirement::ANY)
                    .await
                    .expect("the commit write landed before its ack was lost");
                tx_records
                    .delete(&committed)
                    .await
                    .expect("the committed record is reclaimable");
                Err(BackendError::Unavailable(
                    "injected lost ack (landed, ack lost)".into(),
                ))
            });
            future
        });

        let mut record = TxRecord::new(tx, TxCommitStatus::Pending);
        record.locks.push(lock);
        let error = owner.commit_tx(record).await.unwrap_err();
        assert!(
            matches!(error, TransError::Storage(StorageError::Unavailable(_))),
            "expected an in-doubt outcome, got {error:?}"
        );
    }

    // Regression (ADR-057): retrying an in-doubt commit is only useful while the
    // record it would read is still there. GC may reclaim a landed write once
    // the safety horizon has elapsed since that write, so the commit loop must
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
        let tx = tx_id(b"unconfirmable");
        let lock = TxLock::TopologyParticipant {
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

        let mut record = TxRecord::new(tx, TxCommitStatus::Pending);
        record.locks.push(lock);
        let started = rt::Instant::now();
        let error = owner.commit_tx(record).await.unwrap_err();
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

    /// Whether `operation` is the CAS that flips a transaction
    /// record to `committed`, which is the commit point a lost acknowledgement
    /// puts in doubt.
    fn is_commit_write(operation: &BackendOp<'_>) -> bool {
        matches!(
            operation,
            BackendOp::WriteIf { value, .. }
                if glassdb_storage::txrecord::status(value)
                    .map(|status| status == TxCommitStatus::Committed)
                    .unwrap_or(false)
        )
    }

    /// A commit-record write that the backend applies only when the test
    /// releases it, while its caller already received `Unavailable`.
    #[derive(Clone, Default)]
    struct DelayedWrite(Arc<Mutex<Option<HeldWrite>>>);

    /// The path, value, and expected revision of a held write. A create has no
    /// expected revision.
    type HeldWrite = (String, Vec<u8>, Option<glassdb_backend::Revision>);

    impl DelayedWrite {
        fn hold(&self, operation: &BackendOp<'_>) {
            let held = match operation {
                BackendOp::WriteIf {
                    path,
                    value,
                    expected,
                } => (path.to_string(), value.to_vec(), Some((*expected).clone())),
                BackendOp::WriteIfNotExists { path, value } => {
                    (path.to_string(), value.to_vec(), None)
                }
                _ => panic!("only writes can be held"),
            };
            *self.0.lock().unwrap() = Some(held);
        }

        async fn release(&self, memory: &MemoryBackend) -> Result<(), BackendError> {
            let (path, value, expected) = self.0.lock().unwrap().take().expect("a held write");
            match expected {
                Some(expected) => memory.write_if(&path, value, &expected).await,
                None => memory.write_if_not_exists(&path, value).await,
            }
            .map(|_| ())
        }
    }

    // Regression: a `Pending` read after an in-doubt commit write proves only
    // that the write had not landed yet. If it lands later and GC deletes the
    // committed record, the retry is rejected and finds no record. The owner
    // must report the outcome as in doubt, not as an abort.
    #[tokio::test(start_paused = true)]
    async fn commit_stays_in_doubt_when_a_delayed_write_lands_after_a_pending_read() {
        let memory = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(memory.clone());
        let (owner, _owner_ctx) = new_test_monitor(backend.clone());
        let (_collector, collector_ctx) = new_test_monitor(memory.clone());
        let tx = tx_id(b"delayed-commit");
        let lock = TxLock::TopologyParticipant {
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

        let delayed = DelayedWrite::default();
        let commit_writes = Arc::new(AtomicUsize::new(0));
        backend.set_before({
            let delayed = delayed.clone();
            let commit_writes = commit_writes.clone();
            let collector = collector_ctx.tx_records.clone();
            move |operation| {
                let attempt = is_commit_write(operation)
                    .then(|| commit_writes.fetch_add(1, Ordering::SeqCst));
                if attempt == Some(0) {
                    delayed.hold(operation);
                }
                let delayed = delayed.clone();
                let memory = memory.clone();
                let collector = collector.clone();
                Box::pin(async move {
                    match attempt {
                        Some(0) => Err(BackendError::Unavailable("injected delayed write".into())),
                        Some(1) => {
                            // The first attempt lands after the owner read
                            // `Pending`, then GC deletes the committed record.
                            delayed.release(&memory).await.unwrap();
                            let committed = collector.get_at(&tx, Requirement::ANY).await.unwrap();
                            assert_eq!(
                                committed.value().unwrap().status,
                                TxCommitStatus::Committed
                            );
                            collector.delete(&committed).await.unwrap();
                            Ok(())
                        }
                        _ => Ok(()),
                    }
                })
            }
        });

        let mut record = TxRecord::new(tx, TxCommitStatus::Pending);
        record.locks.push(lock);
        let error = owner.commit_tx(record).await.unwrap_err();
        assert!(
            matches!(error, TransError::Storage(StorageError::Unavailable(_))),
            "expected an in-doubt outcome, got {error:?}"
        );
        assert_eq!(commit_writes.load(Ordering::SeqCst), 2);
    }

    /// Makes the first commit write of `tx` land but report `Unavailable`.
    /// Before the owner reads back, GC deletes the committed record and a peer
    /// wounds the empty path, so the owner reads a blind wound.
    fn land_reclaim_and_wound_the_first_commit(
        backend: &HookBackend,
        memory: Arc<MemoryBackend>,
        collector: TxRecordStore,
        wounder: Monitor,
        tx: TxId,
    ) {
        let delayed = DelayedWrite::default();
        let landed = Arc::new(AtomicBool::new(false));
        backend.set_before(move |operation| {
            let commit = match operation {
                BackendOp::WriteIf { value, .. } | BackendOp::WriteIfNotExists { value, .. } => {
                    glassdb_storage::txrecord::status(value)
                        .is_ok_and(|status| status == TxCommitStatus::Committed)
                }
                _ => false,
            };
            let first = commit && !landed.swap(true, Ordering::SeqCst);
            if first {
                delayed.hold(operation);
            }
            let (delayed, memory, collector, wounder) = (
                delayed.clone(),
                memory.clone(),
                collector.clone(),
                wounder.clone(),
            );
            Box::pin(async move {
                if !first {
                    return Ok(());
                }
                delayed.release(&memory).await.unwrap();
                let committed = collector.get_at(&tx, Requirement::ANY).await.unwrap();
                assert_eq!(committed.value().unwrap().status, TxCommitStatus::Committed);
                collector.delete(&committed).await.unwrap();
                assert_eq!(
                    wounder.preempt_tx(&tx).await.unwrap(),
                    TxFinalStatus::Aborted
                );
                Err(BackendError::Unavailable("injected landed write".into()))
            })
        });
    }

    // Regression (ADR-104): the owner replaced its pending record with the
    // committed one, and the write landed in doubt. GC deleted it, and a peer
    // wounded the empty path. GC never deletes a pending record, so the blind
    // wound proves that the commit landed. The owner must not report an abort,
    // and must not acknowledge the wound as `Aborted`.
    #[tokio::test(start_paused = true)]
    async fn a_blind_wound_after_a_pending_record_proves_the_commit() {
        let memory = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(memory.clone());
        let (owner, owner_ctx) = new_test_monitor(backend.clone());
        let (_collector, collector_ctx) = new_test_monitor(memory.clone());
        let (wounder, _wounder_ctx) = new_test_monitor(memory.clone());
        let tx = tx_id(b"reclaimed-replace");
        let lock = TxLock::TopologyParticipant {
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
        land_reclaim_and_wound_the_first_commit(
            &backend,
            memory.clone(),
            collector_ctx.tx_records.clone(),
            wounder,
            tx,
        );

        let mut record = TxRecord::new(tx, TxCommitStatus::Committed);
        record.locks.push(lock);
        owner.commit_tx(record).await.unwrap();
        assert_eq!(
            owner.tx_status(&tx).await.unwrap(),
            TxCommitStatus::Committed
        );
        assert_eq!(
            owner_ctx
                .tx_records
                .commit_status_at(&tx, Requirement::ANY)
                .await
                .unwrap()
                .status,
            TxCommitStatus::BlindWounded
        );
    }

    // Regression (ADR-104): the owner created its committed record, and the
    // create landed in doubt. GC deleted it, and a peer wounded the empty path.
    // The blind wound cannot tell this from a wound that came before the
    // create, so the owner must report the outcome as in doubt and not replay
    // a body that committed.
    #[tokio::test(start_paused = true)]
    async fn a_blind_wound_after_an_in_doubt_create_is_in_doubt() {
        let memory = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(memory.clone());
        let (owner, _owner_ctx) = new_test_monitor(backend.clone());
        let (_collector, collector_ctx) = new_test_monitor(memory.clone());
        let (wounder, _wounder_ctx) = new_test_monitor(memory.clone());
        let tx = tx_id(b"reclaimed-create");
        owner.begin_tx(&tx);
        land_reclaim_and_wound_the_first_commit(
            &backend,
            memory.clone(),
            collector_ctx.tx_records.clone(),
            wounder,
            tx,
        );

        let mut record = TxRecord::new(tx, TxCommitStatus::Committed);
        record.locks.push(TxLock::TopologyParticipant {
            collection: CollectionAddress::root("test"),
        });
        let result = owner.commit_tx(record).await;
        assert!(
            matches!(
                result,
                Err(TransError::Storage(StorageError::Unavailable(_)))
            ),
            "{result:?}"
        );
    }

    // Regression (ADR-059): a create of the committed record that is in doubt
    // can land after a peer wounds the empty path. The owner must keep the
    // wound pinned. If it acknowledged the wound, GC could delete `Aborted`,
    // and the delayed create would then commit an identity that peers treat as
    // aborted. The blind wound also cannot show that the commit lost, so the
    // outcome is in doubt (ADR-104).
    #[tokio::test(start_paused = true)]
    async fn in_doubt_commit_create_keeps_a_later_wound_pinned() {
        let memory = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(memory.clone());
        let (owner, owner_ctx) = new_test_monitor(backend.clone());
        let (wounder, _wounder_ctx) = new_test_monitor(backend.clone());
        let tx = tx_id(b"delayed-create");

        let owner_operation = owner.begin_owner_operation(&tx).unwrap();
        owner.begin_tx(&tx);

        let delayed = DelayedWrite::default();
        let held = Arc::new(AtomicBool::new(false));
        backend.set_before({
            let delayed = delayed.clone();
            let held = held.clone();
            move |operation| {
                let hold = matches!(operation, BackendOp::WriteIfNotExists { value, .. }
                        if glassdb_storage::txrecord::status(value)
                            .is_ok_and(|status| status == TxCommitStatus::Committed))
                    && !held.swap(true, Ordering::SeqCst);
                if hold {
                    delayed.hold(operation);
                }
                let wounder = wounder.clone();
                Box::pin(async move {
                    if !hold {
                        return Ok(());
                    }
                    // A peer wounds the identity before the owner reads back.
                    assert_eq!(
                        wounder.preempt_tx(&tx).await.unwrap(),
                        TxFinalStatus::Aborted
                    );
                    Err(BackendError::Unavailable("injected delayed create".into()))
                })
            }
        });

        let mut record = TxRecord::new(tx, TxCommitStatus::Committed);
        record.locks.push(TxLock::TopologyParticipant {
            collection: CollectionAddress::root("test"),
        });
        // The blind wound cannot tell a commit that lost from one that landed
        // and that GC reclaimed, so the outcome stays in doubt (ADR-104).
        let result = owner.commit_tx(record).await;
        assert!(
            matches!(
                result,
                Err(TransError::Storage(StorageError::Unavailable(_)))
            ),
            "{result:?}"
        );
        owner_operation.complete();
        // The owner writes no abort-side record for a commit in doubt
        // (ADR-057), so nobody acknowledges the wound.
        assert_eq!(
            owner.abort_owned_tx(&tx).await.unwrap(),
            OwnerAbortOutcome::CommitOutcomePreserved
        );

        let wounded = owner_ctx
            .tx_records
            .get_at(&tx, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(
            wounded.value().unwrap().status,
            TxCommitStatus::BlindWounded
        );
        assert!(matches!(
            delayed.release(&memory).await,
            Err(BackendError::Precondition)
        ));
        assert_eq!(
            owner_ctx
                .tx_records
                .commit_status_at(&tx, Requirement::ANY)
                .await
                .unwrap()
                .status,
            TxCommitStatus::BlindWounded
        );
    }

    #[tokio::test(start_paused = true)]
    async fn commit_retries_status_read_after_rejected_cas() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let b: Arc<dyn Backend> = backend.clone();
        let (owner, _owner_ctx) = new_test_monitor(b.clone());
        let (_racer, racer_ctx) = new_test_monitor(b);
        let tx = tx_id(b"commit-read-retry");
        let lock = TxLock::TopologyParticipant {
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
        let pending = racer_ctx
            .tx_records
            .get_at(&tx, Requirement::ANY)
            .await
            .unwrap();
        let mut refreshed = pending.value().unwrap().as_ref().clone();
        refreshed.timestamp = Some(rt::system_now());

        let refresh = Arc::new(Mutex::new(Some((
            racer_ctx.tx_records.clone(),
            refreshed,
            pending,
        ))));
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
                        if glassdb_storage::txrecord::status(value)
                            .map(|status| status == TxCommitStatus::Committed)
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
                    if let Some((tx_records, pending, expected)) = refresh {
                        tx_records
                            .set_if(&pending, &expected)
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

        let mut record = TxRecord::new(tx, TxCommitStatus::Pending);
        record.locks.push(lock);
        owner.commit_tx(record).await.unwrap();

        assert_eq!(failed_reads.load(Ordering::SeqCst), 1);
        assert_eq!(
            owner.tx_status(&tx).await.unwrap(),
            TxCommitStatus::Committed
        );
    }

    #[tokio::test]
    async fn preempt_tx_returns_the_status_that_won() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, _t) = new_test_monitor(b);
        let pending = tx_id(b"pending");
        mon.begin_tx(&pending);
        assert_eq!(
            mon.preempt_tx(&pending).await.unwrap(),
            TxFinalStatus::Aborted
        );

        let committed = tx_id(b"committed");
        mon.begin_tx(&committed);
        let mut record = TxRecord::new(committed, TxCommitStatus::Committed);
        record.locks.push(TxLock::Key {
            key: logical_key(b"key"),
            typ: LockType::Write,
        });
        mon.commit_tx(record).await.unwrap();
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
                        glassdb_storage::txrecord::status(value).ok()
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
        let tx = tx_id(b"local-quiescent");

        let owner = mon.begin_owner_operation(&tx).unwrap();
        mon.begin_tx(&tx);
        owner.complete();

        assert_eq!(mon.preempt_tx(&tx).await.unwrap(), TxFinalStatus::Aborted);
        assert_eq!(
            written.lock().unwrap().as_slice(),
            [TxCommitStatus::Aborted]
        );
        assert_eq!(
            ctx.tx_records
                .commit_status_at(&tx, Requirement::ANY)
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
                        glassdb_storage::txrecord::status(value).ok()
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
        let tx = tx_id(b"local-active");

        let owner = mon.begin_owner_operation(&tx).unwrap();
        mon.begin_tx(&tx);
        assert_eq!(mon.preempt_tx(&tx).await.unwrap(), TxFinalStatus::Aborted);
        assert_eq!(
            mon.tx_status(&tx).await.unwrap(),
            TxCommitStatus::BlindWounded
        );
        assert_eq!(
            written.lock().unwrap().as_slice(),
            [TxCommitStatus::BlindWounded]
        );

        owner.complete();
        assert_eq!(
            mon.abort_owned_tx(&tx).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
        assert_eq!(
            written.lock().unwrap().as_slice(),
            [TxCommitStatus::BlindWounded, TxCommitStatus::Aborted]
        );
        assert_eq!(
            ctx.tx_records
                .commit_status_at(&tx, Requirement::ANY)
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
        let tx = tx_id(b"local-unresolved");

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
            ctx.tx_records
                .commit_status_at(&tx, Requirement::ANY)
                .await
                .unwrap()
                .status,
            TxCommitStatus::BlindWounded
        );
    }

    #[tokio::test]
    async fn cancellation_after_commit_write_dispatch_does_not_invent_a_wound() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, ctx) = new_test_monitor(b);
        let tx = tx_id(b"terminal-dispatched");

        let owner = mon.begin_owner_operation(&tx).unwrap();
        mon.begin_tx(&tx);
        mon.begin_commit_write(&tx).unwrap();
        drop(owner);

        assert!(matches!(
            mon.abort_owned_tx(&tx).await,
            Err(TransError::Storage(StorageError::Unavailable(_)))
        ));
        assert!(matches!(
            ctx.tx_records.get_at(&tx, Requirement::ANY).await,
            Err(StorageError::NotFound)
        ));
    }

    /// The owner refreshes its pending record before the first wound CAS and
    /// again while the wounder waits after each rejected CAS. A wound CAS
    /// from the status read before the wait would never land.
    #[tokio::test(start_paused = true)]
    async fn preempt_tx_lands_while_the_owner_refreshes_during_each_wait() {
        let memory: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let backend = HookBackend::new(memory.clone());
        let (mon, _wounder) = new_test_monitor(backend.clone());
        let (_owner_mon, owner) = new_test_monitor(memory);
        let tx = tx_id(b"refresh-during-wait");
        owner
            .tx_records
            .set(&TxRecord::new(tx, TxCommitStatus::Pending))
            .await
            .unwrap();
        let refreshes = Arc::new(AtomicUsize::new(0));
        let refresh = {
            let tx_records = owner.tx_records.clone();
            let refreshes = refreshes.clone();
            move || {
                let tx_records = tx_records.clone();
                let n = refreshes.fetch_add(1, Ordering::SeqCst) as u64 + 1;
                Box::pin(async move {
                    let observed = tx_records.get_at(&tx, Requirement::ANY).await.unwrap();
                    let mut record = TxRecord::new(tx, TxCommitStatus::Pending);
                    record.timestamp = Some(SystemTime::UNIX_EPOCH + Duration::from_secs(n));
                    tx_records.set_if(&record, &observed).await.unwrap();
                    Ok(())
                }) as HookFuture
            }
        };
        let is_wound = |operation: &BackendOp<'_>| {
            matches!(
                operation,
                BackendOp::WriteIf { value, .. }
                    if glassdb_storage::txrecord::status(value)
                        .is_ok_and(|status| status == TxCommitStatus::Wounded)
            )
        };
        let wound_writes = Arc::new(AtomicUsize::new(0));
        backend.set_before({
            let refresh = refresh.clone();
            let wound_writes = wound_writes.clone();
            move |operation| {
                if is_wound(operation) && wound_writes.fetch_add(1, Ordering::SeqCst) == 0 {
                    return refresh();
                }
                Box::pin(async { Ok(()) })
            }
        });
        let rejected = Arc::new(AtomicBool::new(false));
        backend.set_after(move |operation, outcome| {
            if is_wound(operation) && !outcome.is_success() {
                rejected.store(true, Ordering::SeqCst);
            }
            let reads_status = matches!(
                operation,
                BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
            );
            if reads_status && rejected.swap(false, Ordering::SeqCst) {
                return refresh();
            }
            Box::pin(async { Ok(()) })
        });

        let preempted = tokio::time::timeout(Duration::from_secs(60), mon.preempt_tx(&tx))
            .await
            .expect("the wound must land while the owner refreshes");
        assert_eq!(preempted.unwrap(), TxFinalStatus::Aborted);
        assert_eq!(wound_writes.load(Ordering::SeqCst), 2);
        assert_eq!(refreshes.load(Ordering::SeqCst), 2);
    }

    #[tokio::test(start_paused = true)]
    async fn preempt_tx_retries_a_pending_refresh() {
        let backend = HookBackend::new(Arc::new(MemoryBackend::new()));
        let b: Arc<dyn Backend> = backend.clone();
        let (mon, _wounder) = new_test_monitor(b.clone());
        let (_owner_mon, owner) = new_test_monitor(b.clone());
        let tx = tx_id(b"refresh-before-wound");
        let pending = owner
            .tx_records
            .set(&TxRecord::new(tx, TxCommitStatus::Pending))
            .await
            .unwrap();

        let mut refreshed = TxRecord::new(tx, TxCommitStatus::Pending);
        refreshed.locks.push(TxLock::Key {
            key: logical_key(b"new-lock"),
            typ: LockType::Write,
        });
        let refresh = Arc::new(Mutex::new(Some((
            owner.tx_records.clone(),
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
                        if glassdb_storage::txrecord::status(value)
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
                    if let Some((tx_records, pending, expected)) = refresh {
                        tx_records
                            .set_if(&pending, &expected)
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
        let final_record = verify
            .tx_records
            .get_at(&tx, Requirement::ANY)
            .await
            .unwrap();
        // The wound replaced the refreshed record, and it holds no manifest
        // (ADR-095).
        let final_record = final_record.value().unwrap();
        assert_eq!(final_record.status, TxCommitStatus::Wounded);
        assert!(final_record.locks.is_empty());
    }

    impl CommittedValue {
        fn known(self) -> KeyCommitStatus {
            match self {
                CommittedValue::Known(value) => value,
                other => panic!("expected a known value, got {other:?}"),
            }
        }
    }

    #[tokio::test]
    async fn committed_value() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let b: Arc<dyn Backend> = Arc::new(backend);
        let (mon1, _t1) = new_test_monitor(b.clone());
        let (mon2, _t2) = new_test_monitor(b.clone());
        let key = logical_key(b"key");

        let tx = tx_id(b"tx2");
        mon1.begin_tx(&tx);
        let mut record = TxRecord::new(tx, TxCommitStatus::Committed);
        record.writes = vec![TxWriteForTest::w(&key, b"val1")];
        record.locks = vec![TxLock::Key {
            key: key.clone(),
            typ: LockType::Write,
        }];
        mon1.commit_tx(record).await.unwrap();

        let cs = mon1.committed_value(&key, &tx).await.unwrap().known();
        assert_eq!(cs.status, TxCommitStatus::Committed);
        assert_eq!(&*cs.value.value, b"val1");
        // From a remote monitor.
        let cs = mon2.committed_value(&key, &tx).await.unwrap().known();
        assert_eq!(cs.status, TxCommitStatus::Committed);
        assert_eq!(&*cs.value.value, b"val1");

        operations.lock().unwrap().clear();
        let cs = mon2
            .committed_value_at(&key, &tx, mon2.current_requirement())
            .await
            .unwrap()
            .known();
        assert_eq!(&*cs.value.value, b"val1");

        // A key the transaction didn't write.
        let key2 = logical_key(b"key2");
        let cs = mon2.committed_value(&key2, &tx).await.unwrap().known();
        assert_eq!(cs.status, TxCommitStatus::Committed);
        assert!(cs.value.not_written);
        assert!(
            operations.lock().unwrap().is_empty(),
            "cached immutable values need no backend request at a later bound"
        );
    }

    #[tokio::test]
    async fn await_local_tx_final() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon1, _t1) = new_test_monitor(b);
        let tx = tx_id(b"tx1");
        mon1.begin_tx(&tx);

        let ch1 = {
            let mon = mon1.clone();
            rt::spawn(async move { mon.await_tx_final(&tx).await })
        };
        let ch2 = {
            let mon = mon1.clone();
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
        let tx = tx_id(b"tx1");
        mon1.begin_tx(&tx);

        let mut waits = Vec::new();
        for _ in 0..3 {
            let mon = mon2.clone();
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
        let tx = tx_id(b"committed");
        let key = logical_key(b"key");
        mon.begin_tx(&tx);
        let mut record = TxRecord::new(tx, TxCommitStatus::Committed);
        record.locks.push(TxLock::Key {
            key,
            typ: LockType::Write,
        });
        mon.commit_tx(record).await.unwrap();
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
        let tx = tx_id(b"local-wound");
        mon.begin_tx(&tx);

        let observed = mon
            .inner
            .tx_records
            .commit_status_at(&tx, Requirement::ANY)
            .await
            .unwrap()
            .observation;
        assert_eq!(
            mon.try_wound_observed(&tx, &observed)
                .await
                .unwrap()
                .0
                .status,
            TxCommitStatus::BlindWounded
        );
        assert_eq!(
            mon.tx_status(&tx).await.unwrap(),
            TxCommitStatus::BlindWounded
        );
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
        let tx = tx_id(b"remote");

        assert!(matches!(
            mon.await_tx_final(&tx).await,
            Err(TransError::Storage(StorageError::Unavailable(_)))
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn await_final_is_cancelled_when_dropped() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, _t) = new_test_monitor(b);
        let tx = tx_id(b"pending");
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
        let tx = tx_id(b"tx1");
        mon.begin_tx(&tx);
        mon.start_refresh_tx(&tx);

        // Advance well past the pending timeout. Refresh keeps it alive.
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

        let st = t
            .tx_records
            .commit_status_at(&tx, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(st.status, TxCommitStatus::Pending);

        // A separate monitor should still see it as pending (not expired).
        let (mon2, _t2) = new_test_monitor(b);
        assert_eq!(mon2.tx_status(&tx).await.unwrap(), TxCommitStatus::Pending);

        mon.abort_owned_tx(&tx).await.unwrap();
    }

    // Regression (review 1.1 / ADR-022): the lazily-materialized pending record
    // the refresher writes must carry the transaction's recorded lock set, so a
    // dead pending transaction still describes its own recovery manifest for GC to
    // prune. Recording locks before the refresher fires must land on the record.
    #[tokio::test(start_paused = true)]
    async fn refresh_records_locks() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (mon, t) = new_test_monitor(b.clone());
        let tx = tx_id(b"tx1");
        let locks = vec![TxLock::Key {
            key: logical_key(b"k"),
            typ: LockType::Write,
        }];
        mon.begin_tx(&tx);
        mon.add_tx_locks(&tx, locks.clone());
        mon.start_refresh_tx(&tx);

        // Advance past the refresh interval so the refresher materializes the
        // pending record.
        tokio::time::sleep(std::time::Duration::from_secs(15)).await;

        let observed = t.tx_records.get_at(&tx, Requirement::ANY).await.unwrap();
        let stored = observed.value().unwrap();
        assert_eq!(stored.status, TxCommitStatus::Pending);
        assert_eq!(stored.locks, locks);

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
        let tx = tx_id(b"live");
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

    // ADR-024: a crashed holder whose pending record exists but stops being
    // refreshed is reclaimed within the pending timeout by the observer-relative
    // no-progress check — even though its absolute (skew-padded) lease is nowhere
    // near expiry — once a watcher has seen it make no progress for that long.
    #[tokio::test(start_paused = true)]
    async fn dead_holder_reclaimed_by_relative_progress() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let timing = ProtocolTiming::new(Duration::from_nanos(1), Duration::from_secs(30));
        let (mon, t) = new_test_monitor_with_timing(b.clone(), timing);
        let tx = tx_id(b"dead");

        // A pending record stamped "now" that never refreshes (a crashed
        // holder). Its absolute lease includes both the pending timeout and
        // skew allowance, so only the relative check can reclaim it sooner.
        let mut record = TxRecord::new(tx, TxCommitStatus::Pending);
        record.timestamp = Some(rt::system_now());
        t.tx_records.set(&record).await.unwrap();

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
        let tx = tx_id(b"committed-during-unknown-grace");

        assert_eq!(
            observer.tx_status(&tx).await.unwrap(),
            TxCommitStatus::Pending
        );
        rt::yield_now().await;

        owner
            .tx_records
            .set(&TxRecord::new(tx, TxCommitStatus::Committed))
            .await
            .unwrap();

        assert_eq!(
            observer.tx_status(&tx).await.unwrap(),
            TxCommitStatus::Committed
        );
        let (_verify, verify) = new_test_monitor(b);
        assert_eq!(
            verify
                .tx_records
                .commit_status_at(&tx, Requirement::ANY)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Committed
        );
    }

    #[tokio::test]
    async fn try_wound_observed_preserves_an_immutable_observation() {
        let backend = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let operations = backend.log();
        let b: Arc<dyn Backend> = Arc::new(backend);
        let (mon, t) = new_test_monitor(b.clone());
        let tx = tx_id(b"already-committed");
        let committed = t
            .tx_records
            .set(&TxRecord::new(tx, TxCommitStatus::Committed))
            .await
            .unwrap();
        operations.lock().unwrap().clear();

        assert_eq!(
            mon.try_wound_observed(&tx, &committed)
                .await
                .unwrap()
                .0
                .status,
            TxCommitStatus::Committed
        );
        assert!(
            operations.lock().unwrap().is_empty(),
            "the final-observation fast path must issue no backend operation"
        );
        let (_verify, verify) = new_test_monitor(b);
        assert_eq!(
            verify
                .tx_records
                .commit_status_at(&tx, Requirement::ANY)
                .await
                .unwrap()
                .status,
            TxCommitStatus::Committed
        );
    }

    // Tiny helper to build a TxWrite in tests.
    struct TxWriteForTest;
    impl TxWriteForTest {
        fn w(key: &LogicalKey, value: &[u8]) -> TxWrite {
            TxWrite {
                key: key.clone(),
                value: Arc::from(value),
                deleted: false,
            }
        }
    }
}
