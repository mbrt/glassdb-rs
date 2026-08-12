//! The transaction commit protocol with serializable isolation for the v2
//! object-native engine (ADR-016 … ADR-021).
//!
//! A read-write transaction validates its reads and installs its locks with one
//! read-modify-write CAS per touched shard (create/delete is coordinated by the
//! per-key entry lock in the owning leaf, ADR-031), flips its transaction object
//! to committed (the commit point), then publishes `current_writer` pointers and
//! releases its locks (write-back). A read-only transaction starts on a pure
//! optimistic fast path. If validation fails, retries lock their point reads
//! and scan predicates so sustained churn cannot make them retry forever.
//!
//! Concurrency control (ADR-002 / ADR-020 / ADR-021 / ADR-024): strict two-phase
//! locking with wound-wait and leases for crash recovery. On a conflict it cannot
//! win, a younger-or-equal transaction **waits while holding its locks**
//! (hold-and-wait, ADR-024) instead of aborting; an older one wounds the holder
//! and proceeds. Distinct priorities cannot deadlock (wound-wait keeps the
//! wait-for graph acyclic); two equal-priority transactions that would cycle are
//! broken by escalating to the serial order. Lock acquisition has two modes: the
//! default **parallel** path locks every shard concurrently; after a
//! [`MAX_DEADLOCK_TIMEOUT`] wait or [`SERIAL_FALLBACK_AFTER`] failed attempts a
//! transaction releases its locks and re-acquires them under the **serial**
//! sorted order (same id, no body re-run), where first-CAS-wins on the lowest
//! contended shard guarantees one contender makes progress. Only a genuine wound
//! aborts-and-renews with priority preserved ([`TxId::renew`]).

use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::{Background, Backoff, RetryConfig, rt};
use glassdb_data::{KeyRef, ObjectPath, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxLog, TxWrite};
use glassdb_storage::{
    CurrentState, InlinePolicy, LeafObservationCheck, LockType, NodeLocks, Requirement,
    SequencePoint, ShardEntry, ShardStore, SplitPolicy, StorageError, Timeline,
};

use crate::access::{Data, ReadAccess, WriteOp};
use crate::collection_commit::{CollectionAttempt, CollectionCommit};
use crate::collections::CollectionData;
use crate::error::TransError;
use crate::gc::Gc;
use crate::key_resolver::KeyResolver;
use crate::key_state_resolver::HolderResolution;
use crate::monitor::{Monitor, OwnerAbortOutcome};
use crate::shard_coord::{
    CoordinatedOutcome, FoldOutcome, ReloadCause, ResolveCtx, ShardCoordinator, ShardResolver,
    StageAdmission, Step,
};
use crate::split::SplitHintSink;
use crate::tlocker::{LockOutcome, LockedTx, Locker};

mod attempt;

use attempt::AttemptState;

/// Number of failed parallel-locking attempts before a transaction escalates to
/// the serial sorted-locking fallback (ADR-020). The parallel path is fast but
/// can *livelock* two equal-priority transactions that each grab a different
/// shard first; after this many failures the transaction switches to sorted
/// acquisition, where first-CAS-wins on the lowest contended shard guarantees
/// one of them makes progress.
const SERIAL_FALLBACK_AFTER: usize = 3;

/// Upper bound on how long a transaction blocks acquiring its locks in the
/// default parallel mode before suspecting a deadlock and escalating to the
/// serial sorted-locking fallback (ADR-024). Under hold-and-wait a
/// younger-or-equal transaction *waits* for a conflicting holder while keeping
/// its locks; distinct priorities cannot cycle (wound-wait), but two
/// equal-priority transactions can each wait on the other forever. This timeout
/// bounds that wait: on elapse the transaction releases its locks and
/// re-acquires them in the global sorted order, where one contender always
/// completes. Reuses v1's 5s budget (ADR-002 / architecture.md).
const MAX_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound for one continuous leaf-capacity retry episode. Revisions and
/// reroutes do not extend it: until acquisition succeeds, capacity has not made
/// foreground progress.
const MAX_LEAF_FULL_WAIT: Duration = Duration::from_secs(30);

#[derive(Default)]
struct DirectCommitCounters {
    candidates: AtomicU64,
    landed: AtomicU64,
}

/// Direct single-key commit coverage for one snapshot or accumulated interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DirectCommitStats {
    /// Mutation attempts shaped for the direct path.
    pub candidates: u64,
    /// Candidates that committed directly.
    pub landed: u64,
}

impl AddAssign for DirectCommitStats {
    fn add_assign(&mut self, rhs: Self) {
        self.candidates += rhs.candidates;
        self.landed += rhs.landed;
    }
}

impl Sub for DirectCommitStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            landed: self.landed.saturating_sub(rhs.landed),
        }
    }
}

/// An opaque handle to an in-progress transaction managed by [`Algo`].
pub struct Handle {
    data: Data,
    collections: CollectionAttempt,
    state: AttemptState,
    id: TxId,
    /// Per-transaction backoff for the internal CAS-contention retry in
    /// [`Algo::acquire_locks`] (a lost shard/root CAS race): advanced before each
    /// same-id re-lock so churning contenders spread out instead of busy-looping.
    /// The lock-holding restart paths (`restart`, `revalidate`) and the read-only
    /// validation paths deliberately do not back off.
    backoff: Backoff,
}

impl Handle {
    /// The transaction's ID.
    pub fn id(&self) -> &TxId {
        &self.id
    }

    /// Whether this read-only attempt is past its optimistic first try and must
    /// use the locked validation path.
    fn should_lock_reads(&self) -> bool {
        self.state.should_lock_reads()
    }

    fn engage(&mut self) -> bool {
        self.state.engage()
    }

    fn commit(&mut self) {
        self.state.commit();
    }

    fn force_locked_reads(&mut self) {
        self.state.force_locked_reads();
    }

    fn needs_abort(&self) -> bool {
        self.state.needs_abort()
    }

    fn assert_resettable(&self) {
        self.state.assert_resettable();
    }

    fn renewals(&self) -> usize {
        self.state.renewals()
    }
}

/// Terminal outcome of [`Algo::acquire_locks`]. CAS contention and suspected
/// deadlocks are resolved *inside* `acquire_locks` (release + same-id re-lock),
/// so they are not represented here — only the two outcomes the commit path must
/// act on remain. Read-version validation happens *after* this returns
/// [`Acquired::Locked`], so a stale read is not an acquisition outcome.
enum Acquired {
    /// Every lock is held; proceed to validate reads, then the commit point.
    Locked(LockedTx),
    /// A higher-priority peer aborted this transaction: renew the id and re-run
    /// ([`TransError::Wounded`]).
    Wounded,
}

/// Describes whether validation runs before locks are acquired or while the
/// transaction's own locks are visible in the coordination tree.
#[derive(Clone, Copy)]
enum ValidationContext<'a> {
    Optimistic,
    LocksHeldBy {
        tx_id: &'a TxId,
        locked: &'a LockedTx,
    },
}

impl<'a> ValidationContext<'a> {
    /// Identifies the lock holder that scan resolution must treat as the
    /// validating transaction itself rather than as a concurrent writer.
    fn own_lock_holder(self) -> Option<&'a TxId> {
        match self {
            Self::Optimistic => None,
            Self::LocksHeldBy { tx_id, .. } => Some(tx_id),
        }
    }

    fn lock_validation(self) -> Option<&'a LockedTx> {
        match self {
            Self::Optimistic => None,
            Self::LocksHeldBy { locked, .. } => Some(locked),
        }
    }
}

/// Commits an eligible single read-write transaction in one conditional leaf
/// CAS (ADR-051): it publishes `Inline { writer, value }` over the resolved
/// predecessor, installing no lock, writing no transaction object, and leaving
/// nothing to write back. The CAS *is* the commit point, so the staged entry is
/// the commit's only record: it takes a per-round claim on the key, and it
/// declines rather than publishing a pointer to a value nothing else holds when
/// the budgets close (a leaf that cannot fit the payload).
///
/// It re-resolves eligibility on every fold and classifies its own fate.
/// `Landed` means committed; `Replay` means nothing was
/// written *and* the loss is certified, so the caller may reevaluate the
/// transaction body under the same id (ADR-053); `Moved` means nothing was
/// written but only the locked protocol can resolve the entry's state;
/// `InDoubt` means the CAS may have committed and must not be re-run.
struct DirectCommitResolver {
    id: TxId,
    raw_key: Vec<u8>,
    leaf_path: ObjectPath,
    key: KeyRef,
    value: Arc<[u8]>,
    read_version: Option<TxId>,
    inline: InlinePolicy,
    split_hints: SplitHintSink,
}

#[async_trait]
impl ShardResolver for DirectCommitResolver {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &std::collections::BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let cur = staged.get(&self.raw_key);

        // Our exact commit marker is already published: an in-doubt CAS landed
        // (possibly under a later holder's lock), so this is an idempotent
        // success rather than a second application (ADR-051).
        if cur.is_some_and(|e| self.committed(&e.current)) {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Landed,
            });
        }

        // A structural gate or a collection-deletion fence needs the logged
        // protocol's coordination, and neither is a race the direct path can
        // arbitrate.
        if staged_locks.structural_gate().lock_type() == LockType::Write
            || staged_locks.delete_intent().is_some()
        {
            return Ok(Step::Skip {
                outcome: self.unlanded(ctx, Ineligible::Locked),
            });
        }

        let res = ctx
            .key_state
            .resolve_holders(&self.key, cur, None, ctx.requirement)
            .await?;
        if let Err(why) = eligible_writer(&res, self.read_version.as_ref()) {
            return Ok(Step::Skip {
                outcome: self.unlanded(ctx, why),
            });
        }
        // A budget the folded leaf closes is a stable property of that leaf, not
        // a race a re-run of the body can win (ADR-053).
        let other_inline_bytes = staged
            .iter()
            .filter(|(key, _)| key.as_slice() != self.raw_key.as_slice())
            .map(|(_, entry)| entry.current.inline_len())
            .sum();
        if !self.inline.admits(other_inline_bytes, self.value.len()) {
            let outcome = self.unlanded(ctx, Ineligible::Locked);
            if self.inline.admits_value(self.value.len()) {
                // Resolution runs in the coordinator worker, so this
                // best-effort observation is detached from the submitter even
                // though the coordinator has no inline-pressure policy.
                self.split_hints.observe_inline_pressure(
                    &self.leaf_path,
                    &self.raw_key,
                    self.value.len(),
                );
            }
            return Ok(Step::Skip { outcome });
        }

        // Publish the value itself as the new current state, dropping the
        // entry's holders: eligibility proved every one of them is final, so an
        // already-committed writer awaiting write-back is help-forwarded and
        // replaced here (its own write-back becomes a no-op). Leaving it in
        // place would resolve the entry *backwards* to it, behind the value
        // this CAS publishes.
        let e = ShardEntry::new(self.raw_key.clone()).with_current(CurrentState::Inline {
            writer: self.id.clone(),
            value: self.value.clone(),
        });
        Ok(Step::Stage {
            entries: vec![(self.raw_key.clone(), e)],
            locks: staged_locks.clone(),
            admission: StageAdmission::InlinePublication,
            outcome: FoldOutcome::Landed,
        })
    }

    fn reorderable(&self) -> bool {
        false
    }

    fn exhausted_outcome(&self, in_doubt: bool) -> FoldOutcome {
        if in_doubt {
            return FoldOutcome::InDoubt("round abandoned after in-doubt CAS".into());
        }
        // An exhausted CAS budget does not certify that this attempt staged
        // nothing durable in an earlier attempt of the round, so it is not a
        // body-replay case (ADR-053).
        FoldOutcome::Moved
    }

    fn excluded_outcome(&self, in_doubt: bool) -> FoldOutcome {
        if in_doubt {
            return FoldOutcome::InDoubt(format!(
                "direct commit for {} in-doubt: excluded after an uncertain CAS",
                self.id
            ));
        }
        // A peer claimed the key before this member folded, so it staged nothing
        // at all this round: a read-modify-write may reevaluate its body against
        // the winner rather than publish a holder (ADR-053).
        self.definitive_loss()
    }

    fn owned_keys(&self) -> Vec<&[u8]> {
        vec![self.raw_key.as_slice()]
    }

    fn logless_keys(&self) -> Vec<&[u8]> {
        vec![self.raw_key.as_slice()]
    }
}

impl DirectCommitResolver {
    /// Whether `current` is this transaction's own published commit marker.
    fn committed(&self, current: &CurrentState) -> bool {
        current.writer() == Some(&self.id) && current.inline() == Some(&self.value)
    }

    /// How to report a fold that is not publishing the commit marker. Every such
    /// reason is only evidence that the marker is *not there now*. Without an
    /// in-doubt CAS that also proves nothing was ever written; after one it
    /// cannot be told from our own commit having landed and then been
    /// superseded, so the ambiguity is irreducible and is never downgraded to a
    /// replay (ADR-051, ADR-053).
    fn unlanded(&self, ctx: &ResolveCtx<'_>, why: Ineligible) -> FoldOutcome {
        if matches!(ctx.cause, ReloadCause::Reloaded { in_doubt: true }) {
            return FoldOutcome::InDoubt(format!(
                "direct commit for {} in-doubt: marker absent after an uncertain CAS",
                self.id
            ));
        }
        match why {
            Ineligible::Replay => self.definitive_loss(),
            Ineligible::Locked => FoldOutcome::Moved,
        }
    }

    /// How to report a loss that provably staged nothing durable. Only a
    /// read-modify-write has a read-dependent computation worth reevaluating; a
    /// blind overwrite would recompute the same bytes, so it takes the locked
    /// protocol instead (ADR-053).
    fn definitive_loss(&self) -> FoldOutcome {
        match self.read_version {
            Some(_) => FoldOutcome::Replay,
            None => FoldOutcome::Moved,
        }
    }
}

/// What an attempted direct commit (ADR-051) established about its transaction,
/// so the engine can tell a certified logless loss from genuine ineligibility
/// (ADR-053). An in-doubt attempt is not represented here: it is an error,
/// because it must never be re-run.
enum DirectAttempt {
    /// The one-CAS commit landed. The transaction is committed.
    Committed,
    /// Nothing durable was staged and the loss is certified, so the
    /// read-modify-write body is reevaluated against current state under the
    /// same, still unengaged, id.
    Replay,
    /// The attempt met state only the regular locked protocol can resolve, so it
    /// acquires and validates through the general path under the same id.
    Locked,
}

/// A transaction shaped like a single read-write overwrite: the value it puts
/// and, for a read-modify-write, the version its read observed.
struct SingleRw {
    key: KeyRef,
    value: Arc<[u8]>,
    read_version: Option<TxId>,
}

/// The predecessor a direct commit builds on and the leaf that owns its key.
struct Predecessor {
    leaf_path: ObjectPath,
    writer: TxId,
}

/// Recognizes a transaction the direct commit path can publish: exactly one put,
/// no scans, and every read of that same key and found. A delete publishes a
/// tombstone and a read that found nothing makes this a create; neither has a
/// predecessor for a direct commit to build on.
fn single_rw_shape(data: &Data) -> Option<SingleRw> {
    if data.writes.len() != 1 || !data.scans.is_empty() {
        return None;
    }
    let write = &data.writes[0];
    let WriteOp::Put(value) = &write.op else {
        return None;
    };
    let mut read_version = None;
    for r in &data.reads {
        if r.key != write.key {
            return None;
        }
        read_version = Some(r.last_writer().cloned()?);
    }
    Some(SingleRw {
        key: write.key.clone(),
        value: value.clone(),
        read_version,
    })
}

/// Why a direct attempt cannot publish over an entry, and therefore what the
/// engine may do about it (ADR-053).
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum Ineligible {
    /// The read this write depends on is definitively superseded. Nothing
    /// durable was staged, so the read-modify-write body can be reevaluated
    /// against the winner under the same id.
    Replay,
    /// The entry holds state the direct path cannot arbitrate. Only the regular
    /// locked protocol resolves it, so replaying the body would spin.
    Locked,
}

/// Decides the effective committed writer a direct commit must build on from
/// lock-domain entry state, or why the key cannot take the direct commit CAS.
///
/// Writer resolution help-forwards a committed holder while lock coordination
/// separately classifies live conflicts. A create / put over a tombstone or a
/// read-modify-write whose read was superseded is rejected (ADR-051).
///
/// Only a superseded read is [`Ineligible::Replay`], and the checks are ordered
/// so a stronger reason wins: a key read as deleted names the same writer that
/// deleted it, so testing existence first keeps it on the locked path (ADR-053).
fn eligible_writer(
    res: &HolderResolution,
    read_version: Option<&TxId>,
) -> Result<TxId, Ineligible> {
    // A live holder is a genuine conflict: defer to the full locked path so it
    // can wound-wait. Terminal holders never reach `pending`.
    if !res.pending.is_empty() {
        return Err(Ineligible::Locked);
    }
    // The key must currently exist; a create or a put over a tombstone has no
    // predecessor value, which the direct path does not handle.
    let writer = match &res.writer {
        Some(w) if !res.deleted => w.clone(),
        _ => return Err(Ineligible::Locked),
    };
    match read_version {
        // A read-modify-write commits only if its read is still current.
        Some(rv) if rv != &writer => Err(Ineligible::Replay),
        // A blind put (no read) is last-writer-wins and always serializable.
        _ => Ok(writer),
    }
}

/// Reports whether the observed leaf contains an exclusive holder whose final
/// state can change the effective writer without rewriting the leaf.
fn read_observation_has_exclusive_holder(read: &ReadAccess) -> Result<bool, TransError> {
    let raw_key = read.key.key();
    let Some(node) = read.observation().value().map(AsRef::as_ref) else {
        return Ok(false);
    };
    let leaf = node
        .as_leaf()
        .ok_or_else(|| TransError::other("read observation contains a non-leaf node"))?;
    Ok(leaf.lookup(raw_key).is_some_and(|entry| {
        matches!(entry.lock_type(), LockType::Write | LockType::Create)
            && !entry.lock_holders().is_empty()
    }))
}

/// Coordinates transactions: read validation, locking, commit, and write-back.
#[derive(Clone)]
pub struct Algo {
    shards: ShardStore,
    resolver: KeyResolver,
    locker: Locker,
    // The single shard-mutation coordinator (ADR-028), shared with the locker:
    // the logless direct commit publishes its value through this — one
    // deduplicated fold round — instead of a bespoke racing shard CAS.
    coord: ShardCoordinator,
    mon: Monitor,
    gc: Gc,
    timeline: Timeline,
    // Factory for each transaction's same-identity acquisition schedule. Other
    // coordination loops own independent schedules from the same engine policy.
    acquisition_retry: RetryConfig,
    split_policy: SplitPolicy,
    inline_policy: InlinePolicy,
    collection_commit: CollectionCommit,
    split_hints: SplitHintSink,
    direct_commit_stats: Arc<DirectCommitCounters>,
    // Weak so a captured `Algo` clone inside a spawned async-abort task does not
    // keep [`Background`] alive past DB shutdown.
    background: Option<Weak<Background>>,
}

impl Algo {
    /// Creates a transaction algorithm coordinator.
    ///
    /// Validation barriers use `timeline`; transaction priorities and leases
    /// share the process-wide model-time domain.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        shards: ShardStore,
        timeline: Timeline,
        acquisition_retry: RetryConfig,
        locker: Locker,
        coord: ShardCoordinator,
        mon: Monitor,
        collection_commit: CollectionCommit,
        gc: Gc,
        background: Option<Weak<Background>>,
        resolver: KeyResolver,
        split_policy: SplitPolicy,
        inline_policy: InlinePolicy,
        split_hints: SplitHintSink,
    ) -> Self {
        Algo {
            shards,
            resolver,
            locker,
            coord,
            mon,
            gc,
            timeline,
            acquisition_retry,
            split_policy,
            inline_policy,
            collection_commit,
            split_hints,
            direct_commit_stats: Arc::new(DirectCommitCounters::default()),
            background,
        }
    }

    /// Returns and resets direct single-key commit coverage counters.
    pub fn direct_commit_stats_and_reset(&self) -> DirectCommitStats {
        DirectCommitStats {
            candidates: self
                .direct_commit_stats
                .candidates
                .swap(0, Ordering::Relaxed),
            landed: self.direct_commit_stats.landed.swap(0, Ordering::Relaxed),
        }
    }

    /// Starts a new transaction with its key and collection-management data.
    /// The id's random prefix and timestamp are deterministic under `--cfg sim`.
    pub fn begin(&self, data: Data, collection_data: CollectionData) -> Handle {
        let id = TxId::new_at(rt::system_now());
        Handle {
            data,
            collections: CollectionAttempt::new(collection_data),
            state: AttemptState::new(),
            id,
            backoff: self.acquisition_retry.backoff(),
        }
    }

    /// Restarts a wounded transaction, preserving its priority (timestamp) while
    /// minting a fresh log identity ([`TxId::renew`]) so it keeps its wound-wait
    /// rank and cannot be starved. Carries the backoff forward and records the
    /// renewal (which drives the serial-locking escalation).
    pub fn rebegin(&self, old: Handle) -> Handle {
        let Handle {
            data,
            collections,
            state,
            id,
            backoff,
        } = old;
        Handle {
            id: id.renew(),
            data,
            collections: collections.renewed(),
            state: state.renew(),
            backoff,
        }
    }

    /// Validates all reads and applies all writes. Returns [`TransError::Wounded`]
    /// only when a higher-priority peer aborted this transaction, so it must
    /// retry with a fresh id (priority preserved), or [`TransError::Retry`] when
    /// the body must re-run in place — a read-only transaction whose reads
    /// changed, a read-write transaction whose read moved before it locked the
    /// key (re-run holding its locks, ADR-024), or a read-modify-write whose
    /// certified logless loss leaves it holding nothing at all (ADR-053). CAS
    /// contention and suspected deadlocks are handled internally.
    pub async fn commit(&self, tx: &mut Handle) -> Result<(), TransError> {
        let owner_operation = self.mon.begin_owner_operation(&tx.id)?;
        let result = self.commit_inner(tx).await;
        owner_operation.complete();
        result
    }

    /// Validates the reads and range scans of a read-only transaction (the
    /// error-recovery path in the db retry loop), returning [`TransError::Retry`]
    /// if any was invalidated. The first attempt is optimistic; after a failure,
    /// the next attempt validates with point and predicate read locks.
    pub async fn validate_reads(&self, tx: &mut Handle) -> Result<(), TransError> {
        let owner_operation = self.mon.begin_owner_operation(&tx.id)?;
        let result = self.validate_attempt_reads(tx).await;
        owner_operation.complete();
        result
    }

    /// Replaces the transaction's data. Allowed before commit (the db retry loop
    /// resets accesses between attempts).
    pub fn reset(&self, tx: &mut Handle, data: Data) {
        tx.assert_resettable();
        tx.data = data;
    }

    /// Replaces both key and collection-management accesses before commit.
    pub fn reset_with_collections(
        &self,
        tx: &mut Handle,
        data: Data,
        collection_data: CollectionData,
    ) {
        self.reset(tx, data);
        tx.collections.replace_data(collection_data);
    }

    /// Aborts a non-committed, engaged transaction, acknowledging it when safe
    /// and releasing its locks lazily. An optimistic read-only attempt never
    /// engaged, so there is nothing to abort.
    pub async fn end(&self, tx: &mut Handle) -> Result<(), TransError> {
        if !tx.needs_abort() {
            return Ok(());
        }
        match self.mon.abort_owned_tx(&tx.id).await? {
            OwnerAbortOutcome::Acknowledged => {
                self.gc.schedule_tx_cleanup(tx.id.clone());
                self.collection_commit.abort(&tx.id, &tx.collections).await
            }
            // A dropped or otherwise unresolved owner operation was pinned as
            // `Wounded`. Its durable manifest and repeated GC passes own
            // cleanup; local rollback must not race an effect that may land
            // after this future returns.
            OwnerAbortOutcome::Pinned => {
                self.gc.schedule_tx_cleanup(tx.id.clone());
                Ok(())
            }
            // The commit point won before cleanup observed its result. Its
            // collection objects and delete fences now belong to the committed
            // log and must be left for write-back/recovery.
            OwnerAbortOutcome::Committed => {
                tx.commit();
                Ok(())
            }
            // The terminal write was dispatched but its result is no longer
            // abortable. Preserve its resources exactly as for an observed
            // committed winner.
            OwnerAbortOutcome::CommitOutcomePreserved => {
                tx.commit();
                Ok(())
            }
            // Another local path already finished this identity. Without its
            // owner record this handle must not attempt collection rollback.
            OwnerAbortOutcome::AlreadyFinished => {
                tx.commit();
                Ok(())
            }
        }
    }

    /// Schedules cancellation recovery for `tx_id` when a transaction future is
    /// dropped before [`Algo::end`] runs. The waited background task pins a safe
    /// pre-dispatch attempt as wounded and returns immediately; idempotent.
    ///
    /// A no-op unless the transaction still holds a live logged identity. An
    /// attempt that never took one — an optimistic read-only validation, or a
    /// logless one-CAS commit (ADR-051) — is invisible to peers, so an aborted
    /// object for its id would invent a transaction that never existed (and,
    /// after a dispatched logless CAS, would not even be true).
    pub fn async_abort(&self, tx_id: &TxId) {
        if !self.mon.is_tracked_local(tx_id) {
            return;
        }
        let Some(bg) = self.background.as_ref().and_then(|w| w.upgrade()) else {
            return;
        };
        let mon = self.mon.clone();
        let gc = self.gc.clone();
        let tx_id = tx_id.clone();
        bg.spawn_waited(async move {
            if mon.abort_owned_tx(&tx_id).await.is_ok() {
                gc.schedule_tx_cleanup(tx_id);
            }
        });
    }

    async fn commit_inner(&self, tx: &mut Handle) -> Result<(), TransError> {
        if tx.data.writes.is_empty() && !tx.collections.has_writes() {
            if tx.should_lock_reads() {
                self.validate_coordination_keys(&tx.data)?;
                return self.commit_locked(tx).await;
            }
            return self.commit_readonly(tx).await;
        }
        self.validate_coordination_keys(&tx.data)?;
        // Try the logless direct path first: a lone overwrite whose value fits
        // the inline budgets commits in one leaf CAS with no transaction object
        // at all (ADR-051). It writes nothing unless it commits, so a
        // non-landing attempt is classified rather than failed (ADR-053).
        if tx.collections.data().reads.is_empty() && tx.collections.data().changes.is_empty() {
            match self.try_commit_direct(tx).await? {
                DirectAttempt::Committed => return Ok(()),
                // A certified logless loss reevaluates the body rather than
                // publishing a holder that would make every subsequent direct
                // attempt on the key ineligible (ADR-053). The id is unengaged —
                // no object, no lock, no published identity — so the ordinary
                // retry contract applies with no cleanup.
                DirectAttempt::Replay => return Err(TransError::Retry),
                DirectAttempt::Locked => {}
            }
        }
        self.commit_locked(tx).await
    }

    async fn validate_attempt_reads(&self, tx: &mut Handle) -> Result<(), TransError> {
        if !tx.data.writes.is_empty() || tx.collections.has_writes() {
            return Err(TransError::other(
                "cannot validate only reads when writes are present",
            ));
        }
        if tx.should_lock_reads() {
            return self.validate_locked_reads(tx).await;
        }
        // The transaction body has finished and no CAS has yet certified its
        // reads, so optimistic validation must establish its own lower bound.
        let validation_start = self.timeline.now();
        if self
            .validate(&tx.data, ValidationContext::Optimistic, validation_start)
            .await?
            && self
                .collection_commit
                .validate(
                    None,
                    &tx.collections,
                    Requirement::AtLeast(validation_start),
                )
                .await?
        {
            return Ok(());
        }
        tx.force_locked_reads();
        Err(TransError::Retry)
    }

    /// Rejects keys that can never fit before the transaction has side effects.
    fn validate_coordination_keys(&self, data: &Data) -> Result<(), TransError> {
        for key in data
            .reads
            .iter()
            .map(|read| &read.key)
            .chain(data.writes.iter().map(|write| &write.key))
        {
            if !self.split_policy.key_fits(key.key()) {
                return Err(TransError::InvalidInput(
                    "key exceeds the coordination node size limit".into(),
                ));
            }
        }
        Ok(())
    }

    /// Read-only fast path: re-resolve each read's effective writer against the
    /// shards and commit if none changed. The first attempt takes no locks; a
    /// failed validation makes the next attempt use the locked path.
    ///
    /// A failed validation does not back off before signalling [`Retry`]: the
    /// re-run re-reads the authoritative values (the cache was just invalidated)
    /// rather than busy-spinning on the stale ones, and an idle delay would only
    /// add commit latency.
    ///
    /// [`Retry`]: TransError::Retry
    async fn commit_readonly(&self, tx: &mut Handle) -> Result<(), TransError> {
        // Read-only commit has no mutation receipt; this barrier separates the
        // completed body from the physical observations that certify it.
        let validation_start = self.timeline.now();
        if self
            .validate(&tx.data, ValidationContext::Optimistic, validation_start)
            .await?
            && self
                .collection_commit
                .validate(
                    None,
                    &tx.collections,
                    Requirement::AtLeast(validation_start),
                )
                .await?
        {
            tx.commit();
            return Ok(());
        }
        tx.force_locked_reads();
        Err(TransError::Retry)
    }

    /// Locked path for read-write transactions and escalated read-only retries.
    async fn commit_locked(&self, tx: &mut Handle) -> Result<(), TransError> {
        let is_new = tx.engage();

        self.collection_commit
            .reconcile_retry(&tx.id, &mut tx.collections)
            .await?;
        if tx.collections.has_writes() {
            let result = self
                .collection_commit
                .persist_manifest(&tx.id, is_new, &tx.collections)
                .await;
            if let Err(error) = result {
                if matches!(error, TransError::AlreadyFinalized) {
                    return self.restart(tx).await;
                }
                return Err(error);
            }
        } else if is_new {
            self.mon.begin_tx(&tx.id);
        }

        self.collection_commit.prepare(&mut tx.collections).await?;
        let directory_locks = self
            .locker
            .collections()
            .lock(
                &tx.id,
                &tx.collections.data().reads,
                &tx.collections.data().changes,
            )
            .await?;

        // Capture before lock acquisition so every successful lock CAS is
        // eligible to certify the reads it protects against this same bound.
        let validation_start = self.timeline.now();
        let locked = match self.acquire_locks(tx, validation_start).await? {
            Acquired::Locked(l) => l,
            // A higher-priority peer aborted us: renew the id and re-run.
            Acquired::Wounded => return self.restart(tx).await,
        };

        // Record the held lock set so both the committed object (below) and the
        // refresher's pending object describe their own back-references, which
        // is what lets GC prune this transaction's locks by reverse check
        // (ADR-022). This tracks the latest acquire; a `revalidate` re-run that
        // drops keys may under-record, which only defers those stale locks to
        // lazy reclaim, never a correctness loss.
        let mut locks = locked.locked_paths();
        locks.extend(directory_locks.into_durable_locks());
        self.mon.record_tx_locks(&tx.id, locks.clone());

        // Validate point reads and scans after their entry/predicate locks are
        // held. A stale dependency re-runs the body under the same id while the
        // acquired locks prevent another change in the validation-to-commit gap.
        if !self
            .validate(
                &tx.data,
                ValidationContext::LocksHeldBy {
                    tx_id: &tx.id,
                    locked: &locked,
                },
                validation_start,
            )
            .await?
            || !self
                .collection_commit
                .validate(
                    Some(&tx.id),
                    &tx.collections,
                    Requirement::AtLeast(validation_start),
                )
                .await?
        {
            self.locker.collections().release(&tx.id, &locks).await?;
            return self.revalidate(tx).await;
        }

        self.collection_commit
            .fence(&tx.id, &mut tx.collections)
            .await?;

        // Commit point: create-or-flip the transaction object to committed.
        if let Err(e) = self
            .commit_writes(&tx.data, &tx.collections, locks.clone(), &tx.id)
            .await
        {
            if matches!(e, TransError::AlreadyFinalized) {
                // An abort-side terminal status won between locking and commit.
                return self.restart(tx).await;
            }
            return Err(e.context(format!("committing writes for tx {}", tx.id)));
        }
        tx.commit();

        if let Err(error) = self
            .locker
            .collections()
            .write_back(&tx.id, &tx.collections.data().changes, &locks)
            .await
        {
            tracing::debug!(%error, "collection-directory write-back deferred");
        }
        self.write_back(&tx.id, locked).await;
        self.collection_commit
            .finish_committed(&tx.collections)
            .await;
        Ok(())
    }

    /// Acquires and validates an escalated read-only attempt whose user body
    /// returned an error. The caller will abort through [`Algo::end`] after a
    /// successful validation, so this deliberately does not commit the handle.
    async fn validate_locked_reads(&self, tx: &mut Handle) -> Result<(), TransError> {
        self.validate_coordination_keys(&tx.data)?;
        if tx.engage() {
            self.mon.begin_tx(&tx.id);
        }
        // The escalated read-only path uses lock CASes as validation evidence,
        // so their shared lower bound must precede acquisition.
        let validation_start = self.timeline.now();
        let directory_locks = self
            .locker
            .collections()
            .lock(&tx.id, &tx.collections.data().reads, &[])
            .await?;
        let locked = match self.acquire_locks(tx, validation_start).await? {
            Acquired::Locked(locked) => locked,
            Acquired::Wounded => return self.restart(tx).await,
        };
        let mut locks = locked.locked_paths();
        locks.extend(directory_locks.into_durable_locks());
        self.mon.record_tx_locks(&tx.id, locks.clone());
        if self
            .validate(
                &tx.data,
                ValidationContext::LocksHeldBy {
                    tx_id: &tx.id,
                    locked: &locked,
                },
                validation_start,
            )
            .await?
            && self
                .collection_commit
                .validate(
                    Some(&tx.id),
                    &tx.collections,
                    Requirement::AtLeast(validation_start),
                )
                .await?
        {
            return Ok(());
        }
        self.locker.collections().release(&tx.id, &locks).await?;
        self.revalidate(tx).await
    }

    /// Resolves the committed predecessor a direct commit would build on, and the
    /// leaf that owns its key, or why the key cannot take the direct commit CAS at
    /// all — a create, a genuinely conflicting entry, a superseded read, or a
    /// closed structural gate. Checked before anything is written, so an
    /// ineligible transaction either replays its body or falls back to the locked
    /// path under the same id (ADR-053). A lock left by an *already-committed*
    /// writer whose write-back is still pending does not block: it is
    /// help-forwarded to its effective writer, the predecessor we build on
    /// (ADR-020).
    ///
    /// Resolves on the shard the transaction body's read already cached
    /// (`Any`: no revalidation round-trip). The commit fold below re-reads the
    /// same shard through the cache (also `Any`), so a steady-state
    /// read-modify-write adds no backend shard load at commit (ADR-030).
    ///
    /// A stale cached snapshot stays safe: it can only make a superseded
    /// read-modify-write *look* eligible, in which case the fold's
    /// version-conditional CAS misses, invalidates that seed, and re-folds over
    /// the winner — which finds the read superseded.
    async fn single_rw_predecessor(
        &self,
        key: &KeyRef,
        read_version: Option<&TxId>,
    ) -> Result<Result<Predecessor, Ineligible>, TransError> {
        let (lock_state, locator) = self
            .resolver
            .resolve_key_holders(key, None, Requirement::Any)
            .await?;
        if locator
            .node()
            .is_some_and(|node| node.structural_gate().lock_type() == LockType::Write)
        {
            return Ok(Err(Ineligible::Locked));
        }
        Ok(
            eligible_writer(&lock_state, read_version).map(|writer| Predecessor {
                // The leaf that owns this key, resolved by descent (ADR-031),
                // so the commit fold and any write-back target it directly
                // instead of recomputing a fixed-hash shard index.
                leaf_path: locator.path,
                writer,
            }),
        )
    }

    /// The logless single read-write path (ADR-051): a transaction that
    /// overwrites exactly one already-existing key with a value both inline
    /// budgets admit commits in **one conditional leaf CAS** that publishes the
    /// value itself. It installs no lock, writes no transaction object, and has
    /// nothing to write back — the CAS is the commit point.
    ///
    /// Nothing is written unless the CAS commits, so a non-landing attempt is
    /// classified rather than failed (ADR-053): a read-modify-write whose loss is
    /// *certified* — excluded from its coordinator round, or superseded before
    /// publication — reports [`DirectAttempt::Replay`] and reevaluates its body
    /// under the **same id**, while everything else reports
    /// [`DirectAttempt::Locked`] and takes the regular locked protocol. An
    /// in-doubt CAS that may have committed is an error
    /// ([`StorageError::Unavailable`]) and is never replayed, because re-running
    /// the body could apply it twice.
    ///
    /// The attempt publishes no pre-commit identity, so it cannot be wounded and
    /// takes no part in wound-wait. Cancellation before the CAS leaves no state;
    /// after it, the outcome is crash-equivalent.
    async fn try_commit_direct(&self, tx: &mut Handle) -> Result<DirectAttempt, TransError> {
        let Some(SingleRw {
            key,
            value,
            read_version,
        }) = single_rw_shape(&tx.data)
        else {
            return Ok(DirectAttempt::Locked);
        };
        self.direct_commit_stats
            .candidates
            .fetch_add(1, Ordering::Relaxed);
        let inline = self.inline_policy;
        // Reject values no partition could admit before routing; aggregate
        // pressure from other keys needs the folded leaf (ADR-056).
        if !inline.admits_value(value.len()) {
            return Ok(DirectAttempt::Locked);
        }
        let raw_key = key.key().to_vec();
        let Predecessor { leaf_path, writer } = match self
            .single_rw_predecessor(&key, read_version.as_ref())
            .await?
        {
            Ok(predecessor) => predecessor,
            // A read superseded before anything was staged is the second
            // certified body-replay case (ADR-053).
            Err(Ineligible::Replay) => return Ok(DirectAttempt::Replay),
            Err(Ineligible::Locked) => return Ok(DirectAttempt::Locked),
        };

        let resolver = Arc::new(DirectCommitResolver {
            id: tx.id.clone(),
            raw_key,
            leaf_path: leaf_path.clone(),
            key,
            value,
            read_version,
            inline,
            split_hints: self.split_hints.clone(),
        });
        let outcome = self
            .coord
            .submit_shard(&leaf_path, &tx.id, resolver, Requirement::Any)
            .await?;
        match outcome {
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Landed,
                ..
            }) => {
                self.direct_commit_stats
                    .landed
                    .fetch_add(1, Ordering::Relaxed);
                tx.commit();
                // The predecessor lost its reference, so it may now be
                // collectable. Only a hint: it was resolved before the CAS, and
                // a logless predecessor has no object to collect at all.
                feed_gc_hints(&self.gc, vec![writer]);
                Ok(DirectAttempt::Committed)
            }
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::InDoubt(msg),
                ..
            }) => Err(TransError::Storage(StorageError::Unavailable(msg))),
            // The round certified that this read-modify-write staged nothing
            // durable, so its body is reevaluated instead of publishing a holder
            // merely because it shared a coordinator round (ADR-053).
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Replay,
                ..
            }) => Ok(DirectAttempt::Replay),
            // These outcomes staged nothing either, but none of them certifies
            // the replay case, so the locked protocol takes over under the same
            // id: the entry moved or is genuinely contended (`Moved`), the inline
            // node did not fit or the round was exhausted (`Conflict`), a split
            // moved the key (`Reroute`), or a shutdown ran no CAS at all
            // (`None`).
            Some(CoordinatedOutcome {
                outcome: FoldOutcome::Moved | FoldOutcome::Conflict | FoldOutcome::Reroute,
                ..
            })
            | None => Ok(DirectAttempt::Locked),
            Some(_) => Err(TransError::other(
                "direct commit produced a non-commit outcome",
            )),
        }
    }

    /// Publishes the committed transaction's pointers and releases its locks.
    /// Idempotent and best-effort: the transaction is already durably committed,
    /// so a write-back failure only delays lazy lock cleanup, never the result.
    /// It is spawned in the background so commit returns immediately rather than
    /// waiting for the pointer publishes and lock releases; a shutdown drains
    /// the spawned task (`Background::spawn_waited`). A live holder defers that
    /// cleanup rather than making the task wait. Without a background executor
    /// (unit tests, or after shutdown dropped it) it releases inline so locks are
    /// not left to lazy reclaim.
    async fn write_back(&self, id: &TxId, locked: LockedTx) {
        match self.background.as_ref().and_then(|w| w.upgrade()) {
            Some(bg) => {
                let locker = self.locker.clone();
                let gc = self.gc.clone();
                let id = id.clone();
                // Cancelling a dedup driver may need to spawn a successor for
                // merged callers, so shutdown drains this finite pass.
                bg.spawn_waited(async move {
                    let superseded = locker.keys().write_back(&id, &locked).await;
                    feed_gc_hints(&gc, superseded);
                });
            }
            None => {
                let superseded = self.locker.keys().write_back(id, &locked).await;
                feed_gc_hints(&self.gc, superseded);
            }
        }
    }

    /// Signals the read-write restart after a genuine wound by returning
    /// [`TransError::Wounded`] so the caller renews the id and re-runs.
    /// Does not back off: the wound already made the identity terminal (its locks are
    /// immediately reclaimable), the locker's CAS loop backs off real lock
    /// contention, and a delay here would only slow the renewed retry.
    async fn restart(&self, _tx: &mut Handle) -> Result<(), TransError> {
        Err(TransError::Wounded)
    }

    /// Acquires every lock the transaction needs, resolving both **CAS
    /// contention** and **suspected deadlocks** internally — without renewing
    /// the id or re-running the body (ADR-020/024). Only one non-success outcome
    /// leaves this loop: [`Acquired::Wounded`], a higher-priority peer having
    /// aborted us (the one conflict that must renew the id and re-run).
    ///
    /// - **CAS contention** (a shard/root lost its bounded CAS race): drop the
    ///   partial locks ([`Locker::release_locks`]) and retry under the **same
    ///   id** after backing off, so a transaction that merely lost a race never
    ///   discards its executed body. Persistent contention escalates to the
    ///   serial order, which removes the equal-priority livelock.
    /// - **Leaf capacity** (a create reached the reserved content limit): drop
    ///   the partial locks and retry under the **same id** after backing off,
    ///   giving the hinted split time to make room. The first capacity failure
    ///   starts [`MAX_LEAF_FULL_WAIT`]; later revisions and reroutes do not reset
    ///   it, because acquisition still has no capacity. Capacity pressure does
    ///   not count toward serial escalation.
    /// - **Suspected deadlock** (the parallel wait exceeded
    ///   [`MAX_DEADLOCK_TIMEOUT`]): drop the out-of-order locks and re-acquire in
    ///   the global serial sorted order, where first-CAS-wins on the lowest
    ///   contended shard guarantees one contender always completes. Serial mode
    ///   cannot deadlock, so it arms no timeout.
    ///
    /// `tx.renewals()` (genuine-wound restarts) starts a heavily-restarted
    /// transaction directly in the serial order as a backstop.
    async fn acquire_locks(
        &self,
        tx: &mut Handle,
        validation_start: SequencePoint,
    ) -> Result<Acquired, TransError> {
        let mut serial = tx.renewals() >= SERIAL_FALLBACK_AFTER;
        let mut conflicts: usize = 0;
        let mut leaf_full_since: Option<rt::Instant> = None;
        loop {
            // A higher-priority peer may have aborted us; re-checked each
            // iteration so a wound landing during a long wait surfaces promptly
            // rather than driving a pointless re-lock.
            if self.was_wounded(tx).await {
                return Ok(Acquired::Wounded);
            }
            let scan_requirement = Requirement::AtLeast(validation_start);
            let outcome = if serial {
                self.locker
                    .keys()
                    .lock_at(&tx.id, &tx.data, true, scan_requirement)
                    .await
            } else {
                let key_locker = self.locker.keys();
                tokio::select! {
                    res = key_locker.lock_at(&tx.id, &tx.data, false, scan_requirement) => res,
                    _ = rt::sleep(MAX_DEADLOCK_TIMEOUT) => Err(TransError::LockTimeout),
                }
            };
            match outcome {
                Ok(LockOutcome::Locked(l)) => return Ok(Acquired::Locked(l)),
                // CAS contention: drop the partial locks and retry under the same
                // id after backing off — no renew, no body re-run. Escalate to
                // the serial order if contention persists.
                Ok(LockOutcome::Conflict) => {
                    self.release_for_retry(tx).await?;
                    conflicts += 1;
                    serial = serial || conflicts >= SERIAL_FALLBACK_AFTER;
                    rt::sleep(tx.backoff.next_delay()).await;
                }
                // Capacity is not lock contention: release anything acquired on
                // other leaves and wait for the hinted split without escalating
                // to the serial lock order or re-running the transaction body.
                Ok(LockOutcome::LeafFull) => {
                    let since = *leaf_full_since.get_or_insert_with(rt::Instant::now);
                    self.release_for_retry(tx).await?;
                    if since.elapsed() >= MAX_LEAF_FULL_WAIT {
                        return Err(TransError::other(format!(
                            "leaf capacity remained unavailable for {} seconds",
                            MAX_LEAF_FULL_WAIT.as_secs()
                        )));
                    }
                    rt::sleep(tx.backoff.next_delay()).await;
                }
                // Suspected deadlock: drop the out-of-order locks and re-acquire
                // in the cannot-deadlock serial order, keeping our id.
                Err(TransError::LockTimeout) => {
                    self.release_for_retry(tx).await?;
                    serial = true;
                }
                Err(e) => return Err(e),
            }
        }
    }

    /// Releases every lock the transaction currently holds before an in-place,
    /// same-id re-lock (the CAS-contention and deadlock-timeout retries). The
    /// transaction object stays pending; only the shard/root lock entries clear.
    async fn release_for_retry(&self, tx: &Handle) -> Result<(), TransError> {
        self.locker
            .keys()
            .release_locks(&tx.id)
            .await
            .map_err(|e| e.context(format!("releasing locks before re-lock for tx {}", tx.id)))
    }

    /// Signals a stale dependency restart (ADR-024/032): a point read or scan
    /// changed before its locks were held, so the body must re-run — but, unlike
    /// [`Algo::restart`], **holding the locks already acquired** and **without
    /// renewing the id**. Returns [`TransError::Retry`], which the db retry loop
    /// re-runs in place (the
    /// transaction object stays pending and its locks stay installed). Any lock
    /// left on a key the re-run no longer touches is reclaimed lazily by the next
    /// contender (ADR-021).
    ///
    /// Unlike [`Algo::restart`] this does **not** back off: the transaction holds
    /// *live* locks here (its object is still pending), so sleeping would block
    /// every peer waiting on those keys and only delay our own release.
    async fn revalidate(&self, _tx: &mut Handle) -> Result<(), TransError> {
        Err(TransError::Retry)
    }

    /// Reports whether the transaction was already aborted by a higher-priority
    /// transaction. Best-effort: a status read error is not treated as a wound.
    async fn was_wounded(&self, tx: &Handle) -> bool {
        matches!(
            self.mon.tx_status(&tx.id).await,
            Ok(TxCommitStatus::Aborted | TxCommitStatus::Wounded)
        )
    }

    /// Reports whether the transaction's snapshot still holds: every read's
    /// effective writer is unchanged (ADR-024) **and** every range scan's
    /// membership dependencies are unchanged (ADR-032 phantom prevention).
    /// When locks are already held, scan resolution ignores this transaction's
    /// own holder ID so it is not mistaken for a concurrent membership change.
    /// Locked validation accepts an exact physical shortcut only from this
    /// transaction's own successful lock CAS. When the leaf has moved, logical
    /// validation compares the observed writer or membership against current
    /// state satisfying the same pre-lock bound; evidence advanced by another
    /// operation can therefore avoid I/O without deciding logical validity.
    async fn validate(
        &self,
        data: &Data,
        context: ValidationContext<'_>,
        validation_start: SequencePoint,
    ) -> Result<bool, TransError> {
        let lock_validation = context.lock_validation();
        let physical_reads_valid = self
            .validate_read_observations(data, validation_start, lock_validation)
            .await?;
        let physical_scans_valid = self
            .validate_scan_observations(data, validation_start, lock_validation)
            .await?;
        let requirement = Requirement::AtLeast(validation_start);
        Ok(
            (physical_reads_valid || self.validate_reads_inner(data, requirement).await?)
                && (physical_scans_valid
                    || self
                        .validate_scans_inner(data, context.own_lock_holder(), validation_start)
                        .await?),
        )
    }

    async fn validate_read_observations(
        &self,
        data: &Data,
        validation_start: SequencePoint,
        lock_validation: Option<&LockedTx>,
    ) -> Result<bool, TransError> {
        for read in &data.reads {
            let leaf_unchanged = match lock_validation {
                Some(locked) => locked.validated(read.observation()),
                None => matches!(
                    self.shards
                        .check_leaf_current(read.observation(), validation_start)
                        .await?,
                    LeafObservationCheck::Current
                ),
            };
            if !leaf_unchanged {
                return Ok(false);
            }
            if read_observation_has_exclusive_holder(read)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    async fn validate_scan_observations(
        &self,
        data: &Data,
        validation_start: SequencePoint,
        lock_validation: Option<&LockedTx>,
    ) -> Result<bool, TransError> {
        for coverage in data.scans.iter().flat_map(|scan| scan.covered()) {
            let leaf_unchanged = match lock_validation {
                Some(locked) => locked.validated(&coverage.observation),
                None => matches!(
                    self.shards
                        .check_leaf_current(&coverage.observation, validation_start)
                        .await?,
                    LeafObservationCheck::Current
                ),
            };
            if !leaf_unchanged {
                return Ok(false);
            }
            for holder in &coverage.pending_membership {
                if self.mon.committed_at(holder, validation_start).await? {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Re-resolves every read's effective writer and reports whether they all
    /// still match what the transaction observed (a consistent snapshot exists).
    /// The read set is resolved in one shard-batched pass (each touched shard is
    /// loaded once) rather than one shard load per key.
    async fn validate_reads_inner(
        &self,
        data: &Data,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        if data.reads.is_empty() {
            return Ok(true);
        }
        let keys: Vec<KeyRef> = data.reads.iter().map(|read| read.key.clone()).collect();
        let current = self.resolver.effective_writers(&keys, requirement).await?;
        for r in &data.reads {
            if current.get(&r.key).and_then(Option::as_ref) != r.last_writer() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Re-scans every range the transaction listed and reports whether each
    /// still covers the same leaves at the same membership versions (ADR-032).
    /// Pending membership writers observed by the original scan are rechecked
    /// because their commit transition does not itself bump the node version.
    /// If physical coverage changed, status-aware resolution distinguishes a
    /// harmless split from a logical page change.
    async fn validate_scans_inner(
        &self,
        data: &Data,
        own_lock_holder: Option<&TxId>,
        validation_start: SequencePoint,
    ) -> Result<bool, TransError> {
        let requirement = Requirement::AtLeast(validation_start);
        for scan in &data.scans {
            let current = self
                .resolver
                .scan_coverage(
                    &scan.collection,
                    &scan.range,
                    scan.frontier(),
                    own_lock_holder,
                    requirement,
                )
                .await?;
            let mut fast = current.len() == scan.covered().len()
                && !current.iter().zip(scan.covered()).any(|(now, observed)| {
                    now.path != observed.path
                        || now.membership_version != observed.membership_version
                });
            if fast {
                for holder in scan
                    .covered()
                    .iter()
                    .flat_map(|leaf| &leaf.pending_membership)
                {
                    if self.mon.committed_at(holder, validation_start).await? {
                        fast = false;
                        break;
                    }
                }
            }
            if fast {
                continue;
            }

            let resolved = self
                .resolver
                .scan_keys_at(
                    &scan.collection,
                    &scan.range,
                    &scan.overlay,
                    own_lock_holder,
                    scan.frontier(),
                    requirement,
                )
                .await?;
            if resolved.keys() != scan.keys() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Builds and writes the committed transaction object (the commit point).
    /// Records `locks` (the held lock set) alongside `writes` so the object
    /// carries its full back-reference set for GC's reverse check (ADR-022).
    async fn commit_writes(
        &self,
        data: &Data,
        collections: &CollectionAttempt,
        locks: Vec<TxLock>,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut tl = TxLog::new(id.clone(), TxCommitStatus::Ok);
        for w in &data.writes {
            let (value, deleted): (Arc<[u8]>, bool) = match &w.op {
                WriteOp::Put(value) => (value.clone(), false),
                WriteOp::Delete => (Arc::from(&[] as &[u8]), true),
            };
            tl.writes.push(TxWrite {
                key: w.key.clone(),
                value,
                deleted,
                prev_writer: TxId::default(),
            });
        }
        collections.committed_manifest(locks).apply_to(&mut tl);
        // `context` preserves the `AlreadyFinalized` sentinel and any in-doubt
        // outcome instead of collapsing them into a generic error.
        self.mon
            .commit_tx(tl)
            .await
            .map_err(|e| e.context("creating transaction object"))
    }
}

/// Feeds the transaction ids a write-back superseded to GC as reverse-check
/// candidates (ADR-022): each is a former `current_writer` a fresh commit's
/// pointer overwrote, so it just lost a reference and may now be collectable.
fn feed_gc_hints(gc: &Gc, superseded: Vec<TxId>) {
    for prev in superseded {
        gc.schedule_tx_cleanup(prev);
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use super::*;
    use crate::access::{ScanRange, WriteAccess};
    use crate::collection_catalog::CollectionCatalog;
    use crate::collection_coordination::CollectionStateResolver;
    use crate::collections::{CollectionChange, CollectionLifecycle, CollectionOp};
    use crate::engine::{Engine, EngineConfig};
    use crate::key_state_resolver::KeyStateResolver;
    use crate::monitor::{ProtocolTiming, TxRecoveryManifest};
    use crate::reader::Reader;
    use glassdb_backend::middleware::{
        BackendOp, HookBackend, HookFuture, OpLog, OpRecord, RecordingBackend,
    };
    use glassdb_backend::{Backend, StatsBackend, memory::MemoryBackend};
    use glassdb_concurr::{Background, RetryConfig};
    use glassdb_data::{
        CollectionAddress, CollectionId, DatabaseId, DbRoot, LeafRef, NodeToken, ObjectPath,
    };
    use glassdb_storage::transaction::{TLogger, TxCommitStatus};
    use glassdb_storage::{
        CachedStore, CollectionRecord, CollectionStore, CurrentState, Node, Shard, ShardEntry,
        ShardStore, StructuralLogStore, TreeRouter,
    };

    const TEST_DB: &str = "testp";

    fn test_collection() -> CollectionAddress {
        CollectionAddress::root(TEST_DB)
    }

    fn test_db_root() -> DbRoot {
        DbRoot::try_from(TEST_DB).unwrap()
    }

    fn test_root_path() -> ObjectPath {
        ObjectPath::TreeRoot {
            collection: test_collection(),
        }
    }

    fn key_ref(key: &[u8]) -> KeyRef {
        KeyRef::new(test_collection(), key)
    }

    struct Tctx {
        backend: Arc<dyn Backend>,
        tlogger: TLogger,
        tmon: Monitor,
        records: CollectionStore,
        shards: ShardStore,
        timeline: Timeline,
        locker: Locker,
    }

    async fn new_algo() -> (Algo, Tctx) {
        new_algo_from_backend(Arc::new(MemoryBackend::new())).await
    }

    async fn new_algo_from_backend(b: Arc<dyn Backend>) -> (Algo, Tctx) {
        new_algo_from_backend_with_cache(b, 1024).await
    }

    async fn new_algo_with_policy(policy: SplitPolicy) -> (Algo, Tctx) {
        new_algo_from_backend_with_cache_and_policy(Arc::new(MemoryBackend::new()), 1024, policy)
            .await
    }

    async fn new_algo_from_backend_with_cache(
        b: Arc<dyn Backend>,
        cache_bytes: usize,
    ) -> (Algo, Tctx) {
        new_algo_from_backend_with_cache_and_policy(b, cache_bytes, SplitPolicy::default()).await
    }

    async fn new_algo_from_backend_with_cache_and_policy(
        b: Arc<dyn Backend>,
        cache_bytes: usize,
        split_policy: SplitPolicy,
    ) -> (Algo, Tctx) {
        let timeline = Timeline::new();
        let objects = CachedStore::new(b.clone(), cache_bytes, timeline.clone(), None);
        let tlogger = TLogger::new(objects.clone(), test_db_root());
        let bg = Arc::new(Background::new());
        let bg_weak = Arc::downgrade(&bg);
        // Leak the background so spawned async aborts can run for the test's
        // lifetime without us threading the owner through every helper.
        std::mem::forget(bg);
        let tmon = Monitor::with_config(
            tlogger.clone(),
            timeline.clone(),
            bg_weak.clone(),
            RetryConfig::default(),
            ProtocolTiming::simulation(),
        );
        let records = CollectionStore::new(objects.clone());
        let shards = ShardStore::new(objects.clone());
        let structural_logs = StructuralLogStore::new(objects.clone());
        let collection_state = CollectionStateResolver::new(
            records.clone(),
            tlogger.clone(),
            tmon.clone(),
            RetryConfig::default(),
        );
        let key_state = KeyStateResolver::new(tmon.clone());
        let resolver = KeyResolver::new(TreeRouter::new(shards.nodes().clone()), key_state.clone());
        let router = TreeRouter::new(shards.nodes().clone());
        let (coord, splitter) = crate::split::Splitter::with_coordinator(
            bg_weak.clone(),
            records.clone(),
            shards.clone(),
            structural_logs.clone(),
            timeline.clone(),
            tmon.clone(),
            key_state,
            RetryConfig::default(),
            test_db_root(),
            split_policy,
            glassdb_storage::InlinePolicy::default(),
        );
        let locker = Locker::new(
            coord.clone(),
            router,
            collection_state.clone(),
            tmon.clone(),
            RetryConfig::default(),
        );
        let collection_lifecycle = CollectionLifecycle::new(
            records.clone(),
            shards.clone(),
            tmon.clone(),
            RetryConfig::default(),
            Arc::new(splitter.clone()),
        );
        let gc = Gc::new(
            bg_weak.clone(),
            tlogger.clone(),
            shards.clone(),
            structural_logs,
            timeline.clone(),
            locker.clone(),
            collection_lifecycle.clone(),
            tmon.clone(),
        );
        let collection_commit = CollectionCommit::new(
            CollectionCatalog::new(collection_state),
            collection_lifecycle,
            tmon.clone(),
            split_policy,
        );

        // Create the collection root so the test collection exists up front.
        records
            .create_record(&test_collection(), &CollectionRecord::new())
            .await
            .unwrap();
        shards
            .create_root(&test_collection(), &Node::leaf(Shard::new()))
            .await
            .unwrap();

        let algo = Algo::new(
            shards.clone(),
            timeline.clone(),
            RetryConfig::default(),
            locker.clone(),
            coord.clone(),
            tmon.clone(),
            collection_commit,
            gc,
            None,
            resolver,
            split_policy,
            glassdb_storage::InlinePolicy::default(),
            splitter.hint_sink(),
        );
        (
            algo,
            Tctx {
                backend: b,
                tlogger,
                tmon,
                records,
                shards,
                timeline,
                locker,
            },
        )
    }

    fn wa(key: &KeyRef, val: &[u8]) -> WriteAccess {
        WriteAccess::put(key.clone(), Arc::from(val))
    }

    fn wdel(key: &KeyRef) -> WriteAccess {
        WriteAccess::delete(key.clone())
    }

    async fn do_read(tctx: &Tctx, key: &KeyRef) -> ReadAccess {
        let outcome = read_outcome(tctx, key).await;
        let (_, _, evidence) = outcome.into_parts();
        ReadAccess::new(key.clone(), evidence)
    }

    async fn read_outcome(tctx: &Tctx, key: &KeyRef) -> crate::reader::ReadOutcome {
        let reader = Reader::new(
            KeyResolver::new(
                TreeRouter::new(tctx.shards.nodes().clone()),
                KeyStateResolver::new(tctx.tmon.clone()),
            ),
            tctx.timeline.clone(),
            RetryConfig::default(),
        );
        match reader.read(key, Duration::MAX).await {
            Ok(outcome) => outcome,
            Err(e) => panic!("reading {key:?}: {e:?}"),
        }
    }

    fn begin_data(tm: &Algo, data: Data) -> Handle {
        tm.begin(data, CollectionData::default())
    }

    /// Runs one resolver fold and retains its complete classification.
    async fn fold_step(
        resolver: &dyn ShardResolver,
        tctx: &Tctx,
        cause: ReloadCause,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        locks: &NodeLocks,
    ) -> Step {
        let key_state = KeyStateResolver::new(tctx.tmon.clone());
        let ctx = ResolveCtx {
            key_state: &key_state,
            tmon: &tctx.tmon,
            requirement: Requirement::Any,
            cause,
        };
        resolver.resolve(&ctx, staged, locks).await.unwrap()
    }

    /// Runs one fold of `resolver` over the leaf state a coordinator round would
    /// hand it, and reports the outcome it classifies. Lets a test drive the
    /// `cause` and node-lock combinations that a live interleaving can only
    /// produce by luck.
    async fn fold(
        resolver: &dyn ShardResolver,
        tctx: &Tctx,
        cause: ReloadCause,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        locks: &NodeLocks,
    ) -> FoldOutcome {
        match fold_step(resolver, tctx, cause, staged, locks).await {
            Step::Skip { outcome } | Step::Stage { outcome, .. } => outcome,
        }
    }

    async fn commit_access(tm: &Algo, d: Data) -> Handle {
        let mut h = begin_data(tm, d);
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
        h
    }

    async fn commit_writes(tm: &Algo, ws: Vec<WriteAccess>) -> Handle {
        commit_access(
            tm,
            Data {
                reads: Vec::new(),
                writes: ws,
                scans: Vec::new(),
            },
        )
        .await
    }

    async fn entry(tctx: &Tctx, key: &[u8]) -> Option<ShardEntry> {
        let loaded = tctx
            .shards
            .load_leaf(&test_root_path(), Requirement::AtLeast(tctx.timeline.now()))
            .await
            .unwrap();
        loaded.entries().lookup(key).cloned()
    }

    #[tokio::test]
    async fn write_new() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        let val = b"v";

        let mut h = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&keyp, val)],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        let tid = h.id().clone();
        tm.end(&mut h).await.unwrap();

        let status = tctx
            .tlogger
            .commit_status_at(&tid, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
        let txlog = tctx.tlogger.get_at(&tid, Requirement::Any).await.unwrap();
        let txlog = txlog.value().unwrap();
        assert_eq!(txlog.writes.len(), 1);
        assert_eq!(txlog.writes[0].key, keyp);
        assert_eq!(&*txlog.writes[0].value, val);

        // The shard entry points at the committed writer and the lock is gone.
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current.writer(), Some(&tid));
        assert!(e.lock_holders().is_empty());
    }

    // Regression (review 1.1 / ADR-022): the committed transaction object must
    // record its full lock set, not just its writes, so GC's reverse liveness
    // check and lock pruning operate on real logs. A transaction that reads one
    // key and creates another records both entry locks plus the leaf's structure
    // and membership scopes (ADR-032).
    #[tokio::test]
    async fn commit_records_locks() {
        let (tm, tctx) = new_algo().await;
        let readp = key_ref(b"r");
        let writep = key_ref(b"w");

        // Seed the read key so it resolves to a committed value.
        commit_writes(&tm, vec![wa(&readp, b"seed")]).await;

        let r = do_read(&tctx, &readp).await;
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: vec![wa(&writep, b"v")],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        let tid = h.id().clone();
        tm.end(&mut h).await.unwrap();

        let txlog = tctx.tlogger.get_at(&tid, Requirement::Any).await.unwrap();
        let txlog = txlog.value().unwrap();
        assert!(txlog.locks.contains(&TxLock::Entry {
            key: readp,
            typ: LockType::Read,
        }));
        assert!(txlog.locks.contains(&TxLock::Entry {
            key: writep,
            typ: LockType::Write,
        }));
        let leaf = LeafRef::root(test_collection());
        assert!(txlog.locks.contains(&TxLock::Membership {
            leaf,
            typ: LockType::Write,
        }));
    }

    #[tokio::test]
    async fn committed_manifest_preserves_prepared_roots_from_earlier_attempts() {
        let (tm, tctx) = new_algo().await;
        let earlier = CollectionAddress::new(
            TEST_DB,
            CollectionId::from_slice(&[1; 16]).expect("fixed ID has the required width"),
        );
        let active = CollectionAddress::new(
            TEST_DB,
            CollectionId::from_slice(&[2; 16]).expect("fixed ID has the required width"),
        );
        let earlier_data = CollectionData {
            reads: Vec::new(),
            changes: vec![CollectionChange {
                parent: test_collection(),
                name: b"earlier".to_vec(),
                collection: earlier.clone(),
                expected: None,
                op: CollectionOp::Create,
            }],
        };
        let active_data = CollectionData {
            reads: Vec::new(),
            changes: vec![CollectionChange {
                parent: test_collection(),
                name: b"active".to_vec(),
                collection: active.clone(),
                expected: None,
                op: CollectionOp::Create,
            }],
        };
        let mut handle = tm.begin(Data::default(), earlier_data);
        tm.collection_commit
            .prepare(&mut handle.collections)
            .await
            .unwrap();
        handle.collections.replace_data(active_data);
        tm.collection_commit
            .prepare(&mut handle.collections)
            .await
            .unwrap();
        let id = handle.id().clone();
        tm.mon.begin_tx(&id);

        tm.commit_writes(&Data::default(), &handle.collections, Vec::new(), &id)
            .await
            .unwrap();

        let log = tctx.tlogger.get_at(&id, Requirement::Any).await.unwrap();
        let log = log.value().unwrap();
        assert_eq!(log.prepared_collections, vec![earlier, active.clone()]);
        assert_eq!(log.collection_changes.len(), 1);
        assert_eq!(log.collection_changes[0].collection, active);
    }

    #[tokio::test]
    async fn pending_collection_manifest_update_preserves_existing_locks() {
        let (tm, tctx) = new_algo().await;
        let created = CollectionAddress::new(
            TEST_DB,
            CollectionId::from_slice(&[3; 16]).expect("fixed ID has the required width"),
        );
        let handle = tm.begin(
            Data::default(),
            CollectionData {
                reads: Vec::new(),
                changes: vec![CollectionChange {
                    parent: test_collection(),
                    name: b"created".to_vec(),
                    collection: created.clone(),
                    expected: None,
                    op: CollectionOp::Create,
                }],
            },
        );
        let id = handle.id().clone();
        let lock = TxLock::Topology {
            collection: test_collection(),
        };
        tm.mon
            .begin_persisted_tx(
                &id,
                TxRecoveryManifest {
                    locks: vec![lock.clone()],
                    ..TxRecoveryManifest::default()
                },
            )
            .await
            .unwrap();

        tm.collection_commit
            .persist_manifest(&id, false, &handle.collections)
            .await
            .unwrap();

        let log = tctx.tlogger.get_at(&id, Requirement::Any).await.unwrap();
        let log = log.value().unwrap();
        assert_eq!(log.status, TxCommitStatus::Pending);
        assert_eq!(log.locks, vec![lock]);
        assert_eq!(log.collection_changes.len(), 1);
        assert_eq!(log.collection_changes[0].collection, created.clone());
        assert_eq!(log.prepared_collections, vec![created]);
    }

    #[tokio::test]
    async fn end_preserves_prepared_collection_when_commit_won() {
        let (tm, tctx) = new_algo().await;
        let prepared = CollectionAddress::new(
            TEST_DB,
            CollectionId::from_slice(&[3; 16]).expect("fixed ID has the required width"),
        );
        let mut handle = tm.begin(
            Data::default(),
            CollectionData {
                reads: Vec::new(),
                changes: vec![CollectionChange {
                    parent: test_collection(),
                    name: b"prepared".to_vec(),
                    collection: prepared.clone(),
                    expected: None,
                    op: CollectionOp::Create,
                }],
            },
        );
        tm.collection_commit
            .prepare(&mut handle.collections)
            .await
            .unwrap();
        let id = handle.id().clone();
        tm.mon.begin_tx(&id);
        assert!(handle.engage());
        tm.commit_writes(&Data::default(), &handle.collections, Vec::new(), &id)
            .await
            .unwrap();

        tm.end(&mut handle).await.unwrap();

        tctx.records
            .load_record(&prepared, Requirement::Any)
            .await
            .expect("committed collection record must survive cleanup");
        tctx.shards
            .load_root(&prepared, Requirement::Any)
            .await
            .expect("committed collection tree root must survive cleanup");
    }

    #[tokio::test]
    async fn a_retried_body_clears_an_abandoned_partial_drop() {
        let (tm, tctx) = new_algo().await;
        let dropped = CollectionAddress::new(
            TEST_DB,
            CollectionId::from_slice(&[3; 16]).expect("fixed ID has the required width"),
        );
        assert!(
            tctx.records
                .create_record(&dropped, &CollectionRecord::new())
                .await
                .unwrap()
        );
        assert!(
            tctx.shards
                .create_root(&dropped, &Node::leaf(Shard::new()))
                .await
                .unwrap()
        );
        let mut handle = tm.begin(
            Data::default(),
            CollectionData {
                reads: Vec::new(),
                changes: vec![CollectionChange {
                    parent: test_collection(),
                    name: b"dropped".to_vec(),
                    collection: dropped.clone(),
                    expected: Some(dropped.id()),
                    op: CollectionOp::Drop,
                }],
            },
        );
        let id = handle.id().clone();
        tm.mon.begin_tx(&id);
        assert!(handle.engage());
        tm.collection_commit
            .fence(&id, &mut handle.collections)
            .await
            .unwrap();
        handle.collections.replace_data(CollectionData::default());

        tm.commit(&mut handle).await.unwrap();

        let (root, _) = tctx
            .shards
            .load_root(&dropped, Requirement::AtLeast(tctx.timeline.now()))
            .await
            .unwrap();
        assert_eq!(root.collection_delete_intent(), None);
        let (record, _) = tctx
            .records
            .load_record(&dropped, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(record.topology_freeze(), None);
        tm.end(&mut handle).await.unwrap();
    }

    #[tokio::test]
    async fn read_then_write_round_trips() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");

        let h = commit_writes(&tm, vec![wa(&keyp, b"init")]).await;
        let _ = h;

        let r = do_read(&tctx, &keyp).await;
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: vec![wa(&keyp, b"v2")],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let r = do_read(&tctx, &keyp).await;
        assert_eq!(r.last_writer().unwrap(), h.id());
    }

    // Full path (ADR-024): a read whose value moved before it was locked does not
    // abort-and-renew; it re-runs the body in place (`Retry`) while holding its
    // locks. The engine validates *after* locking, so unlike a pre-lock check the
    // moved key is itself locked during the re-run window — the v1 guarantee that
    // the retry holds all its locks. Two writes force the full locked path (the
    // direct commit path handles a lone write; see the test below).
    #[tokio::test]
    async fn stale_read_write_retries_holding_locks() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let ka = key_ref(b"k");
        let kb = key_ref(b"k2");

        // Seed both keys so the writes are overwrites (not creates), keeping the
        // transaction on the read-write path rather than a membership change.
        commit_writes(&tm2, vec![wa(&ka, b"v1")]).await;
        commit_writes(&tm2, vec![wa(&kb, b"x1")]).await;
        let ra = do_read(&tctx, &ka).await;

        // Another client overwrites `k`, making `ra` stale.
        commit_writes(&tm2, vec![wa(&ka, b"v2")]).await;

        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![ra],
                writes: vec![wa(&ka, b"v3"), wa(&kb, b"x2")],
                scans: Vec::new(),
            },
        );
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");

        // The moved key is locked by us when the stale read is signalled: the
        // re-run owns the lock and cannot lose it again to the same race.
        let e = entry(&tctx, b"k").await.expect("entry exists");
        assert_eq!(e.lock_holders(), std::slice::from_ref(h.id()));

        tm.end(&mut h).await.unwrap();
    }

    // Single-rw commit (ADR-030): a lone read-modify-write whose read was
    // superseded by *another instance* is caught with a transparent retry, never
    // a surfaced error, and never commits its stale value. This client's cached
    // snapshot predates the peer's create, so the key reads as absent — an
    // unsupported shape rather than a certified stale read, which is why the
    // locked path takes over instead of replaying the body (ADR-053). It resolves
    // as `Wounded` or `Retry` depending on whether the snapshot survived to the
    // commit fold; both converge on a fresh read.
    #[tokio::test]
    async fn single_rw_stale_read_renews_and_converges() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let keyp = key_ref(b"k");

        commit_writes(&tm2, vec![wa(&keyp, b"v1")]).await;
        let ra = do_read(&tctx, &keyp).await;

        // Another client overwrites the key, making `ra` stale.
        let h2 = commit_writes(&tm2, vec![wa(&keyp, b"v2")]).await;

        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![ra],
                writes: vec![wa(&keyp, b"v3")],
                scans: Vec::new(),
            },
        );
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(
            matches!(err, TransError::Wounded | TransError::Retry),
            "a stale read is a transparent retry, got {err:?}"
        );
        tm.end(&mut h).await.unwrap();

        // The stale write never committed: v2 is still current (the abandoned
        // attempt's object is unreferenced, so help-forward cannot promote it).
        assert_eq!(
            do_read(&tctx, &keyp).await.last_writer().cloned().unwrap(),
            *h2.id(),
            "the stale write did not commit; v2 is still current"
        );

        // A fresh read + commit converges (the re-run observes v2 and commits).
        let ra2 = do_read(&tctx, &keyp).await;
        let h3 = commit_access(
            &tm,
            Data {
                reads: vec![ra2],
                writes: vec![wa(&keyp, b"v3")],
                scans: Vec::new(),
            },
        )
        .await;
        assert_eq!(
            do_read(&tctx, &keyp).await.last_writer().cloned().unwrap(),
            *h3.id(),
            "the renewed attempt commits"
        );
    }

    // ADR-024: a suspected deadlock is broken *inside* `Algo`, never surfaced. A
    // transaction that cannot wound the holder of a lock it needs waits; the
    // wait is bounded by `MAX_DEADLOCK_TIMEOUT`, after which the transaction
    // releases its locks and re-acquires them in the cannot-deadlock serial
    // order — under the *same id*, re-running no body. It never returns
    // `LockTimeout`, and once the holder finalizes it commits.
    #[tokio::test(start_paused = true)]
    async fn deadlock_timeout_relocks_serially_keeping_id() {
        use crate::tlocker::LockOutcome;
        use std::time::Duration;
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");

        // An older holder takes the key's write lock and does not finalize.
        let holder = TxId::with_priority(0, b"holder");
        tctx.tmon.begin_tx(&holder);
        let held = tctx
            .locker
            .keys()
            .lock_at(
                &holder,
                &Data {
                    reads: Vec::new(),
                    writes: vec![wa(&keyp, b"h")],
                    scans: Vec::new(),
                },
                false,
                Requirement::AtLeast(tctx.timeline.now()),
            )
            .await
            .unwrap();
        assert!(
            matches!(held, LockOutcome::Locked(_)),
            "older holder should acquire its lock"
        );

        // A younger transaction wants the same key; it cannot wound the holder.
        // Drive its commit concurrently so we can observe it parked waiting.
        let mut h = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&keyp, b"a")],
                scans: Vec::new(),
            },
        );
        let id_before = h.id().clone();
        let tm2 = tm.clone();
        let committing = tokio::spawn(async move {
            let res = tm2.commit(&mut h).await;
            (h, res)
        });

        // Let the parallel wait time out and escalate to serial. Serial cannot
        // wound the older peer either, so the transaction keeps waiting — it has
        // not aborted and has surfaced no error.
        rt::sleep(MAX_DEADLOCK_TIMEOUT + Duration::from_secs(1)).await;
        assert!(
            !committing.is_finished(),
            "younger keeps waiting on the older holder after escalating to serial"
        );

        // Finalizing the holder releases the younger, which commits under its
        // original id without ever surfacing `LockTimeout`.
        tctx.tmon.abort_owned_tx(&holder).await.unwrap();
        let (mut h, res) = committing.await.unwrap();
        res.expect("younger commits once the holder releases");
        assert_eq!(
            *h.id(),
            id_before,
            "the id is preserved across the serial fallback (no renew)"
        );
        tm.end(&mut h).await.unwrap();
    }

    // A database can contain an unsafe singleton written by an older client or
    // admitted under a former policy. If capacity remains unavailable while the
    // splitter cannot relieve it, lock acquisition must report the bounded wait
    // instead of retrying forever.
    #[tokio::test(start_paused = true)]
    async fn leaf_capacity_retry_episode_is_bounded() {
        let policy = SplitPolicy {
            leaf_max_bytes: 384,
            node_max_bytes: 512,
            split_headroom_bytes: 128,
            ..SplitPolicy::default()
        };
        let (tm, tctx) = new_algo_with_policy(policy).await;

        let mut low = 0;
        let mut high = policy.content_limit() + 1;
        while low + 1 < high {
            let middle = low + (high - low) / 2;
            if policy.key_fits(&vec![b'a'; middle]) {
                low = middle;
            } else {
                high = middle;
            }
        }
        let first = vec![b'a'; low];
        let second = vec![b'z'; low];
        assert!(policy.key_fits(&first));
        assert!(policy.key_fits(&second));

        let writer = TxId::with_priority(1, b"old");
        let unsafe_entry = ShardEntry::new(first.clone()).with_current(CurrentState::Inline {
            writer,
            value: Arc::from(vec![b'v'; 128]),
        });
        assert!(!policy.entry_fits_split_budget(&unsafe_entry));
        let creator = TxId::with_priority(2, b"new");
        let mut create_entry = ShardEntry::new(second.clone());
        create_entry.replace_create_lock(creator);
        assert!(
            Node::leaf(Shard::from_entries([unsafe_entry.clone(), create_entry]))
                .content_encoded_len()
                > policy.content_limit(),
            "the accepted second key must reproduce LeafFull"
        );
        assert!(
            Node::leaf(Shard::from_entries([unsafe_entry.clone()])).encoded_len()
                <= policy.node_max_bytes,
            "the grandfathered singleton itself must remain storable"
        );

        let path = test_root_path();
        let loaded = tctx
            .shards
            .load_leaf(&path, Requirement::AtLeast(tctx.timeline.now()))
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(Shard::from_entries([unsafe_entry]));
        assert!(tctx.shards.commit_leaf(edit).await.unwrap());

        let key = key_ref(&second);
        let mut handle = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&key, b"value")],
                scans: Vec::new(),
            },
        );
        let error = tokio::time::timeout(
            MAX_LEAF_FULL_WAIT + Duration::from_secs(10),
            tm.commit(&mut handle),
        )
        .await
        .expect("the unchanged full leaf must produce a terminal result")
        .unwrap_err();
        match error {
            TransError::Other { msg, .. } => {
                assert!(msg.contains("leaf capacity"), "got {msg}");
                assert!(msg.contains("remained unavailable"), "got {msg}");
            }
            other => panic!("expected a stalled-capacity error, got {other:?}"),
        }
        tm.end(&mut handle).await.unwrap();
    }

    /// Controls a hook that makes a bounded number of leaf CASes miss.
    struct FlakyCas {
        path: String,
        armed: std::sync::atomic::AtomicBool,
        remaining: std::sync::atomic::AtomicUsize,
        attempts: std::sync::Mutex<Vec<rt::Instant>>,
    }

    impl FlakyCas {
        fn wrap(
            inner: Arc<dyn Backend>,
            path: String,
            budget: usize,
        ) -> (Arc<HookBackend>, Arc<Self>) {
            let flaky = Arc::new(Self {
                path,
                armed: std::sync::atomic::AtomicBool::new(false),
                remaining: std::sync::atomic::AtomicUsize::new(budget),
                attempts: std::sync::Mutex::new(Vec::new()),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let flaky = flaky.clone();
                move |op| {
                    use std::sync::atomic::Ordering::SeqCst;
                    let targeted = match op {
                        BackendOp::WriteIf { path, value, .. } if path == &flaky.path => {
                            Node::decode(value)
                                .ok()
                                .and_then(|node| node.as_leaf().cloned())
                                .is_some_and(|leaf| {
                                    leaf.entries().any(|entry| !entry.lock_holders().is_empty())
                                })
                        }
                        _ => false,
                    };
                    let armed = targeted && flaky.armed.load(SeqCst);
                    if armed {
                        flaky.attempts.lock().unwrap().push(rt::Instant::now());
                    }
                    let fail = armed
                        && flaky
                            .remaining
                            .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                            .is_ok();
                    let result = if fail {
                        Err(glassdb_backend::BackendError::Precondition)
                    } else {
                        Ok(())
                    };
                    let future: HookFuture = Box::pin(async move { result });
                    future
                }
            });
            (backend, flaky)
        }

        fn arm(&self) {
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn remaining(&self) -> usize {
            self.remaining.load(std::sync::atomic::Ordering::SeqCst)
        }

        fn attempts(&self) -> Vec<rt::Instant> {
            self.attempts.lock().unwrap().clone()
        }
    }

    /// Controls a hook that gates the coordinator's bounded seed read.
    struct Gate {
        notify: Arc<tokio::sync::Notify>,
        armed: std::sync::atomic::AtomicBool,
        skip: std::sync::atomic::AtomicUsize,
    }

    impl Gate {
        fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
            let gate = Arc::new(Self {
                notify: Arc::new(tokio::sync::Notify::new()),
                armed: std::sync::atomic::AtomicBool::new(false),
                skip: std::sync::atomic::AtomicUsize::new(0),
            });
            let backend = HookBackend::new(inner);
            backend.set_before({
                let gate = gate.clone();
                move |op| {
                    use std::sync::atomic::Ordering::SeqCst;
                    let wait = matches!(
                        op,
                        BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                    ) && gate.armed.load(SeqCst)
                        && gate
                            .skip
                            .fetch_update(SeqCst, SeqCst, |n| n.checked_sub(1))
                            .is_err();
                    if wait {
                        gate.armed.store(false, SeqCst);
                    }
                    let notify = gate.notify.clone();
                    let future: HookFuture = Box::pin(async move {
                        if wait {
                            notify.notified().await;
                        }
                        Ok(())
                    });
                    future
                }
            });
            (backend, gate)
        }

        fn arm(&self) {
            // Point routing is cache-local; the coordinator seed is now the
            // first backend read in the lock phase.
            self.skip.store(0, std::sync::atomic::Ordering::SeqCst);
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }

        fn release(&self) {
            self.notify.notify_one();
        }
    }

    /// Controls a post-hook that reports one successfully landed leaf CAS as in-doubt.
    struct InDoubtCas {
        armed: std::sync::atomic::AtomicBool,
    }

    impl InDoubtCas {
        fn wrap(inner: Arc<dyn Backend>) -> (Arc<HookBackend>, Arc<Self>) {
            let in_doubt = Arc::new(Self {
                armed: std::sync::atomic::AtomicBool::new(false),
            });
            let backend = HookBackend::new(inner);
            backend.set_after({
                let in_doubt = in_doubt.clone();
                move |op, outcome| {
                    use std::sync::atomic::Ordering::SeqCst;
                    let fail = outcome.is_success()
                        && matches!(op, BackendOp::WriteIf { path, .. }
                            if path.contains("/_n/") || path.ends_with("/_r"))
                        && in_doubt
                            .armed
                            .compare_exchange(true, false, SeqCst, SeqCst)
                            .is_ok();
                    let result = if fail {
                        Err(glassdb_backend::BackendError::Unavailable(
                            "simulated in-doubt shard CAS".into(),
                        ))
                    } else {
                        Ok(())
                    };
                    let future: HookFuture = Box::pin(async move { result });
                    future
                }
            });
            (backend, in_doubt)
        }

        fn arm(&self) {
            self.armed.store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// A distinct key that shares the same leaf as `base`, for exercising
    /// disjoint-key contention within one leaf object. With split deferred, every
    /// key lives in the collection's single leaf `_r` (ADR-031), so any distinct
    /// key qualifies.
    fn same_shard_sibling(base: &[u8]) -> Vec<u8> {
        let sib = b"sibling".to_vec();
        assert_ne!(sib, base, "sibling must differ from the base key");
        sib
    }

    fn shard_stores(log: &OpLog, path: &str) -> usize {
        log.lock()
            .unwrap()
            .iter()
            .filter(|r| r.path == path && (r.op == "write_if" || r.op == "write_if_not_exists"))
            .count()
    }

    // ADR-028: the logless direct commit is folded by the same shard coordinator
    // as ordinary lock acquisition, so a direct commit and a disjoint-key
    // acquire contending one shard batch into a single CAS round instead of
    // racing two separate loads+CASes. The commit publishes its value and the
    // acquire installs its lock in the one store.
    #[tokio::test(start_paused = true)]
    async fn direct_commit_merges_with_disjoint_acquire() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let rec = Arc::new(RecordingBackend::new(backend));
        let log = rec.log();
        let (tm, tctx) = new_algo_from_backend(rec).await;

        let ka = b"k".to_vec();
        let kb = same_shard_sibling(&ka);
        let kap = key_ref(&ka);
        let kbp = key_ref(&kb);

        // Seed keys A and B committed: the direct commit builds on A's
        // predecessor, and the disjoint acquire overwrites an existing B, so it
        // takes no membership root lock and the round stays a single shard CAS.
        commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
        commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

        let txb = TxId::with_priority(2_000_000_000, b"acquire");
        tctx.tmon.begin_tx(&txb);

        let shard_path = test_root_path().to_string();
        log.lock().unwrap().clear();
        gate.arm();

        // The disjoint acquire is submitted first and becomes the dedup driver,
        // parking in the gated current-bound load; the direct commit then joins
        // that open batch. (Post-ADR-030 the commit's own first attempt is
        // `Any` and would skip the load on a warm cache, so it merges via
        // the driver's already-loading round rather than racing a solo, cache-
        // served CAS — which is exactly the ADR-028 single-round behavior.)
        let (ca, cb) = (tm.clone(), tctx.locker.clone());
        let data_b = Data {
            reads: Vec::new(),
            writes: vec![wa(&kbp, b"vb2")],
            scans: Vec::new(),
        };
        let tb = txb.clone();
        let lock_requirement = Requirement::AtLeast(tctx.timeline.now());
        let acquire = tokio::spawn(async move {
            cb.keys()
                .lock_at(&tb, &data_b, false, lock_requirement)
                .await
        });

        // Let the driver park in the gated load before the commit joins.
        rt::sleep(Duration::from_secs(1)).await;

        let mut ha = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&kap, b"v2")],
                scans: Vec::new(),
            },
        );
        let txa = ha.id().clone();
        let commit = tokio::spawn(async move {
            let result = ca.commit(&mut ha).await;
            (ha, result)
        });

        // Once the commit has queued into the open batch, release the load.
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        let (_ha, committed) = commit.await.unwrap();
        let acquire = acquire.await.unwrap().unwrap();
        committed.expect("the direct commit must land");
        assert!(
            matches!(acquire, LockOutcome::Locked(_)),
            "the disjoint acquire must lock"
        );

        assert_eq!(
            shard_stores(&log, &shard_path),
            1,
            "direct commit and disjoint acquire share one CAS"
        );

        // Both mutations landed in the shared shard write.
        let ea = entry(&tctx, &ka).await.unwrap();
        assert_eq!(
            ea.current,
            CurrentState::Inline {
                writer: txa,
                value: Arc::from(b"v2".as_slice()),
            },
            "the direct commit published its value"
        );
        let eb = entry(&tctx, &kb).await.unwrap();
        assert!(eb.is_locked_by(&txb), "acquire holds B's lock");
    }

    // ADR-028 regression (batched in-doubt): a direct commit co-batched with a
    // disjoint-key acquire whose shared CAS comes back in-doubt (`Unavailable`)
    // recovers idempotently — the engine reloads and re-folds, the commit finds
    // its own marker already published (`Landed`), and the acquire re-installs
    // its own lock (`Locked`) without double-applying. No error is surfaced.
    #[tokio::test(start_paused = true)]
    async fn direct_commit_batched_in_doubt_recovers() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, indoubt) = InDoubtCas::wrap(mem);
        let (backend, gate) = Gate::wrap(backend);
        let (tm, tctx) = new_algo_from_backend(backend).await;

        let ka = b"k".to_vec();
        let kb = same_shard_sibling(&ka);
        let kap = key_ref(&ka);
        let kbp = key_ref(&kb);

        // Seed keys A and B committed (un-gated, before arming): the commit has
        // a predecessor and the acquire overwrites an existing B, so it takes no
        // membership root lock and the round stays a single shard CAS.
        commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
        commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

        let txb = TxId::with_priority(2_000_000_000, b"acquire");
        tctx.tmon.begin_tx(&txb);

        // Arm the merge gate and the in-doubt first CAS together.
        indoubt.arm();
        gate.arm();

        let (ca, cb) = (tm.clone(), tctx.locker.clone());
        let mut ha = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&kap, b"v2")],
                scans: Vec::new(),
            },
        );
        let txa = ha.id().clone();
        let commit = tokio::spawn(async move {
            let result = ca.commit(&mut ha).await;
            (ha, result)
        });
        let data_b = Data {
            reads: Vec::new(),
            writes: vec![wa(&kbp, b"vb2")],
            scans: Vec::new(),
        };
        let tb = txb.clone();
        let lock_requirement = Requirement::AtLeast(tctx.timeline.now());
        let acquire = tokio::spawn(async move {
            cb.keys()
                .lock_at(&tb, &data_b, false, lock_requirement)
                .await
        });

        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        // The in-doubt CAS actually landed, so the re-fold sees both members
        // applied: the commit classifies itself Landed, the acquire re-locks.
        let (_ha, committed) = commit.await.unwrap();
        let acquire = acquire.await.unwrap().unwrap();
        committed.expect("the commit recovers as landed, not in-doubt");
        assert!(
            matches!(acquire, LockOutcome::Locked(_)),
            "the co-batched acquire re-locks idempotently"
        );

        assert_eq!(
            entry(&tctx, &ka).await.unwrap().current,
            CurrentState::Inline {
                writer: txa,
                value: Arc::from(b"v2".as_slice()),
            }
        );
        assert!(entry(&tctx, &kb).await.unwrap().is_locked_by(&txb));
    }

    // ADR-020/024: CAS contention is resolved *inside* `Algo`. A transaction that
    // loses the shard-lock CAS repeatedly releases its (partial) locks and
    // re-acquires them under the *same id* — no renew, no body re-run — escalating
    // to the serial order. It never surfaces `Wounded` for a mere lost race, and
    // commits unchanged once the contention clears. A budget far larger than the
    // ~handful of parallel attempts that fit before the deadlock timeout forces
    // the serial CAS budget to be exhausted, i.e. the `Conflict` path.
    //
    // Uses a two-key write so the transaction is ineligible for the direct commit
    // path (ADR-051) and genuinely exercises the full locked path's same-id
    // serial-fallback behaviour.
    #[tokio::test(start_paused = true)]
    async fn cas_contention_relocks_keeping_id() {
        let retry = RetryConfig {
            initial_interval: Duration::from_millis(10),
            max_interval: Duration::from_millis(20),
        };
        const ACQUISITION_RETRIES: usize = 8;
        let failures = ACQUISITION_RETRIES * crate::shard_coord::CAS_RETRIES;
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, flaky) = FlakyCas::wrap(mem, test_root_path().to_string(), failures);
        Engine::prepare_permanent_collection(backend.as_ref(), TEST_DB)
            .await
            .unwrap();
        let mut config = EngineConfig::default();
        config.set_cache_size(1024);
        config.set_retry_initial_interval(retry.initial_interval);
        config.set_retry_max_interval(retry.max_interval);
        let engine = Engine::open(
            TEST_DB,
            DatabaseId::from_bytes([7; 16]),
            Arc::new(StatsBackend::new(backend)),
            config,
        )
        .await
        .unwrap();
        let keyp = key_ref(b"k");
        let keyp2 = key_ref(b"k2");

        // Seed the keys over a clean connection so their shards exist (the lock
        // CAS is then a `write_if`, the thing we fault).
        let mut seed = engine.begin_transaction(
            Data {
                reads: Vec::new(),
                writes: vec![wa(&keyp, b"v1"), wa(&keyp2, b"v1")],
                scans: Vec::new(),
            },
            CollectionData::default(),
        );
        engine.commit(&mut seed).await.unwrap();
        engine.end(&mut seed).await.unwrap();

        flaky.arm();
        let mut h = engine.begin_transaction(
            Data {
                reads: Vec::new(),
                writes: vec![wa(&keyp, b"v2"), wa(&keyp2, b"v2")],
                scans: Vec::new(),
            },
            CollectionData::default(),
        );
        let id_before = h.id().clone();
        engine
            .commit(&mut h)
            .await
            .expect("commits despite sustained CAS contention");
        assert_eq!(
            *h.id(),
            id_before,
            "CAS contention retries under the same id (no renew)"
        );
        engine.end(&mut h).await.unwrap();

        // The whole budget was consumed, so the transaction did exhaust the
        // serial CAS budget (the `Conflict` path), not merely time out in
        // parallel mode.
        assert_eq!(flaky.remaining(), 0, "expected sustained CAS contention");
        let attempts = flaky.attempts();
        assert!(attempts.len() > failures);
        let acquisition_gaps: Vec<_> = (1..=ACQUISITION_RETRIES)
            .map(|round| {
                let next = round * crate::shard_coord::CAS_RETRIES;
                attempts[next].duration_since(attempts[next - 1])
            })
            .collect();
        assert!(
            acquisition_gaps[0] >= Duration::from_millis(5)
                && acquisition_gaps[0] <= Duration::from_millis(16),
            "configured initial acquisition delay was {:?}",
            acquisition_gaps[0]
        );
        for delay in &acquisition_gaps[ACQUISITION_RETRIES - 2..] {
            assert!(
                *delay >= Duration::from_millis(10) && *delay <= Duration::from_millis(31),
                "configured capped acquisition delay was {delay:?}"
            );
        }
        // It still committed: the shards point at our writer with no live lock.
        for key in [&keyp, &keyp2] {
            let read = engine.read(key, Duration::ZERO).await.unwrap();
            assert_eq!(read.value.unwrap().value.as_ref(), b"v2");
        }
        engine.shutdown().await;
    }

    // A value the inline per-value budget rejects, so its transaction takes the
    // regular locked path instead of ADR-051's logless one (ADR-053).
    fn logged_value() -> Vec<u8> {
        vec![b'v'; glassdb_storage::InlinePolicy::default().max_value_bytes + 1]
    }

    // Builds an algo whose backend records every operation, so tests can prove
    // which commit path ran by counting the CAS writes it issued.
    async fn new_recording_algo() -> (Algo, Tctx, OpLog) {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let (tm, tctx) = new_algo_from_backend(rec).await;
        (tm, tctx, log)
    }

    // CAS-write counts by object kind, the fingerprint of a commit path: a
    // logless direct commit (ADR-051) issues one shard write and no tx object at
    // all; the locked path issues one tx-object write and two shard writes (the
    // lock CAS then the write-back CAS that publishes the pointer — run
    // synchronously here because tests build the algo with no background
    // executor). Node-level
    // locks fold into those writes rather than adding another CAS (ADR-032).
    #[derive(Debug, Default)]
    struct WriteCounts {
        // Writes to a leaf coordination object (ADR-031): a standalone node
        // `/_n/` or the collection root `/_r`, which holds the small collection's
        // single leaf entries. Entry-lock and write-back CAS both land here and
        // cannot be told apart by path alone.
        leaf: usize,
        tx: usize,
    }

    fn write_counts(log: &OpLog) -> WriteCounts {
        let mut c = WriteCounts::default();
        for o in log.lock().unwrap().iter() {
            if o.op != "write_if" && o.op != "write_if_not_exists" {
                continue;
            }
            if let Ok(path) = ObjectPath::try_from(o.path.as_str()) {
                match path {
                    ObjectPath::TreeRoot { .. } | ObjectPath::Node { .. } => c.leaf += 1,
                    ObjectPath::Transaction { .. } => c.tx += 1,
                    _ => {}
                }
            }
        }
        c
    }

    // Transaction shards use two symbols from the same alphabet as path type
    // markers. In particular, a transaction can live under `/_t/_n/`; path
    // substring checks would mistake its object create for a standalone-node
    // write and make commit-path counts depend on random transaction entropy.
    #[test]
    fn write_counts_parses_transaction_shard_named_like_node() {
        let id = TxId::from_bytes(vec![0x97, 0x30]);
        let path = ObjectPath::Transaction {
            db_root: test_db_root(),
            id,
        }
        .to_string();
        assert!(path.contains("/_t/_n/"), "test id mapped to {path:?}");
        let log = Arc::new(std::sync::Mutex::new(vec![OpRecord {
            op: "write_if_not_exists",
            path,
            args: Vec::new(),
        }]));

        let counts = write_counts(&log);
        assert_eq!(counts.leaf, 0);
        assert_eq!(counts.tx, 1);
    }

    // Backend reads against leaf objects: `read` is a cold full read (cache
    // miss), `read_if_modified` a revalidation of a cached copy.
    fn shard_reads(log: &OpLog) -> (usize, usize) {
        let (mut full, mut revalidate) = (0, 0);
        for o in log.lock().unwrap().iter() {
            if !matches!(
                ObjectPath::try_from(o.path.as_str()),
                Ok(ObjectPath::TreeRoot { .. } | ObjectPath::Node { .. })
            ) {
                continue;
            }
            if o.op == "read" {
                full += 1;
            } else if o.op == "read_if_modified" {
                revalidate += 1;
            }
        }
        (full, revalidate)
    }

    // A recording algo with a cache large enough that nothing is evicted, so a
    // warm-cache op count is deterministic across executors.
    async fn new_recording_algo_big_cache() -> (Algo, Tctx, OpLog) {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let (tm, tctx) = new_algo_from_backend_with_cache(rec, 1 << 20).await;
        (tm, tctx, log)
    }

    // ADR-053: a single-key read-modify-write whose value misses the inline
    // budget has no logged fast path to fall to, so it commits through the
    // regular locked protocol: one committed `_t/` object write, one leaf lock
    // CAS, one leaf write-back CAS (run synchronously here because there is no
    // background executor), and no separate membership write — and the new
    // value is durable and readable. With split deferred the leaf is the
    // collection root `_r`, so both leaf CAS's land there (ADR-031).
    #[tokio::test]
    async fn an_overwrite_over_the_inline_budget_takes_the_locked_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
        let r = do_read(&tctx, &keyp).await;

        log.lock().unwrap().clear();
        tctx.locker.stats_and_reset();
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: vec![wa(&keyp, &logged_value())],
                scans: Vec::new(),
            },
        );
        let tid = h.id().clone();
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert!(
            tctx.locker.stats_and_reset().calls >= 1,
            "an over-budget value goes straight to locking, it never replays"
        );
        let c = write_counts(&log);
        assert_eq!(
            c.leaf, 2,
            "locked path: one lock CAS plus one write-back CAS, no membership: {c:?}"
        );
        assert_eq!(c.tx, 1, "one committed-object write: {c:?}");

        // The commit landed: the shard points at us with no live lock, a
        // committed `_t/` object exists, and the value reads back as ours.
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current.writer(), Some(&tid));
        assert!(e.lock_holders().is_empty());
        let status = tctx
            .tlogger
            .commit_status_at(&tid, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(status.status, TxCommitStatus::Ok);
        let r = do_read(&tctx, &keyp).await;
        assert_eq!(r.last_writer().cloned().unwrap(), tid);
    }

    #[tokio::test(start_paused = true)]
    async fn single_rw_observing_a_gate_uses_the_full_locked_path() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
        let read = do_read(&tctx, &keyp).await;

        let gate = TxId::with_priority(0, b"gate");
        tctx.tmon.begin_tx(&gate);
        let (mut root, version) = tctx
            .shards
            .load_root(&test_collection(), Requirement::Any)
            .await
            .unwrap();
        root.set_structural_gate(gate.clone());
        assert!(
            tctx.shards
                .store_root(&test_collection(), &root, &version)
                .await
                .unwrap()
        );

        tctx.locker.stats_and_reset();
        let mut handle = begin_data(
            &tm,
            Data {
                reads: vec![read],
                writes: vec![wa(&keyp, b"v2")],
                scans: Vec::new(),
            },
        );
        let committing_tm = tm.clone();
        let committing = tokio::spawn(async move {
            let result = committing_tm.commit(&mut handle).await;
            (handle, result)
        });
        rt::sleep(Duration::from_millis(50)).await;
        assert!(!committing.is_finished());

        tctx.tmon
            .commit_tx(TxLog::new(gate, TxCommitStatus::Ok))
            .await
            .unwrap();
        let (mut handle, result) = committing.await.unwrap();
        result.unwrap();
        tm.end(&mut handle).await.unwrap();
        assert!(
            tctx.locker.stats_and_reset().calls >= 1,
            "an observed gate bypasses the direct commit"
        );
        assert_eq!(
            do_read(&tctx, &keyp).await.last_writer().cloned(),
            Some(handle.id().clone())
        );
    }

    // ADR-030: a warm single read-write commit reuses the shard the read cached
    // for both its eligibility check and its lock-install fold (`Any`), so
    // it issues no backend shard read for either. The successful install CAS
    // supplies the write-back's lower bound too, so write-back also reuses the
    // installed cached state. A revalidating eligibility, install, or write-back
    // would add a `read_if_modified`, so pinning the total to zero guards the
    // receipt propagation. A large cache keeps this deterministic (nothing is
    // evicted between the read and the commit).
    #[tokio::test]
    async fn single_rw_commit_reuses_cached_shard() {
        let (tm, tctx, log) = new_recording_algo_big_cache().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
        // The read warms the shard in the object cache.
        let r = do_read(&tctx, &keyp).await;

        log.lock().unwrap().clear();
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: vec![wa(&keyp, b"v2")],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let (full, revalidate) = shard_reads(&log);
        assert_eq!(full, 0, "no cold shard read on a warm commit");
        assert_eq!(
            revalidate, 0,
            "eligibility, install, and write-back reuse cache/CAS evidence"
        );
    }

    // A blind single-key put over an existing key (no read) takes the same
    // locked path when its value misses the inline budget.
    #[tokio::test]
    async fn a_blind_put_over_the_inline_budget_takes_the_locked_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

        log.lock().unwrap().clear();
        let mut h = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&keyp, &logged_value())],
                scans: Vec::new(),
            },
        );
        let tid = h.id().clone();
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let c = write_counts(&log);
        assert_eq!(
            c.leaf, 2,
            "locked path: one lock CAS plus one write-back CAS, no membership: {c:?}"
        );
        assert_eq!(c.tx, 1, "one committed-object write: {c:?}");
        assert_eq!(
            entry(&tctx, b"k").await.unwrap().current.writer(),
            Some(&tid)
        );
    }

    // ADR-020 regression: the locked path leaves a write lock held by the
    // *committed* writer until its asynchronous write-back publishes the pointer
    // and releases it. A single-key writer arriving in that window must treat the
    // committed holder as effectively unlocked — help-forwarding it as the
    // predecessor — and stay on the lock-free direct path, rather than bailing to
    // the locked path on the mere presence of the lock (the measured regression).
    // A stale read replays instead.
    #[tokio::test]
    async fn a_committed_holder_keeps_the_next_writer_on_the_direct_path() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        let leaf_path = test_root_path();
        let raw = b"k".to_vec();

        // H0 publishes v1; H1 overwrites through the locked path (its value
        // misses the inline budget), so it has a committed transaction object.
        let h0 = commit_writes(&tm, vec![wa(&keyp, b"v1")])
            .await
            .id()
            .clone();
        let h1 = commit_writes(&tm, vec![wa(&keyp, &logged_value())])
            .await
            .id()
            .clone();

        // Recreate the commit window before write-back: the lock is still held by
        // the committed H1 while the pointer lags at its predecessor H0.
        let loaded = tctx
            .shards
            .load_leaf(&leaf_path, Requirement::AtLeast(tctx.timeline.now()))
            .await
            .unwrap();
        let windowed = Shard::from_entries(loaded.entries().entries().cloned().map(|mut e| {
            if e.key == raw {
                e.replace_write_lock(h1.clone());
                e.current = CurrentState::External { writer: h0.clone() };
            }
            e
        }));
        let mut edit = loaded.into_edit();
        edit.set_entries(windowed);
        assert!(tctx.shards.commit_leaf(edit).await.unwrap());

        // The window is observably at the committed holder H1 (v2), not the
        // lagging pointer H0: the shared resolver already help-forwards it.
        let r = do_read(&tctx, &keyp).await;
        assert_eq!(r.last_writer().cloned().unwrap(), h1);

        // Eligibility mirrors that resolution: given the reconciled lock state,
        // an RMW that read H1 and a blind put are both committable and build on
        // H1, while a read of the superseded H0 is still rejected as stale.
        let requirement = Requirement::AtLeast(tm.timeline.now());
        let (res, _) = tm
            .resolver
            .resolve_key_holders(&keyp, None, requirement)
            .await
            .unwrap();
        assert_eq!(
            eligible_writer(&res, Some(&h1)),
            Ok(h1.clone()),
            "an RMW that read the committed holder builds on it"
        );
        assert_eq!(
            eligible_writer(&res, None),
            Ok(h1.clone()),
            "a blind put builds on the committed holder"
        );
        assert_eq!(
            eligible_writer(&res, Some(&h0)),
            Err(Ineligible::Replay),
            "a read of the superseded value is stale, and replayable"
        );

        // End to end: the writer commits directly over H1 (help-forwarding it
        // into the chain, not orphaning it), taking no lock of its own.
        tctx.locker.stats_and_reset();
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: vec![wa(&keyp, b"v3")],
                scans: Vec::new(),
            },
        );
        let h2 = h.id().clone();
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert_eq!(
            tctx.locker.stats_and_reset().calls,
            0,
            "the committed holder did not push the writer onto the locked path"
        );
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current.writer(), Some(&h2));
        assert!(e.lock_holders().is_empty());
        assert_eq!(
            do_read(&tctx, &keyp).await.last_writer().cloned().unwrap(),
            h2
        );
    }

    // ADR-051: an eligible small overwrite commits in a single conditional leaf
    // CAS that publishes the value itself — no lock, no transaction object, and
    // nothing to write back — and the value reads back from the leaf alone.
    #[tokio::test]
    async fn direct_commit_overwrites_in_one_leaf_cas() {
        let (tm, tctx, log) = new_recording_algo().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
        let r = do_read(&tctx, &keyp).await;

        log.lock().unwrap().clear();
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: vec![wa(&keyp, b"v2")],
                scans: Vec::new(),
            },
        );
        let tid = h.id().clone();
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let c = write_counts(&log);
        assert_eq!(c.leaf, 1, "the commit is one leaf CAS: {c:?}");
        assert_eq!(c.tx, 0, "the transaction has no object at all: {c:?}");

        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(
            e.current,
            CurrentState::Inline {
                writer: tid.clone(),
                value: Arc::from(b"v2".as_slice()),
            }
        );
        assert!(e.lock_holders().is_empty(), "no lock was ever installed");
        assert_eq!(
            read_outcome(&tctx, &keyp)
                .await
                .last_writer()
                .cloned()
                .unwrap(),
            tid
        );
    }

    // ADR-051 regression: a direct commit lands on an entry whose write lock is
    // still held by an *already-committed* writer awaiting write-back, so it must
    // replace that holder. Left in place, writer resolution help-forwards to the
    // holder and resolves the entry *backwards* — to the value this commit just
    // superseded — silently losing updates.
    #[tokio::test]
    async fn direct_commit_replaces_a_committed_holder() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        let leaf_path = test_root_path();
        let raw = b"k".to_vec();

        let h0 = commit_writes(&tm, vec![wa(&keyp, b"v1")])
            .await
            .id()
            .clone();
        let h1 = commit_writes(&tm, vec![wa(&keyp, &logged_value())])
            .await
            .id()
            .clone();

        // The locked path's commit window: the lock is still held by the committed
        // H1 while the current state lags at its predecessor H0.
        let loaded = tctx
            .shards
            .load_leaf(&leaf_path, Requirement::AtLeast(tctx.timeline.now()))
            .await
            .unwrap();
        let windowed = Shard::from_entries(loaded.entries().entries().cloned().map(|mut e| {
            if e.key == raw {
                e.replace_write_lock(h1.clone());
                e.current = CurrentState::External { writer: h0.clone() };
            }
            e
        }));
        let mut edit = loaded.into_edit();
        edit.set_entries(windowed);
        assert!(tctx.shards.commit_leaf(edit).await.unwrap());

        let mut h = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&keyp, b"v3")],
                scans: Vec::new(),
            },
        );
        let h2 = h.id().clone();
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(
            e.current,
            CurrentState::Inline {
                writer: h2.clone(),
                value: Arc::from(b"v3".as_slice()),
            }
        );
        assert!(
            e.lock_holders().is_empty(),
            "the superseded holder was replaced, not preserved"
        );
        let outcome = read_outcome(&tctx, &keyp).await;
        assert_eq!(outcome.last_writer().cloned().unwrap(), h2);
        assert_eq!(&*outcome.value.unwrap().value, b"v3");
    }

    // ADR-051 regression: every reason a fold declines to publish the commit
    // marker must be classified against the round's in-doubt evidence, not just
    // the lost-race one. A structural gate or a collection-delete fence that
    // appears *after* an uncertain CAS is no proof that the CAS did not land, so
    // reporting `Moved` there would let the logged protocol re-run a body whose
    // logless commit may already be durable (and since superseded, invisible).
    #[tokio::test]
    async fn direct_commit_blocked_after_uncertain_cas_stays_in_doubt() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

        let seed = entry(&tctx, b"k").await.unwrap();
        let resolver = DirectCommitResolver {
            id: TxId::with_priority(2, b"direct"),
            raw_key: b"k".to_vec(),
            leaf_path: test_root_path(),
            key: keyp.clone(),
            value: Arc::from(b"v2".as_slice()),
            read_version: seed.current.writer().cloned(),
            inline: InlinePolicy::default(),
            split_hints: tm.split_hints.clone(),
        };
        let staged = BTreeMap::from([(b"k".to_vec(), seed)]);

        let mut gated = NodeLocks::default();
        gated.set_structural_gate(TxId::with_priority(1, b"splitter"));
        let mut fenced = NodeLocks::default();
        fenced.set_delete_intent(TxId::with_priority(1, b"dropper"));

        for (what, locks) in [("a structural gate", &gated), ("a delete fence", &fenced)] {
            // Nothing was written yet, so the logged path may take over.
            let outcome = fold(&resolver, &tctx, ReloadCause::Fresh, &staged, locks).await;
            assert!(
                matches!(outcome, FoldOutcome::Moved),
                "{what} on a fresh fold proves nothing was written, got {outcome:?}"
            );

            let outcome = fold(
                &resolver,
                &tctx,
                ReloadCause::Reloaded { in_doubt: true },
                &staged,
                locks,
            )
            .await;
            assert!(
                matches!(outcome, FoldOutcome::InDoubt(_)),
                "{what} cannot disprove a landed uncertain CAS, got {outcome:?}"
            );
        }
    }

    // ADR-053: only a *superseded read* certifies the body-replay case, and an
    // uncertain CAS still outranks it. Every other way a fold declines is either
    // state the direct path cannot arbitrate or evidence that proves nothing, so
    // it reports `Moved` and the locked protocol takes over. Classifying too
    // broadly would spin the body forever against a holder or a closed budget.
    #[tokio::test]
    async fn direct_commit_replays_only_a_certified_superseded_read() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;
        let seed = entry(&tctx, b"k").await.unwrap();
        let current = seed.current.writer().cloned().unwrap();
        let locks = NodeLocks::default();
        let split_hints = tm.split_hints.clone();

        let direct = |read_version| DirectCommitResolver {
            id: TxId::with_priority(9, b"direct"),
            raw_key: b"k".to_vec(),
            leaf_path: test_root_path(),
            key: keyp.clone(),
            value: Arc::from(b"v2".as_slice()),
            read_version,
            inline: InlinePolicy::default(),
            split_hints: split_hints.clone(),
        };

        // A read the entry has moved past: nothing is staged and the loss is
        // definitive, so the body is reevaluated against the winner.
        let stale = direct(Some(TxId::with_priority(1, b"stale")));
        let staged = BTreeMap::from([(b"k".to_vec(), seed.clone())]);
        let outcome = fold(&stale, &tctx, ReloadCause::Fresh, &staged, &locks).await;
        assert!(
            matches!(outcome, FoldOutcome::Replay),
            "a superseded read staged nothing and can be reevaluated, got {outcome:?}"
        );
        let outcome = fold(
            &stale,
            &tctx,
            ReloadCause::Reloaded { in_doubt: true },
            &staged,
            &locks,
        )
        .await;
        assert!(
            matches!(outcome, FoldOutcome::InDoubt(_)),
            "an uncertain CAS is never downgraded to a replay, got {outcome:?}"
        );

        // A live pending holder is a genuine conflict only wound-wait resolves,
        // even though this transaction also staged nothing.
        let holder = TxId::with_priority(1, b"holder");
        tctx.tmon.begin_tx(&holder);
        let mut held = seed.clone();
        held.replace_write_lock(holder);
        let outcome = fold(
            &direct(Some(current.clone())),
            &tctx,
            ReloadCause::Fresh,
            &BTreeMap::from([(b"k".to_vec(), held)]),
            &locks,
        )
        .await;
        assert!(
            matches!(outcome, FoldOutcome::Moved),
            "a live holder needs the locked protocol, not a replay, got {outcome:?}"
        );

        // A key read as deleted names the very writer that deleted it, so the
        // read is *not* superseded. Testing existence before the read version is
        // what keeps this unsupported shape off the replay path.
        let deleter = TxId::with_priority(1, b"deleter");
        let buried = seed.clone().with_current(CurrentState::Tombstone {
            writer: deleter.clone(),
        });
        let outcome = fold(
            &direct(Some(deleter)),
            &tctx,
            ReloadCause::Fresh,
            &BTreeMap::from([(b"k".to_vec(), buried)]),
            &locks,
        )
        .await;
        assert!(
            matches!(outcome, FoldOutcome::Moved),
            "a put over a tombstone is unsupported, not stale, got {outcome:?}"
        );

        // Aggregate inline admission is owned by the direct resolver. Existing
        // inline values consume the leaf budget, while this key's prior state
        // is replaced rather than double-counted.
        let budgeted = DirectCommitResolver {
            inline: InlinePolicy {
                max_value_bytes: 64,
                max_leaf_bytes: 5,
            },
            ..direct(Some(current.clone()))
        };
        let other_writer = TxId::with_priority(1, b"other");
        let crowded = BTreeMap::from([
            (b"k".to_vec(), seed),
            (
                b"other".to_vec(),
                ShardEntry::new(b"other").with_current(CurrentState::Inline {
                    writer: other_writer,
                    value: Arc::from(b"four".as_slice()),
                }),
            ),
        ]);
        assert!(matches!(
            fold_step(&budgeted, &tctx, ReloadCause::Fresh, &crowded, &locks).await,
            Step::Skip {
                outcome: FoldOutcome::Moved
            }
        ));
        assert_eq!(split_hints.pending_inline_pressure(), 1);
        assert!(matches!(
            fold_step(
                &budgeted,
                &tctx,
                ReloadCause::Reloaded { in_doubt: true },
                &crowded,
                &locks,
            )
            .await,
            Step::Skip {
                outcome: FoldOutcome::InDoubt(_)
            }
        ));
        assert_eq!(split_hints.pending_inline_pressure(), 2);

        let impossible = DirectCommitResolver {
            inline: InlinePolicy {
                max_value_bytes: 64,
                max_leaf_bytes: 1,
            },
            ..direct(Some(current.clone()))
        };
        assert!(matches!(
            fold_step(&impossible, &tctx, ReloadCause::Fresh, &crowded, &locks).await,
            Step::Skip {
                outcome: FoldOutcome::Moved
            }
        ));
        assert_eq!(
            split_hints.pending_inline_pressure(),
            2,
            "a value no leaf can admit does not request a split"
        );

        // The round-level classifications: a same-key claim proves this member
        // folded nothing, while a spent CAS budget proves nothing about an
        // earlier attempt of the same round. And a blind overwrite has no
        // read-dependent computation to reevaluate.
        let rmw = direct(Some(current));
        assert!(matches!(rmw.excluded_outcome(false), FoldOutcome::Replay));
        assert!(matches!(
            rmw.excluded_outcome(true),
            FoldOutcome::InDoubt(_)
        ));
        assert!(
            matches!(rmw.exhausted_outcome(false), FoldOutcome::Moved),
            "an exhausted budget does not certify a replay"
        );
        assert!(
            matches!(direct(None).excluded_outcome(false), FoldOutcome::Moved),
            "a blind overwrite takes the locked protocol instead of replaying"
        );
    }

    // ADR-053: a read-modify-write whose observed version is superseded before
    // anything is published reevaluates its body under the same id. Nothing was
    // staged, so the attempt neither renews nor takes a lock — publishing one
    // would make the key's next direct attempt ineligible for no reason.
    #[tokio::test]
    async fn direct_commit_superseded_read_replays_in_place() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

        // Read v1, then let a later commit supersede it. Both versions are this
        // client's own, so its snapshot sees the winner rather than a stale leaf.
        let stale = do_read(&tctx, &keyp).await;
        let winner = commit_writes(&tm, vec![wa(&keyp, b"v2")])
            .await
            .id()
            .clone();

        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![stale],
                writes: vec![wa(&keyp, b"v3")],
                scans: Vec::new(),
            },
        );
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(
            matches!(err, TransError::Retry),
            "a superseded read replays its body in place, got {err:?}"
        );
        tm.end(&mut h).await.unwrap();
        let status = tctx
            .tlogger
            .commit_status_at(h.id(), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(
            status.status,
            TxCommitStatus::Unknown,
            "ending a replayed attempt writes no transaction object"
        );

        // The stale value never committed and the key kept its logless shape.
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current.writer(), Some(&winner));
        assert!(
            e.lock_holders().is_empty(),
            "a replayed attempt publishes no holder"
        );

        // Reevaluating against the winner commits directly.
        let fresh = do_read(&tctx, &keyp).await;
        let replayed = commit_access(
            &tm,
            Data {
                reads: vec![fresh],
                writes: vec![wa(&keyp, b"v3")],
                scans: Vec::new(),
            },
        )
        .await;
        assert_eq!(
            entry(&tctx, b"k").await.unwrap().current,
            CurrentState::Inline {
                writer: replayed.id().clone(),
                value: Arc::from(b"v3".as_slice()),
            },
            "the replayed body commits in one leaf CAS"
        );
    }

    // ADR-053 regression: two eligible read-modify-writes on one key share a
    // coordinator round, where only one may stage its logless commit. The loser
    // must reevaluate its body under the same id rather than publish a holder —
    // creating one would make every subsequent direct attempt on the key
    // ineligible, turning a local scheduling loss into a lasting logged phase.
    #[tokio::test(start_paused = true)]
    async fn direct_commit_same_key_round_loser_replays_its_body() {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, gate) = Gate::wrap(mem);
        let (tm, tctx) = new_algo_from_backend(backend).await;

        let ka = b"k".to_vec();
        let kb = same_shard_sibling(&ka);
        let kap = key_ref(&ka);
        let kbp = key_ref(&kb);
        commit_writes(&tm, vec![wa(&kap, b"v1")]).await;
        commit_writes(&tm, vec![wa(&kbp, b"vb1")]).await;

        // Both attempts read the same current version, so both are eligible.
        let ra1 = do_read(&tctx, &kap).await;
        let ra2 = do_read(&tctx, &kap).await;
        let rmw = |read| {
            begin_data(
                &tm,
                Data {
                    reads: vec![read],
                    writes: vec![wa(&kap, b"v2")],
                    scans: Vec::new(),
                },
            )
        };
        let (mut h1, mut h2) = (rmw(ra1), rmw(ra2));

        // A disjoint-key acquire drives the round and parks in the gated load, so
        // both direct commits queue into one still-open batch. Their own first
        // fold attempt is cache-served (`Any`, ADR-030), so without a driver they
        // would each win a solo round and never contend.
        gate.arm();
        let driver = TxId::with_priority(1, b"driver");
        tctx.tmon.begin_tx(&driver);
        let locker = tctx.locker.clone();
        let data_b = Data {
            reads: Vec::new(),
            writes: vec![wa(&kbp, b"vb2")],
            scans: Vec::new(),
        };
        let requirement = Requirement::AtLeast(tctx.timeline.now());
        let acquire = tokio::spawn(async move {
            locker
                .keys()
                .lock_at(&driver, &data_b, false, requirement)
                .await
        });
        rt::sleep(Duration::from_secs(1)).await;

        let ta = tm.clone();
        let first = tokio::spawn(async move {
            let res = ta.commit(&mut h1).await;
            (h1, res)
        });
        let tb = tm.clone();
        let second = tokio::spawn(async move {
            let res = tb.commit(&mut h2).await;
            (h2, res)
        });
        rt::sleep(Duration::from_secs(1)).await;
        gate.release();

        assert!(matches!(
            acquire.await.unwrap().unwrap(),
            LockOutcome::Locked(_)
        ));
        let (h1, r1) = first.await.unwrap();
        let (h2, r2) = second.await.unwrap();

        // Which member wins the round's claim depends on id order; that exactly
        // one does is the property under test.
        let (winner, mut replayed) = match (&r1, &r2) {
            (Ok(()), Err(TransError::Retry)) => (h1.id().clone(), h2),
            (Err(TransError::Retry), Ok(())) => (h2.id().clone(), h1),
            other => panic!("expected one commit and one replay, got {other:?}"),
        };

        // The winner's commit is the leaf CAS itself, and the loser left nothing
        // behind for a peer to resolve: no holder on the key and no transaction
        // object under its still-unengaged id.
        let e = entry(&tctx, &ka).await.unwrap();
        assert_eq!(
            e.current,
            CurrentState::Inline {
                writer: winner,
                value: Arc::from(b"v2".as_slice()),
            },
            "the round's winner published its value directly"
        );
        assert!(
            e.lock_holders().is_empty(),
            "the contended round published no holder"
        );
        tm.end(&mut replayed).await.unwrap();
        let status = tctx
            .tlogger
            .commit_status_at(replayed.id(), Requirement::Any)
            .await
            .unwrap();
        assert_eq!(
            status.status,
            TxCommitStatus::Unknown,
            "ending the replayed attempt writes no transaction object"
        );

        // Reevaluating the body against the winner converges without locking.
        let ra3 = do_read(&tctx, &kap).await;
        let mut h = rmw(ra3);
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
        assert_eq!(
            entry(&tctx, &ka).await.unwrap().current.writer(),
            Some(h.id()),
            "the replayed body commits directly on its next attempt"
        );
    }

    // Creating a key is ineligible for the direct commit path (it has no
    // predecessor value to build on), so it takes the locked path. The direct
    // path never calls the locker, so a non-zero lock-call count proves the
    // locked path was taken. The membership-write lock is folded into the same
    // leaf CAS as the entry lock (ADR-032), so lock install + write-back is
    // exactly two.
    #[tokio::test]
    async fn single_rw_create_uses_full_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let keyp = key_ref(b"new");

        log.lock().unwrap().clear();
        tctx.locker.stats_and_reset();
        let mut h = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&keyp, b"v")],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert!(
            tctx.locker.stats_and_reset().calls >= 1,
            "a create takes the full locked path"
        );
        let c = write_counts(&log);
        assert_eq!(
            c.leaf, 2,
            "create folds membership locking into lock install + write-back: {c:?}"
        );
        assert!(entry(&tctx, b"new").await.unwrap().exists());
    }

    // A delete is ineligible for the direct path too (it publishes a tombstone, not
    // a pointer over a predecessor), so it takes the full locked path; the
    // non-zero lock-call count proves it. Membership locking folds into the
    // entry-lock CAS (ADR-032).
    #[tokio::test]
    async fn single_rw_delete_uses_full_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let r = do_read(&tctx, &keyp).await;

        log.lock().unwrap().clear();
        tctx.locker.stats_and_reset();
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: vec![wdel(&keyp)],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert!(
            tctx.locker.stats_and_reset().calls >= 1,
            "a delete takes the full locked path"
        );
        let c = write_counts(&log);
        assert_eq!(
            c.leaf, 2,
            "delete folds membership locking into lock install + write-back: {c:?}"
        );
        assert!(entry(&tctx, b"k").await.unwrap().current.is_tombstone());
    }

    // A two-key write is ineligible (the direct path publishes one value), so
    // the logged path stores external pointers for both committed values
    // (ADR-054).
    #[tokio::test]
    async fn single_rw_multi_key_uses_full_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let ka = key_ref(b"a");
        let kb = key_ref(b"b");

        commit_writes(&tm, vec![wa(&ka, b"v1"), wa(&kb, b"v1")]).await;

        log.lock().unwrap().clear();
        let mut h = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&ka, b"v2"), wa(&kb, b"v2")],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let c = write_counts(&log);
        assert!(c.leaf >= 2, "a multi-key write takes the full path: {c:?}");
        let writer = h.id().clone();
        for (key, key_ref) in [(b"a".as_slice(), &ka), (b"b".as_slice(), &kb)] {
            assert_eq!(
                entry(&tctx, key).await.unwrap().current,
                CurrentState::External {
                    writer: writer.clone()
                }
            );
            assert_eq!(
                read_outcome(&tctx, key_ref)
                    .await
                    .value
                    .unwrap()
                    .value
                    .as_ref(),
                b"v2"
            );
        }
    }

    // Reading a key other than the written one needs that key's shard validated,
    // so the single-key write falls back to the full locked path.
    #[tokio::test]
    async fn single_rw_other_key_read_uses_full_path() {
        let (tm, tctx, log) = new_recording_algo().await;
        let ka = key_ref(b"a");
        let kb = key_ref(b"b");

        commit_writes(&tm, vec![wa(&ka, b"v1"), wa(&kb, b"v1")]).await;
        let ra = do_read(&tctx, &ka).await;

        log.lock().unwrap().clear();
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![ra],
                writes: vec![wa(&kb, b"v2")],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let c = write_counts(&log);
        assert!(
            c.leaf >= 2,
            "a read of another key forces the full path: {c:?}"
        );
    }

    #[tokio::test]
    async fn readonly_validates() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let r = do_read(&tctx, &keyp).await;

        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: Vec::new(),
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn opaque_point_read_evidence_tracks_validation() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

        let outcome = read_outcome(&tctx, &keyp).await;
        let (_, _, evidence) = outcome.into_parts();
        let data = Data {
            reads: vec![ReadAccess::new(keyp.clone(), evidence)],
            writes: Vec::new(),
            scans: Vec::new(),
        };

        let validation_start = tctx.timeline.now();
        assert!(
            tm.validate(&data, ValidationContext::Optimistic, validation_start)
                .await
                .unwrap(),
            "opaque evidence accepts its current value"
        );

        let (peer, _peer_ctx) = new_algo_from_backend(tctx.backend.clone()).await;
        commit_writes(&peer, vec![wa(&keyp, b"v2")]).await;
        let validation_start = tctx.timeline.now();
        assert!(
            !tm.validate(&data, ValidationContext::Optimistic, validation_start)
                .await
                .unwrap(),
            "opaque evidence rejects a superseded value"
        );
    }

    #[tokio::test]
    async fn point_read_re_resolves_writer_at_validation_watermark() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        let previous = commit_writes(&tm, vec![wa(&keyp, b"v1")])
            .await
            .id()
            .clone();

        let holder = TxId::with_priority(1, b"holder");
        tctx.tmon.begin_tx(&holder);
        let holder_data = Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        };
        let locked = match tctx
            .locker
            .keys()
            .lock_at(
                &holder,
                &holder_data,
                false,
                Requirement::AtLeast(tctx.timeline.now()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(locked) => locked,
            _ => panic!("holder lock must succeed"),
        };

        let read = do_read(&tctx, &keyp).await;
        assert_eq!(read.last_writer(), Some(&previous));
        let data = Data {
            reads: vec![read],
            writes: Vec::new(),
            scans: Vec::new(),
        };
        let validation_start = tctx.timeline.now();

        // Finalize only the transaction object. The leaf still contains the
        // same pending lock, so leaf validation alone cannot detect that the
        // effective writer moved.
        let mut log = TxLog::new(holder.clone(), TxCommitStatus::Ok);
        log.locks = locked.locked_paths();
        log.writes.push(TxWrite {
            key: keyp,
            value: Arc::from(b"v2".as_slice()),
            deleted: false,
            prev_writer: previous,
        });
        tctx.tmon.commit_tx(log).await.unwrap();

        assert!(
            !tm.validate_read_observations(&data, validation_start, None)
                .await
                .unwrap(),
            "an exclusive holder prevents the leaf-only shortcut"
        );
        assert!(
            !tm.validate(&data, ValidationContext::Optimistic, validation_start)
                .await
                .unwrap(),
            "writer resolution at the validation watermark observes the committed holder"
        );
    }

    #[tokio::test]
    async fn point_read_accepts_aborted_holder_at_validation_watermark() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        let previous = commit_writes(&tm, vec![wa(&keyp, b"v1")])
            .await
            .id()
            .clone();

        let holder = TxId::with_priority(1, b"holder");
        tctx.tmon.begin_tx(&holder);
        let holder_data = Data {
            reads: Vec::new(),
            writes: vec![wa(&keyp, b"v2")],
            scans: Vec::new(),
        };
        match tctx
            .locker
            .keys()
            .lock_at(
                &holder,
                &holder_data,
                false,
                Requirement::AtLeast(tctx.timeline.now()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(_) => {}
            _ => panic!("holder lock must succeed"),
        }

        let read = do_read(&tctx, &keyp).await;
        assert_eq!(read.last_writer(), Some(&previous));
        let data = Data {
            reads: vec![read],
            writes: Vec::new(),
            scans: Vec::new(),
        };
        let validation_start = tctx.timeline.now();

        // Aborting the holder leaves the previously observed writer effective.
        // The exclusive holder prevents a physical shortcut, then writer
        // resolution at the validation watermark accepts the unchanged value.
        tctx.tmon.abort_owned_tx(&holder).await.unwrap();
        assert!(
            !tm.validate_read_observations(&data, validation_start, None)
                .await
                .unwrap()
        );
        assert!(
            tm.validate(&data, ValidationContext::Optimistic, validation_start)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn locked_validation_requires_its_own_cas_receipt() {
        let (tm, tctx) = new_algo().await;
        let ka = key_ref(b"a");
        let kb = key_ref(b"b");
        commit_writes(&tm, vec![wa(&ka, b"a0"), wa(&kb, b"b0")]).await;

        // `commit_writes` publishes pointers in a background task. This test is
        // about the next lock CAS's receipt, so first wait until that unrelated
        // setup CAS has left the shared leaf quiescent; otherwise it can change
        // the exact observation between `do_read` and the lock under test.
        let mut settled = false;
        for _ in 0..100 {
            if entry(&tctx, b"a")
                .await
                .is_some_and(|entry| entry.lock_holders().is_empty())
            {
                settled = true;
                break;
            }
            rt::yield_now().await;
        }
        assert!(settled, "setup write-back must settle before receipt test");

        let read = do_read(&tctx, &ka).await;
        let observed = read.observation().clone();
        let validation_start = tctx.timeline.now();

        // Another transaction's disjoint lock CAS validates the same pre-CAS
        // leaf after our barrier and therefore advances its shared evidence.
        let other = TxId::with_priority(1, b"other");
        tctx.tmon.begin_tx(&other);
        let other_data = Data {
            reads: Vec::new(),
            writes: vec![wa(&kb, b"b1")],
            scans: Vec::new(),
        };
        let other_locked = match tctx
            .locker
            .keys()
            .lock_at(
                &other,
                &other_data,
                false,
                Requirement::AtLeast(tctx.timeline.now()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(locked) => locked,
            _ => panic!("disjoint lock acquisition must succeed"),
        };
        assert!(other_locked.validated(&observed));

        // Our later lock CAS starts from the leaf containing `other`'s lock. It
        // cannot use `other`'s earlier receipt to certify our original read.
        let current = TxId::with_priority(2, b"current");
        tctx.tmon.begin_tx(&current);
        let current_data = Data {
            reads: vec![read],
            writes: Vec::new(),
            scans: Vec::new(),
        };
        let current_locked = match tctx
            .locker
            .keys()
            .lock_at(
                &current,
                &current_data,
                false,
                Requirement::AtLeast(tctx.timeline.now()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(locked) => locked,
            _ => panic!("disjoint read lock acquisition must succeed"),
        };
        assert!(!current_locked.validated(&observed));
        assert!(
            !tm.validate_read_observations(&current_data, validation_start, Some(&current_locked),)
                .await
                .unwrap()
        );

        tctx.locker.keys().release_locks(&current).await.unwrap();
        tctx.locker.keys().release_locks(&other).await.unwrap();
    }

    #[tokio::test]
    async fn newer_shared_evidence_runs_logical_validation_without_io() {
        let (tm, tctx, log) = new_recording_algo_big_cache().await;
        let ka = key_ref(b"a");
        let kb = key_ref(b"b");
        commit_writes(&tm, vec![wa(&ka, b"a0"), wa(&kb, b"b0")]).await;

        let read = do_read(&tctx, &ka).await;
        let validation_start = tctx.timeline.now();

        // A separate client rewrites the shared leaf for B. Its cache is
        // independent, so it cannot advance the retained observation of A in
        // this database.
        let external_timeline = Timeline::new();
        let external = ShardStore::new(CachedStore::new(
            tctx.backend.clone(),
            1 << 20,
            external_timeline.clone(),
            None,
        ));
        let leaf_path = test_root_path();
        let loaded = external
            .load_leaf(&leaf_path, Requirement::AtLeast(external_timeline.now()))
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, ShardEntry> = loaded
            .entries()
            .entries()
            .cloned()
            .map(|entry| (entry.key.clone(), entry))
            .collect();
        entries.get_mut(b"b".as_slice()).unwrap().current = CurrentState::External {
            writer: TxId::with_priority(3, b"external"),
        };
        let mut edit = loaded.into_edit();
        edit.set_entries(Shard::from_entries(entries.into_values()));
        assert!(external.commit_leaf(edit).await.unwrap());

        // A local disjoint lock observes that external version and publishes a
        // still newer state after our barrier. The original physical revision
        // no longer matches, but A's effective writer remains unchanged.
        let other = TxId::with_priority(4, b"other");
        tctx.tmon.begin_tx(&other);
        let other_data = Data {
            reads: Vec::new(),
            writes: vec![wa(&kb, b"b1")],
            scans: Vec::new(),
        };
        let other_locked = match tctx
            .locker
            .keys()
            .lock_at(
                &other,
                &other_data,
                false,
                Requirement::AtLeast(tctx.timeline.now()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(locked) => locked,
            _ => panic!("disjoint lock acquisition must succeed"),
        };

        let data = Data {
            reads: vec![read],
            writes: Vec::new(),
            scans: Vec::new(),
        };
        log.lock().unwrap().clear();
        assert!(
            !tm.validate_read_observations(&data, validation_start, None)
                .await
                .unwrap(),
            "the retained physical revision changed"
        );
        assert!(
            tm.validate(&data, ValidationContext::Optimistic, validation_start)
                .await
                .unwrap(),
            "logical validation accepts the unchanged writer"
        );
        assert_eq!(
            shard_reads(&log),
            (0, 0),
            "post-bound current evidence satisfies both validation steps locally"
        );

        tctx.locker.keys().release_locks(&other).await.unwrap();
        drop(other_locked);
    }

    #[tokio::test]
    async fn readonly_retry_locks_its_complete_point_read_set() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let ka = key_ref(b"a");
        let kb = key_ref(b"b");

        commit_writes(&tm2, vec![wa(&ka, b"a1"), wa(&kb, b"b1")]).await;
        let ra = do_read(&tctx, &ka).await;
        let rb = do_read(&tctx, &kb).await;
        commit_writes(&tm2, vec![wa(&ka, b"a2")]).await;

        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![ra, rb],
                writes: Vec::new(),
                scans: Vec::new(),
            },
        );
        let err = tm.commit(&mut h).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");
        assert!(h.should_lock_reads());
        for key in [b"a".as_slice(), b"b"] {
            assert_eq!(
                entry(&tctx, key).await.unwrap().lock_type(),
                LockType::None,
                "the failed OCC attempt must not lock"
            );
        }

        // The retry re-reads, then its second validation acquires locks for the
        // complete fresh read set before deciding whether it can commit.
        let ra = do_read(&tctx, &ka).await;
        let rb = do_read(&tctx, &kb).await;
        tm.reset(
            &mut h,
            Data {
                reads: vec![ra, rb],
                writes: Vec::new(),
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        let log = tctx.tlogger.get_at(h.id(), Requirement::Any).await.unwrap();
        let log = log.value().unwrap();
        for key in [ka, kb] {
            assert!(log.locks.contains(&TxLock::Entry {
                key,
                typ: LockType::Read,
            }));
        }
    }

    #[tokio::test]
    async fn readonly_after_delete_not_found() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let deleted_by = commit_writes(&tm, vec![wdel(&keyp)]).await.id().clone();

        // A read now resolves to not-found.
        let r = do_read(&tctx, &keyp).await;
        assert_eq!(r.last_writer(), Some(&deleted_by));
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: Vec::new(),
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn delete_round_trips() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let r = do_read(&tctx, &keyp).await;
        let mut h = begin_data(
            &tm,
            Data {
                reads: vec![r],
                writes: vec![wdel(&keyp)],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let e = entry(&tctx, b"k").await.unwrap();
        assert!(e.current.is_tombstone());
        let r = do_read(&tctx, &keyp).await;
        assert_eq!(r.last_writer(), Some(h.id()));
    }

    #[tokio::test]
    async fn multi_key_commit() {
        let (tm, tctx) = new_algo().await;
        let k1 = key_ref(b"k1");
        let k2 = key_ref(b"k2");

        let mut h = begin_data(
            &tm,
            Data {
                reads: Vec::new(),
                writes: vec![wa(&k1, b"v1"), wa(&k2, b"v2")],
                scans: Vec::new(),
            },
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert!(entry(&tctx, b"k1").await.unwrap().exists());
        assert!(entry(&tctx, b"k2").await.unwrap().exists());
    }

    // Installs live committed pointers for `keys` directly in the collection's
    // root leaf `_r` (no lock holders or pending write-back), giving scan tests a
    // stable membership baseline.
    async fn seed_live_keys(tctx: &Tctx, keys: &[&[u8]]) {
        let path = test_root_path();
        let loaded = tctx
            .shards
            .load_leaf(&path, Requirement::AtLeast(tctx.timeline.now()))
            .await
            .unwrap();
        let mut entries: std::collections::BTreeMap<Vec<u8>, ShardEntry> = loaded
            .entries()
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        for (i, k) in keys.iter().enumerate() {
            let w = TxId::with_priority((i as u64) + 1, b"seed");
            entries.insert(
                k.to_vec(),
                ShardEntry::new(*k).with_current(CurrentState::External { writer: w }),
            );
        }
        let shard = Shard::from_entries(entries.into_values());
        let mut edit = loaded.into_edit();
        edit.set_entries(shard);
        assert!(tctx.shards.commit_leaf(edit).await.unwrap());
    }

    // Builds a read-only listing transaction's [`Data`] from a fresh scan of the
    // test collection, returning the scan's live keys alongside so a test can
    // assert on the snapshot and later re-validate the same coverage.
    async fn scan_data_for_range(tctx: &Tctx, range: ScanRange) -> (Data, Vec<Vec<u8>>) {
        let resolver = KeyResolver::new(
            TreeRouter::new(tctx.shards.nodes().clone()),
            KeyStateResolver::new(tctx.tmon.clone()),
        );
        let scan = resolver
            .scan_keys(&test_collection(), &range, &[], None, None)
            .await
            .unwrap();
        let keys = scan.keys().to_vec();
        let access = scan.into_access(test_collection(), range, Vec::new());
        let data = Data {
            reads: Vec::new(),
            writes: Vec::new(),
            scans: vec![access],
        };
        (data, keys)
    }

    async fn scan_data(tctx: &Tctx) -> (Data, Vec<Vec<u8>>) {
        scan_data_for_range(tctx, ScanRange::all()).await
    }

    #[tokio::test]
    async fn opaque_scan_evidence_tracks_validation() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"c"]).await;

        let range = ScanRange::all();
        let resolver = KeyResolver::new(
            TreeRouter::new(tctx.shards.nodes().clone()),
            KeyStateResolver::new(tctx.tmon.clone()),
        );
        let result = resolver
            .scan_keys(&test_collection(), &range, &[], None, None)
            .await
            .unwrap();
        let data = Data {
            reads: Vec::new(),
            writes: Vec::new(),
            scans: vec![result.into_access(test_collection(), range, Vec::new())],
        };

        let validation_start = tctx.timeline.now();
        assert!(
            tm.validate(&data, ValidationContext::Optimistic, validation_start)
                .await
                .unwrap(),
            "opaque evidence accepts its current membership"
        );

        commit_writes(&tm, vec![wa(&key_ref(b"b"), b"1")]).await;
        let validation_start = tctx.timeline.now();
        assert!(
            !tm.validate(&data, ValidationContext::Optimistic, validation_start)
                .await
                .unwrap(),
            "opaque evidence rejects a changed membership"
        );
    }

    // ADR-031 phantom prevention: a listing whose covered leaves are unchanged
    // commits, but one whose leaf a concurrent create mutated (bumping the leaf
    // version) fails validation and must re-run — so the create can never appear
    // as a phantom inside an already-validated snapshot.
    #[tokio::test]
    async fn scan_detects_racing_create() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"c"]).await;

        let (data, keys) = scan_data(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);

        // No concurrent change: the listing validates and commits.
        tctx.locker.stats_and_reset();
        let mut h = begin_data(&tm, data.clone());
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
        assert_eq!(tctx.locker.stats_and_reset().calls, 0);

        // A create between the scan and (re-)validation bumps the covered leaf.
        commit_writes(&tm, vec![wa(&key_ref(b"b"), b"1")]).await;

        let mut stale = begin_data(&tm, data);
        let err = tm.commit(&mut stale).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");
        assert!(
            stale.should_lock_reads(),
            "scan retry escalates to read locks"
        );

        // The retry computes a fresh page, then commits through the locked path.
        let (fresh, keys) = scan_data(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        tm.reset(&mut stale, fresh);
        tctx.locker.stats_and_reset();
        tm.commit(&mut stale).await.unwrap();
        assert!(tctx.locker.stats_and_reset().calls >= 1);
        let log = tctx
            .tlogger
            .get_at(stale.id(), Requirement::Any)
            .await
            .unwrap();
        let log = log.value().unwrap();
        assert!(log.locks.iter().any(|lock| matches!(
            lock,
            TxLock::Membership {
                typ: LockType::Read,
                ..
            }
        )));
        tm.end(&mut stale).await.unwrap();
    }

    #[tokio::test]
    async fn scan_rechecks_pending_membership_holder_that_commits() {
        let (tm, tctx) = new_algo().await;
        let key_path = key_ref(b"new");
        let holder = TxId::with_priority(1, b"holder");
        tctx.tmon.begin_tx(&holder);
        let holder_data = Data {
            reads: Vec::new(),
            writes: vec![wa(&key_path, b"value")],
            scans: Vec::new(),
        };
        let locked = match tctx
            .locker
            .keys()
            .lock_at(
                &holder,
                &holder_data,
                false,
                Requirement::AtLeast(tctx.timeline.now()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(locked) => locked,
            LockOutcome::Conflict => panic!("holder lock conflicted"),
            LockOutcome::LeafFull => panic!("holder leaf unexpectedly full"),
        };

        // The scan observes the pending create as absent and records its
        // membership holder as a status dependency.
        let (scan, keys) = scan_data(&tctx).await;
        assert!(keys.is_empty());

        // Commit only the transaction object: membership_version is unchanged
        // until write-back, so the dependency is what must reject validation.
        let mut log = TxLog::new(holder.clone(), TxCommitStatus::Ok);
        log.locks = locked.locked_paths();
        log.writes.push(TxWrite {
            key: key_path,
            value: Arc::from(b"value".as_slice()),
            deleted: false,
            prev_writer: TxId::default(),
        });
        tctx.tmon.commit_tx(log).await.unwrap();

        let mut stale = begin_data(&tm, scan);
        let err = tm.commit(&mut stale).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");
    }

    #[tokio::test]
    async fn scan_with_write_records_predicate_locks() {
        let (tm, tctx) = new_algo().await;
        let key_path = key_ref(b"a");
        seed_live_keys(&tctx, &[b"a"]).await;
        let (mut data, _) = scan_data(&tctx).await;
        data.writes.push(wa(&key_path, b"updated"));

        let mut handle = begin_data(&tm, data);
        tm.commit(&mut handle).await.unwrap();
        let log = tctx
            .tlogger
            .get_at(handle.id(), Requirement::Any)
            .await
            .unwrap();
        let log = log.value().unwrap();
        assert!(log.locks.contains(&TxLock::Entry {
            key: key_path,
            typ: LockType::Write,
        }));
        let leaf = LeafRef::root(test_collection());
        assert!(log.locks.contains(&TxLock::Membership {
            leaf,
            typ: LockType::Read,
        }));
    }

    #[tokio::test]
    async fn limited_scan_retry_expands_its_locked_frontier() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"b", b"m", b"z"]).await;
        split_root_in_place(&tctx).await;

        let range = ScanRange {
            start: Vec::new(),
            start_exclusive: false,
            end: None,
            limit: Some(2),
        };
        let (mut stale, keys) = scan_data_for_range(&tctx, range.clone()).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(stale.scans[0].frontier(), Some(b"b".as_slice()));
        stale.writes.push(wa(&key_ref(b"a"), b"updated"));

        // Removing the old frontier means the refreshed two-key page reaches
        // into S1. The first locked validation only owns S0 and must retry.
        commit_writes(&tm, vec![wdel(&key_ref(b"b"))]).await;
        let mut handle = begin_data(&tm, stale);
        let err = tm.commit(&mut handle).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");

        // The body re-runs while S0 stays locked. Its new frontier is `m`, so
        // the next validation adds S1 before committing.
        let (mut fresh, keys) = scan_data_for_range(&tctx, range).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"m".to_vec()]);
        assert_eq!(fresh.scans[0].frontier(), Some(b"m".as_slice()));
        fresh.writes.push(wa(&key_ref(b"a"), b"updated"));
        tm.reset(&mut handle, fresh);
        tm.commit(&mut handle).await.unwrap();

        let log = tctx
            .tlogger
            .get_at(handle.id(), Requirement::Any)
            .await
            .unwrap();
        let log = log.value().unwrap();
        for token in [
            NodeToken::from_bytes([0; 16]),
            NodeToken::from_bytes([1; 16]),
        ] {
            assert!(log.locks.contains(&TxLock::Membership {
                leaf: LeafRef::node(test_collection(), token),
                typ: LockType::Read,
            }));
        }
        tm.end(&mut handle).await.unwrap();
    }

    // ADR-032 phantom prevention: a delete bumps the covered leaf's membership
    // version, so an earlier scan fails re-validation.
    #[tokio::test]
    async fn scan_detects_racing_delete() {
        let (tm, tctx) = new_algo().await;
        let bp = key_ref(b"b");
        seed_live_keys(&tctx, &[b"a", b"b"]).await;

        let (data, keys) = scan_data(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

        commit_writes(&tm, vec![wdel(&bp)]).await;

        let mut stale = begin_data(&tm, data);
        let err = tm.commit(&mut stale).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");
    }

    // A pure split changes physical coverage but not logical membership. The
    // fallback re-resolves the page and accepts it without a false retry.
    #[tokio::test]
    async fn scan_accepts_concurrent_split_with_unchanged_membership() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"m"]).await;

        let (data, _keys) = scan_data(&tctx).await;

        // Grow the tree in place: rewrite `_r` from its single leaf into an index
        // root pointing at two fresh leaves (the shape the background splitter
        // produces), so the covered leaf set is no longer just `_r`.
        split_root_in_place(&tctx).await;

        let mut stable = begin_data(&tm, data);
        tm.commit(&mut stable).await.unwrap();
        tm.end(&mut stable).await.unwrap();
    }

    // ADR-032 boundary protection: on a multi-leaf tree a full scan covers every
    // leaf including the endpoints, so a membership change in the final leaf
    // invalidates the scan.
    #[tokio::test]
    async fn scan_detects_boundary_membership_change() {
        use glassdb_storage::{IndexNode, Node};
        let (tm, tctx) = new_algo().await;
        let s0_token = NodeToken::from_bytes([0; 16]);
        let s1_token = NodeToken::from_bytes([1; 16]);

        // Two-leaf tree: index root over S0(a,c | high "m") -> S1(m,p).
        let leaf = |ks: &[&[u8]], high: Option<&[u8]>, right: Option<&str>| {
            Node::leaf(Shard::from_entries(ks.iter().map(|k| {
                ShardEntry::new(*k).with_current(CurrentState::External {
                    writer: TxId::with_priority(1, b"seed"),
                })
            })))
            .with_high_key(high.map(<[u8]>::to_vec))
            .with_right_sibling(right.map(str::to_string))
        };
        tctx.shards
            .store_node(
                &test_collection(),
                &s0_token,
                &leaf(&[b"a", b"c"], Some(b"m"), Some(s1_token.as_str())),
                None,
            )
            .await
            .unwrap();
        tctx.shards
            .store_node(
                &test_collection(),
                &s1_token,
                &leaf(&[b"m", b"p"], None, None),
                None,
            )
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([
            (b"".to_vec(), s0_token.to_string()),
            (b"m".to_vec(), s1_token.to_string()),
        ]));
        let cur = tctx
            .shards
            .load_leaf(&test_root_path(), Requirement::AtLeast(tctx.timeline.now()))
            .await
            .unwrap();
        tctx.shards
            .store_root(&test_collection(), &root, cur.observation())
            .await
            .unwrap();

        let (data, keys) = scan_data(&tctx).await;
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"c".to_vec(), b"m".to_vec(), b"p".to_vec()]
        );

        // Append a key past the current maximum: it lands in the last covered
        // leaf S1, bumping its version.
        let (s1, ver) = tctx
            .shards
            .load_node(
                &test_collection(),
                &s1_token,
                Requirement::AtLeast(tctx.timeline.now()),
            )
            .await
            .unwrap();
        let mut entries: Vec<ShardEntry> = s1.as_leaf().unwrap().entries().cloned().collect();
        entries.push(ShardEntry::new(b"z").with_current(CurrentState::External {
            writer: TxId::with_priority(2, b"boundary"),
        }));
        let mut new_s1 = Node::leaf(Shard::from_entries(entries));
        let membership_writer = TxId::with_priority(2, b"membership");
        new_s1.set_membership_writer(membership_writer.clone());
        new_s1.remove_membership_holder(&membership_writer);
        tctx.shards
            .store_node(&test_collection(), &s1_token, &new_s1, Some(&ver))
            .await
            .unwrap();

        let mut stale = begin_data(&tm, data);
        let err = tm.commit(&mut stale).await.unwrap_err();
        assert!(matches!(err, TransError::Retry), "got {err:?}");
    }

    // Rewrites the test collection's root `_r` (a single leaf holding `a`,`m`)
    // into a two-level tree: an index root over leaf `S0` (a) and `S1` (m),
    // chained by right-sibling. A CAS on `_r` makes this the topology-growth
    // linearization point, mirroring the in-place root split (ADR-031).
    async fn split_root_in_place(tctx: &Tctx) {
        use glassdb_storage::{IndexNode, Node};
        let s0_token = NodeToken::from_bytes([0; 16]);
        let s1_token = NodeToken::from_bytes([1; 16]);

        let loaded = tctx
            .shards
            .load_leaf(&test_root_path(), Requirement::AtLeast(tctx.timeline.now()))
            .await
            .unwrap();
        let entries: Vec<ShardEntry> = loaded.entries().entries().cloned().collect();
        let (lower, upper): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .partition(|e| e.key.as_slice() < b"m".as_slice());

        let s0 = Node::leaf(Shard::from_entries(lower))
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some(s1_token.to_string()));
        tctx.shards
            .store_node(&test_collection(), &s0_token, &s0, None)
            .await
            .unwrap();
        let s1 = Node::leaf(Shard::from_entries(upper));
        tctx.shards
            .store_node(&test_collection(), &s1_token, &s1, None)
            .await
            .unwrap();

        let root = Node::index(IndexNode::from_children([
            (b"".to_vec(), s0_token.to_string()),
            (b"m".to_vec(), s1_token.to_string()),
        ]));
        assert!(
            tctx.shards
                .store_root(&test_collection(), &root, loaded.observation())
                .await
                .unwrap(),
            "root split CAS must win"
        );
    }
}
