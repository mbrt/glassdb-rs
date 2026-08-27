//! The transaction commit protocol with serializable isolation for the v2
//! object-native engine (ADR-016 … ADR-021).
//!
//! A complete point transaction whose dependencies and inline-admissible outputs
//! share one leaf first attempts ADR-061's logless one-CAS commit. Other
//! read-write transactions validate their reads and install locks with one
//! read-modify-write CAS per touched shard, flip their transaction object to
//! committed, then publish `current_writer` pointers and release their locks.
//! A read-only transaction starts on a pure optimistic fast path. If validation
//! fails, retries lock their point reads and scan predicates so sustained churn
//! cannot make them retry forever.
//!
//! Concurrency control (ADR-002 / ADR-020 / ADR-021 / ADR-024): strict two-phase
//! locking with wound-wait and leases for crash recovery. On a conflict it cannot
//! win, a younger-or-equal transaction **waits while holding its locks**
//! (hold-and-wait, ADR-024) instead of aborting; an older one wounds the holder
//! and proceeds. Distinct priorities cannot deadlock (wound-wait keeps the
//! wait-for graph acyclic); two equal-priority transactions that would cycle are
//! broken by escalating to the serial order. Lock acquisition has two modes: the
//! default **parallel** path locks every shard concurrently; after a
//! [`MAX_DEADLOCK_TIMEOUT`] wait or [`SERIAL_FALLBACK_AFTER`] failed attempts,
//! the attempt owner ends the parallel identity and renews it before the
//! **serial** sorted order. First-CAS-wins on the lowest contended shard then
//! guarantees that one contender makes progress.

use std::sync::{Arc, Weak};
use std::time::Duration;

use glassdb_concurr::{Background, Backoff, RetryConfig, rt};
use glassdb_data::{KeyRef, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxLog, TxWrite};
use glassdb_storage::{
    InlinePolicy, LeafObservationCheck, LockType, NodeStore, Requirement, SplitPolicy, Timeline,
    TreeRouter,
};

use crate::access::{AccessSet, ReadAccess, WriteOp};
use crate::collection_commit::{CollectionAttempt, CollectionCommit};
use crate::collections::CollectionData;
use crate::error::TransError;
use crate::gc::TxCleanupHints;
use crate::key_resolver::KeyResolver;
use crate::monitor::{Monitor, OwnerAbortOutcome};
use crate::shard_coord::ShardCoordinator;
use crate::split::SplitHintSink;
use crate::tlocker::{LockOutcome, LockedTx, Locker};

mod attempt;
mod direct_commit;

use attempt::AttemptState;
pub use direct_commit::DirectCommitStats;
use direct_commit::{DirectAttempt, DirectCommit};

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
/// bounds that wait: on elapse the attempt owner ends the identity and renews
/// it for the global sorted order, where one contender always completes. Reuses
/// v1's 5s budget (ADR-002 / architecture.md).
const MAX_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound for one continuous leaf-capacity retry episode. Revisions and
/// reroutes do not extend it: until acquisition succeeds, capacity has not made
/// foreground progress.
const MAX_LEAF_FULL_WAIT: Duration = Duration::from_secs(30);

struct AttemptRetirement {
    mon: Monitor,
    cleanup_hints: TxCleanupHints,
    background: Option<Weak<Background>>,
}

impl AttemptRetirement {
    /// Hands an abandoned transaction identity to managed recovery.
    fn retire_abandoned(&self, tx_id: &TxId) {
        // An optimistic read-only validation and a logless one-CAS commit have
        // no logged identity. Publishing an abort for either would invent a
        // transaction that peers never observed.
        if !self.mon.is_tracked_local(tx_id) {
            return;
        }
        let Some(bg) = self.background.as_ref().and_then(Weak::upgrade) else {
            return;
        };
        let mon = self.mon.clone();
        let cleanup_hints = self.cleanup_hints.clone();
        let tx_id = tx_id.clone();
        bg.spawn_waited(async move {
            if mon.abort_owned_tx(&tx_id).await.is_ok() {
                cleanup_hints.schedule(tx_id);
            }
        });
    }
}

/// Hands the active identity to recovery if its owner future is abandoned.
struct AttemptRetirementGuard {
    retirement: Arc<AttemptRetirement>,
    armed: Option<TxId>,
}

impl AttemptRetirementGuard {
    fn new(retirement: Arc<AttemptRetirement>, tx_id: TxId) -> Self {
        Self {
            retirement,
            armed: Some(tx_id),
        }
    }

    fn arm(&mut self, tx_id: TxId) {
        assert!(
            self.armed.is_none(),
            "cannot replace a live transaction identity"
        );
        self.armed = Some(tx_id);
    }

    fn disarm(&mut self) {
        self.armed = None;
    }

    fn retire_current(&mut self) {
        if let Some(tx_id) = self.armed.take() {
            self.retirement.retire_abandoned(&tx_id);
        }
    }
}

impl Drop for AttemptRetirementGuard {
    fn drop(&mut self) {
        self.retire_current();
    }
}

/// An opaque handle to an in-progress transaction managed by [`Algo`].
pub struct Handle {
    accesses: AccessSet,
    collections: CollectionAttempt,
    state: AttemptState,
    id: TxId,
    /// Per-transaction backoff for the internal CAS-contention retry in
    /// [`Algo::acquire_locks`] (a lost shard/root CAS race): advanced before each
    /// same-id re-lock so churning contenders spread out instead of busy-looping.
    /// Body replay after a wound or stale read and read-only validation do not
    /// use this schedule.
    backoff: Backoff,
    retirement: AttemptRetirementGuard,
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

    fn force_serial_acquisition(&mut self) {
        self.state.force_serial_acquisition();
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

    fn should_acquire_serially(&self) -> bool {
        self.state.should_acquire_serially() || self.renewals() >= SERIAL_FALLBACK_AFTER
    }

    fn renew(&mut self) {
        let id = self.id.renew();
        self.collections.renew();
        self.state.renew();
        self.id = id.clone();
        self.retirement.arm(id);
    }
}

/// Reports whether an engine operation needs another user-body execution.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyOutcome {
    /// The engine operation reached its normal result.
    Complete,
    /// The engine changed attempt state and needs fresh logical accesses.
    ReplayBody,
}

enum AttemptOutcome {
    Complete,
    RenewForSerial {
        replay_body: bool,
        /// A cancelled lock-acquisition write may still land under the old
        /// identity. Its owner operation must remain unresolved so retirement
        /// pins that identity as `Wounded`.
        pending_writes: bool,
    },
}

impl AttemptOutcome {
    fn has_pending_writes(&self) -> bool {
        matches!(
            self,
            Self::RenewForSerial {
                pending_writes: true,
                ..
            }
        )
    }
}

/// Outcome of one lock-acquisition episode. Read-version validation happens
/// after [`Acquired::Locked`], so a stale read is not an acquisition outcome.
enum Acquired {
    /// Every lock is held; proceed to validate reads, then the commit point.
    Locked(LockedTx),
    /// A higher-priority peer aborted this transaction: renew the id and re-run
    /// ([`TransError::Wounded`]).
    Wounded,
    /// Parallel acquisition must end before the replacement identity enters
    /// sorted serial acquisition.
    RenewForSerial {
        /// A cancelled acquisition write may still land after this function
        /// returns. Retirement must keep the old identity pinned.
        pending_writes: bool,
    },
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

/// Reports whether the observed leaf contains an exclusive holder whose final
/// state can change the effective writer without rewriting the leaf.
fn read_observation_has_exclusive_holder(read: &ReadAccess) -> Result<bool, TransError> {
    let raw_key = read.key().key();
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
    shards: NodeStore,
    resolver: KeyResolver,
    locker: Locker,
    direct_commit: DirectCommit,
    mon: Monitor,
    cleanup_hints: TxCleanupHints,
    timeline: Timeline,
    // Factory for each transaction's same-identity acquisition schedule. Other
    // coordination loops own independent schedules from the same engine policy.
    acquisition_retry: RetryConfig,
    split_policy: SplitPolicy,
    collection_commit: CollectionCommit,
    retirement: Arc<AttemptRetirement>,
    // Weak so a captured `Algo` clone inside a spawned retirement task does not
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
        shards: NodeStore,
        timeline: Timeline,
        acquisition_retry: RetryConfig,
        locker: Locker,
        coord: ShardCoordinator,
        mon: Monitor,
        collection_commit: CollectionCommit,
        cleanup_hints: TxCleanupHints,
        background: Option<Weak<Background>>,
        router: TreeRouter,
        resolver: KeyResolver,
        split_policy: SplitPolicy,
        inline_policy: InlinePolicy,
        split_hints: SplitHintSink,
    ) -> Self {
        let direct_commit = DirectCommit::new(
            router,
            coord,
            inline_policy,
            split_hints,
            cleanup_hints.clone(),
        );
        let retirement = Arc::new(AttemptRetirement {
            mon: mon.clone(),
            cleanup_hints: cleanup_hints.clone(),
            background: background.clone(),
        });
        Algo {
            shards,
            resolver,
            locker,
            direct_commit,
            mon,
            cleanup_hints,
            timeline,
            acquisition_retry,
            split_policy,
            collection_commit,
            retirement,
            background,
        }
    }

    /// Returns and resets direct same-leaf commit coverage counters.
    pub fn direct_commit_stats_and_reset(&self) -> DirectCommitStats {
        self.direct_commit.stats_and_reset()
    }

    /// Starts a new transaction with its key and collection-management accesses.
    /// The id's random prefix and timestamp are deterministic under `--cfg sim`.
    pub fn begin(&self, accesses: AccessSet, collection_data: CollectionData) -> Handle {
        let id = TxId::new_at(rt::system_now());
        Handle {
            accesses,
            collections: CollectionAttempt::new(collection_data),
            state: AttemptState::new(),
            retirement: AttemptRetirementGuard::new(self.retirement.clone(), id.clone()),
            id,
            backoff: self.acquisition_retry.backoff(),
        }
    }

    /// Validates all reads and applies all writes.
    pub async fn commit(&self, tx: &mut Handle) -> Result<BodyOutcome, TransError> {
        loop {
            match self.commit_once(tx).await {
                Ok(AttemptOutcome::Complete) => return Ok(BodyOutcome::Complete),
                Ok(AttemptOutcome::RenewForSerial { replay_body, .. }) => {
                    self.renew_for_serial(tx).await?;
                    if replay_body {
                        return Ok(BodyOutcome::ReplayBody);
                    }
                }
                Err(TransError::Wounded) => {
                    self.renew_after_wound(tx).await;
                    return Ok(BodyOutcome::ReplayBody);
                }
                Err(TransError::Retry) => return Ok(BodyOutcome::ReplayBody),
                Err(error) => return Err(error),
            }
        }
    }

    /// Validates the reads and range scans of a read-only transaction.
    pub async fn validate_reads(&self, tx: &mut Handle) -> Result<BodyOutcome, TransError> {
        loop {
            match self.validate_reads_once(tx).await {
                Ok(AttemptOutcome::Complete) => return Ok(BodyOutcome::Complete),
                Ok(AttemptOutcome::RenewForSerial { replay_body, .. }) => {
                    debug_assert!(
                        !replay_body,
                        "read-only validation cannot change collections"
                    );
                    self.renew_for_serial(tx).await?;
                }
                Err(TransError::Wounded) => {
                    self.renew_after_wound(tx).await;
                    return Ok(BodyOutcome::ReplayBody);
                }
                Err(TransError::Retry) => return Ok(BodyOutcome::ReplayBody),
                Err(error) => return Err(error),
            }
        }
    }

    /// Replaces the transaction's access set before commit.
    pub fn reset(&self, tx: &mut Handle, accesses: AccessSet) {
        tx.assert_resettable();
        tx.accesses = accesses;
    }

    /// Replaces both key and collection-management accesses before commit.
    pub fn reset_with_collections(
        &self,
        tx: &mut Handle,
        accesses: AccessSet,
        collection_data: CollectionData,
    ) {
        self.reset(tx, accesses);
        tx.collections.replace_data(collection_data);
    }

    /// Aborts a non-committed, engaged transaction, acknowledging it when safe
    /// and releasing its locks lazily. An optimistic read-only attempt never
    /// engaged, so there is nothing to abort.
    pub async fn end(&self, tx: &mut Handle) -> Result<(), TransError> {
        let result = self.end_inner(tx).await;
        if result.is_ok() {
            tx.retirement.disarm();
        }
        result
    }

    async fn commit_once(&self, tx: &mut Handle) -> Result<AttemptOutcome, TransError> {
        let owner_operation = self.mon.begin_owner_operation(&tx.id)?;
        let result = self.commit_inner(tx).await;
        // Completing the guard proves that no old-identity write can land
        // after retirement. Dropping it records the opposite fact.
        if !result
            .as_ref()
            .is_ok_and(AttemptOutcome::has_pending_writes)
        {
            owner_operation.complete();
        }
        result
    }

    async fn validate_reads_once(&self, tx: &mut Handle) -> Result<AttemptOutcome, TransError> {
        let owner_operation = self.mon.begin_owner_operation(&tx.id)?;
        let result = self.validate_attempt_reads(tx).await;
        // Completing the guard proves that no old-identity write can land
        // after retirement. Dropping it records the opposite fact.
        if !result
            .as_ref()
            .is_ok_and(AttemptOutcome::has_pending_writes)
        {
            owner_operation.complete();
        }
        result
    }

    async fn end_inner(&self, tx: &mut Handle) -> Result<(), TransError> {
        if !tx.needs_abort() {
            return Ok(());
        }
        match self.mon.abort_owned_tx(&tx.id).await? {
            OwnerAbortOutcome::Acknowledged => {
                self.cleanup_hints.schedule(tx.id.clone());
                self.collection_commit.abort(&tx.id, &tx.collections).await
            }
            // A dropped or otherwise unresolved owner operation was pinned as
            // `Wounded`. Its durable manifest and repeated GC passes own
            // cleanup; local rollback must not race an effect that may land
            // after this future returns.
            OwnerAbortOutcome::Pinned => {
                self.cleanup_hints.schedule(tx.id.clone());
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

    async fn renew_for_serial(&self, tx: &mut Handle) -> Result<(), TransError> {
        self.end(tx).await?;
        tx.renew();
        Ok(())
    }

    async fn renew_after_wound(&self, tx: &mut Handle) {
        let retired_id = tx.id.clone();
        if let Err(error) = self.end(tx).await {
            tracing::debug!(
                transaction = %retired_id,
                error = ?error,
                "wounded transaction retirement deferred to managed recovery"
            );
            tx.retirement.retire_current();
        }
        tx.renew();
    }

    async fn commit_inner(&self, tx: &mut Handle) -> Result<AttemptOutcome, TransError> {
        if !tx.accesses.has_writes() && !tx.collections.has_writes() {
            if tx.should_lock_reads() {
                self.validate_coordination_keys(&tx.accesses)?;
                return self.commit_locked(tx).await;
            }
            return self.commit_readonly(tx).await;
        }
        self.validate_coordination_keys(&tx.accesses)?;
        // Try the logless direct path first: a complete point transaction whose
        // dependencies share one leaf and whose outputs fit inline commits in
        // one leaf CAS with no transaction object (ADR-061). It writes nothing
        // unless it commits, so a non-landing attempt is classified rather than
        // failed (ADR-053).
        if tx.collections.data().reads.is_empty() && tx.collections.data().changes.is_empty() {
            match self
                .direct_commit
                .try_commit(&tx.id, &tx.accesses, &mut tx.state)
                .await?
            {
                DirectAttempt::Committed => return Ok(AttemptOutcome::Complete),
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

    async fn validate_attempt_reads(&self, tx: &mut Handle) -> Result<AttemptOutcome, TransError> {
        if tx.accesses.has_writes() || tx.collections.has_writes() {
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
        let requirement = Requirement::AtLeast(validation_start);
        if self
            .validate(&tx.accesses, ValidationContext::Optimistic, requirement)
            .await?
            && self
                .collection_commit
                .validate(None, &tx.collections, requirement)
                .await?
        {
            return Ok(AttemptOutcome::Complete);
        }
        tx.force_locked_reads();
        Err(TransError::Retry)
    }

    /// Rejects keys that can never fit before the transaction has side effects.
    fn validate_coordination_keys(&self, accesses: &AccessSet) -> Result<(), TransError> {
        for key in accesses.points().map(|point| point.key) {
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
    async fn commit_readonly(&self, tx: &mut Handle) -> Result<AttemptOutcome, TransError> {
        // Read-only commit has no mutation receipt; this barrier separates the
        // completed body from the physical observations that certify it.
        let validation_start = self.timeline.now();
        let requirement = Requirement::AtLeast(validation_start);
        if self
            .validate(&tx.accesses, ValidationContext::Optimistic, requirement)
            .await?
            && self
                .collection_commit
                .validate(None, &tx.collections, requirement)
                .await?
        {
            tx.commit();
            return Ok(AttemptOutcome::Complete);
        }
        tx.force_locked_reads();
        Err(TransError::Retry)
    }

    /// Locked path for read-write transactions and escalated read-only retries.
    async fn commit_locked(&self, tx: &mut Handle) -> Result<AttemptOutcome, TransError> {
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
                    return Err(TransError::Wounded);
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
        let requirement = Requirement::AtLeast(validation_start);
        let locked = match self.acquire_locks(tx, requirement).await? {
            Acquired::Locked(l) => l,
            // A higher-priority peer aborted us: renew the id and re-run.
            Acquired::Wounded => return Err(TransError::Wounded),
            Acquired::RenewForSerial { pending_writes } => {
                return Ok(AttemptOutcome::RenewForSerial {
                    replay_body: tx.collections.has_writes(),
                    pending_writes,
                });
            }
        };

        // Record the held lock set so both the committed object (below) and the
        // refresher's pending object describe their own back-references, which
        // is what lets GC prune this transaction's locks by reverse check
        // (ADR-022). This tracks the latest acquire; a body replay that drops
        // keys may under-record, which only defers those stale locks to lazy
        // reclaim, never a correctness loss.
        let mut locks = locked.locked_paths();
        locks.extend(directory_locks.into_durable_locks());
        self.mon.record_tx_locks(&tx.id, locks.clone());

        // Validate point reads and scans after their entry/predicate locks are
        // held. A stale dependency re-runs the body under the same id while the
        // acquired locks prevent another change in the validation-to-commit gap.
        if !self
            .validate(
                &tx.accesses,
                ValidationContext::LocksHeldBy {
                    tx_id: &tx.id,
                    locked: &locked,
                },
                requirement,
            )
            .await?
            || !self
                .collection_commit
                .validate(Some(&tx.id), &tx.collections, requirement)
                .await?
        {
            self.locker.collections().release(&tx.id, &locks).await?;
            return Err(TransError::Retry);
        }

        self.collection_commit
            .fence(&tx.id, &mut tx.collections)
            .await?;

        // Commit point: create-or-flip the transaction object to committed.
        if let Err(e) = self
            .commit_writes(&tx.accesses, &tx.collections, locks.clone(), &tx.id)
            .await
        {
            if matches!(e, TransError::AlreadyFinalized) {
                // An abort-side terminal status won between locking and commit.
                return Err(TransError::Wounded);
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
        Ok(AttemptOutcome::Complete)
    }

    /// Acquires and validates an escalated read-only attempt whose user body
    /// returned an error. The caller will abort through [`Algo::end`] after a
    /// successful validation, so this deliberately does not commit the handle.
    async fn validate_locked_reads(&self, tx: &mut Handle) -> Result<AttemptOutcome, TransError> {
        self.validate_coordination_keys(&tx.accesses)?;
        if tx.engage() {
            self.mon.begin_tx(&tx.id);
        }
        // The escalated read-only path uses lock CASes as validation evidence,
        // so their shared lower bound must precede acquisition.
        let validation_start = self.timeline.now();
        let requirement = Requirement::AtLeast(validation_start);
        let directory_locks = self
            .locker
            .collections()
            .lock(&tx.id, &tx.collections.data().reads, &[])
            .await?;
        let locked = match self.acquire_locks(tx, requirement).await? {
            Acquired::Locked(locked) => locked,
            Acquired::Wounded => return Err(TransError::Wounded),
            Acquired::RenewForSerial { pending_writes } => {
                return Ok(AttemptOutcome::RenewForSerial {
                    replay_body: false,
                    pending_writes,
                });
            }
        };
        let mut locks = locked.locked_paths();
        locks.extend(directory_locks.into_durable_locks());
        self.mon.record_tx_locks(&tx.id, locks.clone());
        if self
            .validate(
                &tx.accesses,
                ValidationContext::LocksHeldBy {
                    tx_id: &tx.id,
                    locked: &locked,
                },
                requirement,
            )
            .await?
            && self
                .collection_commit
                .validate(Some(&tx.id), &tx.collections, requirement)
                .await?
        {
            return Ok(AttemptOutcome::Complete);
        }
        self.locker.collections().release(&tx.id, &locks).await?;
        Err(TransError::Retry)
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
                let cleanup_hints = self.cleanup_hints.clone();
                let id = id.clone();
                // Cancelling a dedup driver may need to spawn a successor for
                // merged callers, so shutdown drains this finite pass.
                bg.spawn_waited(async move {
                    let superseded = locker.keys().write_back(&id, &locked).await;
                    cleanup_hints.schedule_all(superseded);
                });
            }
            None => {
                let superseded = self.locker.keys().write_back(id, &locked).await;
                self.cleanup_hints.schedule_all(superseded);
            }
        }
    }

    /// Acquires every lock the transaction needs while retaining successful
    /// leaf holds across complete-set retries.
    ///
    /// A parallel timeout or sustained completed conflict pass asks the attempt
    /// owner to end this identity and renew it for sorted serial acquisition.
    /// An attempt that already starts in serial mode keeps its sorted prefix and
    /// retries without another renewal.
    async fn acquire_locks(
        &self,
        tx: &mut Handle,
        requirement: Requirement,
    ) -> Result<Acquired, TransError> {
        let serial = tx.should_acquire_serially();
        let mut conflicts: usize = 0;
        let mut leaf_full_since: Option<rt::Instant> = None;
        loop {
            // A higher-priority peer may have aborted us; re-checked each
            // iteration so a wound landing during a long wait surfaces promptly
            // rather than driving a pointless re-lock.
            if self.was_wounded(tx).await {
                return Ok(Acquired::Wounded);
            }
            let outcome = if serial {
                self.locker
                    .keys()
                    .lock_at(&tx.id, &tx.accesses, true, requirement)
                    .await
            } else {
                let key_locker = self.locker.keys();
                tokio::select! {
                    res = key_locker.lock_at(&tx.id, &tx.accesses, false, requirement) => res,
                    _ = rt::sleep(MAX_DEADLOCK_TIMEOUT) => Err(TransError::LockTimeout),
                }
            };
            match outcome {
                Ok(LockOutcome::Locked(l)) => return Ok(Acquired::Locked(l)),
                // A complete failed pass keeps any leaf holds that landed. The
                // next complete pass can recognize or idempotently complete
                // those same-identity holds.
                Ok(LockOutcome::Conflict) => {
                    conflicts += 1;
                    if !serial && conflicts >= SERIAL_FALLBACK_AFTER {
                        tx.force_serial_acquisition();
                        return Ok(Acquired::RenewForSerial {
                            pending_writes: false,
                        });
                    }
                    rt::sleep(tx.backoff.next_delay()).await;
                }
                // Capacity is not lock contention. Retain other leaf holds and
                // wait for the hinted split without serial escalation.
                Ok(LockOutcome::LeafFull) => {
                    let since = *leaf_full_since.get_or_insert_with(rt::Instant::now);
                    if since.elapsed() >= MAX_LEAF_FULL_WAIT {
                        return Err(TransError::other(format!(
                            "leaf capacity remained unavailable for {} seconds",
                            MAX_LEAF_FULL_WAIT.as_secs()
                        )));
                    }
                    rt::sleep(tx.backoff.next_delay()).await;
                }
                // The dropped parallel acquisition can still publish. Leave its
                // owner operation unresolved so end pins the old identity.
                Err(TransError::LockTimeout) => {
                    tx.force_serial_acquisition();
                    return Ok(Acquired::RenewForSerial {
                        pending_writes: true,
                    });
                }
                Err(e) => return Err(e),
            }
        }
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
        accesses: &AccessSet,
        context: ValidationContext<'_>,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let lock_validation = context.lock_validation();
        let own_lock_holder = context.own_lock_holder();
        let physical_reads_valid = self
            .validate_read_observations(accesses, requirement, lock_validation)
            .await?;
        let physical_scans_valid = self
            .validate_scan_observations(accesses, requirement, lock_validation)
            .await?;
        let reads_valid = physical_reads_valid
            || self
                .validate_reads_inner(accesses, own_lock_holder, requirement)
                .await?;
        if !reads_valid {
            return Ok(false);
        }
        let scans_valid = physical_scans_valid
            || self
                .validate_scans_inner(accesses, own_lock_holder, requirement)
                .await?;
        Ok(scans_valid)
    }

    async fn validate_read_observations(
        &self,
        accesses: &AccessSet,
        requirement: Requirement,
        lock_validation: Option<&LockedTx>,
    ) -> Result<bool, TransError> {
        if lock_validation.is_some() {
            return Ok(false);
        }
        let observations = accesses
            .point_reads()
            .iter()
            .map(|read| read.observation().clone())
            .collect::<Vec<_>>();
        let checks = self
            .shards
            .check_leaves_current(&observations, requirement)
            .await;
        for (read, check) in accesses.point_reads().iter().zip(checks) {
            if !matches!(check?, LeafObservationCheck::Current) {
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
        accesses: &AccessSet,
        requirement: Requirement,
        lock_validation: Option<&LockedTx>,
    ) -> Result<bool, TransError> {
        for coverage in accesses
            .range_scans()
            .iter()
            .flat_map(|scan| scan.covered())
        {
            let leaf_unchanged = match lock_validation {
                Some(locked) => locked.validated(&coverage.observation),
                None => matches!(
                    self.shards
                        .check_leaf_current(&coverage.observation, requirement)
                        .await?,
                    LeafObservationCheck::Current
                ),
            };
            if !leaf_unchanged {
                return Ok(false);
            }
            for holder in &coverage.pending_membership {
                if self.mon.tx_status_at(holder, requirement).await? == TxCommitStatus::Ok {
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
        accesses: &AccessSet,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        if accesses.point_reads().is_empty() {
            return Ok(true);
        }
        let keys: Vec<KeyRef> = accesses
            .point_reads()
            .iter()
            .map(|read| read.key().clone())
            .collect();
        let current = self
            .resolver
            .effective_point_states(&keys, own_lock_holder, requirement)
            .await?;
        for (r, state) in accesses.point_reads().iter().zip(current) {
            if !r.validates(state.writer.as_ref(), state.membership_version) {
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
        accesses: &AccessSet,
        own_lock_holder: Option<&TxId>,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        for scan in accesses.range_scans() {
            let current = self
                .resolver
                .scan_coverage(
                    scan.collection(),
                    scan.range(),
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
                    if self.mon.tx_status_at(holder, requirement).await? == TxCommitStatus::Ok {
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
                    scan.collection(),
                    scan.range(),
                    scan.overlay(),
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
        accesses: &AccessSet,
        collections: &CollectionAttempt,
        locks: Vec<TxLock>,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut tl = TxLog::new(id.clone(), TxCommitStatus::Ok);
        for w in accesses.final_writes() {
            let (value, deleted): (Arc<[u8]>, bool) = match w.operation() {
                WriteOp::Put(value) => (value.clone(), false),
                WriteOp::Delete => (Arc::from(&[] as &[u8]), true),
            };
            tl.writes.push(TxWrite {
                key: w.key().clone(),
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

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;
    use crate::access::{ScanRange, WriteAccess};
    use crate::collections::{CollectionChange, CollectionOp};
    use crate::engine::{AssemblyFixture, Engine, EngineConfig, EngineFixture, engine_fixture};
    use crate::key_state_resolver::KeyStateResolver;
    use crate::monitor::{ProtocolTiming, TxRecoveryManifest};
    use crate::reader::Reader;
    use glassdb_backend::middleware::{
        BackendOp, HookBackend, HookFuture, OpLog, OpRecord, RecordingBackend,
    };
    use glassdb_backend::{Backend, StatsBackend, memory::MemoryBackend};
    use glassdb_concurr::RetryConfig;
    use glassdb_data::{
        CollectionAddress, CollectionId, DatabaseId, DbRoot, LeafRef, NodeToken, ObjectPath,
    };
    use glassdb_storage::transaction::{TLogger, TxCommitStatus};
    use glassdb_storage::{
        CachedStore, CollectionRecord, CollectionStore, CurrentState, Node, NodeStore, Shard,
        ShardEntry, StorageError, TreeRouter,
    };
    use tokio::sync::Notify;

    const TEST_DB: &str = "testp";

    pub(super) fn test_collection() -> CollectionAddress {
        CollectionAddress::root(TEST_DB)
    }

    fn test_db_root() -> DbRoot {
        DbRoot::try_from(TEST_DB).unwrap()
    }

    pub(super) fn test_root_path() -> ObjectPath {
        ObjectPath::TreeRoot {
            collection: test_collection(),
        }
    }

    pub(super) fn key_ref(key: &[u8]) -> KeyRef {
        KeyRef::new(test_collection(), key)
    }

    pub(super) struct Tctx {
        pub(super) backend: Arc<dyn Backend>,
        pub(super) tlogger: TLogger,
        pub(super) tmon: Monitor,
        pub(super) records: CollectionStore,
        pub(super) shards: NodeStore,
        pub(super) timeline: Timeline,
        pub(super) locker: Locker,
        _engine: EngineFixture,
    }

    pub(super) async fn new_algo() -> (Algo, Tctx) {
        new_algo_from_backend(Arc::new(MemoryBackend::new())).await
    }

    pub(super) async fn new_algo_from_backend(b: Arc<dyn Backend>) -> (Algo, Tctx) {
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
        new_algo_from_backend_with_cache_policy_and_retirement(b, cache_bytes, split_policy, false)
            .await
    }

    async fn new_algo_from_backend_with_cache_policy_and_retirement(
        b: Arc<dyn Backend>,
        cache_bytes: usize,
        split_policy: SplitPolicy,
        managed_retirement: bool,
    ) -> (Algo, Tctx) {
        let mut config = EngineConfig::default();
        config.set_cache_size(cache_bytes);
        config.set_split_policy(split_policy);
        config.set_protocol_timing(ProtocolTiming::simulation());
        let foundation = AssemblyFixture::new(b.clone(), test_db_root(), &config);

        // Create the collection root so the test collection exists up front.
        foundation
            .records
            .create_record(&test_collection(), &CollectionRecord::new())
            .await
            .unwrap();
        foundation
            .shards
            .create_root(&test_collection(), &Node::leaf(Shard::new()))
            .await
            .unwrap();

        let engine = engine_fixture(&foundation, test_db_root(), config, managed_retirement);
        let algo = engine.algo.clone();
        (
            algo,
            Tctx {
                backend: b,
                tlogger: foundation.tlogger.clone(),
                tmon: foundation.monitor.clone(),
                records: foundation.records.clone(),
                shards: foundation.shards.clone(),
                timeline: foundation.timeline.clone(),
                locker: engine.locker.clone(),
                _engine: engine,
            },
        )
    }

    pub(super) fn wa(key: &KeyRef, val: &[u8]) -> WriteAccess {
        WriteAccess::put(key.clone(), Arc::from(val))
    }

    pub(super) fn wdel(key: &KeyRef) -> WriteAccess {
        WriteAccess::delete(key.clone())
    }

    pub(super) async fn do_read(tctx: &Tctx, key: &KeyRef) -> ReadAccess {
        let outcome = read_outcome(tctx, key).await;
        let (_, _, evidence) = outcome.into_parts();
        ReadAccess::new(key.clone(), evidence)
    }

    pub(super) async fn read_outcome(tctx: &Tctx, key: &KeyRef) -> crate::reader::ReadOutcome {
        let reader = Reader::new(
            KeyResolver::new(
                TreeRouter::new(tctx.shards.clone(), std::num::NonZeroUsize::MIN),
                KeyStateResolver::new(tctx.tmon.clone()),
                std::num::NonZeroUsize::MIN,
            ),
            tctx.timeline.clone(),
            RetryConfig::default(),
        );
        match reader.read(key, Duration::MAX).await {
            Ok(outcome) => outcome,
            Err(e) => panic!("reading {key:?}: {e:?}"),
        }
    }

    pub(super) fn begin_accesses(tm: &Algo, accesses: AccessSet) -> Handle {
        tm.begin(accesses, CollectionData::default())
    }

    pub(super) async fn commit_access(tm: &Algo, accesses: AccessSet) -> Handle {
        let mut h = begin_accesses(tm, accesses);
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
        h
    }

    pub(super) async fn commit_writes(tm: &Algo, ws: Vec<WriteAccess>) -> Handle {
        commit_access(tm, AccessSet::new(Vec::new(), ws, Vec::new())).await
    }

    pub(super) async fn entry(tctx: &Tctx, key: &[u8]) -> Option<ShardEntry> {
        let loaded = tctx
            .shards
            .load_leaf(&test_root_path(), Requirement::AtLeast(tctx.timeline.now()))
            .await
            .unwrap();
        loaded.entries().lookup(key).cloned()
    }

    #[tokio::test(start_paused = true)]
    async fn abandoned_retirement_hands_off_local_locks_when_its_status_read_fails() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let backend = Arc::new(HookBackend::new(inner));
        let fail_status_read = Arc::new(AtomicBool::new(false));
        let failure_reached = Arc::new(Notify::new());
        backend.set_before({
            let fail_status_read = fail_status_read.clone();
            let failure_reached = failure_reached.clone();
            move |op| {
                let fail = matches!(
                    op,
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                ) && fail_status_read.swap(false, Ordering::SeqCst);
                let failure_reached = failure_reached.clone();
                let future: HookFuture = Box::pin(async move {
                    if fail {
                        failure_reached.notify_one();
                        return Err(glassdb_backend::BackendError::other(
                            "injected retirement status-read failure",
                        ));
                    }
                    Ok(())
                });
                future
            }
        });
        let (algo, tctx) = new_algo_from_backend_with_cache_policy_and_retirement(
            backend.clone(),
            1024,
            SplitPolicy::default(),
            true,
        )
        .await;
        let key = key_ref(b"abandoned");
        let data = AccessSet::new(Vec::new(), vec![wa(&key, b"uncommitted")], Vec::new());
        let abandoned_handle = begin_accesses(&algo, data.clone());
        let abandoned = abandoned_handle.id().clone();
        tctx.tmon.begin_tx(&abandoned);
        let outcome = tctx
            .locker
            .keys()
            .lock_at(&abandoned, &data, false, Requirement::Any)
            .await
            .unwrap();
        assert!(matches!(outcome, LockOutcome::Locked(_)));

        fail_status_read.store(true, Ordering::SeqCst);
        drop(abandoned_handle);
        tokio::time::timeout(Duration::from_secs(1), failure_reached.notified())
            .await
            .expect("retirement never attempted its status read");

        let (peer, peer_ctx) = new_algo_from_backend(backend).await;
        peer_ctx.tmon.preempt_tx(&abandoned).await.unwrap();
        let mut peer_handle = begin_accesses(&peer, data);
        tokio::time::timeout(Duration::from_secs(1), peer.commit(&mut peer_handle))
            .await
            .expect("protocol helping did not make progress")
            .unwrap();
        peer.end(&mut peer_handle).await.unwrap();
    }

    #[tokio::test]
    async fn direct_write_new() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        let val = b"v";

        let mut h = begin_accesses(
            &tm,
            AccessSet::new(Vec::new(), vec![wa(&keyp, val)], Vec::new()),
        );
        tm.commit(&mut h).await.unwrap();
        let tid = h.id().clone();
        tm.end(&mut h).await.unwrap();

        let status = tctx
            .tlogger
            .commit_status_at(&tid, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(status.status, TxCommitStatus::Unknown);
        assert!(
            matches!(
                tctx.tlogger.get_at(&tid, Requirement::Any).await,
                Err(StorageError::NotFound)
            ),
            "a direct create has no transaction object"
        );

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
        let logged = vec![b'v'; InlinePolicy::default().max_value_bytes + 1];
        let mut h = begin_accesses(
            &tm,
            AccessSet::new(vec![r], vec![wa(&writep, &logged)], Vec::new()),
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
        let mut handle = tm.begin(AccessSet::default(), earlier_data);
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

        tm.commit_writes(&AccessSet::default(), &handle.collections, Vec::new(), &id)
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
            AccessSet::default(),
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
            AccessSet::default(),
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
        tm.commit_writes(&AccessSet::default(), &handle.collections, Vec::new(), &id)
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
            AccessSet::default(),
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
        let mut h = begin_accesses(
            &tm,
            AccessSet::new(vec![r], vec![wa(&keyp, b"v2")], Vec::new()),
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let r = do_read(&tctx, &keyp).await;
        assert!(r.validates(Some(h.id()), 0));
    }

    // Full path (ADR-024): a read whose value moved before it was locked does not
    // abort-and-renew; it re-runs the body in place (`Retry`) while holding its
    // locks. The engine validates *after* locking, so unlike a pre-lock check the
    // moved key is itself locked during the re-run window — the v1 guarantee that
    // the retry holds all its locks. An over-inline-budget output explicitly
    // forces the full locked path.
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

        let logged = vec![b'x'; InlinePolicy::default().max_value_bytes + 1];
        let mut h = begin_accesses(
            &tm,
            AccessSet::new(vec![ra], vec![wa(&ka, b"v3"), wa(&kb, &logged)], Vec::new()),
        );
        assert_eq!(tm.commit(&mut h).await.unwrap(), BodyOutcome::ReplayBody);

        // The moved key is locked by us when the stale read is signalled: the
        // re-run owns the lock and cannot lose it again to the same race.
        let e = entry(&tctx, b"k").await.expect("entry exists");
        assert_eq!(e.lock_holders(), std::slice::from_ref(h.id()));

        tm.end(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn wound_renews_the_identity_before_body_replay() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");
        let value = vec![b'x'; InlinePolicy::default().max_value_bytes + 1];
        let mut h = begin_accesses(
            &tm,
            AccessSet::new(Vec::new(), vec![wa(&keyp, &value)], Vec::new()),
        );
        let old_id = h.id().clone();
        assert!(h.engage());
        tctx.tmon.begin_tx(&old_id);
        tctx.tmon.preempt_tx(&old_id).await.unwrap();

        assert_eq!(tm.commit(&mut h).await.unwrap(), BodyOutcome::ReplayBody);
        assert_ne!(*h.id(), old_id);
        assert!(!h.id().older(&old_id));
        assert!(!old_id.older(h.id()));

        tm.end(&mut h).await.unwrap();
    }

    // ADR-065: a timed-out parallel acquisition asks its owner to make the old
    // identity durably abort-side before it renews for sorted serial locking.
    #[tokio::test(start_paused = true)]
    async fn deadlock_timeout_renews_before_serial_acquisition() {
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
                &AccessSet::new(Vec::new(), vec![wa(&keyp, b"h")], Vec::new()),
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
        let mut h = begin_accesses(
            &tm,
            AccessSet::new(Vec::new(), vec![wa(&keyp, b"a")], Vec::new()),
        );
        let id_before = h.id().clone();
        let tm2 = tm.clone();
        let committing = tokio::spawn(async move {
            let res = tm2.commit(&mut h).await;
            (h, res)
        });

        // The parallel wait times out. The algorithm retires the old identity
        // and continues under a renewed identity in serial mode.
        rt::sleep(MAX_DEADLOCK_TIMEOUT + Duration::from_secs(1)).await;
        assert!(!committing.is_finished());

        let old_status = tctx
            .tlogger
            .commit_status_at(&id_before, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(old_status.status, TxCommitStatus::Wounded);

        // The replacement commits after the older holder becomes abort-side.
        tctx.tmon.abort_owned_tx(&holder).await.unwrap();
        let (mut h, res) = committing.await.unwrap();
        assert_eq!(
            res.expect("renewed identity commits once the holder releases"),
            BodyOutcome::Complete
        );
        let renewed_id = h.id().clone();
        assert_ne!(renewed_id, id_before);
        assert!(!renewed_id.older(&id_before));
        assert!(!id_before.older(&renewed_id));
        assert_eq!(*h.id(), renewed_id);
        tm.end(&mut h).await.unwrap();
    }

    // A database can contain an unsafe singleton written by an older client or
    // admitted under a former policy. If capacity remains unavailable while the
    // splitter cannot relieve it, lock acquisition must report the bounded wait
    // instead of retrying forever.
    #[tokio::test(start_paused = true)]
    async fn leaf_capacity_retry_episode_is_bounded() {
        let policy = SplitPolicy::builder()
            .node_soft_max_bytes(384)
            .node_max_bytes(512)
            .split_headroom_bytes(128)
            .build()
            .unwrap();
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
                <= policy.node_max_bytes(),
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
        let mut handle = begin_accesses(
            &tm,
            AccessSet::new(Vec::new(), vec![wa(&key, b"value")], Vec::new()),
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

    // ADR-065: sustained completed parallel conflicts renew once before sorted
    // serial acquisition. They do not replay the transaction body.
    //
    // Direct publication is disabled explicitly so this test genuinely
    // exercises the full locked path's renewed serial-fallback behaviour.
    #[tokio::test(start_paused = true)]
    async fn cas_contention_renews_before_serial_acquisition() {
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
        config.set_inline_policy(InlinePolicy::none());
        let status_backend = backend.clone();
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
            AccessSet::new(
                Vec::new(),
                vec![wa(&keyp, b"v1"), wa(&keyp2, b"v1")],
                Vec::new(),
            ),
            CollectionData::default(),
        );
        engine.commit(&mut seed).await.unwrap();
        engine.end(&mut seed).await.unwrap();

        flaky.arm();
        let mut h = engine.begin_transaction(
            AccessSet::new(
                Vec::new(),
                vec![wa(&keyp, b"v2"), wa(&keyp2, b"v2")],
                Vec::new(),
            ),
            CollectionData::default(),
        );
        let id_before = h.id().clone();
        let outcome = engine
            .commit(&mut h)
            .await
            .expect("serial fallback commits after contention clears");
        assert_eq!(outcome, BodyOutcome::Complete);
        assert_ne!(*h.id(), id_before);
        let status_objects = CachedStore::new(status_backend, 1024, Timeline::new(), None);
        let status_logger = TLogger::new(status_objects.clone(), test_db_root());
        let old_status = status_logger
            .commit_status_at(&id_before, Requirement::Any)
            .await
            .unwrap();
        assert_eq!(old_status.status, TxCommitStatus::Aborted);
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
        status_objects.shutdown().await;
    }

    // Builds an algo whose backend records every operation, so tests can prove
    // which commit path ran by counting the CAS writes it issued.
    pub(super) async fn new_recording_algo() -> (Algo, Tctx, OpLog) {
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
    pub(super) struct WriteCounts {
        // Writes to a leaf coordination object (ADR-031): a standalone node
        // `/_n/` or the collection root `/_r`, which holds the small collection's
        // single leaf entries. Entry-lock and write-back CAS both land here and
        // cannot be told apart by path alone.
        pub(super) leaf: usize,
        pub(super) tx: usize,
    }

    pub(super) fn write_counts(log: &OpLog) -> WriteCounts {
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
    pub(super) fn shard_reads(log: &OpLog) -> (usize, usize) {
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
    pub(super) async fn new_recording_algo_big_cache() -> (Algo, Tctx, OpLog) {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let (tm, tctx) = new_algo_from_backend_with_cache(rec, 1 << 20).await;
        (tm, tctx, log)
    }

    #[tokio::test]
    async fn readonly_validates() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let r = do_read(&tctx, &keyp).await;

        let mut h = begin_accesses(&tm, AccessSet::new(vec![r], Vec::new(), Vec::new()));
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
        let data = AccessSet::new(
            vec![ReadAccess::new(keyp.clone(), evidence)],
            Vec::new(),
            Vec::new(),
        );

        let requirement = Requirement::AtLeast(tctx.timeline.now());
        assert!(
            tm.validate(&data, ValidationContext::Optimistic, requirement)
                .await
                .unwrap(),
            "opaque evidence accepts its current value"
        );

        let (peer, _peer_ctx) = new_algo_from_backend(tctx.backend.clone()).await;
        commit_writes(&peer, vec![wa(&keyp, b"v2")]).await;
        let requirement = Requirement::AtLeast(tctx.timeline.now());
        assert!(
            !tm.validate(&data, ValidationContext::Optimistic, requirement)
                .await
                .unwrap(),
            "opaque evidence rejects a superseded value"
        );
    }

    #[tokio::test]
    async fn unmarked_absence_validates_representation_churn_but_not_membership_aba() {
        let (tm, tctx) = new_algo().await;
        let key = key_ref(b"missing");
        let read = do_read(&tctx, &key).await;
        assert!(read.validates(None, 0));
        assert!(!read.validates(None, 1));
        let data = AccessSet::new(vec![read], Vec::new(), Vec::new());

        let requirement = Requirement::AtLeast(tctx.timeline.now());
        let loaded = tctx
            .shards
            .load_leaf(&test_root_path(), Requirement::Any)
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(Shard::from_entries([ShardEntry::new(b"other")
            .with_current(CurrentState::Tombstone {
                writer: TxId::with_priority(1, b"representation"),
            })]));
        assert!(tctx.shards.commit_leaf(edit).await.unwrap());
        assert!(
            tm.validate(&data, ValidationContext::Optimistic, requirement)
                .await
                .unwrap(),
            "representing an unrelated absence does not change membership"
        );

        let requirement = Requirement::AtLeast(tctx.timeline.now());
        let loaded = tctx
            .shards
            .load_leaf(&test_root_path(), Requirement::Any)
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(Shard::new());
        let mut locks = edit.locks().clone();
        locks.advance_membership_version();
        locks.advance_membership_version();
        edit.set_locks(locks);
        assert!(tctx.shards.commit_leaf(edit).await.unwrap());
        assert!(
            !tm.validate(&data, ValidationContext::Optimistic, requirement)
                .await
                .unwrap(),
            "create-delete-reclaim returning to unmarked absence changes its generation"
        );
    }

    #[tokio::test]
    async fn tombstone_read_is_invalidated_when_its_exact_marker_is_reclaimed() {
        let (tm, tctx) = new_algo().await;
        let key = key_ref(b"deleted");
        let writer = TxId::with_priority(1, b"deleter");
        let loaded = tctx
            .shards
            .load_leaf(&test_root_path(), Requirement::Any)
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(Shard::from_entries([ShardEntry::new(b"deleted")
            .with_current(CurrentState::Tombstone {
                writer: writer.clone(),
            })]));
        assert!(tctx.shards.commit_leaf(edit).await.unwrap());

        let read = do_read(&tctx, &key).await;
        assert!(read.validates(Some(&writer), 0));
        assert!(read.validates(Some(&writer), 1));
        let data = AccessSet::new(vec![read], Vec::new(), Vec::new());
        let requirement = Requirement::AtLeast(tctx.timeline.now());

        let external_timeline = Timeline::new();
        let external = NodeStore::new(
            CachedStore::new(
                tctx.backend.clone(),
                1 << 20,
                external_timeline.clone(),
                None,
            ),
            std::num::NonZeroUsize::MIN,
        );
        let loaded = external
            .load_leaf(
                &test_root_path(),
                Requirement::AtLeast(external_timeline.now()),
            )
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(Shard::new());
        assert!(external.commit_leaf(edit).await.unwrap());

        assert!(
            !tm.validate(&data, ValidationContext::Optimistic, requirement)
                .await
                .unwrap()
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
        let holder_data = AccessSet::new(Vec::new(), vec![wa(&keyp, b"v2")], Vec::new());
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
        assert!(read.validates(Some(&previous), 0));
        let data = AccessSet::new(vec![read], Vec::new(), Vec::new());
        let requirement = Requirement::AtLeast(tctx.timeline.now());

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
            !tm.validate_read_observations(&data, requirement, None)
                .await
                .unwrap(),
            "an exclusive holder prevents the leaf-only shortcut"
        );
        assert!(
            !tm.validate(&data, ValidationContext::Optimistic, requirement)
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
        let holder_data = AccessSet::new(Vec::new(), vec![wa(&keyp, b"v2")], Vec::new());
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
        assert!(read.validates(Some(&previous), 0));
        let data = AccessSet::new(vec![read], Vec::new(), Vec::new());
        let requirement = Requirement::AtLeast(tctx.timeline.now());

        // Aborting the holder leaves the previously observed writer effective.
        // The exclusive holder prevents a physical shortcut, then writer
        // resolution at the validation watermark accepts the unchanged value.
        tctx.tmon.abort_owned_tx(&holder).await.unwrap();
        assert!(
            !tm.validate_read_observations(&data, requirement, None)
                .await
                .unwrap()
        );
        assert!(
            tm.validate(&data, ValidationContext::Optimistic, requirement)
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
        let requirement = Requirement::AtLeast(tctx.timeline.now());

        // Another transaction's disjoint lock CAS validates the same pre-CAS
        // leaf after our barrier and therefore advances its shared evidence.
        let other = TxId::with_priority(1, b"other");
        tctx.tmon.begin_tx(&other);
        let other_data = AccessSet::new(Vec::new(), vec![wa(&kb, b"b1")], Vec::new());
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
        let current_data = AccessSet::new(vec![read], Vec::new(), Vec::new());
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
            !tm.validate_read_observations(&current_data, requirement, Some(&current_locked),)
                .await
                .unwrap()
        );

        tctx.locker
            .keys()
            .release_leaf(&current, &test_root_path())
            .await
            .unwrap();
        tctx.locker
            .keys()
            .release_leaf(&other, &test_root_path())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn newer_shared_evidence_runs_logical_validation_without_io() {
        let (tm, tctx, log) = new_recording_algo_big_cache().await;
        let ka = key_ref(b"a");
        let kb = key_ref(b"b");
        commit_writes(&tm, vec![wa(&ka, b"a0"), wa(&kb, b"b0")]).await;

        let read = do_read(&tctx, &ka).await;
        let requirement = Requirement::AtLeast(tctx.timeline.now());

        // A separate client rewrites the shared leaf for B. Its cache is
        // independent, so it cannot advance the retained observation of A in
        // this database.
        let external_timeline = Timeline::new();
        let external = NodeStore::new(
            CachedStore::new(
                tctx.backend.clone(),
                1 << 20,
                external_timeline.clone(),
                None,
            ),
            std::num::NonZeroUsize::MIN,
        );
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
        let other_data = AccessSet::new(Vec::new(), vec![wa(&kb, b"b1")], Vec::new());
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

        let data = AccessSet::new(vec![read], Vec::new(), Vec::new());
        log.lock().unwrap().clear();
        assert!(
            !tm.validate_read_observations(&data, requirement, None)
                .await
                .unwrap(),
            "the retained physical revision changed"
        );
        assert!(
            tm.validate(&data, ValidationContext::Optimistic, requirement)
                .await
                .unwrap(),
            "logical validation accepts the unchanged writer"
        );
        assert_eq!(
            shard_reads(&log),
            (0, 0),
            "post-bound current evidence satisfies both validation steps locally"
        );

        tctx.locker
            .keys()
            .release_leaf(&other, &test_root_path())
            .await
            .unwrap();
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

        let mut h = begin_accesses(&tm, AccessSet::new(vec![ra, rb], Vec::new(), Vec::new()));
        assert_eq!(tm.commit(&mut h).await.unwrap(), BodyOutcome::ReplayBody);
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
        tm.reset(&mut h, AccessSet::new(vec![ra, rb], Vec::new(), Vec::new()));
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
        assert!(r.validates(Some(&deleted_by), 0));
        let mut h = begin_accesses(&tm, AccessSet::new(vec![r], Vec::new(), Vec::new()));
        tm.commit(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn delete_round_trips() {
        let (tm, tctx) = new_algo().await;
        let keyp = key_ref(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let r = do_read(&tctx, &keyp).await;
        let mut h = begin_accesses(&tm, AccessSet::new(vec![r], vec![wdel(&keyp)], Vec::new()));
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        let e = entry(&tctx, b"k").await.unwrap();
        assert!(e.current.is_tombstone());
        let r = do_read(&tctx, &keyp).await;
        assert!(r.validates(Some(h.id()), 0));
    }

    #[tokio::test]
    async fn multi_key_commit() {
        let (tm, tctx) = new_algo().await;
        let k1 = key_ref(b"k1");
        let k2 = key_ref(b"k2");

        let mut h = begin_accesses(
            &tm,
            AccessSet::new(Vec::new(), vec![wa(&k1, b"v1"), wa(&k2, b"v2")], Vec::new()),
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

    // Builds a read-only listing transaction's [`AccessSet`] from a fresh scan of the
    // test collection, returning the scan's live keys alongside so a test can
    // assert on the snapshot and later re-validate the same coverage.
    async fn scan_accesses_for_range(
        tctx: &Tctx,
        range: ScanRange,
        writes: Vec<WriteAccess>,
    ) -> (AccessSet, Vec<Vec<u8>>) {
        let resolver = KeyResolver::new(
            TreeRouter::new(tctx.shards.clone(), std::num::NonZeroUsize::MIN),
            KeyStateResolver::new(tctx.tmon.clone()),
            std::num::NonZeroUsize::MIN,
        );
        let scan = resolver
            .scan_keys(&test_collection(), &range, &[], None, None)
            .await
            .unwrap();
        let keys = scan.keys().to_vec();
        let access = scan.into_access(test_collection(), range, Vec::new());
        let data = AccessSet::new(Vec::new(), writes, vec![access]);
        (data, keys)
    }

    async fn scan_accesses(tctx: &Tctx) -> (AccessSet, Vec<Vec<u8>>) {
        scan_accesses_for_range(tctx, ScanRange::all(), Vec::new()).await
    }

    #[tokio::test]
    async fn opaque_scan_evidence_tracks_validation() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"c"]).await;

        let range = ScanRange::all();
        let resolver = KeyResolver::new(
            TreeRouter::new(tctx.shards.clone(), std::num::NonZeroUsize::MIN),
            KeyStateResolver::new(tctx.tmon.clone()),
            std::num::NonZeroUsize::MIN,
        );
        let result = resolver
            .scan_keys(&test_collection(), &range, &[], None, None)
            .await
            .unwrap();
        let data = AccessSet::new(
            Vec::new(),
            Vec::new(),
            vec![result.into_access(test_collection(), range, Vec::new())],
        );

        let requirement = Requirement::AtLeast(tctx.timeline.now());
        assert!(
            tm.validate(&data, ValidationContext::Optimistic, requirement)
                .await
                .unwrap(),
            "opaque evidence accepts its current membership"
        );

        commit_writes(&tm, vec![wa(&key_ref(b"b"), b"1")]).await;
        let requirement = Requirement::AtLeast(tctx.timeline.now());
        assert!(
            !tm.validate(&data, ValidationContext::Optimistic, requirement)
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

        let (data, keys) = scan_accesses(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);

        // No concurrent change: the listing validates and commits.
        tctx.locker.stats_and_reset();
        let mut h = begin_accesses(&tm, data.clone());
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
        assert_eq!(tctx.locker.stats_and_reset().calls, 0);

        // A create between the scan and (re-)validation bumps the covered leaf.
        commit_writes(&tm, vec![wa(&key_ref(b"b"), b"1")]).await;

        let mut stale = begin_accesses(&tm, data);
        assert_eq!(
            tm.commit(&mut stale).await.unwrap(),
            BodyOutcome::ReplayBody
        );
        assert!(
            stale.should_lock_reads(),
            "scan retry escalates to read locks"
        );

        // The retry computes a fresh page, then commits through the locked path.
        let (fresh, keys) = scan_accesses(&tctx).await;
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
        let holder_data = AccessSet::new(Vec::new(), vec![wa(&key_path, b"value")], Vec::new());
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
        let (scan, keys) = scan_accesses(&tctx).await;
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

        let mut stale = begin_accesses(&tm, scan);
        assert_eq!(
            tm.commit(&mut stale).await.unwrap(),
            BodyOutcome::ReplayBody
        );
    }

    #[tokio::test]
    async fn scan_with_write_records_predicate_locks() {
        let (tm, tctx) = new_algo().await;
        let key_path = key_ref(b"a");
        seed_live_keys(&tctx, &[b"a"]).await;
        let (data, _) =
            scan_accesses_for_range(&tctx, ScanRange::all(), vec![wa(&key_path, b"updated")]).await;

        let mut handle = begin_accesses(&tm, data);
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
        let (stale, keys) =
            scan_accesses_for_range(&tctx, range.clone(), vec![wa(&key_ref(b"a"), b"updated")])
                .await;
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(stale.range_scans()[0].frontier(), Some(b"b".as_slice()));

        // Removing the old frontier means the refreshed two-key page reaches
        // into S1. The first locked validation only owns S0 and must retry.
        commit_writes(&tm, vec![wdel(&key_ref(b"b"))]).await;
        let mut handle = begin_accesses(&tm, stale);
        assert_eq!(
            tm.commit(&mut handle).await.unwrap(),
            BodyOutcome::ReplayBody
        );

        // The body re-runs while S0 stays locked. Its new frontier is `m`, so
        // the next validation adds S1 before committing.
        let (fresh, keys) =
            scan_accesses_for_range(&tctx, range, vec![wa(&key_ref(b"a"), b"updated")]).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"m".to_vec()]);
        assert_eq!(fresh.range_scans()[0].frontier(), Some(b"m".as_slice()));
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

        let (data, keys) = scan_accesses(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

        commit_writes(&tm, vec![wdel(&bp)]).await;

        let mut stale = begin_accesses(&tm, data);
        assert_eq!(
            tm.commit(&mut stale).await.unwrap(),
            BodyOutcome::ReplayBody
        );
    }

    // A pure split changes physical coverage but not logical membership. The
    // fallback re-resolves the page and accepts it without a false retry.
    #[tokio::test]
    async fn scan_accepts_concurrent_split_with_unchanged_membership() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"m"]).await;

        let (data, _keys) = scan_accesses(&tctx).await;

        // Grow the tree in place: rewrite `_r` from its single leaf into an index
        // root pointing at two fresh leaves (the shape the background splitter
        // produces), so the covered leaf set is no longer just `_r`.
        split_root_in_place(&tctx).await;

        let mut stable = begin_accesses(&tm, data);
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

        let (data, keys) = scan_accesses(&tctx).await;
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

        let mut stale = begin_accesses(&tm, data);
        assert_eq!(
            tm.commit(&mut stale).await.unwrap(),
            BodyOutcome::ReplayBody
        );
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
