//! The transaction commit protocol with serializable isolation for the v2
//! object-native engine (ADR-016 … ADR-021).
//!
//! A complete point transaction whose dependencies and inline-admissible outputs
//! share one leaf first tries ADR-061's direct commit. Other read-write
//! transactions validate their reads and install locks with one read-modify-write
//! CAS per touched leaf, flip their transaction record to committed, then publish
//! current states and release their locks. A read-only transaction
//! starts with optimistic validation. If validation fails, later body replays
//! use locked validation on their point reads and scan predicates so sustained
//! churn cannot make them replay the body forever.
//!
//! Concurrency control (ADR-002 / ADR-020 / ADR-021 / ADR-024): strict two-phase
//! locking with wound-wait and leases for crash recovery. On a conflict it cannot
//! win, a younger-or-equal transaction **waits while holding its locks**
//! (hold-and-wait, ADR-024) instead of aborting; an older one wounds the holder
//! and proceeds. Distinct priorities cannot deadlock (wound-wait keeps the
//! wait-for graph acyclic); two equal-priority transactions that would cycle are
//! broken by escalating to serial acquisition. Lock acquisition has two modes:
//! the default **parallel acquisition** locks every leaf concurrently; after a
//! [`MAX_DEADLOCK_TIMEOUT`] wait, after [`SERIAL_FALLBACK_AFTER`] identity
//! renewals, or after [`SERIAL_FALLBACK_AFTER`] parallel acquisition passes that
//! end in a lock conflict, the commit pass ends and the parallel identity is
//! renewed for **serial acquisition** in sorted order. First-CAS-wins on the
//! lowest contended leaf then guarantees that one contender makes progress.

use std::sync::{Arc, Weak};
use std::time::Duration;

use glassdb_concurr::{Background, Backoff, RetryConfig, rt};
use glassdb_data::{LogicalKey, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxRecord, TxWrite};
use glassdb_storage::{
    CurrentnessBarrier, InlinePolicy, LeafObservationCheck, LockType, NodeSizePolicy, NodeStore,
    Requirement, StorageError, Timeline, TreeRouter,
};

use crate::access::{AccessSet, LeafCoverage, ReadAccess, WriteOp};
use crate::collection_commit::{CollectionCommit, CollectionHandleState, CollectionReservations};
use crate::collections::CatalogAccesses;
use crate::error::TransError;
use crate::gc::GcHints;
use crate::key_resolver::KeyResolver;
use crate::leaf_coord::LeafCoordinator;
use crate::monitor::{Monitor, OwnerAbortOutcome};
use crate::structural::StructuralHintSink;
use crate::tlocker::{LockOutcome, LockedTx, Locker};

mod direct_commit;
mod handle_state;

pub use direct_commit::DirectCommitStats;
use direct_commit::{DirectCommit, DirectOutcome};
use handle_state::HandleState;

/// Threshold for escalating to serial acquisition (ADR-020).
/// [`Handle::should_acquire_serially`] treats this many [`HandleState::renewals`]
/// as a serial trigger, and parallel acquisition uses the same count for
/// acquisition passes that end in a lock conflict. Parallel acquisition is fast
/// but can *livelock* two equal-priority transactions that each grab a
/// different leaf first; serial acquisition, where first-CAS-wins on the lowest
/// contended leaf, guarantees one of them makes progress.
const SERIAL_FALLBACK_AFTER: usize = 3;

/// Upper bound on how long a transaction blocks acquiring its locks with the
/// default parallel acquisition before suspecting a deadlock and escalating to
/// the serial-acquisition fallback (ADR-024). Under hold-and-wait a
/// younger-or-equal transaction *waits* for a conflicting holder while keeping
/// its locks; distinct priorities cannot cycle (wound-wait), but two
/// equal-priority transactions can each wait on the other forever. This timeout
/// bounds that wait: on elapse the commit pass ends and the identity is renewed
/// for the global sorted order, where one contender always completes. Reuses
/// v1's 5s budget (ADR-002 / architecture.md).
const MAX_DEADLOCK_TIMEOUT: Duration = Duration::from_secs(5);

/// Upper bound for one continuous leaf-capacity retry episode. Revisions and
/// reroutes do not extend it: until acquisition succeeds, capacity has not made
/// foreground progress.
const MAX_LEAF_FULL_WAIT: Duration = Duration::from_secs(30);

struct IdentityRetirement {
    mon: Monitor,
    gc_hints: GcHints,
    background: Option<Weak<Background>>,
}

impl IdentityRetirement {
    /// Hands an unfinished transaction identity to managed recovery.
    fn retire_unfinished(&self, tx_id: &TxId) {
        // Optimistic validation and a direct one-CAS commit have no locked-commit
        // identity. Publishing an abort for either would invent a transaction
        // that peers never observed.
        if !self.mon.is_tracked_local(tx_id) {
            return;
        }
        let Some(bg) = self.background.as_ref().and_then(Weak::upgrade) else {
            return;
        };
        let mon = self.mon.clone();
        let gc_hints = self.gc_hints.clone();
        let tx_id = *tx_id;
        bg.spawn_waited(async move {
            if mon.abort_owned_tx(&tx_id).await.is_ok() {
                gc_hints.schedule(tx_id);
            }
        });
    }
}

/// Hands the active identity to recovery if its owner future does not finish.
struct IdentityRetirementGuard {
    retirement: Arc<IdentityRetirement>,
    armed: Option<TxId>,
}

impl IdentityRetirementGuard {
    fn new(retirement: Arc<IdentityRetirement>, tx_id: TxId) -> Self {
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
            self.retirement.retire_unfinished(&tx_id);
        }
    }
}

impl Drop for IdentityRetirementGuard {
    fn drop(&mut self) {
        self.retire_current();
    }
}

/// An opaque handle to an in-progress transaction managed by [`Algo`].
pub struct Handle {
    accesses: AccessSet,
    collections: CollectionHandleState,
    state: HandleState,
    id: TxId,
    /// Per-transaction backoff for the internal CAS-contention retry in
    /// [`Algo::acquire_locks`] (a lost leaf/root CAS race): advanced before each
    /// same-identity re-lock so churning contenders spread out instead of busy-looping.
    /// Body replay after a wound or invalidated read and read-only validation do not
    /// use this schedule.
    backoff: Backoff,
    retirement: IdentityRetirementGuard,
}

impl Handle {
    /// The transaction's ID.
    pub fn id(&self) -> &TxId {
        &self.id
    }

    /// Returns collection-ID reservations owned by the current identity.
    pub(crate) fn collection_reservations(&self) -> CollectionReservations {
        self.collections.reservations()
    }

    /// Whether this read-only transaction is past optimistic validation and must
    /// use locked validation.
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
        self.id = id;
        self.retirement.arm(id);
    }
}

/// Reports whether GlassDB can return a body outcome or must execute the body again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BodyDecision {
    /// The body outcome can be returned.
    ReturnOutcome,
    /// Handle state changed and the transaction needs fresh logical accesses.
    ReplayBody,
}

/// The non-error end of one commit pass.
enum PassOutcome {
    Complete,
    RenewForSerial {
        replay_body: bool,
        /// A cancelled lock-acquisition write may still land under the old
        /// identity. Its owner operation must remain unresolved so retirement
        /// pins that identity as `Wounded`.
        pending_writes: bool,
    },
}

impl PassOutcome {
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

/// Outcome of one lock-acquisition episode. Read validation happens after
/// [`Acquired::Locked`], so an invalidated read is not an acquisition outcome.
enum Acquired {
    /// Every lock is held; proceed to validate reads, then the commit point.
    Locked(LockedTx),
    /// A higher-priority peer aborted this transaction: renew the id and replay
    /// the body ([`TransError::Wounded`]).
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
    nodes: NodeStore,
    resolver: KeyResolver,
    locker: Locker,
    direct_commit: DirectCommit,
    mon: Monitor,
    gc_hints: GcHints,
    timeline: Timeline,
    // Factory for each transaction's same-identity acquisition schedule. Other
    // coordination loops own independent schedules from the same engine policy.
    acquisition_retry: RetryConfig,
    node_size_policy: NodeSizePolicy,
    collection_reservation_limit: usize,
    collection_commit: CollectionCommit,
    retirement: Arc<IdentityRetirement>,
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
        nodes: NodeStore,
        timeline: Timeline,
        acquisition_retry: RetryConfig,
        locker: Locker,
        coord: LeafCoordinator,
        mon: Monitor,
        collection_commit: CollectionCommit,
        gc_hints: GcHints,
        background: Option<Weak<Background>>,
        router: TreeRouter,
        resolver: KeyResolver,
        node_size_policy: NodeSizePolicy,
        collection_reservation_limit: usize,
        inline_policy: InlinePolicy,
        structural_hints: StructuralHintSink,
    ) -> Self {
        let direct_commit = DirectCommit::new(
            router,
            coord,
            inline_policy,
            structural_hints,
            gc_hints.clone(),
        );
        let retirement = Arc::new(IdentityRetirement {
            mon: mon.clone(),
            gc_hints: gc_hints.clone(),
            background: background.clone(),
        });
        Algo {
            nodes,
            resolver,
            locker,
            direct_commit,
            mon,
            gc_hints,
            timeline,
            acquisition_retry,
            node_size_policy,
            collection_reservation_limit,
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
    pub fn begin(&self, accesses: AccessSet, catalog_accesses: CatalogAccesses) -> Handle {
        let id = TxId::new_at(rt::system_now());
        Handle {
            accesses,
            collections: CollectionHandleState::new(
                catalog_accesses,
                self.collection_reservation_limit,
            ),
            state: HandleState::new(),
            retirement: IdentityRetirementGuard::new(self.retirement.clone(), id),
            id,
            backoff: self.acquisition_retry.backoff(),
        }
    }

    /// Validates all reads and applies all writes.
    pub async fn commit(&self, tx: &mut Handle) -> Result<BodyDecision, TransError> {
        loop {
            match self.commit_pass(tx).await {
                Ok(PassOutcome::Complete) => return Ok(BodyDecision::ReturnOutcome),
                Ok(PassOutcome::RenewForSerial { replay_body, .. }) => {
                    self.renew_for_serial(tx).await?;
                    if replay_body {
                        return Ok(BodyDecision::ReplayBody);
                    }
                }
                Err(TransError::Wounded) => {
                    self.renew_after_wound(tx).await;
                    return Ok(BodyDecision::ReplayBody);
                }
                Err(TransError::Retry) => {
                    return Ok(BodyDecision::ReplayBody);
                }
                Err(error) => return self.replay_changed_catalog(tx, error).await,
            }
        }
    }

    /// Validates the reads and range scans of a read-only transaction.
    pub async fn validate_reads(&self, tx: &mut Handle) -> Result<BodyDecision, TransError> {
        loop {
            match self.validation_pass(tx).await {
                Ok(PassOutcome::Complete) => return Ok(BodyDecision::ReturnOutcome),
                Ok(PassOutcome::RenewForSerial { replay_body, .. }) => {
                    debug_assert!(
                        !replay_body,
                        "read-only validation cannot change collections"
                    );
                    self.renew_for_serial(tx).await?;
                }
                Err(TransError::Wounded) => {
                    self.renew_after_wound(tx).await;
                    return Ok(BodyDecision::ReplayBody);
                }
                Err(TransError::Retry) => {
                    return Ok(BodyDecision::ReplayBody);
                }
                Err(error) => return self.replay_changed_catalog(tx, error).await,
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
        catalog_accesses: CatalogAccesses,
    ) {
        self.reset(tx, accesses);
        tx.collections.replace_accesses(catalog_accesses);
    }

    /// Aborts a non-committed, engaged transaction, acknowledging it when safe
    /// and releasing its locks lazily. A read-only transaction that never left
    /// optimistic validation never engaged, so there is nothing to abort.
    pub async fn end(&self, tx: &mut Handle) -> Result<(), TransError> {
        let result = self.end_inner(tx).await;
        if result.is_ok() {
            tx.retirement.disarm();
        }
        result
    }

    /// Runs one commit pass as one owner operation of the current identity.
    async fn commit_pass(&self, tx: &mut Handle) -> Result<PassOutcome, TransError> {
        let owner_operation = self.mon.begin_owner_operation(&tx.id)?;
        let result = self.commit_inner(tx).await;
        let result = self.resolve_reclaimed_resources(tx, result).await;
        // Completing the guard proves that no old-identity write can land
        // after retirement. Dropping it records the opposite fact.
        if !result.as_ref().is_ok_and(PassOutcome::has_pending_writes) {
            owner_operation.complete();
        }
        result
    }

    /// Runs one read-only commit pass as one owner operation of the current
    /// identity.
    async fn validation_pass(&self, tx: &mut Handle) -> Result<PassOutcome, TransError> {
        let owner_operation = self.mon.begin_owner_operation(&tx.id)?;
        let result = self.validate_handle_reads(tx).await;
        let result = self.resolve_reclaimed_resources(tx, result).await;
        // Completing the guard proves that no old-identity write can land
        // after retirement. Dropping it records the opposite fact.
        if !result.as_ref().is_ok_and(PassOutcome::has_pending_writes) {
            owner_operation.complete();
        }
        result
    }

    /// Replays the body when a stale-collection error follows changed directory observations.
    async fn replay_changed_catalog(
        &self,
        tx: &mut Handle,
        error: TransError,
    ) -> Result<BodyDecision, TransError> {
        if !matches!(
            error,
            TransError::StaleCollection | TransError::Storage(StorageError::StaleCollection)
        ) {
            return Err(error);
        }
        // Acquisition can stop before it records all held locks. Retire the
        // identity before the recheck, so a foreign directory holder cannot
        // wait for this identity while the recheck waits for that holder.
        self.end(tx).await?;
        let barrier = self.timeline.currentness_barrier();
        if self
            .collection_commit
            .validate_reads(&tx.collections, barrier)
            .await?
        {
            return Err(error);
        }
        tx.renew();
        Ok(BodyDecision::ReplayBody)
    }

    /// Resolves commit failures caused by a wounded owner's reclaimed resources.
    async fn resolve_reclaimed_resources(
        &self,
        tx: &Handle,
        result: Result<PassOutcome, TransError>,
    ) -> Result<PassOutcome, TransError> {
        if tx.needs_abort()
            && matches!(
                result,
                Err(TransError::Storage(StorageError::NotFound | StorageError::StaleCollection)
                    | TransError::StaleCollection)
            )
            // Local Pending status cannot rule out a peer's wound. Keep the
            // owner operation active until this durable read also completes.
            && self.mon.has_durable_wound(&tx.id).await?
        {
            return Err(TransError::Wounded);
        }
        result
    }

    async fn end_inner(&self, tx: &mut Handle) -> Result<(), TransError> {
        if !tx.needs_abort() {
            return Ok(());
        }
        match self.mon.abort_owned_tx(&tx.id).await? {
            OwnerAbortOutcome::Acknowledged => {
                self.gc_hints.schedule(tx.id);
                self.collection_commit.abort(&tx.id, &tx.collections).await
            }
            // A dropped or otherwise unresolved owner operation was pinned as
            // `Wounded`. Its durable manifest and repeated GC passes own
            // cleanup; local rollback must not race an effect that may land
            // after this future returns.
            OwnerAbortOutcome::Pinned => {
                self.gc_hints.schedule(tx.id);
                Ok(())
            }
            // The commit point won before cleanup observed its result. Its
            // collection objects and drop intents now belong to the committed
            // record and must be left for write-back/recovery.
            OwnerAbortOutcome::Committed => {
                tx.commit();
                Ok(())
            }
            // The commit write was dispatched but its result is no longer
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
        let retired_id = tx.id;
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

    async fn commit_inner(&self, tx: &mut Handle) -> Result<PassOutcome, TransError> {
        if !tx.accesses.has_writes() && !tx.collections.has_writes() {
            if tx.should_lock_reads() {
                self.validate_coordination_keys(&tx.accesses)?;
                return self.commit_locked(tx).await;
            }
            return self.commit_readonly(tx).await;
        }
        self.validate_coordination_keys(&tx.accesses)?;
        // Try direct commit first: a complete point transaction whose
        // dependencies share one leaf and whose outputs fit inline commits in
        // one leaf CAS with no transaction record (ADR-061). It writes nothing
        // unless it commits, so a non-landing direct commit is classified rather
        // than failed (ADR-053).
        if tx.collections.accesses().reads.is_empty()
            && tx.collections.accesses().changes.is_empty()
        {
            match self
                .direct_commit
                .try_commit(&tx.id, &tx.accesses, &mut tx.state)
                .await?
            {
                DirectOutcome::Committed => return Ok(PassOutcome::Complete),
                // A certified direct-commit loss reevaluates the body rather than
                // publishing a holder that would make every subsequent direct
                // commit on the key ineligible (ADR-053). The id is unengaged —
                // no record, no lock, no published identity — so the ordinary
                // retry contract applies with no cleanup.
                DirectOutcome::Replay => return Err(TransError::Retry),
                DirectOutcome::Locked => {}
            }
        }
        self.commit_locked(tx).await
    }

    async fn validate_handle_reads(&self, tx: &mut Handle) -> Result<PassOutcome, TransError> {
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
        let barrier = self.timeline.currentness_barrier();
        if self
            .validate(&tx.accesses, ValidationContext::Optimistic, barrier)
            .await?
            && self
                .collection_commit
                .validate(None, &tx.collections, barrier)
                .await?
        {
            return Ok(PassOutcome::Complete);
        }
        tx.force_locked_reads();
        Err(TransError::Retry)
    }

    /// Rejects keys that can never fit before the transaction has side effects.
    fn validate_coordination_keys(&self, accesses: &AccessSet) -> Result<(), TransError> {
        for key in accesses.points().map(|point| point.key) {
            if !self.node_size_policy.key_fits(key.key()) {
                return Err(TransError::InvalidInput(
                    "key exceeds the coordination node size limit".into(),
                ));
            }
        }
        Ok(())
    }

    /// Optimistic validation for read-only commit: re-resolve each read's
    /// effective writer against the nodes and commit if none changed. The first
    /// body execution takes no locks; a failed validation makes the next replay
    /// use locked validation.
    ///
    /// A failed validation does not back off before signalling [`Retry`]: the
    /// replay re-reads the authoritative values (the cache was just invalidated)
    /// rather than busy-spinning on the stale ones, and an idle delay would only
    /// add commit latency.
    ///
    /// [`Retry`]: TransError::Retry
    async fn commit_readonly(&self, tx: &mut Handle) -> Result<PassOutcome, TransError> {
        // Read-only commit has no CAS receipt; this barrier separates the
        // completed body from the physical observations that certify it.
        let barrier = self.timeline.currentness_barrier();
        if self
            .validate(&tx.accesses, ValidationContext::Optimistic, barrier)
            .await?
            && self
                .collection_commit
                .validate(None, &tx.collections, barrier)
                .await?
        {
            tx.commit();
            return Ok(PassOutcome::Complete);
        }
        tx.force_locked_reads();
        Err(TransError::Retry)
    }

    /// Locked commit for read-write transactions and read-only transactions that
    /// escalated to locked validation.
    async fn commit_locked(&self, tx: &mut Handle) -> Result<PassOutcome, TransError> {
        let is_new = tx.engage();

        self.collection_commit
            .reconcile_replay(&tx.id, &mut tx.collections)
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
                &tx.collections.accesses().reads,
                &tx.collections.accesses().changes,
            )
            .await?;

        // Capture before lock acquisition so every successful lock CAS is
        // eligible to certify the reads it protects against this same bound.
        let barrier = self.timeline.currentness_barrier();
        let requirement = Requirement::after(barrier);
        let locked = match self.acquire_locks(tx, requirement).await? {
            Acquired::Locked(l) => l,
            // A higher-priority peer aborted us: renew the id and replay the body.
            Acquired::Wounded => return Err(TransError::Wounded),
            Acquired::RenewForSerial { pending_writes } => {
                return Ok(PassOutcome::RenewForSerial {
                    replay_body: tx.collections.has_writes(),
                    pending_writes,
                });
            }
        };

        // Record the held lock set so both the committed record (below) and the
        // refresher's pending record describe their own recovery manifest, which
        // is what lets GC prune this transaction's locks by a GC check
        // (ADR-022). This tracks the latest acquire; a body replay that drops
        // keys may under-record, which only defers those stale locks to lazy
        // reclaim, never a correctness loss.
        let mut locks = locked.locked_paths();
        locks.extend(directory_locks.into_durable_locks());
        self.mon.record_tx_locks(&tx.id, locks.clone());

        // Validate point reads and scans after their entry/predicate locks are
        // held. An invalidated read replays the body under the same id while the
        // acquired locks prevent another change in the validation-to-commit gap.
        if !self
            .validate(
                &tx.accesses,
                ValidationContext::LocksHeldBy {
                    tx_id: &tx.id,
                    locked: &locked,
                },
                barrier,
            )
            .await?
            || !self
                .collection_commit
                .validate(Some(&tx.id), &tx.collections, barrier)
                .await?
        {
            self.locker
                .collections()
                .release(&tx.id, &locks, Requirement::ANY)
                .await?;
            return Err(TransError::Retry);
        }

        self.collection_commit
            .fence(&tx.id, &mut tx.collections)
            .await?;

        // Commit point: create-or-flip the transaction record to committed.
        if let Err(e) = self
            .commit_writes(&tx.accesses, &tx.collections, locks.clone(), &tx.id)
            .await
        {
            if matches!(e, TransError::AlreadyFinalized) {
                // An abort-side final status won between locking and commit.
                return Err(TransError::Wounded);
            }
            return Err(e.context(format!("committing writes for tx {}", tx.id)));
        }
        tx.commit();

        if let Err(error) = self
            .locker
            .collections()
            .write_back(
                &tx.id,
                &tx.collections.accesses().changes,
                &locks,
                Requirement::ANY,
            )
            .await
        {
            tracing::debug!(%error, "collection-directory write-back deferred");
        }
        self.write_back(&tx.id, locked).await;
        self.collection_commit
            .finish_committed(&tx.collections)
            .await;
        Ok(PassOutcome::Complete)
    }

    /// Acquires and validates a read-only transaction whose body returned an
    /// error after escalating to locked validation. The caller will abort
    /// through [`Algo::end`] after a successful validation, so this deliberately
    /// does not commit the handle.
    async fn validate_locked_reads(&self, tx: &mut Handle) -> Result<PassOutcome, TransError> {
        self.validate_coordination_keys(&tx.accesses)?;
        if tx.engage() {
            self.mon.begin_tx(&tx.id);
        }
        // The escalated read-only path uses lock CASes as validation evidence,
        // so their shared lower bound must precede acquisition.
        let barrier = self.timeline.currentness_barrier();
        let requirement = Requirement::after(barrier);
        let directory_locks = self
            .locker
            .collections()
            .lock(&tx.id, &tx.collections.accesses().reads, &[])
            .await?;
        let locked = match self.acquire_locks(tx, requirement).await? {
            Acquired::Locked(locked) => locked,
            Acquired::Wounded => return Err(TransError::Wounded),
            Acquired::RenewForSerial { pending_writes } => {
                return Ok(PassOutcome::RenewForSerial {
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
                barrier,
            )
            .await?
            && self
                .collection_commit
                .validate(Some(&tx.id), &tx.collections, barrier)
                .await?
        {
            return Ok(PassOutcome::Complete);
        }
        self.locker
            .collections()
            .release(&tx.id, &locks, Requirement::ANY)
            .await?;
        Err(TransError::Retry)
    }

    /// Publishes the committed transaction's current states and releases its locks.
    /// Idempotent and best-effort: the transaction is already durably committed,
    /// so a write-back failure only delays lazy lock release, never the result.
    /// It is spawned in the background so commit returns immediately rather than
    /// waiting for the current-state publishes and lock releases; a shutdown drains
    /// the spawned task (`Background::spawn_waited`). A live holder defers that
    /// write-back rather than making the task wait. Without a background executor
    /// (unit tests, or after shutdown dropped it) it releases inline so locks are
    /// not left to lazy reclaim.
    async fn write_back(&self, id: &TxId, locked: LockedTx) {
        match self.background.as_ref().and_then(|w| w.upgrade()) {
            Some(bg) => {
                let locker = self.locker.clone();
                let gc_hints = self.gc_hints.clone();
                let id = *id;
                // Cancelling a dedup driver may need to spawn a successor for
                // merged callers, so shutdown drains this finite pass.
                bg.spawn_waited(async move {
                    let superseded = locker.keys().write_back(&id, &locked).await;
                    gc_hints.schedule_all(superseded);
                });
            }
            None => {
                let superseded = self.locker.keys().write_back(id, &locked).await;
                self.gc_hints.schedule_all(superseded);
            }
        }
    }

    /// Acquires every lock the transaction needs while retaining successful
    /// leaf holds across complete-set retries.
    ///
    /// A parallel timeout or repeated conflicting acquisition passes end the
    /// commit pass and ask for an identity renewal for serial acquisition. An
    /// identity that already starts with serial acquisition keeps its sorted
    /// prefix and retries lock acquisition without another renewal.
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
                // A failed acquisition pass keeps any leaf holds that landed.
                // The next acquisition pass can recognize or idempotently
                // complete those same-identity holds.
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
    /// transaction's own successful lock CAS, and only when that CAS left the
    /// leaf with the membership the scan observed. When the leaf has moved, logical
    /// validation compares the observed writer or membership against current
    /// state satisfying the same pre-lock bound; evidence advanced by another
    /// operation can therefore avoid I/O without deciding logical validity.
    async fn validate(
        &self,
        accesses: &AccessSet,
        context: ValidationContext<'_>,
        barrier: CurrentnessBarrier,
    ) -> Result<bool, TransError> {
        let lock_validation = context.lock_validation();
        let own_lock_holder = context.own_lock_holder();
        let physical_reads_valid = self
            .validate_read_observations(accesses, barrier, lock_validation)
            .await?;
        let physical_scans_valid = self
            .validate_scan_observations(accesses, barrier, lock_validation)
            .await?;
        let reads_valid = physical_reads_valid
            || self
                .validate_reads_inner(accesses, own_lock_holder, barrier)
                .await?;
        if !reads_valid {
            return Ok(false);
        }
        let scans_valid = physical_scans_valid
            || self
                .validate_scans_inner(accesses, own_lock_holder, barrier)
                .await?;
        Ok(scans_valid)
    }

    async fn validate_read_observations(
        &self,
        accesses: &AccessSet,
        barrier: CurrentnessBarrier,
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
            .nodes
            .check_leaves_current(&observations, Requirement::after(barrier))
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

    /// Reports whether every covered leaf is still in the exact state the scan
    /// observed, which needs no re-resolution of the page.
    ///
    /// This is stronger than ADR-032's membership-generation condition, so it also
    /// covers what that generation cannot: a split does not bump it, but a split
    /// rewrites the leaf it shrinks, and new leaves enter a range only through
    /// such a rewrite. Exact per-leaf state equality therefore leaves the
    /// covered leaf set unchanged, and the set comparison stays in the logical
    /// fallback.
    async fn validate_scan_observations(
        &self,
        accesses: &AccessSet,
        barrier: CurrentnessBarrier,
        lock_validation: Option<&LockedTx>,
    ) -> Result<bool, TransError> {
        for coverage in accesses
            .range_scans()
            .iter()
            .flat_map(|scan| scan.covered())
        {
            let unchanged = match lock_validation {
                Some(locked) => locked.certifies_membership(
                    &coverage.observation,
                    coverage.membership_generation,
                    barrier,
                ),
                None => matches!(
                    self.nodes
                        .check_leaf_current(&coverage.observation, Requirement::after(barrier))
                        .await?,
                    LeafObservationCheck::Current
                ),
            };
            if !unchanged || self.any_committed([coverage], barrier).await? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Re-resolves every read's effective writer and reports whether they all
    /// still match what the transaction observed (a consistent snapshot exists).
    /// The read set is resolved in one leaf-batched pass (each touched leaf is
    /// loaded once) rather than one leaf load per key.
    async fn validate_reads_inner(
        &self,
        accesses: &AccessSet,
        own_lock_holder: Option<&TxId>,
        barrier: CurrentnessBarrier,
    ) -> Result<bool, TransError> {
        if accesses.point_reads().is_empty() {
            return Ok(true);
        }
        let keys: Vec<LogicalKey> = accesses
            .point_reads()
            .iter()
            .map(|read| read.key().clone())
            .collect();
        let current = self
            .resolver
            .effective_point_states(&keys, own_lock_holder, Requirement::after(barrier))
            .await?;
        for (r, state) in accesses.point_reads().iter().zip(current) {
            if !r.validates(state.writer.as_ref(), state.membership_generation) {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Re-scans every range the transaction listed and reports whether each
    /// still covers the same leaves at the same membership generations (ADR-032).
    /// Pending membership writers observed by the original scan are rechecked
    /// because their commit transition does not itself bump the membership
    /// generation.
    /// If physical coverage changed, status-aware resolution distinguishes a
    /// harmless split from a logical page change.
    async fn validate_scans_inner(
        &self,
        accesses: &AccessSet,
        own_lock_holder: Option<&TxId>,
        barrier: CurrentnessBarrier,
    ) -> Result<bool, TransError> {
        for scan in accesses.range_scans() {
            let current = self
                .resolver
                .scan_coverage(
                    scan.collection(),
                    scan.range(),
                    scan.frontier(),
                    own_lock_holder,
                    Requirement::after(barrier),
                )
                .await?;
            let unchanged = current.len() == scan.covered().len()
                && !current.iter().zip(scan.covered()).any(|(now, observed)| {
                    now.path != observed.path
                        || now.membership_generation != observed.membership_generation
                });
            if unchanged && !self.any_committed(scan.covered(), barrier).await? {
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
                    Requirement::after(barrier),
                )
                .await?;
            if resolved.keys() != scan.keys() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Reports whether any recorded membership holder has committed.
    async fn any_committed<'a>(
        &self,
        covered: impl IntoIterator<Item = &'a LeafCoverage>,
        barrier: CurrentnessBarrier,
    ) -> Result<bool, TransError> {
        for leaf in covered {
            for holder in &leaf.pending_membership {
                if self
                    .mon
                    .tx_status_at(holder, Requirement::after(barrier))
                    .await?
                    == TxCommitStatus::Committed
                {
                    return Ok(true);
                }
            }
        }
        Ok(false)
    }

    /// Builds and writes the committed transaction record (the commit point).
    /// Records `locks` (the held lock set) alongside `writes` so the object
    /// carries its full recovery manifest for the GC check (ADR-022).
    async fn commit_writes(
        &self,
        accesses: &AccessSet,
        collections: &CollectionHandleState,
        locks: Vec<TxLock>,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut record = TxRecord::new(*id, TxCommitStatus::Committed);
        for w in accesses.final_writes() {
            let (value, deleted): (Arc<[u8]>, bool) = match w.operation() {
                WriteOp::Put(value) => (value.clone(), false),
                WriteOp::Delete => (Arc::from(&[] as &[u8]), true),
            };
            record.writes.push(TxWrite {
                key: w.key().clone(),
                value,
                deleted,
                prev_writer: None,
            });
        }
        collections.committed_manifest(locks).apply_to(&mut record);
        // `context` preserves the `AlreadyFinalized` sentinel and any in-doubt
        // outcome instead of collapsing them into a generic error.
        self.mon
            .commit_tx(record)
            .await
            .map_err(|e| e.context("creating transaction record"))
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
        CollectionAddress, CollectionId, DatabaseId, DbPrefix, LeafRef, NodeId, ObjectPath,
    };
    use glassdb_storage::transaction::{TxCommitStatus, TxRecordStore};
    use glassdb_storage::{
        CachedStore, CollectionRecord, CollectionStore, CurrentState, LeafBody, LeafEntry, Node,
        NodeStore, StorageError, TreeRouter,
    };
    use tokio::sync::Notify;

    const TEST_DB: &str = "testp";

    pub(super) fn test_collection() -> CollectionAddress {
        CollectionAddress::root(TEST_DB)
    }

    fn test_db_prefix() -> DbPrefix {
        DbPrefix::try_from(TEST_DB).unwrap()
    }

    pub(super) fn test_node_id(byte: u8) -> NodeId {
        NodeId::from_bytes([byte; 16])
    }

    pub(super) fn test_root_path() -> ObjectPath {
        ObjectPath::TreeRoot {
            collection: test_collection(),
        }
    }

    pub(super) fn logical_key(key: &[u8]) -> LogicalKey {
        LogicalKey::new(test_collection(), key)
    }

    pub(super) struct Tctx {
        pub(super) backend: Arc<dyn Backend>,
        pub(super) tx_records: TxRecordStore,
        pub(super) tmon: Monitor,
        pub(super) records: CollectionStore,
        pub(super) nodes: NodeStore,
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

    async fn new_algo_with_policy(policy: NodeSizePolicy) -> (Algo, Tctx) {
        new_algo_from_backend_with_cache_and_policy(Arc::new(MemoryBackend::new()), 1024, policy)
            .await
    }

    async fn new_algo_from_backend_with_cache(
        b: Arc<dyn Backend>,
        cache_bytes: usize,
    ) -> (Algo, Tctx) {
        new_algo_from_backend_with_cache_and_policy(b, cache_bytes, NodeSizePolicy::default()).await
    }

    async fn new_algo_from_backend_with_cache_and_policy(
        b: Arc<dyn Backend>,
        cache_bytes: usize,
        node_size_policy: NodeSizePolicy,
    ) -> (Algo, Tctx) {
        new_algo_from_backend_with_cache_policy_and_retirement(
            b,
            cache_bytes,
            node_size_policy,
            false,
        )
        .await
    }

    async fn new_algo_from_backend_with_cache_policy_and_retirement(
        b: Arc<dyn Backend>,
        cache_bytes: usize,
        node_size_policy: NodeSizePolicy,
        managed_retirement: bool,
    ) -> (Algo, Tctx) {
        let mut config = EngineConfig::default();
        config.set_cache_size(cache_bytes);
        config.set_node_size_policy(node_size_policy);
        config.set_protocol_timing(ProtocolTiming::simulation());
        let foundation = AssemblyFixture::new(b.clone(), test_db_prefix(), &config);

        // Create the tree root so the test collection exists up front.
        foundation
            .records
            .create_record(&test_collection(), &CollectionRecord::new())
            .await
            .unwrap();
        foundation
            .nodes
            .create_root(&test_collection(), &Node::leaf(LeafBody::new()))
            .await
            .unwrap();

        let engine = engine_fixture(&foundation, test_db_prefix(), config, managed_retirement);
        let algo = engine.algo.clone();
        (
            algo,
            Tctx {
                backend: b,
                tx_records: foundation.tx_records.clone(),
                tmon: foundation.monitor.clone(),
                records: foundation.records.clone(),
                nodes: foundation.nodes.clone(),
                timeline: foundation.timeline.clone(),
                locker: engine.locker.clone(),
                _engine: engine,
            },
        )
    }

    pub(super) fn wa(key: &LogicalKey, val: &[u8]) -> WriteAccess {
        WriteAccess::put(key.clone(), Arc::from(val))
    }

    pub(super) fn wdel(key: &LogicalKey) -> WriteAccess {
        WriteAccess::delete(key.clone())
    }

    pub(super) async fn do_read(tctx: &Tctx, key: &LogicalKey) -> ReadAccess {
        let outcome = read_outcome(tctx, key).await;
        let (_, evidence) = outcome.into_parts();
        ReadAccess::new(key.clone(), evidence)
    }

    pub(super) async fn read_outcome(tctx: &Tctx, key: &LogicalKey) -> crate::reader::ReadOutcome {
        let reader = Reader::new(
            KeyResolver::new(
                TreeRouter::new(tctx.nodes.clone(), std::num::NonZeroUsize::MIN),
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
        tm.begin(accesses, CatalogAccesses::default())
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

    pub(super) async fn entry(tctx: &Tctx, key: &[u8]) -> Option<LeafEntry> {
        let loaded = tctx
            .nodes
            .load_leaf(
                &test_root_path(),
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        loaded.entries().lookup(key).cloned()
    }

    #[tokio::test(start_paused = true)]
    async fn interrupted_retirement_hands_off_local_locks_when_its_status_read_fails() {
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
            NodeSizePolicy::default(),
            true,
        )
        .await;
        let key = logical_key(b"interrupted");
        let accesses = AccessSet::new(Vec::new(), vec![wa(&key, b"uncommitted")], Vec::new());
        let interrupted_handle = begin_accesses(&algo, accesses.clone());
        let interrupted = *interrupted_handle.id();
        tctx.tmon.begin_tx(&interrupted);
        let outcome = tctx
            .locker
            .keys()
            .lock_at(&interrupted, &accesses, false, Requirement::ANY)
            .await
            .unwrap();
        assert!(matches!(outcome, LockOutcome::Locked(_)));

        fail_status_read.store(true, Ordering::SeqCst);
        drop(interrupted_handle);
        tokio::time::timeout(Duration::from_secs(1), failure_reached.notified())
            .await
            .expect("retirement never attempted its status read");

        let (peer, peer_ctx) = new_algo_from_backend(backend).await;
        peer_ctx.tmon.preempt_tx(&interrupted).await.unwrap();
        let mut peer_handle = begin_accesses(&peer, accesses);
        tokio::time::timeout(Duration::from_secs(1), peer.commit(&mut peer_handle))
            .await
            .expect("protocol help-forward did not make progress")
            .unwrap();
        peer.end(&mut peer_handle).await.unwrap();
    }

    #[tokio::test]
    async fn direct_write_new() {
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");
        let val = b"v";

        let mut h = begin_accesses(
            &tm,
            AccessSet::new(Vec::new(), vec![wa(&keyp, val)], Vec::new()),
        );
        tm.commit(&mut h).await.unwrap();
        let tid = *h.id();
        tm.end(&mut h).await.unwrap();

        let status = tctx
            .tx_records
            .commit_status_at(&tid, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(status.status, TxCommitStatus::Unknown);
        assert!(
            matches!(
                tctx.tx_records.get_at(&tid, Requirement::ANY).await,
                Err(StorageError::NotFound)
            ),
            "a direct create has no transaction record"
        );

        // The leaf entry points at the committed writer and the lock is gone.
        let e = entry(&tctx, b"k").await.unwrap();
        assert_eq!(e.current.writer(), Some(&tid));
        assert!(e.lock_holders().is_empty());
    }

    // Regression (review 1.1 / ADR-022): the committed transaction record must
    // record its full lock set, not just its writes, so the GC check
    // and lock pruning operate on real records. A transaction that reads one
    // key and creates another records both key locks plus the leaf's structural
    // gate and membership scopes (ADR-032).
    #[tokio::test]
    async fn commit_records_locks() {
        let (tm, tctx) = new_algo().await;
        let readp = logical_key(b"r");
        let writep = logical_key(b"w");

        // Seed the read key so it resolves to a committed value.
        commit_writes(&tm, vec![wa(&readp, b"seed")]).await;

        let r = do_read(&tctx, &readp).await;
        let external = vec![b'v'; InlinePolicy::default().max_value_bytes + 1];
        let mut h = begin_accesses(
            &tm,
            AccessSet::new(vec![r], vec![wa(&writep, &external)], Vec::new()),
        );
        tm.commit(&mut h).await.unwrap();
        let tid = *h.id();
        tm.end(&mut h).await.unwrap();

        let record = tctx
            .tx_records
            .get_at(&tid, Requirement::ANY)
            .await
            .unwrap();
        let record = record.value().unwrap();
        assert!(record.locks.contains(&TxLock::Key {
            key: readp,
            typ: LockType::Read,
        }));
        assert!(record.locks.contains(&TxLock::Key {
            key: writep,
            typ: LockType::Write,
        }));
        let leaf = LeafRef::root(test_collection());
        assert!(record.locks.contains(&TxLock::Membership {
            leaf,
            typ: LockType::Write,
        }));
    }

    #[tokio::test]
    async fn committed_manifest_preserves_prepared_roots_from_earlier_body_runs() {
        let (tm, tctx) = new_algo().await;
        let earlier = CollectionAddress::new(
            TEST_DB,
            CollectionId::from_slice(&[1; 16]).expect("fixed ID has the required width"),
        );
        let active = CollectionAddress::new(
            TEST_DB,
            CollectionId::from_slice(&[2; 16]).expect("fixed ID has the required width"),
        );
        let earlier_accesses = CatalogAccesses {
            reads: Vec::new(),
            changes: vec![CollectionChange {
                parent: test_collection(),
                name: b"earlier".to_vec(),
                collection: earlier.clone(),
                expected: None,
                op: CollectionOp::Create,
            }],
        };
        let active_accesses = CatalogAccesses {
            reads: Vec::new(),
            changes: vec![CollectionChange {
                parent: test_collection(),
                name: b"active".to_vec(),
                collection: active.clone(),
                expected: None,
                op: CollectionOp::Create,
            }],
        };
        let mut handle = tm.begin(AccessSet::default(), earlier_accesses);
        tm.collection_commit
            .prepare(&mut handle.collections)
            .await
            .unwrap();
        handle.collections.replace_accesses(active_accesses);
        tm.collection_commit
            .prepare(&mut handle.collections)
            .await
            .unwrap();
        let id = *handle.id();
        tm.mon.begin_tx(&id);

        tm.commit_writes(&AccessSet::default(), &handle.collections, Vec::new(), &id)
            .await
            .unwrap();

        let record = tctx.tx_records.get_at(&id, Requirement::ANY).await.unwrap();
        let record = record.value().unwrap();
        assert_eq!(record.prepared_collections, vec![earlier, active.clone()]);
        assert_eq!(record.collection_changes.len(), 1);
        assert_eq!(record.collection_changes[0].collection, active);
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
            CatalogAccesses {
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
        let id = *handle.id();
        let lock = TxLock::TopologyParticipant {
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

        let record = tctx.tx_records.get_at(&id, Requirement::ANY).await.unwrap();
        let record = record.value().unwrap();
        assert_eq!(record.status, TxCommitStatus::Pending);
        assert_eq!(record.locks, vec![lock]);
        assert_eq!(record.collection_changes.len(), 1);
        assert_eq!(record.collection_changes[0].collection, created.clone());
        assert_eq!(record.prepared_collections, vec![created]);
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
            CatalogAccesses {
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
        let id = *handle.id();
        tm.mon.begin_tx(&id);
        assert!(handle.engage());
        tm.commit_writes(&AccessSet::default(), &handle.collections, Vec::new(), &id)
            .await
            .unwrap();

        tm.end(&mut handle).await.unwrap();

        tctx.records
            .load_record(&prepared, Requirement::ANY)
            .await
            .expect("committed collection record must survive cleanup");
        tctx.nodes
            .load_root(&prepared, Requirement::ANY)
            .await
            .expect("committed collection tree root must survive cleanup");
    }

    #[tokio::test]
    async fn a_replayed_body_clears_a_discarded_partial_drop() {
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
            tctx.nodes
                .create_root(&dropped, &Node::leaf(LeafBody::new()))
                .await
                .unwrap()
        );
        let mut handle = tm.begin(
            AccessSet::default(),
            CatalogAccesses {
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
        let id = *handle.id();
        tm.mon.begin_tx(&id);
        assert!(handle.engage());
        tm.collection_commit
            .fence(&id, &mut handle.collections)
            .await
            .unwrap();
        handle
            .collections
            .replace_accesses(CatalogAccesses::default());

        tm.commit(&mut handle).await.unwrap();

        let (root, _) = tctx
            .nodes
            .load_root(
                &dropped,
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert_eq!(root.drop_intent(), None);
        let (record, _) = tctx
            .records
            .load_record(&dropped, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(record.topology_freeze(), None);
        tm.end(&mut handle).await.unwrap();
    }

    #[tokio::test]
    async fn read_then_write_round_trips() {
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");

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
    // abort-and-renew; it replays the body in place (`Retry`) while holding its
    // locks. The engine validates *after* locking, so unlike a pre-lock check the
    // moved key is itself locked during the replay window — the v1 guarantee that
    // the replay holds all its locks. An over-inline-budget output explicitly
    // forces locked commit.
    #[tokio::test]
    async fn invalidated_read_write_retries_holding_locks() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let ka = logical_key(b"k");
        let kb = logical_key(b"k2");

        // Seed both keys so the writes are overwrites (not creates), keeping the
        // transaction on the read-write path rather than a membership change.
        commit_writes(&tm2, vec![wa(&ka, b"v1")]).await;
        commit_writes(&tm2, vec![wa(&kb, b"x1")]).await;
        let ra = do_read(&tctx, &ka).await;

        // Another database instance overwrites `k`, making `ra` stale.
        commit_writes(&tm2, vec![wa(&ka, b"v2")]).await;

        let external = vec![b'x'; InlinePolicy::default().max_value_bytes + 1];
        let mut h = begin_accesses(
            &tm,
            AccessSet::new(
                vec![ra],
                vec![wa(&ka, b"v3"), wa(&kb, &external)],
                Vec::new(),
            ),
        );
        assert_eq!(tm.commit(&mut h).await.unwrap(), BodyDecision::ReplayBody);

        // The moved key is locked by us when the invalidated read is signalled: the
        // replay owns the lock and cannot lose it again to the same race.
        let e = entry(&tctx, b"k").await.expect("entry exists");
        assert_eq!(e.lock_holders(), std::slice::from_ref(h.id()));

        tm.end(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn wound_renews_the_identity_before_body_replay() {
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");
        let value = vec![b'x'; InlinePolicy::default().max_value_bytes + 1];
        let mut h = begin_accesses(
            &tm,
            AccessSet::new(Vec::new(), vec![wa(&keyp, &value)], Vec::new()),
        );
        let old_id = *h.id();
        assert!(h.engage());
        tctx.tmon.begin_tx(&old_id);
        tctx.tmon.preempt_tx(&old_id).await.unwrap();

        assert_eq!(tm.commit(&mut h).await.unwrap(), BodyDecision::ReplayBody);
        assert_ne!(*h.id(), old_id);
        assert!(!h.id().older(&old_id));
        assert!(!old_id.older(h.id()));

        tm.end(&mut h).await.unwrap();
    }

    // ADR-065: a timed-out parallel acquisition asks its owner to make the old
    // identity durably abort-side before it renews for serial acquisition.
    #[tokio::test(start_paused = true)]
    async fn deadlock_timeout_renews_before_serial_acquisition() {
        use crate::tlocker::LockOutcome;
        use std::time::Duration;
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");

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
                Requirement::after(tctx.timeline.currentness_barrier()),
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
        let id_before = *h.id();
        let tm2 = tm.clone();
        let committing = tokio::spawn(async move {
            let res = tm2.commit(&mut h).await;
            (h, res)
        });

        // The parallel wait times out. The algorithm retires the old identity
        // and continues under a renewed identity with serial acquisition.
        rt::sleep(MAX_DEADLOCK_TIMEOUT + Duration::from_secs(1)).await;
        assert!(!committing.is_finished());

        let old_status = tctx
            .tx_records
            .commit_status_at(&id_before, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(old_status.status, TxCommitStatus::Wounded);

        // The replacement commits after the older holder becomes abort-side.
        tctx.tmon.abort_owned_tx(&holder).await.unwrap();
        let (mut h, res) = committing.await.unwrap();
        assert_eq!(
            res.expect("renewed identity commits once the holder releases"),
            BodyDecision::ReturnOutcome
        );
        let renewed_id = *h.id();
        assert_ne!(renewed_id, id_before);
        assert!(!renewed_id.older(&id_before));
        assert!(!id_before.older(&renewed_id));
        assert_eq!(*h.id(), renewed_id);
        tm.end(&mut h).await.unwrap();
    }

    // A database can contain an unsafe singleton written by an older database instance or
    // admitted under a former policy. If capacity remains unavailable while the
    // restructurer cannot relieve it, lock acquisition must report the bounded wait
    // instead of retrying forever.
    #[tokio::test(start_paused = true)]
    async fn leaf_capacity_retry_episode_is_bounded() {
        let policy = NodeSizePolicy::builder()
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
        let unsafe_entry = LeafEntry::new(first.clone()).with_current(CurrentState::Inline {
            writer,
            value: Arc::from(vec![b'v'; 128]),
        });
        assert!(!policy.entry_fits_split_budget(&unsafe_entry));
        let creator = TxId::with_priority(2, b"new");
        let mut create_entry = LeafEntry::new(second.clone());
        create_entry.replace_create_lock(creator);
        assert!(
            Node::leaf(LeafBody::from_entries([unsafe_entry.clone(), create_entry]))
                .content_encoded_len()
                > policy.content_limit(),
            "the accepted second key must reproduce LeafFull"
        );
        assert!(
            Node::leaf(LeafBody::from_entries([unsafe_entry.clone()])).encoded_len()
                <= policy.node_max_bytes(),
            "the grandfathered singleton itself must remain storable"
        );

        let path = test_root_path();
        let loaded = tctx
            .nodes
            .load_leaf(
                &path,
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(LeafBody::from_entries([unsafe_entry]));
        assert!(tctx.nodes.commit_leaf(edit).await.unwrap().is_applied());

        let key = logical_key(&second);
        let mut handle = begin_accesses(
            &tm,
            AccessSet::new(Vec::new(), vec![wa(&key, b"value")], Vec::new()),
        );
        let error = tokio::time::timeout(
            MAX_LEAF_FULL_WAIT + Duration::from_secs(10),
            tm.commit(&mut handle),
        )
        .await
        .expect("the unchanged full leaf must produce a final result")
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
                            .try_update(SeqCst, SeqCst, |n| n.checked_sub(1))
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

    #[tokio::test(start_paused = true)]
    async fn invalidated_read_replay_reports_an_earlier_internal_renewal() {
        let (backend, flaky) = FlakyCas::wrap(
            Arc::new(MemoryBackend::new()),
            test_root_path().to_string(),
            SERIAL_FALLBACK_AFTER * crate::leaf_coord::CAS_RETRIES,
        );
        let (algo, context) = new_algo_from_backend(backend).await;
        let key = logical_key(b"key");
        let external = vec![1; InlinePolicy::default().max_value_bytes + 1];
        commit_writes(&algo, vec![wa(&key, &external)]).await;
        let stale = do_read(&context, &key).await;
        commit_writes(&algo, vec![wa(&key, b"changed")]).await;
        flaky.arm();
        let mut handle = begin_accesses(
            &algo,
            AccessSet::new(vec![stale], vec![wa(&key, &external)], Vec::new()),
        );
        let old_id = *handle.id();
        assert_eq!(
            algo.commit(&mut handle).await.unwrap(),
            BodyDecision::ReplayBody,
        );
        assert_ne!(handle.id(), &old_id);
        assert_eq!(flaky.remaining(), 0);
        algo.end(&mut handle).await.unwrap();
    }

    // ADR-065: sustained completed parallel conflicts renew once before sorted
    // serial acquisition. They do not replay the transaction body.
    //
    // Direct publication is disabled explicitly so this test genuinely
    // exercises locked commit's renewed serial-fallback behaviour.
    #[tokio::test(start_paused = true)]
    async fn cas_contention_renews_before_serial_acquisition() {
        let retry = RetryConfig {
            initial_interval: Duration::from_millis(10),
            max_interval: Duration::from_millis(20),
        };
        const ACQUISITION_RETRIES: usize = 8;
        let failures = ACQUISITION_RETRIES * crate::leaf_coord::CAS_RETRIES;
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
        let keyp = logical_key(b"k");
        let keyp2 = logical_key(b"k2");

        // Seed the keys over a clean connection so their nodes exist (the lock
        // CAS is then a `write_if`, the thing we fault).
        let mut seed = engine.begin_transaction(
            AccessSet::new(
                Vec::new(),
                vec![wa(&keyp, b"v1"), wa(&keyp2, b"v1")],
                Vec::new(),
            ),
            CatalogAccesses::default(),
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
            CatalogAccesses::default(),
        );
        let id_before = *h.id();
        let outcome = engine
            .commit(&mut h)
            .await
            .expect("serial fallback commits after contention clears");
        assert_eq!(outcome, BodyDecision::ReturnOutcome);
        assert_ne!(*h.id(), id_before);
        let status_objects = CachedStore::new(status_backend, 1024, Timeline::new(), None);
        let status_records = TxRecordStore::new(status_objects.clone(), test_db_prefix());
        let old_status = status_records
            .commit_status_at(&id_before, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(old_status.status, TxCommitStatus::Aborted);
        engine.end(&mut h).await.unwrap();

        // The whole budget was consumed, so the transaction did exhaust the
        // serial CAS budget (the `Conflict` path), not merely time out in
        // parallel acquisition.
        assert_eq!(flaky.remaining(), 0, "expected sustained CAS contention");
        let attempts = flaky.attempts();
        assert!(attempts.len() > failures);
        let acquisition_gaps: Vec<_> = (1..=ACQUISITION_RETRIES)
            .map(|round| {
                let next = round * crate::leaf_coord::CAS_RETRIES;
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
        // It still committed: the nodes point at our writer with no live lock.
        for key in [&keyp, &keyp2] {
            let read = engine.read(key, Duration::ZERO).await.unwrap();
            assert_eq!(read.value.unwrap().value.as_ref(), b"v2");
        }
        engine.shutdown().await;
        status_objects.shutdown().await;
    }

    // Builds an algo whose backend records every operation, so tests can prove
    // whether direct commit or locked commit ran by counting the CAS writes it issued.
    pub(super) async fn new_recording_algo() -> (Algo, Tctx, OpLog) {
        let mem: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let rec = Arc::new(RecordingBackend::new(mem));
        let log = rec.log();
        let (tm, tctx) = new_algo_from_backend(rec).await;
        (tm, tctx, log)
    }

    // CAS-write counts by object kind, the fingerprint of direct commit vs locked commit: a
    // Direct commit (ADR-051) issues one leaf write and no transaction record at
    // all; locked commit issues one transaction-record write and two leaf writes (the
    // lock CAS then the write-back CAS that publishes the current state — run
    // synchronously here because tests build the algo with no background
    // executor). Node-level
    // locks are included in those writes rather than adding another CAS (ADR-032).
    #[derive(Debug, Default)]
    pub(super) struct WriteCounts {
        // Writes to a leaf coordination object (ADR-031): a standalone node
        // `/_n/` or the tree root `/_r`, which holds the small collection's
        // single leaf entries. Key-lock and write-back CAS both land here and
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

    // Transaction nodes use two symbols from the same alphabet as path type
    // markers. In particular, a transaction can live under `/_t/_/n/`; path
    // substring checks would mistake its object create for a standalone-node
    // write and make commit-path counts depend on random transaction entropy.
    #[test]
    fn write_counts_parses_transaction_prefix_named_like_node() {
        let id = TxId::with_priority(0, &[0x97, 0x30]);
        let path = ObjectPath::Transaction {
            db_prefix: test_db_prefix(),
            id,
        }
        .to_string();
        assert!(path.contains("/_t/_/n/"), "test id mapped to {path:?}");
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
    pub(super) fn leaf_reads(log: &OpLog) -> (usize, usize) {
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
    async fn locked_rw_commit_reuses_cached_leaf() {
        let (tm, tctx, log) = new_recording_algo_big_cache().await;
        let key = logical_key(b"key");
        commit_writes(&tm, vec![wa(&key, b"initial")]).await;
        let read = do_read(&tctx, &key).await;
        // This value requires locked commit, including lock acquisition,
        // post-lock validation, and write-back.
        let value = vec![b'x'; InlinePolicy::default().max_value_bytes + 1];
        let mut handle = begin_accesses(
            &tm,
            AccessSet::new(vec![read], vec![wa(&key, &value)], Vec::new()),
        );

        log.lock().unwrap().clear();
        assert_eq!(
            tm.commit(&mut handle).await.unwrap(),
            BodyDecision::ReturnOutcome
        );
        tm.end(&mut handle).await.unwrap();
        let writes = write_counts(&log);
        assert_eq!(writes.leaf, 2);
        assert_eq!(writes.tx, 1);
        assert_eq!(leaf_reads(&log), (0, 0));
        let (actual, _) = read_outcome(&tctx, &key).await.into_parts();
        assert_eq!(actual.unwrap().value.as_ref(), value);
    }

    #[tokio::test]
    async fn readonly_validates() {
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let r = do_read(&tctx, &keyp).await;

        let mut h = begin_accesses(&tm, AccessSet::new(vec![r], Vec::new(), Vec::new()));
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn opaque_point_read_evidence_tracks_validation() {
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");
        commit_writes(&tm, vec![wa(&keyp, b"v1")]).await;

        let outcome = read_outcome(&tctx, &keyp).await;
        let (_, evidence) = outcome.into_parts();
        let accesses = AccessSet::new(
            vec![ReadAccess::new(keyp.clone(), evidence)],
            Vec::new(),
            Vec::new(),
        );

        let barrier = tctx.timeline.currentness_barrier();
        assert!(
            tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap(),
            "opaque evidence accepts its current value"
        );

        let (peer, _peer_ctx) = new_algo_from_backend(tctx.backend.clone()).await;
        commit_writes(&peer, vec![wa(&keyp, b"v2")]).await;
        let barrier = tctx.timeline.currentness_barrier();
        assert!(
            !tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap(),
            "opaque evidence rejects a superseded value"
        );
    }

    #[tokio::test]
    async fn unmarked_absence_validates_representation_churn_but_not_membership_aba() {
        let (tm, tctx) = new_algo().await;
        let key = logical_key(b"missing");
        let read = do_read(&tctx, &key).await;
        assert!(read.validates(None, 0));
        assert!(!read.validates(None, 1));
        let accesses = AccessSet::new(vec![read], Vec::new(), Vec::new());

        let barrier = tctx.timeline.currentness_barrier();
        let loaded = tctx
            .nodes
            .load_leaf(&test_root_path(), Requirement::ANY)
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(LeafBody::from_entries([LeafEntry::new(b"other")
            .with_current(CurrentState::Tombstone {
                writer: TxId::with_priority(1, b"representation"),
            })]));
        assert!(tctx.nodes.commit_leaf(edit).await.unwrap().is_applied());
        assert!(
            tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap(),
            "representing an unrelated absence does not change membership"
        );

        let barrier = tctx.timeline.currentness_barrier();
        let loaded = tctx
            .nodes
            .load_leaf(&test_root_path(), Requirement::ANY)
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(LeafBody::new());
        let mut locks = edit.locks().clone();
        locks.advance_membership_generation();
        locks.advance_membership_generation();
        edit.set_locks(locks);
        assert!(tctx.nodes.commit_leaf(edit).await.unwrap().is_applied());
        assert!(
            !tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap(),
            "create-delete-reclaim returning to unmarked absence changes its generation"
        );
    }

    #[tokio::test]
    async fn tombstone_read_is_invalidated_when_its_exact_marker_is_reclaimed() {
        let (tm, tctx) = new_algo().await;
        let key = logical_key(b"deleted");
        let writer = TxId::with_priority(1, b"deleter");
        let loaded = tctx
            .nodes
            .load_leaf(&test_root_path(), Requirement::ANY)
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(LeafBody::from_entries([
            LeafEntry::new(b"deleted").with_current(CurrentState::Tombstone { writer })
        ]));
        assert!(tctx.nodes.commit_leaf(edit).await.unwrap().is_applied());

        let read = do_read(&tctx, &key).await;
        assert!(read.validates(Some(&writer), 0));
        assert!(read.validates(Some(&writer), 1));
        let accesses = AccessSet::new(vec![read], Vec::new(), Vec::new());
        let barrier = tctx.timeline.currentness_barrier();

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
                Requirement::after(external_timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let mut edit = loaded.into_edit();
        edit.set_entries(LeafBody::new());
        assert!(external.commit_leaf(edit).await.unwrap().is_applied());

        assert!(
            !tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn point_read_re_resolves_writer_at_validation_barrier() {
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");
        let previous = *commit_writes(&tm, vec![wa(&keyp, b"v1")]).await.id();

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
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(locked) => locked,
            _ => panic!("holder lock must succeed"),
        };

        let read = do_read(&tctx, &keyp).await;
        assert!(read.validates(Some(&previous), 0));
        let accesses = AccessSet::new(vec![read], Vec::new(), Vec::new());
        let barrier = tctx.timeline.currentness_barrier();

        // Finalize only the transaction record. The leaf still contains the
        // same pending lock, so leaf validation alone cannot detect that the
        // effective writer moved.
        let mut record = TxRecord::new(holder, TxCommitStatus::Committed);
        record.locks = locked.locked_paths();
        record.writes.push(TxWrite {
            key: keyp,
            value: Arc::from(b"v2".as_slice()),
            deleted: false,
            prev_writer: Some(previous),
        });
        tctx.tmon.commit_tx(record).await.unwrap();

        assert!(
            !tm.validate_read_observations(&accesses, barrier, None)
                .await
                .unwrap(),
            "an exclusive holder prevents the leaf-only shortcut"
        );
        assert!(
            !tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap(),
            "writer resolution at the validation barrier observes the committed holder"
        );
    }

    #[tokio::test]
    async fn point_read_accepts_aborted_holder_at_validation_barrier() {
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");
        let previous = *commit_writes(&tm, vec![wa(&keyp, b"v1")]).await.id();

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
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(_) => {}
            _ => panic!("holder lock must succeed"),
        }

        let read = do_read(&tctx, &keyp).await;
        assert!(read.validates(Some(&previous), 0));
        let accesses = AccessSet::new(vec![read], Vec::new(), Vec::new());
        let barrier = tctx.timeline.currentness_barrier();

        // Aborting the holder leaves the previously observed writer effective.
        // The exclusive holder prevents a physical shortcut, then writer
        // resolution at the validation barrier accepts the unchanged value.
        tctx.tmon.abort_owned_tx(&holder).await.unwrap();
        assert!(
            !tm.validate_read_observations(&accesses, barrier, None)
                .await
                .unwrap()
        );
        assert!(
            tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn locked_validation_requires_its_own_cas_receipt() {
        let (tm, tctx) = new_algo().await;
        let ka = logical_key(b"a");
        let kb = logical_key(b"b");
        commit_writes(&tm, vec![wa(&ka, b"a0"), wa(&kb, b"b0")]).await;

        // `commit_writes` publishes current states in a background task. This test is
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
        let observed_membership = observed.value().unwrap().membership_generation();
        let barrier = tctx.timeline.currentness_barrier();

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
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(locked) => locked,
            _ => panic!("disjoint lock acquisition must succeed"),
        };
        assert!(other_locked.certifies_membership(&observed, observed_membership, barrier));

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
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(locked) => locked,
            _ => panic!("disjoint read lock acquisition must succeed"),
        };
        assert!(!current_locked.certifies_membership(&observed, observed_membership, barrier));
        assert!(
            !tm.validate_read_observations(&current_data, barrier, Some(&current_locked),)
                .await
                .unwrap()
        );

        tctx.locker
            .keys()
            .release_leaf(&current, &test_root_path(), Requirement::ANY)
            .await
            .unwrap();
        tctx.locker
            .keys()
            .release_leaf(&other, &test_root_path(), Requirement::ANY)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn newer_shared_evidence_runs_logical_validation_without_io() {
        let (tm, tctx, log) = new_recording_algo_big_cache().await;
        let ka = logical_key(b"a");
        let kb = logical_key(b"b");
        commit_writes(&tm, vec![wa(&ka, b"a0"), wa(&kb, b"b0")]).await;

        let read = do_read(&tctx, &ka).await;
        let barrier = tctx.timeline.currentness_barrier();

        // A separate database instance rewrites the shared leaf for B. Its cache is
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
            .load_leaf(
                &leaf_path,
                Requirement::after(external_timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let mut entries: BTreeMap<Vec<u8>, LeafEntry> = loaded
            .entries()
            .entries()
            .cloned()
            .map(|entry| (entry.key.clone(), entry))
            .collect();
        entries.get_mut(b"b".as_slice()).unwrap().current = CurrentState::External {
            writer: TxId::with_priority(3, b"external"),
        };
        let mut edit = loaded.into_edit();
        edit.set_entries(LeafBody::from_entries(entries.into_values()));
        assert!(external.commit_leaf(edit).await.unwrap().is_applied());

        // A local disjoint lock observes that external state and publishes a
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
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap()
        {
            LockOutcome::Locked(locked) => locked,
            _ => panic!("disjoint lock acquisition must succeed"),
        };

        let accesses = AccessSet::new(vec![read], Vec::new(), Vec::new());
        log.lock().unwrap().clear();
        assert!(
            !tm.validate_read_observations(&accesses, barrier, None)
                .await
                .unwrap(),
            "the retained physical revision changed"
        );
        assert!(
            tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap(),
            "logical validation accepts the unchanged writer"
        );
        assert_eq!(
            leaf_reads(&log),
            (0, 0),
            "post-bound current evidence satisfies both validation steps locally"
        );

        tctx.locker
            .keys()
            .release_leaf(&other, &test_root_path(), Requirement::ANY)
            .await
            .unwrap();
        drop(other_locked);
    }

    #[tokio::test]
    async fn readonly_replay_locks_its_complete_point_read_set() {
        let (tm, tctx) = new_algo().await;
        let (tm2, _t2) = new_algo_from_backend(tctx.backend.clone()).await;
        let ka = logical_key(b"a");
        let kb = logical_key(b"b");

        commit_writes(&tm2, vec![wa(&ka, b"a1"), wa(&kb, b"b1")]).await;
        let ra = do_read(&tctx, &ka).await;
        let rb = do_read(&tctx, &kb).await;
        commit_writes(&tm2, vec![wa(&ka, b"a2")]).await;

        let mut h = begin_accesses(&tm, AccessSet::new(vec![ra, rb], Vec::new(), Vec::new()));
        assert_eq!(tm.commit(&mut h).await.unwrap(), BodyDecision::ReplayBody);
        assert!(h.should_lock_reads());
        for key in [b"a".as_slice(), b"b"] {
            assert_eq!(
                entry(&tctx, key).await.unwrap().lock_type(),
                LockType::None,
                "failed optimistic validation must not lock"
            );
        }

        // The replay re-reads, then its second validation acquires locks for the
        // complete fresh read set before deciding whether it can commit.
        let ra = do_read(&tctx, &ka).await;
        let rb = do_read(&tctx, &kb).await;
        tm.reset(&mut h, AccessSet::new(vec![ra, rb], Vec::new(), Vec::new()));
        tm.commit(&mut h).await.unwrap();
        let record = tctx
            .tx_records
            .get_at(h.id(), Requirement::ANY)
            .await
            .unwrap();
        let record = record.value().unwrap();
        for key in [ka, kb] {
            assert!(record.locks.contains(&TxLock::Key {
                key,
                typ: LockType::Read,
            }));
        }
    }

    #[tokio::test]
    async fn readonly_after_delete_not_found() {
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");

        commit_writes(&tm, vec![wa(&keyp, b"v")]).await;
        let deleted_by = *commit_writes(&tm, vec![wdel(&keyp)]).await.id();

        // A read now resolves to not-found.
        let r = do_read(&tctx, &keyp).await;
        assert!(r.validates(Some(&deleted_by), 0));
        let mut h = begin_accesses(&tm, AccessSet::new(vec![r], Vec::new(), Vec::new()));
        tm.commit(&mut h).await.unwrap();
    }

    #[tokio::test]
    async fn delete_round_trips() {
        let (tm, tctx) = new_algo().await;
        let keyp = logical_key(b"k");

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
        let k1 = logical_key(b"k1");
        let k2 = logical_key(b"k2");

        let mut h = begin_accesses(
            &tm,
            AccessSet::new(Vec::new(), vec![wa(&k1, b"v1"), wa(&k2, b"v2")], Vec::new()),
        );
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();

        assert!(entry(&tctx, b"k1").await.unwrap().exists());
        assert!(entry(&tctx, b"k2").await.unwrap().exists());
    }

    // Installs live committed current states for `keys` directly in the collection's
    // root leaf `_r` (no lock holders or pending write-back), giving scan tests a
    // stable membership baseline.
    async fn seed_live_keys(tctx: &Tctx, keys: &[&[u8]]) {
        let path = test_root_path();
        let loaded = tctx
            .nodes
            .load_leaf(
                &path,
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let mut entries: std::collections::BTreeMap<Vec<u8>, LeafEntry> = loaded
            .entries()
            .entries()
            .cloned()
            .map(|e| (e.key.clone(), e))
            .collect();
        for (i, k) in keys.iter().enumerate() {
            let w = TxId::with_priority((i as u64) + 1, b"seed");
            entries.insert(
                k.to_vec(),
                LeafEntry::new(*k).with_current(CurrentState::External { writer: w }),
            );
        }
        let leaf = LeafBody::from_entries(entries.into_values());
        let mut edit = loaded.into_edit();
        edit.set_entries(leaf);
        assert!(tctx.nodes.commit_leaf(edit).await.unwrap().is_applied());
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
            TreeRouter::new(tctx.nodes.clone(), std::num::NonZeroUsize::MIN),
            KeyStateResolver::new(tctx.tmon.clone()),
            std::num::NonZeroUsize::MIN,
        );
        let scan = resolver
            .scan_keys(&test_collection(), &range, &[], None, None)
            .await
            .unwrap();
        let keys = scan.keys().to_vec();
        let access = scan.into_access(test_collection(), range, Vec::new());
        let accesses = AccessSet::new(Vec::new(), writes, vec![access]);
        (accesses, keys)
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
            TreeRouter::new(tctx.nodes.clone(), std::num::NonZeroUsize::MIN),
            KeyStateResolver::new(tctx.tmon.clone()),
            std::num::NonZeroUsize::MIN,
        );
        let result = resolver
            .scan_keys(&test_collection(), &range, &[], None, None)
            .await
            .unwrap();
        let accesses = AccessSet::new(
            Vec::new(),
            Vec::new(),
            vec![result.into_access(test_collection(), range, Vec::new())],
        );

        let barrier = tctx.timeline.currentness_barrier();
        assert!(
            tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap(),
            "opaque evidence accepts its current membership"
        );

        commit_writes(&tm, vec![wa(&logical_key(b"b"), b"1")]).await;
        let barrier = tctx.timeline.currentness_barrier();
        assert!(
            !tm.validate(&accesses, ValidationContext::Optimistic, barrier)
                .await
                .unwrap(),
            "opaque evidence rejects a changed membership"
        );
    }

    // ADR-031 phantom prevention: a listing whose covered leaves are unchanged
    // commits, but one whose leaf a concurrent create mutated (bumping the leaf
    // generation) fails validation and must replay the body — so the create can never appear
    // as a phantom inside an already-validated snapshot.
    #[tokio::test]
    async fn scan_detects_racing_create() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"c"]).await;

        let (accesses, keys) = scan_accesses(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"c".to_vec()]);

        // No concurrent change: the listing validates and commits.
        tctx.locker.stats_and_reset();
        let mut h = begin_accesses(&tm, accesses.clone());
        tm.commit(&mut h).await.unwrap();
        tm.end(&mut h).await.unwrap();
        assert_eq!(tctx.locker.stats_and_reset().calls, 0);

        // A create between the scan and (re-)validation bumps the covered leaf.
        commit_writes(&tm, vec![wa(&logical_key(b"b"), b"1")]).await;

        let mut stale = begin_accesses(&tm, accesses);
        assert_eq!(
            tm.commit(&mut stale).await.unwrap(),
            BodyDecision::ReplayBody
        );
        assert!(
            stale.should_lock_reads(),
            "scan body replay escalates to read locks"
        );

        // The body replay computes a fresh page, then commits through locked commit.
        let (fresh, keys) = scan_accesses(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()]);
        tm.reset(&mut stale, fresh);
        tctx.locker.stats_and_reset();
        tm.commit(&mut stale).await.unwrap();
        assert!(tctx.locker.stats_and_reset().calls >= 1);
        let record = tctx
            .tx_records
            .get_at(stale.id(), Requirement::ANY)
            .await
            .unwrap();
        let record = record.value().unwrap();
        assert!(record.locks.iter().any(|lock| matches!(
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
        let key_path = logical_key(b"new");
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
                Requirement::after(tctx.timeline.currentness_barrier()),
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

        // Commit only the transaction record: membership_generation is unchanged
        // until write-back, so the dependency is what must reject validation.
        let mut record = TxRecord::new(holder, TxCommitStatus::Committed);
        record.locks = locked.locked_paths();
        record.writes.push(TxWrite {
            key: key_path,
            value: Arc::from(b"value".as_slice()),
            deleted: false,
            prev_writer: None,
        });
        tctx.tmon.commit_tx(record).await.unwrap();

        let mut stale = begin_accesses(&tm, scan);
        assert_eq!(
            tm.commit(&mut stale).await.unwrap(),
            BodyDecision::ReplayBody
        );
    }

    #[tokio::test]
    async fn scan_with_write_records_predicate_locks() {
        let (tm, tctx) = new_algo().await;
        let key_path = logical_key(b"a");
        seed_live_keys(&tctx, &[b"a"]).await;
        let (accesses, _) =
            scan_accesses_for_range(&tctx, ScanRange::all(), vec![wa(&key_path, b"updated")]).await;

        let mut handle = begin_accesses(&tm, accesses);
        tm.commit(&mut handle).await.unwrap();
        let record = tctx
            .tx_records
            .get_at(handle.id(), Requirement::ANY)
            .await
            .unwrap();
        let record = record.value().unwrap();
        assert!(record.locks.contains(&TxLock::Key {
            key: key_path,
            typ: LockType::Write,
        }));
        let leaf = LeafRef::root(test_collection());
        assert!(record.locks.contains(&TxLock::Membership {
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
        let (stale, keys) = scan_accesses_for_range(
            &tctx,
            range.clone(),
            vec![wa(&logical_key(b"a"), b"updated")],
        )
        .await;
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);
        assert_eq!(stale.range_scans()[0].frontier(), Some(b"b".as_slice()));

        // Removing the old frontier means the refreshed two-key page reaches
        // into S1. The first locked validation only owns S0 and must retry.
        commit_writes(&tm, vec![wdel(&logical_key(b"b"))]).await;
        let mut handle = begin_accesses(&tm, stale);
        assert_eq!(
            tm.commit(&mut handle).await.unwrap(),
            BodyDecision::ReplayBody
        );

        // The body replays while S0 stays locked. Its new frontier is `m`, so
        // the next validation adds S1 before committing.
        let (fresh, keys) =
            scan_accesses_for_range(&tctx, range, vec![wa(&logical_key(b"a"), b"updated")]).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"m".to_vec()]);
        assert_eq!(fresh.range_scans()[0].frontier(), Some(b"m".as_slice()));
        tm.reset(&mut handle, fresh);
        tm.commit(&mut handle).await.unwrap();

        let record = tctx
            .tx_records
            .get_at(handle.id(), Requirement::ANY)
            .await
            .unwrap();
        let record = record.value().unwrap();
        for node_id in [test_node_id(0), test_node_id(1)] {
            assert!(record.locks.contains(&TxLock::Membership {
                leaf: LeafRef::node(test_collection(), node_id),
                typ: LockType::Read,
            }));
        }
        tm.end(&mut handle).await.unwrap();
    }

    // ADR-032 phantom prevention: a delete bumps the covered leaf's membership
    // generation, so an earlier scan fails re-validation.
    #[tokio::test]
    async fn scan_detects_racing_delete() {
        let (tm, tctx) = new_algo().await;
        let bp = logical_key(b"b");
        seed_live_keys(&tctx, &[b"a", b"b"]).await;

        let (accesses, keys) = scan_accesses(&tctx).await;
        assert_eq!(keys, vec![b"a".to_vec(), b"b".to_vec()]);

        commit_writes(&tm, vec![wdel(&bp)]).await;

        let mut stale = begin_accesses(&tm, accesses);
        assert_eq!(
            tm.commit(&mut stale).await.unwrap(),
            BodyDecision::ReplayBody
        );
    }

    // A pure split changes physical coverage but not logical membership. The
    // fallback re-resolves the page and accepts it without a false retry.
    #[tokio::test]
    async fn scan_accepts_concurrent_split_with_unchanged_membership() {
        let (tm, tctx) = new_algo().await;
        seed_live_keys(&tctx, &[b"a", b"m"]).await;

        let (accesses, _keys) = scan_accesses(&tctx).await;

        // Grow the tree in place: rewrite `_r` from its single leaf into an index
        // root pointing at two fresh leaves (the shape the background
        // restructurer produces), so the covered leaf set is no longer just `_r`.
        split_root_in_place(&tctx).await;

        let mut stable = begin_accesses(&tm, accesses);
        tm.commit(&mut stable).await.unwrap();
        tm.end(&mut stable).await.unwrap();
    }

    #[tokio::test]
    async fn locked_scan_uses_current_coverage_after_a_peer_split() {
        for insert in [false, true] {
            let (tm, tctx) =
                new_algo_from_backend_with_cache(Arc::new(MemoryBackend::new()), 1 << 20).await;
            commit_writes(
                &tm,
                vec![wa(&logical_key(b"a"), b"a0"), wa(&logical_key(b"m"), b"m0")],
            )
            .await;
            let (accesses, keys) = scan_accesses_for_range(
                &tctx,
                ScanRange::all(),
                vec![wa(&logical_key(b"a"), b"updated")],
            )
            .await;
            assert_eq!(keys, [b"a".to_vec(), b"m".to_vec()]);

            // The peer leaves this instance's cached root and the body's scan
            // coverage unchanged. Acquisition must discover both new leaves.
            let (peer, peer_ctx) = new_algo_from_backend(tctx.backend.clone()).await;
            split_root_in_place(&peer_ctx).await;
            if insert {
                commit_writes(&peer, vec![wa(&logical_key(b"z"), b"new")]).await;
            }

            let mut handle = begin_accesses(&tm, accesses);
            assert_eq!(
                tm.commit(&mut handle).await.unwrap(),
                if insert {
                    BodyDecision::ReplayBody
                } else {
                    BodyDecision::ReturnOutcome
                },
                "logical scan validation must distinguish a split from a phantom"
            );
            if !insert {
                let observed = tctx
                    .tx_records
                    .get_at(handle.id(), Requirement::ANY)
                    .await
                    .unwrap();
                let record = observed.value().unwrap();
                for node_id in [test_node_id(0), test_node_id(1)] {
                    assert!(record.locks.contains(&TxLock::Membership {
                        leaf: LeafRef::node(test_collection(), node_id),
                        typ: LockType::Read,
                    }));
                }
            }
            tm.end(&mut handle).await.unwrap();
            let (value, _) = read_outcome(&tctx, &logical_key(b"a")).await.into_parts();
            assert_eq!(
                value.unwrap().value.as_ref(),
                if insert {
                    b"a0".as_slice()
                } else {
                    b"updated".as_slice()
                }
            );
        }
    }

    // ADR-032 says a split is caught by the covered-leaf-set change, not by the
    // membership generation, which a split never bumps. The locked shortcut compares
    // the exact observed leaf state instead of that generation, and a split rewrites
    // the leaf it shrinks. So a create in the leaf a split produced — a leaf no
    // coverage entry names — still forces logical re-resolution of the page.
    #[tokio::test]
    async fn locked_scan_detects_a_create_in_a_leaf_a_peer_split_produced() {
        use glassdb_storage::Node;
        let (tm, tctx) = new_algo().await;
        let (_, s1_id) = seed_two_leaf_tree(&tctx).await;
        let s2_id = test_node_id(2);
        let seed = TxId::with_priority(1, b"seed");

        // The write makes commit validate the scan while holding its own locks.
        let (accesses, keys) = scan_accesses_for_range(
            &tctx,
            ScanRange::all(),
            vec![wa(&logical_key(b"a"), b"updated")],
        )
        .await;
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"c".to_vec(), b"m".to_vec(), b"p".to_vec()]
        );

        // A peer splits the covered leaf S1(m,p) into S1(m | high "p") -> S2(p).
        // S1 keeps its path, so it stays in this transaction's lock cover; only
        // its state changes. The parent separator may lag (ADR-031), because the
        // right-link carries the scan to S2 either way.
        let entry =
            |key: &[u8]| LeafEntry::new(key).with_current(CurrentState::External { writer: seed });
        tctx.nodes
            .store_node(
                &test_collection(),
                &s2_id,
                &Node::leaf(LeafBody::from_entries([entry(b"p")])),
                None,
            )
            .await
            .unwrap();
        let (_, s1_ver) = tctx
            .nodes
            .load_node(
                &test_collection(),
                &s1_id,
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        tctx.nodes
            .store_node(
                &test_collection(),
                &s1_id,
                &Node::leaf(LeafBody::from_entries([entry(b"m")]))
                    .with_high_key(Some(b"p".to_vec()))
                    .with_right_sibling(Some(s2_id)),
                Some(&s1_ver),
            )
            .await
            .unwrap();

        // A peer then creates `z` in S2. It bumps only S2's membership generation,
        // which no coverage entry of the earlier scan carries.
        let (s2, s2_ver) = tctx
            .nodes
            .load_node(
                &test_collection(),
                &s2_id,
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let mut entries: Vec<LeafEntry> = s2.as_leaf().unwrap().entries().cloned().collect();
        let creator = TxId::with_priority(2, b"phantom");
        entries.push(LeafEntry::new(b"z").with_current(CurrentState::External { writer: creator }));
        let mut new_s2 = Node::leaf(LeafBody::from_entries(entries));
        new_s2.set_membership_writer(creator);
        new_s2.remove_membership_holder(&creator);
        tctx.nodes
            .store_node(&test_collection(), &s2_id, &new_s2, Some(&s2_ver))
            .await
            .unwrap();

        let mut handle = begin_accesses(&tm, accesses);
        assert_eq!(
            tm.commit(&mut handle).await.unwrap(),
            BodyDecision::ReplayBody,
            "a create in the split's new leaf is a phantom the scan must not miss"
        );
        tm.end(&mut handle).await.unwrap();
    }

    // ADR-032 boundary protection: on a multi-leaf tree a full scan covers every
    // leaf including the endpoints, so a membership change in the final leaf
    // invalidates the scan.
    #[tokio::test]
    async fn scan_detects_boundary_membership_change() {
        let (tm, tctx) = new_algo().await;
        let (_, s1_id) = seed_two_leaf_tree(&tctx).await;

        let (accesses, keys) = scan_accesses(&tctx).await;
        assert_eq!(
            keys,
            vec![b"a".to_vec(), b"c".to_vec(), b"m".to_vec(), b"p".to_vec()]
        );

        // Append a key past the current maximum: it lands in the last covered
        // leaf S1, bumping its membership generation.
        let (s1, s1_observation) = tctx
            .nodes
            .load_node(
                &test_collection(),
                &s1_id,
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let mut entries: Vec<LeafEntry> = s1.as_leaf().unwrap().entries().cloned().collect();
        entries.push(LeafEntry::new(b"z").with_current(CurrentState::External {
            writer: TxId::with_priority(2, b"boundary"),
        }));
        let mut new_s1 = Node::leaf(LeafBody::from_entries(entries));
        let membership_writer = TxId::with_priority(2, b"membership");
        new_s1.set_membership_writer(membership_writer);
        new_s1.remove_membership_holder(&membership_writer);
        tctx.nodes
            .store_node(&test_collection(), &s1_id, &new_s1, Some(&s1_observation))
            .await
            .unwrap();

        let mut stale = begin_accesses(&tm, accesses);
        assert_eq!(
            tm.commit(&mut stale).await.unwrap(),
            BodyDecision::ReplayBody
        );
    }

    // Replaces the test collection's root leaf with a two-level tree: an index
    // root over S0(a,c | high "m") -> S1(m,p), chained by right-sibling.
    // Returns both leaf IDs.
    async fn seed_two_leaf_tree(tctx: &Tctx) -> (NodeId, NodeId) {
        use glassdb_storage::{IndexNode, Node};
        let s0_id = test_node_id(0);
        let s1_id = test_node_id(1);

        let leaf = |ks: &[&[u8]], high: Option<&[u8]>, right: Option<NodeId>| {
            Node::leaf(LeafBody::from_entries(ks.iter().map(|k| {
                LeafEntry::new(*k).with_current(CurrentState::External {
                    writer: TxId::with_priority(1, b"seed"),
                })
            })))
            .with_high_key(high.map(<[u8]>::to_vec))
            .with_right_sibling(right)
        };
        tctx.nodes
            .store_node(
                &test_collection(),
                &s0_id,
                &leaf(&[b"a", b"c"], Some(b"m"), Some(s1_id)),
                None,
            )
            .await
            .unwrap();
        tctx.nodes
            .store_node(
                &test_collection(),
                &s1_id,
                &leaf(&[b"m", b"p"], None, None),
                None,
            )
            .await
            .unwrap();
        let root = Node::index(IndexNode::from_children([
            (b"".to_vec(), s0_id),
            (b"m".to_vec(), s1_id),
        ]));
        let cur = tctx
            .nodes
            .load_leaf(
                &test_root_path(),
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        tctx.nodes
            .store_root(&test_collection(), &root, cur.observation())
            .await
            .unwrap();
        (s0_id, s1_id)
    }

    // Rewrites the test collection's root `_r` (a single leaf holding `a`,`m`)
    // into a two-level tree: an index root over leaf `S0` (a) and `S1` (m),
    // chained by right-sibling. A CAS on `_r` makes this the topology-growth
    // linearization point, mirroring the in-place root split (ADR-031).
    async fn split_root_in_place(tctx: &Tctx) {
        use glassdb_storage::{IndexNode, Node};
        let s0_id = test_node_id(0);
        let s1_id = test_node_id(1);

        let loaded = tctx
            .nodes
            .load_leaf(
                &test_root_path(),
                Requirement::after(tctx.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let entries: Vec<LeafEntry> = loaded.entries().entries().cloned().collect();
        let (lower, upper): (Vec<_>, Vec<_>) = entries
            .into_iter()
            .partition(|e| e.key.as_slice() < b"m".as_slice());

        let s0 = Node::leaf(LeafBody::from_entries(lower))
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some(s1_id));
        tctx.nodes
            .store_node(&test_collection(), &s0_id, &s0, None)
            .await
            .unwrap();
        let s1 = Node::leaf(LeafBody::from_entries(upper));
        tctx.nodes
            .store_node(&test_collection(), &s1_id, &s1, None)
            .await
            .unwrap();

        let root = Node::index(IndexNode::from_children([
            (b"".to_vec(), s0_id),
            (b"m".to_vec(), s1_id),
        ]));
        assert!(
            tctx.nodes
                .store_root(&test_collection(), &root, loaded.observation())
                .await
                .unwrap(),
            "root split CAS must win"
        );
    }
}
