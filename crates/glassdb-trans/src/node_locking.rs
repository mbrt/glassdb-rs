//! Shared node-lock policy for leaf mutations and structural operations.
//!
//! The leaf coordinator owns the shared transaction mutation protocol. This
//! module owns the wound-wait transitions applied to membership locks and the
//! full-node quiescing sequence required before a split or a merge closes the
//! structural gate.

use std::collections::BTreeMap;
use std::num::NonZeroUsize;

use async_trait::async_trait;
use glassdb_concurr::map_all_bounded;
use glassdb_data::{CollectionAddress, LogicalKey, ObjectPath, TxId};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{CurrentState, LeafEntry, LeafObservation, LockType, NodeLocks, Requirement};

use crate::error::TransError;
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::{
    CoordinatedOutcome, LeafOperation, MemberOutcome, MemberPolicy, ResolveCtx, StageAdmission,
    Step,
};
use crate::monitor::Monitor;
use crate::wound_wait::{Reclaim, try_reclaim};

/// The maximum concurrent transaction-record operations to resolve the
/// holders of one node lock. A membership read lock can collect many holders
/// with a final status, and they are resolved while a leaf CAS waits.
const HOLDER_RESOLUTION_PARALLELISM: NonZeroUsize = NonZeroUsize::new(16).unwrap();

/// Wound-wait policy over one node's structural gate and membership lock.
pub(crate) struct NodeLockReconciler<'a> {
    key_state: &'a KeyStateResolver,
    monitor: &'a Monitor,
    id: &'a TxId,
    acquisition: GateAcquisition,
}

/// The staged entries of one leaf, as seen by a mutation deciding on its locks.
pub(crate) struct LeafState<'a> {
    pub(crate) collection: &'a CollectionAddress,
    pub(crate) entries: &'a BTreeMap<Vec<u8>, LeafEntry>,
    pub(crate) requirement: Requirement,
}

/// How an operation treats live holders of the node locks that block it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GateAcquisition {
    /// Wounds younger holders and waits for older ones (ADR-044).
    WoundWait,
    /// Neither waits nor wounds: any live holder stops the operation. Merges
    /// are optional maintenance and must not abort transactions (ADR-073). A
    /// direct commit leaves live holders to the locked commit (ADR-061).
    Polite,
}

/// What keeps a structural gate from being installed.
pub(crate) enum GateBlocker {
    /// A live holder that the operation must wait for.
    Holder(TxId),
    /// A merge reservation, which only its structural intent can remove
    /// (ADR-073).
    MergeReservation,
}

impl<'a> NodeLockReconciler<'a> {
    pub(crate) fn new(key_state: &'a KeyStateResolver, monitor: &'a Monitor, id: &'a TxId) -> Self {
        Self::with_acquisition(key_state, monitor, id, GateAcquisition::WoundWait)
    }

    pub(crate) fn with_acquisition(
        key_state: &'a KeyStateResolver,
        monitor: &'a Monitor,
        id: &'a TxId,
        acquisition: GateAcquisition,
    ) -> Self {
        Self {
            key_state,
            monitor,
            id,
            acquisition,
        }
    }

    /// Quiesces one node and closes its structural gate: help-forwards or
    /// removes each foreign holder of its entries and node locks.
    ///
    /// The entries and the locks change together, because a membership writer
    /// leaves the membership lock only with its key locks (ADR-077). `entries`
    /// is `None` for an index node.
    ///
    /// The result is a candidate for CAS against the source observation, not
    /// proof that the stored node is quiescent. With `ANY`, stale Pending or
    /// Unknown status must lead to waiting or a durable wound; final decisions
    /// are stable.
    pub(crate) async fn gate_node(
        &self,
        leaf: Option<&LeafState<'_>>,
        locks: &NodeLocks,
    ) -> Result<GatedNode, TransError> {
        let entries = match leaf {
            Some(leaf) => match self
                .quiesce_entries(leaf.collection, leaf.entries, leaf.requirement)
                .await?
            {
                QuiescedEntries::Ready(entries) => Some(entries),
                QuiescedEntries::Wait(holder) => {
                    return Ok(GatedNode::Blocked(GateBlocker::Holder(holder)));
                }
            },
            None => None,
        };
        let mut locks = locks.clone();
        if let Some(blocker) = self.acquire_structural_gate(&mut locks).await? {
            return Ok(GatedNode::Blocked(blocker));
        }
        Ok(GatedNode::Gated { entries, locks })
    }

    /// Resolves every entry and removes holders this structural operation can
    /// reclaim before closing the node's structural gate.
    async fn quiesce_entries(
        &self,
        collection: &CollectionAddress,
        entries: &BTreeMap<Vec<u8>, LeafEntry>,
        requirement: Requirement,
    ) -> Result<QuiescedEntries, TransError> {
        let mut resolved_entries = BTreeMap::new();
        for (key, entry) in entries {
            let resolved = self
                .key_state
                .resolve_holders(
                    &LogicalKey::new(collection.clone(), key),
                    Some(entry),
                    Some(self.id),
                    requirement,
                )
                .await?;
            for holder in &resolved.pending {
                if self.monitor.tx_status(holder).await? == TxCommitStatus::Unknown {
                    return Ok(QuiescedEntries::Wait(*holder));
                }
                if matches!(self.reclaim(holder).await?, Reclaim::Wait) {
                    return Ok(QuiescedEntries::Wait(*holder));
                }
            }
            let mut quiesced = entry.clone();
            quiesced.current = resolved.resolved_current(Some(entry));
            for holder in quiesced.lock_holders().to_vec() {
                if &holder != self.id {
                    quiesced.release_lock(&holder);
                }
            }
            resolved_entries.insert(key.clone(), quiesced);
        }
        Ok(QuiescedEntries::Ready(resolved_entries))
    }

    /// Admits an ordinary node rewrite by proving the gate absent in the state
    /// that will be conditionally replaced.
    ///
    /// A live gate has priority over new traffic. A gate whose holder has a final status can be
    /// removed by this same CAS: if its structural write was still in flight,
    /// only one of the two CASes can land.
    pub(crate) async fn admit_non_structural(
        &self,
        locks: &mut NodeLocks,
    ) -> Result<Option<TxId>, TransError> {
        if let Some(holder) = self.reconcile_drop_intent(locks).await? {
            return Ok(Some(holder));
        }
        let Some(holder) = locks.structural_gate().holders().first().cloned() else {
            return Ok(None);
        };
        if &holder == self.id {
            return Ok(Some(holder));
        }
        if self.monitor.tx_status(&holder).await?.is_final() {
            locks.remove_structural_gate(&holder);
            return Ok(None);
        }
        Ok(Some(holder))
    }

    /// Closes the structural gate after quiescing membership holders. The
    /// quiesced entries of [`Self::gate_node`] have no key lock of the
    /// membership holders removed here.
    ///
    /// Returns what blocks the gate, or leaves both node-lock scopes free of
    /// foreign holders with a final status, with a structural gate installed
    /// for this operation.
    async fn acquire_structural_gate(
        &self,
        locks: &mut NodeLocks,
    ) -> Result<Option<GateBlocker>, TransError> {
        if let Some(holder) = self.reconcile_drop_intent(locks).await? {
            return Ok(Some(GateBlocker::Holder(holder)));
        }
        if locks.merge_reservation().is_some() {
            return Ok(Some(GateBlocker::MergeReservation));
        }
        if locks.structural_gate().contains(self.id) {
            self.prune_final_membership(locks).await?;
            return Ok(None);
        }
        for holder in locks.structural_gate().holders().to_vec() {
            match self.monitor.tx_status(&holder).await? {
                TxCommitStatus::Pending => {
                    if matches!(self.reclaim(&holder).await?, Reclaim::Wait) {
                        return Ok(Some(GateBlocker::Holder(holder)));
                    }
                }
                TxCommitStatus::Unknown => return Ok(Some(GateBlocker::Holder(holder))),
                TxCommitStatus::Committed | TxCommitStatus::Aborted | TxCommitStatus::Wounded => {}
            }
            locks.remove_structural_gate(&holder);
        }
        locks.remove_membership_holder(self.id);
        let holders = self.foreign_membership_holders(locks);
        if let Reclaimed::Wait(holder) = self.reclaim_holders(&holders).await? {
            return Ok(Some(GateBlocker::Holder(holder)));
        }
        for holder in &holders {
            locks.remove_membership_holder(holder);
        }
        locks.set_structural_gate(*self.id);
        Ok(None)
    }

    async fn reconcile_drop_intent(
        &self,
        locks: &mut NodeLocks,
    ) -> Result<Option<TxId>, TransError> {
        let Some(holder) = locks.drop_intent().cloned() else {
            return Ok(None);
        };
        if &holder == self.id {
            return Ok(None);
        }
        match self.monitor.tx_status(&holder).await? {
            TxCommitStatus::Committed => Err(TransError::StaleCollection),
            TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                locks.remove_drop_intent(&holder);
                Ok(None)
            }
            TxCommitStatus::Pending => match self.reclaim(&holder).await? {
                Reclaim::Wounded => {
                    locks.remove_drop_intent(&holder);
                    Ok(None)
                }
                Reclaim::Wait => Ok(Some(holder)),
            },
            TxCommitStatus::Unknown => Ok(Some(holder)),
        }
    }

    /// Acquires the requested membership lock, returning a holder to wait for.
    ///
    /// A membership writer that this removes appends the release of its key
    /// locks to `changes` (see [`Self::remove_membership_writer`]).
    pub(crate) async fn acquire_membership(
        &self,
        locks: &mut NodeLocks,
        desired: LockType,
        leaf: &LeafState<'_>,
        changes: &mut Vec<(Vec<u8>, LeafEntry)>,
    ) -> Result<Option<TxId>, TransError> {
        let conflicts = match desired {
            LockType::Read => {
                locks.membership().lock_type() == LockType::Write
                    && !locks.membership().contains(self.id)
            }
            LockType::Write => {
                locks.membership().lock_type() != LockType::Write
                    || !locks.membership().contains(self.id)
            }
            _ => false,
        };
        if conflicts && let Some(holder) = self.clear_membership(locks, leaf, changes).await? {
            return Ok(Some(holder));
        }
        match desired {
            LockType::Read if locks.membership().lock_type() != LockType::Write => {
                self.prune_known_final_readers(locks);
                locks.add_membership_reader(*self.id);
            }
            LockType::Write
                if locks.membership().lock_type() != LockType::Write
                    || !locks.membership().contains(self.id) =>
            {
                locks.set_membership_writer(*self.id);
            }
            _ => {}
        }
        Ok(None)
    }

    /// Admits a publication that takes no node lock, returning a holder that
    /// blocks it.
    ///
    /// It removes holders with a final status from the node locks. When the
    /// publication changes the key membership, the membership lock must have
    /// no live holder, and a membership writer that this removes appends the
    /// release of its key locks to `changes`. Otherwise, the publication does
    /// not wait for readers, and its own CAS removes the ones known to be
    /// final.
    pub(crate) async fn admit_publication(
        &self,
        locks: &mut NodeLocks,
        leaf: &LeafState<'_>,
        changes_membership: bool,
        changes: &mut Vec<(Vec<u8>, LeafEntry)>,
    ) -> Result<Option<TxId>, TransError> {
        if let Some(holder) = self.admit_non_structural(locks).await? {
            return Ok(Some(holder));
        }
        if changes_membership {
            return self.clear_membership(locks, leaf, changes).await;
        }
        self.prune_known_final_readers(locks);
        Ok(None)
    }

    /// Removes `holder`, which has a final status, from the membership lock in
    /// `locks`, and appends to `changes` the release of its write locks and
    /// create locks in `leaf`. A committed holder is help-forwarded on these
    /// keys. Keys already in `changes` no longer name the holder.
    ///
    /// A holder leaves the membership write lock only together with these key
    /// locks, so that a key lock outside the membership lock never changes the
    /// key membership (ADR-077). The leaf write rejects any other removal.
    async fn remove_membership_writer(
        &self,
        holder: &TxId,
        status: TxCommitStatus,
        locks: &mut NodeLocks,
        leaf: &LeafState<'_>,
        changes: &mut Vec<(Vec<u8>, LeafEntry)>,
    ) -> Result<(), TransError> {
        debug_assert!(status.is_final(), "{holder} is not final: {status:?}");
        let exclusive = locks.membership().lock_type() == LockType::Write;
        if exclusive {
            for (key, entry) in leaf.entries {
                if !matches!(entry.lock_type(), LockType::Write | LockType::Create)
                    || !entry.is_locked_by(holder)
                    || changes.iter().any(|(staged, _)| staged == key)
                {
                    continue;
                }
                let mut released = entry.clone();
                if status == TxCommitStatus::Committed {
                    released.current = self.committed_current(holder, key, entry, leaf).await?;
                }
                released.release_lock(holder);
                changes.push((key.clone(), released));
            }
        }
        locks.remove_membership_holder(holder);
        Ok(())
    }

    /// Reads the status of each holder, in order, with bounded parallelism.
    async fn holder_statuses(&self, holders: &[TxId]) -> Result<Vec<TxCommitStatus>, TransError> {
        let monitor = self.monitor;
        map_all_bounded(
            holders.iter().copied(),
            HOLDER_RESOLUTION_PARALLELISM,
            |holder| async move { monitor.tx_status(&holder).await },
        )
        .await
        .into_iter()
        .collect()
    }

    /// Returns the current state that the committed `holder` leaves in `entry`.
    async fn committed_current(
        &self,
        holder: &TxId,
        key: &[u8],
        entry: &LeafEntry,
        leaf: &LeafState<'_>,
    ) -> Result<CurrentState, TransError> {
        let resolved = self
            .key_state
            .resolve_holders(
                &LogicalKey::new(leaf.collection.clone(), key),
                Some(entry),
                Some(self.id),
                leaf.requirement,
            )
            .await?;
        // A committed status is final, so a pending result means the holder
        // was not the one resolved. Releasing its lock then would lose its
        // write.
        if !resolved.pending.is_empty() {
            return Err(TransError::other(format!(
                "committed membership holder {holder} resolved as pending"
            )));
        }
        Ok(resolved.resolved_current(Some(entry)))
    }

    /// Removes membership holders with a final status after their entry state was
    /// reconciled. Unknown holders remain live until the monitor classifies
    /// them through its missing-transaction grace period.
    async fn prune_final_membership(&self, locks: &mut NodeLocks) -> Result<(), TransError> {
        let holders = self.foreign_membership_holders(locks);
        let statuses = self.holder_statuses(&holders).await?;
        for (holder, status) in holders.iter().zip(statuses) {
            if status.is_final() {
                locks.remove_membership_holder(holder);
            }
        }
        Ok(())
    }

    /// Removes the shared membership holders whose final status this database
    /// instance already knows, without backend I/O. A shared holder cannot
    /// change the key membership, so its removal needs no help-forward. The
    /// holders stay otherwise, so that a reader does not resolve the holders of
    /// other readers, but the CAS that adds this reader also removes stale ones.
    fn prune_known_final_readers(&self, locks: &mut NodeLocks) {
        if locks.membership().lock_type() != LockType::Read {
            return;
        }
        for holder in self.foreign_membership_holders(locks) {
            if self.monitor.known_final_status(&holder).is_some() {
                locks.remove_membership_holder(&holder);
            }
        }
    }

    fn foreign_membership_holders(&self, locks: &NodeLocks) -> Vec<TxId> {
        locks
            .membership()
            .holders()
            .iter()
            .filter(|holder| *holder != self.id)
            .copied()
            .collect()
    }

    /// Removes every foreign membership holder, reclaiming live ones under
    /// [`GateAcquisition`], or returns a holder to wait for.
    async fn clear_membership(
        &self,
        locks: &mut NodeLocks,
        leaf: &LeafState<'_>,
        changes: &mut Vec<(Vec<u8>, LeafEntry)>,
    ) -> Result<Option<TxId>, TransError> {
        let holders = self.foreign_membership_holders(locks);
        let statuses = match self.reclaim_holders(&holders).await? {
            Reclaimed::Final(statuses) => statuses,
            Reclaimed::Wait(holder) => return Ok(Some(holder)),
        };
        for (holder, status) in holders.iter().zip(statuses) {
            self.remove_membership_writer(holder, status, locks, leaf, changes)
                .await?;
        }
        Ok(None)
    }

    /// Gives every holder a final status, or returns a holder to wait for.
    ///
    /// It does not wound live holders when it must wait for another one,
    /// because the wait makes the caller resolve all holders again. A status
    /// read can still wound a holder whose lease expired.
    async fn reclaim_holders(&self, holders: &[TxId]) -> Result<Reclaimed, TransError> {
        let mut statuses = self.holder_statuses(holders).await?;
        let mut pending = Vec::new();
        for (index, (holder, status)) in holders.iter().zip(&statuses).enumerate() {
            match status {
                TxCommitStatus::Unknown => return Ok(Reclaimed::Wait(*holder)),
                TxCommitStatus::Pending if !self.may_wound(holder) => {
                    return Ok(Reclaimed::Wait(*holder));
                }
                TxCommitStatus::Pending => pending.push((index, *holder)),
                TxCommitStatus::Committed | TxCommitStatus::Aborted | TxCommitStatus::Wounded => {}
            }
        }
        let reclaimed = map_all_bounded(
            pending,
            HOLDER_RESOLUTION_PARALLELISM,
            |(index, holder)| async move { (index, holder, self.reclaim(&holder).await) },
        )
        .await;
        for (index, holder, reclaimed) in reclaimed {
            match reclaimed? {
                // The holder committed before the wound landed.
                Reclaim::Wait => return Ok(Reclaimed::Wait(holder)),
                Reclaim::Wounded => statuses[index] = TxCommitStatus::Wounded,
            }
        }
        Ok(Reclaimed::Final(statuses))
    }

    fn may_wound(&self, holder: &TxId) -> bool {
        self.acquisition == GateAcquisition::WoundWait && self.id.older(holder)
    }

    async fn reclaim(&self, holder: &TxId) -> Result<Reclaim, TransError> {
        match self.acquisition {
            GateAcquisition::WoundWait => try_reclaim(self.monitor, self.id, holder).await,
            GateAcquisition::Polite => Ok(Reclaim::Wait),
        }
    }
}

/// Acquires a leaf structural gate through the shared leaf-mutation engine.
pub(crate) struct StructuralGateOperation {
    id: TxId,
    path: ObjectPath,
    acquisition: GateAcquisition,
}

/// Result of one coordinated structural-gate acquisition attempt.
pub(crate) enum StructuralGateOutcome {
    /// The exact state in which this operation acquired the gate.
    Acquired(LeafObservation),
    /// The gate did not land in this attempt.
    Deferred,
}

impl StructuralGateOperation {
    pub(crate) fn new(id: TxId, path: ObjectPath, acquisition: GateAcquisition) -> Self {
        Self {
            id,
            path,
            acquisition,
        }
    }
}

#[async_trait]
impl MemberPolicy for StructuralGateOperation {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, LeafEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let reconciler = NodeLockReconciler::with_acquisition(
            ctx.key_state,
            ctx.tmon,
            &self.id,
            self.acquisition,
        );
        let leaf = LeafState {
            collection: leaf_collection(&self.path)?,
            entries: staged,
            requirement: ctx.requirement,
        };
        let (entries, locks) = match reconciler.gate_node(Some(&leaf), staged_locks).await? {
            GatedNode::Gated { entries, locks } => (entries.unwrap_or_default(), locks),
            GatedNode::Blocked(GateBlocker::Holder(holder)) => {
                return Ok(Step::Skip {
                    outcome: MemberOutcome::Wait(holder),
                });
            }
            GatedNode::Blocked(GateBlocker::MergeReservation) => {
                return Ok(Step::Skip {
                    outcome: MemberOutcome::Conflict,
                });
            }
        };
        let entries = entries
            .into_iter()
            .filter(|(key, entry)| staged.get(key) != Some(entry))
            .collect();
        Ok(Step::Stage {
            entries,
            locks,
            admission: StageAdmission::ExistingKeys,
            outcome: MemberOutcome::Locked {
                typ: LockType::Write,
                membership: LockType::None,
            },
        })
    }

    fn reorderable(&self) -> bool {
        false
    }

    // The gate is for a structural change, not for a transaction.
    fn delays_transaction(&self) -> bool {
        false
    }

    fn exhausted_outcome(&self, _in_doubt: bool) -> MemberOutcome {
        MemberOutcome::Conflict
    }
}

impl LeafOperation for StructuralGateOperation {
    type Output = StructuralGateOutcome;

    fn path(&self) -> &ObjectPath {
        &self.path
    }

    fn id(&self) -> &TxId {
        &self.id
    }

    fn requirement(&self) -> Requirement {
        Requirement::ANY
    }

    fn complete(&self, outcome: Option<CoordinatedOutcome>) -> Result<Self::Output, TransError> {
        let Some(CoordinatedOutcome {
            outcome:
                MemberOutcome::Locked {
                    typ: LockType::Write,
                    ..
                },
            evidence,
        }) = outcome
        else {
            return Ok(StructuralGateOutcome::Deferred);
        };
        let evidence =
            evidence.ok_or_else(|| TransError::other("acquired gate has no evidence"))?;
        Ok(StructuralGateOutcome::Acquired(evidence.into_observation()))
    }
}

/// Returns the collection of a leaf path.
pub(crate) fn leaf_collection(path: &ObjectPath) -> Result<&CollectionAddress, TransError> {
    match path {
        ObjectPath::TreeRoot { collection } | ObjectPath::Node { collection, .. } => Ok(collection),
        _ => Err(TransError::other("lock target is not a leaf")),
    }
}

/// Result of giving every holder of a node lock a final status.
enum Reclaimed {
    /// The final status of each holder, in order.
    Final(Vec<TxCommitStatus>),
    /// A live holder that the operation must wait for.
    Wait(TxId),
}

/// Result of reconciling all entry holders before gate installation.
enum QuiescedEntries {
    Ready(BTreeMap<Vec<u8>, LeafEntry>),
    Wait(TxId),
}

/// A node state with a structural gate, ready for one CAS.
pub(crate) enum GatedNode {
    /// The quiesced entries of a leaf (`None` for an index node) and the
    /// node locks with the gate. They must land together.
    Gated {
        entries: Option<BTreeMap<Vec<u8>, LeafEntry>>,
        locks: NodeLocks,
    },
    Blocked(GateBlocker),
}
