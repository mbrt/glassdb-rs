//! Shared node-lock policy for leaf mutations and structural operations.
//!
//! The shard coordinator owns the shared transactional fold mechanics. This
//! module owns the wound-wait transitions applied to membership locks and the
//! full-node quiescing sequence required before a split closes the structural
//! gate.

use std::collections::BTreeMap;

use async_trait::async_trait;
use glassdb_data::{CollectionAddress, KeyRef, ObjectPath, TxId};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{LockType, NodeLocks, Requirement, ShardEntry};

use crate::error::TransError;
use crate::key_state_resolver::KeyStateResolver;
use crate::monitor::Monitor;
use crate::shard_coord::{FoldOutcome, ResolveCtx, ShardResolver, StageAdmission, Step};
use crate::wound_wait::{Reclaim, try_reclaim};

/// Wound-wait policy over one node's structural gate and membership lock.
pub(crate) struct NodeLockReconciler<'a> {
    key_state: &'a KeyStateResolver,
    monitor: &'a Monitor,
    id: &'a TxId,
}

impl<'a> NodeLockReconciler<'a> {
    pub(crate) fn new(key_state: &'a KeyStateResolver, monitor: &'a Monitor, id: &'a TxId) -> Self {
        Self {
            key_state,
            monitor,
            id,
        }
    }

    /// Resolves every entry and removes holders this structural operation can
    /// reclaim before closing the node's structural gate.
    pub(crate) async fn quiesce_entries(
        &self,
        collection: &CollectionAddress,
        entries: &BTreeMap<Vec<u8>, ShardEntry>,
        requirement: Requirement,
    ) -> Result<QuiescedEntries, TransError> {
        let mut resolved_entries = BTreeMap::new();
        for (key, entry) in entries {
            let resolved = self
                .key_state
                .resolve_holders(
                    &KeyRef::new(collection.clone(), key),
                    Some(entry),
                    Some(self.id),
                    requirement,
                )
                .await?;
            for holder in &resolved.pending {
                if self.monitor.tx_status(holder).await? == TxCommitStatus::Unknown {
                    return Ok(QuiescedEntries::Wait(holder.clone()));
                }
                if matches!(
                    try_reclaim(self.monitor, self.id, holder).await?,
                    Reclaim::Wait
                ) {
                    return Ok(QuiescedEntries::Wait(holder.clone()));
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
    /// A live gate has priority over new traffic. A finalized gate can be
    /// removed by this same CAS: if its structural write was still in flight,
    /// only one of the two conditional writes can land.
    pub(crate) async fn admit_non_structural(
        &self,
        locks: &mut NodeLocks,
    ) -> Result<Option<TxId>, TransError> {
        if let Some(holder) = self.reconcile_delete_intent(locks).await? {
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

    /// Admits a logless leaf rewrite without waiting for or wounding a live
    /// node-lock holder.
    ///
    /// Terminal holders can be removed by the commit CAS. Pending and unknown
    /// holders force the caller onto the regular locked protocol, whose
    /// lifecycle owns waiting and wound-wait (ADR-061).
    pub(crate) async fn admit_direct(
        &self,
        locks: &mut NodeLocks,
        changes_membership: bool,
    ) -> Result<Option<TxId>, TransError> {
        if let Some(holder) = locks.delete_intent().cloned() {
            match self.monitor.tx_status(&holder).await? {
                TxCommitStatus::Ok => return Err(TransError::StaleCollection),
                TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                    locks.remove_delete_intent(&holder);
                }
                TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                    return Ok(Some(holder));
                }
            }
        }

        for holder in locks.structural_gate().holders().to_vec() {
            match self.monitor.tx_status(&holder).await? {
                TxCommitStatus::Ok | TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                    locks.remove_structural_gate(&holder);
                }
                TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                    return Ok(Some(holder));
                }
            }
        }

        if changes_membership {
            for holder in locks.membership().holders().to_vec() {
                match self.monitor.tx_status(&holder).await? {
                    TxCommitStatus::Ok | TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                        locks.remove_membership_holder(&holder);
                    }
                    TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                        return Ok(Some(holder));
                    }
                }
            }
        }
        Ok(None)
    }

    /// Closes the structural gate after quiescing membership holders.
    ///
    /// Returns the live holder to wait for, or leaves both node-lock scopes free
    /// of finalized foreign holders with structure-write installed for this
    /// operation.
    pub(crate) async fn acquire_structural_gate(
        &self,
        locks: &mut NodeLocks,
    ) -> Result<Option<TxId>, TransError> {
        if let Some(holder) = self.reconcile_delete_intent(locks).await? {
            return Ok(Some(holder));
        }
        if locks.structural_gate().contains(self.id) {
            self.prune_finalized_membership(locks).await?;
            return Ok(None);
        }
        for holder in locks.structural_gate().holders().to_vec() {
            match self.monitor.tx_status(&holder).await? {
                TxCommitStatus::Pending => {
                    if matches!(
                        try_reclaim(self.monitor, self.id, &holder).await?,
                        Reclaim::Wait
                    ) {
                        return Ok(Some(holder));
                    }
                }
                TxCommitStatus::Unknown => return Ok(Some(holder)),
                TxCommitStatus::Ok | TxCommitStatus::Aborted | TxCommitStatus::Wounded => {}
            }
            locks.remove_structural_gate(&holder);
        }
        for holder in locks.membership().holders().to_vec() {
            if &holder == self.id {
                locks.remove_membership_holder(&holder);
                continue;
            }
            match self.monitor.tx_status(&holder).await? {
                TxCommitStatus::Pending => {
                    if matches!(
                        try_reclaim(self.monitor, self.id, &holder).await?,
                        Reclaim::Wait
                    ) {
                        return Ok(Some(holder));
                    }
                }
                TxCommitStatus::Unknown => return Ok(Some(holder)),
                TxCommitStatus::Ok | TxCommitStatus::Aborted | TxCommitStatus::Wounded => {}
            }
            locks.remove_membership_holder(&holder);
        }
        locks.set_structural_gate(self.id.clone());
        Ok(None)
    }

    async fn reconcile_delete_intent(
        &self,
        locks: &mut NodeLocks,
    ) -> Result<Option<TxId>, TransError> {
        let Some(holder) = locks.delete_intent().cloned() else {
            return Ok(None);
        };
        if &holder == self.id {
            return Ok(None);
        }
        match self.monitor.tx_status(&holder).await? {
            TxCommitStatus::Ok => Err(TransError::StaleCollection),
            TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                locks.remove_delete_intent(&holder);
                Ok(None)
            }
            TxCommitStatus::Pending => match try_reclaim(self.monitor, self.id, &holder).await? {
                Reclaim::Wounded => {
                    locks.remove_delete_intent(&holder);
                    Ok(None)
                }
                Reclaim::Wait => Ok(Some(holder)),
            },
            TxCommitStatus::Unknown => Ok(Some(holder)),
        }
    }

    /// Acquires the requested membership lock, returning a holder to wait for.
    pub(crate) async fn acquire_membership(
        &self,
        locks: &mut NodeLocks,
        desired: LockType,
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
        if conflicts {
            for holder in locks.membership().holders().to_vec() {
                if &holder == self.id {
                    continue;
                }
                match self.monitor.tx_status(&holder).await? {
                    TxCommitStatus::Pending => {
                        if matches!(
                            try_reclaim(self.monitor, self.id, &holder).await?,
                            Reclaim::Wait
                        ) {
                            return Ok(Some(holder));
                        }
                    }
                    TxCommitStatus::Unknown => return Ok(Some(holder)),
                    TxCommitStatus::Ok | TxCommitStatus::Aborted | TxCommitStatus::Wounded => {}
                }
                locks.remove_membership_holder(&holder);
            }
        }
        match desired {
            LockType::Read if locks.membership().lock_type() != LockType::Write => {
                locks.add_membership_reader(self.id.clone());
            }
            LockType::Write
                if locks.membership().lock_type() != LockType::Write
                    || !locks.membership().contains(self.id) =>
            {
                locks.set_membership_writer(self.id.clone());
            }
            _ => {}
        }
        Ok(None)
    }

    /// Removes finalized membership holders after their entry state was
    /// reconciled. Unknown holders remain live until the monitor classifies
    /// them through its missing-transaction grace period.
    async fn prune_finalized_membership(&self, locks: &mut NodeLocks) -> Result<(), TransError> {
        for holder in locks.membership().holders().to_vec() {
            if &holder != self.id && self.monitor.tx_status(&holder).await?.is_final() {
                locks.remove_membership_holder(&holder);
            }
        }
        Ok(())
    }
}

/// The leaf-coordinator resolver for structural-gate acquisition.
pub(crate) struct StructuralGateResolver {
    id: TxId,
    path: ObjectPath,
}

impl StructuralGateResolver {
    pub(crate) fn new(id: TxId, path: ObjectPath) -> Self {
        Self { id, path }
    }
}

#[async_trait]
impl ShardResolver for StructuralGateResolver {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let collection = match &self.path {
            ObjectPath::TreeRoot { collection } | ObjectPath::Node { collection, .. } => {
                collection.clone()
            }
            _ => return Err(TransError::other("structural gate target is not a leaf")),
        };
        let reconciler = NodeLockReconciler::new(ctx.key_state, ctx.tmon, &self.id);
        let entries = match reconciler
            .quiesce_entries(&collection, staged, ctx.requirement)
            .await?
        {
            QuiescedEntries::Ready(entries) => entries,
            QuiescedEntries::Wait(holder) => {
                return Ok(Step::Skip {
                    outcome: FoldOutcome::Wait(holder),
                });
            }
        };
        let mut locks = staged_locks.clone();
        if let Some(holder) = reconciler.acquire_structural_gate(&mut locks).await? {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Wait(holder),
            });
        }
        let entries = entries
            .into_iter()
            .filter(|(key, entry)| staged.get(key) != Some(entry))
            .collect();
        Ok(Step::Stage {
            entries,
            locks,
            admission: StageAdmission::ExistingKeys,
            outcome: FoldOutcome::Locked {
                typ: LockType::Write,
                membership: LockType::None,
            },
        })
    }

    fn reorderable(&self) -> bool {
        false
    }

    fn exhausted_outcome(&self, _in_doubt: bool) -> FoldOutcome {
        FoldOutcome::Conflict
    }
}

/// Result of reconciling all entry holders before gate installation.
pub(crate) enum QuiescedEntries {
    Ready(BTreeMap<Vec<u8>, ShardEntry>),
    Wait(TxId),
}
