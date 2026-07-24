//! Shared node-lock policy for leaf mutations and structural operations.
//!
//! The shard coordinator is deliberately policy-free. This module owns the
//! wound-wait transitions applied to membership locks and the full-node
//! quiescing sequence required before a split closes the structural gate.

use std::collections::BTreeMap;

use async_trait::async_trait;
use glassdb_data::{CollectionAddress, KeyRef, LeafRef, TxId};
use glassdb_storage::{LockType, NodeLocks, Requirement, ShardEntry, TxCommitStatus};

use crate::error::TransError;
use crate::monitor::Monitor;
use crate::resolver::Resolver;
use crate::shard_coord::{FoldOutcome, ResolveCtx, ShardResolver, StageAdmission, Step};

/// Result of applying wound-wait to one live holder.
pub(crate) enum Reclaim {
    Wounded,
    Wait,
}

/// Entry state needed by lock coordination after writer resolution and holder
/// liveness classification.
pub(crate) struct LockResolution {
    pub(crate) writer: Option<TxId>,
    pub(crate) deleted: bool,
    pub(crate) pending: Vec<TxId>,
}

/// Resolves an entry's writer and classifies the foreign holders that remain
/// live for lock coordination.
pub(crate) async fn resolve_entry_locks_at(
    resolver: &Resolver,
    monitor: &Monitor,
    key: &KeyRef,
    entry: Option<&ShardEntry>,
    own_lock_holder: Option<&TxId>,
    requirement: Requirement,
) -> Result<LockResolution, TransError> {
    let Some(entry) = entry else {
        return Ok(LockResolution {
            writer: None,
            deleted: false,
            pending: Vec::new(),
        });
    };
    let exclusive = matches!(entry.lock_type, LockType::Write | LockType::Create);
    if exclusive && entry.locked_by.len() > 1 {
        return Err(TransError::other(
            "exclusive shard entry has more than one holder",
        ));
    }
    let own_exclusive = exclusive
        && own_lock_holder.is_some_and(|id| entry.locked_by.iter().any(|holder| holder == id));
    let writer = if own_exclusive {
        entry.current_writer.clone()
    } else {
        resolver
            .resolve_writer_at(key, Some(entry), requirement)
            .await?
            .writer
    };
    let deleted = match &writer {
        None => false,
        Some(writer) if entry.current_writer.as_ref() == Some(writer) => entry.deleted,
        Some(writer) => {
            let value = monitor.committed_value_at(key, writer, requirement).await?;
            value.status == TxCommitStatus::Ok && !value.value.not_written && value.value.deleted
        }
    };
    let mut pending = Vec::new();
    for holder in &entry.locked_by {
        if Some(holder) == own_lock_holder {
            continue;
        }
        if matches!(
            monitor.tx_status_at(holder, requirement).await?,
            TxCommitStatus::Pending | TxCommitStatus::Unknown
        ) {
            pending.push(holder.clone());
        }
    }
    Ok(LockResolution {
        writer,
        deleted,
        pending,
    })
}

/// Resolves entry lock state using the coordination round's evidence bound.
pub(crate) async fn resolve_entry_locks(
    ctx: &ResolveCtx<'_>,
    key: &KeyRef,
    entry: Option<&ShardEntry>,
    own_lock_holder: Option<&TxId>,
) -> Result<LockResolution, TransError> {
    resolve_entry_locks_at(
        ctx.resolver,
        ctx.tmon,
        key,
        entry,
        own_lock_holder,
        ctx.requirement,
    )
    .await
}

/// Applies the transaction priority rule to one pending lock holder.
pub(crate) async fn try_reclaim(
    monitor: &Monitor,
    id: &TxId,
    holder: &TxId,
) -> Result<Reclaim, TransError> {
    if !id.older(holder) {
        return Ok(Reclaim::Wait);
    }
    monitor.wound_tx(holder).await?;
    if monitor.tx_status(holder).await? == TxCommitStatus::Aborted {
        Ok(Reclaim::Wounded)
    } else {
        Ok(Reclaim::Wait)
    }
}

/// Wound-wait policy over one node's structural gate and membership lock.
pub(crate) struct NodeLockReconciler<'a> {
    monitor: &'a Monitor,
    id: &'a TxId,
}

impl<'a> NodeLockReconciler<'a> {
    pub(crate) fn new(monitor: &'a Monitor, id: &'a TxId) -> Self {
        Self { monitor, id }
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

    /// Closes the structural gate after quiescing membership holders.
    ///
    /// Returns the live holder to wait for, or leaves both node-lock scopes free
    /// of finalized foreign holders with structure-write installed for this
    /// operation.
    pub(crate) async fn acquire_structural_gate(
        &self,
        locks: &mut NodeLocks,
    ) -> Result<Option<TxId>, TransError> {
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
                TxCommitStatus::Ok | TxCommitStatus::Aborted => {}
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
                TxCommitStatus::Ok | TxCommitStatus::Aborted => {}
            }
            locks.remove_membership_holder(&holder);
        }
        locks.set_structural_gate(self.id.clone());
        Ok(None)
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
                    TxCommitStatus::Ok | TxCommitStatus::Aborted => {}
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
    path: String,
}

impl StructuralGateResolver {
    pub(crate) fn new(id: TxId, path: String) -> Self {
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
        let collection = LeafRef::from_physical_path(&self.path)
            .map_err(|e| TransError::with_source("parsing leaf path", e))?
            .collection()
            .clone();
        let entries = match quiesce_entries(
            ctx.resolver,
            ctx.tmon,
            &collection,
            &self.id,
            staged,
            ctx.requirement,
        )
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
        let reconciler = NodeLockReconciler::new(ctx.tmon, &self.id);
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

/// Resolves every entry and removes all holders that the structural operation
/// successfully wounds.
pub(crate) async fn quiesce_entries(
    resolver: &Resolver,
    monitor: &Monitor,
    collection: &CollectionAddress,
    id: &TxId,
    entries: &BTreeMap<Vec<u8>, ShardEntry>,
    requirement: Requirement,
) -> Result<QuiescedEntries, TransError> {
    let mut resolved_entries = BTreeMap::new();
    for (key, entry) in entries {
        let resolved = resolve_entry_locks_at(
            resolver,
            monitor,
            &KeyRef::new(collection.clone(), key),
            Some(entry),
            Some(id),
            requirement,
        )
        .await?;
        for holder in &resolved.pending {
            if monitor.tx_status(holder).await? == TxCommitStatus::Unknown {
                return Ok(QuiescedEntries::Wait(holder.clone()));
            }
            if matches!(try_reclaim(monitor, id, holder).await?, Reclaim::Wait) {
                return Ok(QuiescedEntries::Wait(holder.clone()));
            }
        }
        let mut entry = entry.clone();
        entry.current_writer = resolved.writer;
        entry.deleted = resolved.deleted;
        entry.locked_by.retain(|holder| holder == id);
        if entry.locked_by.is_empty() {
            entry.lock_type = LockType::None;
        }
        resolved_entries.insert(key.clone(), entry);
    }
    Ok(QuiescedEntries::Ready(resolved_entries))
}
