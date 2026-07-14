//! Shared node-lock policy for leaf mutations and structural operations.
//!
//! The shard coordinator is deliberately policy-free. This module owns the
//! wound-wait transitions applied to the node-level structure and membership
//! locks, including the leaf-quiescing sequence required before a split takes
//! structure-write.

use std::collections::BTreeMap;

use async_trait::async_trait;
use glassdb_data::{TxId, paths};
use glassdb_storage::{LockType, NodeLocks, ShardEntry, TxCommitStatus};

use crate::error::TransError;
use crate::monitor::Monitor;
use crate::shard_coord::{FoldOutcome, ResolveCtx, ShardResolver, Step};

/// Result of applying wound-wait to one live holder.
pub(crate) enum Reclaim {
    Wounded,
    Wait,
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

/// Wound-wait policy over one node's structure and membership locks.
pub(crate) struct NodeLockReconciler<'a> {
    monitor: &'a Monitor,
    id: &'a TxId,
}

impl<'a> NodeLockReconciler<'a> {
    pub(crate) fn new(monitor: &'a Monitor, id: &'a TxId) -> Self {
        Self { monitor, id }
    }

    /// Removes finalized structure and membership holders.
    async fn prune_finalized(&self, locks: &mut NodeLocks) -> Result<(), TransError> {
        for holder in locks.structure().holders().to_vec() {
            if &holder != self.id
                && self.monitor.tx_status(&holder).await? != TxCommitStatus::Pending
            {
                locks.remove_structure_holder(&holder);
            }
        }
        self.prune_finalized_membership(locks).await
    }

    /// Removes finalized membership holders after a structure-holder wound.
    async fn prune_finalized_membership(&self, locks: &mut NodeLocks) -> Result<(), TransError> {
        for holder in locks.membership().holders().to_vec() {
            if &holder != self.id
                && self.monitor.tx_status(&holder).await? != TxCommitStatus::Pending
            {
                locks.remove_membership_holder(&holder);
            }
        }
        Ok(())
    }

    /// Acquires shared structure protection, returning the holder to wait for.
    async fn acquire_structure_read(
        &self,
        locks: &mut NodeLocks,
    ) -> Result<Option<TxId>, TransError> {
        if locks.structure().lock_type() == LockType::Write && !locks.structure().contains(self.id)
        {
            for holder in locks.structure().holders().to_vec() {
                if &holder == self.id {
                    continue;
                }
                if self.monitor.tx_status(&holder).await? == TxCommitStatus::Pending
                    && matches!(
                        try_reclaim(self.monitor, self.id, &holder).await?,
                        Reclaim::Wait
                    )
                {
                    return Ok(Some(holder));
                }
                locks.remove_structure_holder(&holder);
            }
        }
        locks.add_structure_reader(self.id.clone());
        Ok(None)
    }

    /// Quiesces the node under exclusive structure protection.
    ///
    /// Returns the live holder to wait for, or leaves both node-lock scopes free
    /// of finalized foreign holders with structure-write installed for this
    /// operation.
    pub(crate) async fn acquire_structure_write(
        &self,
        locks: &mut NodeLocks,
    ) -> Result<Option<TxId>, TransError> {
        if locks.structure().lock_type() == LockType::Write && locks.structure().contains(self.id) {
            self.prune_finalized_membership(locks).await?;
            return Ok(None);
        }
        for holder in locks.structure().holders().to_vec() {
            if &holder == self.id {
                continue;
            }
            if self.monitor.tx_status(&holder).await? == TxCommitStatus::Pending
                && matches!(
                    try_reclaim(self.monitor, self.id, &holder).await?,
                    Reclaim::Wait
                )
            {
                return Ok(Some(holder));
            }
            locks.remove_structure_holder(&holder);
        }
        locks.set_structure_writer(self.id.clone());
        self.prune_finalized_membership(locks).await?;
        Ok(None)
    }

    /// Acquires the requested membership lock, returning a holder to wait for.
    async fn acquire_membership(
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
                if self.monitor.tx_status(&holder).await? == TxCommitStatus::Pending
                    && matches!(
                        try_reclaim(self.monitor, self.id, &holder).await?,
                        Reclaim::Wait
                    )
                {
                    return Ok(Some(holder));
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
}

/// A leaf's entries and node locks after finalized holders are resolved.
pub(crate) struct ReconciledLeaf {
    path: String,
    id: TxId,
    entries: BTreeMap<Vec<u8>, ShardEntry>,
    locks: NodeLocks,
}

impl ReconciledLeaf {
    /// Resolves entry holders before discarding their finalized node holds.
    pub(crate) async fn new(
        ctx: &ResolveCtx<'_>,
        path: &str,
        id: &TxId,
        entries: &BTreeMap<Vec<u8>, ShardEntry>,
        locks: &NodeLocks,
    ) -> Result<Self, TransError> {
        let entries = resolve_entries(ctx, path, id, entries).await?;
        let mut locks = locks.clone();
        NodeLockReconciler::new(ctx.tmon, id)
            .prune_finalized(&mut locks)
            .await?;
        Ok(Self {
            path: path.to_string(),
            id: id.clone(),
            entries,
            locks,
        })
    }

    pub(crate) fn entry(&self, key: &[u8]) -> Option<&ShardEntry> {
        self.entries.get(key)
    }

    pub(crate) fn insert_entry(&mut self, key: Vec<u8>, entry: ShardEntry) {
        self.entries.insert(key, entry);
    }

    pub(crate) fn membership_lock_type(&self) -> LockType {
        self.locks.membership().lock_type()
    }

    /// Acquires the node locks required by a data mutation or scan.
    pub(crate) async fn acquire_mutation_locks(
        &mut self,
        ctx: &ResolveCtx<'_>,
        membership: LockType,
    ) -> Result<Option<TxId>, TransError> {
        let reconciler = NodeLockReconciler::new(ctx.tmon, &self.id);
        if let Some(holder) = reconciler.acquire_structure_read(&mut self.locks).await? {
            return Ok(Some(holder));
        }
        if membership != LockType::None
            && let Some(holder) = reconciler
                .acquire_membership(&mut self.locks, membership)
                .await?
        {
            return Ok(Some(holder));
        }
        Ok(None)
    }

    async fn refresh_entries(&mut self, ctx: &ResolveCtx<'_>) -> Result<(), TransError> {
        self.entries = resolve_entries(ctx, &self.path, &self.id, &self.entries).await?;
        Ok(())
    }

    /// Returns the complete delta against the fold state observed on entry.
    pub(crate) fn into_stage(
        self,
        original: &BTreeMap<Vec<u8>, ShardEntry>,
    ) -> (Vec<(Vec<u8>, ShardEntry)>, NodeLocks) {
        let changes = self
            .entries
            .into_iter()
            .filter(|(key, entry)| original.get(key) != Some(entry))
            .collect();
        (changes, self.locks)
    }
}

/// The leaf-coordinator resolver for a split's structure-write acquisition.
pub(crate) struct StructureWriteResolver {
    id: TxId,
    path: String,
}

impl StructureWriteResolver {
    pub(crate) fn new(id: TxId, path: String) -> Self {
        Self { id, path }
    }
}

#[async_trait]
impl ShardResolver for StructureWriteResolver {
    async fn resolve(
        &self,
        ctx: &ResolveCtx<'_>,
        staged: &BTreeMap<Vec<u8>, ShardEntry>,
        staged_locks: &NodeLocks,
    ) -> Result<Step, TransError> {
        let mut leaf = ReconciledLeaf::new(ctx, &self.path, &self.id, staged, staged_locks).await?;
        let reconciler = NodeLockReconciler::new(ctx.tmon, &self.id);
        if let Some(holder) = reconciler.acquire_structure_write(&mut leaf.locks).await? {
            return Ok(Step::Skip {
                outcome: FoldOutcome::Wait(holder),
            });
        }

        // Wounding structure readers changes which entry holders are live. A
        // second pass prevents their stale locks from moving to the sibling.
        leaf.refresh_entries(ctx).await?;
        let (entries, locks) = leaf.into_stage(staged);
        Ok(Step::Stage {
            entries,
            locks,
            outcome: FoldOutcome::Locked {
                typ: LockType::Write,
                membership: LockType::None,
            },
        })
    }

    fn reorderable(&self) -> bool {
        false
    }

    fn exhausted_outcome(&self) -> FoldOutcome {
        FoldOutcome::Conflict
    }
}

async fn resolve_entries(
    ctx: &ResolveCtx<'_>,
    path: &str,
    id: &TxId,
    entries: &BTreeMap<Vec<u8>, ShardEntry>,
) -> Result<BTreeMap<Vec<u8>, ShardEntry>, TransError> {
    let prefix = paths::parse(path)
        .map_err(|e| TransError::with_source("parsing leaf path", e))?
        .prefix;
    let mut resolved_entries = BTreeMap::new();
    for (key, entry) in entries {
        let resolved = ctx
            .resolver
            .resolve_holders(&paths::from_key(&prefix, key), entry, Some(id))
            .await?;
        let mut entry = entry.clone();
        entry.current_writer = resolved.writer;
        entry.deleted = resolved.deleted;
        entry
            .locked_by
            .retain(|holder| holder == id || resolved.pending.contains(holder));
        if entry.locked_by.is_empty() {
            entry.lock_type = LockType::None;
        }
        resolved_entries.insert(key.clone(), entry);
    }
    Ok(resolved_entries)
}
