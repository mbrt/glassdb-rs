//! Shared node-lock policy for leaf mutations and structural operations.
//!
//! The shard coordinator is deliberately policy-free. This module owns the
//! wound-wait transitions applied to the node-level structure and membership
//! locks, including the leaf-quiescing sequence required before a split takes
//! structure-write.

use std::collections::BTreeMap;

use async_trait::async_trait;
use glassdb_data::{KeyRef, LeafRef, TxId};
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

async fn resolve_entries(
    ctx: &ResolveCtx<'_>,
    path: &str,
    id: &TxId,
    entries: &BTreeMap<Vec<u8>, ShardEntry>,
) -> Result<BTreeMap<Vec<u8>, ShardEntry>, TransError> {
    let collection = LeafRef::from_physical_path(path)
        .map_err(|e| TransError::with_source("parsing leaf path", e))?
        .collection()
        .clone();
    let mut resolved_entries = BTreeMap::new();
    for (key, entry) in entries {
        let resolved = resolve_entry_locks(
            ctx,
            &KeyRef::new(collection.clone(), key),
            Some(entry),
            Some(id),
        )
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
