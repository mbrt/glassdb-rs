//! Collection-specific state and phases within the transaction commit protocol.

use std::collections::BTreeSet;

use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::{Requirement, SplitPolicy, TxCollectionChange, TxCollectionOp, TxLock};

use crate::collection_catalog::CollectionCatalog;
use crate::collections::{CollectionData, CollectionLifecycle, CollectionOp};
use crate::error::TransError;
use crate::monitor::{Monitor, TxRecoveryManifest};

/// Collection accesses and physical resources retained across one transaction
/// identity's body retries.
pub(crate) struct CollectionAttempt {
    data: CollectionData,
    prepared: BTreeSet<CollectionAddress>,
    fenced_drops: BTreeSet<CollectionAddress>,
}

/// Coordinates the collection-specific phases around the shared transaction
/// commit point.
#[derive(Clone)]
pub(crate) struct CollectionCommit {
    catalog: CollectionCatalog,
    lifecycle: CollectionLifecycle,
    monitor: Monitor,
    split_policy: SplitPolicy,
}

impl CollectionAttempt {
    /// Starts collection tracking for one transaction identity.
    pub(crate) fn new(data: CollectionData) -> Self {
        Self {
            data,
            prepared: BTreeSet::new(),
            fenced_drops: BTreeSet::new(),
        }
    }

    /// Returns the logical collection accesses from the current body run.
    pub(crate) fn data(&self) -> &CollectionData {
        &self.data
    }

    /// Reports whether the current body changes a collection binding.
    pub(crate) fn has_writes(&self) -> bool {
        self.data.has_writes()
    }

    /// Replaces logical accesses while retaining resources owned by the same
    /// transaction identity.
    pub(crate) fn replace_data(&mut self, data: CollectionData) {
        self.data = data;
    }

    /// Carries logical accesses to a renewed identity without claiming the old
    /// identity's physical resources.
    pub(crate) fn renewed(self) -> Self {
        Self::new(self.data)
    }

    /// Returns the complete recovery manifest for the committed transaction.
    pub(crate) fn committed_manifest(&self, locks: Vec<TxLock>) -> TxRecoveryManifest {
        TxRecoveryManifest {
            locks,
            collection_changes: self.encoded_changes(),
            prepared_collections: self.prepared.iter().cloned().collect(),
        }
    }

    fn pending_manifest(&self, mut current: TxRecoveryManifest) -> TxRecoveryManifest {
        current.collection_changes = self.encoded_changes();
        current.prepared_collections = self
            .prepared
            .iter()
            .cloned()
            .chain(self.created_collections().cloned())
            .collect::<BTreeSet<_>>()
            .into_iter()
            .collect();
        current
    }

    fn encoded_changes(&self) -> Vec<TxCollectionChange> {
        self.data
            .changes
            .iter()
            .map(|change| TxCollectionChange {
                parent: change.parent.clone(),
                name: change.name.clone(),
                collection: change.collection.clone(),
                op: match change.op {
                    CollectionOp::Create => TxCollectionOp::Create,
                    CollectionOp::Drop => TxCollectionOp::Drop,
                },
            })
            .collect()
    }

    fn created_collections(&self) -> impl Iterator<Item = &CollectionAddress> {
        self.data
            .changes
            .iter()
            .filter(|change| change.op == CollectionOp::Create)
            .map(|change| &change.collection)
    }

    fn active_drops(&self) -> BTreeSet<CollectionAddress> {
        self.data
            .changes
            .iter()
            .filter(|change| change.op == CollectionOp::Drop)
            .map(|change| change.collection.clone())
            .collect()
    }
}

impl CollectionCommit {
    /// Creates collection commit coordination over semantic and physical
    /// collection services.
    pub(crate) fn new(
        catalog: CollectionCatalog,
        lifecycle: CollectionLifecycle,
        monitor: Monitor,
        split_policy: SplitPolicy,
    ) -> Self {
        Self {
            catalog,
            lifecycle,
            monitor,
            split_policy,
        }
    }

    /// Clears drop preparation left by an earlier body run under the same
    /// transaction identity.
    pub(crate) async fn reconcile_retry(
        &self,
        id: &TxId,
        attempt: &mut CollectionAttempt,
    ) -> Result<(), TransError> {
        let active_drops = attempt.active_drops();
        let abandoned = attempt
            .fenced_drops
            .difference(&active_drops)
            .cloned()
            .collect::<Vec<_>>();
        self.lifecycle.clear_aborted_drops(id, &abandoned).await?;
        attempt
            .fenced_drops
            .retain(|drop| active_drops.contains(drop));
        Ok(())
    }

    /// Persists the collection recovery metadata before physical preparation
    /// can make new incarnation objects visible to recovery.
    pub(crate) async fn persist_manifest(
        &self,
        id: &TxId,
        is_new: bool,
        attempt: &CollectionAttempt,
    ) -> Result<(), TransError> {
        debug_assert!(attempt.has_writes());
        if is_new {
            let recovery = attempt.pending_manifest(TxRecoveryManifest::default());
            self.monitor.begin_persisted_tx(id, recovery).await
        } else {
            self.monitor
                .update_pending_tx(id, |pending| {
                    let current = std::mem::take(pending);
                    *pending = attempt.pending_manifest(current);
                })
                .await
        }
    }

    /// Prepares every newly created collection and records its ownership in the
    /// in-memory attempt state.
    pub(crate) async fn prepare(&self, attempt: &mut CollectionAttempt) -> Result<(), TransError> {
        let created = attempt.created_collections().cloned().collect::<Vec<_>>();
        attempt.prepared.extend(created);
        self.lifecycle
            .prepare_collections(&attempt.data.changes)
            .await
    }

    /// Validates the attempt's logical directory observations and mutations at
    /// the supplied commit barrier.
    pub(crate) async fn validate(
        &self,
        id: Option<&TxId>,
        attempt: &CollectionAttempt,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        self.catalog
            .validate(
                id,
                &attempt.data.reads,
                &attempt.data.changes,
                requirement,
                &self.split_policy,
            )
            .await
    }

    /// Installs deletion fences for every drop in the current body run.
    pub(crate) async fn fence(
        &self,
        id: &TxId,
        attempt: &mut CollectionAttempt,
    ) -> Result<(), TransError> {
        // Remember every target before its fencing starts so a partial attempt
        // is recoverable by a same-id body retry or abort.
        attempt.fenced_drops.extend(attempt.active_drops());
        self.lifecycle.fence_drops(id, &attempt.data.changes).await
    }

    /// Reclaims physical collection objects that the committed logical changes
    /// no longer need.
    pub(crate) async fn finish_committed(&self, attempt: &CollectionAttempt) {
        let active_prepared = attempt
            .created_collections()
            .cloned()
            .collect::<BTreeSet<_>>();
        let unused = attempt
            .prepared
            .difference(&active_prepared)
            .cloned()
            .collect::<Vec<_>>();
        if let Err(error) = self.lifecycle.reclaim(&unused).await {
            tracing::debug!(%error, "prepared-collection cleanup deferred");
        }
        let dropped = attempt.fenced_drops.iter().cloned().collect::<Vec<_>>();
        if let Err(error) = self.lifecycle.reclaim(&dropped).await {
            tracing::debug!(%error, "dropped-collection cleanup deferred");
        }
    }

    /// Clears physical collection effects owned by an aborted transaction.
    pub(crate) async fn abort(
        &self,
        id: &TxId,
        attempt: &CollectionAttempt,
    ) -> Result<(), TransError> {
        let drops = attempt.fenced_drops.iter().cloned().collect::<Vec<_>>();
        self.lifecycle.clear_aborted_drops(id, &drops).await?;
        let prepared = attempt.prepared.iter().cloned().collect::<Vec<_>>();
        self.lifecycle.reclaim(&prepared).await
    }
}

#[cfg(test)]
mod tests {
    use glassdb_data::CollectionId;

    use super::*;
    use crate::collections::CollectionChange;

    fn address(byte: u8) -> CollectionAddress {
        CollectionAddress::new(
            "db",
            CollectionId::from_slice(&[byte; 16]).expect("fixed ID has the required width"),
        )
    }

    fn create_change(collection: CollectionAddress) -> CollectionChange {
        CollectionChange {
            parent: CollectionAddress::root("db"),
            name: b"child".to_vec(),
            collection,
            expected: None,
            op: CollectionOp::Create,
        }
    }

    #[test]
    fn renewed_attempt_keeps_accesses_without_old_physical_resources() {
        let collection = address(1);
        let mut attempt = CollectionAttempt::new(CollectionData {
            reads: Vec::new(),
            changes: vec![create_change(collection.clone())],
        });
        attempt.prepared.insert(collection.clone());
        attempt.fenced_drops.insert(address(2));

        let renewed = attempt.renewed();

        assert_eq!(renewed.data.changes.len(), 1);
        assert_eq!(renewed.data.changes[0].collection, collection);
        assert!(renewed.prepared.is_empty());
        assert!(renewed.fenced_drops.is_empty());
    }

    #[test]
    fn durable_projections_preserve_prepared_roots_from_prior_body_runs() {
        let earlier = address(1);
        let active = address(2);
        let mut attempt = CollectionAttempt::new(CollectionData {
            reads: Vec::new(),
            changes: vec![create_change(active.clone())],
        });
        attempt.prepared.insert(earlier.clone());

        let retained_lock = TxLock::Topology {
            collection: CollectionAddress::root("db"),
        };
        let recovery = attempt.pending_manifest(TxRecoveryManifest {
            locks: vec![retained_lock.clone()],
            ..TxRecoveryManifest::default()
        });
        assert_eq!(recovery.locks, vec![retained_lock.clone()]);
        assert_eq!(
            recovery.prepared_collections,
            vec![earlier.clone(), active.clone()]
        );
        assert_eq!(recovery.collection_changes.len(), 1);

        attempt.prepared.insert(active.clone());
        let committed = attempt.committed_manifest(vec![retained_lock.clone()]);
        assert_eq!(committed.locks, vec![retained_lock]);
        assert_eq!(
            committed.prepared_collections,
            vec![earlier, active.clone()]
        );
        assert_eq!(committed.collection_changes.len(), 1);
        assert_eq!(committed.collection_changes[0].collection, active);
    }
}
