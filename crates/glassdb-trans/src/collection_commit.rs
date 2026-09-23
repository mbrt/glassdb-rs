//! Collection-specific state and phases within the transaction commit protocol.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};

use glassdb_data::{CollectionAddress, CollectionId, TxId};
use glassdb_storage::transaction::{TxCollectionChange, TxCollectionOp, TxLock};
use glassdb_storage::{CurrentnessBarrier, Requirement, SplitPolicy};

use crate::collection_catalog::CollectionCatalog;
use crate::collections::{CatalogAccesses, CollectionLifecycle, CollectionOp};
use crate::error::TransError;
use crate::monitor::{Monitor, TxRecoveryManifest};

/// Collection accesses and physical resources retained across the body replays
/// of one transaction identity.
pub(crate) struct CollectionHandleState {
    accesses: CatalogAccesses,
    reservations: CollectionReservations,
    prepared: BTreeSet<CollectionAddress>,
    fenced_drops: BTreeSet<CollectionAddress>,
}

/// Issues reusable collection IDs within one transaction identity.
#[derive(Clone)]
pub struct CollectionReservations {
    ids: Arc<Mutex<HashMap<CollectionBinding, CollectionId>>>,
    limit: usize,
}

/// A new collection binding would exceed the transaction identity's reservation limit.
#[derive(Debug, Clone, Copy, thiserror::Error)]
#[error("collection reservation limit of {limit} exceeded")]
pub struct CollectionReservationLimitExceeded {
    /// Maximum new collection bindings reserved by one transaction identity.
    pub limit: usize,
}

type CollectionBinding = (CollectionAddress, Vec<u8>);

impl CollectionReservations {
    /// Reserves the same collection ID for repeated creation of one binding.
    /// Returns an error when a new reservation would exceed the fixed limit.
    pub fn reserve(
        &self,
        parent: &CollectionAddress,
        name: &[u8],
    ) -> Result<CollectionId, CollectionReservationLimitExceeded> {
        let mut ids = self.ids.lock().unwrap();
        let binding = (parent.clone(), name.to_vec());
        if let Some(id) = ids.get(&binding) {
            return Ok(*id);
        }
        if ids.len() >= self.limit {
            return Err(CollectionReservationLimitExceeded { limit: self.limit });
        }
        let id = CollectionId::new_random();
        ids.insert(binding, id);
        Ok(id)
    }

    fn new(limit: usize) -> Self {
        Self {
            ids: Arc::new(Mutex::new(HashMap::new())),
            limit,
        }
    }
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

impl CollectionHandleState {
    /// Starts collection tracking with a fixed reservation limit.
    pub(crate) fn new(accesses: CatalogAccesses, reservation_limit: usize) -> Self {
        Self {
            accesses,
            reservations: CollectionReservations::new(reservation_limit),
            prepared: BTreeSet::new(),
            fenced_drops: BTreeSet::new(),
        }
    }

    /// Returns collection-ID reservations owned by the current identity.
    pub(crate) fn reservations(&self) -> CollectionReservations {
        self.reservations.clone()
    }

    /// Returns the logical collection accesses from the current body run.
    pub(crate) fn accesses(&self) -> &CatalogAccesses {
        &self.accesses
    }

    /// Reports whether the current body changes a collection binding.
    pub(crate) fn has_writes(&self) -> bool {
        self.accesses.has_writes()
    }

    /// Replaces logical accesses while retaining resources owned by the same
    /// transaction identity.
    pub(crate) fn replace_accesses(&mut self, accesses: CatalogAccesses) {
        self.accesses = accesses;
    }

    /// Drops physical resources that belonged to the retired identity.
    pub(crate) fn renew(&mut self) {
        // Old body handles must never allocate from the replacement identity.
        self.reservations = CollectionReservations::new(self.reservations.limit);
        self.prepared.clear();
        self.fenced_drops.clear();
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
        self.accesses
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
        self.accesses
            .changes
            .iter()
            .filter(|change| change.op == CollectionOp::Create)
            .map(|change| &change.collection)
    }

    fn active_drops(&self) -> BTreeSet<CollectionAddress> {
        self.accesses
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
    pub(crate) async fn reconcile_replay(
        &self,
        id: &TxId,
        handle: &mut CollectionHandleState,
    ) -> Result<(), TransError> {
        let active_drops = handle.active_drops();
        let discarded = handle
            .fenced_drops
            .difference(&active_drops)
            .cloned()
            .collect::<Vec<_>>();
        // Fencing has stopped. Cleanup shares the cache that installed this
        // identity's intents and freeze, so no-change results can use ANY.
        self.lifecycle
            .clear_aborted_drops(id, &discarded, Requirement::ANY)
            .await?;
        handle
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
        handle: &CollectionHandleState,
    ) -> Result<(), TransError> {
        debug_assert!(handle.has_writes());
        if is_new {
            let recovery = handle.pending_manifest(TxRecoveryManifest::default());
            self.monitor.begin_persisted_tx(id, recovery).await
        } else {
            self.monitor
                .update_pending_tx(id, |pending| {
                    let current = std::mem::take(pending);
                    *pending = handle.pending_manifest(current);
                })
                .await
        }
    }

    /// Prepares every newly created collection and records its ownership in the
    /// in-memory collection handle state.
    pub(crate) async fn prepare(
        &self,
        handle: &mut CollectionHandleState,
    ) -> Result<(), TransError> {
        let created = handle.created_collections().cloned().collect::<Vec<_>>();
        handle.prepared.extend(created);
        self.lifecycle
            .prepare_collections(&handle.accesses.changes)
            .await
    }

    /// Validates the handle's logical directory observations and mutations at
    /// the supplied commit barrier.
    pub(crate) async fn validate(
        &self,
        id: Option<&TxId>,
        handle: &CollectionHandleState,
        barrier: CurrentnessBarrier,
    ) -> Result<bool, TransError> {
        self.catalog
            .validate(
                id,
                &handle.accesses.reads,
                &handle.accesses.changes,
                barrier,
                &self.split_policy,
            )
            .await
    }

    /// Validates durable directory observations without staged collection changes.
    pub(crate) async fn validate_reads(
        &self,
        handle: &CollectionHandleState,
        barrier: CurrentnessBarrier,
    ) -> Result<bool, TransError> {
        let reads = handle.accesses.clone().into_read_only();
        self.catalog
            .validate(None, &reads.reads, &[], barrier, &self.split_policy)
            .await
    }

    /// Installs deletion fences for every drop in the current body run.
    pub(crate) async fn fence(
        &self,
        id: &TxId,
        handle: &mut CollectionHandleState,
    ) -> Result<(), TransError> {
        // Remember every target before its fencing starts so a partial commit
        // is recoverable by a same-identity body replay or abort.
        handle.fenced_drops.extend(handle.active_drops());
        self.lifecycle
            .fence_drops(id, &handle.accesses.changes)
            .await
    }

    /// Reclaims physical collection objects that the committed logical changes
    /// no longer need.
    pub(crate) async fn finish_committed(&self, handle: &CollectionHandleState) {
        let active_prepared = handle
            .created_collections()
            .cloned()
            .collect::<BTreeSet<_>>();
        let unused = handle
            .prepared
            .difference(&active_prepared)
            .cloned()
            .collect::<Vec<_>>();
        if let Err(error) = self.lifecycle.reclaim(&unused).await {
            tracing::debug!(%error, "prepared-collection cleanup deferred");
        }
        let dropped = handle.fenced_drops.iter().cloned().collect::<Vec<_>>();
        if let Err(error) = self.lifecycle.reclaim(&dropped).await {
            tracing::debug!(%error, "dropped-collection cleanup deferred");
        }
    }

    /// Clears physical collection effects owned by an aborted transaction.
    pub(crate) async fn abort(
        &self,
        id: &TxId,
        handle: &CollectionHandleState,
    ) -> Result<(), TransError> {
        let drops = handle.fenced_drops.iter().cloned().collect::<Vec<_>>();
        // Abort runs after fencing stops and shares its cache, as retry cleanup
        // does. Recovered cleanup needs its separate acknowledgement bound.
        self.lifecycle
            .clear_aborted_drops(id, &drops, Requirement::ANY)
            .await?;
        let prepared = handle.prepared.iter().cloned().collect::<Vec<_>>();
        self.lifecycle.reclaim(&prepared).await.map(|_| ())
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
    fn renewed_identity_keeps_accesses_without_old_physical_resources() {
        let collection = address(1);
        let mut handle = CollectionHandleState::new(
            CatalogAccesses {
                reads: Vec::new(),
                changes: vec![create_change(collection.clone())],
            },
            1,
        );
        handle.prepared.insert(collection.clone());
        handle.fenced_drops.insert(address(2));
        let retired_reservations = handle.reservations();
        let parent = CollectionAddress::root("db");
        let old_id = retired_reservations.reserve(&parent, b"child").unwrap();

        handle.renew();

        let reservations = handle.reservations();
        assert_ne!(reservations.reserve(&parent, b"child").unwrap(), old_id);
        assert_eq!(
            retired_reservations.reserve(&parent, b"child").unwrap(),
            old_id
        );
        for reservations in [retired_reservations, reservations] {
            assert_eq!(
                reservations.reserve(&parent, b"other").unwrap_err().limit,
                1
            );
        }
        assert_eq!(handle.accesses.changes.len(), 1);
        assert_eq!(handle.accesses.changes[0].collection, collection);
        assert!(handle.prepared.is_empty());
        assert!(handle.fenced_drops.is_empty());
    }

    #[test]
    fn durable_projections_preserve_prepared_roots_from_prior_body_runs() {
        let earlier = address(1);
        let active = address(2);
        let mut handle = CollectionHandleState::new(
            CatalogAccesses {
                reads: Vec::new(),
                changes: vec![create_change(active.clone())],
            },
            0,
        );
        handle.prepared.insert(earlier.clone());

        let retained_lock = TxLock::Topology {
            collection: CollectionAddress::root("db"),
        };
        let recovery = handle.pending_manifest(TxRecoveryManifest {
            locks: vec![retained_lock.clone()],
            ..TxRecoveryManifest::default()
        });
        assert_eq!(recovery.locks, vec![retained_lock.clone()]);
        assert_eq!(
            recovery.prepared_collections,
            vec![earlier.clone(), active.clone()]
        );
        assert_eq!(recovery.collection_changes.len(), 1);

        handle.prepared.insert(active.clone());
        let committed = handle.committed_manifest(vec![retained_lock.clone()]);
        assert_eq!(committed.locks, vec![retained_lock]);
        assert_eq!(
            committed.prepared_collections,
            vec![earlier, active.clone()]
        );
        assert_eq!(committed.collection_changes.len(), 1);
        assert_eq!(committed.collection_changes[0].collection, active);
    }
}
