//! Structural-node mutation primitives.

use std::collections::BTreeMap;

use glassdb_concurr::rt;
use glassdb_data::{CollectionAddress, NodeId, ObjectPath, StructuralIntentId, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxRecord};
use glassdb_storage::{
    LeafBody, LeafObservation, LockType, MergeTarget, Node, NodeStore, Requirement, StorageError,
};

use crate::error::TransError;
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::LeafCoordinator;
use crate::monitor::Monitor;
use crate::node_locking::{
    GateAcquisition, NodeLockReconciler, QuiescedEntries, StructuralGateOperation,
    StructuralGateOutcome,
};

use super::NODE_CAS_ATTEMPTS;

/// Shares structural-node mutation primitives between structural changes,
/// parent reconciliation, and recovery.
#[derive(Clone)]
pub(super) struct StructuralNodeAccess {
    nodes: NodeStore,
    mon: Monitor,
    key_state: KeyStateResolver,
    pub(super) coord: LeafCoordinator,
}

impl StructuralNodeAccess {
    pub(super) fn new(
        nodes: NodeStore,
        mon: Monitor,
        key_state: KeyStateResolver,
        coord: LeafCoordinator,
    ) -> Self {
        Self {
            nodes,
            mon,
            key_state,
            coord,
        }
    }

    /// Registers a worker's ephemeral wound-wait identity, which
    /// [`Self::finalize_worker`] retires.
    pub(super) fn begin_worker(&self, id: &TxId) {
        self.mon.begin_tx(id);
    }

    /// Registers a new worker identity and acquires one node's structural gate
    /// under wound-wait.
    ///
    /// `None` reports that the gate was not taken — contention, a wait, or a
    /// lost CAS — and that the identity is already retired, so the caller can
    /// retry without cleanup of its own. An error retires it likewise.
    pub(super) async fn begin_gated_worker(
        &self,
        collection: &CollectionAddress,
        node_id: Option<&NodeId>,
    ) -> Result<Option<(TxId, Node, LeafObservation)>, TransError> {
        let id = TxId::new_at(rt::system_now());
        self.begin_worker(&id);
        match self
            .acquire_structural_gate(collection, node_id, &id, GateAcquisition::WoundWait)
            .await
        {
            Ok(Some((node, observation))) => Ok(Some((id, node, observation))),
            Ok(None) => {
                self.finalize_worker(&id).await;
                Ok(None)
            }
            Err(error) => {
                self.finalize_worker(&id).await;
                Err(error)
            }
        }
    }

    /// Acquires a source node's structural gate. A leaf, including the fixed
    /// root while it is a leaf, joins the shared coordinator round. An index
    /// uses the direct structural CAS path.
    pub(super) async fn acquire_structural_gate(
        &self,
        collection: &CollectionAddress,
        node_id: Option<&NodeId>,
        id: &TxId,
        acquisition: GateAcquisition,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        let path = match node_id {
            Some(node_id) => ObjectPath::Node {
                collection: collection.clone(),
                id: *node_id,
            },
            None => ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
        };
        // This read selects the acquisition path; missing roots defer work.
        // Publication still requires the gated observation and a source CAS.
        let (node, _) = match self.nodes.load_node_at(&path, Requirement::ANY).await {
            Ok(loaded) => loaded,
            Err(StorageError::NotFound) if node_id.is_none() => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        if node.as_leaf().is_some() {
            return self
                .acquire_leaf_structural_gate(&path, id, acquisition)
                .await;
        }
        self.acquire_structural_gate_direct(collection, node_id, id, acquisition)
            .await
    }

    /// Releases a source gate whose acquisition or presence this cache knows.
    ///
    /// The caller must have acquired the gate locally or completed a bounded
    /// read containing this holder through the same cache. The worker must not
    /// acquire this source gate again. Thus an ANY no-holder result proves
    /// removal, while a retained holder is removed by CAS.
    pub(super) async fn release_structural_gate(
        &self,
        collection: &CollectionAddress,
        node_id: Option<&NodeId>,
        id: &TxId,
    ) -> Result<(), TransError> {
        for _ in 0..NODE_CAS_ATTEMPTS {
            let (mut node, observation) = match node_id {
                Some(node_id) => {
                    self.nodes
                        .load_node(collection, node_id, Requirement::ANY)
                        .await?
                }
                None => {
                    let (root, observation) =
                        self.nodes.load_root(collection, Requirement::ANY).await?;
                    (root, observation)
                }
            };
            if !node.remove_structural_gate(id) {
                return Ok(());
            }
            if self
                .store_structural_node(&node, &observation)
                .await?
                .is_some()
            {
                return Ok(());
            }
        }
        Err(TransError::Retry)
    }

    /// Compare-and-swaps `node` over the object `observation` was taken from,
    /// reporting the observation of the state it installed.
    pub(super) async fn store_structural_node(
        &self,
        node: &Node,
        observation: &LeafObservation,
    ) -> Result<Option<LeafObservation>, TransError> {
        Ok(self
            .nodes
            .store_node_at(observation.path(), node, observation)
            .await?)
    }

    /// Makes sure that the merge of `intent` into `target` never takes effect,
    /// once its drain can no longer land (ADR-073). If the absorb landed, it
    /// reverts it. If not, it advances the target's membership generation
    /// when the absorb could still land, because the absorb requires the
    /// generation recorded in the intent.
    pub(super) async fn abandon_merge(
        &self,
        collection: &CollectionAddress,
        target: &MergeTarget,
        intent: &StructuralIntentId,
        requirement: Requirement,
    ) -> Result<(), TransError> {
        self.update_node(collection, &target.node_id, requirement, |node| {
            if node.abandon_merge(intent, &target.boundary) {
                return true;
            }
            if node.is_drained() || node.membership_generation() != target.generation {
                return false;
            }
            let mut locks = node.locks().clone();
            locks.advance_membership_generation();
            node.set_locks(locks);
            true
        })
        .await
    }

    /// Removes the merge reservation of `intent` from node `node_id`, if present.
    pub(super) async fn remove_merge_reservation(
        &self,
        collection: &CollectionAddress,
        node_id: &NodeId,
        intent: &StructuralIntentId,
        requirement: Requirement,
    ) -> Result<(), TransError> {
        self.update_node(collection, node_id, requirement, |node| {
            node.remove_merge_reservation(intent)
        })
        .await
    }

    /// Finalizes a worker's ephemeral wound-wait identity without creating a
    /// transaction record. Structural state, not transaction status, records
    /// the durable outcome of the worker's change.
    pub(super) async fn finalize_worker(&self, id: &TxId) {
        if let Err(error) = self
            .mon
            .commit_tx(TxRecord::new(id.clone(), TxCommitStatus::Committed))
            .await
        {
            tracing::debug!(
                target: "glassdb::restructurer",
                error = %error,
                "finalizing structural worker failed"
            );
        }
    }

    /// Releases the structural gate of a worker from
    /// [`Self::begin_gated_worker`] and retires its identity.
    pub(super) async fn finish_gated_worker(
        &self,
        collection: &CollectionAddress,
        node_id: Option<&NodeId>,
        id: &TxId,
    ) -> Result<(), TransError> {
        let release = self.release_structural_gate(collection, node_id, id).await;
        self.finalize_worker(id).await;
        release
    }

    async fn acquire_leaf_structural_gate(
        &self,
        path: &ObjectPath,
        id: &TxId,
        acquisition: GateAcquisition,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        let outcome = self
            .coord
            .coordinate(StructuralGateOperation::new(
                id.clone(),
                path.clone(),
                acquisition,
            ))
            .await?;
        let StructuralGateOutcome::Acquired(observation) = outcome else {
            return Ok(None);
        };
        // Keep the state paired with the gate proof even if a peer changes the
        // node before this caller resumes. The next mutation checks its revision.
        let node = observation
            .value()
            .ok_or(StorageError::NotFound)?
            .as_ref()
            .clone();
        if node.structural_gate().lock_type() == LockType::Write
            && node.structural_gate().contains(id)
        {
            Ok(Some((node, observation)))
        } else {
            Ok(None)
        }
    }

    async fn acquire_structural_gate_direct(
        &self,
        collection: &CollectionAddress,
        node_id: Option<&NodeId>,
        id: &TxId,
        acquisition: GateAcquisition,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        for _ in 0..NODE_CAS_ATTEMPTS {
            let (mut node, observation) = match node_id {
                Some(node_id) => {
                    self.nodes
                        .load_node(collection, node_id, Requirement::ANY)
                        .await?
                }
                None => match self.nodes.load_root(collection, Requirement::ANY).await {
                    Ok((root, observation)) => (root, observation),
                    Err(StorageError::NotFound) => return Ok(None),
                    Err(error) => return Err(error.into()),
                },
            };
            if node.structural_gate().lock_type() == LockType::Write
                && node.structural_gate().contains(id)
            {
                return Ok(Some((node, observation)));
            }

            let entries: BTreeMap<Vec<u8>, _> = node
                .as_leaf()
                .into_iter()
                .flat_map(LeafBody::entries)
                .cloned()
                .map(|entry| (entry.key.clone(), entry))
                .collect();
            let reconciler =
                NodeLockReconciler::with_acquisition(&self.key_state, &self.mon, id, acquisition);
            let entries = match reconciler
                .quiesce_entries(collection, &entries, Requirement::ANY)
                .await?
            {
                QuiescedEntries::Ready(entries) => entries,
                QuiescedEntries::Wait(_) => return Ok(None),
            };
            let mut locks = node.locks().clone();
            if reconciler
                .acquire_structural_gate(&mut locks)
                .await?
                .is_some()
            {
                return Ok(None);
            }

            if node.as_leaf().is_some() {
                node.set_leaf(LeafBody::from_entries(entries.into_values()))?;
            }
            node.set_locks(locks);
            // The CAS receipt holds the gated state itself. A re-read reports
            // whatever revision this database knows of by then, which the caller
            // would then pair with the body it gated.
            if let Some(gated) = self.store_structural_node(&node, &observation).await? {
                return Ok(Some((node, gated)));
            }
        }
        Ok(None)
    }

    /// Applies `change` to node `node_id` read at `requirement`, retrying on a
    /// lost CAS. `change` returns `false` when the node needs no change. A
    /// missing node needs no change.
    async fn update_node(
        &self,
        collection: &CollectionAddress,
        node_id: &NodeId,
        requirement: Requirement,
        mut change: impl FnMut(&mut Node) -> bool,
    ) -> Result<(), TransError> {
        for _ in 0..NODE_CAS_ATTEMPTS {
            let (mut node, observation) =
                match self.nodes.load_node(collection, node_id, requirement).await {
                    Ok(loaded) => loaded,
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(error) => return Err(error.into()),
                };
            if !change(&mut node) {
                return Ok(());
            }
            if self
                .store_structural_node(&node, &observation)
                .await?
                .is_some()
            {
                return Ok(());
            }
        }
        Err(TransError::Retry)
    }
}
