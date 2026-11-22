//! Structural-node mutation primitives.

use std::future::ready;

use glassdb_concurr::rt;
use glassdb_data::{CollectionAddress, NodeId, ObjectPath, StructuralIntentId, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxRecord};
use glassdb_storage::{
    CasRetries, Converged, Decision, LeafObservation, LockType, MergeTarget, Node, NodeStore,
    Requirement, StorageError,
};

use crate::error::TransError;
use crate::key_state_resolver::KeyStateResolver;
use crate::leaf_coord::LeafCoordinator;
use crate::monitor::Monitor;
use crate::node_locking::{
    GateAcquisition, NodeLockReconciler, StructuralGateOperation, StructuralGateOutcome,
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
        let path = ObjectPath::tree_node(collection, node_id);
        // This read only selects the acquisition path. A missing root only
        // delays the work, so it needs no proof. Publication still needs the
        // gated observation and a source CAS.
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
        self.acquire_index_structural_gate(&path, id, acquisition)
            .await
    }

    /// Releases the structural gate of `id` on a source node.
    ///
    /// A state without the gate ends the release only if it satisfies
    /// `proof`. [`Requirement::ANY`] is enough when this cache holds a state
    /// with the gate: the caller acquired it here, or completed a read through
    /// this cache that showed it. The worker must not acquire this source gate
    /// again.
    pub(super) async fn release_structural_gate(
        &self,
        collection: &CollectionAddress,
        node_id: Option<&NodeId>,
        id: &TxId,
        proof: Requirement,
    ) -> Result<(), TransError> {
        let released = self
            .nodes
            .converge(
                &ObjectPath::tree_node(collection, node_id),
                proof,
                CasRetries::Bounded(NODE_CAS_ATTEMPTS),
                |mut node, _| {
                    ready(Ok::<_, TransError>(if node.remove_structural_gate(id) {
                        Decision::Write(node, ())
                    } else {
                        Decision::Done(())
                    }))
                },
            )
            .await?;
        match released {
            Converged::Written { .. } | Converged::Done(()) => Ok(()),
            Converged::Missing => Err(StorageError::NotFound.into()),
            Converged::Stop(()) | Converged::Exhausted => Err(TransError::Retry),
        }
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
    ///
    /// A target state that needs no change ends the call only if it satisfies
    /// `proof`.
    pub(super) async fn abandon_merge(
        &self,
        collection: &CollectionAddress,
        target: &MergeTarget,
        intent: &StructuralIntentId,
        proof: Requirement,
    ) -> Result<(), TransError> {
        self.update_node(collection, &target.node_id, proof, |node| {
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

    /// Removes the merge reservation of `intent` from node `node_id`, if
    /// present. A state without the reservation ends the call only if it
    /// satisfies `proof`.
    pub(super) async fn remove_merge_reservation(
        &self,
        collection: &CollectionAddress,
        node_id: &NodeId,
        intent: &StructuralIntentId,
        proof: Requirement,
    ) -> Result<(), TransError> {
        self.update_node(collection, node_id, proof, |node| {
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
            .commit_tx(TxRecord::new(*id, TxCommitStatus::Committed))
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
        // Proof: shared local knowledge (same cache, one acquisition). The
        // worker acquired the gate through this cache.
        let release = self
            .release_structural_gate(collection, node_id, id, Requirement::ANY)
            .await;
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
            .coordinate(StructuralGateOperation::new(*id, path.clone(), acquisition))
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

    /// Acquires the structural gate of the index node at `path` with a direct
    /// CAS. An index node has no entries, so only its node locks change.
    async fn acquire_index_structural_gate(
        &self,
        path: &ObjectPath,
        id: &TxId,
        acquisition: GateAcquisition,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        let reconciler =
            NodeLockReconciler::with_acquisition(&self.key_state, &self.mon, id, acquisition);
        let reconciler = &reconciler;
        let gated = self
            .nodes
            .converge(
                path,
                // Proof: conditional mutation. A cached gate of `id` can be
                // stale, because `fence_source_writer` can remove it. The next
                // CAS of the source then rejects the stale state.
                Requirement::ANY,
                CasRetries::Bounded(NODE_CAS_ATTEMPTS),
                move |mut node, observation| async move {
                    let gate = node.structural_gate();
                    if gate.lock_type() == LockType::Write && gate.contains(id) {
                        return Ok(Decision::Done(Some((node, observation))));
                    }
                    // The read that selected this path showed an index node, and
                    // an index node never becomes a leaf again.
                    if node.as_index().is_none() {
                        return Err(TransError::other(
                            "an index node became a leaf before its structural gate",
                        ));
                    }
                    let mut locks = node.locks().clone();
                    if reconciler.gate_locks(&mut locks).await?.is_some() {
                        return Ok(Decision::Stop(None));
                    }
                    node.set_locks(locks);
                    Ok::<_, TransError>(Decision::Write(node, None))
                },
            )
            .await?;
        Ok(match gated {
            // The CAS receipt holds the gated state itself. A re-read reports
            // whatever revision this database knows of by then, which the caller
            // would then pair with the body it gated.
            Converged::Written {
                state, observation, ..
            } => Some((state, observation)),
            Converged::Done(acquired) | Converged::Stop(acquired) => acquired,
            // A missing root defers the work, as in the read that selected
            // this path.
            Converged::Missing if matches!(path, ObjectPath::TreeRoot { .. }) => None,
            Converged::Missing => return Err(StorageError::NotFound.into()),
            Converged::Exhausted => None,
        })
    }

    /// Applies `change` to node `node_id`, retrying on a lost CAS. `change`
    /// returns `false` when the node needs no change, and that state ends the
    /// call only if it satisfies `proof`. A missing node needs no change.
    async fn update_node(
        &self,
        collection: &CollectionAddress,
        node_id: &NodeId,
        proof: Requirement,
        mut change: impl FnMut(&mut Node) -> bool,
    ) -> Result<(), TransError> {
        let updated = self
            .nodes
            .converge(
                &ObjectPath::tree_node(collection, Some(node_id)),
                proof,
                CasRetries::Bounded(NODE_CAS_ATTEMPTS),
                |mut node, _| {
                    ready(Ok::<_, TransError>(if change(&mut node) {
                        Decision::Write(node, ())
                    } else {
                        Decision::Done(())
                    }))
                },
            )
            .await?;
        match updated {
            Converged::Written { .. } | Converged::Done(()) | Converged::Missing => Ok(()),
            Converged::Stop(()) | Converged::Exhausted => Err(TransError::Retry),
        }
    }
}
