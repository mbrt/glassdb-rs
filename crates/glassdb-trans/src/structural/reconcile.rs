//! Parent reconciliation after structural changes (ADR-073).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use glassdb_data::{CollectionAddress, NodeToken, ObjectPath, TxId};
use glassdb_storage::{
    CurrentnessBarrier, IndexNode, LeafObservation, Node, Requirement, StorageError, Timeline,
    TreeRouter,
};

use crate::error::TransError;

use super::candidates::MaintenanceCandidates;
use super::nodes::{StructuralNodeAccess, node_token};
use super::split::SplitReason;

/// Upper bound on the deferred reconciliation queue. Dropping the oldest when
/// full is safe: its parent still routes correctly through right links, only
/// with extra hops.
pub(super) const DEFERRED_RECONCILIATION_CAP: usize = 4096;

/// Bounded attempts to reconcile a contended parent before deferring it to a
/// later sweep. Descent works meanwhile through right links.
const PARENT_RETRIES: usize = 8;

/// Safety bound on the child right-link hops walked by a parent
/// reconciliation, so a malformed or concurrently-mutated chain can never spin
/// the restructurer. A well-formed chain up to a key is far shorter than this.
const MAX_RECONCILE_HOPS: usize = 4096;

/// A parent reconciliation that could not change its parent on the first try
/// (a lost CAS): re-driven by a later [`Restructurer`](super::Restructurer)
/// sweep so the parent does not stay reliant on a right-link walk (ADR-073).
/// `target` is the node that covers `key` after the structural change, and
/// identifies the parent level.
#[derive(Clone)]
pub(super) struct PendingReconciliation {
    pub(super) collection: CollectionAddress,
    pub(super) key: Vec<u8>,
    pub(super) target: NodeToken,
}

pub(super) struct ParentReconciliation {
    pending: PendingReconciliation,
    pub(super) start: CurrentnessBarrier,
    retries_remaining: usize,
}

pub(super) enum ReconciliationOutcome {
    Reconciled,
    ParentRequiresSplit(ParentRequiresSplit),
}

pub(super) struct ParentRequiresSplit {
    pub(super) path: ObjectPath,
    pub(super) reason: SplitReason,
    pub(super) continuation: ParentSplitContinuation,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum ParentSplitContinuation {
    ResumeReconciliation,
    CompleteReconciliation,
}

/// Makes parents agree with the right-link chains of their children, and owns
/// the deferred retry queue (ADR-073).
#[derive(Clone)]
pub(super) struct ParentReconciler {
    structure: StructuralNodeAccess,
    router: TreeRouter,
    timeline: Timeline,
    candidates: MaintenanceCandidates,
    pending: Arc<Mutex<VecDeque<PendingReconciliation>>>,
}

impl ParentReconciler {
    pub(super) fn new(
        structure: StructuralNodeAccess,
        router: TreeRouter,
        timeline: Timeline,
        candidates: MaintenanceCandidates,
    ) -> Self {
        Self {
            structure,
            router,
            timeline,
            candidates,
            pending: Arc::new(Mutex::new(VecDeque::new())),
        }
    }

    /// Queues a reconciliation that a later sweep must re-drive. The oldest is
    /// dropped when full because descent remains correct through right-links.
    pub(super) fn defer(&self, reconciliation: PendingReconciliation) {
        let mut pending = self.pending.lock().unwrap();
        if pending.len() >= DEFERRED_RECONCILIATION_CAP {
            pending.pop_front();
        }
        pending.push_back(reconciliation);
    }

    /// Drains the deferred reconciliations for one sweep.
    pub(super) fn drain_pending(&self) -> Vec<PendingReconciliation> {
        self.pending.lock().unwrap().drain(..).collect()
    }

    /// Starts the reconciliation of the parent that routes `key` to `target`,
    /// after a structural change that `target` now covers `key` for.
    pub(super) fn begin(
        &self,
        collection: &CollectionAddress,
        key: &[u8],
        target: &NodeToken,
    ) -> ParentReconciliation {
        ParentReconciliation {
            pending: PendingReconciliation {
                collection: collection.clone(),
                key: key.to_vec(),
                target: target.clone(),
            },
            start: self.timeline.currentness_barrier(),
            retries_remaining: PARENT_RETRIES,
        }
    }

    /// Makes the parent agree with the chain of its children through the key,
    /// or requests the parent split needed to continue safely.
    pub(super) async fn reconcile(
        &self,
        reconciliation: &mut ParentReconciliation,
    ) -> Result<ReconciliationOutcome, TransError> {
        while reconciliation.retries_remaining > 0 {
            reconciliation.retries_remaining -= 1;
            let pending = &reconciliation.pending;
            let Some(parent) = self
                .router
                .parent_of(
                    &pending.collection,
                    &pending.key,
                    &pending.target,
                    Requirement::after(reconciliation.start),
                )
                .await?
            else {
                return Ok(ReconciliationOutcome::Reconciled);
            };
            let parent_token = match &parent.path {
                ObjectPath::TreeRoot { .. } => None,
                ObjectPath::Node { token, .. } => Some(token.clone()),
                _ => return Err(TransError::other("router returned a non-node parent path")),
            };
            let Some((lock_id, locked_parent, observation)) = self
                .structure
                .begin_gated_worker(&pending.collection, parent_token.as_ref())
                .await?
            else {
                continue;
            };
            let reconciled = self
                .reconcile_gated_parent(
                    pending,
                    &parent.path,
                    &locked_parent,
                    &observation,
                    &lock_id,
                    reconciliation.start,
                )
                .await;
            let released = self
                .structure
                .finish_gated_worker(&pending.collection, parent_token.as_ref(), &lock_id)
                .await;
            match reconciled? {
                Some(outcome) => {
                    released?;
                    return Ok(outcome);
                }
                None => {
                    let _ = released;
                    continue;
                }
            }
        }

        self.defer(reconciliation.pending.clone());
        Err(TransError::Retry)
    }

    /// Returns the children on the right-link chain from the one that `index`
    /// routes keys just below `key` to, through the one that covers `key`.
    pub(super) async fn child_chain(
        &self,
        collection: &CollectionAddress,
        index: &IndexNode,
        key: &[u8],
        barrier: CurrentnessBarrier,
    ) -> Result<Vec<(NodeToken, Node)>, TransError> {
        // The structural change was observed before reconciliation began. A
        // parent's watermark cannot establish that the child reads follow that
        // completed work.
        let requirement = Requirement::after(barrier);
        let Some(first) = index.child_before(key) else {
            return Ok(Vec::new());
        };
        let mut token = node_token(first)?;
        let mut chain = Vec::new();
        for _ in 0..MAX_RECONCILE_HOPS {
            let child = self.router.leaf_at(collection, &token, requirement).await?;
            let node = child.node().ok_or(StorageError::NotFound)?.clone();
            let right = node.right_sibling().map(node_token).transpose()?;
            let covers = node.covers(key);
            chain.push((token, node));
            match right {
                Some(right) if !covers => token = right,
                _ => return Ok(chain),
            }
        }
        Err(TransError::other(
            "parent reconciliation exceeded the right-link hop bound",
        ))
    }

    /// Reconciles the gated parent with the chain of its children through the
    /// key.
    ///
    /// `None` reports a lost CAS, which the caller retries. The gate stays
    /// installed on every path, because the caller releases it once.
    async fn reconcile_gated_parent(
        &self,
        pending: &PendingReconciliation,
        parent_path: &ObjectPath,
        parent: &Node,
        observation: &LeafObservation,
        lock_id: &TxId,
        barrier: CurrentnessBarrier,
    ) -> Result<Option<ReconciliationOutcome>, TransError> {
        let Some(index) = parent.as_index() else {
            return Ok(Some(ReconciliationOutcome::Reconciled));
        };
        let chain = self
            .child_chain(&pending.collection, index, &pending.key, barrier)
            .await?;
        let path: Vec<(&str, &Node)> = chain
            .iter()
            .map(|(token, node)| (token.as_str(), node))
            .collect();
        let mut new_index = index.clone();
        new_index.reconcile(&path);
        if new_index == *index {
            return Ok(Some(ReconciliationOutcome::Reconciled));
        }
        let mut updated = parent.clone();
        updated.set_index(new_index)?;
        let policy = self.candidates.policy();
        if updated.content_encoded_len() > policy.content_limit()
            || updated.encoded_len() > policy.node_max_bytes()
        {
            if index.len() >= 2 {
                return Ok(Some(ReconciliationOutcome::ParentRequiresSplit(
                    ParentRequiresSplit {
                        path: parent_path.clone(),
                        reason: SplitReason::Capacity,
                        continuation: ParentSplitContinuation::ResumeReconciliation,
                    },
                )));
            }
            return Err(TransError::InvalidInput(
                "separator exceeds the coordination node size limit".into(),
            ));
        }

        updated.remove_structural_gate(lock_id);
        if self
            .structure
            .store_structural_node(&updated, observation)
            .await?
            .is_none()
        {
            return Ok(None);
        }
        if updated.over_soft_cap(policy) {
            return Ok(Some(ReconciliationOutcome::ParentRequiresSplit(
                ParentRequiresSplit {
                    path: parent_path.clone(),
                    reason: SplitReason::SoftCap,
                    continuation: ParentSplitContinuation::CompleteReconciliation,
                },
            )));
        }
        self.candidates.observe_index(parent_path, &updated);
        Ok(Some(ReconciliationOutcome::Reconciled))
    }
}
