//! Parent reconciliation after structural changes (ADR-073).

use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

use glassdb_concurr::shard;
use glassdb_data::{CollectionAddress, NodeId, ObjectPath, TxId};
use glassdb_storage::{
    CurrentnessBarrier, IndexNode, LeafObservation, Node, Requirement, StorageError, Timeline,
    TreeRouter,
};
use tokio::sync::Semaphore;

use crate::error::TransError;

use super::candidates::MaintenanceCandidates;
use super::nodes::StructuralNodeAccess;
use super::split::SplitReason;

/// Upper bound on the deferred reconciliation queue. Dropping the oldest when
/// full is safe: its parent still routes correctly through right links, only
/// with extra hops.
pub(super) const DEFERRED_RECONCILIATION_CAP: usize = 4096;

/// The groups of collections whose reconciliations in this database instance
/// take turns. Collections of one group also take turns with each other,
/// which only delays them. Must be a power of two.
const COLLECTION_TURNS: usize = 16;

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
    pub(super) target: NodeId,
}

/// The reconciliation of the parents of one or more routes of one collection.
pub(super) struct ParentReconciliation {
    collection: CollectionAddress,
    // The routes whose parents do not agree with them yet, as the key and
    // the node that covers it after the structural change.
    routes: Vec<(Vec<u8>, NodeId)>,
    pub(super) start: CurrentnessBarrier,
    retries_remaining: usize,
}

pub(super) enum ReconciliationOutcome {
    Reconciled,
    /// The parent must split before the reconciliation can continue. The
    /// caller splits it, and then continues the reconciliation.
    ParentRequiresSplit(ParentRequiresSplit),
}

pub(super) struct ParentRequiresSplit {
    pub(super) path: ObjectPath,
    pub(super) reason: SplitReason,
}

impl ParentReconciliation {
    /// Returns the keys of the routes whose parents do not agree with them
    /// yet.
    pub(super) fn unresolved_keys(&self) -> impl Iterator<Item = &[u8]> {
        self.routes.iter().map(|(key, _)| key.as_slice())
    }
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
    // The sweep, the deferred reconciliations, and structural recovery
    // reconcile the same parents. Without turns, they lose the gates of the
    // parents to each other, and each loss sends changes to recovery.
    collection_turns: Arc<[Semaphore]>,
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
            collection_turns: (0..COLLECTION_TURNS).map(|_| Semaphore::new(1)).collect(),
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

    /// Starts the reconciliation of the parents that route each key of
    /// `routes` to its node, after the structural changes that made the node
    /// cover the key.
    pub(super) fn begin(
        &self,
        collection: &CollectionAddress,
        routes: impl IntoIterator<Item = (Vec<u8>, NodeId)>,
    ) -> ParentReconciliation {
        ParentReconciliation {
            collection: collection.clone(),
            routes: routes.into_iter().collect(),
            start: self.timeline.currentness_barrier(),
            retries_remaining: PARENT_RETRIES,
        }
    }

    /// Makes the parents agree with the chains of their children through
    /// the keys, or requests the parent split needed to continue safely.
    ///
    /// The routes of one parent change it with one gate and one CAS. The
    /// sibling splits of one sweep have the same parent, and they otherwise
    /// lose the gate of the parent to each other. The reconciliations of one
    /// collection in this database instance take turns for the same reason.
    /// A turn ends before the parent split that the outcome can request, so
    /// that the reconciliation of that split can take its own turn.
    pub(super) async fn reconcile(
        &self,
        reconciliation: &mut ParentReconciliation,
    ) -> Result<ReconciliationOutcome, TransError> {
        let turns = shard::index(
            reconciliation.collection.physical_prefix().as_bytes(),
            COLLECTION_TURNS,
        );
        let _turn = self.collection_turns[turns]
            .acquire()
            .await
            .map_err(|_| TransError::other("reconciliation turns are closed"))?;
        while !reconciliation.routes.is_empty() && reconciliation.retries_remaining > 0 {
            reconciliation.retries_remaining -= 1;
            let Some((parent, keys)) = self.next_parent(reconciliation).await? else {
                continue;
            };
            let parent_id = match &parent {
                ObjectPath::TreeRoot { .. } => None,
                ObjectPath::Node { id, .. } => Some(*id),
                _ => return Err(TransError::other("router returned a non-node parent path")),
            };
            let collection = &reconciliation.collection;
            let Some((lock_id, locked_parent, observation)) = self
                .structure
                .begin_gated_worker(collection, parent_id.as_ref())
                .await?
            else {
                continue;
            };
            let key_refs: Vec<&[u8]> = keys.iter().map(Vec::as_slice).collect();
            let reconciled = self
                .reconcile_gated_parent(
                    collection,
                    &key_refs,
                    &parent,
                    &locked_parent,
                    &observation,
                    &lock_id,
                    reconciliation.start,
                )
                .await;
            let released = self
                .structure
                .finish_gated_worker(collection, parent_id.as_ref(), &lock_id)
                .await;
            let Some(gated) = reconciled? else {
                let _ = released;
                continue;
            };
            released?;
            match gated {
                GatedOutcome::Landed(outcome) => {
                    reconciliation.routes.retain(|(key, _)| !keys.contains(key));
                    reconciliation.retries_remaining = PARENT_RETRIES;
                    if let Some(split) = outcome {
                        return Ok(ReconciliationOutcome::ParentRequiresSplit(split));
                    }
                }
                GatedOutcome::SplitFirst(split) => {
                    return Ok(ReconciliationOutcome::ParentRequiresSplit(split));
                }
            }
        }
        if reconciliation.routes.is_empty() {
            return Ok(ReconciliationOutcome::Reconciled);
        }
        for (key, target) in &reconciliation.routes {
            self.defer(PendingReconciliation {
                collection: reconciliation.collection.clone(),
                key: key.clone(),
                target: *target,
            });
        }
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
    ) -> Result<Vec<(NodeId, Node)>, TransError> {
        // The structural change was observed before reconciliation began. A
        // parent's watermark cannot establish that the child reads follow that
        // completed work.
        let requirement = Requirement::after(barrier);
        let Some(first) = index.child_before(key) else {
            return Ok(Vec::new());
        };
        let mut id = first;
        let mut chain = Vec::new();
        for _ in 0..MAX_RECONCILE_HOPS {
            let child = self.router.leaf_at(collection, &id, requirement).await?;
            let node = child.node().ok_or(StorageError::NotFound)?.clone();
            let right = node.right_sibling();
            let covers = node.covers(key);
            chain.push((id, node));
            match right {
                Some(right) if !covers => id = right,
                _ => return Ok(chain),
            }
        }
        Err(TransError::other(
            "parent reconciliation exceeded the right-link hop bound",
        ))
    }

    /// Returns a parent of the routes, and the keys of the routes that it is
    /// the parent of. Removes the routes that no parent routes to their node
    /// any more. `None` reports that no route remains.
    async fn next_parent(
        &self,
        reconciliation: &mut ParentReconciliation,
    ) -> Result<Option<(ObjectPath, Vec<Vec<u8>>)>, TransError> {
        let requirement = Requirement::after(reconciliation.start);
        let mut parents = Vec::with_capacity(reconciliation.routes.len());
        for (key, target) in &reconciliation.routes {
            let parent = self
                .router
                .parent_of(&reconciliation.collection, key, target, requirement)
                .await?;
            parents.push(parent.map(|parent| parent.path));
        }
        let mut parents = parents.into_iter();
        let mut first = None;
        let mut keys = Vec::new();
        reconciliation.routes.retain(|(key, _)| {
            let Some(parent) = parents.next().flatten() else {
                return false;
            };
            let first = first.get_or_insert_with(|| parent.clone());
            if *first == parent {
                keys.push(key.clone());
            }
            true
        });
        Ok(first.map(|first| (first, keys)))
    }

    /// Reconciles the gated parent with the chains of its children through
    /// each of `keys`.
    ///
    /// `None` reports a lost CAS, which the caller retries. The gate stays
    /// installed on every path, because the caller releases it once.
    #[allow(clippy::too_many_arguments)]
    async fn reconcile_gated_parent(
        &self,
        collection: &CollectionAddress,
        keys: &[&[u8]],
        parent_path: &ObjectPath,
        parent: &Node,
        observation: &LeafObservation,
        lock_id: &TxId,
        barrier: CurrentnessBarrier,
    ) -> Result<Option<GatedOutcome>, TransError> {
        let Some(index) = parent.as_index() else {
            return Ok(Some(GatedOutcome::Landed(None)));
        };
        let mut new_index = index.clone();
        for key in keys {
            let chain = self.child_chain(collection, index, key, barrier).await?;
            let path: Vec<(NodeId, &Node)> = chain.iter().map(|(id, node)| (*id, node)).collect();
            new_index.reconcile(&path);
        }
        if new_index == *index {
            return Ok(Some(GatedOutcome::Landed(None)));
        }
        let mut updated = parent.clone();
        updated.set_index(new_index)?;
        let policy = self.candidates.policy();
        if updated.content_encoded_len() > policy.content_limit()
            || updated.encoded_len() > policy.node_max_bytes()
        {
            if index.len() >= 2 {
                return Ok(Some(GatedOutcome::SplitFirst(ParentRequiresSplit {
                    path: parent_path.clone(),
                    reason: SplitReason::Capacity,
                })));
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
            return Ok(Some(GatedOutcome::Landed(Some(ParentRequiresSplit {
                path: parent_path.clone(),
                reason: SplitReason::SoftCap,
            }))));
        }
        self.candidates.observe_index(parent_path, &updated);
        Ok(Some(GatedOutcome::Landed(None)))
    }
}

/// The result of one reconciliation of a gated parent.
enum GatedOutcome {
    /// The parent agrees with the routes. It must split after, if it holds a
    /// split request.
    Landed(Option<ParentRequiresSplit>),
    /// The parent must split before it can agree with the routes.
    SplitFirst(ParentRequiresSplit),
}
