//! Background growth of the B-link coordination tree by leaf and node splits
//! (ADR-031).
//!
//! A leaf that crosses its soft cap is halved so no single object becomes a
//! scalability or contention bottleneck. Splitting runs off the hot path in a
//! periodic background task, fed candidates from stored over-cap leaves,
//! capacity rejections, and inline admission misses — never a key-space
//! enumeration.
//!
//! Every split is a sequence of independent, idempotent compare-and-swaps under
//! a one-node structural gate. Before joining collection topology in `_i`, it
//! writes a `Preparing` intent below its topology participant's `_s` prefix.
//! After taking the source gate it conditionally advances that intent to
//! `Ready`; only then may it create nodes. A lifecycle freeze can therefore
//! find exactly one participant's work and cancel an unadvanced intent without
//! racing late node creation:
//!
//! 0. Advance the structural intent with the source observation and split key;
//!    its created-node IDs were reserved while `Preparing`.
//! 1. Create the right sibling (`write_if_not_exists`) holding the upper half
//!    and inheriting the source's former high-key and right-sibling.
//! 2. **Shrink the source in one CAS** — drop the upper half, set high-key to
//!    the split key, link to the sibling. This is the linearization point:
//!    descent now finds the moved keys by stepping right, and a concurrent
//!    locker that retained the pre-shrink observation loses its CAS and
//!    re-routes (ADR-031 coverage re-check).
//! 3. Reconcile the parent with the right-link chain of its children so
//!    future descents skip the right-link hop (ADR-073); recurse when the
//!    parent itself overflows. Purely an optimization — correctness never
//!    depends on it landing.
//!
//! A leaf split, including a root-leaf split, acquires a structural gate
//! through the shared [`LeafCoordinator`](crate::leaf_coord::LeafCoordinator),
//! in the same batched CAS stream as data mutations on that leaf. Interior
//! indexes use direct structural CASes. The source shrink (or root rewrite)
//! releases the structural gate inline, so no unlocked post-split state is
//! exposed before a separate release CAS. Once a leaf is quiescent behind that
//! gate, holder-free tombstones are removed before the final reason check. The
//! compacted leaf either cancels the split in one CAS or supplies the ordinary
//! recoverable split outputs (ADR-062).
//!
//! The tree root `_r` cannot move (its address is fixed), so when it
//! overflows it splits **in place**: two children are created and the root is
//! rewritten into a two-entry index over them, growing the tree's height while
//! leaving the independent collection record untouched.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use glassdb_data::{CollectionAddress, NodeId, ObjectPath, TxId};
use glassdb_storage::{
    IndexNode, LeafEntry, LeafObservation, Node, NodeStore, Requirement, StorageError, Timeline,
    TreeRouter,
};

use crate::error::TransError;

use super::candidates::{CandidateCause, MaintenanceCandidate, MaintenanceCandidates};
use super::change::{Applied, ParentRoute, Prepared};
use super::reclamation::{ReclamationReporter, reclaim_holder_free_tombstones};
use super::recovery::{PreparedIntent, ReadyChange};
use super::stats::Stats;

/// Decides which splits are necessary, and writes their nodes.
#[derive(Clone)]
pub(super) struct Splitter {
    nodes: NodeStore,
    router: TreeRouter,
    timeline: Timeline,
    // Supplies the size policies, and receives the split outputs that are
    // still over their soft cap.
    candidates: MaintenanceCandidates,
    stats: Arc<Stats>,
    reclamation: ReclamationReporter,
}

/// Why a node may need a split.
#[derive(Clone)]
pub(super) enum SplitReason {
    SoftCap,
    Capacity,
    InlinePressure { key: Vec<u8>, value_len: usize },
}

/// Whether a node still needs the split that its reason asked for.
pub(super) enum SplitNeed {
    Split,
    NotActionable,
    Reroute,
}

/// The node that a split divides.
#[derive(Clone, Copy)]
pub(super) enum SplitTarget<'a> {
    Root,
    NonRoot(&'a NodeId),
}

/// The node writes of one split. The source gate protects them until the
/// source shrink or the root rewrite lands.
pub(super) enum SplitPlan {
    NonRoot {
        source_id: NodeId,
        source: Node,
        right_id: NodeId,
        right: Node,
        split_key: Vec<u8>,
        reclaimed: Vec<TxId>,
    },
    Root {
        left_id: NodeId,
        right_id: NodeId,
        left: Node,
        right: Node,
        index: Node,
        reclaimed: Vec<TxId>,
    },
}

impl Splitter {
    pub(super) fn new(
        nodes: NodeStore,
        router: TreeRouter,
        timeline: Timeline,
        candidates: MaintenanceCandidates,
        stats: Arc<Stats>,
        reclamation: ReclamationReporter,
    ) -> Self {
        Self {
            nodes,
            router,
            timeline,
            candidates,
            stats,
            reclamation,
        }
    }

    /// Finds the node that a split candidate asks to split: the node of the
    /// candidate, or for inline pressure the leaf that covers the key now.
    /// `None` tells that the candidate is no longer actionable.
    pub(super) async fn locate(
        &self,
        path: &ObjectPath,
        reason: &SplitReason,
    ) -> Result<Option<ObjectPath>, TransError> {
        let SplitReason::InlinePressure { key, .. } = reason else {
            return Ok(Some(path.clone()));
        };
        let (ObjectPath::TreeRoot { collection } | ObjectPath::Node { collection, .. }) = path
        else {
            return Err(TransError::other("split candidate is not a tree node"));
        };
        let located = match self
            .router
            .route_key(
                collection,
                key,
                Requirement::after(self.timeline.currentness_barrier()),
            )
            .await
        {
            Ok(located) => located,
            Err(StorageError::NotFound) => {
                self.stats
                    .inline_pressure_discarded
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        match located
            .node()
            .map(|node| self.need(node, reason))
            .unwrap_or(SplitNeed::NotActionable)
        {
            SplitNeed::Split => Ok(Some(located.path)),
            SplitNeed::NotActionable => {
                self.stats
                    .inline_pressure_discarded
                    .fetch_add(1, Ordering::Relaxed);
                Ok(None)
            }
            SplitNeed::Reroute => Err(TransError::Retry),
        }
    }

    /// Classifies whether `node` still needs the split represented by `reason`.
    pub(super) fn need(&self, node: &Node, reason: &SplitReason) -> SplitNeed {
        match reason {
            SplitReason::Capacity => {
                // The rejected operation is retried by its owner. A hint only
                // asks for one split of a divisible node, even below soft caps.
                if node.as_leaf().is_some_and(|leaf| leaf.len() >= 2)
                    || node.as_index().is_some_and(|index| index.len() >= 2)
                {
                    SplitNeed::Split
                } else {
                    SplitNeed::NotActionable
                }
            }
            SplitReason::SoftCap => {
                if node.over_soft_cap(self.candidates.policy()) {
                    SplitNeed::Split
                } else {
                    SplitNeed::NotActionable
                }
            }
            SplitReason::InlinePressure { key, value_len } => {
                let Some(leaf) = node.as_leaf() else {
                    return SplitNeed::Reroute;
                };
                if !node.covers(key) {
                    return SplitNeed::Reroute;
                }
                let inline = self.candidates.inline();
                if !inline.admits_value(*value_len)
                    || leaf.len() < 2
                    || !leaf.lookup(key).is_some_and(LeafEntry::exists)
                {
                    return SplitNeed::NotActionable;
                }
                let other_inline_bytes = leaf
                    .entries()
                    .filter(|entry| entry.key.as_slice() != key.as_slice())
                    .map(|entry| entry.current.inline_len())
                    .sum();
                if inline.admits(other_inline_bytes, *value_len) {
                    SplitNeed::NotActionable
                } else {
                    SplitNeed::Split
                }
            }
        }
    }

    /// Checks the split of the gated source `node` again, and plans its node
    /// writes into the nodes that `prepared` reserved. Holder-free tombstones
    /// are removed first, and can make the split unnecessary (ADR-062).
    pub(super) fn prepare(
        &self,
        target: SplitTarget<'_>,
        reason: &SplitReason,
        worker: &TxId,
        mut node: Node,
        prepared: &PreparedIntent,
    ) -> Prepared<SplitPlan> {
        match self.need(&node, reason) {
            SplitNeed::Split => {}
            need => return self.cancel(reason, need),
        }
        let reclaimed = reclaim_holder_free_tombstones(&mut node);
        match self.need(&node, reason) {
            SplitNeed::Split => {}
            SplitNeed::NotActionable if !reclaimed.is_empty() => {
                return Prepared::ReclaimOnly { node, reclaimed };
            }
            need => return self.cancel(reason, need),
        }
        match target {
            SplitTarget::NonRoot(id) => {
                self.plan_nonroot_split(id, prepared, node, worker, reclaimed)
            }
            SplitTarget::Root => self.plan_root_split(prepared, &node, worker, reclaimed),
        }
    }

    /// Writes the planned nodes of a split whose intent is Ready.
    pub(super) async fn apply(
        &self,
        collection: &CollectionAddress,
        reason: &SplitReason,
        observation: &LeafObservation,
        plan: SplitPlan,
    ) -> Result<Applied, TransError> {
        match plan {
            SplitPlan::NonRoot {
                source_id,
                source,
                right_id,
                right,
                split_key,
                reclaimed,
            } => {
                self.create_node(collection, &right_id, &right).await?;
                if !self
                    .nodes
                    .store_node(collection, &source_id, &source, Some(observation))
                    .await?
                {
                    return Err(TransError::Retry);
                }
                self.record_completed_split(
                    collection,
                    reason,
                    &reclaimed,
                    [(&source_id, &source), (&right_id, &right)],
                );
                Ok(Applied::Landed(Some(ParentRoute {
                    key: split_key,
                    target: right_id,
                })))
            }
            SplitPlan::Root {
                left_id,
                right_id,
                left,
                right,
                index,
                reclaimed,
            } => {
                self.create_node(collection, &left_id, &left).await?;
                self.create_node(collection, &right_id, &right).await?;
                if self
                    .nodes
                    .store_node_at(observation.path(), &index, observation)
                    .await?
                    .is_none()
                {
                    return Err(TransError::Retry);
                }
                self.record_completed_split(
                    collection,
                    reason,
                    &reclaimed,
                    [(&left_id, &left), (&right_id, &right)],
                );
                Ok(Applied::Landed(None))
            }
        }
    }

    /// Cancels a split that the check of the gated source no longer calls
    /// for.
    fn cancel(&self, reason: &SplitReason, need: SplitNeed) -> Prepared<SplitPlan> {
        let result = match need {
            SplitNeed::NotActionable => {
                if reason.is_inline_pressure() {
                    self.stats
                        .inline_pressure_discarded
                        .fetch_add(1, Ordering::Relaxed);
                }
                Ok(())
            }
            SplitNeed::Reroute => Err(TransError::Retry),
            SplitNeed::Split => unreachable!("a required split must remain in coordination"),
        };
        Prepared::Cancel(result)
    }

    /// Splits a non-root source into itself and the reserved right sibling.
    fn plan_nonroot_split(
        &self,
        source_id: &NodeId,
        prepared: &PreparedIntent,
        mut source: Node,
        worker: &TxId,
        reclaimed: Vec<TxId>,
    ) -> Prepared<SplitPlan> {
        let right_id = prepared
            .nonroot_sibling()
            .expect("a prepared non-root intent always reserves one sibling");
        let Some((right, split_key)) = source.split(right_id) else {
            return Prepared::Cancel(Ok(()));
        };
        source.remove_structural_gate(worker);
        Prepared::Ready {
            change: ReadyChange::Split {
                split_key: split_key.clone(),
            },
            plan: SplitPlan::NonRoot {
                source_id: *source_id,
                source,
                right_id,
                right,
                split_key,
                reclaimed,
            },
        }
    }

    /// Builds both root children and the replacement root index.
    fn plan_root_split(
        &self,
        prepared: &PreparedIntent,
        node: &Node,
        worker: &TxId,
        reclaimed: Vec<TxId>,
    ) -> Prepared<SplitPlan> {
        let (left_id, right_id) = prepared
            .root_children()
            .expect("a prepared root intent always reserves two children");
        let (left, right, split_key) = split_into_children(node, right_id, worker);
        let index = Node::index(IndexNode::from_children([
            (Vec::new(), left_id),
            (split_key.clone(), right_id),
        ]));
        let policy = self.candidates.policy();
        if index.content_encoded_len() > policy.content_limit()
            || index.encoded_len() > policy.node_max_bytes()
        {
            return Prepared::Cancel(Err(TransError::InvalidInput(
                "root index exceeds the coordination node size limit".into(),
            )));
        }
        Prepared::Ready {
            change: ReadyChange::Split { split_key },
            plan: SplitPlan::Root {
                left_id,
                right_id,
                left,
                right,
                index,
                reclaimed,
            },
        }
    }

    /// Creates one immutable child reserved by a structural split.
    async fn create_node(
        &self,
        collection: &CollectionAddress,
        id: &NodeId,
        node: &Node,
    ) -> Result<(), TransError> {
        if self.nodes.store_node(collection, id, node, None).await? {
            Ok(())
        } else {
            Err(TransError::Retry)
        }
    }

    /// Publishes statistics, follow-up candidates, and GC hints after the
    /// source/root linearization is acknowledged.
    fn record_completed_split(
        &self,
        collection: &CollectionAddress,
        reason: &SplitReason,
        reclaimed: &[TxId],
        outputs: [(&NodeId, &Node); 2],
    ) {
        self.reclamation.record(reclaimed, false);
        self.stats.splits.fetch_add(1, Ordering::Relaxed);
        for (id, node) in outputs {
            self.enqueue_if_over_soft_cap(collection, id, node);
        }
        if reason.is_inline_pressure() {
            self.stats
                .inline_pressure_completed
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Carries an oversized split output into a later sweep so one hint can
    /// drive the whole split cascade.
    fn enqueue_if_over_soft_cap(&self, collection: &CollectionAddress, id: &NodeId, node: &Node) {
        if !node.over_soft_cap(self.candidates.policy()) {
            return;
        }
        self.candidates.push(MaintenanceCandidate {
            path: ObjectPath::Node {
                collection: collection.clone(),
                id: *id,
            },
            priority: self.candidates.new_id(),
            cause: CandidateCause::Split(SplitReason::SoftCap),
        });
    }
}

impl SplitReason {
    pub(super) fn class(&self) -> u8 {
        match self {
            SplitReason::SoftCap => 0,
            SplitReason::InlinePressure { .. } => 1,
            SplitReason::Capacity => 2,
        }
    }

    pub(super) fn is_inline_pressure(&self) -> bool {
        matches!(self, SplitReason::InlinePressure { .. })
    }
}

impl<'a> SplitTarget<'a> {
    pub(super) fn source_node_id(self) -> Option<&'a NodeId> {
        match self {
            Self::Root => None,
            Self::NonRoot(id) => Some(id),
        }
    }
}

/// Splits `node` (a root leaf or root index) into a lower and an upper child for
/// an in-place root split, returning `(left, right, split_key)`. `left` links to
/// `right_id`; `right` inherits `node`'s former bounds.
fn split_into_children(
    node: &Node,
    right_id: NodeId,
    structure_holder: &TxId,
) -> (Node, Node, Vec<u8>) {
    let mut source = node.clone();
    let (right, split_key) = source
        .split(right_id)
        .expect("a split source has at least two entries/children");
    source.remove_structural_gate(structure_holder);
    (source, right, split_key)
}
