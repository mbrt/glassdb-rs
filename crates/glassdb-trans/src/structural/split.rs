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
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::{CollectionAddress, NodeId, ObjectPath, StructuralIntentId, TxId};
use glassdb_storage::{
    IndexNode, LeafEntry, LeafObservation, Node, NodeStore, PaidChange, Payment, Requirement,
    StorageError, Timeline, TreeRouter,
};

use crate::error::TransError;

use super::candidates::{CandidateCause, MaintenanceCandidate, MaintenanceCandidates};
use super::change::{Applied, ParentRoute, Prepared};
use super::measurements::ChangeKind;
use super::reclamation::{ReclamationReporter, reclaim_holder_free_tombstones};
use super::recovery::{PreparedIntent, ReadyChange};
use super::stats::Stats;
use super::write_spans::{self, SoftCapSplit};

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
    InlinePressure {
        key: Vec<u8>,
        value_len: usize,
    },
    /// A topology rule decided that the leaf splits (ADR-074). The split is
    /// at the key for which the avoidable time of the leaf was measured, or at
    /// the median without one. The leaf can change before the split, so a
    /// measured median can differ from the median at the split.
    Demand {
        at: Option<Vec<u8>>,
        /// The avoidable time in each second that pays for the split.
        rate: Duration,
    },
    /// This database instance loses leaf CASes on the contended key `key` to
    /// other database instances, so the leaf of the key splits away from
    /// their keys (ADR-085).
    Contention {
        key: Vec<u8>,
    },
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
// One value exists for each split, and only while it runs, so the size of its
// largest variant costs nothing.
#[allow(clippy::large_enum_variant)]
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
    /// candidate, or for a split for a key the leaf that covers the key now.
    /// `None` tells that the candidate is no longer actionable.
    pub(super) async fn locate(
        &self,
        path: &ObjectPath,
        reason: &SplitReason,
    ) -> Result<Option<ObjectPath>, TransError> {
        let (ObjectPath::TreeRoot { collection } | ObjectPath::Node { collection, .. }) = path
        else {
            return Err(TransError::other("split candidate is not a tree node"));
        };
        let Some(key) = reason.key() else {
            return self
                .soft_cap_split_waits(collection, path, reason)
                .await
                .map(|waits| (!waits).then(|| path.clone()));
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
                self.record_discarded(reason);
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
                self.record_discarded(reason);
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
            SplitReason::Demand { at, rate } => {
                let divisible = node.as_leaf().is_some_and(|leaf| match at {
                    Some(key) => leaf.divides_at(key),
                    None => leaf.len() >= 2,
                });
                let pays = self.candidates.paid_rates().pays_more(
                    PaidChange::Split,
                    *rate,
                    [node.paid_rate()],
                );
                if divisible && pays {
                    SplitNeed::Split
                } else {
                    SplitNeed::NotActionable
                }
            }
            SplitReason::Contention { key } => {
                if node.as_leaf().is_none() || !node.covers(key) {
                    return SplitNeed::Reroute;
                }
                if self.contention_split_key(node, key).is_some() {
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
        let at = match reason {
            SplitReason::SoftCap => match self.soft_cap_split(target, &node) {
                SoftCapSplit::At(key) => Some(key),
                SoftCapSplit::Median => None,
                SoftCapSplit::Wait if reclaimed.is_empty() => {
                    return self.cancel(reason, SplitNeed::NotActionable);
                }
                SoftCapSplit::Wait => return Prepared::ReclaimOnly { node, reclaimed },
            },
            // The writers of the leaf can stop being recent after the check
            // above, and a contention split never falls back to the median.
            SplitReason::Contention { key } => match self.contention_split_key(&node, key) {
                Some(at) => Some(at),
                None if reclaimed.is_empty() => {
                    return self.cancel(reason, SplitNeed::NotActionable);
                }
                None => return Prepared::ReclaimOnly { node, reclaimed },
            },
            _ => None,
        };
        let split = Split {
            reason,
            at: at.as_deref(),
        };
        match target {
            SplitTarget::NonRoot(id) => {
                self.plan_nonroot_split(id, split, prepared, node, worker, reclaimed)
            }
            SplitTarget::Root => self.plan_root_split(split, prepared, &node, worker, reclaimed),
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

    /// Reports whether the soft cap split of the non-root leaf at `path` waits
    /// for its write spans in cached state, so that it takes no structural gate
    /// (ADR-084). Each write of the leaf asks for the split again. A leaf with
    /// holder-free tombstones does not wait, so that the split reclaims them.
    async fn soft_cap_split_waits(
        &self,
        collection: &CollectionAddress,
        path: &ObjectPath,
        reason: &SplitReason,
    ) -> Result<bool, TransError> {
        let (SplitReason::SoftCap, true, ObjectPath::Node { id, .. }) =
            (reason, self.candidates.keeps_write_spans(), path)
        else {
            return Ok(false);
        };
        let mut node = match self.nodes.load_node(collection, id, Requirement::ANY).await {
            Ok((node, _)) => node,
            Err(StorageError::NotFound) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let reclaimable = !reclaim_holder_free_tombstones(&mut node).is_empty();
        Ok(!reclaimable
            && node.over_soft_cap(self.candidates.policy())
            && self.soft_cap_split(SplitTarget::NonRoot(id), &node) == SoftCapSplit::Wait)
    }

    /// Returns where the soft cap split of the leaf `node` divides it, so that
    /// it keeps the write spans of the leaf (ADR-084). A root leaf splits at
    /// its median: it is the whole collection, and its split cannot defer in
    /// cached state.
    fn soft_cap_split(&self, target: SplitTarget<'_>, node: &Node) -> SoftCapSplit {
        match target {
            SplitTarget::NonRoot(_) if self.candidates.keeps_write_spans() => {
                write_spans::soft_cap_split(node, self.candidates.policy(), rt::system_now())
            }
            _ => SoftCapSplit::Median,
        }
    }

    /// Cancels a split that the check of the gated source no longer calls
    /// for.
    fn cancel(&self, reason: &SplitReason, need: SplitNeed) -> Prepared<SplitPlan> {
        let result = match need {
            SplitNeed::NotActionable => {
                self.record_discarded(reason);
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
        split: Split<'_>,
        prepared: &PreparedIntent,
        mut source: Node,
        worker: &TxId,
        reclaimed: Vec<TxId>,
    ) -> Prepared<SplitPlan> {
        let right_id = prepared
            .nonroot_sibling()
            .expect("a prepared non-root intent always reserves one sibling");
        let Some((right, split_key)) = split.divide(&mut source, right_id, prepared.id()) else {
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
        split: Split<'_>,
        prepared: &PreparedIntent,
        node: &Node,
        worker: &TxId,
        reclaimed: Vec<TxId>,
    ) -> Prepared<SplitPlan> {
        let (left_id, right_id) = prepared
            .root_children()
            .expect("a prepared root intent always reserves two children");
        let (left, right, split_key) =
            split_into_children(split, node, right_id, prepared.id(), worker);
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
        self.stats.record_split();
        if let Some(paid) = outputs[0].1.paid_rate() {
            self.candidates.paid_rates().landed(&paid.intent);
        }
        for (id, node) in outputs {
            self.enqueue_if_over_soft_cap(collection, id, node);
            if node.as_leaf().is_some() {
                self.candidates
                    .observe_leaf_change(collection, *id, ChangeKind::Split);
            }
        }
        if reason.is_inline_pressure() {
            self.stats
                .inline_pressure_completed
                .fetch_add(1, Ordering::Relaxed);
        }
        if matches!(reason, SplitReason::Contention { .. }) {
            self.stats.contention_splits.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Returns where a split of the leaf `node` separates the contended key
    /// `key`, if anywhere.
    fn contention_split_key(&self, node: &Node, key: &[u8]) -> Option<Vec<u8>> {
        self.candidates
            .contention()?
            .split_key(node, key, rt::system_now())
    }

    /// Counts a split candidate that is no longer actionable.
    fn record_discarded(&self, reason: &SplitReason) {
        if reason.is_inline_pressure() {
            self.stats
                .inline_pressure_discarded
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
            requested: rt::Instant::now(),
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
            SplitReason::Demand { .. } => 3,
            // The merge reasons use 4 to 6.
            SplitReason::Contention { .. } => 7,
        }
    }

    /// The key whose leaf the split is for, when the reason names one. The
    /// leaf of the key can change between the request and the split.
    fn key(&self) -> Option<&[u8]> {
        match self {
            SplitReason::InlinePressure { key, .. } | SplitReason::Contention { key } => Some(key),
            SplitReason::SoftCap | SplitReason::Capacity | SplitReason::Demand { .. } => None,
        }
    }

    pub(super) fn is_inline_pressure(&self) -> bool {
        matches!(self, SplitReason::InlinePressure { .. })
    }
}

/// A split of one node: why, and the key of a soft cap split that keeps the
/// write spans of the leaf (ADR-084) or of a split that separates a contended
/// key (ADR-085).
#[derive(Clone, Copy)]
struct Split<'a> {
    reason: &'a SplitReason,
    at: Option<&'a [u8]>,
}

impl Split<'_> {
    /// Divides `node` like [`Node::split`], at the key that the split asks
    /// for, if any. A split that a topology rule asked for keeps its rate as
    /// the paid rate of its leaves (ADR-076).
    fn divide(
        &self,
        node: &mut Node,
        right_id: NodeId,
        intent: &StructuralIntentId,
    ) -> Option<(Node, Vec<u8>)> {
        match (self.reason, self.at) {
            (SplitReason::Demand { at, rate }, _) => {
                let paid = Some(Payment {
                    per_second: *rate,
                    intent: *intent,
                });
                match at {
                    Some(key) => node.split_leaf_at(right_id, key, paid),
                    None => node.split(right_id, paid),
                }
            }
            (SplitReason::SoftCap | SplitReason::Contention { .. }, Some(key)) => {
                node.split_leaf_at(right_id, key, None)
            }
            _ => node.split(right_id, None),
        }
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
    split: Split<'_>,
    node: &Node,
    right_id: NodeId,
    intent: &StructuralIntentId,
    structure_holder: &TxId,
) -> (Node, Node, Vec<u8>) {
    let mut source = node.clone();
    let (right, split_key) = split
        .divide(&mut source, right_id, intent)
        .expect("a split source has entries or children in both halves");
    source.remove_structural_gate(structure_holder);
    (source, right, split_key)
}
