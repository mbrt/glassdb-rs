//! Background merges of underfull nodes into their right sibling (ADR-073).
//!
//! A merge uses the same outer lifecycle as a non-root split. After the Ready
//! transition, one CAS on the target R absorbs the entries of the gated source
//! L and installs a merge reservation. One CAS on L then drains it, which is
//! the linearization point. If the drain cannot land, one CAS on R abandons
//! the merge.

use std::sync::Arc;
use std::sync::atomic::Ordering;

use glassdb_data::{CollectionAddress, NodeId, StructuralIntentId, TxId};
use glassdb_storage::{
    InlinePolicy, LeafBody, LeafObservation, MergeTarget, Node, NodeBody, NodeSizePolicy,
    NodeStore, Requirement, StorageError, Timeline, TreeRouter,
};

use crate::error::TransError;
use crate::monitor::Monitor;

use super::NODE_CAS_ATTEMPTS;
use super::change::{Applied, ParentRoute, Prepared};
use super::nodes::StructuralNodeAccess;
use super::reclamation::{ReclamationReporter, reclaim_holder_free_tombstones};
use super::recovery::ReadyChange;
use super::stats::Stats;

/// Safety bound on the right-link hops that the search for a merge target
/// walks past drained nodes, so a malformed chain can never spin the
/// restructurer.
const MAX_TARGET_SEARCH_HOPS: usize = 4096;

/// Decides which merges are necessary, and writes their nodes.
#[derive(Clone)]
pub(super) struct Merger {
    nodes: NodeStore,
    structural_nodes: StructuralNodeAccess,
    router: TreeRouter,
    timeline: Timeline,
    mon: Monitor,
    policy: NodeSizePolicy,
    inline: InlinePolicy,
    stats: Arc<Stats>,
    reclamation: ReclamationReporter,
}

/// The node writes of one merge. The source gate protects them until the
/// drain lands.
pub(super) struct MergePlan {
    left: Node,
    target: TargetState,
    merge: MergeTarget,
    reclaimed: Vec<TxId>,
}

/// A merge target and the state that the merge decision used.
struct TargetState {
    id: NodeId,
    node: Node,
    observation: LeafObservation,
}

impl Merger {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        nodes: NodeStore,
        structural_nodes: StructuralNodeAccess,
        router: TreeRouter,
        timeline: Timeline,
        mon: Monitor,
        policy: NodeSizePolicy,
        inline: InlinePolicy,
        stats: Arc<Stats>,
        reclamation: ReclamationReporter,
    ) -> Self {
        Self {
            nodes,
            structural_nodes,
            router,
            timeline,
            mon,
            policy,
            inline,
            stats,
            reclamation,
        }
    }

    /// Reports whether cached state shows node `source` as an actionable merge
    /// source. Busy nodes defer the merge, so that it does not write an intent
    /// that the polite gate acquisition would then cancel.
    pub(super) async fn is_actionable(
        &self,
        collection: &CollectionAddress,
        source: &NodeId,
    ) -> Result<bool, TransError> {
        let left = match self
            .nodes
            .load_node(collection, source, Requirement::ANY)
            .await
        {
            Ok((left, _)) => left,
            Err(StorageError::NotFound) => return Ok(false),
            Err(error) => return Err(error.into()),
        };
        let Some(boundary) = left.high_key().filter(|_| !left.is_drained()) else {
            return Ok(false);
        };
        let Some(target) = self.target(collection, &left, Requirement::ANY).await? else {
            return Ok(false);
        };
        if !self.need(&left, &target.node) {
            return Ok(false);
        }
        // Merging the children of two parents would leave the left parent
        // routing through the drained node (ADR-073).
        let parent = self
            .router
            .parent_of(collection, left.low_key(), source, Requirement::ANY)
            .await?;
        if !parent
            .as_ref()
            .and_then(|parent| parent.node())
            .is_some_and(|parent| parent.covers(boundary))
        {
            return Ok(false);
        }
        if left.locks().merge_reservation().is_some()
            || target.node.locks().merge_reservation().is_some()
            || self.has_live_holder(&left).await?
            || self.has_live_holder(&target.node).await?
        {
            return Err(TransError::Retry);
        }
        Ok(true)
    }

    /// Checks the merge of the gated source `left` again in current state, and
    /// plans its node writes. Holder-free tombstones are removed first
    /// (ADR-062).
    pub(super) async fn prepare(
        &self,
        collection: &CollectionAddress,
        mut left: Node,
    ) -> Prepared<MergePlan> {
        let reclaimed = reclaim_holder_free_tombstones(&mut left);
        let target = match self.checked_target(collection, &left).await {
            Ok(Some(target)) => target,
            Ok(None) if !reclaimed.is_empty() => {
                return Prepared::ReclaimOnly {
                    node: left,
                    reclaimed,
                };
            }
            Ok(None) => return Prepared::Cancel(Ok(())),
            Err(error) => return Prepared::Cancel(Err(error)),
        };
        let merge = MergeTarget {
            node_id: target.id,
            boundary: left
                .high_key()
                .expect("a merge source has a right sibling")
                .to_vec(),
            generation: target.node.membership_generation(),
        };
        Prepared::Ready {
            change: ReadyChange::Merge(merge.clone()),
            plan: MergePlan {
                left,
                target,
                merge,
                reclaimed,
            },
        }
    }

    /// Writes the planned nodes of a merge whose intent `intent` is Ready:
    /// the absorb into the target, the drain of the source `source`, and the
    /// removal of the merge reservation.
    pub(super) async fn apply(
        &self,
        collection: &CollectionAddress,
        source: &NodeId,
        observation: &LeafObservation,
        intent: &StructuralIntentId,
        plan: MergePlan,
    ) -> Result<Applied, TransError> {
        let MergePlan {
            left,
            target,
            merge,
            reclaimed,
        } = plan;
        match self
            .absorb(collection, &merge, &left, intent, target)
            .await?
        {
            Some(target_reclaimed) => self.reclamation.record(&target_reclaimed, false),
            None => return Ok(Applied::Stopped),
        }
        let mut drained = left;
        drained.drain(merge.node_id);
        if !self
            .nodes
            .store_node(collection, source, &drained, Some(observation))
            .await?
        {
            self.structural_nodes
                .abandon_merge(collection, &merge, intent, Requirement::ANY)
                .await?;
            return Ok(Applied::Stopped);
        }
        self.reclamation.record(&reclaimed, false);
        self.stats.merges.fetch_add(1, Ordering::Relaxed);
        self.structural_nodes
            .remove_merge_reservation(collection, &merge.node_id, intent, Requirement::ANY)
            .await?;
        Ok(Applied::Landed(Some(ParentRoute {
            key: merge.boundary,
            target: merge.node_id,
        })))
    }

    /// Finds the merge target of the gated `left` in current state and checks
    /// the merge decision again. `None` cancels the merge.
    async fn checked_target(
        &self,
        collection: &CollectionAddress,
        left: &Node,
    ) -> Result<Option<TargetState>, TransError> {
        if left.is_drained() || left.high_key().is_none() {
            return Ok(None);
        }
        let requirement = Requirement::after(self.timeline.currentness_barrier());
        let Some(target) = self.target(collection, left, requirement).await? else {
            return Ok(None);
        };
        if !self.need(left, &target.node) {
            return Ok(None);
        }
        if target.node.locks().merge_reservation().is_some()
            || self.has_live_holder(&target.node).await?
        {
            return Err(TransError::Retry);
        }
        Ok(Some(target))
    }

    /// Finds the node that receives a merge of `left`: the first node on its
    /// right-link chain that is not drained.
    async fn target(
        &self,
        collection: &CollectionAddress,
        left: &Node,
        requirement: Requirement,
    ) -> Result<Option<TargetState>, TransError> {
        let mut next = left.right_sibling();
        for _ in 0..MAX_TARGET_SEARCH_HOPS {
            let Some(id) = next else {
                return Ok(None);
            };
            let (node, observation) = self.nodes.load_node(collection, &id, requirement).await?;
            if !node.is_drained() {
                return Ok(Some(TargetState {
                    id,
                    node,
                    observation,
                }));
            }
            next = node.right_sibling();
        }
        Err(TransError::other(
            "merge target search exceeded the right-link hop bound",
        ))
    }

    /// Reports whether `left` is underfull and whether its merge with `right`
    /// stays at or below half of the threshold of every split cause, so that
    /// the merged node does not split again soon (ADR-073). The merge compacts
    /// both nodes, so their holder-free tombstones do not count.
    fn need(&self, left: &Node, right: &Node) -> bool {
        let (mut left, mut right) = (left.clone(), right.clone());
        reclaim_holder_free_tombstones(&mut left);
        reclaim_holder_free_tombstones(&mut right);
        let policy = &self.policy;
        let fits_bytes = left.content_encoded_len() + right.content_encoded_len()
            <= policy.node_soft_max_bytes().min(policy.content_limit()) / 2;
        match (left.body(), right.body()) {
            (NodeBody::Leaf(left), NodeBody::Leaf(right)) => {
                let inline_len = |leaf: &LeafBody| -> usize {
                    leaf.entries().map(|e| e.current.inline_len()).sum()
                };
                left.entries().filter(|entry| entry.exists()).count() < policy.leaf_min_entries()
                    && left.len() + right.len() <= policy.leaf_max_entries() / 2
                    && inline_len(left) + inline_len(right) <= self.inline.max_leaf_bytes / 2
                    && fits_bytes
            }
            (NodeBody::Index(left), NodeBody::Index(right)) => {
                left.len() < policy.index_min_children()
                    && left.len() + right.len() <= policy.index_max_children() / 2
                    && fits_bytes
            }
            _ => false,
        }
    }

    /// Reports whether a holder of any claim on `node` can still be live.
    async fn has_live_holder(&self, node: &Node) -> Result<bool, TransError> {
        let entry_holders = node
            .as_leaf()
            .into_iter()
            .flat_map(LeafBody::entries)
            .flat_map(|entry| entry.lock_holders());
        let holders = node
            .structural_gate()
            .holders()
            .iter()
            .chain(node.membership_lock().holders())
            .chain(node.drop_intent())
            .chain(entry_holders);
        for holder in holders {
            if !self.mon.tx_status(holder).await?.is_final() {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// Compacts the merge target, adds the entries of the gated `left` to it,
    /// and installs the merge reservation of `intent`. Returns the writers of
    /// the reclaimed tombstones, or `None` when the merge must stop, with no
    /// absorb CAS that can still land.
    async fn absorb(
        &self,
        collection: &CollectionAddress,
        merge: &MergeTarget,
        left: &Node,
        intent: &StructuralIntentId,
        target: TargetState,
    ) -> Result<Option<Vec<TxId>>, TransError> {
        let mut current = (target.node, target.observation);
        for attempt in 0..NODE_CAS_ATTEMPTS {
            if attempt > 0 {
                current = self
                    .nodes
                    .load_node(collection, &merge.node_id, Requirement::ANY)
                    .await?;
            }
            let (mut right, observation) = current.clone();
            // Recovery advances the generation to fence this absorb after it
            // deleted the intent.
            if right.is_drained()
                || right.membership_generation() != merge.generation
                || right.drop_intent().is_some()
                || right.locks().merge_reservation().is_some()
            {
                return Ok(None);
            }
            if let Some(holder) = right.structural_gate().holders().first().cloned() {
                if !self.mon.tx_status(&holder).await?.is_final() {
                    return Ok(None);
                }
                right.remove_structural_gate(&holder);
            }
            // The CAS expects this exact state of R, so it removes only
            // tombstones that have no holder when it lands. R's gate is not
            // necessary for that (ADR-073).
            let reclaimed = reclaim_holder_free_tombstones(&mut right);
            right.absorb(left, *intent)?;
            if right.content_encoded_len() > self.policy.content_limit()
                || right.encoded_len() > self.policy.node_max_bytes()
            {
                return Ok(None);
            }
            if self
                .structural_nodes
                .store_structural_node(&right, &observation)
                .await?
                .is_some()
            {
                return Ok(Some(reclaimed));
            }
        }
        Ok(None)
    }
}
