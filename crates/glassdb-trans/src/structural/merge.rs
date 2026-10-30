//! Background merges of underfull nodes into their right sibling (ADR-073).
//!
//! A merge uses the same outer lifecycle as a non-root split. After the Ready
//! transition, one CAS on the target R absorbs the entries of the gated source
//! L and installs a merge reservation. One CAS on L then drains it, which is
//! the linearization point. If the drain cannot land, one CAS on R abandons
//! the merge.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use glassdb_data::{CollectionAddress, NodeId, StructuralIntentId, TxId};
use glassdb_storage::{
    LeafBody, LeafObservation, MergeTarget, Node, NodeBody, NodeSizePolicy, NodeStore, Requirement,
    StorageError, Timeline, TreeRouter,
};

use crate::error::TransError;
use crate::monitor::Monitor;

use super::NODE_CAS_ATTEMPTS;
use super::change::{Applied, ParentRoute, Prepared};
use super::measurements::ChangeKind;
use super::nodes::StructuralNodeAccess;
use super::policy::{MergeDecision, TopologyDecisions};
use super::reclamation::{ReclamationReporter, reclaim_holder_free_tombstones};
use super::recovery::ReadyChange;
use super::sink::StructuralHintSink;
use super::stats::Stats;

/// Safety bound on the right-link hops that the search for a merge target
/// walks past drained nodes, so a malformed chain can never spin the
/// restructurer.
const MAX_TARGET_SEARCH_HOPS: usize = 4096;

/// Why a merge does not go ahead.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MergeVeto {
    /// The rate of a demand merge does not pay more than the paid rates of
    /// the two leaves (ADR-076).
    Unpaid,
    /// The merged node would be over half of a threshold of a split cause
    /// (ADR-073).
    TooLarge,
    /// The reason does not apply to the nodes.
    NotNeeded,
    /// Another database instance writes one of the two leaves, and this one
    /// does not, so an underfull merge waits for the writers (ADR-085).
    KeptApart,
}

/// Decides which merges are necessary, and writes their nodes.
#[derive(Clone)]
pub(super) struct Merger {
    nodes: NodeStore,
    structural_nodes: StructuralNodeAccess,
    router: TreeRouter,
    timeline: Timeline,
    mon: Monitor,
    // The limits of nodes, which every topology policy keeps.
    sizes: NodeSizePolicy,
    decisions: Arc<dyn TopologyDecisions>,
    // Receives the leaves that a merge wrote.
    hints: StructuralHintSink,
    stats: Arc<Stats>,
    reclamation: ReclamationReporter,
}

/// Why a node may need a merge.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MergeReason {
    /// The node is below an underfull threshold.
    Underfull,
    /// Transactions write keys of the leaf and of its right sibling together
    /// (ADR-084).
    WriteSpan,
    /// A topology rule decided that the leaf merges.
    Demand {
        /// The avoidable time in each second that pays for the merge.
        rate: Duration,
    },
}

/// The node writes of one merge. The source gate protects them until the
/// drain lands.
pub(super) struct MergePlan {
    left: Node,
    target: TargetState,
    merge: MergeTarget,
    reclaimed: Vec<TxId>,
    /// The rate that pays for a merge that a topology rule asked for.
    rate: Option<Duration>,
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
        sizes: NodeSizePolicy,
        decisions: Arc<dyn TopologyDecisions>,
        hints: StructuralHintSink,
        stats: Arc<Stats>,
        reclamation: ReclamationReporter,
    ) -> Self {
        Self {
            nodes,
            structural_nodes,
            router,
            timeline,
            mon,
            sizes,
            decisions,
            hints,
            stats,
            reclamation,
        }
    }

    /// Returns the merge target of node `source` if cached state shows it as
    /// an actionable merge source for `reason`. Busy nodes defer the merge, so
    /// that it does not write an intent that the polite gate acquisition would
    /// then cancel.
    pub(super) async fn actionable_target(
        &self,
        collection: &CollectionAddress,
        source: &NodeId,
        reason: MergeReason,
    ) -> Result<Option<NodeId>, TransError> {
        let left = match self
            .nodes
            .load_node(collection, source, Requirement::ANY)
            .await
        {
            Ok((left, _)) => left,
            Err(StorageError::NotFound) => {
                self.count_demand(reason, |stats| &stats.demand_merge_other);
                return Ok(None);
            }
            Err(error) => return Err(error.into()),
        };
        let Some(boundary) = left.high_key().filter(|_| !left.is_drained()) else {
            self.count_demand(reason, |stats| &stats.demand_merge_other);
            return Ok(None);
        };
        let Some(target) = self.target(collection, &left, Requirement::ANY).await? else {
            self.count_demand(reason, |stats| &stats.demand_merge_other);
            return Ok(None);
        };
        if let Err(veto) = self.need(&left, &target.node, reason) {
            self.count_veto(reason, veto);
            return Ok(None);
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
            self.count_demand(reason, |stats| &stats.demand_merge_other);
            return Ok(None);
        }
        if left.locks().merge_reservation().is_some()
            || target.node.locks().merge_reservation().is_some()
            || self.has_live_holder(&left).await?
            || self.has_live_holder(&target.node).await?
        {
            self.count_demand(reason, |stats| &stats.demand_merge_busy);
            return Err(TransError::Retry);
        }
        Ok(Some(target.id))
    }

    /// Checks the merge of the gated source `left` again in current state, and
    /// plans its node writes. Holder-free tombstones are removed first
    /// (ADR-062).
    pub(super) async fn prepare(
        &self,
        collection: &CollectionAddress,
        mut left: Node,
        reason: MergeReason,
    ) -> Prepared<MergePlan> {
        let reclaimed = reclaim_holder_free_tombstones(&mut left);
        let (target, rate) = match self.checked_target(collection, &left, reason).await {
            Ok(Some(checked)) => checked,
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
                rate,
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
            rate,
        } = plan;
        match self
            .absorb(collection, &merge, &left, intent, target, rate)
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
        self.stats.record_merge();
        let leaves: &[NodeId] = if drained.as_leaf().is_some() {
            &[*source, merge.node_id]
        } else {
            &[]
        };
        self.hints.leaves_changed(
            collection,
            leaves,
            ChangeKind::Merge,
            rate.is_some().then_some(intent),
        );
        self.structural_nodes
            .remove_merge_reservation(collection, &merge.node_id, intent, Requirement::ANY)
            .await?;
        Ok(Applied::Landed(Some(ParentRoute {
            key: merge.boundary,
            target: merge.node_id,
        })))
    }

    /// Finds the merge target of the gated `left` in current state and checks
    /// the merge decision again, with the rate that the merge pays. `None`
    /// cancels the merge.
    async fn checked_target(
        &self,
        collection: &CollectionAddress,
        left: &Node,
        reason: MergeReason,
    ) -> Result<Option<(TargetState, Option<Duration>)>, TransError> {
        if left.is_drained() || left.high_key().is_none() {
            self.count_demand(reason, |stats| &stats.demand_merge_other);
            return Ok(None);
        }
        let requirement = Requirement::after(self.timeline.currentness_barrier());
        let Some(target) = self.target(collection, left, requirement).await? else {
            self.count_demand(reason, |stats| &stats.demand_merge_other);
            return Ok(None);
        };
        let paid_rate = match self.need(left, &target.node, reason) {
            Ok(paid_rate) => paid_rate,
            Err(veto) => {
                self.count_veto(reason, veto);
                return Ok(None);
            }
        };
        if target.node.locks().merge_reservation().is_some()
            || self.has_live_holder(&target.node).await?
        {
            self.count_demand(reason, |stats| &stats.demand_merge_busy);
            return Err(TransError::Retry);
        }
        Ok(Some((target, paid_rate)))
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

    /// Reports whether `left` still has the need of `reason`, and whether its
    /// merge with `right` fits, with the rate that the merge pays (ADR-076).
    /// The topology policy decides whether two leaves merge, the limits of the
    /// merged leaf, and its paid rate. The merge compacts both nodes, so their
    /// holder-free tombstones do not count. Only an underfull merge applies to
    /// index nodes.
    fn need(
        &self,
        left: &Node,
        right: &Node,
        reason: MergeReason,
    ) -> Result<Option<Duration>, MergeVeto> {
        let (mut left, mut right) = (left.clone(), right.clone());
        reclaim_holder_free_tombstones(&mut left);
        reclaim_holder_free_tombstones(&mut right);
        let policy = &self.sizes;
        let fits_bytes = left.content_encoded_len() + right.content_encoded_len()
            <= policy.node_soft_max_bytes().min(policy.content_limit()) / 2;
        match (left.body(), right.body()) {
            (NodeBody::Leaf(left_leaf), NodeBody::Leaf(right_leaf)) => {
                let (max_entries, max_inline_bytes, paid_rate) =
                    match self.decisions.merge(&left, &right, reason) {
                        MergeDecision::Within {
                            max_entries,
                            max_inline_bytes,
                            paid_rate,
                        } => (max_entries, max_inline_bytes, paid_rate),
                        MergeDecision::Veto(veto) => return Err(veto),
                    };
                let inline_len = |leaf: &LeafBody| -> usize {
                    leaf.entries().map(|e| e.current.inline_len()).sum()
                };
                let fits = left_leaf.len() + right_leaf.len() <= max_entries
                    && inline_len(left_leaf) + inline_len(right_leaf) <= max_inline_bytes
                    && fits_bytes;
                if fits {
                    Ok(paid_rate)
                } else {
                    Err(MergeVeto::TooLarge)
                }
            }
            (NodeBody::Index(left), NodeBody::Index(right)) => {
                if reason != MergeReason::Underfull || left.len() >= policy.index_min_children() {
                    Err(MergeVeto::NotNeeded)
                } else if left.len() + right.len() > policy.index_max_children() / 2 || !fits_bytes
                {
                    Err(MergeVeto::TooLarge)
                } else {
                    Ok(None)
                }
            }
            _ => Err(MergeVeto::NotNeeded),
        }
    }

    /// Counts the veto of one attempt of a merge: each underfull merge that
    /// waits for the writers of its leaves, and the other vetoes of the merges
    /// that a topology rule asked for.
    fn count_veto(&self, reason: MergeReason, veto: MergeVeto) {
        let demand_counter: fn(&Stats) -> &AtomicU64 = match veto {
            MergeVeto::KeptApart => {
                self.stats.kept_apart.fetch_add(1, Ordering::Relaxed);
                return;
            }
            MergeVeto::Unpaid => |stats| &stats.demand_merge_unpaid,
            MergeVeto::TooLarge => |stats| &stats.demand_merge_too_large,
            MergeVeto::NotNeeded => |stats| &stats.demand_merge_other,
        };
        self.count_demand(reason, demand_counter);
    }

    /// Counts one outcome of a merge that a topology rule asked for.
    fn count_demand(&self, reason: MergeReason, counter: impl FnOnce(&Stats) -> &AtomicU64) {
        if matches!(reason, MergeReason::Demand { .. }) {
            counter(&self.stats).fetch_add(1, Ordering::Relaxed);
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
        rate: Option<Duration>,
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
            right.absorb(left, *intent, rate)?;
            let policy = &self.sizes;
            if right.content_encoded_len() > policy.content_limit()
                || right.encoded_len() > policy.node_max_bytes()
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

impl MergeReason {
    pub(super) fn class(self) -> u8 {
        match self {
            MergeReason::Underfull => 4,
            MergeReason::Demand { .. } => 5,
            MergeReason::WriteSpan => 6,
        }
    }
}
