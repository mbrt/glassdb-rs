//! The size causes, the default topology policy (ADR-084, ADR-085).

use std::collections::BTreeSet;

use glassdb_concurr::rt;
use glassdb_data::ObjectPath;
use glassdb_storage::{LeafBody, Node};

use crate::access::AccessSet;
use crate::monitor::Monitor;
use crate::structural::candidates::{CandidateCause, MaintenanceCandidates};
use crate::structural::contention::Contention;
use crate::structural::measurements::written_pair;
use crate::structural::merge::{MergeReason, MergeVeto};
use crate::structural::split::{SplitReason, SplitTarget};
use crate::structural::write_spans::{self, SoftCapSplit};

use super::{
    MergeDecision, SizeLimitsPolicy, SplitDecision, TopologyDecisions, TopologySettings,
    TopologySignals,
};

/// Soft caps, underfull thresholds, and inline pressure decide. Write spans
/// move soft cap splits and cause merges (ADR-084), and the keys that several
/// database instances write in one leaf split apart (ADR-085).
pub(crate) struct SizeCausesPolicy {
    base: SizeLimitsPolicy,
    contention: Contention,
    queue: MaintenanceCandidates,
}

impl SizeCausesPolicy {
    /// The size causes of `settings`, for the database instance of `mon`,
    /// which queue their candidates in `queue`.
    pub(crate) fn new(
        settings: TopologySettings,
        mon: Monitor,
        queue: MaintenanceCandidates,
    ) -> Self {
        Self {
            base: SizeLimitsPolicy::new(settings, settings.sizes.leaf_min_entries(), queue.clone()),
            contention: Contention::new(mon),
            queue,
        }
    }

    /// Decides the soft cap split of the leaf `node`, at a key that keeps its
    /// write spans (ADR-084). A root leaf splits at its median: it is the
    /// whole collection, and its split cannot defer in cached state.
    fn soft_cap_split(&self, target: SplitTarget<'_>, node: &Node) -> SplitDecision {
        if !node.over_soft_cap(&self.base.settings().sizes) {
            return SplitDecision::NotNeeded;
        }
        if let SplitTarget::Root = target {
            return SplitDecision::median();
        }
        match write_spans::soft_cap_split(node, &self.base.settings().sizes, rt::system_now()) {
            SoftCapSplit::Median => SplitDecision::median(),
            SoftCapSplit::At(key) => SplitDecision::at(key),
            SoftCapSplit::Wait => SplitDecision::Wait,
        }
    }

    /// Decides the split that separates the contended key `key` of the leaf
    /// `node` from the keys of other database instances (ADR-085).
    fn contention_split(&self, node: &Node, key: &[u8]) -> SplitDecision {
        if node.as_leaf().is_none() || !node.covers(key) {
            return SplitDecision::Reroute;
        }
        match self.contention.split_key(node, key, rt::system_now()) {
            Some(at) => SplitDecision::at(at),
            None => SplitDecision::NotNeeded,
        }
    }
}

impl TopologySignals for SizeCausesPolicy {
    fn observe_leaf(&self, path: &ObjectPath, leaf: &LeafBody) {
        self.base.observe_leaf(path, leaf);
    }

    fn lost_leaf_cas(&self, path: &ObjectPath, round_keys: &BTreeSet<Vec<u8>>) {
        let (ObjectPath::TreeRoot { collection } | ObjectPath::Node { collection, .. }) = path
        else {
            return;
        };
        if let Some(key) = self.contention.lost_leaf_cas(collection, round_keys) {
            self.queue
                .push_cause(path, CandidateCause::Split(SplitReason::Contention { key }));
        }
    }

    /// When the transaction wrote keys in two adjacent leaves, their merge may
    /// keep its write span in one leaf (ADR-084). Two leaves that were too
    /// large to merge when the transaction read them ask for no merge, so that
    /// their commits do not fill the queue.
    fn observe_commit(&self, accesses: &AccessSet) {
        let max_entries = self
            .base
            .settings()
            .sizes
            .leaf_max_entries()
            .saturating_mul(2);
        if let Some((left, _)) =
            written_pair(accesses).filter(|(_, entries)| *entries <= max_entries)
        {
            self.queue
                .push_cause(&left, CandidateCause::Merge(MergeReason::WriteSpan));
        }
    }

    fn inline_pressure(&self, path: &ObjectPath, key: &[u8], value_len: usize) {
        self.base.inline_pressure(path, key, value_len);
    }
}

impl TopologyDecisions for SizeCausesPolicy {
    /// A soft cap split waits for the write spans of its leaf (ADR-084).
    fn soft_cap_splits_wait(&self) -> bool {
        true
    }

    fn split(&self, target: SplitTarget<'_>, node: &Node, reason: &SplitReason) -> SplitDecision {
        match reason {
            SplitReason::SoftCap => self.soft_cap_split(target, node),
            SplitReason::Contention { key } => self.contention_split(node, key),
            _ => self.base.split(target, node, reason),
        }
    }

    /// An underfull merge waits while another database instance writes one
    /// of the two leaves, and this one does not (ADR-085). A merge for write
    /// spans can make a leaf of up to twice the soft cap of entries, because
    /// its soft cap split then keeps the write spans. It keeps a quarter of
    /// the inline budget free, so that the next writes of the merged leaf do
    /// not cause inline pressure at once (ADR-084).
    fn merge(&self, left: &Node, right: &Node, reason: MergeReason) -> MergeDecision {
        let now = rt::system_now();
        match reason {
            MergeReason::Underfull => match self.base.merge(left, right, reason) {
                MergeDecision::Within { .. } if self.contention.keeps_apart(left, right, now) => {
                    MergeDecision::Veto(MergeVeto::KeptApart)
                }
                decision => decision,
            },
            MergeReason::WriteSpan => {
                let sizes = &self.base.settings().sizes;
                if write_spans::pays_for_merge(left, right, sizes, now) {
                    MergeDecision::Within {
                        max_entries: sizes.leaf_max_entries().saturating_mul(2),
                        max_inline_bytes: self.base.settings().inline.max_leaf_bytes / 4 * 3,
                        paid_rate: None,
                    }
                } else {
                    MergeDecision::Veto(MergeVeto::NotNeeded)
                }
            }
            _ => self.base.merge(left, right, reason),
        }
    }
}
