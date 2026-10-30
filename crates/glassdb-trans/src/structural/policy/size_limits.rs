//! The size limits, which every topology policy refines (ADR-031, ADR-056,
//! ADR-073).

use glassdb_data::ObjectPath;
use glassdb_storage::{LeafBody, LeafEntry, Node};

use crate::structural::candidates::{CandidateCause, MaintenanceCandidates};
use crate::structural::merge::{MergeReason, MergeVeto};
use crate::structural::split::{SplitReason, SplitTarget};

use super::{MergeDecision, SplitDecision, TopologyDecisions, TopologySettings, TopologySignals};

/// A leaf over a soft cap splits at its median, and so does a leaf whose
/// values the inline budget cannot carry. A non-root leaf with fewer live
/// entries than a threshold merges into its right sibling when the merged
/// leaf stays at or below half of each split threshold, so that it does not
/// split again soon.
pub(crate) struct SizeLimitsPolicy {
    settings: TopologySettings,
    underfull_live: usize,
    queue: MaintenanceCandidates,
}

impl SizeLimitsPolicy {
    /// The size limits of `settings`, where a non-root leaf with fewer than
    /// `underfull_live` live entries is underfull, which queue their
    /// candidates in `queue`.
    pub(crate) fn new(
        settings: TopologySettings,
        underfull_live: usize,
        queue: MaintenanceCandidates,
    ) -> Self {
        Self {
            settings,
            underfull_live,
            queue,
        }
    }

    pub(crate) fn settings(&self) -> &TopologySettings {
        &self.settings
    }

    /// The decision of a merge whose leaf stays at or below half of each
    /// split threshold, with no paid rate.
    pub(crate) fn half(&self) -> MergeDecision {
        MergeDecision::Within {
            max_entries: self.settings.sizes.leaf_max_entries() / 2,
            max_inline_bytes: self.settings.inline.max_leaf_bytes / 2,
            paid_rate: None,
        }
    }

    /// Whether `leaf` has fewer live entries than the underfull threshold.
    pub(crate) fn underfull(&self, leaf: &LeafBody) -> bool {
        leaf.entries().filter(|entry| entry.exists()).count() < self.underfull_live
    }

    fn inline_pressure_split(&self, node: &Node, key: &[u8], value_len: usize) -> SplitDecision {
        let Some(leaf) = node.as_leaf() else {
            return SplitDecision::Reroute;
        };
        if !node.covers(key) {
            return SplitDecision::Reroute;
        }
        let inline = &self.settings.inline;
        if !inline.admits_value(value_len)
            || leaf.len() < 2
            || !leaf.lookup(key).is_some_and(LeafEntry::exists)
        {
            return SplitDecision::NotNeeded;
        }
        let other_inline_bytes = leaf
            .entries()
            .filter(|entry| entry.key.as_slice() != key)
            .map(|entry| entry.current.inline_len())
            .sum();
        if inline.admits(other_inline_bytes, value_len) {
            SplitDecision::NotNeeded
        } else {
            SplitDecision::median()
        }
    }
}

impl TopologySignals for SizeLimitsPolicy {
    /// Queues the split of the leaf at `path`, now `leaf`, when it is over a
    /// soft cap, or its merge when it is underfull. A leaf needs at least two
    /// entries to be divisible, so a single hot key never splits. The byte
    /// size is a hint that the splitter checks again against the full node,
    /// which adds a little framing.
    fn observe_leaf(&self, path: &ObjectPath, leaf: &LeafBody) {
        let sizes = &self.settings.sizes;
        let over_cap = leaf.len() >= 2
            && (leaf.len() > sizes.leaf_max_entries()
                || leaf.encoded_len() > sizes.node_soft_max_bytes());
        let cause = if over_cap {
            CandidateCause::Split(SplitReason::SoftCap)
        } else if matches!(path, ObjectPath::Node { .. }) && self.underfull(leaf) {
            CandidateCause::Merge(MergeReason::Underfull)
        } else {
            return;
        };
        self.queue.push_cause(path, cause);
    }

    /// Queues the inline pressure split of `key` in the leaf at `path`, when
    /// the inline budget could carry the value alone.
    fn inline_pressure(&self, path: &ObjectPath, key: &[u8], value_len: usize) {
        if self.settings.inline.admits_value(value_len) {
            let reason = SplitReason::InlinePressure {
                key: key.to_vec(),
                value_len,
            };
            self.queue.push_cause(path, CandidateCause::Split(reason));
        }
    }
}

impl TopologyDecisions for SizeLimitsPolicy {
    fn split(&self, _target: SplitTarget<'_>, node: &Node, reason: &SplitReason) -> SplitDecision {
        match reason {
            SplitReason::SoftCap if node.over_soft_cap(&self.settings.sizes) => {
                SplitDecision::median()
            }
            SplitReason::InlinePressure { key, value_len } => {
                self.inline_pressure_split(node, key, *value_len)
            }
            _ => SplitDecision::NotNeeded,
        }
    }

    fn merge(&self, left: &Node, _right: &Node, reason: MergeReason) -> MergeDecision {
        match reason {
            MergeReason::Underfull if left.as_leaf().is_some_and(|leaf| self.underfull(leaf)) => {
                self.half()
            }
            _ => MergeDecision::Veto(MergeVeto::NotNeeded),
        }
    }
}
