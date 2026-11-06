//! The topology policies: what decides when the leaves of one database
//! instance split and merge.
//!
//! A policy is one struct with two traits. [`TopologySignals`] receives the
//! topology signals of the transactions, the leaf coordinator, and the stored
//! leaves, through the [`StructuralHintSink`](super::StructuralHintSink), and
//! queues candidates.
//! [`TopologyDecisions`] answers the splitter and the merger, which check each
//! candidate again on the current node. Each policy keeps the state that its
//! decisions need. The restructurer keeps the changes themselves, and the
//! checks that keep the tree correct.

mod size_causes;
mod writers;

use std::collections::BTreeSet;

use glassdb_data::ObjectPath;
use glassdb_storage::{InlinePolicy, LeafBody, Node, NodeSizePolicy};

use crate::access::AccessSet;

use super::merge::{MergeReason, MergeVeto};
use super::split::{SplitReason, SplitTarget};

pub(crate) use size_causes::SizeCausesPolicy;
pub(crate) use writers::WritersPolicy;

/// What decides the splits and merges of leaves that the hard caps do not
/// force.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopologyPolicy {
    /// Soft caps, underfull thresholds, and inline pressure decide (ADR-031,
    /// ADR-056, ADR-073). Only the leaf sizes decide, so that the topology is
    /// predictable.
    SizeCauses,
    /// The size causes decide, refined by the writers of the leaf entries:
    /// write spans move soft cap splits and cause merges (ADR-084), and the
    /// keys that several database instances write in one leaf split apart
    /// (ADR-085). Contention comes before write spans and leaf sizes, so no
    /// merge puts the keys of two database instances back together (ADR-088).
    /// The default.
    #[default]
    Writers,
}

/// The topology signals of one database instance, for the policy that decides
/// from them. Each method does nothing by default, except for the stored
/// leaves, which every policy uses, because the soft caps and the underfull
/// leaves start there.
pub(crate) trait TopologySignals: Send + Sync {
    /// Notes that the leaf at `path` was just stored, holding `leaf`.
    fn observe_leaf(&self, path: &ObjectPath, leaf: &LeafBody);

    /// Notes a lost leaf CAS of a round of `path` on the keys `round_keys`
    /// (ADR-085).
    fn lost_leaf_cas(&self, _path: &ObjectPath, _round_keys: &BTreeSet<Vec<u8>>) {}

    /// Notes that a transaction with `accesses` committed.
    fn observe_commit(&self, _accesses: &AccessSet) {}

    /// Notes that a direct commit candidate of `path` could not carry the
    /// value of `value_len` bytes of `key` inline.
    fn inline_pressure(&self, _path: &ObjectPath, _key: &[u8], _value_len: usize) {}
}

/// The decisions of one topology policy on current nodes, for the changes that
/// the restructurer runs. A policy that refines [`SizeCausesPolicy`] gives it
/// the reasons that it does not refine.
pub(crate) trait TopologyDecisions: Send + Sync {
    /// Decides the split of the leaf `node`, the node of `target`, for
    /// `reason`.
    fn split(&self, target: SplitTarget<'_>, node: &Node, reason: &SplitReason) -> SplitDecision;

    /// Decides the merge of the leaf `left` into its right sibling `right`
    /// for `reason`. Holder-free tombstones are already removed from both.
    fn merge(&self, left: &Node, right: &Node, reason: MergeReason) -> MergeDecision;
}

/// The limits of nodes and of inline values, which the engine and every
/// topology policy share.
#[derive(Debug, Clone, Copy)]
pub(crate) struct TopologySettings {
    pub(crate) sizes: NodeSizePolicy,
    pub(crate) inline: InlinePolicy,
}

/// How a policy splits a node for one reason, on its current state.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum SplitDecision {
    /// At `at`, or at the median key without one.
    Split { at: Option<Vec<u8>> },
    /// The reason holds, but the split waits for a later write of the leaf.
    Wait,
    /// The reason no longer holds.
    NotNeeded,
    /// The node no longer covers the key of the reason.
    Reroute,
}

impl SplitDecision {
    /// A split at the median key.
    pub(crate) fn median() -> Self {
        Self::Split { at: None }
    }

    /// A split at `key`.
    pub(crate) fn at(key: Vec<u8>) -> Self {
        Self::Split { at: Some(key) }
    }
}

/// How a policy merges two leaves for one reason.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MergeDecision {
    /// The merge goes ahead when the merged leaf fits these limits.
    Within {
        max_entries: usize,
        max_inline_bytes: usize,
    },
    /// The merge does not go ahead.
    Veto(MergeVeto),
}
