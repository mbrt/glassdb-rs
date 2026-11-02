//! The intake of the observations of one database instance.

use std::collections::BTreeSet;
use std::sync::Arc;

use glassdb_data::ObjectPath;
use glassdb_storage::{LeafBody, Node, NodeSizePolicy};

use crate::access::AccessSet;
use crate::leaf_coord::StructuralHinter;

use super::candidates::{CandidateCause, MaintenanceCandidates};
use super::merge::MergeReason;
use super::policy::TopologySignals;
use super::split::SplitReason;

/// The intake of the topology signals of the transactions, the leaf
/// coordinator, and the stored nodes, in one database instance. It queues the
/// changes that the tree needs with every topology policy, and gives the other
/// signals to the policy, which ignores the ones that it does not use.
#[derive(Clone)]
pub struct StructuralHintSink {
    queue: MaintenanceCandidates,
    sizes: NodeSizePolicy,
    policy: Arc<dyn TopologySignals>,
}

impl StructuralHintSink {
    /// The intake into `queue` and into the observations of `policy`, for
    /// nodes of `sizes`.
    pub(super) fn new(
        queue: MaintenanceCandidates,
        sizes: NodeSizePolicy,
        policy: Arc<dyn TopologySignals>,
    ) -> Self {
        Self {
            queue,
            sizes,
            policy,
        }
    }

    /// Notes that a transaction with `accesses` committed.
    pub(crate) fn observe_commit(&self, accesses: &AccessSet) {
        self.policy.observe_commit(accesses);
    }

    /// Notes that a direct commit candidate of `path` could not carry the
    /// value of `value_len` bytes of `key` inline.
    pub(crate) fn observe_inline_pressure(&self, path: &ObjectPath, key: &[u8], value_len: usize) {
        self.policy.inline_pressure(path, key, value_len);
    }

    /// Notes that the index node at `path` is now `node`. An underfull index
    /// node that is not the tree root merges, with every topology policy.
    pub(super) fn observe_index(&self, path: &ObjectPath, node: &Node) {
        let underfull = node
            .as_index()
            .is_some_and(|index| index.len() < self.sizes.index_min_children());
        if matches!(path, ObjectPath::Node { .. }) && underfull {
            self.queue
                .push_cause(path, CandidateCause::Merge(MergeReason::Underfull));
        }
    }

    #[cfg(test)]
    pub(crate) fn pending_inline_pressure(&self) -> usize {
        self.queue.pending_inline_pressure()
    }
}

impl StructuralHinter for StructuralHintSink {
    fn observe_leaf(&self, path: &ObjectPath, leaf: &LeafBody) {
        self.policy.observe_leaf(path, leaf);
    }

    /// A mutation that the hard caps rejected asks for one split of its node,
    /// with every topology policy.
    fn capacity_rejected(&self, path: &ObjectPath) {
        self.queue
            .push_cause(path, CandidateCause::Split(SplitReason::Capacity));
    }

    fn lost_leaf_cas(&self, path: &ObjectPath, round_keys: &BTreeSet<Vec<u8>>) {
        self.policy.lost_leaf_cas(path, round_keys);
    }
}
