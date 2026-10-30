//! The intake of the observations of one database instance.

use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use glassdb_data::{CollectionAddress, NodeId, ObjectPath, StructuralIntentId};
use glassdb_storage::{LeafBody, Node, NodeSizePolicy};

use crate::access::AccessSet;
use crate::leaf_coord::{LeafDelayKind, StructuralHinter};

use super::candidates::{CandidateCause, MaintenanceCandidates};
use super::measurements::ChangeKind;
use super::merge::MergeReason;
use super::policy::{Interests, SignalKind, TopologySignals};
#[cfg(test)]
use super::policy::{SizeLimitsPolicy, TopologySettings};
use super::split::SplitReason;

/// The intake of the topology signals of the transactions, the scans, the
/// leaf coordinator, and the changes that landed, in one database instance. It
/// queues the changes that the tree needs with every topology policy, and gives
/// the other signals to the policy, which ignores the ones that it does not
/// use. A producer asks [`Self::wants`] only for a signal that costs it real
/// work to measure, so that a policy does not cost what it does not use.
#[derive(Clone)]
pub struct StructuralHintSink {
    queue: MaintenanceCandidates,
    sizes: NodeSizePolicy,
    policy: Arc<dyn TopologySignals>,
    interests: Interests,
}

impl StructuralHintSink {
    /// The intake into `queue` and into the observations of `policy`, for
    /// nodes of `sizes`.
    pub(super) fn new(
        queue: MaintenanceCandidates,
        sizes: NodeSizePolicy,
        policy: Arc<dyn TopologySignals>,
    ) -> Self {
        let interests = policy.interests();
        Self {
            queue,
            sizes,
            policy,
            interests,
        }
    }

    /// Whether the topology policy uses the signals of `kind`.
    pub(crate) fn wants(&self, kind: SignalKind) -> bool {
        self.interests.contains(kind)
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

    /// Notes that a direct commit candidate of `path` used `time` more than a
    /// direct commit, because the leaf could not carry its value inline.
    pub(crate) fn inline_pressure_time(&self, path: &ObjectPath, time: Duration) {
        self.policy.inline_pressure_time(path, time);
    }

    /// Notes that a direct commit candidate with keys in the adjacent leaves
    /// `left` and `right` used `time` more than a direct commit.
    pub(crate) fn adjacent_miss_time(&self, left: &ObjectPath, right: &ObjectPath, time: Duration) {
        self.policy.adjacent_miss_time(left, right, time);
    }

    /// Notes that a conflict ended a commit pass of a transaction with
    /// `accesses` without a commit, after `time` with the body run before it.
    pub(crate) fn observe_conflict_pass(&self, accesses: &AccessSet, time: Duration) {
        self.policy.conflict_pass(accesses, time);
    }

    /// Notes that a scan that continued from `left` used `time` to read the
    /// adjacent leaf `right`.
    pub(crate) fn scan_crossing_time(&self, left: &ObjectPath, right: &ObjectPath, time: Duration) {
        self.policy.scan_crossing_time(left, right, time);
    }

    /// Notes that a split or merge of `kind` that landed wrote `leaves` of
    /// `collection`, and that the intent `paid` paid for it (ADR-076).
    pub(super) fn leaves_changed(
        &self,
        collection: &CollectionAddress,
        leaves: &[NodeId],
        kind: ChangeKind,
        paid: Option<&StructuralIntentId>,
    ) {
        self.policy.leaves_changed(collection, leaves, kind, paid);
    }

    /// Notes that a leaf change of `kind` that landed held the structural gate
    /// of its source for `took`.
    pub(super) fn change_timed(&self, kind: ChangeKind, took: Duration) {
        self.policy.change_timed(kind, took);
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

    /// Returns a sink that no restructurer reads, for a component that a test
    /// runs without one.
    #[cfg(test)]
    pub(crate) fn detached() -> Self {
        let queue = MaintenanceCandidates::new();
        let settings = TopologySettings {
            sizes: NodeSizePolicy::default(),
            inline: glassdb_storage::InlinePolicy::default(),
        };
        let policy = Arc::new(SizeLimitsPolicy::new(
            settings,
            settings.sizes.leaf_min_entries(),
            queue.clone(),
        ));
        Self::new(queue, settings.sizes, policy)
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

    fn leaf_delay(&self, path: &ObjectPath, time: Duration, split_key: &[u8], kind: LeafDelayKind) {
        if self.wants(SignalKind::LeafDelays) {
            self.policy.leaf_delay(path, time, split_key, kind);
        }
    }

    fn lost_leaf_cas(&self, path: &ObjectPath, round_keys: &BTreeSet<Vec<u8>>) {
        self.policy.lost_leaf_cas(path, round_keys);
    }

    fn wants(&self, kind: SignalKind) -> bool {
        StructuralHintSink::wants(self, kind)
    }
}
