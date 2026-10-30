//! The topology policies: what decides when the leaves of one database
//! instance split and merge.
//!
//! A policy is one struct with two traits. [`TopologySignals`] receives the
//! topology signals of the transactions, the scans, the leaf coordinator, and
//! the changes that landed, through the
//! [`StructuralHintSink`](super::StructuralHintSink), and queues candidates.
//! [`TopologyDecisions`] answers the splitter and the merger, which check each
//! candidate again on the current node. Each policy keeps the measurements that
//! its decisions need. The restructurer keeps the changes themselves, and the
//! checks that keep the tree correct.

mod avoidable_time;
mod size_causes;
mod size_limits;

use std::collections::BTreeSet;
use std::time::Duration;

use glassdb_data::{CollectionAddress, NodeId, ObjectPath, StructuralIntentId};
use glassdb_storage::{InlinePolicy, LeafBody, Node, NodeSizePolicy};

use crate::access::AccessSet;
use crate::leaf_coord::LeafDelayKind;

use super::measurements::ChangeKind;
use super::merge::{MergeReason, MergeVeto};
use super::split::{SplitReason, SplitTarget};
use super::stats::TopologyTimeStats;

pub(crate) use avoidable_time::AvoidableTimePolicy;
#[cfg(test)]
pub(crate) use avoidable_time::{DEMAND_MAX_AGE, request_candidate};
pub(crate) use size_causes::SizeCausesPolicy;
pub(crate) use size_limits::SizeLimitsPolicy;

/// One kind of topology signal that costs its producer real work to measure,
/// so that the producer measures it only for a policy that asks for it. The
/// other signals cost little, and every policy receives them. A new kind does
/// not change the policies that do not use it: they keep their [`Interests`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SignalKind {
    /// The time that the rounds of the leaf coordinator lose to the topology
    /// of their leaf. The coordinator tracks every round to measure it.
    LeafDelays,
}

/// The kinds of topology signals that a policy uses.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Interests(u32);

impl Interests {
    /// No kind of signal.
    pub(crate) const fn none() -> Self {
        Self(0)
    }

    /// These interests, and `kind`.
    pub(crate) const fn with(self, kind: SignalKind) -> Self {
        Self(self.0 | 1 << kind as u32)
    }

    /// Whether these interests include `kind`.
    pub(crate) const fn contains(self, kind: SignalKind) -> bool {
        self.0 & 1 << kind as u32 != 0
    }
}

/// The topology signals of one database instance, for the policy that decides
/// from them. Each method does nothing by default, except for the stored
/// leaves, which every policy uses, because the soft caps and the underfull
/// leaves start there. The producers measure the signals of a
/// [`SignalKind`] only when [`Self::interests`] includes it.
pub(crate) trait TopologySignals: Send + Sync {
    /// The kinds of signals that the policy uses, which the sink reads once.
    fn interests(&self) -> Interests {
        Interests::none()
    }

    /// Notes that the leaf at `path` was just stored, holding `leaf`.
    fn observe_leaf(&self, path: &ObjectPath, leaf: &LeafBody);

    /// Notes a lost leaf CAS of a round of `path` on the keys `round_keys`
    /// (ADR-085).
    fn lost_leaf_cas(&self, _path: &ObjectPath, _round_keys: &BTreeSet<Vec<u8>>) {}

    /// Notes `time` that the rounds of `path` lost, and that one split at
    /// `split_key` can remove (ADR-074).
    fn leaf_delay(
        &self,
        _path: &ObjectPath,
        _time: Duration,
        _split_key: &[u8],
        _kind: LeafDelayKind,
    ) {
    }

    /// Notes that a transaction with `accesses` committed.
    fn observe_commit(&self, _accesses: &AccessSet) {}

    /// Notes that a direct commit candidate of `path` could not carry the
    /// value of `value_len` bytes of `key` inline.
    fn inline_pressure(&self, _path: &ObjectPath, _key: &[u8], _value_len: usize) {}

    /// Notes that a commit of `path` took `time` more than a direct commit,
    /// because the leaf could not carry its value inline.
    fn inline_pressure_time(&self, _path: &ObjectPath, _time: Duration) {}

    /// Notes that a commit with keys in the adjacent leaves `left` and `right`
    /// took `time` more than a direct commit.
    fn adjacent_miss_time(&self, _left: &ObjectPath, _right: &ObjectPath, _time: Duration) {}

    /// Notes that a conflict ended a commit pass of a transaction with
    /// `accesses` without a commit, after `time` with the body run before it.
    fn conflict_pass(&self, _accesses: &AccessSet, _time: Duration) {}

    /// Notes that a scan that continued from `left` took `time` to read the
    /// adjacent leaf `right`.
    fn scan_crossing_time(&self, _left: &ObjectPath, _right: &ObjectPath, _time: Duration) {}

    /// Notes that a split or merge of `kind` that landed wrote `leaves` of
    /// `collection`, and that the intent `paid` paid for it (ADR-076).
    fn leaves_changed(
        &self,
        _collection: &CollectionAddress,
        _leaves: &[NodeId],
        _kind: ChangeKind,
        _paid: Option<&StructuralIntentId>,
    ) {
    }

    /// Notes that a leaf change of `kind` that landed held the structural gate
    /// of its source for `took`.
    fn change_timed(&self, _kind: ChangeKind, _took: Duration) {}

    /// The interval of the windows after which the policy decides, if it
    /// decides in windows.
    fn window(&self) -> Option<Duration> {
        None
    }

    /// Ends one window, and queues the changes that it calls for.
    fn end_window(&self) {}

    /// Returns and resets the time that the policy measured.
    fn take_topology_time(&self) -> TopologyTimeStats {
        TopologyTimeStats::default()
    }
}

/// The decisions of one topology policy on current nodes, for the changes that
/// the restructurer runs. A policy that refines [`SizeLimitsPolicy`] gives it
/// the reasons that it does not refine.
pub(crate) trait TopologyDecisions: Send + Sync {
    /// Decides the split of the leaf `node`, the node of `target`, for
    /// `reason`.
    fn split(&self, target: SplitTarget<'_>, node: &Node, reason: &SplitReason) -> SplitDecision;

    /// Whether a soft cap split of a non-root leaf can wait. Only then does the
    /// splitter check it in cached state before it takes a structural gate.
    fn soft_cap_splits_wait(&self) -> bool {
        false
    }

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
    /// At `at`, or at the median key without one. Both halves keep
    /// `paid_rate` as their paid rate (ADR-076).
    Split {
        at: Option<Vec<u8>>,
        paid_rate: Option<Duration>,
    },
    /// The reason holds, but the split waits for a later write of the leaf.
    Wait,
    /// The reason no longer holds.
    NotNeeded,
    /// The node no longer covers the key of the reason.
    Reroute,
}

impl SplitDecision {
    /// A split at the median key, with no paid rate.
    pub(crate) fn median() -> Self {
        Self::Split {
            at: None,
            paid_rate: None,
        }
    }

    /// A split at `key`, with no paid rate.
    pub(crate) fn at(key: Vec<u8>) -> Self {
        Self::Split {
            at: Some(key),
            paid_rate: None,
        }
    }
}

/// How a policy merges two leaves for one reason.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MergeDecision {
    /// The merge goes ahead when the merged leaf fits these limits. The
    /// merged leaf keeps `paid_rate` as its paid rate (ADR-076).
    Within {
        max_entries: usize,
        max_inline_bytes: usize,
        paid_rate: Option<Duration>,
    },
    /// The merge does not go ahead.
    Veto(MergeVeto),
}
