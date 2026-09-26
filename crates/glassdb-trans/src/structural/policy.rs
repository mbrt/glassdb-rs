//! The public seam where a topology policy decides the splits and merges of
//! leaves from one window of measurements (ADR-074).
//!
//! The engine keeps the decisions that correctness or the tree shape needs:
//! leaves that the hard cap rejects split, and index nodes split and merge on
//! size. It also checks each decision of a policy again, and skips the ones
//! that it cannot do safely.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::time::Duration;

use glassdb_data::ObjectPath;
use glassdb_storage::{LeafBody, NodeSizePolicy};

use super::avoidable::{MergeTime, SplitTime};

/// The window length of the policies in this module.
const DEFAULT_WINDOW: Duration = Duration::from_secs(1);

/// The default multiples of the typical split and merge times. They are less
/// than one, because a change keeps its effect after the window that paid for
/// it, and each database instance sees only its own part of the time
/// (ADR-074).
const DEFAULT_SPLIT_THRESHOLD: f64 = 0.25;
const DEFAULT_MERGE_THRESHOLD: f64 = 0.1;

/// Decides the splits and merges of leaves from one window of measurements.
///
/// A policy has no state that the engine must keep: it gets all of the
/// measurements of a window, and returns the changes that it wants. The engine
/// checks each change again against current state. It skips a split of a leaf
/// with less than two entries, a split at a key that leaves one half empty, a
/// merge of the root, and a merge that the merge rules of ADR-073 do not allow,
/// except the underfull threshold.
pub trait TopologyPolicy: Send + Sync + 'static {
    /// Returns the length of one window.
    fn window(&self) -> Duration {
        DEFAULT_WINDOW
    }

    /// Returns the leaf changes that the measurements of `window` call for.
    fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange>;
}

/// The measurements of one database instance in one window.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TopologyWindow {
    /// The time since the start of the window.
    pub elapsed: Duration,
    /// The typical time of one split of this database instance.
    pub split_time: Duration,
    /// The typical time of one merge of this database instance.
    pub merge_time: Duration,
    /// The node size policy of this database instance.
    pub node_size: NodeSizePolicy,
    /// The leaves with measurements in this window.
    pub leaves: BTreeMap<LeafId, LeafWindow>,
    /// The adjacent leaves with merge-side time in this window.
    pub pairs: Vec<PairWindow>,
}

/// The measurements of one leaf in one window.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct LeafWindow {
    /// The avoidable time that one split of the leaf can remove.
    pub avoidable: SplitTime,
    /// The split key of the last leaf delay in this window, if any. A split
    /// there puts the delayed keys and the other keys in different halves.
    /// The inline pressure of the leaf has no split key, because a split at
    /// the median removes it.
    pub split_key: Option<Vec<u8>>,
    /// The last size that this instance wrote or split, if any in this window.
    pub size: Option<LeafSize>,
    /// Whether a split that landed wrote the leaf in this window or the last
    /// one. The measurements start at the last change.
    pub split_recently: bool,
    /// Whether a merge that landed wrote the leaf in this window or the last
    /// one. The measurements start at the last change.
    pub merged_recently: bool,
}

/// The measurements of two adjacent leaves in one window.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PairWindow {
    pub left: LeafId,
    pub right: LeafId,
    /// The avoidable time that one merge of the two leaves can remove.
    pub avoidable: MergeTime,
}

/// The size of one leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct LeafSize {
    pub entries: usize,
    /// The entries that are not tombstones.
    pub live_entries: usize,
    pub encoded_bytes: usize,
}

/// An opaque identity of one leaf of one collection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LeafId(ObjectPath);

/// One change of one leaf that a policy asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TopologyChange {
    /// Split the leaf in two at its median.
    Split(LeafId),
    /// Split the leaf in two at the key. The engine skips the split if one
    /// half would be empty.
    SplitAt(LeafId, Vec<u8>),
    /// Merge the leaf into its right sibling.
    Merge(LeafId),
}

/// Splits a leaf when its split-side time in one window is more than a
/// multiple of the typical split time, and merges two adjacent leaves when
/// their merge-side time is more than a multiple of the typical merge time plus
/// the split-side time of both leaves (ADR-074). A split is at the split key of
/// the leaf when its leaf delays alone pay for it, else at the median. Leaves
/// over a soft cap also split, at the median. A leaf that a split wrote in this
/// window or the last one does not merge, and a leaf that a merge wrote does not
/// split on avoidable time.
#[derive(Debug, Clone, Copy)]
pub struct AvoidableTimePolicy {
    split_threshold: f64,
    merge_threshold: f64,
}

/// Decides on size alone, as the engine does without a policy: a leaf over a
/// soft cap or with inline pressure splits, and a leaf with few live entries
/// merges into its right sibling (ADR-031, ADR-056, ADR-073). Like the engine,
/// it has no hold-down window.
#[derive(Debug, Clone, Copy, Default)]
pub struct SizePolicy;

/// Changes no leaf. Only the hard cap splits leaves.
#[derive(Debug, Clone, Copy, Default)]
pub struct FixedTopology;

impl LeafSize {
    /// Reports whether the leaf is divisible and over a soft cap of `policy`.
    pub fn over_soft_cap(&self, policy: &NodeSizePolicy) -> bool {
        self.entries >= 2
            && (self.entries > policy.leaf_max_entries()
                || self.encoded_bytes > policy.node_soft_max_bytes())
    }

    /// Reports whether the leaf has fewer live entries than `policy` allows.
    pub fn underfull(&self, policy: &NodeSizePolicy) -> bool {
        self.live_entries < policy.leaf_min_entries()
    }

    pub(super) fn of(leaf: &LeafBody) -> Self {
        Self {
            entries: leaf.len(),
            live_entries: leaf.entries().filter(|entry| entry.exists()).count(),
            encoded_bytes: leaf.encoded_len(),
        }
    }
}

impl LeafId {
    /// Reports whether the leaf is the root of its tree, which cannot merge.
    pub fn is_root(&self) -> bool {
        matches!(self.0, ObjectPath::TreeRoot { .. })
    }

    pub(super) fn new(path: ObjectPath) -> Self {
        Self(path)
    }

    pub(super) fn into_path(self) -> ObjectPath {
        self.0
    }
}

impl fmt::Display for LeafId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl AvoidableTimePolicy {
    /// Creates the policy of ADR-074.
    pub fn new() -> Self {
        Self {
            split_threshold: DEFAULT_SPLIT_THRESHOLD,
            merge_threshold: DEFAULT_MERGE_THRESHOLD,
        }
    }

    /// Sets the multiple of the typical split time that the split-side time
    /// of a leaf must be more than. Defaults to 0.25.
    ///
    /// # Panics
    ///
    /// If `multiple` is negative or not finite.
    pub fn split_threshold(mut self, multiple: f64) -> Self {
        self.split_threshold = checked_multiple(multiple);
        self
    }

    /// Sets the multiple of the typical merge time that the merge-side time of
    /// two leaves must be more than, in addition to their split-side time.
    /// Defaults to 0.1.
    ///
    /// # Panics
    ///
    /// If `multiple` is negative or not finite.
    pub fn merge_threshold(mut self, multiple: f64) -> Self {
        self.merge_threshold = checked_multiple(multiple);
        self
    }
}

impl Default for AvoidableTimePolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl TopologyPolicy for AvoidableTimePolicy {
    fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
        let split_time = window.split_time.mul_f64(self.split_threshold);
        let merge_time = window.merge_time.mul_f64(self.merge_threshold);
        // Leaves that split recently, or that take part in a change of this
        // window, do not merge.
        let mut held: BTreeSet<&LeafId> = window
            .leaves
            .iter()
            .filter(|(_, leaf)| leaf.split_recently)
            .map(|(id, _)| id)
            .collect();
        let mut changes = Vec::new();
        for (id, leaf) in &window.leaves {
            let over_cap = leaf
                .size
                .is_some_and(|size| size.over_soft_cap(&window.node_size));
            let pays = !leaf.merged_recently && leaf.avoidable.total() > split_time;
            // Inline pressure asks for balanced halves, so the split key
            // decides only a split that the leaf delays pay for alone.
            let at_key = pays && leaf.avoidable.leaf_delays() > split_time;
            let change = match &leaf.split_key {
                Some(key) if at_key => TopologyChange::SplitAt(id.clone(), key.clone()),
                _ if pays || over_cap => TopologyChange::Split(id.clone()),
                _ => continue,
            };
            changes.push(change);
            held.insert(id);
        }
        let split_side = |id: &LeafId| {
            window
                .leaves
                .get(id)
                .map_or(Duration::ZERO, |leaf| leaf.avoidable.total())
        };
        for pair in &window.pairs {
            if held.contains(&pair.left) || held.contains(&pair.right) {
                continue;
            }
            let keep = merge_time + split_side(&pair.left) + split_side(&pair.right);
            if pair.avoidable.total() > keep {
                changes.push(TopologyChange::Merge(pair.left.clone()));
                held.insert(&pair.left);
                held.insert(&pair.right);
            }
        }
        changes
    }
}

impl TopologyPolicy for SizePolicy {
    fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
        let policy = &window.node_size;
        window
            .leaves
            .iter()
            .filter_map(|(id, leaf)| {
                let over_cap = leaf.size.is_some_and(|size| size.over_soft_cap(policy));
                if over_cap || !leaf.avoidable.inline_pressure.is_zero() {
                    Some(TopologyChange::Split(id.clone()))
                } else if !id.is_root() && leaf.size.is_some_and(|size| size.underfull(policy)) {
                    Some(TopologyChange::Merge(id.clone()))
                } else {
                    None
                }
            })
            .collect()
    }
}

impl TopologyPolicy for FixedTopology {
    fn decide(&self, _window: &TopologyWindow) -> Vec<TopologyChange> {
        Vec::new()
    }
}

fn checked_multiple(multiple: f64) -> f64 {
    assert!(
        multiple.is_finite() && multiple >= 0.0,
        "a threshold multiple must be finite and not negative, got {multiple}"
    );
    multiple
}

#[cfg(test)]
mod tests;
