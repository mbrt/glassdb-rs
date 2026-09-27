//! The public seam where a topology policy decides the splits and merges of
//! leaves from one window of measurements (ADR-074).
//!
//! The engine keeps the decisions that correctness or the tree shape needs:
//! leaves that the hard cap rejects split, and index nodes split and merge on
//! size. It also checks each decision of a policy again, and skips the ones
//! that it cannot do safely.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;
use std::sync::Mutex;
use std::time::Duration;

use glassdb_data::ObjectPath;
use glassdb_storage::{LeafBody, NodeSizePolicy};

use super::avoidable::{CommitKind, MergeTime, SplitTime};

/// The window length of the policies in this module.
const DEFAULT_WINDOW: Duration = Duration::from_secs(1);

/// The default multiples of the typical split and merge times. They are less
/// than one, because a change keeps its effect after the windows that paid for
/// it, and each database instance sees only its own part of the time
/// (ADR-074). The split multiple applies to a moving average, which stays
/// below the time of the windows that have the most time.
const DEFAULT_SPLIT_THRESHOLD: f64 = 0.05;
const DEFAULT_MERGE_THRESHOLD: f64 = 0.1;

/// The multiple of the divided conflict time of a leaf that estimates the
/// time that a split adds. After a split, the transactions that conflict wait
/// for the locks of more leaves, and they conflict more (ADR-074).
const DIVIDED_CONFLICT_WEIGHT: f64 = 4.0;

/// The half-life of the moving averages of the avoidable time policy.
const HALF_LIFE: Duration = Duration::from_secs(10);

/// A moving average below this, in seconds, is zero, so that the policy
/// drops it.
const NEGLIGIBLE_SECONDS: f64 = 1e-6;

/// Decides the splits and merges of leaves from one window of measurements.
///
/// The engine keeps no state for a policy: a policy gets all of the
/// measurements of a window, and returns the changes that it wants. A policy
/// that decides on more than one window keeps its own state. The engine
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
    /// The transactions of this instance that committed after point reads of
    /// the leaf.
    pub committed: TransactionCounts,
    /// The committed transactions whose point reads of the leaf a split at
    /// its median puts in both halves. After the split, they use one more
    /// leaf, and a direct commit becomes a locked commit.
    pub divided: TransactionCounts,
    /// The sum of the latencies of the committed transactions, each from its
    /// start to its commit.
    pub latency: Duration,
    /// The time of the commit passes of this instance whose point reads of
    /// the leaf a split at its median puts in both halves, each with the body
    /// run before it, also of the passes that did not commit. It shows the
    /// transactions that seldom commit.
    pub divided_time: Duration,
    /// The part of `divided_time` in commit passes that a conflict ended
    /// without a commit. After a split, these transactions wait for the locks
    /// of more leaves.
    pub divided_conflict_time: Duration,
    /// The coordinator rounds of this instance whose leaf CAS landed.
    pub rounds: u64,
    /// The round members of those rounds. A split divides the members of a
    /// round between two leaves, so each leaf CAS carries fewer of them.
    pub round_members: u64,
}

/// Transactions, by the commit that they used.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct TransactionCounts {
    pub direct: u64,
    pub locked: u64,
    pub read_only: u64,
}

/// The measurements of two adjacent leaves in one window.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PairWindow {
    pub left: LeafId,
    pub right: LeafId,
    /// The avoidable time that one merge of the two leaves can remove.
    pub avoidable: MergeTime,
    /// The time of the commit passes of this instance whose point reads are
    /// in the two leaves and in no other leaf, each with the body run before
    /// it, also of the passes that did not commit. After a merge, these
    /// transactions use one leaf.
    pub crossing_time: Duration,
    /// The part of `crossing_time` in commit passes that a conflict ended
    /// without a commit.
    pub crossing_conflict_time: Duration,
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

/// Splits a leaf when the moving average of its net split-side time is more
/// than a multiple of the typical split time, and merges two adjacent leaves
/// when their merge-side time is more than a multiple of the typical merge
/// time plus the split-side time of both leaves (ADR-074).
///
/// The net split-side time is the split-side time, less an estimate of the
/// time that a split adds to the transactions that conflict on keys of both
/// halves. The merge side also has the moving average of the conflict time of
/// the transactions over the two leaves only, which shows the time that a
/// split added after the split. A split is at the split key of the leaf when
/// its leaf delays alone pay for it, else at the median. Leaves over a soft cap
/// also split, at the median. A leaf that a split wrote in this window or the
/// last one does not merge, and a leaf that a merge wrote does not split on
/// avoidable time.
#[derive(Debug)]
pub struct AvoidableTimePolicy {
    split_threshold: f64,
    merge_threshold: f64,
    averages: Mutex<Averages>,
}

/// The moving averages of the avoidable time policy.
#[derive(Debug, Default)]
struct Averages {
    leaves: BTreeMap<LeafId, NetSplitTime>,
    /// The crossing conflict time of two adjacent leaves in one window, in
    /// seconds.
    crossing: BTreeMap<(LeafId, LeafId), f64>,
}

/// The moving averages of the split-side time of one leaf in one window, less
/// the time that a split adds, in seconds. They can be negative.
#[derive(Debug, Default)]
struct NetSplitTime {
    /// All split causes.
    total: f64,
    /// The leaf delays alone.
    delays: f64,
    /// The split key of the last leaf delay since the leaf changed, because
    /// the delays of the average come from all these windows.
    split_key: Option<Vec<u8>>,
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

impl TransactionCounts {
    /// Returns the sum of all commits.
    pub fn total(&self) -> u64 {
        self.direct + self.locked + self.read_only
    }

    pub(super) fn add(&mut self, kind: CommitKind) {
        let count = match kind {
            CommitKind::Direct => &mut self.direct,
            CommitKind::Locked => &mut self.locked,
            CommitKind::ReadOnly => &mut self.read_only,
        };
        *count += 1;
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
            averages: Mutex::default(),
        }
    }

    /// Sets the multiple of the typical split time that the moving average of
    /// the net split-side time of a leaf must be more than. Defaults to 0.05.
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

    /// Returns the splits of the leaves whose net split-side time pays for a
    /// split or that are over a soft cap, and holds these leaves.
    fn splits(
        &self,
        window: &TopologyWindow,
        leaves: &mut BTreeMap<LeafId, NetSplitTime>,
        decay: f64,
        held: &mut BTreeSet<LeafId>,
    ) -> Vec<TopologyChange> {
        let split_time = window
            .split_time
            .mul_f64(self.split_threshold)
            .as_secs_f64();
        leaves.retain(|id, net| {
            net.decay(decay);
            window.leaves.contains_key(id) || !net.is_negligible()
        });
        let mut changes = Vec::new();
        for (id, leaf) in &window.leaves {
            // The measurements start again at each change of the leaf.
            if leaf.split_recently || leaf.merged_recently {
                leaves.remove(id);
            }
            let net = leaves.entry(id.clone()).or_default();
            net.add(leaf, 1.0 - decay);
            let over_cap = leaf
                .size
                .is_some_and(|size| size.over_soft_cap(&window.node_size));
            let pays = !leaf.merged_recently && net.total > split_time;
            // Inline pressure asks for balanced halves, so the split key
            // decides only a split that the leaf delays pay for alone.
            let at_key = pays && net.delays > split_time;
            let change = match &net.split_key {
                Some(key) if at_key => TopologyChange::SplitAt(id.clone(), key.clone()),
                _ if pays || over_cap => TopologyChange::Split(id.clone()),
                _ => continue,
            };
            // A split can take more than one window to land. If the average
            // stays, the next window asks for a second split of the same
            // leaf. The engine retries a deferred split itself.
            leaves.remove(id);
            changes.push(change);
            held.insert(id.clone());
        }
        changes
    }

    /// Returns the merges of the adjacent leaves whose merge-side time pays
    /// for a merge, except of held leaves, and holds the merged leaves.
    fn merges(
        &self,
        window: &TopologyWindow,
        crossing: &mut BTreeMap<(LeafId, LeafId), f64>,
        decay: f64,
        held: &mut BTreeSet<LeafId>,
    ) -> Vec<TopologyChange> {
        let merge_time = window.merge_time.mul_f64(self.merge_threshold);
        let changed = |id: &LeafId| {
            window
                .leaves
                .get(id)
                .is_some_and(|leaf| leaf.split_recently || leaf.merged_recently)
        };
        crossing.retain(|(left, right), time| {
            *time *= decay;
            // The measurements start again at each change of the leaves.
            !changed(left) && !changed(right) && *time >= NEGLIGIBLE_SECONDS
        });
        // The other merge causes are of one window, because the leaves that
        // scans cross have them only in some windows, and their average stays
        // below the time of these windows.
        let mut merge_sides: BTreeMap<(LeafId, LeafId), Duration> = window
            .pairs
            .iter()
            .map(|pair| {
                (
                    (pair.left.clone(), pair.right.clone()),
                    pair.avoidable.total(),
                )
            })
            .collect();
        for pair in &window.pairs {
            *crossing
                .entry((pair.left.clone(), pair.right.clone()))
                .or_default() += (1.0 - decay) * pair.crossing_conflict_time.as_secs_f64();
        }
        // The average also decides for the pairs of earlier windows. When
        // another instance splits a leaf of such a pair, its transactions are
        // over three leaves, and no pair has their crossing time. The merge of
        // the left leaf with its current right sibling then takes one of these
        // leaves away.
        for (ids, average) in crossing.iter() {
            *merge_sides.entry(ids.clone()).or_default() += Duration::from_secs_f64(*average);
        }
        let split_side = |id: &LeafId| {
            window
                .leaves
                .get(id)
                .map_or(Duration::ZERO, |leaf| leaf.avoidable.total())
        };
        let mut changes = Vec::new();
        for ((left, right), merge_side) in merge_sides {
            if held.contains(&left) || held.contains(&right) {
                continue;
            }
            if merge_side <= merge_time + split_side(&left) + split_side(&right) {
                continue;
            }
            changes.push(TopologyChange::Merge(left.clone()));
            held.insert(left.clone());
            held.insert(right.clone());
            // A merge can take more than one window to land, and after it the
            // left leaf is gone. So the average must not ask again.
            crossing.remove(&(left, right));
        }
        changes
    }
}

impl Default for AvoidableTimePolicy {
    fn default() -> Self {
        Self::new()
    }
}

impl TopologyPolicy for AvoidableTimePolicy {
    fn decide(&self, window: &TopologyWindow) -> Vec<TopologyChange> {
        let decay = 0.5_f64.powf(window.elapsed.as_secs_f64() / HALF_LIFE.as_secs_f64());
        let mut averages = self.averages.lock().unwrap();
        let Averages { leaves, crossing } = &mut *averages;
        // Leaves that split recently, or that take part in a change of this
        // window, do not merge.
        let mut held: BTreeSet<LeafId> = window
            .leaves
            .iter()
            .filter(|(_, leaf)| leaf.split_recently)
            .map(|(id, _)| id.clone())
            .collect();
        let mut changes = self.splits(window, leaves, decay, &mut held);
        changes.extend(self.merges(window, crossing, decay, &mut held));
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

impl NetSplitTime {
    /// Adds the measurements of `leaf` in one window with `weight`, less the
    /// time that a split would have added.
    fn add(&mut self, leaf: &LeafWindow, weight: f64) {
        let added = DIVIDED_CONFLICT_WEIGHT * leaf.divided_conflict_time.as_secs_f64();
        self.total += weight * (leaf.avoidable.total().as_secs_f64() - added);
        self.delays += weight * (leaf.avoidable.leaf_delays().as_secs_f64() - added);
        if leaf.split_key.is_some() {
            self.split_key.clone_from(&leaf.split_key);
        }
    }

    fn decay(&mut self, factor: f64) {
        self.total *= factor;
        self.delays *= factor;
    }

    fn is_negligible(&self) -> bool {
        self.total.abs() < NEGLIGIBLE_SECONDS && self.delays.abs() < NEGLIGIBLE_SECONDS
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
