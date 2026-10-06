//! The rules that decide the splits and merges of leaves from one window of
//! measurements (ADR-074).
//!
//! The engine keeps the decisions that correctness or the tree shape needs:
//! leaves that the hard cap rejects or that are over a soft cap split, leaves
//! with no live entries merge, and index nodes split and merge on size. It also
//! checks each request of a rule again, and skips the ones that it cannot do
//! safely.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use glassdb_data::ObjectPath;

/// The length of one window of measurements.
pub(super) const TOPOLOGY_WINDOW: Duration = Duration::from_secs(1);

/// The multiples of the typical split and merge times. They are less than one,
/// because a change keeps its effect after the windows that paid for it, and
/// each database instance sees only its own part of the time (ADR-074). The
/// split multiple applies to a moving average, which stays below the time of
/// the windows that have the most time.
const SPLIT_THRESHOLD: f64 = 0.05;
const MERGE_THRESHOLD: f64 = 0.1;

/// The multiple of the divided conflict time of a leaf that estimates the
/// time that a split adds. After a split, the transactions that conflict wait
/// for the locks of more leaves, and they conflict more (ADR-074).
const DIVIDED_CONFLICT_WEIGHT: f64 = 4.0;

/// The half-life of the moving averages of the avoidable time rule.
const HALF_LIFE: Duration = Duration::from_secs(10);

/// A moving average below this, in seconds, is zero, so that the rule drops
/// it.
const NEGLIGIBLE_SECONDS: f64 = 1e-6;

/// The windows with no measurement of a leaf or a pair after which the rule
/// drops its mean rate.
const STALE_MEAN_WINDOWS: u64 = 60;

/// What decides the splits and merges of leaves that the hard caps do not
/// force.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
#[non_exhaustive]
pub enum TopologyPolicy {
    /// The avoidable time of the transactions of each database instance
    /// decides (ADR-074). Leaves over a soft cap still split, and leaves with
    /// no live entries still merge.
    #[default]
    AvoidableTime,
    /// Soft caps, underfull thresholds, and inline pressure decide (ADR-031,
    /// ADR-056, ADR-073).
    SizeCauses,
}

/// Decides the splits and merges of leaves from one window of measurements.
///
/// The engine keeps no state for a rule: a rule gets all of the measurements
/// of a window, and returns the changes that it wants. A rule that decides on
/// more than one window keeps its own state. The engine checks each request
/// again against current state. It skips a split of a leaf with less than two
/// entries, a split at a key that leaves one half empty, a merge of the root,
/// and a merge that the merge rules of ADR-073 do not allow, except the
/// underfull threshold.
pub(super) trait TopologyRule: Send {
    /// Returns the changes that the measurements of `window` call for.
    fn decide(&mut self, window: &TopologyWindow) -> Vec<RuleRequest>;
}

/// The measurements of one database instance in one window.
#[derive(Debug, Clone)]
pub(super) struct TopologyWindow {
    /// The typical time of one split of this database instance.
    pub(super) split_time: Duration,
    /// The typical time of one merge of this database instance.
    pub(super) merge_time: Duration,
    /// The leaves with measurements or changes in this window.
    pub(super) leaves: BTreeMap<LeafId, LeafWindow>,
    /// The adjacent leaves with merge-side time in this window.
    pub(super) pairs: Vec<PairWindow>,
}

/// The measurements of one leaf in one window.
#[derive(Debug, Clone, Default)]
pub(super) struct LeafWindow {
    /// The lost CAS, queue wait, and slow leaf CAS time that one split at
    /// `split_key` can remove.
    pub(super) delay_time: Duration,
    /// The split key of the last leaf delay in this window, if any. A split
    /// there puts the delayed keys and the other keys in different halves.
    pub(super) split_key: Option<Vec<u8>>,
    /// The locked commit penalty of the direct commit candidates whose values
    /// the leaf could not carry inline. A split at the median can remove it.
    pub(super) inline_pressure_time: Duration,
    /// The time of the commit passes of this instance that a conflict ended
    /// without a commit, and whose point reads of the leaf a split at its
    /// median puts in both halves, each with the body run before it. After a
    /// split, these transactions wait for the locks of more leaves. A
    /// transaction that starves seldom commits, so only the time of these
    /// passes shows it in each window.
    pub(super) divided_conflict_time: Duration,
    /// Whether a split that landed wrote the leaf in this window or the last
    /// one. The measurements start at the last change.
    pub(super) split_recently: bool,
    /// Whether a merge that landed wrote the leaf in this window or the last
    /// one. The measurements start at the last change.
    pub(super) merged_recently: bool,
}

/// The measurements of two adjacent leaves in one window.
#[derive(Debug, Clone)]
pub(super) struct PairWindow {
    pub(super) left: LeafId,
    pub(super) right: LeafId,
    /// The adjacent cross-leaf miss and scan crossing time that one merge of
    /// the two leaves can remove.
    pub(super) avoidable_time: Duration,
    /// The time of the commit passes of this instance that a conflict ended
    /// without a commit, and whose point reads are in the two leaves and in
    /// no other leaf, each with the body run before it. After a merge, these
    /// transactions use one leaf.
    pub(super) crossing_conflict_time: Duration,
}

/// An opaque identity of one leaf of one collection.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(super) struct LeafId(ObjectPath);

/// One structural change that a rule asks for, with the rate of avoidable
/// time that pays for it. The leaves of the change keep this rate as their
/// paid rate, and the opposite change of these leaves must pay more
/// (ADR-076).
#[derive(Debug, Clone, PartialEq)]
pub(super) struct RuleRequest {
    pub(super) change: ChangeRequest,
    /// The avoidable time in each second.
    pub(super) rate: Duration,
}

/// One structural change of one leaf that a rule asks for.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum ChangeRequest {
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
/// its leaf delays alone pay for it, else at the median. A leaf that a split
/// wrote in this window or the last one does not merge, and a leaf that a merge
/// wrote does not split.
#[derive(Debug, Default)]
pub(super) struct AvoidableTimeRule {
    /// The moving averages of the net split-side time of each leaf.
    leaves: BTreeMap<LeafId, NetSplitTime>,
    /// The moving averages of the crossing conflict time of two adjacent
    /// leaves in one window, in seconds.
    crossing: BTreeMap<(LeafId, LeafId), f64>,
    /// The mean net split-side time of each leaf since its last change that
    /// this instance knows of. A leaf that receives a merge of another
    /// instance keeps its ID, so its mean can include windows from before
    /// that merge.
    split_rates: BTreeMap<LeafId, MeanRate>,
    /// The mean merge-side time of each pair since a change of its leaves
    /// that this instance knows of.
    merge_rates: BTreeMap<(LeafId, LeafId), MeanRate>,
    /// The number of windows that the rule decided.
    windows: u64,
}

/// The mean time in each window since one window, in seconds. A window with
/// no time counts as zero.
#[derive(Debug)]
struct MeanRate {
    since: u64,
    last: u64,
    sum: f64,
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

impl TopologyPolicy {
    /// Returns the rule of this policy, or none when the size causes decide.
    pub(super) fn rule(self) -> Option<Box<dyn TopologyRule>> {
        match self {
            TopologyPolicy::SizeCauses => None,
            TopologyPolicy::AvoidableTime => Some(Box::new(AvoidableTimeRule::default())),
        }
    }
}

impl LeafWindow {
    /// Returns the time that one split of the leaf can remove.
    fn avoidable_time(&self) -> Duration {
        self.delay_time + self.inline_pressure_time
    }

    /// Returns the time that one split of the leaf can remove, less the time
    /// that it would add, in seconds. It can be negative.
    fn net_split_seconds(&self) -> f64 {
        self.avoidable_time().as_secs_f64() - self.split_added_seconds()
    }

    /// Returns the estimate of the time that a split of the leaf would add to
    /// the transactions that conflict on keys of both halves, in seconds.
    fn split_added_seconds(&self) -> f64 {
        DIVIDED_CONFLICT_WEIGHT * self.divided_conflict_time.as_secs_f64()
    }
}

impl LeafId {
    pub(super) fn new(path: ObjectPath) -> Self {
        Self(path)
    }

    pub(super) fn into_path(self) -> ObjectPath {
        self.0
    }
}

impl AvoidableTimeRule {
    /// Returns the splits of the leaves whose net split-side time pays for a
    /// split, and holds these leaves.
    fn splits(
        &mut self,
        window: &TopologyWindow,
        decay: f64,
        held: &mut BTreeSet<LeafId>,
    ) -> Vec<RuleRequest> {
        let split_time = window.split_time.mul_f64(SPLIT_THRESHOLD).as_secs_f64();
        let Self {
            leaves,
            split_rates,
            windows,
            ..
        } = self;
        let windows = *windows;
        leaves.retain(|id, net| {
            net.decay(decay);
            window.leaves.contains_key(id) || !net.is_negligible()
        });
        let mut requests = Vec::new();
        for (id, leaf) in &window.leaves {
            // The measurements start again at each change of the leaf.
            if leaf.split_recently || leaf.merged_recently {
                leaves.remove(id);
                split_rates.remove(id);
            }
            split_rates
                .entry(id.clone())
                .or_insert_with(|| MeanRate::new(windows))
                .add(windows, leaf.net_split_seconds());
            let net = leaves.entry(id.clone()).or_default();
            net.add(leaf, 1.0 - decay);
            if leaf.merged_recently || net.total <= split_time {
                continue;
            }
            // Inline pressure asks for balanced halves, so the split key
            // decides only a split that the leaf delays pay for alone.
            let request = match &net.split_key {
                Some(key) if net.delays > split_time => {
                    ChangeRequest::SplitAt(id.clone(), key.clone())
                }
                _ => ChangeRequest::Split(id.clone()),
            };
            // A split can take more than one window to land. If the average
            // stays, the next window asks for a second split of the same
            // leaf. The engine retries a deferred split itself.
            leaves.remove(id);
            let rate = split_rates[id].mean(windows);
            requests.push(RuleRequest {
                change: request,
                rate,
            });
            held.insert(id.clone());
        }
        requests
    }

    /// Returns the merges of the adjacent leaves whose merge-side time pays
    /// for a merge, except of held leaves, and holds the merged leaves.
    fn merges(
        &mut self,
        window: &TopologyWindow,
        decay: f64,
        held: &mut BTreeSet<LeafId>,
    ) -> Vec<RuleRequest> {
        let merge_time = window.merge_time.mul_f64(MERGE_THRESHOLD);
        let Self {
            crossing,
            merge_rates,
            windows,
            ..
        } = self;
        let windows = *windows;
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
        merge_rates.retain(|(left, right), _| !changed(left) && !changed(right));
        for pair in &window.pairs {
            let time = pair.avoidable_time + pair.crossing_conflict_time;
            merge_rates
                .entry((pair.left.clone(), pair.right.clone()))
                .or_insert_with(|| MeanRate::new(windows))
                .add(windows, time.as_secs_f64());
        }
        // The other merge causes are of one window, because the leaves that
        // scans cross have them only in some windows, and their average stays
        // below the time of these windows.
        let mut merge_sides: BTreeMap<(LeafId, LeafId), Duration> = window
            .pairs
            .iter()
            .map(|pair| ((pair.left.clone(), pair.right.clone()), pair.avoidable_time))
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
                .map_or(Duration::ZERO, LeafWindow::avoidable_time)
        };
        let mut requests = Vec::new();
        for ((left, right), merge_side) in merge_sides {
            if held.contains(&left) || held.contains(&right) {
                continue;
            }
            if merge_side <= merge_time + split_side(&left) + split_side(&right) {
                continue;
            }
            let rate = self
                .merge_rates
                .get(&(left.clone(), right.clone()))
                .map_or_else(
                    || {
                        Duration::from_secs_f64(
                            merge_side.as_secs_f64() / TOPOLOGY_WINDOW.as_secs_f64(),
                        )
                    },
                    |mean| mean.mean(windows),
                );
            requests.push(RuleRequest {
                change: ChangeRequest::Merge(left.clone()),
                rate,
            });
            held.insert(left.clone());
            held.insert(right.clone());
            // A merge can take more than one window to land, and after it the
            // left leaf is gone. So the average must not ask again.
            crossing.remove(&(left, right));
        }
        requests
    }
}

impl TopologyRule for AvoidableTimeRule {
    fn decide(&mut self, window: &TopologyWindow) -> Vec<RuleRequest> {
        // The engine asks for a decision once in each window.
        let decay = 0.5_f64.powf(TOPOLOGY_WINDOW.as_secs_f64() / HALF_LIFE.as_secs_f64());
        // Leaves that split recently, or that take part in a change of this
        // window, do not merge.
        let mut held: BTreeSet<LeafId> = window
            .leaves
            .iter()
            .filter(|(_, leaf)| leaf.split_recently)
            .map(|(id, _)| id.clone())
            .collect();
        let mut requests = self.splits(window, decay, &mut held);
        requests.extend(self.merges(window, decay, &mut held));
        let windows = self.windows;
        self.split_rates.retain(|_, mean| !mean.is_stale(windows));
        self.merge_rates.retain(|_, mean| !mean.is_stale(windows));
        self.windows += 1;
        requests
    }
}

impl MeanRate {
    fn new(window: u64) -> Self {
        Self {
            since: window,
            last: window,
            sum: 0.0,
        }
    }

    /// Adds the time of the window `window`, in seconds.
    fn add(&mut self, window: u64, seconds: f64) {
        self.sum += seconds;
        self.last = window;
    }

    /// Returns the mean time in each window up to `window`, but not less
    /// than zero.
    fn mean(&self, window: u64) -> Duration {
        let windows = (window - self.since + 1) as f64;
        let seconds = (self.sum / windows) / TOPOLOGY_WINDOW.as_secs_f64();
        Duration::from_secs_f64(seconds.max(0.0))
    }

    /// Reports whether the leaf or pair had no measurement for so long that a
    /// mean of it measures a load that is gone.
    fn is_stale(&self, window: u64) -> bool {
        window - self.last > STALE_MEAN_WINDOWS
    }
}

impl NetSplitTime {
    /// Adds the measurements of `leaf` in one window with `weight`, less the
    /// time that a split would have added.
    fn add(&mut self, leaf: &LeafWindow, weight: f64) {
        self.total += weight * leaf.net_split_seconds();
        self.delays += weight * (leaf.delay_time.as_secs_f64() - leaf.split_added_seconds());
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

#[cfg(test)]
mod tests;
