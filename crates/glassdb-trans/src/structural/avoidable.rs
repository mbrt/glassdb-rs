//! The avoidable time of the leaves of one database instance (ADR-074).
//!
//! Transactions report the time that one split of a leaf, or one merge of two
//! adjacent leaves, would remove. When a topology policy decides the leaf
//! changes, the restructurer takes these reports one window at a time, and
//! measures the typical time of its own changes.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::{AddAssign, Sub};
use std::sync::Mutex;
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::ObjectPath;
use glassdb_storage::{LeafBody, NodeSizePolicy};

use crate::leaf_coord::LeafDelay;

use super::policy::{LeafId, LeafSize, LeafWindow, PairWindow, TopologyWindow};

/// The time of one split or merge until this database instance measures one.
const DEFAULT_CHANGE_TIME: Duration = Duration::from_millis(500);

/// The weight of a new measurement in a typical time is one part in this many.
const TYPICAL_TIME_WEIGHT: u32 = 8;

/// Avoidable time that one split of its leaf can remove.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitTime {
    /// Round members from a leaf CAS that failed, because a CAS for other keys
    /// landed first, until a CAS of the round landed.
    pub lost_cas: Duration,
    /// Waits of round members for an earlier round with none of their keys.
    pub queue_wait: Duration,
    /// Round members in a leaf CAS, for the time above a slow leaf CAS of the
    /// instance.
    pub slow_cas: Duration,
    /// Locked commits of direct commit candidates whose values the leaf could
    /// not carry inline, minus the typical direct commit time.
    pub inline_pressure: Duration,
}

/// Avoidable time that one merge of two adjacent leaves can remove.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeTime {
    /// Locked commits of direct commit candidates whose keys are in the two
    /// leaves, minus the typical direct commit time.
    pub adjacent_miss: Duration,
    /// Reads of the right leaf by scans that continued from the left leaf.
    pub scan_crossing: Duration,
}

/// Avoidable time that this database instance measured since the last reset.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct AvoidableTimeStats {
    pub split: SplitTime,
    pub merge: MergeTime,
}

/// Collects the avoidable time of one database instance, and when a topology
/// policy decides the leaf changes, the windows that it decides on.
#[derive(Debug)]
pub(super) struct AvoidableTime {
    // Without a policy nothing takes the windows, so they must stay empty.
    windows: bool,
    state: Mutex<State>,
    split_time: TypicalTime,
    merge_time: TypicalTime,
}

/// The kind of a structural change whose time the restructurer measured.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChangeKind {
    Split,
    Merge,
}

#[derive(Debug)]
struct State {
    window_started: rt::Instant,
    leaves: BTreeMap<ObjectPath, LeafWindow>,
    pairs: BTreeMap<(ObjectPath, ObjectPath), MergeTime>,
    changed: BTreeSet<ObjectPath>,
    changed_last_window: BTreeSet<ObjectPath>,
    totals: AvoidableTimeStats,
}

/// The moving average of the time of one operation.
#[derive(Debug, Default)]
pub(crate) struct TypicalTime {
    average: Mutex<Option<Duration>>,
}

impl SplitTime {
    /// Returns the sum of all causes.
    pub fn total(&self) -> Duration {
        self.lost_cas + self.queue_wait + self.slow_cas + self.inline_pressure
    }

    /// Returns the split-side time of one leaf delay.
    pub(super) fn of_delay(delay: LeafDelay, time: Duration) -> Self {
        let mut split = Self::default();
        let field = match delay {
            LeafDelay::LostCas => &mut split.lost_cas,
            LeafDelay::QueueWait => &mut split.queue_wait,
            LeafDelay::SlowCas => &mut split.slow_cas,
        };
        *field = time;
        split
    }
}

impl MergeTime {
    /// Returns the sum of all causes.
    pub fn total(&self) -> Duration {
        self.adjacent_miss + self.scan_crossing
    }
}

impl AddAssign for SplitTime {
    fn add_assign(&mut self, rhs: Self) {
        self.lost_cas += rhs.lost_cas;
        self.queue_wait += rhs.queue_wait;
        self.slow_cas += rhs.slow_cas;
        self.inline_pressure += rhs.inline_pressure;
    }
}

impl Sub for SplitTime {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            lost_cas: self.lost_cas.saturating_sub(rhs.lost_cas),
            queue_wait: self.queue_wait.saturating_sub(rhs.queue_wait),
            slow_cas: self.slow_cas.saturating_sub(rhs.slow_cas),
            inline_pressure: self.inline_pressure.saturating_sub(rhs.inline_pressure),
        }
    }
}

impl AddAssign for MergeTime {
    fn add_assign(&mut self, rhs: Self) {
        self.adjacent_miss += rhs.adjacent_miss;
        self.scan_crossing += rhs.scan_crossing;
    }
}

impl Sub for MergeTime {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            adjacent_miss: self.adjacent_miss.saturating_sub(rhs.adjacent_miss),
            scan_crossing: self.scan_crossing.saturating_sub(rhs.scan_crossing),
        }
    }
}

impl AddAssign for AvoidableTimeStats {
    fn add_assign(&mut self, rhs: Self) {
        self.split += rhs.split;
        self.merge += rhs.merge;
    }
}

impl Sub for AvoidableTimeStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            split: self.split - rhs.split,
            merge: self.merge - rhs.merge,
        }
    }
}

impl TypicalTime {
    /// Returns the typical time, or none before the first measurement.
    pub(crate) fn get(&self) -> Option<Duration> {
        *self.average.lock().unwrap()
    }

    /// Adds one measurement.
    pub(crate) fn record(&self, took: Duration) {
        let mut average = self.average.lock().unwrap();
        *average = Some(match *average {
            None => took,
            Some(old) if took >= old => old + (took - old) / TYPICAL_TIME_WEIGHT,
            Some(old) => old - (old - took) / TYPICAL_TIME_WEIGHT,
        });
    }
}

impl AvoidableTime {
    /// Creates a collector that only keeps totals.
    pub(super) fn totals_only() -> Self {
        Self::new(false)
    }

    /// Creates a collector that also keeps the windows of a topology policy.
    pub(super) fn with_windows() -> Self {
        Self::new(true)
    }

    /// Adds split-side time of the leaf at `path`.
    pub(super) fn add_split_time(&self, path: &ObjectPath, time: SplitTime) {
        if time.total().is_zero() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.totals.split += time;
        if self.windows {
            state.leaf(path).avoidable += time;
        }
    }

    /// Adds merge-side time of the adjacent leaves at `left` and `right`.
    pub(super) fn add_merge_time(&self, left: &ObjectPath, right: &ObjectPath, time: MergeTime) {
        if time.total().is_zero() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.totals.merge += time;
        if self.windows {
            *state
                .pairs
                .entry((left.clone(), right.clone()))
                .or_default() += time;
        }
    }

    /// Records the last known size of the leaf at `path`.
    pub(super) fn observe_size(&self, path: &ObjectPath, leaf: &LeafBody) {
        if self.windows {
            self.state.lock().unwrap().leaf(path).size = Some(LeafSize::of(leaf));
        }
    }

    /// Records that a structural change that landed wrote the leaf at `path`.
    pub(super) fn record_changed(&self, path: ObjectPath) {
        if self.windows {
            self.state.lock().unwrap().changed.insert(path);
        }
    }

    /// Records the time of one structural change that landed.
    pub(super) fn record_change(&self, kind: ChangeKind, took: Duration) {
        match kind {
            ChangeKind::Split => self.split_time.record(took),
            ChangeKind::Merge => self.merge_time.record(took),
        }
    }

    /// Returns the measurements since the last call, and starts a new window.
    pub(super) fn take_window(&self, node_size: NodeSizePolicy) -> TopologyWindow {
        let mut state = self.state.lock().unwrap();
        let now = rt::Instant::now();
        let elapsed = now.saturating_duration_since(state.window_started);
        state.window_started = now;
        let changed = std::mem::take(&mut state.changed);
        let changed_before = std::mem::replace(&mut state.changed_last_window, changed.clone());
        for path in changed_before.into_iter().chain(changed) {
            state.leaf(&path).changed = true;
        }
        let leaves = std::mem::take(&mut state.leaves)
            .into_iter()
            .map(|(path, leaf)| (LeafId::new(path), leaf))
            .collect();
        let pairs = std::mem::take(&mut state.pairs)
            .into_iter()
            .map(|((left, right), avoidable)| PairWindow {
                left: LeafId::new(left),
                right: LeafId::new(right),
                avoidable,
            })
            .collect();
        drop(state);
        TopologyWindow {
            elapsed,
            split_time: self.split_time.get().unwrap_or(DEFAULT_CHANGE_TIME),
            merge_time: self.merge_time.get().unwrap_or(DEFAULT_CHANGE_TIME),
            node_size,
            leaves,
            pairs,
        }
    }

    /// Returns and resets the totals since the last call.
    pub(super) fn take_stats(&self) -> AvoidableTimeStats {
        std::mem::take(&mut self.state.lock().unwrap().totals)
    }

    fn new(windows: bool) -> Self {
        Self {
            windows,
            state: Mutex::new(State {
                window_started: rt::Instant::now(),
                leaves: BTreeMap::new(),
                pairs: BTreeMap::new(),
                changed: BTreeSet::new(),
                changed_last_window: BTreeSet::new(),
                totals: AvoidableTimeStats::default(),
            }),
            split_time: TypicalTime::default(),
            merge_time: TypicalTime::default(),
        }
    }
}

impl State {
    fn leaf(&mut self, path: &ObjectPath) -> &mut LeafWindow {
        self.leaves.entry(path.clone()).or_default()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::TypicalTime;

    #[test]
    fn typical_time_moves_one_eighth_toward_each_measurement() {
        let typical = TypicalTime::default();
        assert_eq!(typical.get(), None);

        typical.record(Duration::from_millis(80));
        typical.record(Duration::from_millis(160));
        assert_eq!(typical.get(), Some(Duration::from_millis(90)));

        typical.record(Duration::from_millis(10));
        assert_eq!(typical.get(), Some(Duration::from_millis(80)));
    }
}
