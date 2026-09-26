//! The avoidable time of the leaves of one database instance, and the rule
//! that decides leaf splits and merges from it (ADR-074).
//!
//! Transactions report the time that one split of a leaf, or one merge of two
//! adjacent leaves, would remove. The sums of one window decide a change as
//! soon as they pay for it.

use std::collections::BTreeMap;
use std::ops::{AddAssign, Sub};
use std::sync::Mutex;
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::ObjectPath;

use crate::leaf_coord::LeafDelay;

/// The length of one window of avoidable time.
const WINDOW: Duration = Duration::from_secs(1);

/// The time of one split or merge until this database instance measures one.
const DEFAULT_CHANGE_TIME: Duration = Duration::from_millis(500);

/// The weight of a new measurement in a typical time is one part in this many.
const TYPICAL_TIME_WEIGHT: u32 = 8;

/// The default multiples of the typical split and merge times. They are less
/// than one, because a change keeps its effect after the window that paid for
/// it, and each database instance sees only its own part of the time
/// (ADR-074).
const DEFAULT_SPLIT_THRESHOLD: f64 = 0.25;
const DEFAULT_MERGE_THRESHOLD: f64 = 0.1;

/// What decides the splits and merges of leaves below the hard cap. Index
/// nodes split and merge on size with every rule.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum LeafChanges {
    /// Avoidable time decides (ADR-074). A leaf splits when its split-side
    /// time in one window is more than `split_threshold` typical splits. Two
    /// adjacent leaves merge when their merge-side time in one window is more
    /// than `merge_threshold` typical merges, plus the split-side time of both.
    /// Leaves over a soft cap also split.
    AvoidableTime {
        split_threshold: f64,
        merge_threshold: f64,
    },
    /// Soft caps, underfull thresholds, and inline pressure decide (ADR-031,
    /// ADR-056, ADR-073).
    Size,
}

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

/// Collects the avoidable time of one database instance, and decides which
/// leaves pay for a change.
#[derive(Debug)]
pub(super) struct AvoidableTime {
    // None when the size causes decide: then only the totals are kept.
    thresholds: Option<Thresholds>,
    state: Mutex<State>,
    split_time: TypicalTime,
    merge_time: TypicalTime,
}

/// The kind of a structural change of a leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ChangeKind {
    Split,
    Merge,
}

/// Where a split that the avoidable time of a leaf pays for divides the leaf.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum SplitPlace {
    /// At the split key of the last leaf delay, because the leaf delays alone
    /// pay for the split.
    SplitKey,
    /// At the median, because inline pressure asks for balanced halves.
    Median,
}

/// The moving average of the time of one operation.
#[derive(Debug, Default)]
pub(crate) struct TypicalTime {
    average: Mutex<Option<Duration>>,
}

#[derive(Debug, Clone, Copy)]
struct Thresholds {
    split: f64,
    merge: f64,
}

#[derive(Debug)]
struct State {
    window_started: rt::Instant,
    leaves: BTreeMap<ObjectPath, WindowSum<SplitTime>>,
    pairs: BTreeMap<(ObjectPath, ObjectPath), WindowSum<MergeTime>>,
    changes: Vec<(ObjectPath, ChangeKind)>,
    changes_last_window: Vec<(ObjectPath, ChangeKind)>,
    totals: AvoidableTimeStats,
}

/// The time of one leaf or pair in the current window.
#[derive(Debug, Default)]
struct WindowSum<T> {
    time: T,
    // One change for each leaf or pair in a window is enough: the candidate
    // queue retries a change that does not land.
    requested: bool,
}

impl LeafChanges {
    /// Returns the avoidable time rule with the thresholds of ADR-074.
    pub fn avoidable_time() -> Self {
        Self::AvoidableTime {
            split_threshold: DEFAULT_SPLIT_THRESHOLD,
            merge_threshold: DEFAULT_MERGE_THRESHOLD,
        }
    }
}

impl Default for LeafChanges {
    fn default() -> Self {
        Self::avoidable_time()
    }
}

impl SplitTime {
    /// Returns the sum of all causes.
    pub fn total(&self) -> Duration {
        self.leaf_delays() + self.inline_pressure
    }

    /// Returns the sum of the causes that a split at the split key removes.
    pub fn leaf_delays(&self) -> Duration {
        self.lost_cas + self.queue_wait + self.slow_cas
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
    /// Creates a collector for `rule`.
    ///
    /// # Panics
    ///
    /// If a threshold of `rule` is negative or not finite.
    pub(super) fn new(rule: LeafChanges) -> Self {
        let thresholds = match rule {
            LeafChanges::AvoidableTime {
                split_threshold,
                merge_threshold,
            } => Some(Thresholds {
                split: checked_multiple(split_threshold),
                merge: checked_multiple(merge_threshold),
            }),
            LeafChanges::Size => None,
        };
        Self {
            thresholds,
            state: Mutex::new(State {
                window_started: rt::Instant::now(),
                leaves: BTreeMap::new(),
                pairs: BTreeMap::new(),
                changes: Vec::new(),
                changes_last_window: Vec::new(),
                totals: AvoidableTimeStats::default(),
            }),
            split_time: TypicalTime::default(),
            merge_time: TypicalTime::default(),
        }
    }

    /// Reports whether avoidable time decides the leaf changes.
    pub(super) fn decides(&self) -> bool {
        self.thresholds.is_some()
    }

    /// Adds split-side time of the leaf at `path`. Returns where to split the
    /// leaf once in a window, when the time of the leaf in the window pays for
    /// one split.
    pub(super) fn add_split_time(&self, path: &ObjectPath, time: SplitTime) -> Option<SplitPlace> {
        if time.total().is_zero() {
            return None;
        }
        let mut state = self.state.lock().unwrap();
        state.totals.split += time;
        let thresholds = self.thresholds?;
        state.roll(rt::Instant::now());
        let held = state.changed_recently(path, ChangeKind::Merge);
        let split_time = self.typical(&self.split_time).mul_f64(thresholds.split);
        let leaf = state.leaves.entry(path.clone()).or_default();
        leaf.time += time;
        let pays = !held && !leaf.requested && leaf.time.total() > split_time;
        leaf.requested |= pays;
        pays.then(|| {
            if leaf.time.leaf_delays() > split_time {
                SplitPlace::SplitKey
            } else {
                SplitPlace::Median
            }
        })
    }

    /// Adds merge-side time of the adjacent leaves at `left` and `right`.
    /// Returns true once in a window, when the time of the two leaves in the
    /// window pays for one merge and for the split-side time of both leaves.
    pub(super) fn add_merge_time(
        &self,
        left: &ObjectPath,
        right: &ObjectPath,
        time: MergeTime,
    ) -> bool {
        if time.total().is_zero() {
            return false;
        }
        let mut state = self.state.lock().unwrap();
        state.totals.merge += time;
        let Some(thresholds) = self.thresholds else {
            return false;
        };
        state.roll(rt::Instant::now());
        let held = [left, right].into_iter().any(|path| {
            state.changed_recently(path, ChangeKind::Split)
                || state.leaves.get(path).is_some_and(|leaf| leaf.requested)
        });
        let split_side = |path: &ObjectPath| {
            state
                .leaves
                .get(path)
                .map_or(Duration::ZERO, |leaf| leaf.time.total())
        };
        let keep = self.typical(&self.merge_time).mul_f64(thresholds.merge)
            + split_side(left)
            + split_side(right);
        let pair = state
            .pairs
            .entry((left.clone(), right.clone()))
            .or_default();
        pair.time += time;
        let pays = !held && !pair.requested && pair.time.total() > keep;
        pair.requested |= pays;
        pays
    }

    /// Records that a structural change of `kind` that landed wrote the leaf
    /// at `path`.
    pub(super) fn record_changed(&self, path: ObjectPath, kind: ChangeKind) {
        if self.thresholds.is_none() {
            return;
        }
        let mut state = self.state.lock().unwrap();
        state.roll(rt::Instant::now());
        // Time from before the change measured a topology that is gone. If it
        // stays, the same time can pay for the same change again.
        state.leaves.remove(&path);
        state
            .pairs
            .retain(|(left, right), _| *left != path && *right != path);
        state.changes.push((path, kind));
    }

    /// Records the time of one structural change that landed.
    pub(super) fn record_change(&self, kind: ChangeKind, took: Duration) {
        match kind {
            ChangeKind::Split => self.split_time.record(took),
            ChangeKind::Merge => self.merge_time.record(took),
        }
    }

    /// Returns and resets the totals since the last call.
    pub(super) fn take_stats(&self) -> AvoidableTimeStats {
        std::mem::take(&mut self.state.lock().unwrap().totals)
    }

    fn typical(&self, time: &TypicalTime) -> Duration {
        time.get().unwrap_or(DEFAULT_CHANGE_TIME)
    }
}

impl State {
    /// Starts a new window when the current one has ended. A leaf that
    /// changed in the window before stays held down for one more window.
    fn roll(&mut self, now: rt::Instant) {
        let elapsed = now.saturating_duration_since(self.window_started);
        if elapsed < WINDOW {
            return;
        }
        self.window_started = now;
        self.leaves.clear();
        self.pairs.clear();
        let changes = std::mem::take(&mut self.changes);
        self.changes_last_window = if elapsed < 2 * WINDOW {
            changes
        } else {
            Vec::new()
        };
    }

    /// Reports whether a change of `kind` wrote the leaf at `path` in this
    /// window or the last one.
    fn changed_recently(&self, path: &ObjectPath, kind: ChangeKind) -> bool {
        self.changes
            .iter()
            .chain(&self.changes_last_window)
            .any(|(changed, changed_kind)| changed == path && *changed_kind == kind)
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
