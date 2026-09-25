//! The avoidable time of the leaves of one database instance (ADR-074).
//!
//! Transactions report the time that one split of a leaf, or one merge of two
//! adjacent leaves, would remove.

use std::ops::{AddAssign, Sub};
use std::sync::Mutex;
use std::time::Duration;

use glassdb_data::ObjectPath;

use crate::leaf_coord::LeafDelay;

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

/// Collects the avoidable time of one database instance.
#[derive(Debug, Default)]
pub(super) struct AvoidableTime {
    totals: Mutex<AvoidableTimeStats>,
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
    /// Adds split-side time of the leaf at `path`. Only the totals keep it,
    /// until a policy decides with the time of each leaf.
    pub(super) fn add_split_time(&self, _path: &ObjectPath, time: SplitTime) {
        self.totals.lock().unwrap().split += time;
    }

    /// Adds merge-side time of the adjacent leaves at `left` and `right`.
    pub(super) fn add_merge_time(&self, _left: &ObjectPath, _right: &ObjectPath, time: MergeTime) {
        self.totals.lock().unwrap().merge += time;
    }

    /// Returns and resets the totals since the last call.
    pub(super) fn take_stats(&self) -> AvoidableTimeStats {
        std::mem::take(&mut *self.totals.lock().unwrap())
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
