//! The monotonic floors of the stale views of one database instance, and how
//! one stale read-only transaction of a view reads at its floor (ADR-102,
//! ADR-103).
//!
//! A floor point orders the calls of a view and selects the states that a
//! read uses. It is not currentness evidence: do not convert it to a barrier
//! or a requirement (`docs/guides/storage-consistency.md`).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::timeline::{SequencePoint, StalenessCutoff};

/// The earliest cut point that the next stale read-only transaction of a
/// stale view can use (ADR-097, ADR-102). Clones share one floor.
///
/// It only rises. Use it only with the timeline of the database instance of
/// its view. [`ViewFloors::new_floor`] creates it.
#[derive(Debug, Clone)]
pub struct MonotonicFloor(Arc<AtomicU64>);

impl MonotonicFloor {
    /// Captures how one stale read-only transaction reads: at the floor, if an
    /// earlier transaction raised it and it is not older than `cutoff`
    /// (ADR-103).
    pub fn view_read(&self, cutoff: StalenessCutoff) -> ViewRead {
        let floor = self.get();
        ViewRead(
            (floor != SequencePoint::default() && floor >= cutoff.point())
                .then_some(FloorPoint(floor)),
        )
    }

    pub(crate) fn get(&self) -> SequencePoint {
        SequencePoint::from_raw(self.0.load(Ordering::SeqCst))
    }

    pub(crate) fn raise(&self, point: SequencePoint) {
        self.0.fetch_max(point.raw(), Ordering::SeqCst);
    }
}

/// How the reads of one stale read-only transaction of a stale view use the
/// decoded cache (ADR-103). Only storage modules see its point.
#[derive(Debug, Clone, Copy)]
pub struct ViewRead(Option<FloorPoint>);

impl ViewRead {
    /// Returns how a replay of the body reads: at the latest states, because
    /// the replay follows a cut that the floor could not give. The reads still
    /// mark the leaves that they read (ADR-103).
    pub fn for_replay(self) -> Self {
        Self(None)
    }

    /// The floor at which the reads use the state that was current, when the
    /// floor is within the staleness bound of the transaction.
    pub(crate) fn point(self) -> Option<FloorPoint> {
        self.0
    }
}

/// The monotonic floor of a stale view, as one stale read-only transaction
/// captured it within its staleness bound (ADR-103). A read at this point uses
/// the state that was current at it, when the decoded cache has one.
///
/// Do not convert it to a barrier or a requirement.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FloorPoint(SequencePoint);

impl FloorPoint {
    pub(crate) fn point(self) -> SequencePoint {
        self.0
    }
}

/// The monotonic floors of the live stale views of one database instance.
/// When a path that a view read changes, the decoded cache keeps only the
/// earlier states that these floors can use (ADR-103).
#[derive(Debug, Clone, Default)]
pub struct ViewFloors(Arc<Mutex<Vec<Weak<AtomicU64>>>>);

impl ViewFloors {
    /// Creates the floor of a new stale view. It admits every cut point.
    pub fn new_floor(&self) -> MonotonicFloor {
        let floor = MonotonicFloor(Arc::default());
        let mut floors = self.0.lock().unwrap();
        floors.retain(|floor| floor.strong_count() > 0);
        floors.push(Arc::downgrade(&floor.0));
        floor
    }

    /// The distinct floors that calls of the live views raised, in ascending
    /// order.
    pub(crate) fn raised(&self) -> Vec<SequencePoint> {
        let mut floors = self.0.lock().unwrap();
        floors.retain(|floor| floor.strong_count() > 0);
        let mut raised: Vec<_> = floors
            .iter()
            .filter_map(Weak::upgrade)
            .map(|floor| floor.load(Ordering::SeqCst))
            .filter(|point| *point != 0)
            .map(SequencePoint::from_raw)
            .collect();
        raised.sort_unstable();
        raised.dedup();
        raised
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::timeline::Timeline;

    #[test]
    fn a_floor_gives_a_read_point_only_within_the_cutoff() {
        let timeline = Timeline::new();
        let floor = ViewFloors::default().new_floor();
        assert!(floor.view_read(StalenessCutoff::ANY).point().is_none());
        floor.raise(timeline.now());
        assert!(floor.view_read(StalenessCutoff::ANY).point().is_some());
        let zero = timeline.staleness_cutoff(Duration::ZERO);
        assert!(floor.view_read(zero).point().is_none());
    }

    #[test]
    fn view_floors_list_the_raised_floors_of_the_live_views() {
        let views = ViewFloors::default();
        let (low, high, _unraised) = (views.new_floor(), views.new_floor(), views.new_floor());
        assert!(views.raised().is_empty());
        high.raise(SequencePoint::from_raw(9));
        low.raise(SequencePoint::from_raw(4));
        let same = high.clone();
        same.raise(SequencePoint::from_raw(9));
        assert_eq!(
            views.raised(),
            [SequencePoint::from_raw(4), SequencePoint::from_raw(9)]
        );
        drop(low);
        assert_eq!(views.raised(), [SequencePoint::from_raw(9)]);
    }
}
