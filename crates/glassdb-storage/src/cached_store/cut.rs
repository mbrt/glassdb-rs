//! Cut-point search over the currentness intervals of retained observations
//! (ADR-097).
//!
//! Changes to this module require the review in
//! `docs/guides/storage-consistency.md` (E5).

use std::collections::BTreeMap;
use std::time::Duration;

use glassdb_data::ObjectPath;

use super::Observation;
use crate::timeline::{CurrentnessBarrier, MonotonicFloor, SequencePoint, Timeline};

/// The currentness intervals of the states that one stale read-only
/// transaction depends on.
///
/// Add the observations of each typed store, then [`plan`](Self::plan) a cut.
pub struct CutSearch {
    timeline: Timeline,
    latest_start: SequencePoint,
    states: usize,
    unknown_start: bool,
}

/// A planned cut point and the barrier for the checks that reach it.
///
/// A state whose watermark is before the target needs a check after the
/// barrier. When every state reaches the target, the target is a cut point.
#[derive(Debug, Clone, Copy)]
pub struct CutTarget {
    target: SequencePoint,
    barrier: CurrentnessBarrier,
    latest_start: SequencePoint,
}

impl CutSearch {
    pub(super) fn new(timeline: Timeline) -> Self {
        Self {
            timeline,
            latest_start: SequencePoint::default(),
            states: 0,
            unknown_start: false,
        }
    }

    /// Adds the distinct states among `observations`.
    pub(super) fn add<V>(&mut self, observations: &[Observation<V>]) {
        // Only observations of one path can be the same state.
        let mut by_path = BTreeMap::<&ObjectPath, Vec<&Observation<V>>>::new();
        for observation in observations {
            let states = by_path.entry(observation.path()).or_default();
            if states.iter().any(|state| state.same_state(observation)) {
                continue;
            }
            states.push(observation);
            self.states += 1;
            // An absence has no revision, so a later absence of the same path
            // shares its evidence even when the object existed in between. Its
            // span is not a currentness interval.
            match observation.evidence.start() {
                Some(start) if observation.exists() => {
                    self.latest_start = self.latest_start.max(start);
                }
                _ => self.unknown_start = true,
            }
        }
    }

    /// Plans a cut point no older than `max_staleness` and not before
    /// `floor`, or returns `None` when no state can supply one.
    ///
    /// Call it after the body completes: its barrier is also the validation
    /// barrier when the target does not become a cut point, so it must follow
    /// every read of the body.
    ///
    /// Several states need a point inside every interval, so the target is
    /// at or after their latest start. One state needs no interval: it was
    /// current when its read took effect, which is after its watermark.
    pub fn plan(&self, max_staleness: Duration, floor: &MonotonicFloor) -> Option<CutTarget> {
        if self.unknown_start {
            return None;
        }
        let mut bound = floor.get();
        if max_staleness != Duration::MAX {
            bound = bound.max(self.timeline.approximate_cutoff(max_staleness));
        }
        let target = if self.states > 1 {
            bound.max(self.latest_start)
        } else {
            bound
        };
        Some(CutTarget {
            target,
            barrier: self.timeline.currentness_barrier(),
            latest_start: self.latest_start,
        })
    }
}

impl CutTarget {
    /// The barrier that a check of a lagging state must reach. It follows
    /// the target, because the target is never in the future. It is captured
    /// after the body and before the checks, so it is also a validation
    /// barrier when the target does not become a cut point.
    pub fn barrier(&self) -> CurrentnessBarrier {
        self.barrier
    }

    /// Reports whether a state already covers the target without a check.
    pub(super) fn is_reached_by<V>(&self, observation: &Observation<V>) -> bool {
        observation.current_after() >= self.target
    }

    /// Raises `floor` after the target became a cut point, so that a later cut
    /// point follows every commit that the states show.
    pub fn raise_floor(&self, floor: &MonotonicFloor) {
        floor.raise(self.latest_start);
    }
}
