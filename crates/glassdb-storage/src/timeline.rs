//! Database-local sequence points for ordering cache currentness evidence.
//!
//! Sequence points are strictly ordered within one open database, but they are
//! neither wall time nor portable timestamps. A [`Timeline`] is shared by the
//! decoded object cache and every higher-level component that captures
//! currentness barriers. A persistent cache may order a new timeline after
//! evidence from the previous open.
//!
//! Changes to this module require the review in
//! `docs/guides/storage-consistency.md` (E1, E2).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use glassdb_concurr::rt;

use crate::stale_view::MonotonicFloor;

/// A point on one database-local [`Timeline`].
///
/// Sequence points may be persisted only to chain cache evidence across opens
/// of the same database identity. They cannot otherwise be exchanged between
/// database instances or processes. Mixing values from independent timelines
/// is invalid; this is a documented boundary rather than a dynamically checked
/// one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SequencePoint(u64);

impl SequencePoint {
    pub(crate) fn from_raw(value: u64) -> Self {
        SequencePoint(value)
    }

    pub(crate) fn raw(self) -> u64 {
        self.0
    }
}

/// A database-local bound separating completed work from subsequent operations.
///
/// Capture it after prerequisite work completes and before the operations used
/// as dependent currentness evidence. All dependencies must reach that barrier.
/// Use it only with the database instance that captured it. It does not order
/// the contents returned by overlapping operations.
///
/// A transaction's validation barrier is captured after its body and before
/// the key and predicate lock CASes used as validation evidence.
///
/// Only the timeline may construct a barrier. Do not add constructors from
/// sequence points, observations, receipts, or freshness requirements, nor
/// a default value, public sequence-point accessors, implicit conversions, or
/// serialization.
/// Its point remains private even from other storage modules; those
/// modules can test evidence without extracting the bound.
/// Such conversions would let an operation's invocation point stand in for
/// a barrier captured after completed work. The explicit conversion to
/// `Requirement::after` retains the opaque barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CurrentnessBarrier {
    point: SequencePoint,
}

impl CurrentnessBarrier {
    pub(crate) fn is_reached_by(self, point: SequencePoint) -> bool {
        point >= self.point
    }
}

/// The freshness requirement a cached entry must satisfy before it is served.
///
/// Construct requirements explicitly with `ANY` or `after`. Do not
/// add constructors from observations, receipts, or raw sequence points, nor
/// expose the stored bound. An invocation point cannot establish a barrier
/// after completed work. Requirements cannot be converted back into barriers.
/// This also applies to crate-visible accessors: a requirement states the
/// evidence needed, but cannot supply evidence to advance an observation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Requirement(Option<SequencePoint>);

impl Requirement {
    /// Accepts any usable cached entry without a backend check.
    pub const ANY: Self = Self(None);

    /// Requires evidence that reaches the captured currentness barrier.
    pub fn after(barrier: CurrentnessBarrier) -> Self {
        Self(Some(barrier.point))
    }

    /// Returns the stronger of two requirements.
    pub fn stricter(self, other: Self) -> Self {
        Self(self.0.max(other.0))
    }

    pub(crate) fn is_satisfied_by(self, current_after: SequencePoint) -> bool {
        self.0.is_none_or(|bound| current_after >= bound)
    }
}

/// The oldest cut point that a read-only transaction accepts (ADR-097,
/// ADR-099).
///
/// The transaction captures it when it starts, so its staleness bound is
/// measured from the start of the call. A transaction that is not stale has a
/// zero bound. It converts a duration to a sequence
/// point by approximation, so it is not a currentness barrier.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct StalenessCutoff(SequencePoint);

impl StalenessCutoff {
    /// Accepts a cut point of any age, also from a previous open.
    pub const ANY: Self = Self(SequencePoint(0));

    pub(crate) fn point(self) -> SequencePoint {
        self.0
    }
}

// The barrier point stays private to this module, so the floor of a stale view
// rises to a barrier here.
impl MonotonicFloor {
    /// Raises the floor to a barrier captured after completed work.
    pub fn raise_to_barrier(&self, barrier: CurrentnessBarrier) {
        self.raise(barrier.point);
    }
}

pub(crate) trait TimeSource: Send + Sync {
    fn elapsed(&self) -> Duration;
}

struct RuntimeSource {
    origin: rt::Instant,
}

impl RuntimeSource {
    fn new() -> Self {
        Self {
            origin: rt::Instant::now(),
        }
    }
}

impl TimeSource for RuntimeSource {
    fn elapsed(&self) -> Duration {
        self.origin.elapsed()
    }
}

struct Inner {
    source: Arc<dyn TimeSource>,
    base: u64,
    last: AtomicU64,
}

/// Allocates sequence points shared by one open database.
#[derive(Clone)]
pub struct Timeline(Arc<Inner>);

impl Timeline {
    /// Creates a timeline backed by the active runtime's monotonic clock.
    pub fn new() -> Self {
        Self::from_source(Arc::new(RuntimeSource::new()), None)
    }

    /// Captures a currentness barrier after completed prerequisite work.
    pub fn currentness_barrier(&self) -> CurrentnessBarrier {
        CurrentnessBarrier { point: self.now() }
    }

    /// Creates a timeline whose first allocation follows the last recovered
    /// sequence point, or starts a fresh timeline when it is `None`.
    pub fn starting_after(previous: Option<SequencePoint>) -> Self {
        Self::from_source(Arc::new(RuntimeSource::new()), previous)
    }

    /// Allocates the next local sequence point.
    pub(crate) fn now(&self) -> SequencePoint {
        let elapsed = self
            .0
            .base
            .saturating_add(duration_to_nanos(self.0.source.elapsed()));
        let mut current = self.0.last.load(Ordering::SeqCst);
        loop {
            let next = elapsed.max(current.saturating_add(1));
            match self
                .0
                .last
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return SequencePoint(next),
                Err(actual) => current = actual,
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn with_source(source: Arc<dyn TimeSource>) -> Self {
        Self::from_source(source, None)
    }

    /// Captures the staleness cutoff of a transaction that starts now and
    /// accepts a cut point no older than `max_staleness`.
    ///
    /// A zero bound subtracts nothing, so its cutoff is a point allocated
    /// now, as a currentness barrier is. Every transaction that is not stale,
    /// and a stale read-only transaction with a zero bound, rely on this to
    /// follow every operation that completed before they start (ADR-099).
    pub fn staleness_cutoff(&self, max_staleness: Duration) -> StalenessCutoff {
        if max_staleness == Duration::MAX {
            StalenessCutoff::ANY
        } else {
            StalenessCutoff(self.approximate_cutoff(max_staleness))
        }
    }

    /// Derives the approximate sequence cutoff used only to bound the age of
    /// a cut point (ADR-097).
    fn approximate_cutoff(&self, max_staleness: Duration) -> SequencePoint {
        SequencePoint(
            self.now()
                .raw()
                .saturating_sub(duration_to_nanos(max_staleness))
                .max(self.0.base),
        )
    }

    fn from_source(source: Arc<dyn TimeSource>, previous: Option<SequencePoint>) -> Self {
        let base = previous
            .map(|previous| {
                previous
                    .raw()
                    .checked_add(1)
                    .expect("persistent cache rejects exhausted sequence points")
            })
            .unwrap_or(0);
        Timeline(Arc::new(Inner {
            source,
            base,
            last: AtomicU64::new(base.saturating_sub(1)),
        }))
    }
}

impl Default for Timeline {
    fn default() -> Self {
        Self::new()
    }
}

fn duration_to_nanos(duration: Duration) -> u64 {
    duration.as_nanos().min(u128::from(u64::MAX)) as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FixedSource(Duration);

    impl TimeSource for FixedSource {
        fn elapsed(&self) -> Duration {
            self.0
        }
    }

    #[test]
    fn allocations_are_strictly_ordered_at_one_instant() {
        let timeline = Timeline::with_source(Arc::new(FixedSource(Duration::ZERO)));
        let first = timeline.now();
        let second = timeline.now();
        let third = timeline.clone().now();

        assert!(first < second);
        assert!(second < third);
    }

    #[test]
    fn a_zero_staleness_cutoff_follows_every_earlier_point() {
        let timeline = Timeline::with_source(Arc::new(FixedSource(Duration::from_secs(2))));
        let earlier = timeline.now();
        assert!(timeline.staleness_cutoff(Duration::ZERO).point() > earlier);
    }

    #[test]
    fn elapsed_time_is_a_floor_for_sequence_points() {
        let timeline = Timeline::with_source(Arc::new(FixedSource(Duration::from_secs(2))));
        assert!(timeline.now().raw() >= 2_000_000_000);
    }

    #[test]
    fn persisted_evidence_starts_a_new_session_after_its_maximum() {
        let timeline = Timeline::from_source(
            Arc::new(FixedSource(Duration::ZERO)),
            Some(SequencePoint::from_raw(41)),
        );

        assert_eq!(timeline.now(), SequencePoint::from_raw(42));
    }

    #[test]
    fn persisted_base_preserves_elapsed_distance() {
        let timeline = Timeline::from_source(
            Arc::new(FixedSource(Duration::from_secs(3))),
            Some(SequencePoint::from_raw(41)),
        );

        assert_eq!(timeline.now(), SequencePoint::from_raw(3_000_000_042));
    }

    #[test]
    fn bounded_staleness_does_not_cross_a_persisted_session_boundary() {
        let timeline = Timeline::from_source(
            Arc::new(FixedSource(Duration::from_secs(10))),
            Some(SequencePoint::from_raw(20_000_000_000)),
        );

        assert_eq!(
            timeline.approximate_cutoff(Duration::from_secs(60)),
            SequencePoint::from_raw(20_000_000_001)
        );
    }

    #[test]
    fn requirement_satisfaction_follows_currentness_evidence() {
        let earlier = SequencePoint::from_raw(1);
        let later = SequencePoint::from_raw(2);

        assert!(Requirement::ANY.is_satisfied_by(earlier));
        assert!(Requirement(Some(earlier)).is_satisfied_by(earlier));
        assert!(Requirement(Some(earlier)).is_satisfied_by(later));
        assert!(!Requirement(Some(later)).is_satisfied_by(earlier));
    }
}
