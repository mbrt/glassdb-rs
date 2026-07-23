//! Database-local sequence points for ordering cache currentness evidence.
//!
//! Sequence points are strictly ordered within one open database, but they are
//! neither wall time nor portable timestamps. A [`Timeline`] is shared by the
//! decoded object cache and every higher-level component that captures
//! currentness barriers. A persistent cache may order a new timeline after
//! evidence from the previous open.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use glassdb_concurr::rt;

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

    /// Allocates a barrier satisfied by every operation invoked afterward and
    /// not by any operation that definitively completed beforehand.
    pub fn now(&self) -> SequencePoint {
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

    /// Creates a timeline whose first allocation follows the last recovered
    /// sequence point, or starts a fresh timeline when it is `None`.
    pub fn starting_after(previous: Option<SequencePoint>) -> Self {
        Self::from_source(Arc::new(RuntimeSource::new()), previous)
    }

    #[cfg(test)]
    pub(crate) fn with_source(source: Arc<dyn TimeSource>) -> Self {
        Self::from_source(source, None)
    }

    /// Derives the approximate sequence cutoff used only by bounded-staleness
    /// reads.
    pub(crate) fn approximate_cutoff(&self, max_staleness: Duration) -> SequencePoint {
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
}
