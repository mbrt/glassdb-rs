//! Database-local sequence points for ordering cache currentness evidence.
//!
//! Sequence points are strictly ordered within one open database, but they are
//! neither wall time nor durable timestamps. A [`Timeline`] is shared by the
//! decoded object cache and every higher-level component that captures
//! currentness barriers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use glassdb_concurr::rt;

/// A point on one database-local [`Timeline`].
///
/// Sequence points cannot be persisted or exchanged between database instances
/// or processes. Mixing values from different timelines is invalid; this is a
/// documented boundary rather than a dynamically checked one.
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
    last: AtomicU64,
}

/// Allocates sequence points shared by one open database.
#[derive(Clone)]
pub struct Timeline(Arc<Inner>);

impl Timeline {
    /// Creates a timeline backed by the active runtime's monotonic clock.
    pub fn new() -> Self {
        Self::with_source(Arc::new(RuntimeSource::new()))
    }

    pub(crate) fn with_source(source: Arc<dyn TimeSource>) -> Self {
        Timeline(Arc::new(Inner {
            source,
            last: AtomicU64::new(0),
        }))
    }

    /// Allocates a barrier satisfied by every operation invoked afterward and
    /// not by any operation that definitively completed beforehand.
    pub fn now(&self) -> SequencePoint {
        let elapsed = duration_to_nanos(self.0.source.elapsed());
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

    /// Derives the approximate sequence cutoff used only by bounded-staleness
    /// reads.
    pub(crate) fn approximate_cutoff(&self, max_staleness: Duration) -> SequencePoint {
        SequencePoint(
            self.now()
                .raw()
                .saturating_sub(duration_to_nanos(max_staleness)),
        )
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
}
