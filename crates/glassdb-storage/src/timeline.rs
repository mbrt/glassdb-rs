//! Database-local logical time for ordering cache validation evidence.
//!
//! Logical time is monotonic within one open database, but it is neither wall
//! time nor a durable timestamp. A [`Timeline`] is shared by the decoded object
//! cache and every higher-level component that captures validation barriers.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use glassdb_concurr::rt;

/// A point on one database-local [`Timeline`].
///
/// Logical times cannot be persisted or exchanged between database instances
/// or processes. Mixing values from different timelines is invalid; this is a
/// documented boundary rather than a dynamically checked one.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct LogicalTime(u64);

impl LogicalTime {
    pub(crate) fn from_raw(value: u64) -> Self {
        LogicalTime(value)
    }

    pub(crate) fn raw(self) -> u64 {
        self.0
    }

    pub(crate) fn saturating_sub(self, duration: Duration) -> Self {
        LogicalTime(self.0.saturating_sub(duration_to_nanos(duration)))
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

/// Allocates logical times shared by one open database.
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

    /// Returns a barrier satisfied by every operation started from now on and
    /// not by any operation that has already completed.
    pub fn now(&self) -> LogicalTime {
        LogicalTime(
            duration_to_nanos(self.0.source.elapsed())
                .max(self.0.last.load(Ordering::SeqCst).saturating_add(1)),
        )
    }

    /// Allocates the unique logical start time for a backend operation.
    pub(crate) fn tick(&self) -> LogicalTime {
        let elapsed = duration_to_nanos(self.0.source.elapsed());
        let mut current = self.0.last.load(Ordering::SeqCst);
        loop {
            let next = elapsed.max(current.saturating_add(1));
            match self
                .0
                .last
                .compare_exchange(current, next, Ordering::SeqCst, Ordering::SeqCst)
            {
                Ok(_) => return LogicalTime(next),
                Err(actual) => current = actual,
            }
        }
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
