//! The retry schedules of one database instance.

use std::time::Duration;

use glassdb_concurr::RetrySchedule;

/// The retry schedules of one database instance, one for each cause of a
/// retry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RetryTiming {
    /// Retries of a CAS round on a leaf or a collection record that lost to
    /// the write of another transaction. The other writer is a short time
    /// ahead, so a short delay is enough. If the delay grows to seconds, a
    /// database instance that keeps winning can starve the others. Defaults to
    /// 20 ms first, and at most 300 ms.
    pub contention: RetrySchedule,
    /// Retries while a backend fails or throttles, or while other
    /// transactions must make progress first: unavailable reads, conditional
    /// writes with no known result, writes of transaction records, and new
    /// lock acquisitions after a leaf round used all of its CAS attempts or
    /// the leaf is full. Defaults to 200 ms first, and at most 5 s.
    pub wait: RetrySchedule,
    /// Waits for a lock holder: the polls of its transaction record, and the
    /// waits between two reads of the object that it holds (ADR-024). The
    /// backend is healthy, and a waiting transaction pays each delay, so the
    /// delay stays short. Defaults to 200 ms first, and at most 1 s.
    pub holder_poll: RetrySchedule,
}

impl Default for RetryTiming {
    fn default() -> Self {
        // The perfbench `split-merge-fight`, `mixed`, and `contention`
        // scenarios selected the contention schedule
        // (hack/perf/investigations.md, 2026-10-05).
        Self {
            contention: RetrySchedule {
                initial_interval: Duration::from_millis(20),
                max_interval: Duration::from_millis(300),
            },
            wait: RetrySchedule {
                initial_interval: Duration::from_millis(200),
                max_interval: Duration::from_secs(5),
            },
            holder_poll: RetrySchedule {
                initial_interval: Duration::from_millis(200),
                max_interval: Duration::from_secs(1),
            },
        }
    }
}

impl RetryTiming {
    /// Returns a timing that uses `schedule` for all retries.
    #[cfg(test)]
    pub(crate) fn uniform(schedule: RetrySchedule) -> Self {
        Self {
            contention: schedule,
            wait: schedule,
            holder_poll: schedule,
        }
    }
}
