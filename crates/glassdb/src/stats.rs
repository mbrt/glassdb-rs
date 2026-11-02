//! Cumulative performance counters for a database. Ported from the Go
//! `stats.go` (the backend counting is provided by `glassdb_backend`'s
//! `StatsBackend`).

use std::ops::{AddAssign, Sub};
use std::time::Duration;

use glassdb_backend::BackendStats;
use glassdb_storage::CacheStats;
use glassdb_trans::{
    CoordinatorStats, DirectCommitStats, GcStats, LockerStats, MonitorStats, TopologyStats,
};

/// Transaction activity for one snapshot or accumulated interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionStats {
    /// Number of completed transactions.
    pub completed: u64,
    /// Time spent within transactions.
    pub elapsed: Duration,
    /// Number of reads.
    pub reads: u64,
    /// Number of writes.
    pub writes: u64,
    /// Number of body replays.
    pub replays: u64,
}

impl AddAssign for TransactionStats {
    fn add_assign(&mut self, rhs: Self) {
        // Destructuring fails to compile when a field is added and not summed.
        let Self {
            completed,
            elapsed,
            reads,
            writes,
            replays,
        } = rhs;
        self.completed += completed;
        self.elapsed += elapsed;
        self.reads += reads;
        self.writes += writes;
        self.replays += replays;
    }
}

impl Sub for TransactionStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self {
            completed: self.completed.saturating_sub(rhs.completed),
            elapsed: self.elapsed.saturating_sub(rhs.elapsed),
            reads: self.reads.saturating_sub(rhs.reads),
            writes: self.writes.saturating_sub(rhs.writes),
            replays: self.replays.saturating_sub(rhs.replays),
        }
    }
}

/// Holds cumulative performance counters for a database.
///
/// Counters only increase over time and are never reset. Subtract snapshots to
/// measure a specific interval.
///
/// Each field is named after the component that counts. Inside a group, a
/// counter is a plural noun of what happened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Transaction execution activity.
    pub transactions: TransactionStats,
    /// Backend object operations.
    pub backend: BackendStats,
    /// Decoded L1 and persistent encoded-body L2 cache activity.
    pub cache: CacheStats,
    /// Transaction final-status cache activity.
    pub monitor: MonitorStats,
    /// Distributed-locker activity.
    pub locker: LockerStats,
    /// Shared leaf-coordinator activity.
    pub coordinator: CoordinatorStats,
    /// Direct-commit coverage.
    pub direct_commit: DirectCommitStats,
    /// Background split and merge activity.
    pub topology: TopologyStats,
    /// Garbage collection activity.
    pub gc: GcStats,
}

impl AddAssign for Stats {
    fn add_assign(&mut self, rhs: Self) {
        // Destructuring fails to compile when a field is added and not summed.
        let Self {
            transactions,
            backend,
            cache,
            monitor,
            locker,
            coordinator,
            direct_commit,
            topology,
            gc,
        } = rhs;
        self.transactions += transactions;
        self.backend += backend;
        self.cache += cache;
        self.monitor += monitor;
        self.locker += locker;
        self.coordinator += coordinator;
        self.direct_commit += direct_commit;
        self.topology += topology;
        self.gc += gc;
    }
}

impl Sub for Stats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self {
            transactions: self.transactions - rhs.transactions,
            backend: self.backend - rhs.backend,
            cache: self.cache - rhs.cache,
            monitor: self.monitor - rhs.monitor,
            locker: self.locker - rhs.locker,
            coordinator: self.coordinator - rhs.coordinator,
            direct_commit: self.direct_commit - rhs.direct_commit,
            topology: self.topology - rhs.topology,
            gc: self.gc - rhs.gc,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_trans::{MergeStats, SplitCauseStats, SplitStats};

    #[test]
    fn subtraction_and_addition_cover_nested_groups() {
        let before = Stats {
            transactions: TransactionStats {
                completed: 2,
                elapsed: Duration::from_secs(3),
                ..Default::default()
            },
            backend: BackendStats {
                reads: 12,
                writes: 8,
                lists: 3,
            },
            topology: TopologyStats {
                splits: SplitStats {
                    completed: 2,
                    inline_pressure: SplitCauseStats {
                        candidates: 2,
                        completed: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                merges: MergeStats {
                    kept_apart: 4,
                    ..Default::default()
                },
                tombstones_reclaimed: 4,
            },
            ..Default::default()
        };
        let after = Stats {
            transactions: TransactionStats {
                completed: 5,
                elapsed: Duration::from_secs(8),
                ..Default::default()
            },
            backend: BackendStats {
                reads: 19,
                writes: 11,
                lists: 5,
            },
            topology: TopologyStats {
                splits: SplitStats {
                    completed: 3,
                    inline_pressure: SplitCauseStats {
                        candidates: 5,
                        completed: 2,
                        discarded: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                merges: MergeStats {
                    kept_apart: 10,
                    ..Default::default()
                },
                tombstones_reclaimed: 9,
            },
            ..Default::default()
        };
        let delta = Stats {
            transactions: TransactionStats {
                completed: 3,
                elapsed: Duration::from_secs(5),
                ..Default::default()
            },
            backend: BackendStats {
                reads: 7,
                writes: 3,
                lists: 2,
            },
            topology: TopologyStats {
                splits: SplitStats {
                    completed: 1,
                    inline_pressure: SplitCauseStats {
                        candidates: 3,
                        completed: 1,
                        discarded: 1,
                        ..Default::default()
                    },
                    ..Default::default()
                },
                merges: MergeStats {
                    kept_apart: 6,
                    ..Default::default()
                },
                tombstones_reclaimed: 5,
            },
            ..Default::default()
        };

        assert_eq!(after - before, delta);
        let mut sum = before;
        sum += delta;
        assert_eq!(sum, after);
    }

    #[test]
    fn subtraction_saturates_at_zero() {
        let high = Stats {
            backend: BackendStats {
                reads: 2,
                ..Default::default()
            },
            ..Default::default()
        };

        assert_eq!(Stats::default() - high, Stats::default());
    }
}
