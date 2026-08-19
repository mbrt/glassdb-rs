//! Cumulative performance counters for a database. Ported from the Go
//! `stats.go` (the backend counting is provided by `glassdb_backend`'s
//! `StatsBackend`).

use std::ops::{AddAssign, Sub};
use std::time::Duration;

use glassdb_backend::BackendStats;
use glassdb_storage::CacheStats;
use glassdb_trans::{DirectCommitStats, LockerStats, ShardCoordinatorStats, SplitterStats};

/// Transaction activity for one snapshot or accumulated interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransactionStats {
    /// Number of completed transactions.
    pub completed: u64,
    /// Time spent within transactions.
    pub elapsed: Duration,
    /// Number of reads.
    pub reads: u64,
    /// Number of distinct transactional reads derived entirely from local objects.
    /// Counted once per key per transaction attempt, including cached
    /// not-found results.
    pub cache_hits: u64,
    /// Number of writes.
    pub writes: u64,
    /// Number of retried transactions.
    pub retries: u64,
}

impl AddAssign for TransactionStats {
    fn add_assign(&mut self, rhs: Self) {
        self.completed += rhs.completed;
        self.elapsed += rhs.elapsed;
        self.reads += rhs.reads;
        self.cache_hits += rhs.cache_hits;
        self.writes += rhs.writes;
        self.retries += rhs.retries;
    }
}

impl Sub for TransactionStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            completed: self.completed.saturating_sub(rhs.completed),
            elapsed: self.elapsed.saturating_sub(rhs.elapsed),
            reads: self.reads.saturating_sub(rhs.reads),
            cache_hits: self.cache_hits.saturating_sub(rhs.cache_hits),
            writes: self.writes.saturating_sub(rhs.writes),
            retries: self.retries.saturating_sub(rhs.retries),
        }
    }
}

/// Holds cumulative performance counters for a database.
///
/// Counters only increase over time and are never reset. Subtract snapshots to
/// measure a specific interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Transaction execution activity.
    pub transactions: TransactionStats,
    /// Backend object operations.
    pub backend: BackendStats,
    /// Decoded L1 and persistent encoded-body L2 cache activity.
    pub cache: CacheStats,
    /// Distributed-locker activity.
    pub locker: LockerStats,
    /// Shared shard-coordinator activity.
    pub coordinator: ShardCoordinatorStats,
    /// Logless direct-commit coverage.
    pub direct_commit: DirectCommitStats,
    /// Background tree-split activity.
    pub splitter: SplitterStats,
}

impl AddAssign for Stats {
    fn add_assign(&mut self, rhs: Self) {
        self.transactions += rhs.transactions;
        self.backend += rhs.backend;
        self.cache += rhs.cache;
        self.locker += rhs.locker;
        self.coordinator += rhs.coordinator;
        self.direct_commit += rhs.direct_commit;
        self.splitter += rhs.splitter;
    }
}

impl Sub for Stats {
    type Output = Stats;

    fn sub(self, other: Stats) -> Stats {
        Stats {
            transactions: self.transactions - other.transactions,
            backend: self.backend - other.backend,
            cache: self.cache - other.cache,
            locker: self.locker - other.locker,
            coordinator: self.coordinator - other.coordinator,
            direct_commit: self.direct_commit - other.direct_commit,
            splitter: self.splitter - other.splitter,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_trans::InlinePressureStats;

    #[test]
    fn subtraction_covers_component_snapshots() {
        let before = Stats {
            transactions: TransactionStats {
                completed: 2,
                elapsed: Duration::from_secs(3),
                reads: 7,
                cache_hits: 4,
                writes: 5,
                retries: 1,
            },
            backend: BackendStats {
                obj_reads: 12,
                obj_writes: 8,
                obj_lists: 3,
            },
            cache: CacheStats {
                l1_hits: 4,
                l2_hits: 2,
                ..Default::default()
            },
            locker: LockerStats { calls: 6 },
            coordinator: ShardCoordinatorStats {
                submissions: 10,
                rounds: 8,
                cas_retries: 2,
            },
            direct_commit: DirectCommitStats {
                candidates: 7,
                landed: 5,
            },
            splitter: SplitterStats {
                candidates: 3,
                completed: 2,
                deferred: 1,
                tombstones_reclaimed: 4,
                splits_avoided: 1,
                inline_pressure: InlinePressureStats {
                    candidates: 2,
                    completed: 1,
                    deferred: 1,
                    discarded: 0,
                },
            },
        };
        let after = Stats {
            transactions: TransactionStats {
                completed: 5,
                elapsed: Duration::from_secs(8),
                reads: 18,
                cache_hits: 10,
                writes: 9,
                retries: 3,
            },
            backend: BackendStats {
                obj_reads: 19,
                obj_writes: 11,
                obj_lists: 5,
            },
            cache: CacheStats {
                l1_hits: 9,
                l2_hits: 3,
                ..Default::default()
            },
            locker: LockerStats { calls: 10 },
            coordinator: ShardCoordinatorStats {
                submissions: 14,
                rounds: 10,
                cas_retries: 3,
            },
            direct_commit: DirectCommitStats {
                candidates: 11,
                landed: 8,
            },
            splitter: SplitterStats {
                candidates: 5,
                completed: 3,
                deferred: 1,
                tombstones_reclaimed: 9,
                splits_avoided: 3,
                inline_pressure: InlinePressureStats {
                    candidates: 5,
                    completed: 2,
                    deferred: 1,
                    discarded: 1,
                },
            },
        };
        assert_eq!(
            after - before,
            Stats {
                transactions: TransactionStats {
                    completed: 3,
                    elapsed: Duration::from_secs(5),
                    reads: 11,
                    cache_hits: 6,
                    writes: 4,
                    retries: 2,
                },
                backend: BackendStats {
                    obj_reads: 7,
                    obj_writes: 3,
                    obj_lists: 2,
                },
                cache: CacheStats {
                    l1_hits: 5,
                    l2_hits: 1,
                    ..Default::default()
                },
                locker: LockerStats { calls: 4 },
                coordinator: ShardCoordinatorStats {
                    submissions: 4,
                    rounds: 2,
                    cas_retries: 1,
                },
                direct_commit: DirectCommitStats {
                    candidates: 4,
                    landed: 3,
                },
                splitter: SplitterStats {
                    candidates: 2,
                    completed: 1,
                    deferred: 0,
                    tombstones_reclaimed: 5,
                    splits_avoided: 2,
                    inline_pressure: InlinePressureStats {
                        candidates: 3,
                        completed: 1,
                        deferred: 0,
                        discarded: 1,
                    },
                },
            }
        );
    }
}
