//! Cumulative performance counters for a database. Ported from the Go
//! `stats.go` (the backend counting is provided by `glassdb_backend`'s
//! `StatsBackend`).

use std::ops::Sub;
use std::time::Duration;

use glassdb_backend::BackendStats;

/// Holds cumulative performance counters for a database.
///
/// Counters only increase over time and are never reset. Subtract snapshots to
/// measure a specific interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Number of completed transactions.
    pub tx_n: u64,
    /// Time spent within transactions.
    pub tx_time: Duration,
    /// Number of reads.
    pub tx_reads: u64,
    /// Number of distinct transactional reads derived entirely from local objects.
    /// Counted once per key per transaction attempt, including cached
    /// not-found results.
    pub tx_cache_hits: u64,
    /// Number of writes.
    pub tx_writes: u64,
    /// Number of retried transactions.
    pub tx_retries: u64,

    /// Number of object reads.
    pub obj_reads: u64,
    /// Number of object writes.
    pub obj_writes: u64,
    /// Number of list calls.
    pub obj_lists: u64,

    /// Number of lock-acquisition calls made by the distributed locker.
    pub lock_calls: u64,
    /// Number of shard-coordinator inner CAS retries performed under contention.
    pub lock_retries: u64,

    /// Number of mutation requests submitted to the shard coordinator.
    pub coord_submissions: u64,
    /// Number of deduplicated shard-coordinator worker rounds started.
    pub coord_rounds: u64,

    /// Number of deduplicated split candidates processed in the background.
    pub split_candidates: u64,
    /// Number of locally observed split source/root linearizations.
    pub split_completed: u64,
    /// Number of retryable split candidates requeued for a later sweep.
    pub split_deferred: u64,
}

impl Stats {
    pub(crate) fn add(&mut self, other: &Stats) {
        self.tx_n += other.tx_n;
        self.tx_time += other.tx_time;
        self.tx_reads += other.tx_reads;
        self.tx_cache_hits += other.tx_cache_hits;
        self.tx_writes += other.tx_writes;
        self.tx_retries += other.tx_retries;
        self.obj_reads += other.obj_reads;
        self.obj_writes += other.obj_writes;
        self.obj_lists += other.obj_lists;
        self.lock_calls += other.lock_calls;
        self.lock_retries += other.lock_retries;
        self.coord_submissions += other.coord_submissions;
        self.coord_rounds += other.coord_rounds;
        self.split_candidates += other.split_candidates;
        self.split_completed += other.split_completed;
        self.split_deferred += other.split_deferred;
    }

    pub(crate) fn add_backend(&mut self, b: &BackendStats) {
        self.obj_reads += b.obj_reads;
        self.obj_writes += b.obj_writes;
        self.obj_lists += b.obj_lists;
    }

    pub(crate) fn add_protocol(
        &mut self,
        lock_calls: u64,
        coord: glassdb_trans::ShardCoordinatorStats,
        split: glassdb_trans::SplitterStats,
    ) {
        self.lock_calls += lock_calls;
        self.lock_retries += coord.cas_retries;
        self.coord_submissions += coord.submissions;
        self.coord_rounds += coord.rounds;
        self.split_candidates += split.candidates;
        self.split_completed += split.completed;
        self.split_deferred += split.deferred;
    }
}

impl Sub for Stats {
    type Output = Stats;

    fn sub(self, other: Stats) -> Stats {
        Stats {
            tx_n: self.tx_n.saturating_sub(other.tx_n),
            tx_time: self.tx_time.saturating_sub(other.tx_time),
            tx_reads: self.tx_reads.saturating_sub(other.tx_reads),
            tx_cache_hits: self.tx_cache_hits.saturating_sub(other.tx_cache_hits),
            tx_writes: self.tx_writes.saturating_sub(other.tx_writes),
            tx_retries: self.tx_retries.saturating_sub(other.tx_retries),
            obj_reads: self.obj_reads.saturating_sub(other.obj_reads),
            obj_writes: self.obj_writes.saturating_sub(other.obj_writes),
            obj_lists: self.obj_lists.saturating_sub(other.obj_lists),
            lock_calls: self.lock_calls.saturating_sub(other.lock_calls),
            lock_retries: self.lock_retries.saturating_sub(other.lock_retries),
            coord_submissions: self
                .coord_submissions
                .saturating_sub(other.coord_submissions),
            coord_rounds: self.coord_rounds.saturating_sub(other.coord_rounds),
            split_candidates: self.split_candidates.saturating_sub(other.split_candidates),
            split_completed: self.split_completed.saturating_sub(other.split_completed),
            split_deferred: self.split_deferred.saturating_sub(other.split_deferred),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subtraction_covers_protocol_counters() {
        let before = Stats {
            coord_submissions: 10,
            coord_rounds: 8,
            split_candidates: 3,
            split_completed: 2,
            split_deferred: 1,
            ..Default::default()
        };
        let after = Stats {
            coord_submissions: 14,
            coord_rounds: 10,
            split_candidates: 5,
            split_completed: 3,
            split_deferred: 1,
            ..Default::default()
        };
        let delta = after - before;
        assert_eq!(delta.coord_submissions, 4);
        assert_eq!(delta.coord_rounds, 2);
        assert_eq!(delta.split_candidates, 2);
        assert_eq!(delta.split_completed, 1);
        assert_eq!(delta.split_deferred, 0);
    }
}
