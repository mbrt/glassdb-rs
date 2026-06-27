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
}

impl Stats {
    pub(crate) fn add(&mut self, other: &Stats) {
        self.tx_n += other.tx_n;
        self.tx_time += other.tx_time;
        self.tx_reads += other.tx_reads;
        self.tx_writes += other.tx_writes;
        self.tx_retries += other.tx_retries;
        self.obj_reads += other.obj_reads;
        self.obj_writes += other.obj_writes;
        self.obj_lists += other.obj_lists;
    }

    pub(crate) fn add_backend(&mut self, b: &BackendStats) {
        self.obj_reads += b.obj_reads;
        self.obj_writes += b.obj_writes;
        self.obj_lists += b.obj_lists;
    }
}

impl Sub for Stats {
    type Output = Stats;

    fn sub(self, other: Stats) -> Stats {
        Stats {
            tx_n: self.tx_n.saturating_sub(other.tx_n),
            tx_time: self.tx_time.saturating_sub(other.tx_time),
            tx_reads: self.tx_reads.saturating_sub(other.tx_reads),
            tx_writes: self.tx_writes.saturating_sub(other.tx_writes),
            tx_retries: self.tx_retries.saturating_sub(other.tx_retries),
            obj_reads: self.obj_reads.saturating_sub(other.obj_reads),
            obj_writes: self.obj_writes.saturating_sub(other.obj_writes),
            obj_lists: self.obj_lists.saturating_sub(other.obj_lists),
        }
    }
}
