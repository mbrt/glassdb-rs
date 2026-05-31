//! Cumulative performance counters for a database. Ported from the Go
//! `stats.go` (the backend counting is provided by `glassdb_backend`'s
//! `StatsBackend`).

use std::time::Duration;

use glassdb_backend::BackendStats;

/// Holds cumulative performance counters for a database.
///
/// Counters only increase over time and are never reset. Use [`Stats::sub`] to
/// measure a specific interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Stats {
    /// Number of completed transactions.
    pub tx_n: i64,
    /// Time spent within transactions.
    pub tx_time: Duration,
    /// Number of reads.
    pub tx_reads: i64,
    /// Number of writes.
    pub tx_writes: i64,
    /// Number of retried transactions.
    pub tx_retries: i64,

    /// Number of metadata reads.
    pub meta_reads: i64,
    /// Number of metadata writes.
    pub meta_writes: i64,
    /// Number of object reads.
    pub obj_reads: i64,
    /// Number of object writes.
    pub obj_writes: i64,
    /// Number of list calls.
    pub obj_lists: i64,
}

impl Stats {
    /// Returns the difference between two snapshots (`self - other`), useful to
    /// measure the counters accumulated within a time span.
    pub fn sub(&self, other: &Stats) -> Stats {
        Stats {
            tx_n: self.tx_n - other.tx_n,
            tx_time: self.tx_time.saturating_sub(other.tx_time),
            tx_reads: self.tx_reads - other.tx_reads,
            tx_writes: self.tx_writes - other.tx_writes,
            tx_retries: self.tx_retries - other.tx_retries,
            meta_reads: self.meta_reads - other.meta_reads,
            meta_writes: self.meta_writes - other.meta_writes,
            obj_reads: self.obj_reads - other.obj_reads,
            obj_writes: self.obj_writes - other.obj_writes,
            obj_lists: self.obj_lists - other.obj_lists,
        }
    }

    pub(crate) fn add(&mut self, other: &Stats) {
        self.tx_n += other.tx_n;
        self.tx_time += other.tx_time;
        self.tx_reads += other.tx_reads;
        self.tx_writes += other.tx_writes;
        self.tx_retries += other.tx_retries;
        self.meta_reads += other.meta_reads;
        self.meta_writes += other.meta_writes;
        self.obj_reads += other.obj_reads;
        self.obj_writes += other.obj_writes;
        self.obj_lists += other.obj_lists;
    }

    pub(crate) fn add_backend(&mut self, b: &BackendStats) {
        self.meta_reads += b.meta_reads;
        self.meta_writes += b.meta_writes;
        self.obj_reads += b.obj_reads;
        self.obj_writes += b.obj_writes;
        self.obj_lists += b.obj_lists;
    }
}
