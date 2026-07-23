use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};

/// Cumulative statistics for the decoded L1 and persistent encoded-body L2.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct CacheStats {
    /// Decoded L1 reads served locally.
    pub l1_hits: u64,
    /// Decoded L1 reads that needed a lower tier.
    pub l1_misses: u64,
    /// Usable encoded L2 records found.
    pub l2_hits: u64,
    /// Encoded L2 lookups without a usable record.
    pub l2_misses: u64,
    /// Bytes read from the L2 container.
    pub l2_bytes_read: u64,
    /// Bytes written to the L2 container.
    pub l2_bytes_written: u64,
    /// Backend conditional reads seeded by unverified L2 bodies.
    pub l2_conditional_validations: u64,
    /// L2 initialization, runtime, or corruption errors.
    pub l2_errors: u64,
}

impl AddAssign for CacheStats {
    fn add_assign(&mut self, rhs: Self) {
        self.l1_hits += rhs.l1_hits;
        self.l1_misses += rhs.l1_misses;
        self.l2_hits += rhs.l2_hits;
        self.l2_misses += rhs.l2_misses;
        self.l2_bytes_read += rhs.l2_bytes_read;
        self.l2_bytes_written += rhs.l2_bytes_written;
        self.l2_conditional_validations += rhs.l2_conditional_validations;
        self.l2_errors += rhs.l2_errors;
    }
}

impl Sub for CacheStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            l1_hits: self.l1_hits.saturating_sub(rhs.l1_hits),
            l1_misses: self.l1_misses.saturating_sub(rhs.l1_misses),
            l2_hits: self.l2_hits.saturating_sub(rhs.l2_hits),
            l2_misses: self.l2_misses.saturating_sub(rhs.l2_misses),
            l2_bytes_read: self.l2_bytes_read.saturating_sub(rhs.l2_bytes_read),
            l2_bytes_written: self.l2_bytes_written.saturating_sub(rhs.l2_bytes_written),
            l2_conditional_validations: self
                .l2_conditional_validations
                .saturating_sub(rhs.l2_conditional_validations),
            l2_errors: self.l2_errors.saturating_sub(rhs.l2_errors),
        }
    }
}

pub(crate) struct CacheMetrics {
    l1_hits: AtomicU64,
    l1_misses: AtomicU64,
    l2_hits: AtomicU64,
    l2_misses: AtomicU64,
    l2_bytes_read: AtomicU64,
    l2_bytes_written: AtomicU64,
    l2_conditional_validations: AtomicU64,
    l2_errors: AtomicU64,
}

impl CacheMetrics {
    pub(crate) fn new() -> Self {
        Self {
            l1_hits: AtomicU64::new(0),
            l1_misses: AtomicU64::new(0),
            l2_hits: AtomicU64::new(0),
            l2_misses: AtomicU64::new(0),
            l2_bytes_read: AtomicU64::new(0),
            l2_bytes_written: AtomicU64::new(0),
            l2_conditional_validations: AtomicU64::new(0),
            l2_errors: AtomicU64::new(0),
        }
    }

    pub(crate) fn l1_hit(&self) {
        self.l1_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn l1_miss(&self) {
        self.l1_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn l2_hit(&self) {
        self.l2_hits.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn l2_miss(&self) {
        self.l2_misses.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn l2_read(&self, bytes: usize) {
        self.l2_bytes_read
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn l2_write(&self, bytes: usize) {
        self.l2_bytes_written
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    pub(crate) fn l2_conditional_validation(&self) {
        self.l2_conditional_validations
            .fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn l2_error(&self) {
        self.l2_errors.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot_and_reset(&self) -> CacheStats {
        macro_rules! take {
            ($field:ident) => {
                self.$field.swap(0, Ordering::Relaxed)
            };
        }
        CacheStats {
            l1_hits: take!(l1_hits),
            l1_misses: take!(l1_misses),
            l2_hits: take!(l2_hits),
            l2_misses: take!(l2_misses),
            l2_bytes_read: take!(l2_bytes_read),
            l2_bytes_written: take!(l2_bytes_written),
            l2_conditional_validations: take!(l2_conditional_validations),
            l2_errors: take!(l2_errors),
        }
    }
}
