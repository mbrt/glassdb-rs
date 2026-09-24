//! Activity counters of the restructurer.

use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};

/// Live counters that the scheduler and every structural change update.
#[derive(Default)]
pub(super) struct Stats {
    pub(super) candidates: AtomicU64,
    pub(super) splits: AtomicU64,
    pub(super) deferred: AtomicU64,
    pub(super) tombstones_reclaimed: AtomicU64,
    pub(super) splits_avoided: AtomicU64,
    pub(super) merges: AtomicU64,
    pub(super) inline_pressure_candidates: AtomicU64,
    pub(super) inline_pressure_completed: AtomicU64,
    pub(super) inline_pressure_deferred: AtomicU64,
    pub(super) inline_pressure_discarded: AtomicU64,
}

/// Background split and merge activity for one snapshot or accumulated
/// interval.
///
/// `splits` counts locally observed source/root linearizations. A split may
/// also be `deferred` if a later publication or cleanup step needs another
/// sweep, so the fields are not mutually exclusive outcomes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RestructurerStats {
    /// Deduplicated candidates processed for any split or merge cause.
    pub candidates: u64,
    /// Locally observed source/root split linearizations for any cause.
    pub splits: u64,
    /// Retryable candidate attempts requeued for any cause.
    pub deferred: u64,
    /// Holder-free tombstone entries removed by acknowledged leaf rewrites.
    pub tombstones_reclaimed: u64,
    /// Actionable splits cancelled after tombstone reclamation removed the need.
    pub splits_avoided: u64,
    /// Locally observed merge linearizations (drains of the merged node).
    pub merges: u64,
    /// Activity attributable specifically to aggregate inline pressure.
    pub inline_pressure: InlinePressureStats,
}

/// Split activity attributable to aggregate inline pressure.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct InlinePressureStats {
    /// Processed candidates.
    pub candidates: u64,
    /// Locally observed leaf splits.
    pub completed: u64,
    /// Retryable candidate attempts requeued.
    pub deferred: u64,
    /// Candidates discarded after authoritative revalidation.
    pub discarded: u64,
}

impl Stats {
    /// Returns the counters and resets them.
    pub(super) fn take(&self) -> RestructurerStats {
        let take = |counter: &AtomicU64| counter.swap(0, Ordering::Relaxed);
        RestructurerStats {
            candidates: take(&self.candidates),
            splits: take(&self.splits),
            deferred: take(&self.deferred),
            tombstones_reclaimed: take(&self.tombstones_reclaimed),
            splits_avoided: take(&self.splits_avoided),
            merges: take(&self.merges),
            inline_pressure: InlinePressureStats {
                candidates: take(&self.inline_pressure_candidates),
                completed: take(&self.inline_pressure_completed),
                deferred: take(&self.inline_pressure_deferred),
                discarded: take(&self.inline_pressure_discarded),
            },
        }
    }
}

impl AddAssign for InlinePressureStats {
    fn add_assign(&mut self, rhs: Self) {
        self.candidates += rhs.candidates;
        self.completed += rhs.completed;
        self.deferred += rhs.deferred;
        self.discarded += rhs.discarded;
    }
}

impl Sub for InlinePressureStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            completed: self.completed.saturating_sub(rhs.completed),
            deferred: self.deferred.saturating_sub(rhs.deferred),
            discarded: self.discarded.saturating_sub(rhs.discarded),
        }
    }
}

impl AddAssign for RestructurerStats {
    fn add_assign(&mut self, rhs: Self) {
        self.candidates += rhs.candidates;
        self.splits += rhs.splits;
        self.deferred += rhs.deferred;
        self.tombstones_reclaimed += rhs.tombstones_reclaimed;
        self.splits_avoided += rhs.splits_avoided;
        self.merges += rhs.merges;
        self.inline_pressure += rhs.inline_pressure;
    }
}

impl Sub for RestructurerStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            splits: self.splits.saturating_sub(rhs.splits),
            deferred: self.deferred.saturating_sub(rhs.deferred),
            tombstones_reclaimed: self
                .tombstones_reclaimed
                .saturating_sub(rhs.tombstones_reclaimed),
            splits_avoided: self.splits_avoided.saturating_sub(rhs.splits_avoided),
            merges: self.merges.saturating_sub(rhs.merges),
            inline_pressure: self.inline_pressure - rhs.inline_pressure,
        }
    }
}
