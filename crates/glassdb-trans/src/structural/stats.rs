//! Activity counters of the topology maintenance.

use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};

use super::candidates::CandidateCause;
use super::split::SplitReason;

/// Background split and merge activity for one snapshot or accumulated
/// interval.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopologyStats {
    /// Splits of nodes.
    pub splits: SplitStats,
    /// Merges of nodes.
    pub merges: MergeStats,
    /// Holder-free tombstone entries removed by acknowledged leaf rewrites.
    pub tombstones_reclaimed: u64,
}

impl AddAssign for TopologyStats {
    fn add_assign(&mut self, rhs: Self) {
        // Destructuring fails to compile when a field is added and not summed.
        let Self {
            splits,
            merges,
            tombstones_reclaimed,
        } = rhs;
        self.splits += splits;
        self.merges += merges;
        self.tombstones_reclaimed += tombstones_reclaimed;
    }
}

impl Sub for TopologyStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self {
            splits: self.splits - rhs.splits,
            merges: self.merges - rhs.merges,
            tombstones_reclaimed: self
                .tombstones_reclaimed
                .saturating_sub(rhs.tombstones_reclaimed),
        }
    }
}

/// Split activity for any cause, and for the causes that have their own
/// counters.
///
/// A split can also be `deferred` if a later publication or cleanup step
/// needs another sweep, so the fields are not mutually exclusive outcomes.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitStats {
    /// Deduplicated candidates processed for any cause.
    pub candidates: u64,
    /// Locally observed source/root split linearizations for any cause.
    pub completed: u64,
    /// Retryable candidate attempts requeued for any cause.
    pub deferred: u64,
    /// Actionable splits cancelled after tombstone reclamation removed the
    /// need.
    pub avoided: u64,
    /// Splits that separate a contended key from the keys of other database
    /// instances (ADR-085).
    pub contention: SplitCauseStats,
    /// Splits for aggregate inline pressure (ADR-056).
    pub inline_pressure: SplitCauseStats,
}

impl AddAssign for SplitStats {
    fn add_assign(&mut self, rhs: Self) {
        // Destructuring fails to compile when a field is added and not summed.
        let Self {
            candidates,
            completed,
            deferred,
            avoided,
            contention,
            inline_pressure,
        } = rhs;
        self.candidates += candidates;
        self.completed += completed;
        self.deferred += deferred;
        self.avoided += avoided;
        self.contention += contention;
        self.inline_pressure += inline_pressure;
    }
}

impl Sub for SplitStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            completed: self.completed.saturating_sub(rhs.completed),
            deferred: self.deferred.saturating_sub(rhs.deferred),
            avoided: self.avoided.saturating_sub(rhs.avoided),
            contention: self.contention - rhs.contention,
            inline_pressure: self.inline_pressure - rhs.inline_pressure,
        }
    }
}

/// Split activity attributable to one cause. Candidates, completed splits,
/// and deferred attempts also count in the [`SplitStats`] that holds them.
/// Discarded candidates count only here.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SplitCauseStats {
    /// Processed candidates.
    pub candidates: u64,
    /// Locally observed splits.
    pub completed: u64,
    /// Retryable candidate attempts requeued.
    pub deferred: u64,
    /// Candidates discarded after authoritative revalidation.
    pub discarded: u64,
}

impl AddAssign for SplitCauseStats {
    fn add_assign(&mut self, rhs: Self) {
        // Destructuring fails to compile when a field is added and not summed.
        let Self {
            candidates,
            completed,
            deferred,
            discarded,
        } = rhs;
        self.candidates += candidates;
        self.completed += completed;
        self.deferred += deferred;
        self.discarded += discarded;
    }
}

impl Sub for SplitCauseStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            completed: self.completed.saturating_sub(rhs.completed),
            deferred: self.deferred.saturating_sub(rhs.deferred),
            discarded: self.discarded.saturating_sub(rhs.discarded),
        }
    }
}

/// Merge activity.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct MergeStats {
    /// Deduplicated candidates processed for any cause.
    pub candidates: u64,
    /// Locally observed merge linearizations (drains of the merged node).
    pub completed: u64,
    /// Retryable candidate attempts requeued for any cause.
    pub deferred: u64,
    /// Checks of underfull merges that waited, because another database
    /// instance writes one of the two leaves, and this one does not
    /// (ADR-085). A merge is checked again at each write of its leaf, so one
    /// merge can count many times.
    pub kept_apart: u64,
}

impl AddAssign for MergeStats {
    fn add_assign(&mut self, rhs: Self) {
        // Destructuring fails to compile when a field is added and not summed.
        let Self {
            candidates,
            completed,
            deferred,
            kept_apart,
        } = rhs;
        self.candidates += candidates;
        self.completed += completed;
        self.deferred += deferred;
        self.kept_apart += kept_apart;
    }
}

impl Sub for MergeStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            completed: self.completed.saturating_sub(rhs.completed),
            deferred: self.deferred.saturating_sub(rhs.deferred),
            kept_apart: self.kept_apart.saturating_sub(rhs.kept_apart),
        }
    }
}

/// Live counters that the scheduler and every structural change update.
#[derive(Default)]
pub(super) struct Stats {
    splits: SplitCounters,
    merges: MergeCounters,
    tombstones_reclaimed: AtomicU64,
}

#[derive(Default)]
struct SplitCounters {
    candidates: AtomicU64,
    completed: AtomicU64,
    deferred: AtomicU64,
    avoided: AtomicU64,
    contention: CauseCounters,
    inline_pressure: CauseCounters,
}

#[derive(Default)]
struct CauseCounters {
    candidates: AtomicU64,
    completed: AtomicU64,
    deferred: AtomicU64,
    discarded: AtomicU64,
}

#[derive(Default)]
struct MergeCounters {
    candidates: AtomicU64,
    completed: AtomicU64,
    deferred: AtomicU64,
    kept_apart: AtomicU64,
}

impl Stats {
    /// Returns the counters and resets them.
    pub(super) fn take(&self) -> TopologyStats {
        TopologyStats {
            splits: self.splits.take(),
            merges: self.merges.take(),
            tombstones_reclaimed: take(&self.tombstones_reclaimed),
        }
    }

    /// Counts one candidate that a sweep processes.
    pub(super) fn count_candidate(&self, cause: &CandidateCause) {
        match cause {
            CandidateCause::Split(reason) => {
                bump(&self.splits.candidates);
                if let Some(counters) = self.splits.of(reason) {
                    bump(&counters.candidates);
                }
            }
            CandidateCause::Merge(_) => bump(&self.merges.candidates),
        }
    }

    /// Counts one candidate attempt that a sweep requeues.
    pub(super) fn count_deferred(&self, cause: &CandidateCause) {
        match cause {
            CandidateCause::Split(reason) => {
                bump(&self.splits.deferred);
                if let Some(counters) = self.splits.of(reason) {
                    bump(&counters.deferred);
                }
            }
            CandidateCause::Merge(_) => bump(&self.merges.deferred),
        }
    }

    /// Counts one split candidate that is no longer actionable.
    pub(super) fn count_discarded_split(&self, reason: &SplitReason) {
        if let Some(counters) = self.splits.of(reason) {
            bump(&counters.discarded);
        }
    }

    /// Counts one landed split.
    pub(super) fn record_split(&self, reason: &SplitReason) {
        bump(&self.splits.completed);
        if let Some(counters) = self.splits.of(reason) {
            bump(&counters.completed);
        }
    }

    /// Counts one landed merge.
    pub(super) fn record_merge(&self) {
        bump(&self.merges.completed);
    }

    /// Counts one underfull merge that waits for the writers of its leaves.
    pub(super) fn record_kept_apart(&self) {
        bump(&self.merges.kept_apart);
    }

    /// Counts the tombstones that an acknowledged node write removed.
    /// `split_avoided` tells that the reclamation made a split unnecessary.
    pub(super) fn record_reclaimed(&self, tombstones: u64, split_avoided: bool) {
        self.tombstones_reclaimed
            .fetch_add(tombstones, Ordering::Relaxed);
        if split_avoided {
            bump(&self.splits.avoided);
        }
    }
}

impl SplitCounters {
    /// Returns the counters of the cause of `reason`, if the cause has its own.
    fn of(&self, reason: &SplitReason) -> Option<&CauseCounters> {
        match reason {
            SplitReason::Contention { .. } => Some(&self.contention),
            SplitReason::InlinePressure { .. } => Some(&self.inline_pressure),
            SplitReason::SoftCap | SplitReason::Capacity => None,
        }
    }

    fn take(&self) -> SplitStats {
        SplitStats {
            candidates: take(&self.candidates),
            completed: take(&self.completed),
            deferred: take(&self.deferred),
            avoided: take(&self.avoided),
            contention: self.contention.take(),
            inline_pressure: self.inline_pressure.take(),
        }
    }
}

impl CauseCounters {
    fn take(&self) -> SplitCauseStats {
        SplitCauseStats {
            candidates: take(&self.candidates),
            completed: take(&self.completed),
            deferred: take(&self.deferred),
            discarded: take(&self.discarded),
        }
    }
}

impl MergeCounters {
    fn take(&self) -> MergeStats {
        MergeStats {
            candidates: take(&self.candidates),
            completed: take(&self.completed),
            deferred: take(&self.deferred),
            kept_apart: take(&self.kept_apart),
        }
    }
}

fn bump(counter: &AtomicU64) {
    counter.fetch_add(1, Ordering::Relaxed);
}

fn take(counter: &AtomicU64) -> u64 {
    counter.swap(0, Ordering::Relaxed)
}
