//! Activity counters of the restructurer.

use std::ops::{AddAssign, Sub};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

/// Live counters that the scheduler and every structural change update.
#[derive(Default)]
pub(super) struct Stats {
    pub(super) candidates: AtomicU64,
    pub(super) splits: AtomicU64,
    pub(super) deferred: AtomicU64,
    pub(super) stale_demand: AtomicU64,
    pub(super) tombstones_reclaimed: AtomicU64,
    pub(super) splits_avoided: AtomicU64,
    pub(super) merges: AtomicU64,
    pub(super) inline_pressure_candidates: AtomicU64,
    pub(super) inline_pressure_completed: AtomicU64,
    pub(super) inline_pressure_deferred: AtomicU64,
    pub(super) inline_pressure_discarded: AtomicU64,
    pub(super) demand_merge_unpaid: AtomicU64,
    pub(super) demand_merge_too_large: AtomicU64,
    pub(super) demand_merge_busy: AtomicU64,
    pub(super) demand_merge_other: AtomicU64,
    pub(super) demand_merge_stale: AtomicU64,
}

/// Background split and merge activity for one snapshot or accumulated
/// interval, and the time that transactions reported to the topology rule
/// that decides it.
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
    /// Candidates of a topology rule dropped because they were too old to
    /// still show the current load (ADR-083).
    pub stale_demand: u64,
    /// Holder-free tombstone entries removed by acknowledged leaf rewrites.
    pub tombstones_reclaimed: u64,
    /// Actionable splits cancelled after tombstone reclamation removed the need.
    pub splits_avoided: u64,
    /// Locally observed merge linearizations (drains of the merged node).
    pub merges: u64,
    /// Activity attributable specifically to aggregate inline pressure.
    pub inline_pressure: InlinePressureStats,
    /// The time that the topology rule measured, by kind. Zero when the size
    /// causes decide.
    pub topology_time: TopologyTimeStats,
    /// The merges that a topology rule asked for and that did not go ahead,
    /// by cause.
    pub demand_merges: DemandMergeStats,
}

/// The attempts of merges that a topology rule asked for, and that did not go
/// ahead, by cause. Each attempt counts once, for the first cause that
/// applies, in the order of the fields. A merge is checked on cached state
/// and again on current state, and each check can count. A deferred merge can
/// count more than once.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DemandMergeStats {
    /// The rate did not pay more than the paid rates of the leaves (ADR-076).
    pub unpaid: u64,
    /// The merged leaf would be over half of a split threshold (ADR-073).
    pub too_large: u64,
    /// A live transaction or structural change held a claim on one of the two
    /// leaves, or another merge reserved one, so the merge was deferred. These
    /// also count in [`RestructurerStats::deferred`].
    pub busy: u64,
    /// The leaf was gone or drained, had no right sibling, or had another
    /// parent than its right sibling.
    pub other: u64,
    /// The request was too old when a sweep took it (ADR-083). These also
    /// count in [`RestructurerStats::stale_demand`].
    pub stale: u64,
}

/// The time that transactions of one database instance reported to its
/// topology rule (ADR-074), by kind. Each value is a sum over the waiting
/// members of a round or over the leaves of a transaction, with no weight of
/// the rule, so it can be more than the wall time.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TopologyTimeStats {
    /// Split side: leaf CASes lost to keys in the other half of the leaf.
    pub lost_cas: Duration,
    /// Split side: waits for an earlier round on keys in the other half.
    pub queue_wait: Duration,
    /// Split side: leaf CASes after such a round that took more time than a
    /// leaf CAS that the backend does not delay.
    pub slow_cas: Duration,
    /// Split side: locked commits of values that the leaf could not carry
    /// inline.
    pub inline_pressure: Duration,
    /// Split cost: commit passes that a conflict ended, over both halves of a
    /// leaf.
    pub divided_conflict: Duration,
    /// Merge side: adjacent cross-leaf misses and scan crossings.
    pub merge_side: Duration,
    /// Merge side: commit passes that a conflict ended, over two adjacent
    /// leaves only.
    pub crossing_conflict: Duration,
}

/// Split activity attributable to aggregate inline pressure. Only the size
/// causes topology policy requests these splits (ADR-075).
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
            stale_demand: take(&self.stale_demand),
            tombstones_reclaimed: take(&self.tombstones_reclaimed),
            splits_avoided: take(&self.splits_avoided),
            merges: take(&self.merges),
            inline_pressure: InlinePressureStats {
                candidates: take(&self.inline_pressure_candidates),
                completed: take(&self.inline_pressure_completed),
                deferred: take(&self.inline_pressure_deferred),
                discarded: take(&self.inline_pressure_discarded),
            },
            topology_time: TopologyTimeStats::default(),
            demand_merges: DemandMergeStats {
                unpaid: take(&self.demand_merge_unpaid),
                too_large: take(&self.demand_merge_too_large),
                busy: take(&self.demand_merge_busy),
                other: take(&self.demand_merge_other),
                stale: take(&self.demand_merge_stale),
            },
        }
    }

    /// Counts one landed split.
    pub(super) fn record_split(&self) {
        self.splits.fetch_add(1, Ordering::Relaxed);
    }

    /// Counts one landed merge.
    pub(super) fn record_merge(&self) {
        self.merges.fetch_add(1, Ordering::Relaxed);
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

impl AddAssign for DemandMergeStats {
    fn add_assign(&mut self, rhs: Self) {
        self.unpaid += rhs.unpaid;
        self.too_large += rhs.too_large;
        self.busy += rhs.busy;
        self.other += rhs.other;
        self.stale += rhs.stale;
    }
}

impl Sub for DemandMergeStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            unpaid: self.unpaid.saturating_sub(rhs.unpaid),
            too_large: self.too_large.saturating_sub(rhs.too_large),
            busy: self.busy.saturating_sub(rhs.busy),
            other: self.other.saturating_sub(rhs.other),
            stale: self.stale.saturating_sub(rhs.stale),
        }
    }
}

impl AddAssign for TopologyTimeStats {
    fn add_assign(&mut self, rhs: Self) {
        self.lost_cas += rhs.lost_cas;
        self.queue_wait += rhs.queue_wait;
        self.slow_cas += rhs.slow_cas;
        self.inline_pressure += rhs.inline_pressure;
        self.divided_conflict += rhs.divided_conflict;
        self.merge_side += rhs.merge_side;
        self.crossing_conflict += rhs.crossing_conflict;
    }
}

impl Sub for TopologyTimeStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            lost_cas: self.lost_cas.saturating_sub(rhs.lost_cas),
            queue_wait: self.queue_wait.saturating_sub(rhs.queue_wait),
            slow_cas: self.slow_cas.saturating_sub(rhs.slow_cas),
            inline_pressure: self.inline_pressure.saturating_sub(rhs.inline_pressure),
            divided_conflict: self.divided_conflict.saturating_sub(rhs.divided_conflict),
            merge_side: self.merge_side.saturating_sub(rhs.merge_side),
            crossing_conflict: self.crossing_conflict.saturating_sub(rhs.crossing_conflict),
        }
    }
}

impl AddAssign for RestructurerStats {
    fn add_assign(&mut self, rhs: Self) {
        self.candidates += rhs.candidates;
        self.splits += rhs.splits;
        self.deferred += rhs.deferred;
        self.stale_demand += rhs.stale_demand;
        self.tombstones_reclaimed += rhs.tombstones_reclaimed;
        self.splits_avoided += rhs.splits_avoided;
        self.merges += rhs.merges;
        self.inline_pressure += rhs.inline_pressure;
        self.topology_time += rhs.topology_time;
        self.demand_merges += rhs.demand_merges;
    }
}

impl Sub for RestructurerStats {
    type Output = Self;

    fn sub(self, rhs: Self) -> Self::Output {
        Self {
            candidates: self.candidates.saturating_sub(rhs.candidates),
            splits: self.splits.saturating_sub(rhs.splits),
            deferred: self.deferred.saturating_sub(rhs.deferred),
            stale_demand: self.stale_demand.saturating_sub(rhs.stale_demand),
            tombstones_reclaimed: self
                .tombstones_reclaimed
                .saturating_sub(rhs.tombstones_reclaimed),
            splits_avoided: self.splits_avoided.saturating_sub(rhs.splits_avoided),
            merges: self.merges.saturating_sub(rhs.merges),
            inline_pressure: self.inline_pressure - rhs.inline_pressure,
            topology_time: self.topology_time - rhs.topology_time,
            demand_merges: self.demand_merges - rhs.demand_merges,
        }
    }
}
