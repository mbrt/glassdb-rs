//! The write budget of background GC (ADR-078).

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use glassdb_backend::WriteMeter;
use glassdb_concurr::rt;

// GC writes at most as much as the transactions of its instance.
const BETA: f64 = 1.0;
// GC spends the budget of a burst within about this time after the burst.
const SPEND_TIME: Duration = Duration::from_secs(600);
// Records that scans find late keep their budget for about this time.
const KEEP_TIME: Duration = Duration::from_secs(7200);
// Lets an instance with few transactions delete garbage of other instances.
pub(super) const DEFAULT_BASE_RATE: f64 = 100.0;
// A lower base rate is too small to let GC ever finish its work, and its waits
// do not fit in a `Duration`.
pub(super) const MIN_BASE_RATE: f64 = 0.01;
// The allowance holds the writes of this much time at the spend limit, so a
// pause of GC does not turn into a burst.
const ALLOWANCE_TIME: Duration = Duration::from_secs(1);
// Each check that starts reserves this many writes. Most checks make one or two
// writes, and the estimate of the check replaces the reservation when it ends.
const CHECK_RESERVATION: u64 = 1;

/// Limits the rate of the writes that background GC sends.
///
/// The budget follows the writes of the instance. All writes count except
/// those that GC estimates as its own, so splits, merges, and lost CASes of
/// transactions also add budget. The estimate is rough on purpose: the budget
/// limits a rate, and precision would need attribution in the write path.
pub(super) struct WriteBudget {
    meter: Option<WriteMeter>,
    base_rate: f64,
    // The estimated writes of background GC, which spend the budget.
    background: Arc<AtomicU64>,
    // The estimated writes of full GCs, which spend none.
    full_gc: Arc<AtomicU64>,
    // The highest value seen of `meter - background - full_gc`. A GC write is
    // counted by the meter when it starts, and by the estimate only when its
    // check ends, so the difference can drop and rise again for the same write.
    other_writes: u64,
    spent: u64,
    reserved: u64,
    earned: f64,
    allowance: f64,
    updated: rt::Instant,
}

impl WriteBudget {
    /// Creates a budget over the writes that `meter` counts, with at least
    /// `base_rate` writes in each second. `background` and `full_gc` are the
    /// estimated writes of background GC and of full GCs. Without a meter, the
    /// budget allows only the base rate.
    pub(super) fn new(
        meter: Option<WriteMeter>,
        base_rate: f64,
        background: Arc<AtomicU64>,
        full_gc: Arc<AtomicU64>,
        now: rt::Instant,
    ) -> Self {
        let base_rate = valid_base_rate(base_rate);
        let mut budget = Self {
            meter,
            base_rate,
            background,
            full_gc,
            other_writes: 0,
            spent: 0,
            reserved: 0,
            earned: 0.0,
            allowance: base_rate * ALLOWANCE_TIME.as_secs_f64(),
            updated: now,
        };
        // Writes before the start, such as those of an earlier full GC, earn
        // and spend nothing.
        budget.other_writes = budget.other_writes_now();
        budget.spent = budget.background.load(Ordering::Relaxed);
        budget
    }

    /// Adds the writes since the last refresh and spends those of background
    /// GC.
    pub(super) fn refresh(&mut self, now: rt::Instant) {
        let elapsed = now.saturating_duration_since(self.updated).as_secs_f64();
        self.updated = now;
        self.earned *= (-elapsed / KEEP_TIME.as_secs_f64()).exp();
        let other = self.other_writes_now();
        if other > self.other_writes {
            self.earned += BETA * (other - self.other_writes) as f64;
            self.other_writes = other;
        }
        let spent = self.background.load(Ordering::Relaxed);
        let writes = spent.saturating_sub(self.spent);
        self.spent = spent;
        self.earned = (self.earned - writes as f64).max(0.0);
        let limit = self.spend_limit();
        self.allowance = (self.allowance + limit * elapsed)
            .min(limit * ALLOWANCE_TIME.as_secs_f64())
            - writes as f64;
    }

    /// Reserves the writes of one check, and reports whether it can start.
    /// The reservation keeps a refill from starting as many checks as the
    /// concurrency allows.
    pub(super) fn reserve_check(&mut self) -> bool {
        if self.allowance <= 0.0 {
            return false;
        }
        self.allowance -= CHECK_RESERVATION as f64;
        self.reserved += CHECK_RESERVATION;
        true
    }

    /// Returns the reservation of a check that completed. Its estimated writes
    /// spend the budget at the next refresh.
    pub(super) fn release_check(&mut self) {
        let returned = self.reserved.min(CHECK_RESERVATION);
        self.reserved -= returned;
        self.allowance += returned as f64;
    }

    /// Reports whether background GC can start another check.
    pub(super) fn allows_checks(&self) -> bool {
        self.allowance > 0.0
    }

    /// Returns when the allowance becomes positive again, or `None` when it is
    /// positive now. The wait is at most `max_wait`.
    pub(super) fn refill_at(&self, max_wait: Duration) -> Option<rt::Instant> {
        if self.allows_checks() {
            return None;
        }
        let wait = (-self.allowance + 1.0) / self.spend_limit();
        let wait = Duration::try_from_secs_f64(wait).map_or(max_wait, |wait| wait.min(max_wait));
        Some(self.updated + wait)
    }

    fn other_writes_now(&self) -> u64 {
        let writes = self.meter.as_ref().map_or(0, WriteMeter::writes);
        writes
            .saturating_sub(self.background.load(Ordering::Relaxed))
            .saturating_sub(self.full_gc.load(Ordering::Relaxed))
    }

    /// The writes in each second that GC can spend now.
    fn spend_limit(&self) -> f64 {
        self.earned / SPEND_TIME.as_secs_f64() + self.base_rate
    }
}

/// Returns a usable base rate: finite, and not below [`MIN_BASE_RATE`].
pub(super) fn valid_base_rate(rate: f64) -> f64 {
    if rate.is_nan() {
        return DEFAULT_BASE_RATE;
    }
    rate.clamp(MIN_BASE_RATE, f64::MAX)
}

#[cfg(test)]
mod tests {
    use glassdb_backend::{Backend, Revision, StatsBackend, memory::MemoryBackend};

    use super::*;

    struct Fixture {
        backend: StatsBackend,
        background: Arc<AtomicU64>,
        full_gc: Arc<AtomicU64>,
        start: rt::Instant,
    }

    impl Fixture {
        fn new() -> Self {
            Self {
                backend: StatsBackend::new(Arc::new(MemoryBackend::new())),
                background: Arc::default(),
                full_gc: Arc::default(),
                start: rt::Instant::now(),
            }
        }

        fn budget(&self, base_rate: f64) -> WriteBudget {
            WriteBudget::new(
                Some(self.backend.write_meter()),
                base_rate,
                self.background.clone(),
                self.full_gc.clone(),
                self.start,
            )
        }

        async fn write(&self, count: usize) {
            for _ in 0..count {
                // A rejected write also counts, like a lost CAS of a transaction.
                let _ = self
                    .backend
                    .write_if("k", Vec::new(), &Revision::default())
                    .await;
            }
        }

        fn at(&self, seconds: u64) -> rt::Instant {
            self.start + Duration::from_secs(seconds)
        }
    }

    // Starts and completes one-write checks while the allowance permits, and
    // returns their number.
    fn run_checks(budget: &mut WriteBudget, background: &AtomicU64) -> u64 {
        let mut checks = 0;
        while budget.reserve_check() {
            background.fetch_add(1, Ordering::Relaxed);
            budget.release_check();
            budget.refresh(budget.updated);
            checks += 1;
        }
        checks
    }

    #[tokio::test(start_paused = true)]
    async fn an_idle_instance_runs_at_the_base_rate() {
        let fixture = Fixture::new();
        let mut budget = fixture.budget(DEFAULT_BASE_RATE);
        run_checks(&mut budget, &fixture.background);

        budget.refresh(fixture.at(1));

        assert_eq!(run_checks(&mut budget, &fixture.background), 100);
    }

    #[tokio::test(start_paused = true)]
    async fn other_writes_raise_the_spend_limit() {
        let fixture = Fixture::new();
        let mut budget = fixture.budget(DEFAULT_BASE_RATE);
        run_checks(&mut budget, &fixture.background);
        fixture.write(60_000).await;

        budget.refresh(fixture.at(1));

        // 60,000 earned writes over the 600 s spend time add 100 writes in
        // each second to the base rate.
        let checks = run_checks(&mut budget, &fixture.background);
        assert!((195..=200).contains(&checks), "{checks}");
    }

    #[tokio::test(start_paused = true)]
    async fn gc_writes_earn_no_budget() {
        let fixture = Fixture::new();
        let mut budget = fixture.budget(DEFAULT_BASE_RATE);
        run_checks(&mut budget, &fixture.background);
        fixture.write(60_000).await;
        // Half of the writes come from a full GC, half from background checks
        // that the budget already allowed.
        fixture.full_gc.fetch_add(30_000, Ordering::Relaxed);
        fixture.background.fetch_add(30_000, Ordering::Relaxed);

        budget.refresh(fixture.at(600));

        // The background writes spend all the budget of 600 s, and the full
        // GC writes spend none of it.
        assert!(!budget.allows_checks());
        assert_eq!(budget.earned, 0.0);
    }

    #[tokio::test(start_paused = true)]
    async fn writes_in_flight_earn_budget_once() {
        let fixture = Fixture::new();
        let mut budget = fixture.budget(DEFAULT_BASE_RATE);
        // The meter counts a GC write when it starts, before the check that
        // estimates it completes.
        fixture.write(1_000).await;
        budget.refresh(fixture.at(1));
        let earned = budget.earned;
        fixture.background.fetch_add(1_000, Ordering::Relaxed);
        budget.refresh(fixture.at(1));
        fixture.write(1_000).await;
        fixture.background.fetch_add(1_000, Ordering::Relaxed);

        budget.refresh(fixture.at(1));

        assert!(earned > 0.0);
        assert_eq!(
            budget.earned, 0.0,
            "the first writes earned once, then GC spent them"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_refill_starts_no_burst_of_checks() {
        let fixture = Fixture::new();
        let mut budget = fixture.budget(DEFAULT_BASE_RATE);
        fixture.background.fetch_add(1_000, Ordering::Relaxed);
        budget.refresh(fixture.start);
        assert!(!budget.allows_checks());
        let refill = budget.refill_at(Duration::from_secs(60)).unwrap();

        budget.refresh(refill);

        // The checks started before their writes are known are bounded by the
        // allowance, not by the concurrency.
        let mut started = 0;
        while budget.reserve_check() {
            started += 1;
        }
        assert_eq!(started, 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_pause_does_not_turn_into_a_burst() {
        let fixture = Fixture::new();
        let mut budget = fixture.budget(DEFAULT_BASE_RATE);
        run_checks(&mut budget, &fixture.background);

        budget.refresh(fixture.at(3600));

        assert_eq!(run_checks(&mut budget, &fixture.background), 100);
    }

    #[tokio::test(start_paused = true)]
    async fn an_invalid_base_rate_still_lets_gc_progress() {
        // A NaN uses the default rate, which needs more writes to block.
        for rate in [0.0, -1.0, f64::NAN, f64::MIN_POSITIVE] {
            let fixture = Fixture::new();
            let mut budget = fixture.budget(rate);
            fixture.background.fetch_add(1_000, Ordering::Relaxed);
            budget.refresh(fixture.start);

            let refill = budget
                .refill_at(Duration::from_secs(600))
                .expect("the writes used the allowance");

            assert!(refill.saturating_duration_since(fixture.start) <= Duration::from_secs(600));
        }
    }
}
