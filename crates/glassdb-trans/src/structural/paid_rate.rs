//! The paid rates that hold the topology of leaves (ADR-076).
//!
//! A split or merge that a topology rule asks for keeps its paid rate in its
//! leaves. The opposite change of these leaves must pay more than a margin
//! times the paid rate, which halves at each half-life. Each database instance
//! measures the age of a paid rate on its own clock, from when it first sees
//! it, so that no instance compares its clock with another one.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use glassdb_concurr::rt;
use glassdb_data::StructuralIntentId;
use glassdb_storage::{PaidChange, PaidRate};

/// The multiple of a paid rate that the opposite change must pay more than.
/// A multiple above one keeps a topology when the two changes cost about the
/// same, because each change also costs backend writes (ADR-076).
const MARGIN: f64 = 1.5;

/// The time in which a paid rate halves, so that a change of the load can
/// change the topology again.
const HALF_LIFE: Duration = Duration::from_secs(60);

/// The paid rates whose first sighting an instance keeps. Above it, the
/// instance forgets the oldest sightings. A forgotten paid rate that the
/// instance sees again holds as a new one, so the bound is far above the
/// paid rates that an instance compares in some half-lives.
const KEPT_PAID_RATES: usize = 4096;

/// The local times when this database instance first saw each paid rate.
#[derive(Debug, Default)]
pub(super) struct PaidRates {
    first_seen: Mutex<HashMap<StructuralIntentId, rt::Instant>>,
}

impl PaidRates {
    /// Reports whether a change of kind `change` that pays `rate` pays more
    /// than each paid rate in `paid` of the opposite kind.
    pub(super) fn pays_more<'a>(
        &self,
        change: PaidChange,
        rate: Duration,
        paid: impl IntoIterator<Item = Option<&'a PaidRate>>,
    ) -> bool {
        let now = rt::Instant::now();
        let mut first_seen = self.first_seen.lock().unwrap();
        let pays = paid
            .into_iter()
            .flatten()
            .filter(|paid| paid.change != change)
            .all(|paid| {
                let seen = *first_seen.entry(paid.intent).or_insert(now);
                let halvings = now.duration_since(seen).as_secs_f64() / HALF_LIFE.as_secs_f64();
                rate.as_secs_f64() > MARGIN * paid.per_second.as_secs_f64() * 0.5_f64.powf(halvings)
            });
        forget_oldest(&mut first_seen);
        pays
    }

    /// Records that a change of this database instance with the structural
    /// intent `intent` landed now, so that the age of its paid rate starts at
    /// the landing.
    pub(super) fn landed(&self, intent: &StructuralIntentId) {
        let mut first_seen = self.first_seen.lock().unwrap();
        first_seen.insert(*intent, rt::Instant::now());
        forget_oldest(&mut first_seen);
    }
}

/// Keeps at most [`KEPT_PAID_RATES`] sightings, the most recent ones.
fn forget_oldest(first_seen: &mut HashMap<StructuralIntentId, rt::Instant>) {
    if first_seen.len() <= KEPT_PAID_RATES {
        return;
    }
    let mut seen: Vec<rt::Instant> = first_seen.values().copied().collect();
    seen.sort_unstable();
    let oldest_kept = seen[seen.len() - KEPT_PAID_RATES];
    first_seen.retain(|_, seen| *seen >= oldest_kept);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn paid(change: PaidChange, millis: u64) -> PaidRate {
        PaidRate {
            change,
            per_second: Duration::from_millis(millis),
            intent: StructuralIntentId::from_bytes([7; 16]),
        }
    }

    // A change pays more than the opposite paid rate only above the margin,
    // and a paid rate of the same kind does not hold it.
    #[tokio::test(start_paused = true)]
    async fn a_change_must_pay_more_than_the_margin_of_the_opposite_paid_rate() {
        let rates = PaidRates::default();
        let split = paid(PaidChange::Split, 1000);
        let merge = PaidChange::Merge;

        assert!(!rates.pays_more(merge, Duration::from_millis(1500), [Some(&split)]));
        assert!(rates.pays_more(merge, Duration::from_millis(1501), [Some(&split)]));
        assert!(rates.pays_more(PaidChange::Split, Duration::ZERO, [Some(&split)]));
        assert!(rates.pays_more(merge, Duration::ZERO, [None]));
    }

    // The paid rate halves at each half-life, from the first time that this
    // instance saw it.
    #[tokio::test(start_paused = true)]
    async fn a_paid_rate_halves_at_each_half_life_since_it_was_first_seen() {
        let rates = PaidRates::default();
        let split = paid(PaidChange::Split, 1000);
        let merge = PaidChange::Merge;
        assert!(!rates.pays_more(merge, Duration::from_millis(1000), [Some(&split)]));

        tokio::time::advance(HALF_LIFE).await;
        assert!(!rates.pays_more(merge, Duration::from_millis(700), [Some(&split)]));
        assert!(rates.pays_more(merge, Duration::from_millis(800), [Some(&split)]));
    }

    // Regression: an instance forgot when it first saw a paid rate after some
    // half-lives, and the next comparison then saw the paid rate as new, at
    // its full value.
    #[tokio::test(start_paused = true)]
    async fn an_old_paid_rate_does_not_hold_again_at_its_full_value() {
        let rates = PaidRates::default();
        let split = paid(PaidChange::Split, 1000);
        assert!(!rates.pays_more(
            PaidChange::Merge,
            Duration::from_millis(1000),
            [Some(&split)]
        ));

        tokio::time::advance(HALF_LIFE * 11).await;

        assert!(rates.pays_more(PaidChange::Merge, Duration::from_millis(2), [Some(&split)]));
    }

    // A merge compares with the paid rates of both leaves.
    #[tokio::test(start_paused = true)]
    async fn a_merge_pays_more_than_the_paid_rates_of_both_leaves() {
        let rates = PaidRates::default();
        let low = paid(PaidChange::Split, 100);
        let high = PaidRate {
            intent: StructuralIntentId::from_bytes([8; 16]),
            ..paid(PaidChange::Split, 1000)
        };

        assert!(!rates.pays_more(
            PaidChange::Merge,
            Duration::from_millis(1000),
            [Some(&low), Some(&high)]
        ));
    }
}
