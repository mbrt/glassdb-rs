//! Retry-with-backoff utilities. Ported from the Go `concurr` backoff helpers.

use std::time::Duration;

const INITIAL_INTERVAL: Duration = Duration::from_millis(200);
const MAX_INTERVAL: Duration = Duration::from_secs(5);
const MULTIPLIER: f64 = 1.5;
/// Fraction by which a backoff delay is randomized: a delay `d` becomes uniform
/// in `[0.5*d, 1.5*d]`, with the upper end limited by the configured maximum.
const JITTER_FACTOR: f64 = 0.5;

/// Tunes the exponential backoff used to retry transient operations: the first
/// delay, and the cap each delay grows toward.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Base interval before the first retry, limited to `max_interval`.
    pub initial_interval: Duration,
    /// Upper bound on the per-retry delay, including jitter.
    pub max_interval: Duration,
}

impl Default for RetryConfig {
    fn default() -> Self {
        RetryConfig {
            initial_interval: INITIAL_INTERVAL,
            max_interval: MAX_INTERVAL,
        }
    }
}

impl RetryConfig {
    /// Starts a fresh exponential backoff schedule from this configuration. Each
    /// retry loop gets its own [`Backoff`] so the interval resets per attempt.
    pub fn backoff(&self) -> Backoff {
        Backoff {
            current: self.initial_interval.min(self.max_interval),
            max: self.max_interval,
        }
    }
}

/// A stateful exponential backoff schedule. Call [`Backoff::next_delay`] before
/// each wait to get the (jittered) delay and advance the schedule, instead of
/// recomputing the interval growth and jitter at every call site.
#[derive(Debug, Clone)]
pub struct Backoff {
    current: Duration,
    max: Duration,
}

impl Backoff {
    /// Returns the jittered delay to wait before the next attempt, then grows the
    /// base interval toward `max` (capped) for the following call. Jitter is
    /// always on: it spreads retries to avoid thundering-herd contention when
    /// many clients poll the same transaction.
    pub fn next_delay(&mut self) -> Duration {
        let delay = jittered(self.current, self.max);
        self.current = Duration::try_from_secs_f64(self.current.as_secs_f64() * MULTIPLIER)
            .unwrap_or(self.max)
            .min(self.max);
        delay
    }
}

/// Perturbs `d` within its jitter band and the configured maximum.
fn jittered(d: Duration, max: Duration) -> Duration {
    let base = d.as_secs_f64();
    let min = base * (1.0 - JITTER_FACTOR);
    let upper = (base * (1.0 + JITTER_FACTOR)).min(max.as_secs_f64());
    Duration::try_from_secs_f64(min + crate::entropy::uniform_unit() * (upper - min))
        .unwrap_or(max)
        .min(max)
}

/// Adjusts scan intervals from recent progress within fixed bounds.
pub struct ScanCadence {
    minimum: Duration,
    maximum: Duration,
    productive: f64,
}

impl ScanCadence {
    /// Starts a scan schedule at its minimum interval.
    pub fn new(minimum: Duration, maximum: Duration) -> Self {
        Self {
            minimum,
            maximum,
            productive: 1.0,
        }
    }

    /// Updates the demand estimate from whether a scan made useful progress.
    pub fn observe(&mut self, useful: bool) {
        self.productive = 0.75 * self.productive + 0.25 * f64::from(useful);
    }

    /// Returns an interval with random variation within the configured bounds.
    pub fn delay(&self) -> Duration {
        let seconds = self.minimum.as_secs_f64()
            / self
                .productive
                .powi(2)
                .max(self.minimum.as_secs_f64() / self.maximum.as_secs_f64());
        Duration::from_secs_f64(seconds * (0.9 + 0.2 * crate::entropy::uniform_unit()))
            .clamp(self.minimum, self.maximum)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jitter_stays_within_half_to_one_and_a_half() {
        let base = Duration::from_millis(200);
        let lo = base.mul_f64(1.0 - JITTER_FACTOR);
        let hi = base.mul_f64(1.0 + JITTER_FACTOR);
        for _ in 0..10_000 {
            let d = jittered(base, Duration::MAX);
            assert!(d >= lo && d <= hi, "jittered {d:?} out of [{lo:?}, {hi:?}]");
        }
    }

    #[test]
    fn jitter_varies() {
        let base = Duration::from_millis(200);
        let first = jittered(base, Duration::MAX);
        // Over many draws at least one differs from the first; a fixed output
        // would mean jitter is not actually applied.
        assert!((0..1000).any(|_| jittered(base, Duration::MAX) != first));
    }

    #[test]
    fn backoff_grows_exponentially_and_caps() {
        let cfg = RetryConfig {
            initial_interval: Duration::from_millis(100),
            max_interval: Duration::from_millis(400),
        };
        let mut b = cfg.backoff();
        // Each delay is jittered, but its base interval should track
        // 100 -> 150 -> 225 -> ... capped at 400. Check the delay stays within
        // the jitter band of the expected (capped) base for many steps.
        let mut base = cfg.initial_interval;
        for _ in 0..20 {
            let d = b.next_delay();
            let lo = base.mul_f64(1.0 - JITTER_FACTOR);
            let hi = base.mul_f64(1.0 + JITTER_FACTOR).min(cfg.max_interval);
            assert!(d >= lo && d <= hi, "delay {d:?} out of [{lo:?}, {hi:?}]");
            base = std::cmp::min(base.mul_f64(MULTIPLIER), cfg.max_interval);
        }
        // Once capped, the base no longer grows.
        assert_eq!(base, cfg.max_interval);
    }

    #[test]
    fn backoff_caps_an_initial_interval_above_the_maximum() {
        for max_interval in [Duration::ZERO, Duration::from_millis(1)] {
            let mut backoff = RetryConfig {
                initial_interval: Duration::from_secs(1),
                max_interval,
            }
            .backoff();
            for _ in 0..20 {
                assert!(backoff.next_delay() <= max_interval);
            }
        }
    }

    #[test]
    fn backoff_handles_the_largest_duration() {
        let mut backoff = RetryConfig {
            initial_interval: Duration::MAX,
            max_interval: Duration::MAX,
        }
        .backoff();
        for _ in 0..20 {
            assert!(backoff.next_delay() >= Duration::MAX / 2);
        }
    }
}

#[cfg(all(test, sim))]
mod sim_tests {
    use super::*;

    #[test]
    fn retry_delays_never_exceed_the_configured_maximum() {
        crate::exec::block_on_with(crate::exec::TapeScheduler::new(Vec::new()), 7, async {
            for config in [
                RetryConfig::default(),
                RetryConfig {
                    initial_interval: Duration::from_millis(1),
                    max_interval: Duration::from_millis(1),
                },
            ] {
                let mut backoff = config.backoff();
                let mut delays = Vec::new();
                for _ in 0..100 {
                    let delay = backoff.next_delay();
                    assert!(delay <= config.max_interval);
                    delays.push(delay);
                }
                assert!(delays.windows(2).any(|pair| pair[0] != pair[1]));
            }
        });
    }
}
