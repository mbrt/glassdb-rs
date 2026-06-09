//! Retry-with-backoff utilities. Ported from the Go `concurr` backoff helpers.
//! A closure returns [`RetryErr::Transient`] to be retried or
//! [`RetryErr::Permanent`] to stop immediately.

use std::future::Future;
use std::time::Duration;

const INITIAL_INTERVAL: Duration = Duration::from_millis(200);
const MAX_INTERVAL: Duration = Duration::from_secs(5);
const MULTIPLIER: f64 = 1.5;
/// Fraction by which a backoff delay is randomized: a delay `d` becomes uniform
/// in `[0.5*d, 1.5*d]`.
const JITTER_FACTOR: f64 = 0.5;

/// Tunes the exponential backoff used to retry transient operations: the first
/// delay, and the cap each delay grows toward.
#[derive(Debug, Clone, Copy)]
pub struct RetryConfig {
    /// Delay before the first retry; grows exponentially up to `max_interval`.
    pub initial_interval: Duration,
    /// Upper bound on the per-retry delay.
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
            current: self.initial_interval,
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
        let delay = jittered(self.current);
        self.current = std::cmp::min(self.current.mul_f64(MULTIPLIER), self.max);
        delay
    }
}

/// Returns a value in `[0, 1)`, drawn from the OS RNG in normal builds and from
/// the deterministic executor's seeded entropy under `--cfg sim` so replays stay
/// byte-identical.
#[cfg(not(sim))]
fn rand_unit() -> f64 {
    use rand::RngExt;
    rand::rng().random::<f64>()
}

#[cfg(sim)]
fn rand_unit() -> f64 {
    // Inside the executor draw from its seeded entropy; outside it (e.g. ordinary
    // tokio tests built with `--cfg sim`) fall back to the OS RNG.
    if crate::rt::in_sim() {
        let mut b = [0u8; 8];
        crate::rt::fill_random(&mut b);
        // Map the 53 high bits to [0, 1), as rand's StandardUniform does for f64.
        ((u64::from_le_bytes(b) >> 11) as f64) / ((1u64 << 53) as f64)
    } else {
        use rand::RngExt;
        rand::rng().random::<f64>()
    }
}

/// Perturbs `d` by +/-[`JITTER_FACTOR`], uniformly in `[0.5*d, 1.5*d]`.
fn jittered(d: Duration) -> Duration {
    let base = d.as_secs_f64();
    let min = base * (1.0 - JITTER_FACTOR);
    let span = base * (2.0 * JITTER_FACTOR);
    Duration::from_secs_f64(min + rand_unit() * span)
}

/// Classifies a retryable operation's error.
#[derive(Debug)]
pub enum RetryErr<E> {
    /// The operation should be retried after a backoff.
    Transient(E),
    /// The operation must not be retried; the inner error is returned as-is.
    Permanent(E),
}

impl<E> RetryErr<E> {
    /// Convenience constructor for a transient error.
    pub fn transient(e: E) -> Self {
        RetryErr::Transient(e)
    }

    /// Convenience constructor for a permanent error.
    pub fn permanent(e: E) -> Self {
        RetryErr::Permanent(e)
    }
}

/// Retries `f` with default exponential backoff until it succeeds or returns a
/// permanent error. Callers cancel a retry loop by dropping the returned
/// future (e.g. via [`tokio::time::timeout`] or [`tokio::select!`]).
pub async fn retry_with_backoff<T, E, F, Fut>(f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RetryErr<E>>>,
{
    retry(INITIAL_INTERVAL, MAX_INTERVAL, f).await
}

/// Retries `f` with exponential backoff between `initial` and `max` intervals.
/// Callers wrap the call in `tokio::time::timeout` to bound the total wait;
/// dropping the future is the only cancellation mechanism.
pub async fn retry<T, E, F, Fut>(initial: Duration, max: Duration, mut f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RetryErr<E>>>,
{
    let mut backoff = RetryConfig {
        initial_interval: initial,
        max_interval: max,
    }
    .backoff();
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(RetryErr::Permanent(e)) => return Err(e),
            Err(RetryErr::Transient(_e)) => {
                crate::rt::sleep(backoff.next_delay()).await;
            }
        }
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
            let d = jittered(base);
            assert!(d >= lo && d <= hi, "jittered {d:?} out of [{lo:?}, {hi:?}]");
        }
    }

    #[test]
    fn jitter_varies() {
        let base = Duration::from_millis(200);
        let first = jittered(base);
        // Over many draws at least one differs from the first; a fixed output
        // would mean jitter is not actually applied.
        assert!((0..1000).any(|_| jittered(base) != first));
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
            let hi = base.mul_f64(1.0 + JITTER_FACTOR);
            assert!(d >= lo && d <= hi, "delay {d:?} out of [{lo:?}, {hi:?}]");
            base = std::cmp::min(base.mul_f64(MULTIPLIER), cfg.max_interval);
        }
        // Once capped, the base no longer grows.
        assert_eq!(base, cfg.max_interval);
    }
}
