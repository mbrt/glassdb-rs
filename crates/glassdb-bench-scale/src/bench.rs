//! Timing-sample collection and analysis, ported from the Go
//! `internal/testkit/bench` package.
//!
//! [`Bench`] accumulates per-operation latency samples over a configurable
//! duration; [`Results`] computes the mean and percentiles (using the same R8
//! interpolation method as the Go code, so the numbers line up).
//!
//! A [`Bench`] can carry a `time_scale` multiplier ([`Bench::with_time_scale`])
//! applied to every recorded latency and to the total duration. It compensates
//! for a backend that compresses wall-clock time — `DelayBackend`'s
//! `--delay-scale` sleeps and rate-limits at `s * real`, so passing
//! `1.0 / s` reports latency and throughput in the *simulated*
//! (real-time-equivalent) domain instead of the compressed wall-clock one.
//! Count-based metrics (ops/tx, retries) are unaffected either way.

use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const DEFAULT_DURATION: Duration = Duration::from_secs(10);
const MIN_SAMPLES: usize = 10;

/// Tracks timing samples over a configurable duration for benchmarking. Shared
/// across concurrent workers, so all methods take `&self`.
pub struct Bench {
    expected_duration: Duration,
    /// Multiplier applied to every recorded latency and to the total duration,
    /// to report simulated (real-time-equivalent) values when the backend
    /// compresses wall-clock time. `1.0` records raw wall-clock.
    time_scale: f64,
    inner: Mutex<Inner>,
}

struct Inner {
    start_time: Option<Instant>,
    tot_duration: Duration,
    samples: Vec<Duration>,
}

impl Bench {
    /// Creates a benchmark that runs for `duration` (or the 10s default when
    /// `duration` is zero), recording raw wall-clock latencies.
    pub fn new(duration: Duration) -> Self {
        Self::with_time_scale(duration, 1.0)
    }

    /// Like [`Bench::new`], but multiplies every recorded latency and the total
    /// duration by `time_scale`. Pass `1.0 / delay_scale` to undo a
    /// `DelayBackend`'s wall-clock compression so the reported latency and
    /// throughput are in the simulated (real-time-equivalent) domain. Values
    /// that are not finite and positive fall back to `1.0`.
    ///
    /// `duration` is the wall-clock run length and is never scaled, so the
    /// benchmark still stops after that much real time.
    pub fn with_time_scale(duration: Duration, time_scale: f64) -> Self {
        let expected = if duration.is_zero() {
            DEFAULT_DURATION
        } else {
            duration
        };
        let time_scale = if time_scale.is_finite() && time_scale > 0.0 {
            time_scale
        } else {
            1.0
        };
        Bench {
            expected_duration: expected,
            time_scale,
            inner: Mutex::new(Inner {
                start_time: None,
                tot_duration: Duration::ZERO,
                samples: Vec::new(),
            }),
        }
    }

    /// Begins the benchmark timer.
    pub fn start(&self) {
        self.inner.lock().unwrap().start_time = Some(Instant::now());
    }

    /// Records the total elapsed time since [`Bench::start`] (time-scaled).
    pub fn end(&self) {
        let mut g = self.inner.lock().unwrap();
        if let Some(start) = g.start_time {
            g.tot_duration = start.elapsed().mul_f64(self.time_scale);
        }
    }

    /// Reports whether the benchmark has run long enough and collected enough
    /// samples.
    pub fn is_finished(&self) -> bool {
        let g = self.inner.lock().unwrap();
        match g.start_time {
            Some(start) if start.elapsed() >= self.expected_duration => {
                g.samples.len() >= MIN_SAMPLES
            }
            _ => false,
        }
    }

    /// Times `f` and records the duration as a sample on success. The
    /// operation's error (if any) is propagated unchanged.
    pub async fn measure<F, Fut, E>(&self, f: F) -> Result<(), E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let start = Instant::now();
        f().await?;
        self.record_raw(start.elapsed());
        Ok(())
    }

    /// Number of samples (successfully timed operations) recorded so far. A
    /// cheap live progress signal for adaptive/sequential stopping, without
    /// cloning the whole sample vector.
    pub fn sample_count(&self) -> usize {
        self.inner.lock().unwrap().samples.len()
    }

    /// Returns a snapshot of the collected results.
    pub fn results(&self) -> Results {
        let g = self.inner.lock().unwrap();
        Results {
            samples: g.samples.clone(),
            tot_duration: g.tot_duration,
        }
    }

    /// Records one raw wall-clock latency sample, applying the time-scale
    /// compensation so the stored value is in the reported (simulated) domain.
    fn record_raw(&self, raw: Duration) {
        self.inner
            .lock()
            .unwrap()
            .samples
            .push(raw.mul_f64(self.time_scale));
    }
}

/// The collected timing samples and total duration of a benchmark run.
#[derive(Debug, Clone, Default)]
pub struct Results {
    pub samples: Vec<Duration>,
    pub tot_duration: Duration,
}

impl Results {
    /// The arithmetic mean of all samples (zero when there are none).
    pub fn avg(&self) -> Duration {
        if self.samples.is_empty() {
            return Duration::ZERO;
        }
        let sum: f64 = self.samples.iter().map(|d| d.as_secs_f64()).sum();
        Duration::from_secs_f64(sum / self.samples.len() as f64)
    }

    /// The sample at the given percentile (0.0..=1.0), using interpolation
    /// method R8 from Hyndman and Fan (1996), matching the Go implementation.
    pub fn percentile(&self, pctile: f64) -> Duration {
        assert!(
            !self.samples.is_empty() && (0.0..=1.0).contains(&pctile),
            "invalid percentile parameters"
        );
        let mut xs: Vec<f64> = self.samples.iter().map(|d| d.as_secs_f64()).collect();
        xs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n_total = xs.len() as f64;
        let n = 1.0 / 3.0 + pctile * (n_total + 1.0 / 3.0);
        let kf = n.floor();
        let frac = n - kf;
        let k = kf as isize;
        let secs = if k <= 0 {
            xs[0]
        } else if k as usize >= xs.len() {
            xs[xs.len() - 1]
        } else {
            xs[k as usize - 1] + frac * (xs[k as usize] - xs[k as usize - 1])
        };
        Duration::from_secs_f64(secs)
    }

    /// Relative half-width of this run's throughput 95% confidence interval,
    /// derived from the sample count (see [`rate_rel_ci`]). Smaller is tighter.
    pub fn rate_rel_ci(&self) -> f64 {
        rate_rel_ci(self.samples.len())
    }
}

/// z for a two-sided 95% confidence interval (the standard-normal quantile).
pub const Z_95: f64 = 1.96;

/// Sample count a rate/throughput estimate needs for its 95% confidence interval
/// to reach `target_rel_ci` relative half-width, under the independent-arrivals
/// (Poisson) approximation `rel-CI ~= z / sqrt(n)`, so `n ~= (z / target_ci)^2`.
///
/// Returns 0 when `target_rel_ci <= 0` (meaning "no target"). Real contention
/// correlates arrivals, so the true interval is a touch wider — this is the
/// standard rate-estimate bound, not an exact guarantee. Enables sequential
/// (adaptive) sampling: run until [`Bench::sample_count`] reaches this value.
pub fn samples_for_rel_ci(target_rel_ci: f64) -> u64 {
    if target_rel_ci > 0.0 {
        (Z_95 / target_rel_ci).powi(2).ceil() as u64
    } else {
        0
    }
}

/// Achieved relative half-width of a rate/throughput 95% confidence interval
/// from `n` samples (`z / sqrt(n)`, the [`samples_for_rel_ci`] inverse). Returns
/// a large finite sentinel for `n == 0` so callers can serialize it.
pub fn rate_rel_ci(n: usize) -> f64 {
    if n == 0 {
        99.0
    } else {
        Z_95 / (n as f64).sqrt()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_endpoints_and_median() {
        let r = Results {
            samples: (1..=10).map(|i| Duration::from_millis(i * 10)).collect(),
            tot_duration: Duration::ZERO,
        };
        // The min/max percentiles clamp to the smallest/largest sample.
        assert_eq!(r.percentile(0.0), Duration::from_millis(10));
        assert_eq!(r.percentile(1.0), Duration::from_millis(100));
        // The median lands inside the sample range.
        let p50 = r.percentile(0.5);
        assert!(p50 >= Duration::from_millis(40) && p50 <= Duration::from_millis(70));
    }

    #[test]
    fn time_scale_multiplies_recorded_latencies() {
        // A 0.02 delay-scale compresses wall-clock 50x, so reporting in the
        // simulated domain multiplies each measured latency by 1/0.02 = 50.
        let b = Bench::with_time_scale(Duration::from_secs(1), 50.0);
        b.record_raw(Duration::from_millis(10));
        b.record_raw(Duration::from_millis(20));
        let r = b.results();
        assert_eq!(
            r.samples,
            vec![Duration::from_millis(500), Duration::from_secs(1)]
        );
    }

    #[test]
    fn new_records_raw_wall_clock() {
        // The default (real-time) bench applies no scaling.
        let b = Bench::new(Duration::from_secs(1));
        b.record_raw(Duration::from_millis(10));
        assert_eq!(b.results().samples, vec![Duration::from_millis(10)]);
    }

    #[test]
    fn non_positive_time_scale_falls_back_to_one() {
        for bad in [0.0, -1.0, f64::NAN, f64::INFINITY] {
            let b = Bench::with_time_scale(Duration::from_secs(1), bad);
            b.record_raw(Duration::from_millis(10));
            assert_eq!(
                b.results().samples,
                vec![Duration::from_millis(10)],
                "time_scale {bad} should fall back to 1.0"
            );
        }
    }

    #[test]
    fn avg_of_known_samples() {
        let r = Results {
            samples: vec![
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(30),
            ],
            tot_duration: Duration::ZERO,
        };
        assert_eq!(r.avg(), Duration::from_millis(20));
    }

    #[test]
    fn samples_for_rel_ci_inverts_rate_rel_ci() {
        // The target count is the smallest n whose achieved CI meets the target.
        for target in [0.05, 0.1, 0.15, 0.2] {
            let n = samples_for_rel_ci(target);
            assert!(n > 0);
            assert!(
                rate_rel_ci(n as usize) <= target,
                "n={n} should meet target={target}, got {}",
                rate_rel_ci(n as usize)
            );
            assert!(
                rate_rel_ci(n as usize - 1) > target,
                "n-1={} should miss target={target}",
                n - 1
            );
        }
    }

    #[test]
    fn samples_for_rel_ci_zero_disables_target() {
        for off in [0.0, -0.1, f64::NAN] {
            assert_eq!(samples_for_rel_ci(off), 0);
        }
    }

    #[test]
    fn rate_rel_ci_of_empty_is_large_and_finite() {
        let ci = rate_rel_ci(0);
        assert!(ci.is_finite() && ci > 1.0);
        assert_eq!(Results::default().rate_rel_ci(), ci);
    }
}
