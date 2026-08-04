//! Timing-sample collection and analysis, ported from the Go
//! `internal/testkit/bench` package.
//!
//! [`Bench`] accumulates per-operation latency samples over a configurable
//! duration; [`Results`] computes the mean and percentiles (using the same R8
//! interpolation method as the Go code, so the numbers line up).
//!
//! Latency samples and throughput duration use process-wide model time. The
//! stopping window deliberately uses unscaled wall time so acceleration changes
//! neither experiment length nor the minimum sample requirement.

use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant as WallInstant};

use glassdb::Error;
use glassdb_concurr::rt;

const DEFAULT_DURATION: Duration = Duration::from_secs(10);
const MIN_SAMPLES: usize = 10;

/// Tracks timing samples over a configurable duration for benchmarking. Shared
/// across concurrent workers, so all methods take `&self`.
pub struct Bench {
    expected_duration: Duration,
    inner: Mutex<Inner>,
}

struct Inner {
    wall_start: Option<WallInstant>,
    model_start: Option<rt::Instant>,
    tot_duration: Duration,
    samples: Vec<Duration>,
}

impl Bench {
    /// Creates a benchmark that runs for `duration` of wall time (or the 10s
    /// default when zero) and reports model-time latency and throughput.
    pub fn new(duration: Duration) -> Self {
        let expected = if duration.is_zero() {
            DEFAULT_DURATION
        } else {
            duration
        };
        Bench {
            expected_duration: expected,
            inner: Mutex::new(Inner {
                wall_start: None,
                model_start: None,
                tot_duration: Duration::ZERO,
                samples: Vec::new(),
            }),
        }
    }

    /// Begins the benchmark timer.
    pub fn start(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.wall_start = Some(WallInstant::now());
        inner.model_start = Some(rt::Instant::now());
    }

    /// Records the model time elapsed since [`Bench::start`].
    pub fn end(&self) {
        let mut g = self.inner.lock().unwrap();
        if let Some(start) = g.model_start {
            g.tot_duration = start.elapsed();
        }
    }

    /// Reports whether the benchmark has run long enough and collected enough
    /// samples.
    pub fn is_finished(&self) -> bool {
        let g = self.inner.lock().unwrap();
        match g.wall_start {
            Some(start) if start.elapsed() >= self.expected_duration => {
                g.samples.len() >= MIN_SAMPLES
            }
            _ => false,
        }
    }

    /// Times one logical GlassDB operation and records it on success.
    ///
    /// An unknown transaction outcome is replayed as part of the same sample,
    /// so the latency includes every attempt. This benchmark-only policy can
    /// double-apply a non-idempotent mutation and must not be copied into
    /// application code. Every definitive error is propagated unchanged.
    pub async fn measure<F, Fut>(&self, mut f: F) -> Result<(), Error>
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Result<(), Error>>,
    {
        let start = rt::Instant::now();
        loop {
            match f().await {
                Ok(()) => {
                    self.record(start.elapsed());
                    return Ok(());
                }
                Err(Error::InDoubt(reason)) => {
                    eprintln!(
                        "WARNING: measured transaction outcome is in doubt; retrying: {reason}"
                    );
                    rt::yield_now().await;
                }
                Err(err) => return Err(err),
            }
        }
    }

    /// Times one non-transactional operation and records it on success.
    pub async fn measure_once<F, Fut, E>(&self, f: F) -> Result<(), E>
    where
        F: FnOnce() -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        let start = rt::Instant::now();
        f().await?;
        self.record(start.elapsed());
        Ok(())
    }

    /// Number of samples (successfully timed operations) recorded so far. A
    /// cheap live progress signal for adaptive/sequential stopping, without
    /// cloning the whole sample vector.
    pub fn sample_count(&self) -> usize {
        self.inner.lock().unwrap().samples.len()
    }

    /// Returns the configured wall-clock measurement duration.
    pub fn expected_duration(&self) -> Duration {
        self.expected_duration
    }

    /// Returns a snapshot of the collected results.
    pub fn results(&self) -> Results {
        let g = self.inner.lock().unwrap();
        Results {
            samples: g.samples.clone(),
            tot_duration: g.tot_duration,
        }
    }

    /// Records one model-time latency sample.
    fn record(&self, duration: Duration) {
        self.inner.lock().unwrap().samples.push(duration);
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
    fn records_model_time_samples() {
        let b = Bench::new(Duration::from_secs(1));
        b.record(Duration::from_millis(10));
        b.record(Duration::from_millis(20));
        let r = b.results();
        assert_eq!(
            r.samples,
            vec![Duration::from_millis(10), Duration::from_millis(20)]
        );
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

    #[tokio::test]
    async fn in_doubt_replay_is_one_logical_sample() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = AtomicUsize::new(0);
        let bench = Bench::new(Duration::from_secs(1));
        bench.start();

        bench
            .measure(|| {
                let attempt = attempts.fetch_add(1, Ordering::Relaxed);
                async move {
                    if attempt == 0 {
                        Err(Error::InDoubt("lost acknowledgement".into()))
                    } else {
                        Ok(())
                    }
                }
            })
            .await
            .unwrap();

        assert_eq!(attempts.load(Ordering::Relaxed), 2);
        assert_eq!(bench.sample_count(), 1);
    }

    #[tokio::test]
    async fn definitive_error_is_not_replayed() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        let attempts = AtomicUsize::new(0);
        let bench = Bench::new(Duration::from_secs(1));
        let err = bench
            .measure(|| {
                attempts.fetch_add(1, Ordering::Relaxed);
                async { Err(Error::NotFound) }
            })
            .await
            .unwrap_err();

        assert!(matches!(err, Error::NotFound));
        assert_eq!(attempts.load(Ordering::Relaxed), 1);
        assert_eq!(bench.sample_count(), 0);
    }
}
