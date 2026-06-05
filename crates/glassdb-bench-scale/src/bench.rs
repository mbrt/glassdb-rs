//! Timing-sample collection and analysis, ported from the Go
//! `internal/testkit/bench` package.
//!
//! [`Bench`] accumulates per-operation latency samples over a configurable
//! duration; [`Results`] computes the mean and percentiles (using the same R8
//! interpolation method as the Go code, so the numbers line up).

use std::future::Future;
use std::sync::Mutex;
use std::time::{Duration, Instant};

const DEFAULT_DURATION: Duration = Duration::from_secs(10);
const MIN_SAMPLES: usize = 10;

/// Tracks timing samples over a configurable duration for benchmarking. Shared
/// across concurrent workers, so all methods take `&self`.
pub struct Bench {
    expected_duration: Duration,
    inner: Mutex<Inner>,
}

struct Inner {
    start_time: Option<Instant>,
    tot_duration: Duration,
    samples: Vec<Duration>,
}

impl Bench {
    /// Creates a benchmark that runs for `duration` (or the 10s default when
    /// `duration` is zero).
    pub fn new(duration: Duration) -> Self {
        let expected = if duration.is_zero() {
            DEFAULT_DURATION
        } else {
            duration
        };
        Bench {
            expected_duration: expected,
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

    /// Records the total elapsed time since [`Bench::start`].
    pub fn end(&self) {
        let mut g = self.inner.lock().unwrap();
        if let Some(start) = g.start_time {
            g.tot_duration = start.elapsed();
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
        let d = start.elapsed();
        self.inner.lock().unwrap().samples.push(d);
        Ok(())
    }

    /// Returns a snapshot of the collected results.
    pub fn results(&self) -> Results {
        let g = self.inner.lock().unwrap();
        Results {
            samples: g.samples.clone(),
            tot_duration: g.tot_duration,
        }
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
}
