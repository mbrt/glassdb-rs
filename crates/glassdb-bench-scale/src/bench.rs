//! Timing-sample collection and analysis, ported from the Go
//! `internal/testkit/bench` package.
//!
//! [`Bench`] accumulates per-operation latency samples over a configurable
//! duration; [`Results`] computes the mean and percentiles (using the same R8
//! interpolation method as the Go code, so the numbers line up).
//!
//! Latency samples, throughput duration, and the stopping window use
//! process-wide model time, so a run measures the same model time at each
//! speedup.

use std::future::Future;
use std::sync::Mutex;
use std::time::Duration;

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
    start: Option<rt::Instant>,
    tot_duration: Duration,
    samples: Vec<Duration>,
    completions: Vec<Duration>,
}

impl Bench {
    /// Creates a benchmark that runs for `duration` of model time (or the 10s
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
                start: None,
                tot_duration: Duration::ZERO,
                samples: Vec::new(),
                completions: Vec::new(),
            }),
        }
    }

    /// Begins the benchmark timer.
    pub fn start(&self) {
        let mut inner = self.inner.lock().unwrap();
        inner.start = Some(rt::Instant::now());
    }

    /// Records the model time elapsed since [`Bench::start`].
    pub fn end(&self) {
        let mut g = self.inner.lock().unwrap();
        if let Some(start) = g.start {
            g.tot_duration = start.elapsed();
        }
    }

    /// Reports whether the benchmark has run long enough and collected enough
    /// samples.
    pub fn is_finished(&self) -> bool {
        let g = self.inner.lock().unwrap();
        match g.start {
            Some(start) if start.elapsed() >= self.expected_duration => {
                g.samples.len() >= MIN_SAMPLES
            }
            _ => false,
        }
    }

    /// Times one logical GlassDB operation and records it on success.
    ///
    /// An in-doubt transaction outcome is retried as part of the same sample,
    /// so the latency includes every retry. This benchmark-only policy can
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

    /// Returns the configured model-time measurement duration.
    pub fn expected_duration(&self) -> Duration {
        self.expected_duration
    }

    /// Returns a snapshot of the collected results.
    pub fn results(&self) -> Results {
        let g = self.inner.lock().unwrap();
        Results {
            samples: g.samples.clone(),
            completions: g.completions.clone(),
            tot_duration: g.tot_duration,
        }
    }

    /// Records one model-time latency sample, unless the benchmark has not
    /// started. Operations can run before [`Bench::start`] to warm up the
    /// system without becoming part of the results.
    fn record(&self, latency: Duration) {
        let mut inner = self.inner.lock().unwrap();
        if let Some(start) = inner.start {
            inner.samples.push(latency);
            inner.completions.push(start.elapsed());
        }
    }
}

/// The collected timing samples, completion times, and total duration of a
/// benchmark run.
#[derive(Debug, Clone, Default)]
pub struct Results {
    pub samples: Vec<Duration>,
    /// Model time from [`Bench::start`] to the end of each sample, in
    /// completion order.
    pub completions: Vec<Duration>,
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

    /// Adds the samples of `other` as if its run continued this one.
    pub fn append(&mut self, other: Results) {
        let offset = self.tot_duration;
        self.samples.extend(other.samples);
        self.completions
            .extend(other.completions.into_iter().map(|at| at + offset));
        self.tot_duration += other.tot_duration;
    }

    /// Counts the samples that complete in each of `windows` equal parts of
    /// the benchmark's duration.
    ///
    /// Samples that complete after [`Bench::end`] are in no window. Returns no
    /// windows when the duration is zero.
    pub fn window_counts(&self, windows: usize) -> Vec<u64> {
        if windows == 0 || self.tot_duration.is_zero() {
            return Vec::new();
        }
        let mut counts = vec![0; windows];
        let width = self.tot_duration.as_secs_f64() / windows as f64;
        for &completion in &self.completions {
            if completion >= self.tot_duration {
                continue;
            }
            // Rounding can put the last instant just past the final window.
            let index = (completion.as_secs_f64() / width) as usize;
            counts[index.min(windows - 1)] += 1;
        }
        counts
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percentile_endpoints_and_median() {
        let r = Results {
            samples: (1..=10).map(|i| Duration::from_millis(i * 10)).collect(),
            ..Default::default()
        };
        // The min/max percentiles clamp to the smallest/largest sample.
        assert_eq!(r.percentile(0.0), Duration::from_millis(10));
        assert_eq!(r.percentile(1.0), Duration::from_millis(100));
        // The median lands inside the sample range.
        let p50 = r.percentile(0.5);
        assert!(p50 >= Duration::from_millis(40) && p50 <= Duration::from_millis(70));
    }

    #[tokio::test]
    async fn records_only_operations_that_end_after_start() {
        let bench = Bench::new(Duration::from_secs(1));
        let ok = || async { Ok::<_, ()>(()) };
        bench.measure_once(ok).await.unwrap();
        bench.start();
        bench.measure_once(ok).await.unwrap();
        assert_eq!(bench.sample_count(), 1);
    }

    #[test]
    fn avg_of_known_samples() {
        let r = Results {
            samples: vec![
                Duration::from_millis(10),
                Duration::from_millis(20),
                Duration::from_millis(30),
            ],
            ..Default::default()
        };
        assert_eq!(r.avg(), Duration::from_millis(20));
    }

    #[test]
    fn window_counts_split_the_duration_into_equal_parts() {
        let r = Results {
            completions: [0, 249, 250, 650, 999, 1000, 1200]
                .map(Duration::from_millis)
                .to_vec(),
            tot_duration: Duration::from_secs(1),
            ..Default::default()
        };
        // Windows of 250 ms. Completions at and after the end of the
        // benchmark are in no window.
        assert_eq!(r.window_counts(4), [2, 1, 1, 1]);
        assert_eq!(r.window_counts(0), [] as [u64; 0]);
        assert_eq!(Results::default().window_counts(4), [] as [u64; 0]);
    }

    #[tokio::test(start_paused = true)]
    async fn completions_are_offsets_from_the_start_and_end_bounds_the_windows() {
        let bench = Bench::new(Duration::from_secs(1));
        let sleep = |millis| {
            move || async move {
                rt::sleep(Duration::from_millis(millis)).await;
                Ok::<_, ()>(())
            }
        };
        bench.measure_once(sleep(1000)).await.unwrap();
        bench.start();
        bench.measure_once(sleep(100)).await.unwrap();
        bench.measure_once(sleep(300)).await.unwrap();
        bench.end();
        // A transaction that completes while the workers drain.
        bench.measure_once(sleep(50)).await.unwrap();

        let results = bench.results();
        assert_eq!(
            results.completions,
            [100, 400, 450].map(Duration::from_millis)
        );
        assert_eq!(results.tot_duration, Duration::from_millis(400));
        assert_eq!(results.samples.len(), 3);
        assert_eq!(results.window_counts(2), [1, 0]);
    }

    #[test]
    fn appended_results_continue_the_timeline() {
        let run = |completions: [u64; 2], millis| Results {
            samples: vec![Duration::from_millis(10); 2],
            completions: completions.map(Duration::from_millis).to_vec(),
            tot_duration: Duration::from_millis(millis),
        };
        let mut results = run([100, 900], 1000);
        results.append(run([0, 400], 1000));

        assert_eq!(results.samples.len(), 4);
        assert_eq!(results.tot_duration, Duration::from_secs(2));
        assert_eq!(results.window_counts(2), [2, 2]);
        assert_eq!(results.window_counts(4), [1, 1, 2, 0]);
    }

    #[tokio::test]
    async fn in_doubt_retry_is_one_logical_sample() {
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
    async fn definitive_error_is_not_retried() {
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
