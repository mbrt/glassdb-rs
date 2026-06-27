//! A [`Backend`] decorator that simulates network latency and per-object write
//! rate limiting. Ported from the Go `middleware.DelayBackend`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::rt::{self, Instant};
use rand_distr::{Distribution, StandardNormal};

use crate::{Backend, BackendError, ReadReply, Version};

/// Typical latency values observed with Google Cloud Storage.
pub fn gcs_delays() -> DelayOptions {
    DelayOptions {
        meta_read: Latency::new(22, 7),
        meta_write: Latency::new(31, 8),
        obj_read: Latency::new(57, 7),
        obj_write: Latency::new(70, 15),
        list: Latency::new(10, 3),
        same_obj_write_ps: 1,
        // GCS has no documented per-prefix request-rate limit, so the prefix
        // limiter is disabled.
        prefix_read_ps: 0,
        prefix_write_ps: 0,
        prefix_depth: 0,
        scale: 0.0,
    }
}

/// Typical latency values for Amazon S3 Standard accessed in-region, derived
/// from AWS guidance and public benchmarks (p50 GET ~30 ms, p50 PUT ~70 ms,
/// with a long right tail captured by the lognormal model).
///
/// Unlike GCS, S3 has no per-object write limit; throughput scales per prefix.
/// `same_obj_write_ps` is therefore set high so the per-object limiter never
/// binds, and the per-prefix request-rate limit is modeled separately via
/// `prefix_read_ps` / `prefix_write_ps` / `prefix_depth` (S3 sustains at least
/// 5,500 GET/HEAD and 3,500 PUT/COPY/POST/DELETE requests per second per
/// partitioned prefix before returning `503 SlowDown`).
pub fn s3_delays() -> DelayOptions {
    DelayOptions {
        meta_read: Latency::new(21, 9),
        meta_write: Latency::new(75, 19),
        obj_read: Latency::new(22, 9),
        obj_write: Latency::new(55, 18),
        list: Latency::new(22, 8),
        same_obj_write_ps: 3500,
        prefix_read_ps: 5500,
        prefix_write_ps: 3500,
        prefix_depth: 2,
        scale: 0.0,
    }
}

/// The mean and standard deviation of an operation's duration.
#[derive(Debug, Clone, Copy)]
pub struct Latency {
    pub mean: Duration,
    pub std_dev: Duration,
}

impl Latency {
    /// Builds a [`Latency`] from a mean and standard deviation in milliseconds.
    pub fn new(mean_ms: u64, std_dev_ms: u64) -> Self {
        Latency {
            mean: Duration::from_millis(mean_ms),
            std_dev: Duration::from_millis(std_dev_ms),
        }
    }
}

/// Configures simulated latency for each type of backend operation.
#[derive(Debug, Clone, Copy)]
pub struct DelayOptions {
    pub meta_read: Latency,
    pub meta_write: Latency,
    pub obj_read: Latency,
    pub obj_write: Latency,
    pub list: Latency,
    /// How many writes per second to the same object before being rate limited.
    pub same_obj_write_ps: i64,
    /// Caps the GET/HEAD request rate against a shared key prefix, modeling
    /// S3's documented per-prefix request-rate limit. A request that would
    /// exceed the rate is delayed (not failed) until the bucket refills, so the
    /// cap bounds throughput without inflating transaction-retry counts. Zero
    /// disables the limit.
    pub prefix_read_ps: i64,
    /// Caps the PUT/POST/DELETE request rate against a shared key prefix (the
    /// write analog of [`Self::prefix_read_ps`]). Zero disables the limit.
    pub prefix_write_ps: i64,
    /// Selects how many leading `/`-separated path segments form a throttled
    /// prefix, i.e. the partition granularity (depth 1 groups every object
    /// under the database root into a single hot partition; depth 2 throttles
    /// each immediate subtree independently). Ignored when both prefix rates
    /// are zero.
    pub prefix_depth: usize,
    /// Multiplies all delay durations. A value of `0.0` is treated as `1.0`.
    /// Use values `< 1` to compress delays (e.g. `0.001` for a 1000x speedup).
    pub scale: f64,
}

/// A [`Backend`] decorator that injects simulated network latency, per-object
/// write rate limiting, and per-prefix request-rate ceilings before delegating
/// to the inner backend.
pub struct DelayBackend {
    inner: Arc<dyn Backend>,
    scale: f64,
    obj_read: Lognormal,
    obj_write: Lognormal,
    list: Lognormal,
    rlimit: RateLimiter,
    prefix_reads: Option<PrefixLimiter>,
    prefix_writes: Option<PrefixLimiter>,
    retry_delay: Duration,
}

impl DelayBackend {
    /// Wraps `inner`, simulating the latencies described by `opts`.
    ///
    /// The content-CAS-only trait (ADR-023) has no metadata-only operations, so
    /// `opts.meta_read` / `opts.meta_write` are unused; they remain in
    /// [`DelayOptions`] only for config-shape stability.
    pub fn new(inner: Arc<dyn Backend>, opts: DelayOptions) -> Self {
        let scale = if opts.scale == 0.0 { 1.0 } else { opts.scale };
        DelayBackend {
            inner,
            scale,
            obj_read: Lognormal::from_latency(opts.obj_read),
            obj_write: Lognormal::from_latency(opts.obj_write),
            list: Lognormal::from_latency(opts.list),
            rlimit: RateLimiter::new(opts.same_obj_write_ps, scale),
            prefix_reads: PrefixLimiter::new(opts.prefix_read_ps, opts.prefix_depth, scale),
            prefix_writes: PrefixLimiter::new(opts.prefix_write_ps, opts.prefix_depth, scale),
            retry_delay: secs_f64_or_zero(opts.obj_write.mean.as_secs_f64() * 2.0 * scale),
        }
    }

    async fn delay(&self, ln: &Lognormal) {
        let ms = ln.rand();
        rt::sleep(secs_f64_or_zero(ms * self.scale / 1_000.0)).await;
    }

    /// Blocks on the read prefix limiter (a no-op when it is disabled).
    async fn prefix_read_wait(&self, path: &str) {
        if let Some(l) = &self.prefix_reads {
            l.wait(path).await;
        }
    }

    /// Blocks on the write prefix limiter (a no-op when it is disabled).
    async fn prefix_write_wait(&self, path: &str) {
        if let Some(l) = &self.prefix_writes {
            l.wait(path).await;
        }
    }

    /// Blocks until a write token is available for `path`, retrying with
    /// backoff. Returns when a token is acquired; the caller cancels by
    /// dropping the surrounding future.
    async fn backoff(&self, path: &str) {
        let max = self.retry_delay.saturating_mul(10);
        let mut interval = self.retry_delay;
        loop {
            if self.rlimit.try_acquire_token(path) {
                return;
            }
            rt::sleep(interval).await;
            interval = std::cmp::min(interval.mul_f64(1.5), max);
        }
    }
}

#[async_trait]
impl Backend for DelayBackend {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.prefix_read_wait(path).await;
        self.delay(&self.obj_read).await;
        self.inner.read(path).await
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        self.prefix_read_wait(path).await;
        self.delay(&self.obj_read).await;
        self.inner.read_if_modified(path, expected).await
    }

    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError> {
        self.prefix_write_wait(path).await;
        self.backoff(path).await;
        self.delay(&self.obj_write).await;
        self.inner.write(path, value).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        self.prefix_write_wait(path).await;
        self.backoff(path).await;
        self.delay(&self.obj_write).await;
        self.inner.write_if(path, value, expected).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        self.prefix_write_wait(path).await;
        self.backoff(path).await;
        self.delay(&self.obj_write).await;
        self.inner.write_if_not_exists(path, value).await
    }

    async fn delete(&self, path: &str) -> Result<(), BackendError> {
        self.prefix_write_wait(path).await;
        self.backoff(path).await;
        self.delay(&self.obj_write).await;
        self.inner.delete(path).await
    }

    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.prefix_read_wait(dir_path).await;
        self.delay(&self.list).await;
        self.inner.list(dir_path).await
    }
}

/// A lognormal distribution over operation durations, in milliseconds.
#[derive(Debug, Clone, Copy)]
struct Lognormal {
    mu: f64,
    sigma: f64,
}

impl Lognormal {
    /// Derives the lognormal parameters from a desired mean and standard
    /// deviation (https://stats.stackexchange.com/a/95506).
    fn from_latency(l: Latency) -> Self {
        let mean = l.mean.as_secs_f64() * 1_000.0;
        let std_dev = l.std_dev.as_secs_f64() * 1_000.0;
        if mean <= 0.0 {
            // A zero mean has no meaningful lognormal; yield a zero delay.
            return Lognormal {
                mu: f64::NEG_INFINITY,
                sigma: 0.0,
            };
        }
        let s_by_m = std_dev / mean;
        let v = (s_by_m * s_by_m + 1.0).ln();
        Lognormal {
            mu: mean.ln() - 0.5 * v,
            sigma: v.sqrt(),
        }
    }

    /// Samples a duration in milliseconds.
    fn rand(&self) -> f64 {
        let n: f64 = StandardNormal.sample(&mut rand::rng());
        (n * self.sigma + self.mu).exp()
    }
}

/// A per-object token-bucket rate limiter. Mirrors the Go `rateLimiter`,
/// including its use of wall-clock time — here `tokio::time::Instant`, so it
/// stays deterministic under paused time in tests.
struct RateLimiter {
    tokens_per_sec: i64,
    scale: f64,
    buckets: Mutex<HashMap<String, BucketState>>,
}

#[derive(Debug, Clone, Copy)]
struct BucketState {
    last_check: Instant,
    tokens: i64,
}

impl RateLimiter {
    fn new(tokens_per_sec: i64, scale: f64) -> Self {
        RateLimiter {
            tokens_per_sec,
            scale,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    fn try_acquire_token(&self, key: &str) -> bool {
        if self.tokens_per_sec == 0 {
            return false;
        }
        let window = Duration::from_secs_f64(self.scale);
        let now = Instant::now();
        let mut buckets = self.buckets.lock().unwrap();
        let Some(entry) = buckets.get(key).copied() else {
            buckets.insert(
                key.to_string(),
                BucketState {
                    last_check: now,
                    tokens: self.tokens_per_sec - 1,
                },
            );
            return true;
        };
        let elapsed = now.duration_since(entry.last_check);
        if elapsed >= window {
            let refilled =
                (elapsed.as_secs_f64() / window.as_secs_f64() * self.tokens_per_sec as f64) as i64;
            let new_tokens = (entry.tokens + refilled).min(self.tokens_per_sec);
            if new_tokens <= 0 {
                return false;
            }
            buckets.insert(
                key.to_string(),
                BucketState {
                    last_check: now,
                    tokens: new_tokens - 1,
                },
            );
            return true;
        }
        buckets.insert(
            key.to_string(),
            BucketState {
                last_check: entry.last_check,
                tokens: entry.tokens - 1,
            },
        );
        true
    }
}

/// A per-prefix request-rate limiter using a continuous token bucket per
/// prefix. Mirrors the Go `prefixLimiter`. Unlike [`RateLimiter`] (tuned for
/// infrequent per-object writes), it behaves correctly under thousands of
/// concurrent acquisitions per second: callers that exceed the rate are told
/// how long to wait, and that debt accumulates so the long-run rate converges
/// to the cap. Timekeeping uses `tokio::time::Instant`, so it stays
/// deterministic under paused time.
struct PrefixLimiter {
    /// Tokens added per wall-clock second.
    rate: f64,
    /// Bucket capacity, in tokens.
    burst: f64,
    depth: usize,
    buckets: Mutex<HashMap<String, TokenBucket>>,
}

#[derive(Debug, Clone, Copy)]
struct TokenBucket {
    tokens: f64,
    last_fill: Instant,
}

impl PrefixLimiter {
    /// Builds a per-prefix limiter, or `None` (no throttling) when
    /// `rate_per_sec` is non-positive. `depth` selects how many leading path
    /// segments form the throttled prefix.
    fn new(rate_per_sec: i64, depth: usize, scale: f64) -> Option<PrefixLimiter> {
        if rate_per_sec <= 0 {
            return None;
        }
        // Delays are sleep-scaled by `scale` (see `delay`), so a sub-unit scale
        // compresses wall-clock time; the request rate must grow by the same
        // factor to keep the simulated rate constant.
        Some(PrefixLimiter {
            rate: rate_per_sec as f64 / scale,
            burst: rate_per_sec as f64,
            depth: depth.max(1),
            buckets: Mutex::new(HashMap::new()),
        })
    }

    /// Blocks until a request token for `path`'s prefix is available. The
    /// caller cancels by dropping the surrounding future.
    async fn wait(&self, path: &str) {
        let d = self.reserve(prefix_key(path, self.depth), Instant::now());
        if d.is_zero() {
            return;
        }
        rt::sleep(d).await;
    }

    /// Takes a token for `key` and returns how long the caller must wait before
    /// the request may proceed (zero if a token was immediately available).
    fn reserve(&self, key: &str, now: Instant) -> Duration {
        let mut buckets = self.buckets.lock().unwrap();
        let b = buckets.entry(key.to_string()).or_insert(TokenBucket {
            tokens: self.burst,
            last_fill: now,
        });
        let elapsed = now.saturating_duration_since(b.last_fill).as_secs_f64();
        if elapsed > 0.0 {
            b.tokens = self.burst.min(b.tokens + elapsed * self.rate);
            b.last_fill = now;
        }
        b.tokens -= 1.0;
        if b.tokens >= 0.0 {
            return Duration::ZERO;
        }
        // Negative tokens represent queued demand: wait for them to refill.
        secs_f64_or_zero(-b.tokens / self.rate)
    }
}

/// Returns the first `depth` `/`-separated segments of `path`, which defines
/// the granularity at which the request-rate ceiling is applied.
fn prefix_key(path: &str, depth: usize) -> &str {
    let bytes = path.as_bytes();
    let mut idx = 0;
    for _ in 0..depth {
        match bytes[idx..].iter().position(|&c| c == b'/') {
            Some(rel) => idx += rel + 1,
            None => return path,
        }
    }
    &path[..idx - 1]
}

/// Builds a [`Duration`] from a fractional number of seconds, clamping
/// non-finite or negative inputs to zero (so a degenerate latency never
/// panics `Duration::from_secs_f64`).
fn secs_f64_or_zero(secs: f64) -> Duration {
    if secs.is_finite() && secs > 0.0 {
        Duration::from_secs_f64(secs)
    } else {
        Duration::ZERO
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Advances tokio's (paused) virtual clock.
    async fn advance(d: Duration) {
        tokio::time::advance(d).await;
    }

    // Ports the Go middleware `TestRateLimiter`, driving the limiter with
    // paused tokio time. Because the limiter reads `tokio::time::Instant`,
    // advancing the runtime clock moves its refill window.
    #[tokio::test(start_paused = true)]
    async fn rate_limiter_token_refill() {
        let rl = RateLimiter::new(1, 1.0);

        // Four requests sneak through within the first second (tokens go
        // negative because the window has not elapsed).
        assert!(rl.try_acquire_token("k"));
        advance(Duration::from_millis(100)).await;
        assert!(rl.try_acquire_token("k"));
        advance(Duration::from_millis(100)).await;
        assert!(rl.try_acquire_token("k"));
        advance(Duration::from_millis(700)).await;
        assert!(rl.try_acquire_token("k"));
        advance(Duration::from_millis(150)).await;

        // ~1050ms elapsed with 3 extra sneaked in, so we are rejected for
        // roughly the next 4 seconds.
        let mut elapsed = Duration::from_millis(1050);
        while elapsed < Duration::from_secs(4) {
            assert!(!rl.try_acquire_token("k"), "elapsed: {elapsed:?}");
            advance(Duration::from_millis(250)).await;
            elapsed += Duration::from_millis(250);
        }

        // The bucket has recovered enough to sneak in five more.
        for i in 0..5 {
            assert!(rl.try_acquire_token("k"), "i: {i}");
        }

        advance(Duration::from_secs(1)).await;
        // And now we are blocked again for the next few seconds.
        let mut elapsed = Duration::ZERO;
        while elapsed < Duration::from_secs(4) {
            assert!(!rl.try_acquire_token("k"), "elapsed: {elapsed:?}");
            advance(Duration::from_millis(250)).await;
            elapsed += Duration::from_millis(250);
        }
        assert!(rl.try_acquire_token("k"));
    }

    #[tokio::test]
    async fn rate_limiter_disabled_when_zero() {
        let rl = RateLimiter::new(0, 1.0);
        assert!(!rl.try_acquire_token("k"));
    }

    // Ports the Go middleware `TestPrefixLimiterScale`. Compressing time by
    // 1000x must raise the wall-clock rate by 1000x so the simulated rate is
    // unchanged: the post-burst wait shrinks from 10ms to 10us.
    #[tokio::test(start_paused = true)]
    async fn prefix_limiter_scale() {
        let l = PrefixLimiter::new(100, 1, 1.0 / 1000.0).expect("limiter enabled");
        let now = Instant::now();
        for _ in 0..100 {
            l.reserve("bench", now);
        }
        assert_eq!(l.reserve("bench", now), Duration::from_micros(10));
    }

    #[test]
    fn prefix_limiter_disabled_when_non_positive() {
        assert!(PrefixLimiter::new(0, 2, 1.0).is_none());
        assert!(PrefixLimiter::new(-1, 2, 1.0).is_none());
    }

    // Ports the Go middleware `TestPrefixKey`.
    #[test]
    fn prefix_key_segments() {
        let cases = [
            ("bench/_c/abc/_k/def", 1, "bench"),
            ("bench/_c/abc/_k/def", 2, "bench/_c"),
            ("bench/_t/xyz", 3, "bench/_t/xyz"),
            ("bench", 2, "bench"),
            ("a/b", 2, "a/b"),
        ];
        for (path, depth, want) in cases {
            assert_eq!(prefix_key(path, depth), want, "path={path} depth={depth}");
        }
    }
}
