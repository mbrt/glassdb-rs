//! A [`Backend`] decorator that simulates network latency and per-object write
//! rate limiting. Ported from the Go `middleware.DelayBackend`.

use std::collections::HashMap;
use std::num::{NonZeroU32, NonZeroUsize};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::rt::{self, Instant};
use rand_distr::{Distribution, StandardNormal};

use crate::{
    Backend, BackendError, ListCursor, ListLimit, ListPage, ListRequest, ReadReply, Version,
};

/// Typical latency values observed with Google Cloud Storage.
pub fn gcs_delays() -> DelayOptions {
    DelayOptions {
        latency: ProviderLatencyProfile {
            meta_read: Latency::new(22, 7),
            meta_write: Latency::new(31, 8),
            obj_read: Latency::new(57, 7),
            obj_write: Latency::new(70, 15),
            list: Latency::new(10, 3),
        },
        rate_limits: WriteRateLimits {
            same_obj_write_ps: RateLimit::PerSecond(NonZeroU32::new(1).unwrap()),
            same_obj_write_retry_delay: Duration::from_millis(140),
            // GCS has no documented per-prefix request-rate limit, so the
            // prefix limiter is disabled.
            prefix_read_ps: RateLimit::Unlimited,
            prefix_write_ps: RateLimit::Unlimited,
            prefix_depth: 0,
        },
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
        latency: ProviderLatencyProfile {
            meta_read: Latency::new(21, 9),
            meta_write: Latency::new(75, 19),
            obj_read: Latency::new(22, 9),
            obj_write: Latency::new(55, 18),
            list: Latency::new(22, 8),
        },
        rate_limits: WriteRateLimits {
            same_obj_write_ps: RateLimit::PerSecond(NonZeroU32::new(3500).unwrap()),
            same_obj_write_retry_delay: Duration::from_millis(110),
            prefix_read_ps: RateLimit::PerSecond(NonZeroU32::new(5500).unwrap()),
            prefix_write_ps: RateLimit::PerSecond(NonZeroU32::new(3500).unwrap()),
            prefix_depth: 2,
        },
    }
}

/// The mean and standard deviation of an operation's duration.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Latency {
    pub mean: Duration,
    pub std_dev: Duration,
}

/// Latencies observed for each type of provider operation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ProviderLatencyProfile {
    pub meta_read: Latency,
    pub meta_write: Latency,
    pub obj_read: Latency,
    pub obj_write: Latency,
    pub list: Latency,
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

/// Selects whether an operation is unlimited or capped at a positive rate.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RateLimit {
    /// Does not apply rate limiting.
    Unlimited,
    /// Allows the given positive number of operations per second.
    PerSecond(NonZeroU32),
}

/// Configures provider write and shared-prefix request-rate limits.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WriteRateLimits {
    /// Caps writes to the same object.
    pub same_obj_write_ps: RateLimit,
    /// Delay before retrying when the same-object write limit is exceeded.
    pub same_obj_write_retry_delay: Duration,
    /// Caps the GET/HEAD request rate against a shared key prefix, modeling
    /// S3's documented per-prefix request-rate limit. A request that would
    /// exceed the rate is delayed (not failed) until the bucket refills, so the
    /// cap bounds throughput without inflating transaction-retry counts.
    pub prefix_read_ps: RateLimit,
    /// Caps the PUT/POST/DELETE request rate against a shared key prefix (the
    /// write analog of [`Self::prefix_read_ps`]).
    pub prefix_write_ps: RateLimit,
    /// Selects how many leading `/`-separated path segments form a throttled
    /// prefix, i.e. the partition granularity (depth 1 groups every object
    /// under the database root into a single hot partition; depth 2 throttles
    /// each immediate subtree independently). Ignored when both prefix limits
    /// are unlimited.
    pub prefix_depth: usize,
}

/// Configures simulated provider latency and rate limiting.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DelayOptions {
    pub latency: ProviderLatencyProfile,
    pub rate_limits: WriteRateLimits,
}

/// An invalid combination of delay and rate-limit options.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DelayOptionsError {
    /// A prefix limit was enabled without selecting a prefix.
    #[error("prefix depth must be greater than zero when a prefix rate limit is enabled")]
    ZeroPrefixDepth,
    /// Per-object backoff was enabled without a positive retry delay.
    #[error("object write retry delay must be greater than zero when its rate limit is enabled")]
    ZeroObjectRetryDelay,
}

/// A [`Backend`] decorator that injects simulated network latency, per-object
/// write rate limiting, and per-prefix request-rate ceilings before delegating
/// to the inner backend.
pub struct DelayBackend {
    inner: Arc<dyn Backend>,
    obj_read: Lognormal,
    obj_write: Lognormal,
    list: Lognormal,
    rlimit: Option<RateLimiter>,
    prefix_reads: Option<PrefixLimiter>,
    prefix_writes: Option<PrefixLimiter>,
}

impl DelayBackend {
    /// Wraps `inner`, simulating the latencies described by `opts`.
    ///
    /// The conditional-only trait (ADR-042) has no metadata-only operations, so
    /// `opts.latency.meta_read` / `opts.latency.meta_write` are unused here;
    /// fake provider servers consume them from the shared latency profile.
    pub fn new(inner: Arc<dyn Backend>, opts: DelayOptions) -> Result<Self, DelayOptionsError> {
        let DelayOptions {
            latency,
            rate_limits,
        } = opts;
        let retry_delay = rate_limits.same_obj_write_retry_delay;
        let rlimit = match rate_limits.same_obj_write_ps {
            RateLimit::Unlimited => None,
            RateLimit::PerSecond(rate) => {
                if retry_delay.is_zero() {
                    return Err(DelayOptionsError::ZeroObjectRetryDelay);
                }
                Some(RateLimiter::new(rate, retry_delay))
            }
        };

        Ok(DelayBackend {
            inner,
            obj_read: Lognormal::from_latency(latency.obj_read),
            obj_write: Lognormal::from_latency(latency.obj_write),
            list: Lognormal::from_latency(latency.list),
            rlimit,
            prefix_reads: PrefixLimiter::from_limit(
                rate_limits.prefix_read_ps,
                rate_limits.prefix_depth,
            )?,
            prefix_writes: PrefixLimiter::from_limit(
                rate_limits.prefix_write_ps,
                rate_limits.prefix_depth,
            )?,
        })
    }

    async fn delay(&self, ln: &Lognormal) {
        let ms = ln.rand();
        rt::sleep(secs_f64_or_zero(ms / 1_000.0)).await;
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

    /// Blocks on the object limiter when one is enabled.
    async fn object_write_wait(&self, path: &str) {
        if let Some(l) = &self.rlimit {
            l.wait(path).await;
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

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        self.prefix_write_wait(path).await;
        self.object_write_wait(path).await;
        self.delay(&self.obj_write).await;
        self.inner.write_if(path, value, expected).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        self.prefix_write_wait(path).await;
        self.object_write_wait(path).await;
        self.delay(&self.obj_write).await;
        self.inner.write_if_not_exists(path, value).await
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        self.prefix_write_wait(path).await;
        self.object_write_wait(path).await;
        self.delay(&self.obj_write).await;
        self.inner.delete_if(path, expected).await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        match ListRequest::new(prefix, cursor, limit) {
            Ok(request) => self.list_request(request).await,
            Err(_) => {
                self.prefix_read_wait(prefix).await;
                self.delay(&self.list).await;
                self.inner.list(prefix, cursor, limit).await
            }
        }
    }

    async fn list_request(&self, request: ListRequest<'_>) -> Result<ListPage, BackendError> {
        self.prefix_read_wait(request.prefix()).await;
        self.delay(&self.list).await;
        self.inner.list_request(request).await
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
/// including its use of model monotonic time, so it stays coherent with
/// accelerated latency and deterministic under paused time in tests.
struct RateLimiter {
    tokens_per_sec: i64,
    retry_delay: Duration,
    buckets: Mutex<HashMap<String, BucketState>>,
}

#[derive(Debug, Clone, Copy)]
struct BucketState {
    last_check: Instant,
    tokens: i64,
}

impl RateLimiter {
    fn new(tokens_per_sec: NonZeroU32, retry_delay: Duration) -> Self {
        RateLimiter {
            tokens_per_sec: i64::from(tokens_per_sec.get()),
            retry_delay,
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Blocks until a write token is available for `key`.
    async fn wait(&self, key: &str) {
        let max = self.retry_delay.saturating_mul(10);
        let mut interval = self.retry_delay;
        loop {
            if self.try_acquire_token(key) {
                return;
            }
            rt::sleep(interval).await;
            interval = std::cmp::min(interval.mul_f64(1.5), max);
        }
    }

    fn try_acquire_token(&self, key: &str) -> bool {
        let window = Duration::from_secs(1);
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
/// to the cap. Timekeeping uses model monotonic time, so it stays coherent with
/// accelerated latency and deterministic under paused time.
struct PrefixLimiter {
    /// Tokens added per model second.
    rate: f64,
    /// Bucket capacity, in tokens.
    burst: f64,
    depth: NonZeroUsize,
    buckets: Mutex<HashMap<String, TokenBucket>>,
}

#[derive(Debug, Clone, Copy)]
struct TokenBucket {
    tokens: f64,
    last_fill: Instant,
}

impl PrefixLimiter {
    /// Builds the optional limiter selected by `limit`.
    fn from_limit(
        limit: RateLimit,
        depth: usize,
    ) -> Result<Option<PrefixLimiter>, DelayOptionsError> {
        let RateLimit::PerSecond(rate_per_sec) = limit else {
            return Ok(None);
        };
        let depth = NonZeroUsize::new(depth).ok_or(DelayOptionsError::ZeroPrefixDepth)?;
        let rate = f64::from(rate_per_sec.get());
        Ok(Some(PrefixLimiter {
            rate,
            burst: rate,
            depth,
            buckets: Mutex::new(HashMap::new()),
        }))
    }

    /// Blocks until a request token for `path`'s prefix is available. The
    /// caller cancels by dropping the surrounding future.
    async fn wait(&self, path: &str) {
        let d = self.reserve(prefix_key(path, self.depth.get()), Instant::now());
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

    // Ports the Go middleware `TestRateLimiter`, driving the model-time limiter
    // with paused Tokio time.
    #[tokio::test(start_paused = true)]
    async fn rate_limiter_token_refill() {
        let rl = RateLimiter::new(NonZeroU32::new(1).unwrap(), Duration::from_millis(1));

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

    // Ports the Go middleware `TestPrefixLimiter` at its nominal model-time
    // rate. Process-wide acceleration is covered at the runtime seam.
    #[tokio::test(start_paused = true)]
    async fn prefix_limiter_rate() {
        let l = PrefixLimiter::from_limit(RateLimit::PerSecond(NonZeroU32::new(100).unwrap()), 1)
            .unwrap()
            .expect("limiter enabled");
        let now = Instant::now();
        for _ in 0..100 {
            l.reserve("bench", now);
        }
        assert_eq!(l.reserve("bench", now), Duration::from_millis(10));
    }

    #[test]
    fn prefix_limiter_unlimited() {
        assert!(
            PrefixLimiter::from_limit(RateLimit::Unlimited, 0)
                .unwrap()
                .is_none()
        );
    }

    // Ports the Go middleware `TestPrefixKey`.
    #[test]
    fn prefix_key_segments() {
        let cases = [
            ("bench/_c/abc/_n/def", 1, "bench"),
            ("bench/_c/abc/_n/def", 2, "bench/_c"),
            ("bench/_t/xyz", 3, "bench/_t/xyz"),
            ("bench", 2, "bench"),
            ("a/b", 2, "a/b"),
        ];
        for (path, depth, want) in cases {
            assert_eq!(prefix_key(path, depth), want, "path={path} depth={depth}");
        }
    }
}
