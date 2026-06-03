//! A [`Backend`] decorator that simulates network latency and per-object write
//! rate limiting. Ported from the Go `middleware.DelayBackend`.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use glassdb_concurr::Ctx;
use rand_distr::{Distribution, StandardNormal};
use tokio::time::Instant;

use crate::{Backend, BackendError, Metadata, ReadReply, Tags, Version, WriterId};

/// Typical latency values observed with Google Cloud Storage.
pub fn gcs_delays() -> DelayOptions {
    DelayOptions {
        meta_read: Latency::new(22, 7),
        meta_write: Latency::new(31, 8),
        obj_read: Latency::new(57, 7),
        obj_write: Latency::new(70, 15),
        list: Latency::new(10, 3),
        same_obj_write_ps: 1,
        scale: 0.0,
    }
}

/// Typical latency values for Amazon S3 Standard accessed in-region.
///
/// `meta_write` is higher than the other backends because S3 has no
/// metadata-only update: `set_tags_if` re-uploads the object (a GET followed by
/// a PUT). Unlike GCS, S3 has no per-object write limit; throughput scales per
/// prefix, so `same_obj_write_ps` is set to the documented per-prefix PUT rate
/// rather than 1, which effectively removes the per-object write bottleneck.
pub fn s3_delays() -> DelayOptions {
    DelayOptions {
        meta_read: Latency::new(28, 12),
        meta_write: Latency::new(100, 25),
        obj_read: Latency::new(30, 12),
        obj_write: Latency::new(74, 25),
        list: Latency::new(30, 10),
        same_obj_write_ps: 3500,
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
    /// Multiplies all delay durations. A value of `0.0` is treated as `1.0`.
    /// Use values `< 1` to compress delays (e.g. `0.001` for a 1000x speedup).
    pub scale: f64,
}

/// A [`Backend`] decorator that injects simulated network latency and
/// per-object write rate limiting before delegating to the inner backend.
pub struct DelayBackend {
    inner: Arc<dyn Backend>,
    scale: f64,
    meta_read: Lognormal,
    meta_write: Lognormal,
    obj_read: Lognormal,
    obj_write: Lognormal,
    list: Lognormal,
    rlimit: RateLimiter,
    retry_delay: Duration,
}

impl DelayBackend {
    /// Wraps `inner`, simulating the latencies described by `opts`.
    pub fn new(inner: Arc<dyn Backend>, opts: DelayOptions) -> Self {
        let scale = if opts.scale == 0.0 { 1.0 } else { opts.scale };
        DelayBackend {
            inner,
            scale,
            meta_read: Lognormal::from_latency(opts.meta_read),
            meta_write: Lognormal::from_latency(opts.meta_write),
            obj_read: Lognormal::from_latency(opts.obj_read),
            obj_write: Lognormal::from_latency(opts.obj_write),
            list: Lognormal::from_latency(opts.list),
            rlimit: RateLimiter::new(opts.same_obj_write_ps, scale),
            retry_delay: secs_f64_or_zero(opts.obj_write.mean.as_secs_f64() * 2.0 * scale),
        }
    }

    async fn delay(&self, ln: &Lognormal) {
        let ms = ln.rand();
        tokio::time::sleep(secs_f64_or_zero(ms * self.scale / 1_000.0)).await;
    }

    /// Blocks until a write token is available for `path`, retrying with
    /// backoff. Returns [`BackendError::Cancelled`] if `ctx` is cancelled.
    async fn backoff(&self, ctx: &Ctx, path: &str) -> Result<(), BackendError> {
        let max = self.retry_delay.saturating_mul(10);
        let mut interval = self.retry_delay;
        loop {
            if self.rlimit.try_acquire_token(path) {
                return Ok(());
            }
            tokio::select! {
                _ = ctx.cancelled() => return Err(BackendError::Cancelled),
                _ = tokio::time::sleep(interval) => {}
            }
            interval = std::cmp::min(interval.mul_f64(1.5), max);
        }
    }
}

#[async_trait]
impl Backend for DelayBackend {
    async fn read_if_modified(
        &self,
        ctx: &Ctx,
        path: &str,
        expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError> {
        self.delay(&self.obj_read).await;
        self.inner
            .read_if_modified(ctx, path, expected_writer)
            .await
    }

    async fn read(&self, ctx: &Ctx, path: &str) -> Result<ReadReply, BackendError> {
        self.delay(&self.obj_read).await;
        self.inner.read(ctx, path).await
    }

    async fn get_metadata(&self, ctx: &Ctx, path: &str) -> Result<Metadata, BackendError> {
        self.delay(&self.meta_read).await;
        self.inner.get_metadata(ctx, path).await
    }

    async fn set_tags_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.backoff(ctx, path).await?;
        self.delay(&self.meta_write).await;
        self.inner.set_tags_if(ctx, path, expected, tags).await
    }

    async fn write(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.backoff(ctx, path).await?;
        self.delay(&self.obj_write).await;
        self.inner.write(ctx, path, value, tags).await
    }

    async fn write_if(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.backoff(ctx, path).await?;
        self.delay(&self.obj_write).await;
        self.inner.write_if(ctx, path, value, expected, tags).await
    }

    async fn write_if_not_exists(
        &self,
        ctx: &Ctx,
        path: &str,
        value: Vec<u8>,
        tags: Tags,
    ) -> Result<Metadata, BackendError> {
        self.backoff(ctx, path).await?;
        self.delay(&self.obj_write).await;
        self.inner.write_if_not_exists(ctx, path, value, tags).await
    }

    async fn delete(&self, ctx: &Ctx, path: &str) -> Result<(), BackendError> {
        self.backoff(ctx, path).await?;
        self.delay(&self.obj_write).await;
        self.inner.delete(ctx, path).await
    }

    async fn delete_if(
        &self,
        ctx: &Ctx,
        path: &str,
        expected: &Version,
    ) -> Result<(), BackendError> {
        self.backoff(ctx, path).await?;
        self.delay(&self.obj_write).await;
        self.inner.delete_if(ctx, path, expected).await
    }

    async fn list(&self, ctx: &Ctx, dir_path: &str) -> Result<Vec<String>, BackendError> {
        self.delay(&self.list).await;
        self.inner.list(ctx, dir_path).await
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

    // Advances the virtual clock. Real tokio's `time::advance` is async; under
    // the madsim simulator it is synchronous. This helper hides the difference
    // so the test body reads the same in both configurations.
    async fn advance(d: Duration) {
        #[cfg(madsim)]
        tokio::time::advance(d);
        #[cfg(not(madsim))]
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
}
