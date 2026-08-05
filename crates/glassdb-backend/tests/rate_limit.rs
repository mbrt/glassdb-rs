#![cfg(not(sim))]

use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::Duration;

use glassdb_backend::Backend;
use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::middleware::{
    DelayBackend, DelayOptions, DelayOptionsError, Latency, ProviderLatencyProfile, RateLimit,
    WriteRateLimits,
};

/// Creates a positive per-second rate for test configurations.
fn per_second(rate: u32) -> RateLimit {
    RateLimit::PerSecond(NonZeroU32::new(rate).unwrap())
}

/// Returns a zero-latency configuration with every limiter disabled.
fn unlimited_options() -> DelayOptions {
    DelayOptions {
        latency: ProviderLatencyProfile {
            meta_read: Latency::new(0, 0),
            meta_write: Latency::new(0, 0),
            obj_read: Latency::new(0, 0),
            obj_write: Latency::new(0, 0),
            list: Latency::new(0, 0),
        },
        rate_limits: WriteRateLimits {
            same_obj_write_ps: RateLimit::Unlimited,
            same_obj_write_retry_delay: Duration::ZERO,
            prefix_read_ps: RateLimit::Unlimited,
            prefix_write_ps: RateLimit::Unlimited,
            prefix_depth: 0,
        },
    }
}

#[test]
fn unlimited_limits_allow_zero_depth_and_retry_timing() {
    assert!(DelayBackend::new(Arc::new(MemoryBackend::new()), unlimited_options()).is_ok());
}

#[test]
fn enabled_prefix_limits_require_positive_depth() {
    let mut options = unlimited_options();
    options.rate_limits.prefix_read_ps = per_second(1);
    assert_eq!(
        DelayBackend::new(Arc::new(MemoryBackend::new()), options).err(),
        Some(DelayOptionsError::ZeroPrefixDepth)
    );

    options.rate_limits.prefix_read_ps = RateLimit::Unlimited;
    options.rate_limits.prefix_write_ps = per_second(1);
    assert_eq!(
        DelayBackend::new(Arc::new(MemoryBackend::new()), options).err(),
        Some(DelayOptionsError::ZeroPrefixDepth)
    );
}

#[test]
fn enabled_object_limit_requires_positive_retry_timing() {
    let mut options = unlimited_options();
    options.rate_limits.same_obj_write_ps = per_second(1);
    assert_eq!(
        DelayBackend::new(Arc::new(MemoryBackend::new()), options).err(),
        Some(DelayOptionsError::ZeroObjectRetryDelay)
    );
}

#[tokio::test(start_paused = true)]
async fn unlimited_object_writes_do_not_back_off() {
    let mut options = unlimited_options();
    options.latency.obj_write = Latency::new(1, 0);
    let backend = DelayBackend::new(Arc::new(MemoryBackend::new()), options).unwrap();

    let outcome = tokio::time::timeout(Duration::from_millis(100), async {
        let mut version = backend.write_if_not_exists("same", vec![0]).await.unwrap();
        for value in 1..=3 {
            version = backend
                .write_if("same", vec![value], &version)
                .await
                .unwrap();
        }
    })
    .await;

    assert!(outcome.is_ok(), "unlimited writes unexpectedly timed out");
}

#[tokio::test(start_paused = true)]
async fn one_per_second_object_limit_advances_time_before_retrying() {
    let mut options = unlimited_options();
    options.rate_limits.same_obj_write_ps = per_second(1);
    options.rate_limits.same_obj_write_retry_delay = Duration::from_millis(2);
    let backend = DelayBackend::new(Arc::new(MemoryBackend::new()), options).unwrap();

    let first = backend.write_if_not_exists("same", vec![0]).await.unwrap();
    let second = backend.write_if("same", vec![1], &first).await.unwrap();
    tokio::time::advance(Duration::from_secs(1)).await;

    let started = tokio::time::Instant::now();
    tokio::time::timeout(
        Duration::from_secs(3),
        backend.write_if("same", vec![2], &second),
    )
    .await
    .expect("enabled limiter did not make progress")
    .unwrap();

    assert!(
        started.elapsed() >= Duration::from_millis(900),
        "rate-limited retry did not advance model time"
    );
}
