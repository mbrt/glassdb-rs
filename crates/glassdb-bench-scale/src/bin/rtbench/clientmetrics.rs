//! Client-side resource metrics, ported from the Go `rtbench/clientmetrics.go`.
//!
//! Counts HTTP-level activity of the shared S3 client (so retries and
//! throttling responses are visible even when the SDK retryer absorbs them)
//! and samples the peak OS-thread count over a measurement window. Together
//! with the CPU accounting in [`crate::cpu`], this distinguishes a client-side
//! ceiling from backend throttling.
//!
//! Two metrics from the Go version have no clean Rust analog and are reported
//! as best-effort: `new_conns` (TLS handshakes; not surfaced by the SDK HTTP
//! stack) is always zero, and the peak-goroutine count is approximated by the
//! peak OS-thread count (the worker model uses threads, not goroutines).

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::thread::JoinHandle;
use std::time::Duration;

use aws_smithy_runtime_api::box_error::BoxError;
use aws_smithy_runtime_api::client::interceptors::Intercept;
use aws_smithy_runtime_api::client::interceptors::context::{
    BeforeDeserializationInterceptorContextRef, BeforeTransmitInterceptorContextRef,
};
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::config_bag::ConfigBag;

/// Accumulates HTTP-level activity of the shared S3 client. Counting happens at
/// the interceptor layer, so each retry attempt and each throttling response is
/// visible here even when the SDK retryer absorbs them.
#[derive(Debug, Default)]
pub struct HttpMetrics {
    /// Total HTTP attempts, including retries.
    pub requests: AtomicI64,
    /// 503 SlowDown / 429 throttling responses.
    pub throttle: AtomicI64,
    /// Other 5xx responses.
    pub server_err: AtomicI64,
    /// 2xx responses.
    pub success: AtomicI64,
    /// Connections opened (TLS handshakes). Not surfaced by the Rust SDK HTTP
    /// stack, so always zero (see module docs).
    pub new_conns: AtomicI64,
}

impl HttpMetrics {
    pub fn snapshot(&self) -> HttpSnapshot {
        HttpSnapshot {
            requests: self.requests.load(Ordering::Relaxed),
            throttle: self.throttle.load(Ordering::Relaxed),
            server_err: self.server_err.load(Ordering::Relaxed),
            success: self.success.load(Ordering::Relaxed),
            new_conns: self.new_conns.load(Ordering::Relaxed),
        }
    }
}

/// A point-in-time copy of [`HttpMetrics`], used to compute per-step deltas.
#[derive(Debug, Clone, Copy, Default)]
pub struct HttpSnapshot {
    pub requests: i64,
    pub throttle: i64,
    pub server_err: i64,
    pub success: i64,
    pub new_conns: i64,
}

impl HttpSnapshot {
    pub fn sub(self, o: HttpSnapshot) -> HttpSnapshot {
        HttpSnapshot {
            requests: self.requests - o.requests,
            throttle: self.throttle - o.throttle,
            server_err: self.server_err - o.server_err,
            success: self.success - o.success,
            new_conns: self.new_conns - o.new_conns,
        }
    }
}

/// An aws-sdk interceptor that records every request attempt and classifies its
/// HTTP response status into the [`HttpMetrics`] counters.
#[derive(Debug, Clone)]
pub struct HttpCounter {
    metrics: Arc<HttpMetrics>,
}

impl HttpCounter {
    pub fn new(metrics: Arc<HttpMetrics>) -> Self {
        HttpCounter { metrics }
    }
}

impl Intercept for HttpCounter {
    fn name(&self) -> &'static str {
        "GlassdbHttpCounter"
    }

    fn read_before_transmit(
        &self,
        _ctx: &BeforeTransmitInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        self.metrics.requests.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn read_before_deserialization(
        &self,
        ctx: &BeforeDeserializationInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        _cfg: &mut ConfigBag,
    ) -> Result<(), BoxError> {
        let status = ctx.response().status().as_u16();
        if status == 503 || status == 429 {
            self.metrics.throttle.fetch_add(1, Ordering::Relaxed);
        } else if status >= 500 {
            self.metrics.server_err.fetch_add(1, Ordering::Relaxed);
        } else if (200..300).contains(&status) {
            self.metrics.success.fetch_add(1, Ordering::Relaxed);
        }
        Ok(())
    }
}

/// Tracks the peak OS-thread count over a measurement window by polling
/// `/proc/self/status`. The analog of the Go `goroutineSampler`: a high peak
/// relative to the offered concurrency hints at workers piling up behind a
/// shared bottleneck.
pub struct ThreadSampler {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<usize>>,
}

impl ThreadSampler {
    pub fn start() -> ThreadSampler {
        let stop = Arc::new(AtomicBool::new(false));
        let stop_bg = stop.clone();
        let handle = std::thread::spawn(move || {
            let mut peak = current_thread_count();
            while !stop_bg.load(Ordering::Relaxed) {
                std::thread::sleep(Duration::from_millis(100));
                let n = current_thread_count();
                if n > peak {
                    peak = n;
                }
            }
            peak
        });
        ThreadSampler {
            stop,
            handle: Some(handle),
        }
    }

    pub fn stop_and_peak(mut self) -> usize {
        self.stop.store(true, Ordering::Relaxed);
        self.handle
            .take()
            .map(|h| h.join().unwrap_or(0))
            .unwrap_or(0)
    }
}

#[cfg(target_os = "linux")]
fn current_thread_count() -> usize {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find_map(|l| l.strip_prefix("Threads:"))
                .and_then(|v| v.trim().parse().ok())
        })
        .unwrap_or(0)
}

#[cfg(not(target_os = "linux"))]
fn current_thread_count() -> usize {
    0
}
