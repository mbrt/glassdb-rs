//! A plaintext HTTP client whose DNS resolution sleeps on tokio's blocking
//! pool, used to reproduce the real-S3 throughput collapse locally.
//!
//! On real S3 every new connection resolves the endpoint via `getaddrinfo`,
//! which hyper runs on tokio's blocking pool. Under connection bursts (driven by
//! S3 throttling / HTTP-2 stream limits) hundreds of these run at once and
//! saturate the 512-thread blocking pool, producing the `sys`-CPU storm, thread
//! explosion and throughput collapse observed on the EC2 runs. Plain loopback
//! has trivial IP "resolution", so the default `fakes3` path cannot reproduce
//! it (peak threads stay at ~29 even at 27k connection churns).
//!
//! This client injects a configurable per-resolution blocking sleep (modeling
//! getaddrinfo latency) and churns idle connections, so a new connection — and
//! thus a blocking-pool resolution — is needed on essentially every request,
//! the way it is on S3 under throttling.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::sync::Mutex;

use aws_smithy_runtime_api::client::http::{
    HttpClient, HttpConnector, HttpConnectorFuture, HttpConnectorSettings, SharedHttpClient,
    SharedHttpConnector,
};
use aws_smithy_runtime_api::client::orchestrator::{HttpRequest, HttpResponse};
use aws_smithy_runtime_api::client::result::ConnectorError;
use aws_smithy_runtime_api::client::runtime_components::RuntimeComponents;
use aws_smithy_types::body::SdkBody;
use hyper_util::client::legacy::Client as HyperClient;
use hyper_util::client::legacy::connect::HttpConnector as HyperHttpConnector;
use hyper_util::client::legacy::connect::dns::Name;
use hyper_util::rt::{TokioExecutor, TokioTimer};

type Conn = HyperHttpConnector<SleepyResolver>;

/// Resolver that ignores the name, sleeps `latency` on tokio's blocking pool
/// (modeling `getaddrinfo`), then returns the fake server's loopback address.
///
/// With `cache` set it resolves once and reuses the result, modeling the
/// production `CachingDnsResolver` fix: the blocking lookup runs a single time
/// (single-flighted across a concurrent connection burst) instead of on every
/// connection, so the blocking pool never fills.
#[derive(Clone)]
struct SleepyResolver {
    latency: Duration,
    addr: SocketAddr,
    cache: Option<Arc<Mutex<Option<SocketAddr>>>>,
}

impl tower_service::Service<Name> for SleepyResolver {
    type Response = std::iter::Once<SocketAddr>;
    type Error = std::io::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _name: Name) -> Self::Future {
        let latency = self.latency;
        let addr = self.addr;
        let cache = self.cache.clone();
        Box::pin(async move {
            match cache {
                // Hold the lock across the cold lookup so a concurrent burst
                // single-flights: the first caller pays one blocking sleep,
                // queued callers then read the cached address. Mirrors the
                // production `CachingDnsResolver`.
                Some(cache) => {
                    let mut slot = cache.lock().await;
                    if slot.is_none() {
                        let _ =
                            tokio::task::spawn_blocking(move || std::thread::sleep(latency)).await;
                        *slot = Some(addr);
                    }
                    Ok(std::iter::once(slot.expect("populated above")))
                }
                // No cache: every connection re-resolves, so a burst saturates
                // the blocking pool exactly as on S3 without the fix.
                None => {
                    let _ = tokio::task::spawn_blocking(move || std::thread::sleep(latency)).await;
                    Ok(std::iter::once(addr))
                }
            }
        })
    }
}

/// Adapts a hyper 1.0 client to the smithy [`HttpConnector`] interface (a thin
/// reimplementation of `aws_smithy_http_client`'s private `Adapter`, which the
/// public API only exposes for the default GAI resolver).
#[derive(Clone)]
struct SleepyConnector {
    inner: HyperClient<Conn, SdkBody>,
}

impl std::fmt::Debug for SleepyConnector {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("SleepyConnector")
    }
}

impl HttpConnector for SleepyConnector {
    fn call(&self, request: HttpRequest) -> HttpConnectorFuture {
        let request = match request.try_into_http1x() {
            Ok(r) => r,
            Err(e) => return HttpConnectorFuture::ready(Err(ConnectorError::user(e.into()))),
        };
        let client = self.inner.clone();
        HttpConnectorFuture::new(async move {
            let response = client
                .request(request)
                .await
                .map_err(|e| ConnectorError::other(e.into(), None))?
                .map(SdkBody::from_body_1_x);
            HttpResponse::try_from(response).map_err(|e| ConnectorError::other(e.into(), None))
        })
    }
}

#[derive(Clone, Debug)]
struct SleepyHttpClient {
    connector: SharedHttpConnector,
}

impl HttpClient for SleepyHttpClient {
    fn http_connector(
        &self,
        _settings: &HttpConnectorSettings,
        _components: &RuntimeComponents,
    ) -> SharedHttpConnector {
        self.connector.clone()
    }
}

/// Builds a plaintext HTTP client that resolves every connection through a
/// blocking-pool sleep of `dns_latency` and churns idle connections (so the
/// resolver runs on essentially every request), pointed at `addr` (the fake
/// server's loopback address).
///
/// With `cache` set, the resolver caches the address after the first lookup,
/// modeling the production `CachingDnsResolver` fix; the blocking sleep then
/// runs once instead of per connection, so the blocking pool stays drained.
pub fn sleepy_dns_http_client(
    dns_latency: Duration,
    addr: SocketAddr,
    cache: bool,
) -> SharedHttpClient {
    let resolver = SleepyResolver {
        latency: dns_latency,
        addr,
        cache: cache.then(|| Arc::new(Mutex::new(None))),
    };
    let mut http = HyperHttpConnector::new_with_resolver(resolver);
    http.set_nodelay(true);
    let inner: HyperClient<Conn, SdkBody> = HyperClient::builder(TokioExecutor::new())
        .pool_timer(TokioTimer::new())
        // Keep no idle connections, so a new connection (and thus a blocking
        // getaddrinfo) is needed on essentially every request. This makes the
        // saturated blocking pool the throughput bottleneck, the way S3's
        // throttling-driven connection churn does — rather than letting warm
        // connections route around it.
        .pool_max_idle_per_host(0)
        .build(http);
    let connector = SharedHttpConnector::new(SleepyConnector { inner });
    SharedHttpClient::new(SleepyHttpClient { connector })
}
