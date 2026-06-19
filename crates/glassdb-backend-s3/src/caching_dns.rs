//! A caching DNS resolver for the tuned S3 HTTP client.
//!
//! Hyper resolves a connection's host with `getaddrinfo`, which it runs on
//! tokio's blocking thread pool. The SDK does this for *every new connection*,
//! so when a high-concurrency S3 workload opens a burst of connections (e.g.
//! after `503 SlowDown` resets the warm pool) hundreds of blocking
//! `getaddrinfo` calls pile onto the 512-thread blocking pool at once. The pool
//! saturates, a `sys`-CPU storm follows, the thread count explodes, and
//! throughput collapses.
//!
//! Since an S3 client talks to a single endpoint host, resolving it once and
//! reusing the result removes that per-connection blocking work entirely:
//! [`CachingDnsResolver`] caches each host's addresses for a TTL and
//! single-flights concurrent misses, so a connection burst pays one
//! `getaddrinfo` instead of one per connection.

use std::collections::HashMap;
use std::net::{IpAddr, ToSocketAddrs};
use std::sync::Arc;
use std::time::{Duration, Instant};

use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use tokio::sync::Mutex;

/// How long a resolved host is reused before it is looked up again. Short
/// enough that S3's DNS-based load spreading still rotates endpoints, long
/// enough that a connection burst does not re-resolve per connection.
const DEFAULT_TTL: Duration = Duration::from_secs(30);

/// Resolves a host via the system resolver (`getaddrinfo`). Like hyper's default
/// resolver, the blocking lookup runs on tokio's blocking pool.
#[derive(Clone, Debug, Default)]
pub struct SystemDnsResolver;

impl ResolveDns for SystemDnsResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        let host = name.to_string();
        DnsFuture::new(async move {
            tokio::task::spawn_blocking(move || {
                // Port is irrelevant; the connector pairs the returned IPs with
                // the request's port. `0` just satisfies `ToSocketAddrs`.
                (host.as_str(), 0u16)
                    .to_socket_addrs()
                    .map(|addrs| addrs.map(|s| s.ip()).collect::<Vec<IpAddr>>())
            })
            .await
            .map_err(ResolveDnsError::new)?
            .map_err(ResolveDnsError::new)
        })
    }
}

/// Wraps a [`ResolveDns`] with a per-host TTL cache, so a host is resolved at
/// most once per [`DEFAULT_TTL`] instead of once per connection.
#[derive(Clone, Debug)]
pub struct CachingDnsResolver<R = SystemDnsResolver> {
    inner: R,
    ttl: Duration,
    cache: Arc<Mutex<HashMap<String, CacheEntry>>>,
}

#[derive(Clone, Debug)]
struct CacheEntry {
    addrs: Vec<IpAddr>,
    fetched: Instant,
}

impl Default for CachingDnsResolver {
    fn default() -> Self {
        Self::new(SystemDnsResolver, DEFAULT_TTL)
    }
}

impl<R> CachingDnsResolver<R> {
    /// Wraps `inner`, caching each resolved host for `ttl`.
    pub fn new(inner: R, ttl: Duration) -> Self {
        Self {
            inner,
            ttl,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

impl<R> ResolveDns for CachingDnsResolver<R>
where
    R: ResolveDns + Clone + 'static,
{
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        let inner = self.inner.clone();
        let ttl = self.ttl;
        let cache = self.cache.clone();
        let host = name.to_string();
        DnsFuture::new(async move {
            // Hold the lock across the (rare) miss lookup so concurrent
            // resolutions of the same host single-flight: a burst of new
            // connections pays one `getaddrinfo`, then every queued caller reads
            // the just-populated entry. The client talks to one host, so
            // serializing distinct hosts here is irrelevant in practice.
            let mut guard = cache.lock().await;
            if let Some(entry) = guard.get(&host)
                && entry.fetched.elapsed() < ttl
            {
                return Ok(entry.addrs.clone());
            }
            let addrs = inner.resolve_dns(&host).await?;
            guard.insert(
                host,
                CacheEntry {
                    addrs: addrs.clone(),
                    fetched: Instant::now(),
                },
            );
            Ok(addrs)
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::Ipv4Addr;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    /// A resolver that counts how many real lookups it performs and returns a
    /// fixed address, so a test can assert the cache collapses repeats.
    #[derive(Clone, Debug)]
    struct CountingResolver {
        calls: Arc<AtomicUsize>,
        addr: IpAddr,
    }

    impl CountingResolver {
        fn new() -> Self {
            Self {
                calls: Arc::new(AtomicUsize::new(0)),
                addr: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
            }
        }
    }

    impl ResolveDns for CountingResolver {
        fn resolve_dns<'a>(&'a self, _name: &'a str) -> DnsFuture<'a> {
            let calls = self.calls.clone();
            let addr = self.addr;
            DnsFuture::new(async move {
                calls.fetch_add(1, Ordering::SeqCst);
                Ok(vec![addr])
            })
        }
    }

    #[tokio::test]
    async fn repeated_resolves_of_same_host_hit_cache() {
        let inner = CountingResolver::new();
        let calls = inner.calls.clone();
        let resolver = CachingDnsResolver::new(inner, Duration::from_secs(60));

        for _ in 0..5 {
            let addrs = resolver.resolve_dns("s3.example.com").await.unwrap();
            assert_eq!(addrs, vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1))]);
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1, "host resolved once");
    }

    #[tokio::test]
    async fn concurrent_cold_resolves_single_flight() {
        let inner = CountingResolver::new();
        let calls = inner.calls.clone();
        let resolver = CachingDnsResolver::new(inner, Duration::from_secs(60));

        // A burst of concurrent cold lookups must collapse to a single
        // underlying resolution, mirroring a connection burst.
        let mut handles = Vec::new();
        for _ in 0..64 {
            let r = resolver.clone();
            handles.push(tokio::spawn(async move {
                r.resolve_dns("s3.example.com").await.unwrap()
            }));
        }
        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(calls.load(Ordering::SeqCst), 1, "burst resolved once");
    }

    #[tokio::test]
    async fn distinct_hosts_resolved_separately() {
        let inner = CountingResolver::new();
        let calls = inner.calls.clone();
        let resolver = CachingDnsResolver::new(inner, Duration::from_secs(60));

        resolver.resolve_dns("a.example.com").await.unwrap();
        resolver.resolve_dns("b.example.com").await.unwrap();
        resolver.resolve_dns("a.example.com").await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2, "one lookup per host");
    }

    #[tokio::test]
    async fn entry_re_resolved_after_ttl() {
        let inner = CountingResolver::new();
        let calls = inner.calls.clone();
        let resolver = CachingDnsResolver::new(inner, Duration::from_millis(20));

        resolver.resolve_dns("s3.example.com").await.unwrap();
        tokio::time::sleep(Duration::from_millis(40)).await;
        resolver.resolve_dns("s3.example.com").await.unwrap();

        assert_eq!(calls.load(Ordering::SeqCst), 2, "re-resolved after TTL");
    }
}
