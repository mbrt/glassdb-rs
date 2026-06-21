//! Async DNS resolver for the tuned S3 HTTP client.
//!
//! hyper's default resolver runs the blocking `getaddrinfo` on tokio's blocking
//! thread pool, once per new connection. The SDK opens a fresh connection per
//! in-flight op when its warm pool is insufficient (cold ramp, stream
//! saturation, or retry bursts under `503 SlowDown`), so a high-concurrency
//! workload fires hundreds of concurrent blocking lookups. They saturate the
//! 512-thread blocking pool, the OS thread count explodes, and throughput
//! collapses.
//!
//! [`AsyncDnsResolver`] resolves names with hickory-dns, a pure-Rust resolver
//! that performs lookups asynchronously on the tokio reactor — never on the
//! blocking pool — so a connection burst cannot saturate it. hickory also caches
//! responses (honoring their TTL), so repeated lookups for the S3 endpoint are
//! served from memory.

use std::net::IpAddr;
use std::sync::Arc;

use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use hickory_resolver::{Resolver, TokioResolver};

type BoxError = Box<dyn std::error::Error + Send + Sync>;

/// A [`ResolveDns`] backed by hickory-dns: lookups run on the tokio reactor
/// (not the blocking pool) and are cached with their DNS TTL.
#[derive(Clone, Debug)]
pub struct AsyncDnsResolver {
    resolver: Arc<TokioResolver>,
}

impl AsyncDnsResolver {
    /// Builds a resolver from the system configuration (`/etc/resolv.conf` on
    /// Unix, the registry on Windows). Fails only if that configuration cannot
    /// be loaded.
    pub fn from_system() -> Result<Self, BoxError> {
        let resolver = Resolver::builder_tokio()?.build()?;
        Ok(Self {
            resolver: Arc::new(resolver),
        })
    }
}

impl ResolveDns for AsyncDnsResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        let resolver = self.resolver.clone();
        let host = name.to_string();
        DnsFuture::new(async move {
            let lookup = resolver
                .lookup_ip(host)
                .await
                .map_err(ResolveDnsError::new)?;
            Ok(lookup.iter().collect::<Vec<IpAddr>>())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn builds_from_system_config() {
        // Wiring smoke test: the resolver must construct from the host's DNS
        // configuration without a network round trip.
        AsyncDnsResolver::from_system().expect("resolver builds from system config");
    }

    #[tokio::test]
    async fn resolves_ip_literal_offline() {
        // An IP literal is returned verbatim without a DNS query, so this
        // exercises the adapter (string in, `Vec<IpAddr>` out) deterministically
        // and offline.
        let resolver = AsyncDnsResolver::from_system().expect("resolver builds");
        let addrs = resolver.resolve_dns("127.0.0.1").await.expect("resolves");
        assert_eq!(addrs, vec![IpAddr::from([127, 0, 0, 1])]);
    }
}
