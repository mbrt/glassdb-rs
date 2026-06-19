//! HTTP client factory tuned for high-concurrency S3 workloads.
//!
//! High-concurrency S3 workloads in the bench can pile up DNS+TLS handshakes
//! on tokio's blocking thread pool when the SDK opens a fresh connection for
//! every in-flight op. Returning a single, reusable [`SharedHttpClient`] with
//! HTTPS (rustls + aws-lc as the crypto provider) and a generous idle pool
//! steers the SDK toward connection reuse, so handshake cost is paid once and
//! ALPN negotiates HTTP/2 with S3 on top of the reused connection.
//!
//! Connection reuse alone is not enough: when S3 throttles a hot prefix it
//! resets the warm pool, so the SDK opens a burst of fresh connections and
//! hyper resolves the endpoint host with a blocking `getaddrinfo` *per
//! connection* on tokio's blocking pool. A few hundred concurrent ops saturate
//! the 512-thread pool, and throughput collapses. The client therefore also
//! caches DNS via [`CachingDnsResolver`], so a burst pays one `getaddrinfo`
//! instead of one per connection.
//!
//! The function is intentionally a free helper rather than baked into
//! [`crate::S3Backend::new`]: it composes with `aws_config`'s loader so callers
//! can layer their own interceptors, credentials, or region selection on top
//! of the recommended transport.

use std::time::Duration;

use aws_smithy_http_client::{Builder, tls};
use aws_smithy_runtime_api::client::http::SharedHttpClient;

use crate::caching_dns::CachingDnsResolver;

/// HTTP client tuned for high-concurrency S3 workloads: rustls (aws-lc crypto
/// provider) plus hyper's default 90-second idle pool, so the SDK keeps
/// reusing connections instead of repeatedly paying DNS+TLS handshake cost on
/// tokio's blocking thread pool. ALPN negotiates HTTP/2 with S3 on the reused
/// connections.
///
/// Pass the result to `aws_config::defaults(...).http_client(...)` (or
/// `aws_sdk_s3::config::Builder::http_client(...)`) before constructing the
/// `Client` you hand to [`crate::S3Backend::new`]. Example:
///
/// ```no_run
/// # async fn example(bucket: String) {
/// let base = aws_config::defaults(aws_config::BehaviorVersion::latest())
///     .http_client(glassdb_backend_s3::tuned_http_client())
///     .load()
///     .await;
/// let conf = aws_sdk_s3::config::Builder::from(&base).build();
/// let client = aws_sdk_s3::Client::from_conf(conf);
/// let backend = glassdb_backend_s3::S3Backend::new(client, bucket);
/// # let _ = backend;
/// # }
/// ```
pub fn tuned_http_client() -> SharedHttpClient {
    Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::AwsLc,
        ))
        .pool_idle_timeout(Duration::from_secs(90))
        .build_with_resolver(CachingDnsResolver::default())
}
