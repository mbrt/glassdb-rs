//! A pure-Rust, in-process fake S3 server (the analog of the Go tests'
//! `gofakes3` + `httptest.Server`).
//!
//! It implements just the REST subset the [`crate::S3Backend`] uses and talks
//! plain HTTP/1.1 over a real loopback socket, so a real [`aws_sdk_s3::Client`]
//! exercises its full transport stack (SDK → smithy → hyper connection pool →
//! TCP) against it. That is the key difference from the in-memory
//! `DelayBackend`, which never touches HTTP: this is what lets a benchmark
//! reproduce *client transport* behavior (connection pooling, head-of-line
//! blocking under load) locally, with no AWS account.
//!
//! Three knobs make it useful beyond unit tests (see [`FakeS3Options`]):
//!
//! * **Simulated latency** — each served operation sleeps for a lognormally
//!   distributed time derived from a [`ProviderLatencyProfile`] (e.g. the
//!   latency in [`s3_delays`](glassdb_backend::middleware::s3_delays)). Without
//!   it a loopback server answers in microseconds and the connection pool is
//!   never stressed, so the transport effects under study never appear.
//! * **Connection counting** — every accepted TCP connection bumps an optional
//!   shared counter, giving the server-side connection-churn signal the Rust
//!   SDK does not surface on the client side.
//! * **Seeded entropy** — latency sampling reads from a server-owned deterministic
//!   stream, so the same request order can be replayed with the same delays.
//!
//! Fault injection (`503 SlowDown`, lost acknowledgements) is retained for the
//! retry tests.

mod faults;
mod latency;
mod lifecycle;
mod parsing;
mod routing;
mod state;

use std::sync::Arc;
use std::sync::atomic::AtomicU64;

use glassdb_backend::middleware::ProviderLatencyProfile;

pub use lifecycle::FakeS3;

/// Default seed for the fake server's latency entropy stream.
pub const DEFAULT_FAKE_S3_ENTROPY_SEED: u64 = 0x4641_4b45_5f53_3300;

/// Options for [`FakeS3::start_with`].
pub struct FakeS3Options {
    /// When set, every served operation sleeps for a simulated duration derived
    /// from this profile (e.g. the latency in
    /// [`s3_delays`](glassdb_backend::middleware::s3_delays)), in model time.
    /// `None` serves with no added latency (the default, used by the unit tests).
    pub latency: Option<ProviderLatencyProfile>,
    /// When set, every accepted TCP connection increments this counter. Lets a
    /// caller observe server-side connection churn across a measurement window.
    pub conn_counter: Option<Arc<AtomicU64>>,
    /// Seeds the server-owned entropy stream used to sample operation latency.
    /// Equal seeds replay the same latency sequence for the same request order.
    pub entropy_seed: u64,
}

impl Default for FakeS3Options {
    fn default() -> Self {
        Self {
            latency: None,
            conn_counter: None,
            entropy_seed: DEFAULT_FAKE_S3_ENTROPY_SEED,
        }
    }
}
