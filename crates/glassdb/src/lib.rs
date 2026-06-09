//! GlassDB: a stateless ACID key/value store on top of object storage.
//!
//! Public API ported from the Go root package: [`DB`] opens a database over a
//! [`glassdb_backend::Backend`], [`Collection`] groups keys, and [`Tx`] runs a
//! serializable transaction (with automatic conflict retries) via [`DB::tx`].
//!
//! # Cancellation
//!
//! Every public async entry point takes a [`Ctx`] and is durability-safe to
//! cancel: dropping a future mid-flight is equivalent to a crash and is
//! recovered by the commit protocol, so it never corrupts data. Callers should
//! prefer cancelling through the [`Ctx`] cancel token over dropping the future
//! (`tokio::time::timeout`, `select!`, `JoinHandle::abort`). A `Ctx`
//! cancellation unwinds in-memory coordination promptly, whereas a dropped
//! future leaves on-storage locks held by the abandoned attempt to be reclaimed
//! only after wait/lease timeouts. See [`DB::tx`] for details.

mod collection;
mod db;
pub mod diagnostics;
mod error;
mod iter;
#[cfg(feature = "sim")]
pub mod sim;
mod stats;
mod tx;
mod version;

pub use collection::Collection;
pub use db::{DbBuilder, DB};
pub use diagnostics::Diagnostics;
pub use error::Error;
pub use iter::{CollectionsIter, KeysIter};
pub use stats::Stats;
pub use tx::Tx;

// Re-export the backend abstraction so callers can construct a DB without
// depending on the backend crate directly.
pub use glassdb_backend::{self as backend, memory, middleware, Backend};

// Cloud backends, gated behind features so the heavy SDK dependencies are only
// pulled in when requested.
#[cfg(feature = "gcs")]
pub use glassdb_backend_gcs as gcs;
#[cfg(feature = "s3")]
pub use glassdb_backend_s3 as s3;

// Re-export the cancellation context, required by every public entry point.
pub use glassdb_concurr::Ctx;

// The deterministic simulation runtime (only under `--cfg sim`). Used by the
// concurrency fuzzer and the `concurrent_sim` self-check to drive the harness on
// the in-repo executor with a `TapeScheduler`/seed.
#[cfg(sim)]
pub use glassdb_concurr::rt;
