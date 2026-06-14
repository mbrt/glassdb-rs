//! GlassDB: a stateless ACID key/value store on top of object storage.
//!
//! Public API: [`Database`] opens a database over a
//! [`glassdb_backend::Backend`], [`Collection`] groups keys, and [`Transaction`] runs a
//! serializable transaction (with automatic conflict retries) via [`Database::tx`].
//!
//! # Cancellation
//!
//! Every public async entry point is durability-safe to cancel: dropping a
//! future mid-flight is equivalent to a crash and is recovered by the commit
//! protocol, so it never corrupts data. Cancel by wrapping the future with
//! `tokio::time::timeout`, `tokio::select!`, or aborting a `JoinHandle`. Locks
//! held by an abandoned attempt are reclaimed after wait/lease timeouts. See
//! [`Database::tx`] for details.

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
pub use db::{Database, DatabaseBuilder};
pub use diagnostics::Diagnostics;
pub use error::Error;
pub use iter::{CollectionsIter, KeysIter};
pub use stats::Stats;
pub use tx::Transaction;

// Re-export the backend abstraction so callers can construct a Database without
// depending on the backend crate directly.
pub use glassdb_backend::{self as backend, Backend, memory, middleware};

// Cloud backends, gated behind features so the heavy SDK dependencies are only
// pulled in when requested.
#[cfg(feature = "gcs")]
pub use glassdb_backend_gcs as gcs;
#[cfg(feature = "s3")]
pub use glassdb_backend_s3 as s3;

// The deterministic simulation runtime (only under `--cfg sim`). Used by the
// concurrency fuzzer and the `concurrent_sim` self-check to drive the harness on
// the in-repo executor with a `TapeScheduler`/seed.
#[cfg(sim)]
pub use glassdb_concurr::rt;
