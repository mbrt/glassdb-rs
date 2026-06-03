//! GlassDB: a stateless ACID key/value store on top of object storage.
//!
//! Public API ported from the Go root package: [`DB`] opens a database over a
//! [`glassdb_backend::Backend`], [`Collection`] groups keys, and [`Tx`] runs a
//! serializable transaction (with automatic conflict retries) via [`DB::tx`].

mod collection;
mod db;
mod error;
mod iter;
#[cfg(feature = "sim")]
pub mod sim;
mod stats;
mod tx;
mod version;

pub use collection::Collection;
pub use db::{Options, DB};
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
