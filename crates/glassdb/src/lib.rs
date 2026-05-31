//! GlassDB: a stateless ACID key/value store on top of object storage.
//!
//! Public API ported from the Go root package: [`DB`] opens a database over a
//! [`glassdb_backend::Backend`], [`Collection`] groups keys, and [`Tx`] runs a
//! serializable transaction (with automatic conflict retries) via [`DB::tx`].

mod collection;
mod db;
mod error;
mod iter;
mod stats;
mod tx;
mod version;

pub use collection::Collection;
pub use db::{Options, DB};
pub use error::Error;
pub use iter::{CollectionsIter, KeysIter};
pub use stats::Stats;
pub use tx::{FqKey, ReadResult, Tx};

// Re-export the backend abstraction so callers can construct a DB without
// depending on the backend crate directly.
pub use glassdb_backend::{self as backend, memory, Backend};

// Re-export the cancellation context, required by every public entry point.
pub use glassdb_concurr::Ctx;
