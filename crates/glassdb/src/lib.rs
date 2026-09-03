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
//! held by an interrupted local attempt are synchronously handed to managed
//! retirement; helpers and garbage collection may reclaim their physical
//! resources later. See [`Database::tx`] for details.
//!
//! # Transaction-body panics
//!
//! Panics propagate without read validation or transparent retry, including on
//! stale snapshots. The unwind path uses the same managed retirement as
//! cancellation, so framework-owned transaction resources remain recoverable.

mod collection;
mod db;
pub mod diagnostics;
mod error;
mod iter;
mod scan;
#[cfg(feature = "sim")]
pub mod sim;
mod stats;
mod tx;
mod version;

pub use collection::{Collection, CollectionPath};
pub use db::{Database, DatabaseBuilder};
pub use diagnostics::Diagnostics;
pub use error::Error;
pub use iter::{CollectionEntry, CollectionIter, KeyIter};
pub use scan::{KeyPage, KeyScan};
pub use stats::{Stats, TransactionStats};
pub use tx::Transaction;

/// Returns an error from a transaction attempt when `condition` is false.
///
/// This is the transaction-body analogue of `assert!`: returning the error lets
/// [`Database::tx`] validate the attempt's reads and retry if they were
/// inconsistent. Assertions and other panics bypass that validation; their
/// cleanup safety does not make them snapshot-transparent.
#[macro_export]
macro_rules! ensure_tx {
    ($condition:expr, $error:expr $(,)?) => {
        if !$condition {
            return Err($error);
        }
    };
}

// The split soft-cap policy, so callers can tune when a collection's B-link
// tree splits (see [`DatabaseBuilder::split_policy`]), and the inline-value
// budgets (see [`DatabaseBuilder::inline_policy`]).
pub use glassdb_data::MAX_COLLECTION_NAME_BYTES;
pub use glassdb_storage::{
    CacheStats, InlinePolicy, InvalidSplitPolicy, PersistentCacheConfig, SplitPolicy,
    SplitPolicyBuilder,
};
pub use glassdb_trans::{
    DirectCommitStats, InlinePressureStats, LeafCoordinatorStats, LockerStats, ProtocolTiming,
    SplitterStats,
};

// Re-export the backend abstraction so callers can construct a Database without
// depending on the backend crate directly.
pub use glassdb_backend::{self as backend, Backend, BackendStats, memory, middleware};

// Cloud backends, gated behind features so the heavy SDK dependencies are only
// pulled in when requested.
#[cfg(feature = "gcs")]
pub use glassdb_backend_gcs as gcs;
#[cfg(feature = "s3")]
pub use glassdb_backend_s3 as s3;

// Deterministic execution control and runtime services (only under `--cfg sim`).
// The concurrency fuzzers and simulation self-checks use `exec` to drive the
// harness while application code uses `rt` for task and time services.
#[cfg(sim)]
pub use glassdb_concurr::{exec, rt};
