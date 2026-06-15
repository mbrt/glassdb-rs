//! Operator diagnostics for hang-prone coordination paths.
//!
//! [`Database::diagnostics`] returns a [`Diagnostics`] snapshot capturing the lock
//! coordinator's live state: per-key dedup state and per-transaction held
//! locks. The snapshot is pull-only and zero cost unless called.
//!
//! The signals are tuned to the orphan-key (dedup) and partial-lock-set
//! (locker) hangs the coordination layer is most prone to: an entry with a
//! non-empty queue but no active op, or a transaction whose held set does not
//! cover its needed paths, is the visible signature of those bug classes.
//!
//! For event-style breadcrumbs (e.g. `parallel_lock_timeout_fallback_to_serial`,
//! `inline_driver_dropped_handoff`), register a [`tracing`] subscriber on the
//! `glassdb::dedup`, `glassdb::locker`, and `glassdb::algo` targets, e.g. via
//! `tracing-subscriber` and `RUST_LOG=glassdb=debug`.
//!
//! [`Database::diagnostics`]: crate::Database::diagnostics
//! [`tracing`]: https://docs.rs/tracing

use std::fmt;

pub use glassdb_trans::{DedupKeySnapshot, TxLockSnapshot};

/// A snapshot of the lock coordinator's live state. Returned by
/// [`crate::Database::diagnostics`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostics {
    /// Per-key dedup state inside the locker (one entry per key with live
    /// coordination state). Sorted by key.
    pub locker_dedup: Vec<DedupKeySnapshot>,
    /// One entry per transaction holding any local-cache lock. Sorted by tx id.
    pub transactions: Vec<TxLockSnapshot>,
}

impl fmt::Display for Diagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Diagnostics:")?;
        writeln!(f, "  locker dedup ({} keys):", self.locker_dedup.len())?;
        for k in &self.locker_dedup {
            writeln!(
                f,
                "    {} active_op={} batch={} pending={} queue={}",
                k.key, k.has_active_op, k.batch_count, k.pending_count, k.queue_count,
            )?;
        }
        writeln!(f, "  transactions ({}):", self.transactions.len())?;
        for t in &self.transactions {
            writeln!(f, "    {} ({} locks)", t.tx_id, t.locks.len())?;
            for l in &t.locks {
                writeln!(f, "      {} {}", l.typ, l.path)?;
            }
        }
        Ok(())
    }
}
