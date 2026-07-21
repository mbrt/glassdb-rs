//! Operator diagnostics for hang-prone coordination paths.
//!
//! [`Database::diagnostics`] returns a [`Diagnostics`] snapshot capturing the
//! shard coordinator's live dedup state and each transaction's locally held
//! locks. The snapshot is pull-only and zero cost unless called.
//!
//! The signals are tuned to the orphan-key (dedup) and partial-lock-set
//! (locker) hangs the coordination layer is most prone to: an entry with a
//! non-empty queue but no active op, or a transaction whose held set does not
//! cover its needed paths, is the visible signature of those bug classes.
//!
//! For event-style deduplication breadcrumbs such as
//! `inline_driver_dropped_handoff`, register a [`tracing`] subscriber on the
//! `glassdb::dedup` target. Splitter and explicit backend-logging middleware
//! events use the stable `glassdb::splitter` and `glassdb::backend` targets.
//!
//! [`Database::diagnostics`]: crate::Database::diagnostics
//! [`tracing`]: https://docs.rs/tracing

use std::fmt;

pub use glassdb_trans::{DedupKeySnapshot, HeldLeafSnapshot, TxLockSnapshot};

/// A snapshot of the shard coordinator's and locker's live state. Returned by
/// [`crate::Database::diagnostics`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostics {
    /// Per-object dedup state inside the shard coordinator.
    ///
    /// Contains one entry per path with live coordination state, sorted by key.
    pub coordinator_dedup: Vec<DedupKeySnapshot>,
    /// One entry per transaction holding any local-cache lock. Sorted by tx id.
    pub transactions: Vec<TxLockSnapshot>,
}

impl fmt::Display for Diagnostics {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        writeln!(f, "Diagnostics:")?;
        writeln!(
            f,
            "  coordinator dedup ({} paths):",
            self.coordinator_dedup.len()
        )?;
        for k in &self.coordinator_dedup {
            writeln!(
                f,
                "    {} active_op={} batch={} pending={} queue={}",
                k.key, k.has_active_op, k.batch_count, k.pending_count, k.queue_count,
            )?;
        }
        writeln!(f, "  transactions ({}):", self.transactions.len())?;
        for t in &self.transactions {
            writeln!(f, "    {} ({} leaves)", t.tx_id, t.leaves.len())?;
            for leaf in &t.leaves {
                writeln!(
                    f,
                    "      {} entry={} membership={}",
                    leaf.path, leaf.entry_lock, leaf.membership_lock
                )?;
            }
        }
        Ok(())
    }
}
