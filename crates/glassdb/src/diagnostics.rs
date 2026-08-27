//! Operator diagnostics for hang-prone coordination paths.
//!
//! [`Database::diagnostics`] returns a [`Diagnostics`] snapshot of the shard
//! coordinator's live dedup state. It reads existing coordinator state only
//! when called and does not maintain separate diagnostic state.
//!
//! The signal is tuned to orphan-key hangs in the coordination layer: an entry
//! with a non-empty queue but no active operation is the visible signature of
//! that bug class.
//!
//! For event-style deduplication breadcrumbs such as
//! `inline_driver_dropped_handoff`, register a [`tracing`] subscriber on the
//! `glassdb::dedup` target. Splitter and explicit backend-logging middleware
//! events use the stable `glassdb::splitter`, `glassdb::write_back`, and
//! `glassdb::backend` targets.
//!
//! [`Database::diagnostics`]: crate::Database::diagnostics
//! [`tracing`]: https://docs.rs/tracing

use std::fmt;

pub use glassdb_concurr::DedupKeySnapshot;

/// A snapshot of the shard coordinator's live state. Returned by
/// [`crate::Database::diagnostics`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Diagnostics {
    /// Per-object dedup state inside the shard coordinator.
    ///
    /// Contains one entry per path with live coordination state, sorted by key.
    pub coordinator_dedup: Vec<DedupKeySnapshot>,
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
        Ok(())
    }
}
