//! Garbage collection of finalized transaction objects.
//!
//! **Inert in v2 (ADR-022 deferred).** In the v2 object-native layout a
//! committed transaction object *is* the value store: a key's live value lives
//! in the transaction object its shard entry's `current_writer` points at, and
//! readers help-forward through it. Deleting a committed transaction object
//! would therefore drop live values. A correct collector must mark-sweep the
//! shard `current_writer` graph (and roots) before reclaiming an object, which
//! is deferred to the GC ADR.
//!
//! Until then this type is a no-op: nothing is scheduled and nothing is swept,
//! so no referenced object can ever be lost. The trade-off is unbounded object
//! growth, accepted for now. The [`Gc::new`]/[`Gc::start`]/
//! [`Gc::schedule_tx_cleanup`] surface is kept so the write-back path and the
//! database wiring compile unchanged and the future collector can drop in.

use std::sync::Weak;

use glassdb_concurr::Background;
use glassdb_data::TxId;
use glassdb_storage::TLogger;

/// Garbage collector for finalized transaction objects. Inert in v2; see the
/// module documentation.
#[derive(Clone)]
pub struct Gc {
    // Held only so the constructor signature and ownership model match the
    // future collector; unused while GC is inert.
    _bg: Weak<Background>,
    _tl: TLogger,
}

impl Gc {
    /// Creates a GC using the given background executor and logger. Inert in v2.
    pub fn new(bg: Weak<Background>, tl: TLogger) -> Self {
        Gc { _bg: bg, _tl: tl }
    }

    /// Starts the background cleanup loop. No-op while GC is inert: spawning a
    /// sweeper that deletes committed transaction objects would drop live values
    /// (the objects are the value store), so nothing is started.
    pub fn start(&self) {}

    /// Enqueues a finalized transaction object for later deletion. No-op while
    /// GC is inert: a committed transaction object is referenced by its shard
    /// entries' `current_writer` pointers and must not be deleted.
    pub(crate) fn schedule_tx_cleanup(&self, _tid: TxId) {}
}
