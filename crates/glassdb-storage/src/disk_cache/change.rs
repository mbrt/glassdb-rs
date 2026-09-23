//! L2 path changes and their bounded lifetime tracking.
//!
//! An L2 path change starts when the process learns that the L2 copy of one
//! path is out of date, before the matching L1 change becomes visible. It ends
//! when the worker finishes the queued work of the latest change of that path.
//! While a change is pending, L2 does not serve or promote the path, so delayed
//! admissions or promotions cannot restore knowledge that the process already
//! invalidated (ADR-045).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const MAX_PENDING_CHANGES: usize = 4096;

/// The L2 change state of one path.
#[derive(Default)]
pub(crate) struct PathChanges {
    state: Mutex<ChangeState>,
}

/// Provides the change state of one path and keeps its owner alive while
/// queued L2 work can still refer to it.
pub(crate) trait PathChangeOwner: Send + Sync {
    fn path_changes(&self) -> &PathChanges;
}

/// One started change of one path. Dropping it ends the change, unless a
/// later change of the same path started.
pub(crate) struct PendingChange {
    owner: Arc<dyn PathChangeOwner>,
    change: u64,
    pending_changes: Arc<AtomicUsize>,
}

/// Limits the number of pending changes over all paths.
pub(super) struct PendingChangeLimit {
    pending_changes: Arc<AtomicUsize>,
}

/// The number of the latest change of one path, and whether it is pending.
#[derive(Default)]
struct ChangeState {
    latest: u64,
    pending: bool,
}

impl PathChanges {
    /// Reports whether the latest change of the path is pending.
    pub(crate) fn is_pending(&self) -> bool {
        self.state.lock().unwrap().pending
    }

    /// Returns the number of the latest change and whether it is pending.
    pub(super) fn snapshot(&self) -> (u64, bool) {
        let state = self.state.lock().unwrap();
        (state.latest, state.pending)
    }

    fn begin(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.latest = state.latest.wrapping_add(1);
        // Zero identifies the state before the first change, so work queued
        // before any change must not match a wrapped counter.
        if state.latest == 0 {
            state.latest = 1;
        }
        state.pending = true;
        state.latest
    }

    fn finish(&self, change: u64) {
        let mut state = self.state.lock().unwrap();
        if state.latest == change {
            state.pending = false;
        }
    }
}

impl PathChangeOwner for PathChanges {
    fn path_changes(&self) -> &PathChanges {
        self
    }
}

impl PendingChange {
    /// Reports whether this change is still the latest change of its path.
    pub(super) fn is_latest(&self) -> bool {
        self.owner.path_changes().snapshot() == (self.change, true)
    }
}

impl Drop for PendingChange {
    fn drop(&mut self) {
        self.owner.path_changes().finish(self.change);
        self.pending_changes.fetch_sub(1, Ordering::AcqRel);
    }
}

impl PendingChangeLimit {
    pub(super) fn new() -> Self {
        Self {
            pending_changes: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Starts a new change of the owner's path, or returns `None` when too
    /// many changes are pending.
    pub(super) fn begin(&self, owner: Arc<dyn PathChangeOwner>) -> Option<PendingChange> {
        self.pending_changes
            .try_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < MAX_PENDING_CHANGES).then_some(current + 1)
            })
            .ok()?;
        let change = owner.path_changes().begin();
        Some(PendingChange {
            owner,
            change,
            pending_changes: self.pending_changes.clone(),
        })
    }

    #[cfg(test)]
    pub(super) fn pending_count(&self) -> usize {
        self.pending_changes.load(Ordering::Acquire)
    }
}
