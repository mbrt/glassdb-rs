//! Persistent-cache path epochs and their bounded lifetime tracking.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

const MAX_ACTIVE_FENCES: usize = 4096;

#[derive(Default)]
pub(crate) struct PathFence {
    state: Mutex<FenceState>,
}

/// Provides the path fence and retains its semantic owner while queued L2 work
/// can still refer to it.
pub(crate) trait FenceContext: Send + Sync {
    fn fence(&self) -> &PathFence;
}

pub(crate) struct FenceGuard {
    context: Arc<dyn FenceContext>,
    epoch: u64,
    active_fences: Arc<AtomicUsize>,
}

pub(super) struct FenceTracker {
    active_fences: Arc<AtomicUsize>,
}

#[derive(Default)]
struct FenceState {
    epoch: u64,
    active: bool,
}

impl PathFence {
    pub(crate) fn is_active(&self) -> bool {
        self.state.lock().unwrap().active
    }

    pub(super) fn snapshot(&self) -> (u64, bool) {
        let state = self.state.lock().unwrap();
        (state.epoch, state.active)
    }

    fn begin(&self) -> u64 {
        let mut state = self.state.lock().unwrap();
        state.epoch = state.epoch.wrapping_add(1);
        if state.epoch == 0 {
            state.epoch = 1;
        }
        state.active = true;
        state.epoch
    }

    fn finish(&self, epoch: u64) {
        let mut state = self.state.lock().unwrap();
        if state.epoch == epoch {
            state.active = false;
        }
    }
}

impl FenceContext for PathFence {
    fn fence(&self) -> &PathFence {
        self
    }
}

impl FenceGuard {
    pub(super) fn is_current(&self) -> bool {
        self.context.fence().snapshot() == (self.epoch, true)
    }
}

impl Drop for FenceGuard {
    fn drop(&mut self) {
        self.context.fence().finish(self.epoch);
        self.active_fences.fetch_sub(1, Ordering::AcqRel);
    }
}

impl FenceTracker {
    pub(super) fn new() -> Self {
        Self {
            active_fences: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub(super) fn begin(&self, context: Arc<dyn FenceContext>) -> Option<FenceGuard> {
        self.active_fences
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                (current < MAX_ACTIVE_FENCES).then_some(current + 1)
            })
            .ok()?;
        let epoch = context.fence().begin();
        Some(FenceGuard {
            context,
            epoch,
            active_fences: self.active_fences.clone(),
        })
    }
}
