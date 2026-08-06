//! Mutation outcome normalization and cancellation-safe reconciliation.

use std::sync::Arc;

use glassdb_backend::BackendError;

use super::knowledge::{Expected, Knowledge};
use super::path_lane::PathPermit;
use crate::disk_cache::{FenceGuard, PersistentCache};
use crate::error::StorageError;
use crate::timeline::SequencePoint;

/// The knowledge transition implied by a completed backend mutation call.
pub(super) enum MutationOutcome<T> {
    Success {
        value: T,
        current_at: Option<SequencePoint>,
    },
    Conflict,
    DefiniteFailure(StorageError),
    Uncertain(StorageError),
}

impl<T> MutationOutcome<T> {
    pub(super) fn success(value: T, current_at: Option<SequencePoint>) -> Self {
        Self::Success { value, current_at }
    }

    pub(super) fn conflict() -> Self {
        Self::Conflict
    }

    pub(super) fn failed(error: BackendError) -> Self {
        match error {
            BackendError::Unavailable(message) => {
                Self::Uncertain(StorageError::Unavailable(message))
            }
            error => Self::DefiniteFailure(error.into()),
        }
    }
}

/// Owns one admitted mutation until its knowledge and persistent state agree.
pub(super) struct MutationRound {
    knowledge: Knowledge,
    persistent: Option<PersistentCache>,
    path: Arc<str>,
    expected: Expected,
    permit: Option<PathPermit>,
    l2_fence: Option<FenceGuard>,
    armed: bool,
}

impl MutationRound {
    pub(super) fn new(
        knowledge: Knowledge,
        persistent: Option<PersistentCache>,
        path: Arc<str>,
        mut expected: Expected,
        permit: PathPermit,
    ) -> Self {
        knowledge.capture_expected(&path, &mut expected);
        Self {
            knowledge,
            persistent,
            path,
            expected,
            permit: Some(permit),
            l2_fence: None,
            armed: true,
        }
    }

    /// Reconciles one normalized backend outcome before releasing its path lane.
    pub(super) fn finish<T, R>(
        mut self,
        outcome: MutationOutcome<T>,
        apply_success: impl FnOnce(T) -> R,
    ) -> Result<Option<R>, StorageError> {
        match outcome {
            MutationOutcome::Success { value, current_at } => {
                self.begin_path_change();
                let result = apply_success(value);
                if let Some(current_at) = current_at {
                    self.knowledge.advance_expected(&self.expected, current_at);
                }
                self.invalidate_l2();
                self.complete();
                Ok(Some(result))
            }
            MutationOutcome::Conflict => {
                self.begin_path_change();
                self.knowledge
                    .invalidate_expected(&self.path, &self.expected);
                self.invalidate_l2();
                self.complete();
                Ok(None)
            }
            MutationOutcome::DefiniteFailure(error) => {
                self.complete();
                Err(error)
            }
            MutationOutcome::Uncertain(error) => {
                self.begin_path_change();
                self.knowledge.invalidate(&self.path);
                self.invalidate_l2();
                self.complete();
                Err(error)
            }
        }
    }

    fn begin_path_change(&mut self) {
        if self.l2_fence.is_some() {
            return;
        }
        let Some(persistent) = &self.persistent else {
            return;
        };
        let permit = self
            .permit
            .as_ref()
            .expect("an active mutation retains its path permit");
        let state = permit.state();
        self.l2_fence = persistent.begin_fence(state.l2_fence().clone(), state.clone());
    }

    fn invalidate_l2(&mut self) {
        let (Some(persistent), Some(fence)) = (&self.persistent, self.l2_fence.take()) else {
            return;
        };
        persistent.invalidate(self.path.clone(), fence);
    }

    fn complete(&mut self) {
        self.armed = false;
        self.permit.take();
    }
}

impl Drop for MutationRound {
    fn drop(&mut self) {
        if self.armed {
            self.begin_path_change();
            self.knowledge.invalidate(&self.path);
            self.invalidate_l2();
        }
        self.permit.take();
    }
}
