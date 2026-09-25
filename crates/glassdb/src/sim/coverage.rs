//! Checks that a simulation run exercises structural changes.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use glassdb_backend::Backend;
use glassdb_concurr::rt;

use crate::{Database, Error};

use super::SimMedia;
use super::harness::SimWorkload;

/// The structural changes that a run without faults must make.
#[derive(Debug, Clone, Copy, Default)]
pub struct StructuralCoverage {
    /// At least one instance splits a node.
    pub splits: bool,
    /// At least one instance merges a node.
    pub merges: bool,
}

/// Runs `workload`, and checks that a run without faults makes the required
/// structural changes. A change of a policy or of a workload can stop all
/// structural changes, and the invariant of the workload alone still passes.
#[derive(Debug, Clone, Default)]
pub struct Covered<W> {
    /// The workload whose invariant the run checks.
    pub workload: W,
    /// The structural changes that the run must make.
    pub requires: StructuralCoverage,
}

/// The state of the covered workload, and the structural changes that the
/// instances made.
pub struct CoveredState<S> {
    inner: Arc<S>,
    split: AtomicBool,
    merged: AtomicBool,
}

impl<W: SimWorkload> SimWorkload for Covered<W> {
    type Op = W::Op;
    type State = CoveredState<W::State>;

    const CONTINUE_AFTER_ADMISSIBLE_ERROR: bool = W::CONTINUE_AFTER_ADMISSIBLE_ERROR;

    fn clients(&self) -> &[Vec<W::Op>] {
        self.workload.clients()
    }

    fn new_state(&self) -> CoveredState<W::State> {
        CoveredState {
            inner: Arc::new(self.workload.new_state()),
            split: AtomicBool::new(false),
            merged: AtomicBool::new(false),
        }
    }

    async fn open_db(
        backend: &Arc<dyn Backend>,
        media: Option<SimMedia>,
    ) -> Result<Database, Error> {
        W::open_db(backend, media).await
    }

    async fn seed(&self, db: &Database) {
        self.workload.seed(db).await;
    }

    async fn run_op(
        db: &Database,
        op: &W::Op,
        state: &CoveredState<W::State>,
    ) -> Result<(), Error> {
        let result = W::run_op(db, op, &state.inner).await;
        let stats = db.stats().splitter;
        if stats.completed > 0 {
            state.split.store(true, Ordering::Relaxed);
        }
        if stats.merges > 0 {
            state.merged.store(true, Ordering::Relaxed);
        }
        result
    }

    async fn verify(&self, db: &Database, state: &CoveredState<W::State>, allow_in_doubt: bool) {
        self.workload.verify(db, &state.inner, allow_in_doubt).await;
        if allow_in_doubt {
            return;
        }
        assert!(
            !self.requires.splits || state.split.load(Ordering::Relaxed),
            "no instance split a node, so the run did not exercise splits"
        );
        assert!(
            !self.requires.merges || state.merged.load(Ordering::Relaxed),
            "no instance merged a node, so the run did not exercise merges"
        );
    }

    fn spawn_observer(
        &self,
        backbone: &Arc<dyn Backend>,
        state: &Arc<CoveredState<W::State>>,
        media: Option<SimMedia>,
    ) -> Option<rt::JoinHandle<()>> {
        self.workload.spawn_observer(backbone, &state.inner, media)
    }
}
