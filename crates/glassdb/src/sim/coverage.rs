//! Checks that a simulation run exercises structural changes.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use glassdb_backend::Backend;
use glassdb_concurr::rt;

use crate::{Database, Error};

use super::SimMedia;
use super::harness::{FaultConfig, SimWorkload, pct_sweep};

/// The percentage of the checked runs of a [`Covered::pct_sweep`] that must
/// make each required structural change. A schedule can delay a change past
/// the end of its run: for example, the instance that checks a merge last can
/// use an old cached copy of the merge target, which vetoes the merge. So the
/// sweep catches only a change that stops in most runs.
const SWEEP_MIN_SHARE_PERCENT: u64 = 10;

/// The structural changes that a run without faults must make.
#[derive(Debug, Clone, Copy, Default)]
pub struct StructuralCoverage {
    /// At least one instance splits a node.
    pub splits: bool,
    /// At least one instance merges a node.
    pub merges: bool,
}

/// Runs `workload`, and checks that runs without faults make the required
/// structural changes. A change of a policy or of a workload can stop all
/// structural changes, and the invariant of the workload alone still passes.
///
/// A run under a fixed schedule must make each required change. A
/// [`Covered::pct_sweep`] requires them only in a share of its runs. Sweep a
/// covered workload only with it: in the plain [`pct_sweep`], each run checks
/// its own changes.
#[derive(Debug, Clone, Default)]
pub struct Covered<W> {
    workload: W,
    requires: StructuralCoverage,
    /// The runs of the sweep that collects them, if any. Without a sweep,
    /// each run checks its own changes.
    sweep: Option<Arc<Mutex<SweepTally>>>,
}

/// The state of the covered workload, and the structural changes that the
/// instances made.
pub struct CoveredState<S> {
    inner: Arc<S>,
    split: AtomicBool,
    merged: AtomicBool,
}

/// The checked runs of one sweep, and how many of them made each structural
/// change.
#[derive(Debug, Default)]
struct SweepTally {
    runs: u64,
    splits: u64,
    merges: u64,
}

impl<W: SimWorkload> Covered<W> {
    /// Returns `workload`, which must make the changes of `requires`.
    pub fn new(workload: W, requires: StructuralCoverage) -> Self {
        Self {
            workload,
            requires,
            sweep: None,
        }
    }

    /// Runs a [`pct_sweep`] of the workload with `faults`. Each run checks
    /// the invariant of the workload. Then the sweep checks that a share of
    /// its runs without faults that finish in time made each required
    /// structural change. A sweep
    /// too small for one run of that share checks no change.
    pub fn pct_sweep(&self, faults: FaultConfig) {
        let tally = Arc::default();
        let swept = Self {
            workload: self.workload.clone(),
            requires: self.requires,
            sweep: Some(Arc::clone(&tally)),
        };
        pct_sweep(&swept, faults);
        let tally = tally.lock().unwrap();
        let min_runs = tally.runs * SWEEP_MIN_SHARE_PERCENT / 100;
        assert!(
            !self.requires.splits || tally.splits >= min_runs,
            "only {} of {} runs split a node, and the sweep needs {min_runs}",
            tally.splits,
            tally.runs,
        );
        assert!(
            !self.requires.merges || tally.merges >= min_runs,
            "only {} of {} runs merged a node, and the sweep needs {min_runs}",
            tally.merges,
            tally.runs,
        );
    }
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
        let stats = db.stats().topology;
        if stats.splits.completed > 0 {
            state.split.store(true, Ordering::Relaxed);
        }
        if stats.merges.completed > 0 {
            state.merged.store(true, Ordering::Relaxed);
        }
        result
    }

    async fn verify(&self, db: &Database, state: &CoveredState<W::State>, allow_in_doubt: bool) {
        self.workload.verify(db, &state.inner, allow_in_doubt).await;
        if allow_in_doubt {
            return;
        }
        let split = state.split.load(Ordering::Relaxed);
        let merged = state.merged.load(Ordering::Relaxed);
        if let Some(sweep) = &self.sweep {
            let mut tally = sweep.lock().unwrap();
            tally.runs += 1;
            tally.splits += u64::from(split);
            tally.merges += u64::from(merged);
            return;
        }
        assert!(
            !self.requires.splits || split,
            "no instance split a node, so the run did not exercise splits"
        );
        assert!(
            !self.requires.merges || merged,
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
