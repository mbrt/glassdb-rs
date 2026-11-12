//! Structural coverage checks of PCT sweeps.
//!
//! Each test runs its sweep in a child process, so that it can set the number
//! of seeds of the sweep.

use std::process::{Command, Output};

use glassdb::sim::{Covered, FaultConfig, SimWorkload, StructuralCoverage};
use glassdb::{Database, Error};

/// Set in the child processes of these tests, which run the sweeps.
const SWEEP_CHILD_VAR: &str = "GLASSDB_COVERAGE_SWEEP_CHILD";

const SPLITS_AND_MERGES: StructuralCoverage = StructuralCoverage {
    splits: true,
    merges: true,
};

const MERGES: StructuralCoverage = StructuralCoverage {
    splits: false,
    merges: true,
};

/// A workload whose only operation does nothing, so that no run splits or
/// merges a node.
#[derive(Clone)]
struct IdleWorkload {
    clients: Vec<Vec<()>>,
}

impl Default for IdleWorkload {
    fn default() -> Self {
        Self {
            clients: vec![vec![()]],
        }
    }
}

impl SimWorkload for IdleWorkload {
    type Op = ();
    type State = ();

    fn clients(&self) -> &[Vec<()>] {
        &self.clients
    }

    fn new_state(&self) {}

    async fn seed(&self, _db: &Database) {}

    async fn run_op(_db: &Database, _op: &(), _state: &()) -> Result<(), Error> {
        Ok(())
    }

    async fn verify(&self, _db: &Database, _state: &(), _allow_in_doubt: bool) {}
}

/// Sweeps the idle workload with `faults`, and requires `requires`, if this
/// process is the child of a test. Returns whether it is.
fn sweep_if_child(requires: StructuralCoverage, faults: FaultConfig) -> bool {
    if std::env::var_os(SWEEP_CHILD_VAR).is_none() {
        return false;
    }
    Covered::new(IdleWorkload::default(), requires).pct_sweep(faults);
    true
}

/// Runs the test `test` of this module in a child process, with a sweep of
/// `seeds` seeds.
fn run_child(test: &str, seeds: u64) -> Output {
    let test = format!("{}::{test}", module_path!().split_once("::").unwrap().1);
    Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &test, "--nocapture", "--test-threads=1"])
        .env(SWEEP_CHILD_VAR, "1")
        .env("GLASSDB_PCT_SEEDS", seeds.to_string())
        .output()
        .unwrap()
}

/// Asserts that the child process ran one test, and that the test passed. A
/// wrong test name selects no test, and the child process also succeeds.
fn assert_child_passed(output: &Output) {
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(output.status.success(), "{stderr}");
    assert!(stdout.contains("1 passed"), "{stdout}");
}

/// Asserts that the test in the child process failed with `message`.
fn assert_child_failed(output: &Output, message: &str) {
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(!output.status.success(), "{stderr}");
    assert!(stderr.contains(message), "{stderr}");
}

// The nightly sweep once failed because one schedule delayed a merge past the
// end of its run. A sweep requires the changes only in a share of its runs, so
// a sweep with fewer runs than one run of that share requires none.
#[test]
fn a_small_sweep_accepts_runs_without_structural_changes() {
    if sweep_if_child(SPLITS_AND_MERGES, FaultConfig::none()) {
        return;
    }
    let output = run_child("a_small_sweep_accepts_runs_without_structural_changes", 9);
    assert_child_passed(&output);
}

#[test]
fn a_sweep_without_splits_fails() {
    if sweep_if_child(SPLITS_AND_MERGES, FaultConfig::none()) {
        return;
    }
    let output = run_child("a_sweep_without_splits_fails", 10);
    assert_child_failed(
        &output,
        "only 0 of 10 runs split a node, and the sweep needs 1",
    );
}

#[test]
fn a_sweep_without_merges_fails() {
    if sweep_if_child(MERGES, FaultConfig::none()) {
        return;
    }
    let output = run_child("a_sweep_without_merges_fails", 10);
    assert_child_failed(
        &output,
        "only 0 of 10 runs merged a node, and the sweep needs 1",
    );
}

// Runs with faults can stop before their changes, so the sweep does not count
// them.
#[test]
fn a_sweep_does_not_count_runs_with_faults() {
    if sweep_if_child(SPLITS_AND_MERGES, FaultConfig::failures(7)) {
        return;
    }
    let output = run_child("a_sweep_does_not_count_runs_with_faults", 10);
    assert_child_passed(&output);
}
