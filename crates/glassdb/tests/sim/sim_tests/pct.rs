//! Failure reports and seeds of PCT runs.

use std::os::unix::process::ExitStatusExt;
use std::process::{Command, Output};

use glassdb::sim::{FaultConfig, InstanceId, SimWorkload, pct_assert, pct_sweep};
use glassdb::{Database, Error};

/// The signal of an abort.
const SIGABRT: i32 = 6;

/// Set in the child process of
/// `a_pct_failure_names_its_seed_even_when_the_process_aborts`.
const ABORT_CHILD_VAR: &str = "GLASSDB_PCT_ABORT_CHILD";

/// Set in the child processes of the tests that sweep a failing workload.
const SWEEP_CHILD_VAR: &str = "GLASSDB_PCT_SWEEP_CHILD";

/// Sets the first seed of a sweep.
const SEED_START_VAR: &str = "GLASSDB_PCT_SEED_START";

/// A workload whose only operation panics, so the first run of a sweep fails
/// and names its seed.
#[derive(Clone)]
struct FailingWorkload {
    clients: Vec<Vec<()>>,
}

impl Default for FailingWorkload {
    fn default() -> Self {
        Self {
            clients: vec![vec![()]],
        }
    }
}

impl SimWorkload for FailingWorkload {
    type Op = ();
    type State = ();
    type Instance = ();

    fn clients(&self) -> &[Vec<()>] {
        &self.clients
    }

    fn new_state(&self) {}

    async fn seed(&self, _db: &Database) {}

    fn open_instance(_db: &Database, _id: InstanceId) {}

    async fn run_op(_db: &Database, _instance: &(), _op: &(), _state: &()) -> Result<(), Error> {
        panic!("the operation fails");
    }

    async fn verify(&self, _db: &Database, _state: &(), _allow_in_doubt: bool) {}
}

/// A workload whose only operation panics, and then panics again in a
/// destructor while the first panic unwinds. The second panic aborts the
/// process.
#[derive(Clone)]
struct AbortingWorkload {
    clients: Vec<Vec<()>>,
}

impl Default for AbortingWorkload {
    fn default() -> Self {
        Self {
            clients: vec![vec![()]],
        }
    }
}

struct PanicDuringUnwind;

impl Drop for PanicDuringUnwind {
    fn drop(&mut self) {
        if std::thread::panicking() {
            panic!("a destructor panics during the unwind");
        }
    }
}

impl SimWorkload for AbortingWorkload {
    type Op = ();
    type State = ();
    type Instance = ();

    fn clients(&self) -> &[Vec<()>] {
        &self.clients
    }

    fn new_state(&self) {}

    async fn seed(&self, _db: &Database) {}

    fn open_instance(_db: &Database, _id: InstanceId) {}

    async fn run_op(_db: &Database, _instance: &(), _op: &(), _state: &()) -> Result<(), Error> {
        let _destructor = PanicDuringUnwind;
        panic!("the operation fails");
    }

    async fn verify(&self, _db: &Database, _state: &(), _allow_in_doubt: bool) {}
}

// The nightly sweep once lost its seed: a destructor panicked while the first
// panic unwound, and the process aborted before the sweep reported the seed.
// The child process repeats that sequence.
#[test]
fn a_pct_failure_names_its_seed_even_when_the_process_aborts() {
    if std::env::var_os(ABORT_CHILD_VAR).is_some() {
        pct_assert(&AbortingWorkload::default(), FaultConfig::none(), 41);
        return;
    }

    let test = format!(
        "{}::a_pct_failure_names_its_seed_even_when_the_process_aborts",
        module_path!().split_once("::").unwrap().1
    );
    let output = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", &test, "--nocapture", "--test-threads=1"])
        .env(ABORT_CHILD_VAR, "1")
        .output()
        .unwrap();
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert_eq!(output.status.signal(), Some(SIGABRT), "{stderr}");
    assert!(stderr.contains("PCT run failed at seed 41"), "{stderr}");
}

/// Sweeps the failing workload if this process is the child of a test.
/// Returns whether it is.
fn sweep_if_child() -> bool {
    if std::env::var_os(SWEEP_CHILD_VAR).is_none() {
        return false;
    }
    pct_sweep(&FailingWorkload::default(), FaultConfig::none());
    true
}

/// Runs the test `test` of this module in a child process that sweeps the
/// failing workload from `start`, or from a random seed.
fn run_sweep_child(test: &str, start: Option<u64>) -> Output {
    let test = format!("{}::{test}", module_path!().split_once("::").unwrap().1);
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", &test, "--nocapture", "--test-threads=1"])
        .env(SWEEP_CHILD_VAR, "1")
        .env_remove(SEED_START_VAR);
    if let Some(start) = start {
        command.env(SEED_START_VAR, start.to_string());
    }
    command.output().unwrap()
}

/// Returns the seed of the failed run that the child process reports.
fn failed_seed(output: &Output) -> u64 {
    let stderr = String::from_utf8_lossy(&output.stderr);
    let (_, rest) = stderr
        .split_once("PCT run failed at seed ")
        .unwrap_or_else(|| panic!("no failed seed: {stderr}"));
    rest.split_once(' ').unwrap().0.parse().unwrap()
}

#[test]
fn a_sweep_starts_at_the_seed_that_the_environment_sets() {
    if sweep_if_child() {
        return;
    }
    let start = 1 << 40;
    let output = run_sweep_child(
        "a_sweep_starts_at_the_seed_that_the_environment_sets",
        Some(start),
    );
    assert_eq!(failed_seed(&output), start);
}

// Sweeps that start from the same seed run the same schedules again, so each
// sweep starts from a random seed.
#[test]
fn sweeps_start_at_different_seeds() {
    if sweep_if_child() {
        return;
    }
    let first = run_sweep_child("sweeps_start_at_different_seeds", None);
    let second = run_sweep_child("sweeps_start_at_different_seeds", None);
    assert_ne!(failed_seed(&first), failed_seed(&second));
}
