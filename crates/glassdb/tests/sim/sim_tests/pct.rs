//! Failure reports of PCT runs.

use std::os::unix::process::ExitStatusExt;
use std::process::Command;

use glassdb::sim::{FaultConfig, SimWorkload, pct_assert};
use glassdb::{Database, Error};

/// The signal of an abort.
const SIGABRT: i32 = 6;

/// Set in the child process of
/// `a_pct_failure_names_its_seed_even_when_the_process_aborts`.
const ABORT_CHILD_VAR: &str = "GLASSDB_PCT_ABORT_CHILD";

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

    fn clients(&self) -> &[Vec<()>] {
        &self.clients
    }

    fn new_state(&self) {}

    async fn seed(&self, _db: &Database) {}

    async fn run_op(_db: &Database, _op: &(), _state: &()) -> Result<(), Error> {
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
