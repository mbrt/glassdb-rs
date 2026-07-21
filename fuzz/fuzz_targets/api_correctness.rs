//! Deterministic transaction-API fuzz target, inspired by FoundationDB's
//! `FuzzApiCorrectness` workload.
//!
//! Each client runs randomized transaction programs containing reads, writes,
//! deletes, and explicit aborts. The harness checks read-your-writes,
//! repeatable-read, abort, atomicity, and final-state invariants against an exact
//! model that retains every possible in-doubt commit outcome. Schedule and fault
//! tapes make interleavings, failures, and slow mutations coverage-guidable and
//! exactly reproducible.
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo +nightly fuzz run api_correctness <crash-file>
//! ```
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| glassdb::sim::replay_input::<glassdb::sim::ApiWorkload>(data));
