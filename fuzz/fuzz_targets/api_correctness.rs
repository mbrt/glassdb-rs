//! Deterministic transaction-API fuzz target, inspired by FoundationDB's
//! `FuzzApiCorrectness` workload.
//!
//! Each client runs randomized transaction programs containing key operations,
//! collection creation/opening/listing, nested collections, collection data,
//! drops, and explicit aborts. The harness checks read-your-writes, path/direct
//! lookup agreement, non-recursive drop, abort, atomicity, and final-state
//! invariants against an exact model that retains every possible in-doubt
//! commit outcome. Schedule and fault tapes make interleavings, backend
//! failures, cache-media failures, and slow mutations coverage-guidable and
//! exactly reproducible. Every decoded workload runs both without and with the
//! persistent cache; the cached run uses only basic media delays and pre-effect
//! failures.
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo +nightly fuzz run api-correctness <crash-file>
//! ```
#![no_main]
#![recursion_limit = "256"]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| glassdb::sim::replay_input::<glassdb::sim::ApiWorkload>(data));
