//! Deterministic concurrency fuzz target (see ADR-008 and ADR-011).
//!
//! libFuzzer bytes are split into an `rng_seed`, a [`Workload`], a
//! [`FaultConfig`], and the remaining bytes, which are halved into a *schedule
//! tape* and a *fault tape*. The harness runs every client as its own task over
//! a shared in-process backend on the in-repo deterministic executor
//! ([`glassdb::rt`], `--cfg sim`); a [`TapeScheduler`] consumes the schedule
//! tape to choose task interleavings, while the fault tape guides the fault
//! schedule (backend faults and crash timing). Both dimensions are therefore
//! part of the libFuzzer input and become coverage-guidable. Scheduling, time,
//! randomness, and faults are all a deterministic function of the input, so a
//! crashing input replays the exact interleaving.
//!
//! [`run_and_assert_with_faults`] panics on any invariant violation
//! (`acked <= final <= started`, plus non-negativity and serializability),
//! which libFuzzer reports as a crash. `cargo fuzz` overrides the
//! `fuzz/.cargo/config.toml` rustflags, so `--cfg sim` must be passed through
//! the environment (cargo-fuzz appends its sanitizer/coverage flags to it):
//!
//! ```bash
//! RUSTFLAGS="--cfg sim" cargo +nightly fuzz run concurrent_tx <crash-file>
//! ```
#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use glassdb::rt::{block_on_with, TapeScheduler};
use glassdb::sim::{run_and_assert_with_faults, FaultConfig, Workload};
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let seed: u64 = u.arbitrary().unwrap_or(0);
    let workload = Workload::arbitrary(&mut u).unwrap_or_default();
    let faults = FaultConfig::arbitrary(&mut u).unwrap_or_default();
    // Split the remaining bytes into a schedule tape and a fault tape. The
    // scheduler falls back to a deterministic default once its tape is spent, and
    // the fault schedule falls back to the seed, so a short tape is fine.
    let rest = u.take_rest();
    let mid = rest.len() / 2;
    let schedule_tape = rest[..mid].to_vec();
    let fault_tape = rest[mid..].to_vec();
    block_on_with(TapeScheduler::new(schedule_tape), seed, async move {
        run_and_assert_with_faults(workload, faults, seed, fault_tape).await
    });
});
