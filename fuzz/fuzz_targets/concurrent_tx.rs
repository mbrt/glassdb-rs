//! Deterministic concurrency fuzz target (see ADR-008 and ADR-011).
//!
//! libFuzzer bytes are split into an `rng_seed`, a [`RmwWorkload`], a
//! [`FaultConfig`], and the remaining bytes, which are halved into a *schedule
//! tape* and a *fault tape*. The harness runs every client as its own task over
//! a shared in-process backend on the in-repo deterministic executor
//! ([`glassdb::rt`], `--cfg sim`); a [`TapeScheduler`] consumes the schedule
//! tape to choose task interleavings, while the fault tape guides transport
//! failures, crash timing, and one-shot slow mutations. Both dimensions are
//! therefore part of the libFuzzer input and become coverage-guidable.
//! Scheduling, time, randomness, failures, and delays are all deterministic
//! functions of the input, so a crashing input replays the exact interleaving.
//!
//! [`run_and_assert_with_faults`] panics on any invariant violation
//! (`acked <= final <= started`, plus non-negativity and serializability),
//! which libFuzzer reports as a crash. `cargo fuzz` overrides the
//! `fuzz/.cargo/config.toml` rustflags, so `--cfg sim --cfg tokio_unstable` must
//! be passed through the environment (cargo-fuzz appends its sanitizer/coverage
//! flags to it):
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo +nightly fuzz run concurrent_tx <crash-file>
//! ```
#![no_main]

use libfuzzer_sys::fuzz_target;

// The decode-and-run logic lives in the generic `glassdb::sim::replay_input` so
// the committed-corpus replay test (crates/glassdb/tests/fuzz_corpus.rs)
// exercises the exact same path as the fuzzer.
fuzz_target!(|data: &[u8]| glassdb::sim::replay_input::<glassdb::sim::RmwWorkload>(data));
