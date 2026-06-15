//! Deterministic Cycle fuzz target (ported from FoundationDB's `Cycle.cpp`).
//!
//! libFuzzer bytes are split into an `rng_seed`, a [`CycleWorkload`], a
//! [`FaultConfig`], and the remaining bytes, which are halved into a *schedule
//! tape* and a *fault tape* — the same layout as the `concurrent_tx` target. The
//! harness lays down a ring (`key(i) -> (i + 1) % N`) and runs every client as
//! its own task over a shared in-process backend on the in-repo deterministic
//! executor ([`glassdb::rt`], `--cfg sim`); each client repeatedly rotates three
//! consecutive ring edges. Because that rotation does not commute, any isolation
//! or atomicity break splits, shrinks, or grows the ring.
//!
//! [`replay_cycle_input`] reads back every node's next-pointer and asserts the
//! ring is still a single cycle of length `N`; any violation panics, which
//! libFuzzer reports as a crash. The invariant holds even with faults active,
//! since each swap is atomic. `cargo fuzz` overrides the `fuzz/.cargo/config.toml`
//! rustflags, so `--cfg sim --cfg tokio_unstable` must be passed through the
//! environment:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo +nightly fuzz run cycle <crash-file>
//! ```
#![no_main]

use libfuzzer_sys::fuzz_target;

// The decode-and-run logic lives in `glassdb::sim::replay_cycle_input` so the
// committed-corpus replay test (crates/glassdb/tests/fuzz_corpus.rs) exercises
// the exact same path as the fuzzer.
fuzz_target!(|data: &[u8]| glassdb::sim::replay_cycle_input(data));
