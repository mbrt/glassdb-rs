//! Deterministic concurrency fuzz target (see ADR-008).
//!
//! libFuzzer bytes are split into a madsim `seed` and a [`Workload`]. The
//! workload runs an N-client read-modify-write mix over a shared in-memory
//! backend on the madsim runtime, which makes scheduling, time, and randomness
//! a deterministic function of the seed. [`run_and_assert`] panics on any
//! serializability violation, which libFuzzer reports as a crash; the crashing
//! input reproduces the exact schedule via (madsim must be enabled — `cargo
//! fuzz` overrides `config.toml` rustflags, so pass it through the environment):
//!
//! ```bash
//! RUSTFLAGS="--cfg madsim" cargo +nightly fuzz run concurrent_tx <crash-file>
//! ```
#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use glassdb::sim::{run_and_assert, Workload};
use libfuzzer_sys::fuzz_target;
use madsim::runtime::Runtime;
use madsim::Config;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let seed: u64 = u.arbitrary().unwrap_or(0);
    let workload = Workload::arbitrary(&mut u).unwrap_or_default();
    Runtime::with_seed_and_config(seed, Config::default()).block_on(run_and_assert(workload));
});
