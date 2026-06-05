//! Deterministic concurrency fuzz target (see ADR-008).
//!
//! libFuzzer bytes are split into a madsim `seed`, a [`Workload`], and a
//! [`FaultConfig`]. Each client opens its own [`glassdb::DB`] on its own madsim
//! node and reaches a dedicated storage node over the simulated network, while a
//! seeded nemesis injects network and node faults. Scheduling, time,
//! randomness, and the fault schedule are all a deterministic function of the
//! seed. [`run_and_assert_with_faults`] panics on any invariant violation
//! (`acked <= final <= started`, plus non-negativity and serializability),
//! which libFuzzer reports as a crash; the crashing input reproduces the exact
//! schedule via (madsim must be enabled — `cargo fuzz` overrides `config.toml`
//! rustflags, so pass it through the environment):
//!
//! ```bash
//! RUSTFLAGS="--cfg madsim" cargo +nightly fuzz run concurrent_tx <crash-file>
//! ```
#![no_main]

use arbitrary::{Arbitrary, Unstructured};
use glassdb::sim::{run_and_assert_with_faults, FaultConfig, Workload};
use libfuzzer_sys::fuzz_target;
use madsim::runtime::Runtime;
use madsim::Config;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let seed: u64 = u.arbitrary().unwrap_or(0);
    let workload = Workload::arbitrary(&mut u).unwrap_or_default();
    let faults = FaultConfig::arbitrary(&mut u).unwrap_or_default();
    Runtime::with_seed_and_config(seed, Config::default())
        .block_on(run_and_assert_with_faults(workload, faults));
});
