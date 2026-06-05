//! Deterministic-simulation self-checks for the concurrency fuzzer (ADR-008).
//!
//! These only build under the madsim simulator with the `sim` harness feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg madsim" cargo test -p glassdb --features sim
//! ```
//!
//! The central guarantee is that, for a fixed workload and seed, the engine
//! issues a **byte-for-byte identical stream of backend operations** on every
//! run. Two interleavings can reach the same final state while issuing
//! different operations, so an identical op stream — not just a matching final
//! result — is what proves the schedule itself replayed deterministically.
#![cfg(all(madsim, feature = "sim"))]

use glassdb::middleware::first_divergence;
use glassdb::sim::{
    run_and_assert, run_and_assert_with_faults, run_and_record, run_and_record_with_faults,
    FaultConfig, Op, Workload,
};
use madsim::runtime::Runtime;
use madsim::Config;

/// A contended workload: every client hammers overlapping keys with single- and
/// multi-key increments interleaved with read-only transactions.
fn contended_workload() -> Workload {
    Workload {
        clients: vec![
            vec![
                Op::Rmw(0),
                Op::MultiRmw(0, 1),
                Op::ReadOnly(vec![0, 1, 2]),
                Op::Rmw(2),
            ],
            vec![
                Op::Rmw(1),
                Op::MultiRmw(1, 2),
                Op::ReadOnly(vec![0, 1, 3]),
                Op::Rmw(0),
            ],
            vec![
                Op::MultiRmw(2, 3),
                Op::Rmw(3),
                Op::Rmw(0),
                Op::MultiRmw(0, 3),
            ],
        ],
    }
}

/// Runs the contended workload; used as the fixed function for
/// `Runtime::check_determinism` (which requires a plain `fn` pointer).
async fn run_contended() {
    run_and_assert(contended_workload()).await;
}

/// The op stream recorded for `workload` under `seed`.
fn record_once(seed: u64, workload: &Workload) -> Vec<glassdb::middleware::OpRecord> {
    let log =
        Runtime::with_seed_and_config(seed, Config::default()).block_on(run_and_record(workload));
    let recorded = log.lock().unwrap();
    recorded.clone()
}

#[test]
fn op_stream_is_byte_identical_across_runs() {
    let workload = contended_workload();
    // Seeds chosen to drive a range of interleavings (aborts, wound-wait
    // restarts, background pollers).
    for seed in [0u64, 1, 7, 42, 1234, 0xDEAD_BEEF] {
        let first = record_once(seed, &workload);
        let second = record_once(seed, &workload);
        assert!(
            !first.is_empty(),
            "seed {seed}: expected the workload to issue backend operations"
        );
        if let Some((idx, a, b)) = first_divergence(&first, &second) {
            panic!(
                "seed {seed}: backend op stream diverged at index {idx}\n  \
                 run 1 ({} ops): {a:?}\n  run 2 ({} ops): {b:?}",
                first.len(),
                second.len(),
            );
        }
    }
}

#[test]
fn distinct_seeds_can_produce_distinct_schedules() {
    // Not a correctness requirement, but if every seed produced the identical
    // op stream the scheduler would not be exploring anything. We only require
    // that *some* pair differs across a spread of seeds.
    let workload = contended_workload();
    let baseline = record_once(0, &workload);
    let differs = (1u64..16).any(|seed| record_once(seed, &workload) != baseline);
    assert!(
        differs,
        "no seed in 1..16 changed the schedule; the simulator may not be \
         varying interleavings"
    );
}

#[test]
fn check_determinism_on_result() {
    // A second, cheaper guard: madsim reruns the future with the same seed and
    // verifies its internal randomness/scheduling log matches.
    for seed in [0u64, 3, 99] {
        Runtime::check_determinism(seed, Config::default(), run_contended);
    }
}

#[test]
fn serializability_holds_under_contention() {
    // run_and_assert panics on any violation; reaching the end means the
    // invariant held for this seed.
    Runtime::with_seed_and_config(2024, Config::default()).block_on(run_contended());
}

/// The op stream recorded for `workload` under `seed` with the nemesis active.
fn record_once_faults(
    seed: u64,
    workload: &Workload,
    faults: FaultConfig,
) -> Vec<glassdb::middleware::OpRecord> {
    let log = Runtime::with_seed_and_config(seed, Config::default())
        .block_on(run_and_record_with_faults(workload, faults));
    let recorded = log.lock().unwrap();
    recorded.clone()
}

#[test]
fn op_stream_is_byte_identical_with_faults() {
    // Determinism must hold even with the fault nemesis running: scheduling,
    // time, randomness, and the fault schedule are all functions of the seed.
    let workload = contended_workload();
    let faults = FaultConfig::enabled(7);
    for seed in [0u64, 1, 7, 42, 1234, 0xDEAD_BEEF] {
        let first = record_once_faults(seed, &workload, faults);
        let second = record_once_faults(seed, &workload, faults);
        if let Some((idx, a, b)) = first_divergence(&first, &second) {
            panic!(
                "seed {seed}: faulted op stream diverged at index {idx}\n  \
                 run 1 ({} ops): {a:?}\n  run 2 ({} ops): {b:?}",
                first.len(),
                second.len(),
            );
        }
    }
}

#[test]
fn serializability_holds_under_faults() {
    // With faults the invariant relaxes to acked <= final <= started; a
    // violation (lost or fabricated write) panics inside run_and_assert.
    let workload = contended_workload();
    for seed in [0u64, 3, 99, 2024] {
        Runtime::with_seed_and_config(seed, Config::default()).block_on(
            run_and_assert_with_faults(workload.clone(), FaultConfig::enabled(9)),
        );
    }
}
