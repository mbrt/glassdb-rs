//! Deterministic-simulation self-checks for the concurrency fuzzer
//! (ADR-010/011).
//!
//! These only build under the in-repo simulation executor with the `sim`
//! harness feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo test -p glassdb --features sim --test sim
//! ```
//!
//! The central guarantee is that, for a fixed workload, schedule tape, and seed,
//! the engine issues a **byte-for-byte identical stream of backend operations**
//! on every run. Two interleavings can reach the same final state while issuing
//! different operations, so an identical op stream — not just a matching final
//! result — is what proves the schedule itself replayed deterministically.
use crate::sim_support::{
    assert_no_divergence, assert_slow_mutation_modes, fault_tape, record_faults_with_tape, tape,
};

use glassdb::exec::{TapeScheduler, block_on_with};
use glassdb::middleware::OpRecord;
use glassdb::sim::{
    FaultConfig, RmwOp, RmwWorkload, pct_record, pct_sweep, run_and_assert,
    run_and_assert_with_faults, run_and_record,
};

/// A contended workload: every client hammers overlapping keys with single- and
/// multi-key increments interleaved with read-only transactions.
fn contended_workload() -> RmwWorkload {
    RmwWorkload {
        clients: vec![
            vec![
                RmwOp::Rmw(0),
                RmwOp::MultiRmw(0, 1),
                RmwOp::ReadOnly(vec![0, 1, 2]),
                RmwOp::Rmw(2),
            ],
            vec![
                RmwOp::Rmw(1),
                RmwOp::MultiRmw(1, 2),
                RmwOp::ReadOnly(vec![0, 1, 3]),
                RmwOp::Rmw(0),
            ],
            vec![
                RmwOp::MultiRmw(2, 3),
                RmwOp::Rmw(3),
                RmwOp::Rmw(0),
                RmwOp::MultiRmw(0, 3),
            ],
        ],
    }
}

fn record_once(seed: u64, workload: &RmwWorkload) -> Vec<OpRecord> {
    let workload = workload.clone();
    let log = block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
        run_and_record(&workload).await
    });
    log.lock().unwrap().clone()
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
        assert_no_divergence(&format!("seed {seed}: backend"), &first, &second);
    }
}

#[test]
fn distinct_tapes_can_produce_distinct_schedules() {
    // Not a correctness requirement, but if every tape produced the identical op
    // stream the scheduler would not be exploring anything. We only require that
    // *some* tape differs across a spread of seeds.
    let workload = contended_workload();
    let baseline = record_once(0, &workload);
    let differs = (1u64..16).any(|seed| record_once(seed, &workload) != baseline);
    assert!(
        differs,
        "no tape in 1..16 changed the schedule; the scheduler may not be \
         varying interleavings"
    );
}

#[test]
fn existing_rmw_workload_exercises_inline_pressure_splitting() {
    let workload = contended_workload();
    let recorded = record_once(0, &workload);
    assert!(
        recorded.iter().any(|op| op.path.contains("/_s/")),
        "the existing RMW schedule should reach a structural split"
    );
}

#[test]
fn serializability_holds_under_contention() {
    // run_and_assert panics on any violation; reaching the end means the
    // invariant held for this tape.
    for seed in [0u64, 3, 99, 2024] {
        let workload = contended_workload();
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert(workload).await
        });
    }
}

#[test]
fn serializability_holds_under_faults() {
    // With faults the invariant relaxes to acked <= final <= started; a
    // violation (lost or fabricated write) panics inside run_and_assert.
    let workload = contended_workload();
    for seed in [0u64, 3, 99, 2024] {
        let w = workload.clone();
        let ft = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert_with_faults(w, FaultConfig::failures(9), seed, ft).await
        });
    }
}

#[test]
fn serializability_holds_with_slow_mutations() {
    assert_slow_mutation_modes("RMW workload", &contended_workload());
}

#[test]
fn fault_tape_guides_the_fault_schedule() {
    // Same schedule tape and seed but different *fault* tapes must be able to
    // change the recorded op stream; otherwise the fault schedule would only be
    // seed-sampled, not coverage-guidable. An all-low tape fires every fault; an
    // all-high tape fires none. The intensity must be high enough that the
    // impactful faults (fail-before / lost-ack) have non-zero probability.
    let workload = contended_workload();
    let faults = FaultConfig::failures(128);
    let differs = [0u64, 1, 7, 42, 1234].iter().any(|&seed| {
        let all_on = record_faults_with_tape(seed, &workload, faults, vec![0u8; 4096]);
        let all_off = record_faults_with_tape(seed, &workload, faults, vec![0xffu8; 4096]);
        all_on != all_off
    });
    assert!(
        differs,
        "the fault tape changed no op stream; the fault schedule may not be \
         tape-guided"
    );
}

#[test]
fn recovery_holds_under_crash_restart_and_outages() {
    // High intensity drives multiple client crashes (→ crash-and-restart on the
    // same backend) and sustained, all-or-nothing per-client transport outages.
    // The acked-bounds invariant must survive the recovery paths those faults
    // exercise: lease expiry, lock-lease recovery, and a restarted client
    // reclaiming its own orphaned locks.
    let workload = contended_workload();
    let faults = FaultConfig::failures(200);
    for seed in [0u64, 1, 7, 42, 99, 1234] {
        let workload = workload.clone();
        let ft = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert_with_faults(workload, faults, seed, ft).await
        });
    }
}

#[test]
fn pct_schedule_is_byte_identical_per_seed() {
    // The PCT policy must be just as reproducible as the tape policy: a fixed
    // seed yields a byte-for-byte identical op stream across runs.
    let workload = contended_workload();
    let faults = FaultConfig::failures(5);
    for seed in [0u64, 1, 7, 42, 1234] {
        let first = pct_record(&workload, faults, seed);
        let second = pct_record(&workload, faults, seed);
        let first = first.lock().unwrap().clone();
        let second = second.lock().unwrap().clone();
        assert_no_divergence(&format!("seed {seed}: PCT"), &first, &second);
    }
}

#[test]
fn pct_seed_breadth_holds_serializability() {
    // Seed-breadth sweep: many PCT schedules over the contended workload, with
    // and without faults. Any invariant violation panics inside the sweep.
    let workload = contended_workload();
    pct_sweep(&workload, FaultConfig::failures(7), 0..32);
    pct_sweep(&workload, FaultConfig::none(), 0..16);
}
