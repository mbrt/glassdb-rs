//! Deterministic-simulation self-checks for the Cycle fuzz workload (ported from
//! FoundationDB's `Cycle.cpp`; see ADR-010/011).
//!
//! These only build under the in-repo simulation executor with the `sim` harness
//! feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo test -p glassdb --features sim
//! ```
//!
//! The Cycle workload is the serializability oracle the commutative
//! RMW-increment workload (`concurrent_sim.rs`) cannot be: each transaction
//! rotates three consecutive ring edges, an operation that does not commute, so
//! any isolation or atomicity break splits, shrinks, or grows the ring. The
//! harness asserts the ring is still a single cycle of length `N`. As with the
//! increment workload, a fixed (workload, tape, seed) must issue a byte-for-byte
//! identical backend op stream on every run.
#![cfg(all(sim, feature = "sim"))]

mod sim_support;

use sim_support::{
    assert_no_divergence, fault_tape, record_faults_with_tape, record_once, record_with_tapes, tape,
};

use glassdb::rt::{TapeScheduler, block_on_with};
use glassdb::sim::{
    CycleWorkload, FaultConfig, pct_record, pct_sweep, run_and_assert, run_and_assert_with_faults,
};

/// A contended ring: a small node count with several clients each rotating
/// overlapping edges, so transactions conflict on shared keys. A few concurrent
/// ring snapshots run alongside, exercising the read-side serializability oracle
/// and `Tx`'s concurrent-read path.
fn contended_cycle() -> CycleWorkload {
    CycleWorkload {
        node_count: 6,
        clients: vec![vec![0, 2, 4, 1], vec![1, 3, 5, 0], vec![2, 4, 0, 3]],
        snapshot_reads: 3,
    }
}

/// Boundary-heavy Cycle workload: larger ring, maximum generated client shape,
/// and read-only snapshots enabled to exercise concurrent reads.
fn max_snapshot_cycle() -> CycleWorkload {
    CycleWorkload {
        node_count: 12,
        clients: vec![
            vec![0, 2, 4, 6, 8, 10, 1, 3],
            vec![1, 3, 5, 7, 9, 11, 2, 4],
            vec![2, 4, 6, 8, 10, 0, 3, 5],
            vec![3, 5, 7, 9, 11, 1, 4, 6],
        ],
        snapshot_reads: 8,
    }
}

#[test]
fn op_stream_is_byte_identical_across_runs() {
    let workload = contended_cycle();
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
fn ring_invariant_holds_under_contention() {
    // run_and_assert panics on any violation; reaching the end means the
    // ring stayed a single cycle of length N for this tape.
    for seed in [0u64, 3, 99, 2024] {
        let workload = contended_cycle();
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert(workload).await
        });
    }
}

#[test]
fn op_stream_is_byte_identical_with_faults() {
    // Determinism must hold even with faults active: scheduling, time,
    // randomness, and the fault schedule are all functions of the tape and seed.
    let workload = contended_cycle();
    let faults = FaultConfig::enabled(7);
    for seed in [0u64, 1, 7, 42, 1234, 0xDEAD_BEEF] {
        let first = record_faults_with_tape(seed, &workload, faults, fault_tape(seed));
        let second = record_faults_with_tape(seed, &workload, faults, fault_tape(seed));
        assert_no_divergence(&format!("seed {seed}: faulted"), &first, &second);
    }
}

#[test]
fn ring_invariant_holds_under_faults() {
    // The ring invariant is robust to faults: each swap is atomic, so the ring
    // stays a single N-cycle whether a swap commits or aborts. A lost or
    // fabricated write that broke the ring would panic inside the harness.
    let workload = contended_cycle();
    for seed in [0u64, 3, 99, 2024] {
        let w = workload.clone();
        let ft = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert_with_faults(w, FaultConfig::enabled(9), seed, ft).await
        });
    }
}

#[test]
fn ring_holds_under_crash_restart_and_outages() {
    // High intensity drives multiple client crashes (-> crash-and-restart on the
    // same backend) and sustained per-client transport outages. Each run must
    // stay byte-for-byte deterministic per (tape, seed), and the ring invariant
    // (asserted inside the harness) must survive the recovery paths those faults
    // exercise: lease expiry, lock-lease recovery, and a restarted client
    // reclaiming its own orphaned locks.
    let workload = contended_cycle();
    let faults = FaultConfig::enabled(200);
    for seed in [0u64, 1, 7, 42, 99, 1234] {
        let ft = fault_tape(seed);
        let first = record_faults_with_tape(seed, &workload, faults, ft.clone());
        let second = record_faults_with_tape(seed, &workload, faults, ft);
        assert_no_divergence(&format!("seed {seed}: recovery"), &first, &second);
    }
}

#[test]
fn boundary_tapes_replay_deterministically() {
    let workload = max_snapshot_cycle();
    let faults = FaultConfig::enabled(128);
    for (schedule, fault_tape) in [
        (Vec::new(), Vec::new()),
        (vec![0], Vec::new()),
        (vec![255, 1], vec![0; 16]),
    ] {
        let first = record_with_tapes(77, &workload, faults, schedule.clone(), fault_tape.clone());
        let second = record_with_tapes(77, &workload, faults, schedule, fault_tape);
        assert_no_divergence("cycle boundary tape replay", &first, &second);
    }
}

#[test]
fn pct_schedule_is_byte_identical_per_seed() {
    // The PCT policy must be just as reproducible as the tape policy: a fixed
    // seed yields a byte-for-byte identical op stream across runs.
    let workload = contended_cycle();
    let faults = FaultConfig::enabled(5);
    for seed in [0u64, 1, 7, 42, 1234] {
        let first = pct_record(&workload, faults, seed);
        let second = pct_record(&workload, faults, seed);
        let first = first.lock().unwrap().clone();
        let second = second.lock().unwrap().clone();
        assert_no_divergence(&format!("seed {seed}: PCT"), &first, &second);
    }
}

#[test]
fn concurrent_snapshot_reader_runs_and_stays_deterministic() {
    // The workload verifies directly that the configured read-only observer ran;
    // backend-op counts are not evidence of execution because a snapshot can be
    // satisfied by retained decoded-object evidence. The two runs must still be
    // byte-for-byte deterministic even though the observer issues all N pointer
    // reads concurrently within one transaction.
    let with_reader = contended_cycle(); // snapshot_reads = 3
    for seed in [0u64, 1, 42, 1234] {
        let first = record_once(seed, &with_reader);
        let second = record_once(seed, &with_reader);
        assert_no_divergence(&format!("seed {seed}: snapshot-reader"), &first, &second);
    }
}

#[test]
fn pct_seed_breadth_holds_ring_invariant() {
    // Seed-breadth sweep: many PCT schedules over the contended ring, with and
    // without faults. Any invariant violation panics inside the sweep.
    let workload = contended_cycle();
    pct_sweep(&workload, FaultConfig::enabled(7), 0..32);
    pct_sweep(&workload, FaultConfig::none(), 0..16);
}
