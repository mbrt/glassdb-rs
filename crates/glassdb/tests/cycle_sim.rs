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

use glassdb::middleware::{first_divergence, OpRecord};
use glassdb::rt::{block_on_with, TapeScheduler};
use glassdb::sim::{
    cycle_pct_record, cycle_pct_sweep, run_cycle_and_assert, run_cycle_and_assert_with_faults,
    run_cycle_and_record, run_cycle_and_record_with_faults, CycleWorkload, FaultConfig,
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

/// A deterministic schedule tape derived from `seed` (a simple LCG expansion),
/// long enough to cover every scheduling decision a run makes.
fn tape(seed: u64) -> Vec<u8> {
    let mut s = seed;
    (0..8192)
        .map(|_| {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (s >> 33) as u8
        })
        .collect()
}

/// A deterministic fault tape derived from `seed`, distinct from the schedule
/// tape so the interleaving and the fault schedule vary independently.
fn fault_tape(seed: u64) -> Vec<u8> {
    tape(seed ^ 0xA5A5_A5A5_A5A5_A5A5)
}

/// The op stream recorded for `workload` under `tape(seed)`/`seed`.
fn record_once(seed: u64, workload: &CycleWorkload) -> Vec<OpRecord> {
    let w = workload.clone();
    let log = block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
        run_cycle_and_record(&w).await
    });
    let recorded = log.lock().unwrap();
    recorded.clone()
}

/// The op stream recorded for `workload` under `tape(seed)`/`seed` with faults
/// active and guided by `ft`.
fn record_faults_with_tape(
    seed: u64,
    workload: &CycleWorkload,
    faults: FaultConfig,
    ft: Vec<u8>,
) -> Vec<OpRecord> {
    let w = workload.clone();
    let log = block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
        run_cycle_and_record_with_faults(&w, faults, seed, ft).await
    });
    let recorded = log.lock().unwrap();
    recorded.clone()
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
fn ring_invariant_holds_under_contention() {
    // run_cycle_and_assert panics on any violation; reaching the end means the
    // ring stayed a single cycle of length N for this tape.
    for seed in [0u64, 3, 99, 2024] {
        let workload = contended_cycle();
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_cycle_and_assert(workload).await
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
fn ring_invariant_holds_under_faults() {
    // The ring invariant is robust to faults: each swap is atomic, so the ring
    // stays a single N-cycle whether a swap commits or aborts. A lost or
    // fabricated write that broke the ring would panic inside the harness.
    let workload = contended_cycle();
    for seed in [0u64, 3, 99, 2024] {
        let w = workload.clone();
        let ft = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_cycle_and_assert_with_faults(w, FaultConfig::enabled(9), seed, ft).await
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
        if let Some((idx, a, b)) = first_divergence(&first, &second) {
            panic!(
                "seed {seed}: recovery op stream diverged at index {idx}\n  \
                 run 1 ({} ops): {a:?}\n  run 2 ({} ops): {b:?}",
                first.len(),
                second.len(),
            );
        }
    }
}

#[test]
fn pct_schedule_is_byte_identical_per_seed() {
    // The PCT policy must be just as reproducible as the tape policy: a fixed
    // seed yields a byte-for-byte identical op stream across runs.
    let workload = contended_cycle();
    let faults = FaultConfig::enabled(5);
    for seed in [0u64, 1, 7, 42, 1234] {
        let first = cycle_pct_record(&workload, faults, seed);
        let second = cycle_pct_record(&workload, faults, seed);
        let first = first.lock().unwrap().clone();
        let second = second.lock().unwrap().clone();
        if let Some((idx, a, b)) = first_divergence(&first, &second) {
            panic!(
                "seed {seed}: PCT op stream diverged at index {idx}\n  \
                 run 1 ({} ops): {a:?}\n  run 2 ({} ops): {b:?}",
                first.len(),
                second.len(),
            );
        }
    }
}

#[test]
fn concurrent_snapshot_reader_runs_and_stays_deterministic() {
    // The read-only observer must (a) actually run — adding backend reads on top
    // of the swap-only stream — and (b) keep the run byte-for-byte deterministic
    // even though it issues all N pointer reads concurrently within one
    // transaction. (b) is the real test of the concurrent-read path: if joining
    // the reads introduced any nondeterminism, the two streams would diverge.
    let with_reader = contended_cycle(); // snapshot_reads = 3
    let without = CycleWorkload {
        snapshot_reads: 0,
        ..contended_cycle()
    };
    for seed in [0u64, 1, 42, 1234] {
        let first = record_once(seed, &with_reader);
        let second = record_once(seed, &with_reader);
        if let Some((idx, a, b)) = first_divergence(&first, &second) {
            panic!(
                "seed {seed}: snapshot-reader op stream diverged at index {idx}\n  \
                 run 1 ({} ops): {a:?}\n  run 2 ({} ops): {b:?}",
                first.len(),
                second.len(),
            );
        }
        let baseline = record_once(seed, &without);
        assert!(
            first.len() > baseline.len(),
            "seed {seed}: enabling the snapshot reader added no backend ops \
             ({} with reader vs {} without) — the observer did not run",
            first.len(),
            baseline.len(),
        );
    }
}

#[test]
fn pct_seed_breadth_holds_ring_invariant() {
    // Seed-breadth sweep: many PCT schedules over the contended ring, with and
    // without faults. Any invariant violation panics inside the sweep.
    let workload = contended_cycle();
    cycle_pct_sweep(&workload, FaultConfig::enabled(7), 0..32);
    cycle_pct_sweep(&workload, FaultConfig::none(), 0..16);
}
