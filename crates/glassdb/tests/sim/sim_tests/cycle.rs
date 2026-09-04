//! Deterministic-simulation self-checks for the Cycle fuzz workload (ported from
//! FoundationDB's `Cycle.cpp`; see ADR-010/011).
//!
//! These only build under the in-repo simulation executor with the `sim` harness
//! feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo test -p glassdb --features sim --test sim
//! ```
//!
//! The Cycle workload is the serializability oracle the commutative
//! RMW-increment workload (`concurrent_sim.rs`) cannot be: each transaction
//! rotates three consecutive ring edges, an operation that does not commute, so
//! any isolation or atomicity break splits, shrinks, or grows the ring. The
//! harness asserts the ring is still a single cycle of length `N`.
use crate::sim_support::{assert_slow_mutation_modes, fault_tape, tape};

use glassdb::exec::{TapeScheduler, block_on_with};
use glassdb::sim::{
    CycleWorkload, FaultConfig, pct_sweep, run_and_assert, run_and_assert_with_faults,
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
fn ring_invariant_holds_under_faults() {
    // The ring invariant is robust to faults: each swap is atomic, so the ring
    // stays a single N-cycle whether a swap commits or aborts. A lost or
    // fabricated write that broke the ring would panic inside the harness.
    let workload = contended_cycle();
    for seed in [0u64, 3, 99, 2024] {
        let w = workload.clone();
        let ft = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert_with_faults(w, FaultConfig::failures(9), seed, ft).await
        });
    }
}

#[test]
fn ring_invariant_holds_with_slow_mutations() {
    assert_slow_mutation_modes("Cycle workload", &contended_cycle());
}

#[test]
fn ring_holds_under_crash_restart_and_outages() {
    // High intensity drives multiple client crashes (-> crash-and-restart on the
    // same backend) and sustained per-client transport outages. The ring
    // invariant must survive lease expiry, lock-lease recovery, and a restarted
    // client reclaiming its own orphaned locks.
    let workload = contended_cycle();
    let faults = FaultConfig::failures(200);
    for seed in [0u64, 1, 7, 42, 99, 1234] {
        let w = workload.clone();
        let ft = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert_with_faults(w, faults, seed, ft).await
        });
    }
}

#[test]
fn pct_seed_breadth_holds_ring_invariant() {
    // Seed-breadth sweep: many PCT schedules over the contended ring, with and
    // without faults. Any invariant violation panics inside the sweep.
    let workload = contended_cycle();
    pct_sweep(&workload, FaultConfig::failures(7), 0..32);
    pct_sweep(&workload, FaultConfig::none(), 0..16);
}
