//! Deterministic-simulation self-checks for the membership fuzz workload
//! (ADR-031 dynamic range sharding).
//!
//! These only build under the in-repo simulation executor with the `sim` harness
//! feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo test -p glassdb --features sim --test sim
//! ```
//!
//! The membership workload drives the B-link tree the increment/cycle workloads
//! never touch: with a tiny split soft cap a couple of live keys overflow a leaf,
//! so clients concurrently creating, deleting, and listing keys force leaf/root
//! splits, right-link traversal, and cross-leaf sorted listing. The harness
//! asserts every committed listing is strictly sorted and drawn from the key
//! universe, and that the final key set matches the per-key membership
//! accounting across tape, PCT, fault, and recovery schedules.
use crate::sim_support::{assert_slow_mutation_modes, fault_tape, tape};

use glassdb::exec::{TapeScheduler, block_on_with};
use glassdb::sim::{
    FaultConfig, MembOp, MembershipWorkload, pct_sweep, run_and_assert, run_and_assert_with_faults,
};

/// A contended membership workload over three clients, each owning a disjoint
/// slice of the 8-key universe by residue (client `i` owns keys `k` with
/// `k % 3 == i`): client 0 -> {0,3,6}, client 1 -> {1,4,7}, client 2 -> {2,5}.
/// Puts, deletes, full listings, and bounded pages interleave so keys created by
/// different clients share leaves and split concurrently with scans.
fn contended_membership() -> MembershipWorkload {
    MembershipWorkload {
        clients: vec![
            vec![
                MembOp::Put(0),
                MembOp::Put(3),
                MembOp::List,
                MembOp::Delete(0),
                MembOp::Put(6),
                MembOp::RangePage {
                    start: 1,
                    end: 7,
                    limit: 2,
                },
            ],
            vec![
                MembOp::Put(1),
                MembOp::Put(4),
                MembOp::Put(7),
                MembOp::List,
                MembOp::Delete(4),
                MembOp::PrefixPage(3),
            ],
            vec![
                MembOp::Put(2),
                MembOp::List,
                MembOp::Put(5),
                MembOp::Delete(2),
                MembOp::Put(5),
            ],
        ],
    }
}

/// A workload that fills the whole key universe: every client puts all of its
/// residue-class keys, so the final live set is all eight keys — which, at a
/// two-entry leaf cap, cannot fit in one leaf and forces the listing to scan
/// across split leaves.
fn fill_all_keys() -> MembershipWorkload {
    MembershipWorkload {
        clients: vec![
            vec![MembOp::Put(0), MembOp::Put(3), MembOp::Put(6), MembOp::List],
            vec![MembOp::Put(1), MembOp::Put(4), MembOp::Put(7), MembOp::List],
            vec![MembOp::Put(2), MembOp::Put(5), MembOp::List],
        ],
    }
}

#[test]
fn membership_invariant_holds_under_contention() {
    // run_and_assert panics on any violation; reaching the end means every
    // committed listing was well-formed and the final set matched the accounting.
    for seed in [0u64, 3, 99, 2024] {
        let workload = contended_membership();
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert(workload).await
        });
    }
}

#[test]
fn full_universe_lists_every_key_across_leaves() {
    // Filling all eight keys cannot fit in a two-entry leaf, so a correct
    // fault-free final listing proves the scan traverses split leaves (via
    // right-links) without dropping or duplicating a key. The harness's
    // fault-free verify checks the final set equals the accounting exactly.
    for seed in [0u64, 5, 77, 4242] {
        let workload = fill_all_keys();
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert(workload).await
        });
    }
}

#[test]
fn membership_holds_under_faults() {
    // With faults the invariant relaxes to the in-doubt bound: a listed key must
    // be either the last committed state or the ambiguous outcome of an op left
    // in-doubt. A lost or fabricated create/delete outside that bound panics
    // inside the harness.
    let workload = contended_membership();
    for seed in [0u64, 3, 99, 2024] {
        let w = workload.clone();
        let ft = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert_with_faults(w, FaultConfig::failures(9), seed, ft).await
        });
    }
}

#[test]
fn membership_holds_with_slow_mutations() {
    assert_slow_mutation_modes("membership workload", &contended_membership());
}

#[test]
fn membership_holds_under_crash_restart_and_outages() {
    // High intensity drives multiple client crashes (-> crash-and-restart on the
    // same backend) and sustained per-client transport outages. The membership
    // bound must survive the recovery paths those faults exercise while splits
    // run concurrently in the background.
    let workload = contended_membership();
    let faults = FaultConfig::failures(200);
    for seed in [0u64, 1, 7, 42, 99, 1234] {
        let workload = workload.clone();
        let faults_tape = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert_with_faults(workload, faults, seed, faults_tape).await
        });
    }
}

#[test]
fn pct_seed_breadth_holds_membership() {
    // Seed-breadth sweep: many PCT schedules over the contended workload, with
    // and without faults. Any invariant violation panics inside the sweep.
    let workload = contended_membership();
    pct_sweep(&workload, FaultConfig::failures(7), 0..32);
    pct_sweep(&workload, FaultConfig::none(), 0..16);
}
