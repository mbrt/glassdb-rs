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
    Covered, FaultConfig, MembOp, MembershipWorkload, MergingMembershipWorkload,
    StructuralCoverage, pct_sweep, run_and_assert, run_and_assert_with_faults,
};

const SPLITS: StructuralCoverage = StructuralCoverage {
    splits: true,
    merges: false,
};

/// Operations that give the structural changes caused by earlier operations the
/// time to land while the clients still run. Most changes land during the
/// scans. A merge defers while a transaction holds a lock on its nodes, so the
/// pause lets a deferred change land.
fn settle() -> Vec<MembOp> {
    vec![
        MembOp::List,
        MembOp::RangePage {
            start: 0,
            end: 8,
            limit: 3,
        },
        MembOp::PrefixPage(4),
        MembOp::List,
        MembOp::Pause,
    ]
}

/// Appends [`settle`] to each client.
fn with_settle(mut clients: Vec<Vec<MembOp>>) -> Vec<Vec<MembOp>> {
    for client in &mut clients {
        client.extend(settle());
    }
    clients
}

/// A contended membership workload over three clients, each owning a disjoint
/// slice of the 8-key universe by residue (client `i` owns keys `k` with
/// `k % 3 == i`): client 0 -> {0,3,6}, client 1 -> {1,4,7}, client 2 -> {2,5}.
/// Puts, deletes, full listings, and bounded pages interleave so keys created by
/// different clients share leaves and split concurrently with scans.
fn contended_membership() -> Covered<MembershipWorkload> {
    let workload = MembershipWorkload {
        clients: with_settle(vec![
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
        ]),
    };
    Covered {
        workload,
        requires: SPLITS,
    }
}

/// A workload that fills the whole key universe: every client puts all of its
/// residue-class keys, so the final live set is all eight keys — which, at a
/// two-entry leaf cap, cannot fit in one leaf and forces the listing to scan
/// across split leaves.
fn fill_all_keys() -> Covered<MembershipWorkload> {
    let workload = MembershipWorkload {
        clients: with_settle(vec![
            vec![MembOp::Put(0), MembOp::Put(3), MembOp::Put(6), MembOp::List],
            vec![MembOp::Put(1), MembOp::Put(4), MembOp::Put(7), MembOp::List],
            vec![MembOp::Put(2), MembOp::Put(5), MembOp::List],
        ]),
    };
    Covered {
        workload,
        requires: SPLITS,
    }
}

/// Fills the key universe, lets the fill split leaves, and then deletes all keys
/// except those of client 2, which only reads after the fill. Only a committed
/// leaf write queues a merge candidate. Clients 0 and 1 share one instance, and
/// each deletes its keys from right to left, so that their last delete writes
/// to the leftmost leaf after all other deletes. That leaf then merges
/// (ADR-073) while the clients settle.
fn fill_then_shrink() -> Covered<MergingMembershipWorkload> {
    let workload = MergingMembershipWorkload(MembershipWorkload {
        clients: vec![
            [
                vec![MembOp::Put(0), MembOp::Put(3), MembOp::Put(6)],
                settle(),
                vec![MembOp::Delete(6), MembOp::Delete(3), MembOp::Delete(0)],
                settle(),
            ]
            .concat(),
            [
                vec![MembOp::Put(1), MembOp::Put(4), MembOp::Put(7)],
                settle(),
                vec![MembOp::Delete(7), MembOp::Delete(4), MembOp::Delete(1)],
                settle(),
            ]
            .concat(),
            [vec![MembOp::Put(2), MembOp::Put(5)], settle(), settle()].concat(),
        ],
    });
    Covered {
        workload,
        requires: StructuralCoverage {
            splits: true,
            merges: true,
        },
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
    // be either the last committed state or a possible outcome of an op left
    // in doubt. A lost or fabricated create/delete outside that bound panics
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

#[test]
fn membership_holds_while_nodes_merge() {
    for seed in [0u64, 3, 99, 2024] {
        let workload = fill_then_shrink();
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert(workload).await
        });
    }
}

#[test]
fn membership_holds_while_nodes_merge_under_faults_and_restarts() {
    // Crashes and outages interrupt merges at every step, so structural
    // recovery must finish or abandon them within the membership bound.
    for (faults, seeds) in [
        (FaultConfig::failures(9), [0u64, 3, 99, 2024].as_slice()),
        (
            FaultConfig::failures(200),
            [0u64, 1, 7, 42, 99, 1234].as_slice(),
        ),
    ] {
        for &seed in seeds {
            let workload = fill_then_shrink();
            let faults_tape = fault_tape(seed);
            block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
                run_and_assert_with_faults(workload, faults, seed, faults_tape).await
            });
        }
    }
}

#[test]
fn pct_seed_breadth_holds_membership_while_nodes_merge() {
    let workload = fill_then_shrink();
    pct_sweep(&workload, FaultConfig::failures(7), 0..32);
    pct_sweep(&workload, FaultConfig::none(), 0..16);
}
