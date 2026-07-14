//! Deterministic-simulation self-checks for the membership fuzz workload
//! (ADR-031 dynamic range sharding).
//!
//! These only build under the in-repo simulation executor with the `sim` harness
//! feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo test -p glassdb --features sim
//! ```
//!
//! The membership workload drives the B-link tree the increment/cycle workloads
//! never touch: with a tiny split soft cap a couple of live keys overflow a leaf,
//! so clients concurrently creating, deleting, and listing keys force leaf/root
//! splits, right-link traversal, and cross-leaf sorted listing. The harness
//! asserts every committed listing is strictly sorted and drawn from the key
//! universe, and that the final key set matches the per-key membership
//! accounting. As with the other workloads, a fixed (workload, tape, seed) must
//! issue a byte-for-byte identical backend op stream on every run.
#![cfg(all(sim, feature = "sim"))]

mod sim_support;

use sim_support::{
    assert_no_divergence, fault_tape, record_faults_with_tape, record_once, record_with_tapes, tape,
};

use glassdb::rt::{TapeScheduler, block_on_with};
use glassdb::sim::{
    FaultConfig, MembOp, MembershipWorkload, pct_record, pct_sweep, run_and_assert,
    run_and_assert_with_faults,
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
fn op_stream_is_byte_identical_across_runs() {
    let workload = contended_membership();
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
fn op_stream_is_byte_identical_with_faults() {
    // Determinism must hold even with faults active: scheduling, time,
    // randomness, and the fault schedule are all functions of the tape and seed.
    let workload = contended_membership();
    let faults = FaultConfig::enabled(7);
    for seed in [0u64, 1, 7, 42, 1234, 0xDEAD_BEEF] {
        let first = record_faults_with_tape(seed, &workload, faults, fault_tape(seed));
        let second = record_faults_with_tape(seed, &workload, faults, fault_tape(seed));
        assert_no_divergence(&format!("seed {seed}: faulted"), &first, &second);
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
            run_and_assert_with_faults(w, FaultConfig::enabled(9), seed, ft).await
        });
    }
}

#[test]
fn membership_holds_under_crash_restart_and_outages() {
    // High intensity drives multiple client crashes (-> crash-and-restart on the
    // same backend) and sustained per-client transport outages. Each run must
    // stay byte-for-byte deterministic per (tape, seed), and the membership bound
    // (asserted inside the harness) must survive the recovery paths those faults
    // exercise while splits run concurrently in the background.
    let workload = contended_membership();
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
    let workload = fill_all_keys();
    let faults = FaultConfig::enabled(128);
    for (schedule, fault_tape) in [
        (Vec::new(), Vec::new()),
        (vec![0], Vec::new()),
        (vec![255, 1], vec![0; 16]),
    ] {
        let first = record_with_tapes(77, &workload, faults, schedule.clone(), fault_tape.clone());
        let second = record_with_tapes(77, &workload, faults, schedule, fault_tape);
        assert_no_divergence("membership boundary tape replay", &first, &second);
    }
}

#[test]
fn pct_schedule_is_byte_identical_per_seed() {
    // The PCT policy must be just as reproducible as the tape policy: a fixed
    // seed yields a byte-for-byte identical op stream across runs.
    let workload = contended_membership();
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
fn pct_seed_breadth_holds_membership() {
    // Seed-breadth sweep: many PCT schedules over the contended workload, with
    // and without faults. Any invariant violation panics inside the sweep.
    let workload = contended_membership();
    pct_sweep(&workload, FaultConfig::enabled(7), 0..32);
    pct_sweep(&workload, FaultConfig::none(), 0..16);
}
