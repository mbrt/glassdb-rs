//! Deterministic-simulation self-checks for the concurrency fuzzer
//! (ADR-010/011).
//!
//! These only build under the in-repo simulation executor with the `sim`
//! harness feature:
//!
//! ```bash
//! RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo test -p glassdb --features sim
//! ```
//!
//! The central guarantee is that, for a fixed workload, schedule tape, and seed,
//! the engine issues a **byte-for-byte identical stream of backend operations**
//! on every run. Two interleavings can reach the same final state while issuing
//! different operations, so an identical op stream — not just a matching final
//! result — is what proves the schedule itself replayed deterministically.
#![cfg(all(sim, feature = "sim"))]

use glassdb::middleware::{OpRecord, first_divergence};
use glassdb::rt::{TapeScheduler, block_on_with};
use glassdb::sim::{
    FaultConfig, Op, Workload, pct_record, pct_sweep, run_and_assert, run_and_assert_with_faults,
    run_and_record, run_and_record_with_faults,
};

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

fn assert_no_divergence(label: &str, first: &[OpRecord], second: &[OpRecord]) {
    if let Some((idx, a, b)) = first_divergence(first, second) {
        panic!(
            "{label}: op stream diverged at index {idx}\n  \
             run 1 ({} ops): {a:?}\n  run 2 ({} ops): {b:?}",
            first.len(),
            second.len(),
        );
    }
}

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

/// The op stream recorded for `workload` under `tape`/`seed`.
fn record_once(seed: u64, workload: &Workload) -> Vec<OpRecord> {
    let w = workload.clone();
    let log = block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
        run_and_record(&w).await
    });
    let recorded = log.lock().unwrap();
    recorded.clone()
}

/// The op stream recorded for `workload` under the schedule `tape(seed)`/`seed`
/// with faults active and guided by `ft`.
fn record_faults_with_tape(
    seed: u64,
    workload: &Workload,
    faults: FaultConfig,
    ft: Vec<u8>,
) -> Vec<OpRecord> {
    let w = workload.clone();
    let log = block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
        run_and_record_with_faults(&w, faults, seed, ft).await
    });
    let recorded = log.lock().unwrap();
    recorded.clone()
}

/// The op stream recorded for `workload` under `tape`/`seed` with faults active.
fn record_once_faults(seed: u64, workload: &Workload, faults: FaultConfig) -> Vec<OpRecord> {
    record_faults_with_tape(seed, workload, faults, fault_tape(seed))
}

/// Records with caller-provided schedule and fault tapes, for boundary cases
/// like tape exhaustion that the seed-expanded helper intentionally avoids.
fn record_with_tapes(
    seed: u64,
    workload: &Workload,
    faults: FaultConfig,
    schedule_tape: Vec<u8>,
    fault_tape: Vec<u8>,
) -> Vec<OpRecord> {
    let w = workload.clone();
    let log = block_on_with(TapeScheduler::new(schedule_tape), seed, async move {
        run_and_record_with_faults(&w, faults, seed, fault_tape).await
    });
    let recorded = log.lock().unwrap();
    recorded.clone()
}

/// A boundary-heavy workload: maximum generated client/op shape, with frequent
/// read-only transactions mixed into contended writes.
fn max_read_heavy_workload() -> Workload {
    Workload {
        clients: vec![
            vec![
                Op::ReadOnly(vec![0, 1, 2, 3]),
                Op::Rmw(0),
                Op::ReadOnly(vec![0, 2]),
                Op::MultiRmw(0, 1),
                Op::ReadOnly(vec![1, 3]),
                Op::Rmw(2),
                Op::ReadOnly(vec![0, 1, 2, 3]),
                Op::MultiRmw(2, 3),
            ],
            vec![
                Op::ReadOnly(vec![3, 2, 1, 0]),
                Op::Rmw(1),
                Op::ReadOnly(vec![1, 2]),
                Op::MultiRmw(1, 2),
                Op::ReadOnly(vec![0, 3]),
                Op::Rmw(3),
                Op::ReadOnly(vec![2, 3]),
                Op::MultiRmw(0, 3),
            ],
            vec![
                Op::ReadOnly(vec![]),
                Op::MultiRmw(2, 0),
                Op::ReadOnly(vec![0]),
                Op::Rmw(2),
                Op::ReadOnly(vec![1, 2, 3]),
                Op::MultiRmw(3, 1),
                Op::ReadOnly(vec![0, 1, 2, 3]),
                Op::Rmw(0),
            ],
            vec![
                Op::ReadOnly(vec![2]),
                Op::Rmw(3),
                Op::ReadOnly(vec![0, 3]),
                Op::MultiRmw(0, 2),
                Op::ReadOnly(vec![1]),
                Op::Rmw(1),
                Op::ReadOnly(vec![0, 1, 2, 3]),
                Op::MultiRmw(1, 3),
            ],
        ],
    }
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
fn op_stream_is_byte_identical_with_faults() {
    // Determinism must hold even with faults active: scheduling, time,
    // randomness, and the fault schedule are all functions of the tape and seed.
    let workload = contended_workload();
    let faults = FaultConfig::enabled(7);
    for seed in [0u64, 1, 7, 42, 1234, 0xDEAD_BEEF] {
        let first = record_once_faults(seed, &workload, faults);
        let second = record_once_faults(seed, &workload, faults);
        assert_no_divergence(&format!("seed {seed}: faulted"), &first, &second);
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
            run_and_assert_with_faults(w, FaultConfig::enabled(9), seed, ft).await
        });
    }
}

#[test]
fn fault_tape_guides_the_fault_schedule() {
    // Same schedule tape and seed but different *fault* tapes must be able to
    // change the recorded op stream; otherwise the fault schedule would only be
    // seed-sampled, not coverage-guidable. An all-low tape fires every fault; an
    // all-high tape fires none. The intensity must be high enough that the
    // impactful faults (fail-before / lost-ack) have non-zero probability.
    let workload = contended_workload();
    let faults = FaultConfig::enabled(128);
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
fn boundary_tapes_replay_deterministically() {
    let workload = max_read_heavy_workload();
    let faults = FaultConfig::enabled(128);
    for (schedule, fault_tape) in [
        (Vec::new(), Vec::new()),
        (vec![0], Vec::new()),
        (vec![255, 1], vec![0; 16]),
    ] {
        let first = record_with_tapes(77, &workload, faults, schedule.clone(), fault_tape.clone());
        let second = record_with_tapes(77, &workload, faults, schedule, fault_tape);
        assert_no_divergence("boundary tape replay", &first, &second);
    }
}

#[test]
fn recovery_holds_under_crash_restart_and_outages() {
    // High intensity drives multiple client crashes (→ crash-and-restart on the
    // same backend) and sustained, all-or-nothing per-client transport outages.
    // Each run must stay byte-for-byte deterministic per (tape, seed), and the
    // acked-bounds invariant (asserted inside the harness) must survive the
    // recovery paths those faults exercise: lease expiry, lock-lease recovery,
    // and a restarted client reclaiming its own orphaned locks.
    let workload = contended_workload();
    let faults = FaultConfig::enabled(200);
    for seed in [0u64, 1, 7, 42, 99, 1234] {
        let ft = fault_tape(seed);
        let first = record_faults_with_tape(seed, &workload, faults, ft.clone());
        let second = record_faults_with_tape(seed, &workload, faults, ft);
        assert_no_divergence(&format!("seed {seed}: recovery"), &first, &second);
    }
}

#[test]
fn pct_schedule_is_byte_identical_per_seed() {
    // The PCT policy must be just as reproducible as the tape policy: a fixed
    // seed yields a byte-for-byte identical op stream across runs.
    let workload = contended_workload();
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
fn pct_seed_breadth_holds_serializability() {
    // Seed-breadth sweep: many PCT schedules over the contended workload, with
    // and without faults. Any invariant violation panics inside the sweep.
    let workload = contended_workload();
    pct_sweep(&workload, FaultConfig::enabled(7), 0..32);
    pct_sweep(&workload, FaultConfig::none(), 0..16);
}
