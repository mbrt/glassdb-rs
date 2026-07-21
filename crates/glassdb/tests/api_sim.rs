//! Deterministic-simulation self-checks for the transaction API fuzz workload.
//! The workload is inspired by FoundationDB's `FuzzApiCorrectness`: randomized
//! calls within a transaction are checked against an exact state model while a
//! tape-guided scheduler and fault injector explore interleavings and failures.
#![cfg(all(sim, feature = "sim"))]

mod sim_support;

use sim_support::{
    assert_no_divergence, assert_slow_mutation_modes, fault_tape, record_faults_with_tape,
    record_once, record_with_tapes, tape,
};

use glassdb::rt::{TapeScheduler, block_on_with};
use glassdb::sim::{
    ApiAction, ApiTransaction, ApiWorkload, FaultConfig, pct_record, pct_sweep, run_and_assert,
    run_and_assert_with_faults,
};

fn program(client: usize, actions: Vec<ApiAction>, abort: bool) -> ApiTransaction {
    ApiTransaction {
        client,
        actions,
        abort,
    }
}

fn contended_api_workload() -> ApiWorkload {
    use ApiAction::{Delete, Read, Write};
    ApiWorkload {
        clients: vec![
            vec![
                program(0, vec![Write(0, 1), Read(0), Write(3, 2), Read(3)], false),
                program(0, vec![Write(0, 9), Delete(3), Read(0), Read(3)], true),
                program(0, vec![Delete(0), Read(0), Write(6, 6), Read(6)], false),
            ],
            vec![
                program(1, vec![Read(1), Write(1, 11), Read(1), Write(4, 14)], false),
                program(1, vec![Delete(1), Write(7, 17), Read(7)], false),
                program(1, vec![Delete(4), Write(4, 44), Read(4)], true),
            ],
            vec![
                program(2, vec![Write(2, 22), Write(5, 25), Read(2), Read(5)], false),
                program(2, vec![Delete(2), Read(2), Write(2, 32), Read(2)], false),
                program(2, vec![Delete(5), Read(5), Read(5)], false),
            ],
        ],
    }
}

#[test]
fn op_stream_is_byte_identical_across_runs() {
    let workload = contended_api_workload();
    let first = record_once(31, &workload);
    let second = record_once(31, &workload);
    assert_no_divergence("API workload", &first, &second);
}

#[test]
fn transaction_program_invariants_hold_under_contention() {
    let workload = contended_api_workload();
    block_on_with(TapeScheduler::new(tape(47)), 47, async move {
        run_and_assert(workload).await
    });
}

#[test]
fn op_stream_is_byte_identical_with_faults() {
    let workload = contended_api_workload();
    let faults = FaultConfig::failures(224);
    let first = record_faults_with_tape(59, &workload, faults, fault_tape(59));
    let second = record_faults_with_tape(59, &workload, faults, fault_tape(59));
    assert_no_divergence("faulted API workload", &first, &second);
}

#[test]
fn model_holds_under_crash_restart_and_outages() {
    let workload = contended_api_workload();
    block_on_with(TapeScheduler::new(tape(71)), 71, async move {
        run_and_assert_with_faults(workload, FaultConfig::failures(255), 71, fault_tape(71)).await
    });
}

#[test]
fn api_model_holds_with_slow_mutations() {
    assert_slow_mutation_modes("API workload", &contended_api_workload());
}

#[test]
fn boundary_tapes_replay_deterministically() {
    let workload = contended_api_workload();
    for (schedule, fault_tape) in [
        (Vec::new(), Vec::new()),
        (vec![0], vec![255]),
        (vec![255], vec![0]),
    ] {
        let first = record_with_tapes(
            83,
            &workload,
            FaultConfig::failures(192),
            schedule.clone(),
            fault_tape.clone(),
        );
        let second = record_with_tapes(
            83,
            &workload,
            FaultConfig::failures(192),
            schedule,
            fault_tape,
        );
        assert_no_divergence("boundary API tapes", &first, &second);
    }
}

#[test]
fn pct_schedule_is_byte_identical_per_seed() {
    let workload = contended_api_workload();
    let first = pct_record(&workload, FaultConfig::failures(160), 97);
    let second = pct_record(&workload, FaultConfig::failures(160), 97);
    assert_no_divergence(
        "PCT API workload",
        &first.lock().unwrap(),
        &second.lock().unwrap(),
    );
}

#[test]
fn pct_seed_breadth_holds_api_model() {
    pct_sweep(&contended_api_workload(), FaultConfig::failures(192), 0..16);
}
