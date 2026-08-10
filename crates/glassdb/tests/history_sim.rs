//! Deterministic-simulation checks for exact transaction histories.
//!
//! This workload records point and concurrent-group reads, writes, and
//! normalized bounded membership scans. Long-lived snapshot reads remain
//! outside its specification.
#![cfg(all(sim, feature = "sim"))]

mod sim_support;

use glassdb::rt::{TapeScheduler, block_on_with};
use glassdb::sim::{
    FaultConfig, HistoryInstruction as I, HistoryTransaction, HistoryWorkload, pct_record,
    pct_sweep, record_input, run_and_assert, run_and_assert_with_faults,
};

use sim_support::{
    assert_no_divergence, assert_slow_mutation_modes, fault_tape, record_faults_with_tape,
    record_once, tape,
};

fn transaction(op_id: u64, client_id: usize, instructions: Vec<I>) -> HistoryTransaction {
    HistoryTransaction {
        op_id,
        client_id,
        instructions,
    }
}

fn contended_history() -> HistoryWorkload {
    HistoryWorkload {
        clients: vec![
            vec![
                transaction(
                    0,
                    0,
                    vec![
                        I::Read {
                            key: 0,
                            register: 0,
                        },
                        I::Yield,
                        I::WriteIncremented {
                            key: 0,
                            register: 0,
                        },
                    ],
                ),
                transaction(
                    1,
                    0,
                    vec![
                        I::ReadGroup {
                            // Reverse key order to ensure the trace records a
                            // group, not join-result ordering.
                            reads: vec![(2, 1), (1, 0)],
                        },
                        I::WriteIncremented {
                            key: 1,
                            register: 0,
                        },
                        I::WriteIncremented {
                            key: 2,
                            register: 1,
                        },
                    ],
                ),
                transaction(
                    2,
                    0,
                    vec![
                        I::Delete { key: 2 },
                        I::Scan {
                            start: 0,
                            end: 3,
                            after: None,
                            limit: 3,
                        },
                    ],
                ),
            ],
            vec![
                transaction(
                    3,
                    1,
                    vec![
                        I::Read {
                            key: 0,
                            register: 0,
                        },
                        I::WriteIncremented {
                            key: 0,
                            register: 0,
                        },
                    ],
                ),
                transaction(
                    4,
                    1,
                    vec![
                        I::Scan {
                            start: 0,
                            end: 3,
                            after: Some(0),
                            limit: 1,
                        },
                        I::Read {
                            key: 1,
                            register: 0,
                        },
                        I::WriteRegister {
                            key: 2,
                            register: 0,
                        },
                    ],
                ),
                transaction(5, 1, vec![I::WriteLiteral { key: 1, value: 7 }, I::Abort]),
            ],
        ],
    }
}

#[test]
fn point_group_and_range_history_holds_under_tape_schedules() {
    for seed in [0u64, 3, 99, 2024] {
        let workload = contended_history();
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert(workload).await
        });
    }
}

#[test]
fn backend_stream_is_byte_identical_with_transport_faults_and_crashes() {
    let workload = contended_history();
    let faults = FaultConfig::failures(200);
    for seed in [0u64, 7, 42, 1234] {
        let faults_tape = fault_tape(seed);
        let first = record_faults_with_tape(seed, &workload, faults, faults_tape.clone());
        let second = record_faults_with_tape(seed, &workload, faults, faults_tape);
        assert_no_divergence(&format!("seed {seed}: history recovery"), &first, &second);
    }
}

#[test]
fn exact_history_holds_with_slow_mutations() {
    assert_slow_mutation_modes("history workload", &contended_history());
}

#[test]
fn cache_free_and_simulated_cache_replay_identically() {
    // record_input performs one cache-free and one simulated-persistent-cache
    // run. Repeating the same input proves both modes consume deterministic
    // schedule, transport-fault, crash, and media-fault tapes.
    let input = b"history-stage-three-seed-with-schedule-and-fault-tail";
    let first = record_input::<HistoryWorkload>(input);
    let second = record_input::<HistoryWorkload>(input);
    assert_no_divergence("history cache-mode replay", &first, &second);
}

#[test]
fn pct_schedules_are_reproducible_and_serializable() {
    let workload = contended_history();
    let faults = FaultConfig::failures(7);
    for seed in [0u64, 1, 7, 42] {
        let first = pct_record(&workload, faults, seed);
        let second = pct_record(&workload, faults, seed);
        let first = first.lock().unwrap().clone();
        let second = second.lock().unwrap().clone();
        assert_no_divergence(&format!("seed {seed}: history PCT"), &first, &second);
    }
    pct_sweep(&workload, faults, 0..16);
}

#[test]
fn faulted_backend_stream_is_nonempty() {
    let log = record_once(17, &contended_history());
    assert!(!log.is_empty());
}

#[test]
fn exact_history_holds_with_guided_faults() {
    for seed in [0u64, 3, 99] {
        let workload = contended_history();
        let faults_tape = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert_with_faults(workload, FaultConfig::failures(9), seed, faults_tape).await
        });
    }
}
