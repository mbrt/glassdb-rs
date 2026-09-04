//! Deterministic-simulation checks for exact transaction histories.
//!
//! This workload records point and concurrent-group reads, writes, and
//! normalized bounded membership scans.
use glassdb::exec::{TapeScheduler, block_on_with};
use glassdb::sim::{
    FaultConfig, HistoryInstruction as I, HistoryTransaction, HistoryWorkload, pct_sweep,
    run_and_assert, run_and_assert_with_faults,
};

use crate::sim_support::{assert_slow_mutation_modes, fault_tape, tape};

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
fn exact_history_holds_with_slow_mutations() {
    assert_slow_mutation_modes("history workload", &contended_history());
}

#[test]
fn pct_seed_breadth_holds_exact_history() {
    let workload = contended_history();
    let faults = FaultConfig::failures(7);
    pct_sweep(&workload, faults, 0..16);
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
