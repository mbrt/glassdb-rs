//! Deterministic-simulation checks for exact transaction histories.
//!
//! This workload records point and concurrent-group reads, writes, and
//! normalized bounded membership scans.
use glassdb::exec::{TapeScheduler, block_on_with};
use glassdb::sim::{
    Covered, FaultConfig, HistoryCollectionOp as C, HistoryInstruction as I, HistoryTransaction,
    HistoryWorkload, StructuralCoverage, pct_sweep, run_and_assert, run_and_assert_with_faults,
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
            ],
            vec![transaction(
                2,
                1,
                vec![
                    I::Delete { key: 2 },
                    I::Scan {
                        start: 0,
                        end: 3,
                        after: None,
                        limit: 3,
                    },
                ],
            )],
            vec![
                transaction(
                    3,
                    2,
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
                    2,
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
            ],
            vec![transaction(
                5,
                3,
                vec![I::WriteLiteral { key: 1, value: 7 }, I::Abort],
            )],
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

fn shared_collection_history() -> HistoryWorkload {
    let actions = [
        [C::Write(10), C::Drop, C::Write(20)],
        [C::Read, C::Create, C::Read],
        [C::WriteNested(30), C::DropNested, C::Drop],
        [C::CreateIfAbsent, C::CreateNested, C::Read],
    ];
    HistoryWorkload {
        clients: actions
            .into_iter()
            .enumerate()
            .map(|(client_id, actions)| {
                actions
                    .into_iter()
                    .enumerate()
                    .map(|(index, operation)| {
                        transaction(
                            (client_id * 3 + index) as u64,
                            client_id,
                            vec![
                                I::Collection { slot: 0, operation },
                                I::WriteLiteral {
                                    key: client_id as u8 % 3,
                                    value: (client_id * 3 + index) as u8,
                                },
                            ],
                        )
                    })
                    .collect()
            })
            .collect(),
    }
}

#[test]
fn shared_collection_history_holds_under_contention_and_failures() {
    let workload = shared_collection_history();
    for faults in [FaultConfig::none(), FaultConfig::failures(7)] {
        pct_sweep(&workload, faults, 0..8);
    }
}

#[test]
fn shared_collection_history_holds_with_slow_mutations() {
    assert_slow_mutation_modes("shared collection history", &shared_collection_history());
}

/// Splits the seeded leaf, deletes two of its three keys, and then scans. The
/// write of client 0 finds the seeded leaf over its two-entry cap, and the
/// split puts the keys in two leaves. Client 0 then deletes key 1 and then key
/// 0. With either split key, the last delete leaves the left leaf with no live
/// entries and the right leaf with key 2 alone, so the left leaf merges into
/// its right sibling (ADR-074). The scans give the changes the time to land
/// while the clients still run.
fn shrinking_history() -> Covered<HistoryWorkload> {
    let scans = |count| {
        std::iter::repeat_with(|| {
            vec![I::Scan {
                start: 0,
                end: 3,
                after: None,
                limit: 3,
            }]
        })
        .take(count)
    };
    let shrinking = std::iter::once(vec![I::WriteLiteral { key: 2, value: 1 }])
        .chain(scans(16))
        .chain([vec![I::Delete { key: 1 }], vec![I::Delete { key: 0 }]])
        .chain(scans(20));
    let reading = std::iter::once(vec![I::Read {
        key: 2,
        register: 0,
    }])
    .chain(scans(20));
    // The checker of the history bounds its logical operations, so the other
    // clients run nothing.
    let clients: [Vec<Vec<I>>; 4] = [
        shrinking.collect(),
        reading.collect(),
        Vec::new(),
        Vec::new(),
    ];
    let workload = HistoryWorkload {
        clients: clients
            .into_iter()
            .enumerate()
            .map(|(client_id, programs)| {
                programs
                    .into_iter()
                    .enumerate()
                    .map(|(index, instructions)| {
                        transaction((client_id * 64 + index) as u64, client_id, instructions)
                    })
                    .collect()
            })
            .collect(),
    };
    Covered {
        workload,
        requires: StructuralCoverage {
            splits: true,
            merges: true,
        },
    }
}

#[test]
fn exact_history_holds_while_leaves_merge() {
    for seed in [0u64, 3, 99, 2024] {
        let workload = shrinking_history();
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert(workload).await
        });
    }
}

#[test]
fn pct_seed_breadth_holds_exact_history_while_leaves_merge() {
    let workload = shrinking_history();
    pct_sweep(&workload, FaultConfig::none(), 0..16);
    pct_sweep(&workload, FaultConfig::failures(7), 0..16);
}
