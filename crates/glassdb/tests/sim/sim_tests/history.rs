//! Deterministic-simulation checks for exact transaction histories.
//!
//! This workload records point and concurrent-group reads, writes, and
//! normalized bounded membership scans, in transactions and in stale read-only
//! transactions.
use glassdb::exec::{TapeScheduler, block_on_with};
use glassdb::sim::{
    Covered, FaultConfig, HistoryCollectionOp as C, HistoryInstruction as I, HistoryMode,
    HistoryTransaction, HistoryWorkload, StructuralCoverage, pct_sweep, run_and_assert,
    run_and_assert_with_faults,
};

use crate::sim_support::{assert_slow_mutation_modes, fault_tape, tape};

fn transaction(op_id: u64, client_id: usize, instructions: Vec<I>) -> HistoryTransaction {
    in_mode(op_id, client_id, HistoryMode::Strict, instructions)
}

fn in_mode(
    op_id: u64,
    client_id: usize,
    mode: HistoryMode,
    instructions: Vec<I>,
) -> HistoryTransaction {
    HistoryTransaction {
        op_id,
        client_id,
        mode,
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
    pct_sweep(&workload, faults);
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

/// Stale read-only transactions with both bounds, around increments, writes,
/// and deletes of both instances.
fn stale_history() -> HistoryWorkload {
    use HistoryMode::{StaleMax, StaleZero, Strict};
    let increment = |key| {
        vec![
            I::Read { key, register: 0 },
            I::Yield,
            I::WriteIncremented { key, register: 0 },
        ]
    };
    let group = |first, second| I::ReadGroup {
        reads: vec![(first, 0), (second, 1)],
    };
    let scan = |limit| I::Scan {
        start: 0,
        end: 3,
        after: None,
        limit,
    };
    HistoryWorkload {
        clients: vec![
            vec![
                in_mode(0, 0, Strict, increment(0)),
                in_mode(1, 0, StaleMax, vec![group(0, 1)]),
                in_mode(2, 0, StaleZero, vec![scan(3), I::Yield, group(0, 2)]),
            ],
            vec![
                in_mode(
                    3,
                    1,
                    StaleMax,
                    vec![I::Read {
                        key: 0,
                        register: 0,
                    }],
                ),
                in_mode(4, 1, Strict, vec![I::WriteLiteral { key: 1, value: 9 }]),
                in_mode(5, 1, StaleMax, vec![group(1, 2), scan(2)]),
            ],
            vec![
                in_mode(6, 2, Strict, increment(1)),
                in_mode(7, 2, StaleMax, vec![group(1, 2)]),
            ],
            vec![
                in_mode(8, 3, Strict, vec![I::Delete { key: 2 }]),
                in_mode(
                    9,
                    3,
                    StaleZero,
                    vec![I::Read {
                        key: 0,
                        register: 0,
                    }],
                ),
                in_mode(
                    10,
                    3,
                    StaleMax,
                    vec![
                        scan(2),
                        I::Read {
                            key: 0,
                            register: 0,
                        },
                    ],
                ),
            ],
        ],
    }
}

#[test]
fn stale_history_holds_under_tape_schedules() {
    for seed in [0u64, 3, 99, 2024] {
        let workload = stale_history();
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert(workload).await
        });
    }
}

#[test]
fn stale_history_holds_with_guided_faults() {
    for seed in [0u64, 3, 99] {
        let workload = stale_history();
        let faults_tape = fault_tape(seed);
        block_on_with(TapeScheduler::new(tape(seed)), seed, async move {
            run_and_assert_with_faults(workload, FaultConfig::failures(9), seed, faults_tape).await
        });
    }
}

#[test]
fn pct_seed_breadth_holds_stale_history() {
    pct_sweep(&stale_history(), FaultConfig::failures(7));
}

/// One instance caches the values of two shared collections, and the other
/// updates both values in one transaction. Each value has its own leaf, and an
/// update does not change a binding. So a strict read of the first collection
/// refreshes only its own leaf, and a stale read can then see its new value.
/// Without the monotonic floor, a later stale read of the second collection
/// could still use its old leaf.
fn floor_history() -> HistoryWorkload {
    use HistoryMode::{StaleMax, Strict};
    let collection = |slot, operation| I::Collection { slot, operation };
    let read = |slot| collection(slot, C::Read);
    HistoryWorkload {
        clients: vec![
            vec![
                in_mode(
                    0,
                    0,
                    Strict,
                    vec![collection(0, C::Write(1)), collection(1, C::Write(1))],
                ),
                in_mode(1, 0, StaleMax, vec![read(0), read(1)]),
                in_mode(2, 0, StaleMax, vec![read(0), read(1)]),
                in_mode(3, 0, Strict, vec![read(0)]),
                in_mode(4, 0, StaleMax, vec![read(0)]),
                in_mode(5, 0, StaleMax, vec![read(1)]),
            ],
            vec![
                in_mode(6, 1, StaleMax, vec![I::Yield, read(1)]),
                in_mode(7, 1, StaleMax, vec![I::Yield, read(1)]),
            ],
            update_after(8, 2, 2, 2),
            update_after(11, 3, 4, 3),
        ],
    }
}

/// Runs `fillers` strict reads on `client` to delay an update of both shared
/// collections to `value`.
fn update_after(first_id: u64, client: usize, fillers: u64, value: u8) -> Vec<HistoryTransaction> {
    let update = |slot| I::Collection {
        slot,
        operation: C::Update(value),
    };
    (first_id..first_id + fillers)
        .map(|id| {
            transaction(
                id,
                client,
                vec![I::Read {
                    key: 0,
                    register: 0,
                }],
            )
        })
        .chain(std::iter::once(transaction(
            first_id + fillers,
            client,
            vec![update(0), update(1)],
        )))
        .collect()
}

#[test]
fn pct_seed_breadth_holds_floor_history() {
    pct_sweep(&floor_history(), FaultConfig::none());
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

// Seed 5 without faults is a regression schedule. A nested write gets
// `StaleCollection` because another client dropped the nested collection. While
// the write checks its directory reads, a third client drops the shared
// collection. The check loaded the root before that drop, so the root still
// bound the shared collection, and the check returned the error to the client
// instead of replaying the body.
#[test]
fn shared_collection_history_holds_under_contention_and_failures() {
    let workload = shared_collection_history();
    for faults in [FaultConfig::none(), FaultConfig::failures(7)] {
        pct_sweep(&workload, faults);
    }
}

#[test]
fn shared_collection_history_holds_with_slow_mutations() {
    assert_slow_mutation_modes("shared collection history", &shared_collection_history());
}

/// Updates of a shared collection that open it by name and list nothing, so
/// that only their key accesses protect the binding (ADR-092). Drops and
/// creates of the same name race them. An update alone uses the direct commit,
/// and an update with a write of another collection uses the locked commit.
fn updated_collection_history() -> HistoryWorkload {
    let actions = [
        [
            (C::Update(1), false),
            (C::Update(2), true),
            (C::Update(3), false),
        ],
        [(C::Drop, true), (C::Create, true), (C::Drop, true)],
        [
            (C::UpdateNested(4), false),
            (C::UpdateNested(5), true),
            (C::Update(6), false),
        ],
        [
            (C::CreateNested, true),
            (C::DropNested, true),
            (C::CreateIfAbsent, true),
        ],
    ];
    HistoryWorkload {
        clients: actions
            .into_iter()
            .enumerate()
            .map(|(client_id, actions)| {
                actions
                    .into_iter()
                    .enumerate()
                    .map(|(index, (operation, other_write))| {
                        let id = (client_id * 3 + index) as u8;
                        let mut instructions = vec![I::Collection { slot: 0, operation }];
                        if other_write {
                            instructions.push(I::WriteLiteral {
                                key: client_id as u8 % 3,
                                value: id,
                            });
                        }
                        transaction(id.into(), client_id, instructions)
                    })
                    .collect()
            })
            .collect(),
    }
}

#[test]
fn updated_collection_history_holds_under_contention_and_failures() {
    let workload = updated_collection_history();
    for faults in [FaultConfig::none(), FaultConfig::failures(7)] {
        pct_sweep(&workload, faults);
    }
}

#[test]
fn updated_collection_history_holds_with_slow_mutations() {
    assert_slow_mutation_modes("updated collection history", &updated_collection_history());
}

/// Splits the seeded leaf, deletes two of its three keys, and then scans. The
/// write of client 0 finds the seeded leaf over its two-entry cap, and the
/// split puts the keys in two leaves. Client 0 then deletes key 1 and then key
/// 0. With either split key, the last delete leaves the left leaf with no live
/// entries and the right leaf with key 2 alone, so the left leaf merges into
/// its right sibling (ADR-073). The scans give the changes the time to land
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
    Covered::new(
        workload,
        StructuralCoverage {
            splits: true,
            merges: true,
        },
    )
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
    workload.pct_sweep(FaultConfig::none());
    workload.pct_sweep(FaultConfig::failures(7));
}
