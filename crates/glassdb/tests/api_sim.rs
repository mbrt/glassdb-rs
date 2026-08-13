//! Deterministic-simulation self-checks for the transaction API fuzz workload.
//! The workload is inspired by FoundationDB's `FuzzApiCorrectness`: randomized
//! key and collection-lifecycle calls are checked against an exact state model
//! while a tape-guided scheduler and fault injector explore interleavings and
//! failures.
#![cfg(all(sim, feature = "sim"))]

mod sim_support;

use sim_support::{assert_slow_mutation_modes, fault_tape, tape};

use glassdb::rt::{TapeScheduler, block_on_with};
use glassdb::sim::{
    ApiAction, ApiTransaction, ApiWorkload, FaultConfig, pct_sweep, run_and_assert,
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
    use ApiAction::{
        CreateCollection, CreateCollectionIfAbsent, CreateNestedCollection, Delete, DropCollection,
        DropNestedCollection, InspectCollections, Read, ReadCollection, Write, WriteCollection,
        WriteNestedCollection,
    };
    ApiWorkload {
        clients: vec![
            vec![
                program(0, vec![Write(0, 1), Read(0), Write(3, 2), Read(3)], false),
                program(0, vec![Write(0, 9), Delete(3), Read(0), Read(3)], true),
                program(0, vec![Delete(0), Read(0), Write(6, 6), Read(6)], false),
                program(0, vec![WriteCollection(0, 50)], false),
                program(0, vec![CreateNestedCollection(0)], false),
                program(0, vec![DropCollection(0)], false),
                program(0, vec![DropNestedCollection(0)], false),
                program(0, vec![DropCollection(0)], false),
            ],
            vec![
                program(1, vec![Read(1), Write(1, 11), Read(1), Write(4, 14)], false),
                program(1, vec![Delete(1), Write(7, 17), Read(7)], false),
                program(1, vec![Delete(4), Write(4, 44), Read(4)], true),
                program(1, vec![CreateCollection(0)], false),
                program(1, vec![CreateCollection(0)], false),
                program(1, vec![WriteNestedCollection(0, 51)], false),
                program(1, vec![InspectCollections], false),
                program(1, vec![DropNestedCollection(0)], false),
            ],
            vec![
                program(2, vec![Write(2, 22), Write(5, 25), Read(2), Read(5)], false),
                program(2, vec![Delete(2), Read(2), Write(2, 32), Read(2)], false),
                program(2, vec![Delete(5), Read(5), Read(5)], false),
                program(2, vec![CreateCollectionIfAbsent(1)], true),
                program(2, vec![CreateCollectionIfAbsent(1)], false),
                program(2, vec![ReadCollection(1)], false),
                program(2, vec![WriteCollection(1, 52)], false),
                program(2, vec![DropCollection(1)], false),
            ],
        ],
    }
}

#[test]
fn transaction_program_invariants_hold_under_contention() {
    let workload = contended_api_workload();
    block_on_with(TapeScheduler::new(tape(47)), 47, async move {
        run_and_assert(workload).await
    });
}

#[test]
fn model_holds_under_crash_restart_and_outages() {
    let workload = contended_api_workload();
    block_on_with(TapeScheduler::new(tape(71)), 71, async move {
        run_and_assert_with_faults(workload, FaultConfig::failures(254), 71, fault_tape(71)).await
    });
}

#[test]
fn api_model_holds_with_slow_mutations() {
    assert_slow_mutation_modes("API workload", &contended_api_workload());
}

#[test]
fn pct_seed_breadth_holds_api_model() {
    pct_sweep(&contended_api_workload(), FaultConfig::failures(192), 0..16);
}
