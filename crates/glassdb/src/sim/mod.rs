#![cfg(sim)]

//! Deterministic-simulation workloads and execution harness.
//!
//! The shared [`SimWorkload`] harness runs concurrent clients over a deterministic
//! scheduler with tape-guided faults. Generated workloads have four logical
//! clients: adjacent pairs share one database instance, its caches and transport,
//! and its crash/restart lifetime. Each client runs up to six operations in order,
//! or three transactions for [`HistoryWorkload`]. Empty programs keep smaller
//! cases available. The optional cycle observer uses a separate instance and a
//! faultless transport. Five workloads check database consistency through public
//! state and transaction histories; retained protocol resources alone are outside
//! their scope:
//!
//! - [`RmwWorkload`] stresses shared-key serializability and in-doubt increments.
//! - [`CycleWorkload`] detects isolation failures with non-commuting ring updates.
//! - [`MembershipWorkload`] exercises key membership, splits, and listing.
//! - [`ApiWorkload`] checks transaction-local key operations, collection
//!   lifecycle, nested paths, and aborts.
//! - [`HistoryWorkload`] checks complete point/group-read, write, and
//!   membership-scan and shared collection histories against an
//!   implementation-independent sequential specification. Its clients continue
//!   after admissible public errors while retaining in-doubt outcomes.

mod api;
mod cycle;
mod harness;
mod history;
mod membership;
mod rmw;
mod slow_backend;

pub use api::{ApiAcct, ApiAction, ApiTransaction, ApiWorkload};
pub use cycle::CycleWorkload;
pub use glassdb_storage::sim::{MediaFaultProfile, MediaPause, SimMedia};
use glassdb_storage::{PersistentCacheConfig, SplitPolicy};
pub use harness::{
    FaultConfig, SimWorkload, run_and_assert, run_and_assert_with_faults, run_and_record,
    run_and_record_with_faults,
};
#[cfg(sim)]
pub use harness::{
    PCT_DEFAULT_DEPTH, PCT_DEFAULT_STEPS, pct_assert, pct_record, pct_sweep, record_input,
    replay_input,
};
pub use history::{HistoryCollectionOp, HistoryInstruction, HistoryTransaction, HistoryWorkload};
pub use membership::{MembOp, MembershipAcct, MembershipWorkload};
pub use rmw::{RMW_KEY_COUNT, RmwAcct, RmwOp, RmwWorkload};

use crate::db::DatabaseBuilder;

impl DatabaseBuilder {
    /// Enables the persistent cache over deterministic simulation media.
    pub fn simulated_persistent_cache(
        self,
        config: PersistentCacheConfig,
        media: SimMedia,
    ) -> Self {
        self.configure_persistent_cache(config, Some(media.into()))
    }
}

pub(super) const CLIENT_COUNT: usize = 4;
pub(super) const CLIENTS_PER_INSTANCE: usize = 2;
pub(super) const MAX_INSTANCES: usize = CLIENT_COUNT.div_ceil(CLIENTS_PER_INSTANCE);
pub(super) const MAX_OPS_PER_CLIENT: usize = 6;

pub(super) fn write_int(value: i64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub(super) fn try_read_int(value: &[u8]) -> Option<i64> {
    Some(i64::from_le_bytes(value.try_into().ok()?))
}

pub(super) fn read_int(value: &[u8]) -> i64 {
    try_read_int(value).expect("integer value has the wrong width")
}

pub(super) fn key_name(key: usize) -> Vec<u8> {
    format!("k{key}").into_bytes()
}

pub(super) fn tiny_split_policy() -> SplitPolicy {
    SplitPolicy::builder()
        .leaf_max_entries(2)
        .node_soft_max_bytes(1 << 20)
        .index_max_children(2)
        .build()
        .expect("tiny simulation split policy is valid")
}

pub(super) fn assert_valid_listing(keys: &[Vec<u8>], universe_size: usize) {
    let universe: Vec<Vec<u8>> = (0..universe_size).map(key_name).collect();
    for pair in keys.windows(2) {
        assert!(
            pair[0] < pair[1],
            "listing not strictly sorted: {:?} !< {:?}",
            pair[0],
            pair[1]
        );
    }
    for key in keys {
        assert!(
            universe.contains(key),
            "listing contains unknown key {key:?}"
        );
    }
}

#[cfg(test)]
mod sim_tests {
    use arbitrary::{Arbitrary, Unstructured};

    use super::*;

    fn assert_program_lengths<W>(data: &[u8], expected: &[usize]) -> W
    where
        W: SimWorkload + for<'a> Arbitrary<'a>,
    {
        let workload = W::arbitrary(&mut Unstructured::new(data)).unwrap();
        let lengths: Vec<_> = workload.clients().iter().map(Vec::len).collect();
        assert_eq!(lengths, expected);
        workload
    }

    fn assert_empty_clients<W>()
    where
        W: SimWorkload + for<'a> Arbitrary<'a>,
    {
        let workload = W::default();
        assert_eq!(workload.clients().len(), 4);
        assert!(workload.clients().iter().all(Vec::is_empty));
        assert_program_lengths::<W>(&[], &[0, 0, 0, 0]);
    }

    fn assert_operation_budget<W>(maximum: usize)
    where
        W: SimWorkload + for<'a> Arbitrary<'a>,
    {
        for byte in 0..=u8::MAX {
            let input = [byte; 512];
            let workload = W::arbitrary(&mut Unstructured::new(&input)).unwrap();
            assert_eq!(workload.clients().len(), 4);
            assert!(workload.clients().iter().all(|ops| ops.len() <= maximum));
        }
    }

    #[test]
    fn generated_workloads_keep_four_clients_with_empty_programs() {
        assert_empty_clients::<RmwWorkload>();
        assert_empty_clients::<CycleWorkload>();
        assert_empty_clients::<MembershipWorkload>();
        assert_empty_clients::<ApiWorkload>();
        assert_empty_clients::<HistoryWorkload>();
    }

    #[test]
    fn truncated_workloads_keep_an_active_client_among_empty_programs() {
        for client in 0..4 {
            let mut input = vec![0; client];
            input.push(1);
            let mut expected = [0; 4];
            expected[client] = 1;
            assert_program_lengths::<RmwWorkload>(&input, &expected);
            assert_program_lengths::<MembershipWorkload>(&input, &expected);
            assert_program_lengths::<ApiWorkload>(&input, &expected);
            assert_program_lengths::<HistoryWorkload>(&input, &expected);
            input.insert(0, 0);
            assert_program_lengths::<CycleWorkload>(&input, &expected);
        }
    }

    #[test]
    fn four_client_workloads_keep_the_total_operation_budget() {
        assert_operation_budget::<RmwWorkload>(6);
        assert_operation_budget::<CycleWorkload>(6);
        assert_operation_budget::<MembershipWorkload>(6);
        assert_operation_budget::<ApiWorkload>(6);
        assert_operation_budget::<HistoryWorkload>(3);
        let input = [6; 512];
        assert_program_lengths::<RmwWorkload>(&input, &[6, 6, 6, 6]);
        assert_program_lengths::<CycleWorkload>(&input, &[6, 6, 6, 6]);
        assert_program_lengths::<MembershipWorkload>(&input, &[6, 6, 6, 6]);
        assert_program_lengths::<ApiWorkload>(&input, &[6, 6, 6, 6]);
        assert_program_lengths::<HistoryWorkload>(&[3; 512], &[3, 3, 3, 3]);
    }

    #[test]
    fn generated_operations_keep_logical_client_ownership() {
        let input = [6; 512];
        let membership = assert_program_lengths::<MembershipWorkload>(&input, &[6, 6, 6, 6]);
        for (client, operations) in membership.clients.iter().enumerate() {
            for operation in operations {
                match operation {
                    MembOp::Put(key) | MembOp::Delete(key) => assert_eq!(key % 4, client),
                    _ => panic!("expected a generated membership mutation"),
                }
            }
        }

        let api = assert_program_lengths::<ApiWorkload>(&input, &[6, 6, 6, 6]);
        for (client, operations) in api.clients.iter().enumerate() {
            for operation in operations {
                assert_eq!(operation.client, client);
                for action in &operation.actions {
                    match action {
                        ApiAction::Read(key)
                        | ApiAction::Write(key, _)
                        | ApiAction::Delete(key) => {
                            assert_eq!(key % 4, client);
                        }
                        _ => panic!("expected a generated point access"),
                    }
                }
            }
        }

        let history = assert_program_lengths::<HistoryWorkload>(&[3; 512], &[3, 3, 3, 3]);
        let mut ids = Vec::new();
        for (client, operations) in history.clients.iter().enumerate() {
            for operation in operations {
                assert_eq!(operation.client_id, client);
                ids.push(operation.op_id);
            }
        }
        assert_eq!(ids, (0..12).collect::<Vec<_>>());
    }

    #[test]
    fn cycle_observer_keeps_eight_snapshots_with_empty_clients() {
        let workload = assert_program_lengths::<CycleWorkload>(&[0, 0, 0, 0, 0, 8], &[0, 0, 0, 0]);
        assert_eq!(workload.snapshot_reads, 8);
    }
}
