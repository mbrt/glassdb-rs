//! Deterministic-simulation workloads and execution harness.
//!
//! The shared [`SimWorkload`] harness runs concurrent clients over a deterministic
//! scheduler with tape-guided faults. Four focused workloads provide independent
//! correctness oracles:
//!
//! - [`RmwWorkload`] stresses shared-key serializability and in-doubt increments.
//! - [`CycleWorkload`] detects isolation failures with non-commuting ring updates.
//! - [`MembershipWorkload`] exercises key membership, splits, and listing.
//! - [`ApiWorkload`] checks transaction-local reads, writes, deletes, and aborts.

mod api;
mod cycle;
mod harness;
mod membership;
mod rmw;

pub use api::{ApiAcct, ApiAction, ApiTransaction, ApiWorkload};
pub use cycle::CycleWorkload;
pub use harness::{
    FaultConfig, SimWorkload, run_and_assert, run_and_assert_with_faults, run_and_record,
    run_and_record_with_faults,
};
#[cfg(sim)]
pub use harness::{
    PCT_DEFAULT_DEPTH, PCT_DEFAULT_STEPS, pct_assert, pct_record, pct_sweep, record_input,
    replay_input,
};
pub use membership::{MembOp, MembershipAcct, MembershipWorkload};
pub use rmw::{RMW_KEY_COUNT, RmwAcct, RmwOp, RmwWorkload};

use glassdb_storage::SplitPolicy;

pub(super) const MAX_CLIENTS: usize = 4;
pub(super) const MAX_OPS_PER_CLIENT: usize = 8;

pub(super) fn write_int(value: i64) -> Vec<u8> {
    value.to_le_bytes().to_vec()
}

pub(super) fn read_int(value: &[u8]) -> i64 {
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(value);
    i64::from_le_bytes(bytes)
}

pub(super) fn key_name(key: usize) -> Vec<u8> {
    format!("k{key}").into_bytes()
}

pub(super) fn tiny_split_policy() -> SplitPolicy {
    SplitPolicy {
        leaf_max_entries: 2,
        leaf_max_bytes: 1 << 20,
        index_max_children: 2,
        leaf_hard_cap_bytes: usize::MAX,
    }
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
