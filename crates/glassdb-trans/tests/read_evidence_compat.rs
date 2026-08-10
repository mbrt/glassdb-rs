#![allow(deprecated)]

use glassdb_data::{KeyRef, TxId};
use glassdb_storage::LeafObservation;
use glassdb_trans::{ReadAccess, ReadOutcome, ReadValue};

fn legacy_access(key: KeyRef, last_writer: Option<TxId>, leaf: LeafObservation) -> ReadAccess {
    ReadAccess {
        key,
        last_writer,
        leaf,
    }
}

fn legacy_outcome(
    value: Option<ReadValue>,
    last_writer: Option<TxId>,
    cache_hit: bool,
    leaf: LeafObservation,
) -> ReadOutcome {
    ReadOutcome {
        value,
        last_writer,
        cache_hit,
        leaf,
    }
}

fn opaque_round_trip(key: KeyRef, outcome: ReadOutcome) -> (ReadAccess, ReadOutcome) {
    let (value, cache_hit, evidence) = outcome.into_parts();
    let access = ReadAccess::new(key, evidence.clone());
    let outcome = ReadOutcome::new(value, cache_hit, evidence);
    (access, outcome)
}

#[test]
fn legacy_and_opaque_point_read_construction_compile() {
    let _: fn(KeyRef, Option<TxId>, LeafObservation) -> ReadAccess = legacy_access;
    let _: fn(Option<ReadValue>, Option<TxId>, bool, LeafObservation) -> ReadOutcome =
        legacy_outcome;
    let _: fn(KeyRef, ReadOutcome) -> (ReadAccess, ReadOutcome) = opaque_round_trip;
}
