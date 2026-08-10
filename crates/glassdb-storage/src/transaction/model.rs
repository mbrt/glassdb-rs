use std::sync::Arc;
use std::time::SystemTime;

use glassdb_data::{CollectionAddress, KeyRef, LeafRef, TxId};

use crate::cached_store::Observation;
use crate::error::StorageError;
use crate::lock::LockType;

/// The commit state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TxCommitStatus {
    #[default]
    Unknown,
    Ok,
    Aborted,
    Pending,
    Wounded,
}

impl TxCommitStatus {
    /// Reports whether the transaction can still commit under this identity.
    pub fn is_final(self) -> bool {
        matches!(
            self,
            TxCommitStatus::Ok | TxCommitStatus::Aborted | TxCommitStatus::Wounded
        )
    }

    /// Reports whether the persisted status can no longer change.
    pub fn is_immutable(self) -> bool {
        matches!(self, TxCommitStatus::Ok | TxCommitStatus::Aborted)
    }
}

/// The normalized durable state of a transaction-log record.
///
/// Missing records are represented explicitly; [`TxCommitStatus::Unknown`] is
/// never a persisted state.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxRecordState {
    Missing,
    Pending,
    Wounded,
    Committed,
    Aborted,
}

/// The semantic relationship between two transaction-record states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxLifecycleRelation {
    /// Both sides describe the same semantic state.
    Same,
    /// The durable lifecycle permits advancing from the first state to the second.
    CanAdvance,
    /// Advancing would contradict an existing durable decision.
    Blocks,
}

impl TxRecordState {
    /// Normalizes an optional persisted status, rejecting `Unknown`.
    pub fn try_from_status(status: Option<TxCommitStatus>) -> Result<Self, StorageError> {
        match status {
            None => Ok(TxRecordState::Missing),
            Some(TxCommitStatus::Pending) => Ok(TxRecordState::Pending),
            Some(TxCommitStatus::Wounded) => Ok(TxRecordState::Wounded),
            Some(TxCommitStatus::Ok) => Ok(TxRecordState::Committed),
            Some(TxCommitStatus::Aborted) => Ok(TxRecordState::Aborted),
            Some(TxCommitStatus::Unknown) => Err(StorageError::other(
                "unknown is not a persisted transaction status",
            )),
        }
    }

    /// Returns the normalized state represented by an exact observation.
    pub fn try_from_observation(observed: &Observation<TxLog>) -> Result<Self, StorageError> {
        Self::try_from_status(observed.value().map(|log| log.status))
    }

    /// Relates this state to a desired durable state using the transaction
    /// lifecycle graph.
    pub fn relation_to(self, desired: TxRecordState) -> TxLifecycleRelation {
        if self == desired {
            return TxLifecycleRelation::Same;
        }
        match (self, desired) {
            (
                TxRecordState::Missing,
                TxRecordState::Pending
                | TxRecordState::Wounded
                | TxRecordState::Committed
                | TxRecordState::Aborted,
            )
            | (
                TxRecordState::Pending,
                TxRecordState::Wounded | TxRecordState::Committed | TxRecordState::Aborted,
            )
            | (TxRecordState::Wounded, TxRecordState::Aborted)
            | (TxRecordState::Committed | TxRecordState::Aborted, TxRecordState::Missing) => {
                TxLifecycleRelation::CanAdvance
            }
            _ => TxLifecycleRelation::Blocks,
        }
    }
}

/// The full contents of a transaction log entry.
#[derive(Debug, Clone)]
pub struct TxLog {
    pub id: TxId,
    /// `None` means "use the current time when persisting".
    pub timestamp: Option<SystemTime>,
    pub status: TxCommitStatus,
    pub writes: Vec<TxWrite>,
    pub locks: Vec<TxLock>,
    pub collection_changes: Vec<TxCollectionChange>,
    pub prepared_collections: Vec<CollectionAddress>,
}

impl TxLog {
    /// Creates an empty log for the given transaction.
    pub fn new(id: TxId, status: TxCommitStatus) -> Self {
        TxLog {
            id,
            timestamp: None,
            status,
            writes: Vec::new(),
            locks: Vec::new(),
            collection_changes: Vec::new(),
            prepared_collections: Vec::new(),
        }
    }
}

/// One direct-child directory mutation committed by a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxCollectionChange {
    pub parent: CollectionAddress,
    pub name: Vec<u8>,
    pub collection: CollectionAddress,
    pub op: TxCollectionOp,
}

/// The effect a transaction applies to one direct-child binding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxCollectionOp {
    Create,
    Drop,
}

/// A single write within a transaction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TxWrite {
    pub key: KeyRef,
    pub value: Arc<[u8]>,
    pub deleted: bool,
    pub prev_writer: TxId,
}

/// A transaction lock backreference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TxLock {
    Entry {
        key: KeyRef,
        typ: LockType,
    },
    Membership {
        leaf: LeafRef,
        typ: LockType,
    },
    Directory {
        collection: CollectionAddress,
        typ: LockType,
    },
    Topology {
        collection: CollectionAddress,
    },
}

impl TxLock {
    /// Returns the lock type recorded for this backreference.
    pub fn typ(&self) -> LockType {
        match self {
            TxLock::Entry { typ, .. }
            | TxLock::Membership { typ, .. }
            | TxLock::Directory { typ, .. } => *typ,
            TxLock::Topology { .. } => LockType::Write,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lifecycle_relation_defines_the_complete_state_graph() {
        use TxLifecycleRelation::{Blocks, CanAdvance, Same};
        use TxRecordState::{Aborted, Committed, Missing, Pending, Wounded};

        let states = [Missing, Pending, Wounded, Committed, Aborted];
        let advances = [
            (Missing, Pending),
            (Missing, Wounded),
            (Missing, Committed),
            (Missing, Aborted),
            (Pending, Wounded),
            (Pending, Committed),
            (Pending, Aborted),
            (Wounded, Aborted),
            (Committed, Missing),
            (Aborted, Missing),
        ];

        for current in states {
            for desired in states {
                let expected = if current == desired {
                    Same
                } else if advances.contains(&(current, desired)) {
                    CanAdvance
                } else {
                    Blocks
                };
                assert_eq!(current.relation_to(desired), expected);
            }
        }
    }
}
