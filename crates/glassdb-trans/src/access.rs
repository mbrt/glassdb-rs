//! The vocabulary a transaction body accumulates: the point reads, key writes,
//! and range scans it performed, each carrying the physical dependencies commit
//! validates it against.
//!
//! These are the argument and result shapes shared by the commit algorithm, the
//! locker, and the resolver. They carry no commit policy, so the modules below
//! [`Algo`](crate::algo::Algo) do not have to reach up into it for their own
//! signatures.

use std::sync::Arc;

use glassdb_data::{CollectionAddress, KeyRef, TxId};
use glassdb_storage::LeafObservation;

/// Opaque validation evidence retained by a transactional point read.
#[derive(Debug, Clone)]
pub struct ReadEvidence {
    last_writer: Option<TxId>,
    leaf: LeafObservation,
}

impl ReadEvidence {
    pub(crate) fn new(last_writer: Option<TxId>, leaf: LeafObservation) -> Self {
        Self { last_writer, leaf }
    }

    pub(crate) fn into_parts(self) -> (Option<TxId>, LeafObservation) {
        (self.last_writer, self.leaf)
    }
}

/// A single key read within a transaction.
#[derive(Debug, Clone)]
pub struct ReadAccess {
    pub key: KeyRef,
    /// Effective writer observed by the read, including a tombstone writer.
    #[deprecated(note = "construct point-read access with ReadAccess::new")]
    pub last_writer: Option<TxId>,
    /// Exact leaf state from which the writer was resolved.
    #[deprecated(note = "construct point-read access with ReadAccess::new")]
    pub leaf: LeafObservation,
}

impl ReadAccess {
    /// Creates point-read access from its logical key and opaque validation evidence.
    #[allow(deprecated)]
    pub fn new(key: KeyRef, evidence: ReadEvidence) -> Self {
        let (last_writer, leaf) = evidence.into_parts();
        Self {
            key,
            last_writer,
            leaf,
        }
    }

    #[allow(deprecated)]
    pub(crate) fn last_writer(&self) -> Option<&TxId> {
        self.last_writer.as_ref()
    }

    #[allow(deprecated)]
    pub(crate) fn observation(&self) -> &LeafObservation {
        &self.leaf
    }
}

/// A single key write within a transaction.
#[derive(Debug, Clone)]
pub struct WriteAccess {
    pub key: KeyRef,
    pub(crate) op: WriteOp,
}

/// The write operation staged for a key.
#[derive(Debug, Clone)]
pub(crate) enum WriteOp {
    Put(Arc<[u8]>),
    Delete,
}

impl WriteAccess {
    pub fn put(key: KeyRef, value: Arc<[u8]>) -> Self {
        Self {
            key,
            op: WriteOp::Put(value),
        }
    }

    pub fn delete(key: KeyRef) -> Self {
        Self {
            key,
            op: WriteOp::Delete,
        }
    }
}

/// A range/sorted listing performed within a transaction (ADR-031 phantom
/// prevention). It records the logical page plus the membership version and
/// pending membership-write holders of every covered leaf. Commit validates
/// those dependencies and falls back to the logical page after physical churn.
#[derive(Debug, Clone)]
pub struct ScanAccess {
    /// Collection the scan ranged over.
    pub collection: CollectionAddress,
    /// Normalized logical range and page limit.
    pub range: ScanRange,
    /// Staged membership mutations visible when the scan ran.
    pub overlay: Vec<ScanMutation>,
    /// Keys surfaced to the transaction body.
    pub keys: Vec<Vec<u8>>,
    /// Inclusive validation/locking frontier; `None` means positive infinity.
    pub frontier: Option<Vec<u8>>,
    /// The leaves the scan covered, in key order, with membership dependencies.
    pub covered: Vec<LeafCoverage>,
}

/// A normalized half-open key range used by the transaction engine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanRange {
    /// Inclusive lower bound before applying `start_exclusive`.
    pub start: Vec<u8>,
    /// Whether the lower-bound key itself is excluded.
    pub start_exclusive: bool,
    /// Exclusive upper bound; `None` means positive infinity.
    pub end: Option<Vec<u8>>,
    /// Maximum number of keys to surface; `None` is unbounded.
    pub limit: Option<usize>,
}

impl ScanRange {
    /// Returns the unbounded range over every raw key.
    pub fn all() -> Self {
        Self {
            start: Vec::new(),
            start_exclusive: false,
            end: None,
            limit: None,
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.limit == Some(0)
            || self
                .end
                .as_deref()
                .is_some_and(|end| self.start.as_slice() >= end)
    }

    /// Reports whether `key` lies in this normalized range.
    pub fn contains(&self, key: &[u8]) -> bool {
        let above_start = if self.start_exclusive {
            key > self.start.as_slice()
        } else {
            key >= self.start.as_slice()
        };
        above_start && self.end.as_deref().is_none_or(|end| key < end)
    }
}

/// One staged membership mutation captured at scan time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ScanMutation {
    /// Raw collection key.
    pub key: Vec<u8>,
    /// Whether the staged state makes the key present.
    pub present: bool,
}

/// One leaf a scan covered and its membership-only validation dependencies.
#[derive(Debug, Clone)]
pub struct LeafCoverage {
    pub path: Arc<str>,
    pub membership_version: u64,
    pub pending_membership: Vec<TxId>,
    pub(crate) observation: LeafObservation,
}

impl PartialEq for LeafCoverage {
    fn eq(&self, other: &Self) -> bool {
        self.path == other.path
            && self.membership_version == other.membership_version
            && self.pending_membership == other.pending_membership
    }
}

impl Eq for LeafCoverage {}

/// The reads, writes, and range scans that make up a transaction.
#[derive(Debug, Clone, Default)]
pub struct Data {
    pub reads: Vec<ReadAccess>,
    pub writes: Vec<WriteAccess>,
    pub scans: Vec<ScanAccess>,
}
