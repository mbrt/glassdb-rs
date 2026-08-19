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
    absence_generation: Option<u64>,
    leaf: LeafObservation,
}

impl ReadEvidence {
    pub(crate) fn new(last_writer: Option<TxId>, leaf: LeafObservation) -> Self {
        let absence_generation = last_writer
            .is_none()
            .then(|| leaf.value().map_or(0, |node| node.membership_version()));
        Self {
            last_writer,
            absence_generation,
            leaf,
        }
    }

    pub(crate) fn last_writer(&self) -> Option<&TxId> {
        self.last_writer.as_ref()
    }

    pub(crate) fn observation(&self) -> &LeafObservation {
        &self.leaf
    }

    pub(crate) fn absence_generation(&self) -> Option<u64> {
        self.absence_generation
    }

    pub(crate) fn validates(&self, writer: Option<&TxId>, membership_version: u64) -> bool {
        self.last_writer.as_ref() == writer
            && self
                .absence_generation
                .is_none_or(|observed| observed == membership_version)
    }
}

/// A single key read within a transaction.
#[derive(Debug, Clone)]
pub struct ReadAccess {
    pub key: KeyRef,
    evidence: ReadEvidence,
}

impl ReadAccess {
    /// Creates point-read access from its logical key and opaque validation evidence.
    pub fn new(key: KeyRef, evidence: ReadEvidence) -> Self {
        Self { key, evidence }
    }

    pub(crate) fn last_writer(&self) -> Option<&TxId> {
        self.evidence.last_writer()
    }

    pub(crate) fn observation(&self) -> &LeafObservation {
        self.evidence.observation()
    }

    pub(crate) fn absence_generation(&self) -> Option<u64> {
        self.evidence.absence_generation()
    }

    pub(crate) fn validates(&self, writer: Option<&TxId>, membership_version: u64) -> bool {
        self.evidence.validates(writer, membership_version)
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

/// Opaque validation evidence retained by a transactional range scan.
#[derive(Debug, Clone)]
pub(crate) struct ScanEvidence {
    keys: Vec<Vec<u8>>,
    covered: Vec<LeafCoverage>,
    frontier: Option<Vec<u8>>,
}

impl ScanEvidence {
    pub(crate) fn new(
        keys: Vec<Vec<u8>>,
        covered: Vec<LeafCoverage>,
        frontier: Option<Vec<u8>>,
    ) -> Self {
        Self {
            keys,
            covered,
            frontier,
        }
    }

    pub(crate) fn keys(&self) -> &[Vec<u8>] {
        &self.keys
    }

    pub(crate) fn frontier(&self) -> Option<&[u8]> {
        self.frontier.as_deref()
    }

    pub(crate) fn covered(&self) -> &[LeafCoverage] {
        &self.covered
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
    evidence: ScanEvidence,
}

impl ScanAccess {
    pub(crate) fn new(
        collection: CollectionAddress,
        range: ScanRange,
        overlay: Vec<ScanMutation>,
        evidence: ScanEvidence,
    ) -> Self {
        Self {
            collection,
            range,
            overlay,
            evidence,
        }
    }

    pub(crate) fn keys(&self) -> &[Vec<u8>] {
        self.evidence.keys()
    }

    pub(crate) fn frontier(&self) -> Option<&[u8]> {
        self.evidence.frontier()
    }

    pub(crate) fn covered(&self) -> &[LeafCoverage] {
        self.evidence.covered()
    }
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
pub(crate) struct LeafCoverage {
    pub(crate) path: Arc<str>,
    pub(crate) membership_version: u64,
    pub(crate) pending_membership: Vec<TxId>,
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
