//! The access-set module owns the point reads, final key writes, and range scans
//! from one transaction-body execution. It normalizes point facts and retains
//! the physical dependencies used for validation.
//!
//! Its interface supplies shared access facts to the commit algorithm, locker,
//! and resolver. It carries no routing, locking, or commit policy.

use std::cmp::Ordering;
use std::sync::Arc;

use glassdb_data::{CollectionAddress, LogicalKey, TxId};
use glassdb_storage::LeafObservation;

/// Opaque validation evidence retained by a transactional point read.
#[derive(Debug, Clone)]
pub struct ReadEvidence {
    predicate: ReadPredicate,
    leaf: LeafObservation,
}

impl ReadEvidence {
    pub(crate) fn new(last_writer: Option<TxId>, leaf: LeafObservation) -> Self {
        let absence_generation = last_writer
            .is_none()
            .then(|| leaf.value().map_or(0, |node| node.membership_version()));
        Self {
            predicate: ReadPredicate::new(last_writer, absence_generation),
            leaf,
        }
    }

    pub(crate) fn observation(&self) -> &LeafObservation {
        &self.leaf
    }

    pub(crate) fn predicate(&self) -> &ReadPredicate {
        &self.predicate
    }

    pub(crate) fn validates(&self, writer: Option<&TxId>, membership_version: u64) -> bool {
        self.predicate.validates(writer, membership_version)
    }
}

/// The logical point-read fact used to validate an effective key state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReadPredicate {
    last_writer: Option<TxId>,
    absence_generation: Option<u64>,
}

impl ReadPredicate {
    pub(crate) fn new(last_writer: Option<TxId>, absence_generation: Option<u64>) -> Self {
        Self {
            last_writer,
            absence_generation,
        }
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
    key: LogicalKey,
    evidence: ReadEvidence,
}

impl ReadAccess {
    /// Creates point-read access from its logical key and opaque validation evidence.
    pub fn new(key: LogicalKey, evidence: ReadEvidence) -> Self {
        Self { key, evidence }
    }

    pub(crate) fn key(&self) -> &LogicalKey {
        &self.key
    }

    pub(crate) fn observation(&self) -> &LeafObservation {
        self.evidence.observation()
    }

    pub(crate) fn predicate(&self) -> &ReadPredicate {
        self.evidence.predicate()
    }

    pub(crate) fn validates(&self, writer: Option<&TxId>, membership_version: u64) -> bool {
        self.evidence.validates(writer, membership_version)
    }
}

/// A single key write within a transaction.
#[derive(Debug, Clone)]
pub struct WriteAccess {
    key: LogicalKey,
    op: WriteOp,
}

/// The write operation staged for a key.
#[derive(Debug, Clone)]
pub(crate) enum WriteOp {
    Put(Arc<[u8]>),
    Delete,
}

impl WriteAccess {
    pub fn put(key: LogicalKey, value: Arc<[u8]>) -> Self {
        Self {
            key,
            op: WriteOp::Put(value),
        }
    }

    pub fn delete(key: LogicalKey) -> Self {
        Self {
            key,
            op: WriteOp::Delete,
        }
    }

    pub(crate) fn key(&self) -> &LogicalKey {
        &self.key
    }

    pub(crate) fn operation(&self) -> &WriteOp {
        &self.op
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
    collection: CollectionAddress,
    /// Normalized logical range and page limit.
    range: ScanRange,
    /// Staged membership mutations visible when the scan ran.
    overlay: Vec<ScanMutation>,
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

    pub(crate) fn collection(&self) -> &CollectionAddress {
        &self.collection
    }

    pub(crate) fn range(&self) -> &ScanRange {
        &self.range
    }

    pub(crate) fn overlay(&self) -> &[ScanMutation] {
        &self.overlay
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

/// The point reads, final key writes, and range scans from one transaction-body
/// execution.
#[derive(Debug, Clone, Default)]
pub struct AccessSet {
    reads: Vec<ReadAccess>,
    writes: Vec<WriteAccess>,
    scans: Vec<ScanAccess>,
}

impl AccessSet {
    /// Creates an immutable access set in deterministic key order.
    pub fn new(
        mut reads: Vec<ReadAccess>,
        mut writes: Vec<WriteAccess>,
        scans: Vec<ScanAccess>,
    ) -> Self {
        normalize(&mut reads);
        normalize(&mut writes);
        Self {
            reads,
            writes,
            scans,
        }
    }

    /// Returns the number of distinct point reads.
    pub fn read_count(&self) -> usize {
        self.reads.len()
    }

    /// Returns the number of distinct final key writes.
    pub fn write_count(&self) -> usize {
        self.writes.len()
    }

    /// Returns the validation dependencies without final key writes.
    pub fn into_read_only(mut self) -> Self {
        self.writes.clear();
        self
    }

    pub(crate) fn has_writes(&self) -> bool {
        !self.writes.is_empty()
    }

    pub(crate) fn point_reads(&self) -> &[ReadAccess] {
        &self.reads
    }

    pub(crate) fn final_writes(&self) -> &[WriteAccess] {
        &self.writes
    }

    pub(crate) fn range_scans(&self) -> &[ScanAccess] {
        &self.scans
    }

    /// Returns each point key once, with its read and final-write facts paired.
    pub(crate) fn points(&self) -> PointAccesses<'_> {
        PointAccesses {
            reads: &self.reads,
            writes: &self.writes,
            read_index: 0,
            write_index: 0,
        }
    }

    /// Returns the complete point-mutation shape used by logless commit.
    pub(crate) fn direct_shape(&self) -> Option<DirectShape<'_>> {
        (self.has_writes() && self.scans.is_empty()).then_some(DirectShape { accesses: self })
    }
}

/// A complete point-mutation access set with no range scans.
pub(crate) struct DirectShape<'a> {
    accesses: &'a AccessSet,
}

impl DirectShape<'_> {
    pub(crate) fn points(&self) -> PointAccesses<'_> {
        self.accesses.points()
    }

    pub(crate) fn read_count(&self) -> usize {
        self.accesses.read_count()
    }

    pub(crate) fn write_count(&self) -> usize {
        self.accesses.write_count()
    }
}

/// One key's optional point-read fact and optional final key write.
#[derive(Clone, Copy)]
pub(crate) struct PointAccess<'a> {
    pub(crate) key: &'a LogicalKey,
    pub(crate) read: Option<&'a ReadAccess>,
    pub(crate) write: Option<&'a WriteAccess>,
}

/// A zero-allocation merge of the access set's sorted read and write runs.
pub(crate) struct PointAccesses<'a> {
    reads: &'a [ReadAccess],
    writes: &'a [WriteAccess],
    read_index: usize,
    write_index: usize,
}

impl<'a> Iterator for PointAccesses<'a> {
    type Item = PointAccess<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        let read = self.reads.get(self.read_index);
        let write = self.writes.get(self.write_index);
        match (read, write) {
            (Some(read), Some(write)) => match read.key().cmp(write.key()) {
                Ordering::Less => {
                    self.read_index += 1;
                    Some(PointAccess {
                        key: read.key(),
                        read: Some(read),
                        write: None,
                    })
                }
                Ordering::Equal => {
                    self.read_index += 1;
                    self.write_index += 1;
                    Some(PointAccess {
                        key: read.key(),
                        read: Some(read),
                        write: Some(write),
                    })
                }
                Ordering::Greater => {
                    self.write_index += 1;
                    Some(PointAccess {
                        key: write.key(),
                        read: None,
                        write: Some(write),
                    })
                }
            },
            (Some(read), None) => {
                self.read_index += 1;
                Some(PointAccess {
                    key: read.key(),
                    read: Some(read),
                    write: None,
                })
            }
            (None, Some(write)) => {
                self.write_index += 1;
                Some(PointAccess {
                    key: write.key(),
                    read: None,
                    write: Some(write),
                })
            }
            (None, None) => None,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let reads = self.reads.len() - self.read_index;
        let writes = self.writes.len() - self.write_index;
        (reads.max(writes), Some(reads + writes))
    }
}

trait KeyedAccess {
    fn key(&self) -> &LogicalKey;
}

impl KeyedAccess for ReadAccess {
    fn key(&self) -> &LogicalKey {
        self.key()
    }
}

impl KeyedAccess for WriteAccess {
    fn key(&self) -> &LogicalKey {
        self.key()
    }
}

fn normalize<T: KeyedAccess>(accesses: &mut Vec<T>) {
    accesses.sort_by(|left, right| left.key().cmp(right.key()));
    if !accesses
        .windows(2)
        .any(|pair| pair[0].key() == pair[1].key())
    {
        return;
    }
    // Reversing the stable order makes `dedup_by` retain the final record for
    // each duplicate key, which matches transaction staging without a map.
    accesses.reverse();
    accesses.dedup_by(|left, right| left.key() == right.key());
    accesses.reverse();
}

#[cfg(test)]
mod tests {
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_storage::{CachedStore, NodeStore, Requirement, Timeline};

    use super::*;

    fn key(raw: &[u8]) -> LogicalKey {
        LogicalKey::new(CollectionAddress::root("accesstest"), raw)
    }

    async fn point_read(key: LogicalKey, last_writer: Option<TxId>) -> ReadAccess {
        let store = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1024 * 1024,
            Timeline::new(),
            None,
        );
        let observation = NodeStore::new(store, std::num::NonZeroUsize::MIN)
            .load_root_state(key.collection(), Requirement::Any)
            .await
            .unwrap();
        ReadAccess::new(key, ReadEvidence::new(last_writer, observation))
    }

    #[tokio::test]
    async fn normalizes_and_merges_point_facts() {
        let a = key(b"a");
        let b = key(b"b");
        let old_writer = TxId::with_priority(1, b"old");
        let reads = vec![
            point_read(b.clone(), Some(old_writer.clone())).await,
            point_read(a.clone(), Some(old_writer.clone())).await,
            point_read(a.clone(), None).await,
        ];
        let writes = vec![
            WriteAccess::put(b.clone(), Arc::from(b"b".as_slice())),
            WriteAccess::put(a.clone(), Arc::from(b"old".as_slice())),
            WriteAccess::delete(a.clone()),
        ];

        let accesses = AccessSet::new(reads, writes, Vec::new());

        assert_eq!(accesses.read_count(), 2);
        assert_eq!(accesses.write_count(), 2);
        let points = accesses.points().collect::<Vec<_>>();
        assert_eq!(points.len(), 2);
        assert_eq!(points[0].key, &a);
        assert_eq!(points[1].key, &b);
        let final_read = points[0].read.unwrap();
        assert!(final_read.validates(None, 0));
        assert!(!final_read.validates(Some(&old_writer), 0));
        assert!(matches!(
            points[0].write.unwrap().operation(),
            WriteOp::Delete
        ));
        assert!(points[1].read.is_some());
        assert!(matches!(
            points[1].write.unwrap().operation(),
            WriteOp::Put(value) if value.as_ref() == b"b"
        ));
    }

    #[test]
    fn read_predicate_distinguishes_blind_writes_from_observed_absence() {
        let writer = TxId::with_priority(1, b"writer");
        let present = ReadPredicate::new(Some(writer.clone()), None);
        let absent = ReadPredicate::new(None, Some(7));

        assert!(present.validates(Some(&writer), 99));
        assert!(!present.validates(None, 99));
        assert!(absent.validates(None, 7));
        assert!(!absent.validates(None, 8));

        let blind = AccessSet::new(
            Vec::new(),
            vec![WriteAccess::delete(key(b"blind"))],
            Vec::new(),
        );
        assert!(blind.points().next().unwrap().read.is_none());
    }

    #[test]
    fn read_only_projection_keeps_scan_order_and_overlay() {
        let collection = CollectionAddress::root("accesstest");
        let first = ScanAccess::new(
            collection.clone(),
            ScanRange {
                start: b"b".to_vec(),
                start_exclusive: false,
                end: None,
                limit: None,
            },
            vec![ScanMutation {
                key: b"staged".to_vec(),
                present: true,
            }],
            ScanEvidence::new(Vec::new(), Vec::new(), None),
        );
        let second = ScanAccess::new(
            collection,
            ScanRange::all(),
            Vec::new(),
            ScanEvidence::new(Vec::new(), Vec::new(), None),
        );
        let accesses = AccessSet::new(
            Vec::new(),
            vec![WriteAccess::delete(key(b"staged"))],
            vec![first, second],
        )
        .into_read_only();

        assert_eq!(accesses.write_count(), 0);
        assert_eq!(accesses.range_scans()[0].range().start, b"b");
        assert_eq!(
            accesses.range_scans()[0].overlay(),
            &[ScanMutation {
                key: b"staged".to_vec(),
                present: true,
            }]
        );
        assert!(accesses.range_scans()[1].range().start.is_empty());
    }

    #[test]
    fn direct_shape_accepts_only_point_mutations() {
        let write = || WriteAccess::delete(key(b"key"));
        assert!(
            AccessSet::new(Vec::new(), vec![write()], Vec::new())
                .direct_shape()
                .is_some()
        );
        assert!(AccessSet::default().direct_shape().is_none());

        let scan = ScanAccess::new(
            CollectionAddress::root("accesstest"),
            ScanRange::all(),
            Vec::new(),
            ScanEvidence::new(Vec::new(), Vec::new(), None),
        );
        assert!(
            AccessSet::new(Vec::new(), vec![write()], vec![scan])
                .direct_shape()
                .is_none()
        );
    }
}
