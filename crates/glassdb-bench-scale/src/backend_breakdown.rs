//! Opt-in backend-operation attribution for performance diagnostics.

use std::ops::Sub;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use glassdb_backend::{
    Backend, BackendError, ListCursor, ListLimit, ListPage, ListRequest, ReadReply, Version,
};
use glassdb_data::ObjectPath;

/// Reads, mutations, and lists attributed to one physical object role.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperationCounts {
    pub reads: u64,
    pub writes: u64,
    pub lists: u64,
    pub read_bytes: u64,
    pub write_bytes: u64,
}

impl OperationCounts {
    pub fn total(self) -> u64 {
        self.reads + self.writes + self.lists
    }
}

impl Sub for OperationCounts {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            reads: self.reads.saturating_sub(other.reads),
            writes: self.writes.saturating_sub(other.writes),
            lists: self.lists.saturating_sub(other.lists),
            read_bytes: self.read_bytes.saturating_sub(other.read_bytes),
            write_bytes: self.write_bytes.saturating_sub(other.write_bytes),
        }
    }
}

/// Backend operations grouped by GlassDB's physical object roles.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendBreakdown {
    pub database_metadata: OperationCounts,
    pub collection_record: OperationCounts,
    pub node: OperationCounts,
    pub transaction_log: OperationCounts,
    pub structural_log: OperationCounts,
    pub other: OperationCounts,
}

impl BackendBreakdown {
    pub fn rows(self) -> [(&'static str, OperationCounts); 6] {
        [
            ("backend.database_metadata", self.database_metadata),
            ("backend.collection_record", self.collection_record),
            ("backend.node", self.node),
            ("backend.transaction_log", self.transaction_log),
            ("backend.structural_log", self.structural_log),
            ("backend.other", self.other),
        ]
    }

    pub fn total(self) -> u64 {
        self.rows().into_iter().map(|(_, ops)| ops.total()).sum()
    }

    pub fn read_bytes(self) -> u64 {
        self.rows().into_iter().map(|(_, ops)| ops.read_bytes).sum()
    }

    pub fn write_bytes(self) -> u64 {
        self.rows()
            .into_iter()
            .map(|(_, ops)| ops.write_bytes)
            .sum()
    }
}

impl Sub for BackendBreakdown {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            database_metadata: self.database_metadata - other.database_metadata,
            collection_record: self.collection_record - other.collection_record,
            node: self.node - other.node,
            transaction_log: self.transaction_log - other.transaction_log,
            structural_log: self.structural_log - other.structural_log,
            other: self.other - other.other,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ObjectRole {
    DatabaseMetadata,
    CollectionRecord,
    Node,
    TransactionLog,
    StructuralLog,
    Other,
}

#[derive(Default)]
struct AtomicCounts {
    reads: AtomicU64,
    writes: AtomicU64,
    lists: AtomicU64,
    read_bytes: AtomicU64,
    write_bytes: AtomicU64,
}

impl AtomicCounts {
    fn snapshot(&self) -> OperationCounts {
        OperationCounts {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            lists: self.lists.load(Ordering::Relaxed),
            read_bytes: self.read_bytes.load(Ordering::Relaxed),
            write_bytes: self.write_bytes.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct Counters {
    database_metadata: AtomicCounts,
    collection_record: AtomicCounts,
    node: AtomicCounts,
    transaction_log: AtomicCounts,
    structural_log: AtomicCounts,
    other: AtomicCounts,
}

impl Counters {
    fn role(&self, role: ObjectRole) -> &AtomicCounts {
        match role {
            ObjectRole::DatabaseMetadata => &self.database_metadata,
            ObjectRole::CollectionRecord => &self.collection_record,
            ObjectRole::Node => &self.node,
            ObjectRole::TransactionLog => &self.transaction_log,
            ObjectRole::StructuralLog => &self.structural_log,
            ObjectRole::Other => &self.other,
        }
    }

    fn snapshot(&self) -> BackendBreakdown {
        BackendBreakdown {
            database_metadata: self.database_metadata.snapshot(),
            collection_record: self.collection_record.snapshot(),
            node: self.node.snapshot(),
            transaction_log: self.transaction_log.snapshot(),
            structural_log: self.structural_log.snapshot(),
            other: self.other.snapshot(),
        }
    }
}

/// Snapshot handle retained by a benchmark after wrapping its backend.
#[derive(Clone)]
pub struct BackendBreakdownHandle(Arc<Counters>);

impl BackendBreakdownHandle {
    pub fn snapshot(&self) -> BackendBreakdown {
        self.0.snapshot()
    }
}

/// Wraps `inner` with classified counters and returns the snapshot handle.
pub fn wrap(inner: Arc<dyn Backend>) -> (Arc<dyn Backend>, BackendBreakdownHandle) {
    let counters = Arc::new(Counters::default());
    let backend: Arc<dyn Backend> = Arc::new(ClassifiedBackend {
        inner,
        counters: counters.clone(),
    });
    (backend, BackendBreakdownHandle(counters))
}

struct ClassifiedBackend {
    inner: Arc<dyn Backend>,
    counters: Arc<Counters>,
}

impl ClassifiedBackend {
    fn count_read(&self, path: &str) {
        self.counters
            .role(classify(path))
            .reads
            .fetch_add(1, Ordering::Relaxed);
    }

    fn count_write(&self, path: &str) {
        self.counters
            .role(classify(path))
            .writes
            .fetch_add(1, Ordering::Relaxed);
    }

    fn count_list(&self, path: &str) {
        self.counters
            .role(classify(path))
            .lists
            .fetch_add(1, Ordering::Relaxed);
    }

    fn count_read_bytes(&self, path: &str, bytes: usize) {
        self.counters
            .role(classify(path))
            .read_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn count_write_bytes(&self, path: &str, bytes: usize) {
        self.counters
            .role(classify(path))
            .write_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }
}

fn classify(path: &str) -> ObjectRole {
    match ObjectPath::try_from(path) {
        Ok(path) => classify_object(&path),
        Err(_) => ObjectRole::Other,
    }
}

fn classify_object(path: &ObjectPath) -> ObjectRole {
    match path {
        ObjectPath::DatabaseMetadata { .. } => ObjectRole::DatabaseMetadata,
        ObjectPath::CollectionRecord { .. } => ObjectRole::CollectionRecord,
        ObjectPath::TreeRoot { .. } | ObjectPath::Node { .. } => ObjectRole::Node,
        ObjectPath::Transaction { .. } => ObjectRole::TransactionLog,
        ObjectPath::StructuralRecord { .. } => ObjectRole::StructuralLog,
    }
}

#[async_trait]
impl Backend for ClassifiedBackend {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.count_read(path);
        let reply = self.inner.read(path).await?;
        self.count_read_bytes(path, reply.contents.len());
        Ok(reply)
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        self.count_read(path);
        let reply = self.inner.read_if_modified(path, expected).await?;
        self.count_read_bytes(path, reply.contents.len());
        Ok(reply)
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        self.count_write(path);
        self.count_write_bytes(path, value.len());
        self.inner.write_if(path, value, expected).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        self.count_write(path);
        self.count_write_bytes(path, value.len());
        self.inner.write_if_not_exists(path, value).await
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        self.count_write(path);
        self.inner.delete_if(path, expected).await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        let Ok(request) = ListRequest::new(prefix, cursor, limit) else {
            self.count_list(prefix);
            return self.inner.list(prefix, cursor, limit).await;
        };
        self.list_request(request).await
    }

    async fn list_request(&self, request: ListRequest<'_>) -> Result<ListPage, BackendError> {
        let prefix = request.prefix();
        self.count_list(prefix);
        self.inner.list_request(request).await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_data::{CollectionAddress, DbRoot, NodeToken, StructuralRecordId, TxId};

    use super::*;

    #[test]
    fn every_object_path_variant_is_classified() {
        let db_root = DbRoot::try_from("db").unwrap();
        let collection = CollectionAddress::root("db");
        let participant = TxId::from_bytes(b"participant".to_vec());
        let cases = [
            (
                ObjectPath::DatabaseMetadata {
                    db_root: db_root.clone(),
                },
                ObjectRole::DatabaseMetadata,
            ),
            (
                ObjectPath::CollectionRecord {
                    collection: collection.clone(),
                },
                ObjectRole::CollectionRecord,
            ),
            (
                ObjectPath::TreeRoot {
                    collection: collection.clone(),
                },
                ObjectRole::Node,
            ),
            (
                ObjectPath::Node {
                    collection: collection.clone(),
                    token: NodeToken::from_bytes([1; 16]),
                },
                ObjectRole::Node,
            ),
            (
                ObjectPath::Transaction {
                    db_root: db_root.clone(),
                    id: participant.clone(),
                },
                ObjectRole::TransactionLog,
            ),
            (
                ObjectPath::StructuralRecord {
                    db_root,
                    participant,
                    record_id: StructuralRecordId::from(NodeToken::from_bytes([2; 16])),
                },
                ObjectRole::StructuralLog,
            ),
        ];

        for (path, expected) in cases {
            assert_eq!(classify_object(&path), expected);
            assert_eq!(classify(&path.to_string()), expected);
        }
    }

    #[test]
    fn misleading_embedded_markers_are_not_classified() {
        for path in [
            "db/glassdb/_t/00/00",
            "db/_c/_t/_i",
            "db/_c/0000000000000000000000/_i/_n/token",
            "db/_c/0000000000000000000000/_n/_t",
            "db/_t/_n/not-a-transaction",
            "db/_s/_t/record",
            "db/_t/0F/",
            "db/_s/record",
            "db/unknown",
        ] {
            assert_eq!(classify(path), ObjectRole::Other, "classified {path:?}");
        }
    }

    #[tokio::test]
    async fn every_backend_method_counts_and_preserves_results() {
        const COLLECTION_RECORD: &str = "db/_c/0000000000000000000000/_i";
        const COLLECTION_PREFIX: &str = "db/_c/0000000000000000000000/";
        const SECOND_OBJECT: &str = "db/_c/0000000000000000000000/_n/0000000000000000000000";

        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        inner
            .write_if_not_exists(SECOND_OBJECT, b"node".to_vec())
            .await
            .unwrap();
        let (backend, handle) = wrap(inner);

        let version = backend
            .write_if_not_exists(COLLECTION_RECORD, b"root".to_vec())
            .await
            .unwrap();
        let reply = backend.read(COLLECTION_RECORD).await.unwrap();
        assert_eq!(reply.contents, b"root");
        assert!(matches!(
            backend
                .read_if_modified(COLLECTION_RECORD, &reply.version)
                .await,
            Err(BackendError::Precondition)
        ));
        let version = backend
            .write_if(COLLECTION_RECORD, b"new".to_vec(), &version)
            .await
            .unwrap();
        let compatibility_page = backend
            .list(COLLECTION_PREFIX, None, NonZeroUsize::new(1).unwrap())
            .await
            .unwrap();
        let cursor = compatibility_page.next.clone().unwrap();
        let request_page = backend
            .list_request(
                ListRequest::new(
                    COLLECTION_PREFIX,
                    Some(&cursor),
                    NonZeroUsize::new(1).unwrap(),
                )
                .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(compatibility_page.objects, [COLLECTION_RECORD]);
        assert_eq!(request_page.objects, [SECOND_OBJECT]);
        assert!(request_page.next.is_none());
        let error = backend
            .list("invalid", None, NonZeroUsize::new(10).unwrap())
            .await
            .unwrap_err();
        assert_eq!(
            error.to_string(),
            "list prefix must be empty or end in '/': \"invalid\""
        );
        backend
            .delete_if(COLLECTION_RECORD, &version)
            .await
            .unwrap();
        assert!(matches!(
            backend.read(COLLECTION_RECORD).await,
            Err(BackendError::NotFound)
        ));

        let got = handle.snapshot();
        assert_eq!(
            got.collection_record,
            OperationCounts {
                reads: 3,
                writes: 3,
                lists: 0,
                read_bytes: 4,
                write_bytes: 7,
            }
        );
        assert_eq!(got.other.lists, 3);
        assert_eq!(got.total(), 9);
    }

    #[test]
    fn snapshots_subtract_saturating() {
        let earlier = BackendBreakdown {
            node: OperationCounts {
                reads: 2,
                writes: 3,
                lists: 1,
                read_bytes: 8,
                write_bytes: 13,
            },
            ..Default::default()
        };
        let later = BackendBreakdown {
            node: OperationCounts {
                reads: 5,
                writes: 4,
                lists: 0,
                read_bytes: 12,
                write_bytes: 10,
            },
            ..Default::default()
        };
        assert_eq!(
            (later - earlier).node,
            OperationCounts {
                reads: 3,
                writes: 1,
                lists: 0,
                read_bytes: 4,
                write_bytes: 0,
            }
        );
    }
}
