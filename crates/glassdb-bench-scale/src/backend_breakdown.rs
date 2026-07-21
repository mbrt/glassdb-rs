//! Opt-in backend-operation attribution for performance diagnostics.

use std::ops::Sub;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use async_trait::async_trait;
use glassdb_backend::{Backend, BackendError, ListCursor, ListLimit, ListPage, ReadReply, Version};

/// Reads, mutations, and lists attributed to one physical object role.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct OperationCounts {
    pub reads: u64,
    pub writes: u64,
    pub lists: u64,
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
        }
    }
}

/// Backend operations grouped by GlassDB's physical object roles.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackendBreakdown {
    pub database_metadata: OperationCounts,
    pub collection_root: OperationCounts,
    pub node: OperationCounts,
    pub transaction_log: OperationCounts,
    pub structural_log: OperationCounts,
    pub other: OperationCounts,
}

impl BackendBreakdown {
    pub fn rows(self) -> [(&'static str, OperationCounts); 6] {
        [
            ("backend.database_metadata", self.database_metadata),
            ("backend.collection_root", self.collection_root),
            ("backend.node", self.node),
            ("backend.transaction_log", self.transaction_log),
            ("backend.structural_log", self.structural_log),
            ("backend.other", self.other),
        ]
    }

    pub fn total(self) -> u64 {
        self.rows().into_iter().map(|(_, ops)| ops.total()).sum()
    }
}

impl Sub for BackendBreakdown {
    type Output = Self;

    fn sub(self, other: Self) -> Self {
        Self {
            database_metadata: self.database_metadata - other.database_metadata,
            collection_root: self.collection_root - other.collection_root,
            node: self.node - other.node,
            transaction_log: self.transaction_log - other.transaction_log,
            structural_log: self.structural_log - other.structural_log,
            other: self.other - other.other,
        }
    }
}

#[derive(Clone, Copy)]
enum ObjectRole {
    DatabaseMetadata,
    CollectionRoot,
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
}

impl AtomicCounts {
    fn snapshot(&self) -> OperationCounts {
        OperationCounts {
            reads: self.reads.load(Ordering::Relaxed),
            writes: self.writes.load(Ordering::Relaxed),
            lists: self.lists.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct Counters {
    database_metadata: AtomicCounts,
    collection_root: AtomicCounts,
    node: AtomicCounts,
    transaction_log: AtomicCounts,
    structural_log: AtomicCounts,
    other: AtomicCounts,
}

impl Counters {
    fn role(&self, role: ObjectRole) -> &AtomicCounts {
        match role {
            ObjectRole::DatabaseMetadata => &self.database_metadata,
            ObjectRole::CollectionRoot => &self.collection_root,
            ObjectRole::Node => &self.node,
            ObjectRole::TransactionLog => &self.transaction_log,
            ObjectRole::StructuralLog => &self.structural_log,
            ObjectRole::Other => &self.other,
        }
    }

    fn snapshot(&self) -> BackendBreakdown {
        BackendBreakdown {
            database_metadata: self.database_metadata.snapshot(),
            collection_root: self.collection_root.snapshot(),
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
}

fn classify(path: &str) -> ObjectRole {
    let mut parts = path.split('/').filter(|part| !part.is_empty());
    let Some(_database) = parts.next() else {
        return ObjectRole::Other;
    };
    let Some(mut part) = parts.next() else {
        return ObjectRole::Other;
    };
    while part == "_c" {
        if parts.next().is_none() {
            return ObjectRole::Other;
        }
        let Some(next) = parts.next() else {
            return ObjectRole::Other;
        };
        part = next;
    }
    match part {
        "glassdb" => ObjectRole::DatabaseMetadata,
        "_i" => ObjectRole::CollectionRoot,
        "_n" => ObjectRole::Node,
        "_t" => ObjectRole::TransactionLog,
        "_s" => ObjectRole::StructuralLog,
        _ => ObjectRole::Other,
    }
}

#[async_trait]
impl Backend for ClassifiedBackend {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        self.count_read(path);
        self.inner.read(path).await
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        self.count_read(path);
        self.inner.read_if_modified(path, expected).await
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        self.count_write(path);
        self.inner.write_if(path, value, expected).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        self.count_write(path);
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
        self.count_list(prefix);
        self.inner.list(prefix, cursor, limit).await
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use glassdb_backend::memory::MemoryBackend;

    use super::*;

    #[test]
    fn physical_paths_are_classified_without_parsing_payloads() {
        assert!(matches!(
            classify("db/glassdb"),
            ObjectRole::DatabaseMetadata
        ));
        assert!(matches!(
            classify("db/_c/Y29sbA/_i"),
            ObjectRole::CollectionRoot
        ));
        assert!(matches!(
            classify("db/_c/_t/_i"),
            ObjectRole::CollectionRoot
        ));
        assert!(matches!(
            classify("db/_c/Y29sbA/_n/token"),
            ObjectRole::Node
        ));
        assert!(matches!(
            classify("db/_t/0F/encoded"),
            ObjectRole::TransactionLog
        ));
        assert!(matches!(classify("db/_t/0F/"), ObjectRole::TransactionLog));
        assert!(matches!(
            classify("db/_s/record"),
            ObjectRole::StructuralLog
        ));
        assert!(matches!(classify("db/unknown"), ObjectRole::Other));
    }

    #[tokio::test]
    async fn every_backend_method_counts_and_preserves_results() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (backend, handle) = wrap(inner);

        let version = backend
            .write_if_not_exists("db/_c/Y29sbA/_i", b"root".to_vec())
            .await
            .unwrap();
        let reply = backend.read("db/_c/Y29sbA/_i").await.unwrap();
        assert_eq!(reply.contents, b"root");
        assert!(matches!(
            backend
                .read_if_modified("db/_c/Y29sbA/_i", &reply.version)
                .await,
            Err(BackendError::Precondition)
        ));
        let version = backend
            .write_if("db/_c/Y29sbA/_i", b"new".to_vec(), &version)
            .await
            .unwrap();
        let page = backend
            .list("db/_c/Y29sbA/", None, NonZeroUsize::new(10).unwrap())
            .await
            .unwrap();
        assert_eq!(page.objects, ["db/_c/Y29sbA/_i"]);
        backend
            .delete_if("db/_c/Y29sbA/_i", &version)
            .await
            .unwrap();
        assert!(matches!(
            backend.read("db/_c/Y29sbA/_i").await,
            Err(BackendError::NotFound)
        ));

        let got = handle.snapshot();
        assert_eq!(
            got.collection_root,
            OperationCounts {
                reads: 3,
                writes: 3,
                lists: 0,
            }
        );
        assert_eq!(got.other.lists, 1);
        assert_eq!(got.total(), 7);
    }

    #[test]
    fn snapshots_subtract_saturating() {
        let earlier = BackendBreakdown {
            node: OperationCounts {
                reads: 2,
                writes: 3,
                lists: 1,
            },
            ..Default::default()
        };
        let later = BackendBreakdown {
            node: OperationCounts {
                reads: 5,
                writes: 4,
                lists: 0,
            },
            ..Default::default()
        };
        assert_eq!(
            (later - earlier).node,
            OperationCounts {
                reads: 3,
                writes: 1,
                lists: 0,
            }
        );
    }
}
