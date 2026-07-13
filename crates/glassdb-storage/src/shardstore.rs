//! Compare-and-swap I/O for the v2 coordination objects (ADR-031).
//!
//! B-link nodes (`{prefix}/_n/<token>`) and collection roots (`{prefix}/_i`)
//! are the coordination units. Each store is an unconditional create-if-absent
//! or a version-conditional compare-and-swap — the only coordination primitive
//! v2 needs (ADR-023).
//!
//! Reads and writes go through the [`ObjectCache`], so a hot node/root
//! revalidates with a version-conditional `read_if_modified` and serves its
//! cached body without a re-transfer. The backend version changes on every
//! content write — exactly when a cached copy must be invalidated — so this is
//! safe.

use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_data::paths;

use crate::error::StorageError;
use crate::node::Node;
use crate::object_cache::{Freshness, ObjectCache};
use crate::root::CollectionRoot;
use crate::shard::Shard;

/// Reads and compare-and-swaps B-link node and collection-root objects through
/// the [`ObjectCache`], the v2 coordination substrate.
#[derive(Clone)]
pub struct ShardStore {
    objects: ObjectCache,
}

/// A B-link leaf loaded for one coordination round (ADR-031): its per-key
/// `entries`, the backing object's `version` (`None` if the object does not
/// exist yet), and the private [`LeafKind`] context needed to write the entries
/// back into the right object kind. The leaf is the CAS unit for its keys; it
/// lives either in the collection root `_i` (a small collection's single leaf)
/// or in a standalone node `_n`.
pub struct LoadedLeaf {
    pub entries: Shard,
    pub version: Option<backend::Version>,
    kind: LeafKind,
}

impl LoadedLeaf {
    /// The reconstruction context handed back to [`ShardStore::store_leaf`].
    pub fn kind(&self) -> &LeafKind {
        &self.kind
    }

    /// Returns the complete node carrying this leaf's entries and coordination.
    pub fn node(&self) -> &Node {
        match &self.kind {
            LeafKind::Root(root) => root.node(),
            LeafKind::Node(node) => node,
        }
    }

    /// Reports whether this loaded leaf still owns `key` — i.e. `key` is below
    /// its high-key. A `false` result means a split moved `key` to a right
    /// sibling after the key was routed here, so a caller must re-descend rather
    /// than mutate this (now wrong) leaf (ADR-031). The collection root leaf
    /// `_i` spans the whole key space until it splits into an index, so it
    /// always owns `key`.
    pub fn owns(&self, key: &[u8]) -> bool {
        match &self.kind {
            LeafKind::Root(_) => true,
            LeafKind::Node(node) => node.owns(key),
        }
    }
}

/// How a leaf's entries are written back to storage, preserving everything the
/// entries do not own: the collection metadata when the leaf is the root `_i`,
/// or the high-key/right-sibling when it is a standalone node `_n`. Opaque:
/// produced by [`ShardStore::load_leaf`] and consumed by
/// [`ShardStore::store_leaf`].
pub enum LeafKind {
    /// The leaf is the root node carried in the collection root `_i`; the stored
    /// [`CollectionRoot`] is preserved (subcollections + membership) with only
    /// its node replaced.
    Root(CollectionRoot),
    /// The leaf is a standalone node `_n`; its bounds are preserved.
    Node(Node),
}

impl ShardStore {
    /// Creates a shard store that reads and compare-and-swaps through `objects`.
    pub fn new(objects: ObjectCache) -> Self {
        ShardStore { objects }
    }

    /// Returns the shared coordination-object cache backing this store.
    pub fn object_cache(&self) -> ObjectCache {
        self.objects.clone()
    }

    /// Loads the B-link root node from the collection root `_i` under `prefix`,
    /// or `None` if the collection does not exist yet (ADR-031). The root object
    /// carries both the node and the collection metadata; this returns just the
    /// node (a leaf while small, an index once grown) with the root's version.
    pub async fn load_root_node(
        &self,
        prefix: &str,
        freshness: Freshness,
    ) -> Result<Option<(Node, backend::Version)>, StorageError> {
        match self
            .objects
            .read(&paths::collection_info(prefix), freshness)
            .await
        {
            Ok(r) => Ok(Some((
                CollectionRoot::decode(&r.value)?.node().clone(),
                r.version,
            ))),
            Err(StorageError::NotFound) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Loads the non-root node named `token` (`{prefix}/_n/<token>`, ADR-031). A
    /// [`StorageError::NotFound`] means the node is missing — a dangling child or
    /// right-sibling reference, which a descent surfaces rather than silently
    /// skips.
    pub async fn load_node(
        &self,
        prefix: &str,
        token: &str,
        freshness: Freshness,
    ) -> Result<(Node, backend::Version), StorageError> {
        let r = self
            .objects
            .read(&paths::from_node(prefix, token), freshness)
            .await?;
        Ok((Node::decode(&r.value)?, r.version))
    }

    /// Compare-and-swaps the non-root node named `token`. `expected = None` means
    /// create-if-absent (a freshly split-out sibling). Returns `false` on a
    /// precondition miss, `true` on success.
    pub async fn store_node(
        &self,
        prefix: &str,
        token: &str,
        node: &Node,
        expected: Option<&backend::Version>,
    ) -> Result<bool, StorageError> {
        let path = paths::from_node(prefix, token);
        let body: Arc<[u8]> = Arc::from(node.encode());
        let res = match expected {
            Some(v) => self.objects.write_if(&path, body, v).await,
            None => self.objects.write_if_not_exists(&path, body).await,
        };
        match res {
            Ok(_) => Ok(true),
            Err(StorageError::Precondition | StorageError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Deletes the standalone node `_n/<token>`, ignoring a missing object.
    pub async fn delete_node(&self, prefix: &str, token: &str) -> Result<(), StorageError> {
        match self.objects.delete(&paths::from_node(prefix, token)).await {
            Ok(()) | Err(StorageError::NotFound) => Ok(()),
            Err(e) => Err(e),
        }
    }

    /// Loads the leaf that lives at object `path` (ADR-031): the root leaf when
    /// `path` is a collection root `_i`, else a standalone node `_n`. Returns the
    /// empty leaf with no version when the object does not exist yet (the root
    /// leaf is created lazily on the first key write). Returns
    /// [`StorageError::Precondition`] if a concurrent split turned the routed
    /// path into an index, so the caller can re-descend.
    pub async fn load_leaf(
        &self,
        path: &str,
        freshness: Freshness,
    ) -> Result<LoadedLeaf, StorageError> {
        if paths::is_collection_info(path) {
            match self.objects.read(path, freshness).await {
                Ok(r) => {
                    let root = CollectionRoot::decode(&r.value)?;
                    let entries = root
                        .node()
                        .as_leaf()
                        .cloned()
                        .ok_or(StorageError::Precondition)?;
                    Ok(LoadedLeaf {
                        entries,
                        version: Some(r.version),
                        kind: LeafKind::Root(root),
                    })
                }
                Err(StorageError::NotFound) => Ok(LoadedLeaf {
                    entries: Shard::new(),
                    version: None,
                    kind: LeafKind::Root(CollectionRoot::new()),
                }),
                Err(e) => Err(e),
            }
        } else {
            match self.objects.read(path, freshness).await {
                Ok(r) => {
                    let node = Node::decode(&r.value)?;
                    let entries = node.as_leaf().cloned().ok_or(StorageError::Precondition)?;
                    Ok(LoadedLeaf {
                        entries,
                        version: Some(r.version),
                        kind: LeafKind::Node(node),
                    })
                }
                Err(StorageError::NotFound) => Ok(LoadedLeaf {
                    entries: Shard::new(),
                    version: None,
                    kind: LeafKind::Node(Node::leaf(Shard::new())),
                }),
                Err(e) => Err(e),
            }
        }
    }

    /// Compare-and-swaps the leaf at object `path`, writing `entries` back into
    /// the object kind captured by `kind` (root metadata or node bounds are
    /// preserved). `expected = None` means create-if-absent. Returns `false` on
    /// a precondition miss (reload and retry), `true` on success.
    pub async fn store_leaf(
        &self,
        path: &str,
        entries: &Shard,
        kind: &LeafKind,
        expected: Option<&backend::Version>,
    ) -> Result<bool, StorageError> {
        let body: Arc<[u8]> = match kind {
            LeafKind::Root(root) => {
                let mut root = root.clone();
                let mut node = root.node().clone();
                node.set_leaf(entries.clone())?;
                root.set_node(node);
                Arc::from(root.encode())
            }
            LeafKind::Node(node) => {
                let mut node = node.clone();
                node.set_leaf(entries.clone())?;
                Arc::from(node.encode())
            }
        };
        let res = match expected {
            Some(v) => self.objects.write_if(path, body, v).await,
            None => self.objects.write_if_not_exists(path, body).await,
        };
        match res {
            Ok(_) => Ok(true),
            Err(StorageError::Precondition | StorageError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Compare-and-swaps a complete leaf node while preserving root metadata.
    pub async fn store_leaf_node(
        &self,
        path: &str,
        node: &Node,
        kind: &LeafKind,
        expected: Option<&backend::Version>,
    ) -> Result<bool, StorageError> {
        if node.as_leaf().is_none() {
            return Err(StorageError::other("node is not a leaf"));
        }
        let body: Arc<[u8]> = match kind {
            LeafKind::Root(root) => {
                let mut root = root.clone();
                root.set_node(node.clone());
                Arc::from(root.encode())
            }
            LeafKind::Node(_) => Arc::from(node.encode()),
        };
        let res = match expected {
            Some(v) => self.objects.write_if(path, body, v).await,
            None => self.objects.write_if_not_exists(path, body).await,
        };
        match res {
            Ok(_) => Ok(true),
            Err(StorageError::Precondition | StorageError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Loads the collection root under `prefix`, or [`StorageError::NotFound`] if
    /// the collection does not exist.
    pub async fn load_root(
        &self,
        prefix: &str,
    ) -> Result<(CollectionRoot, backend::Version), StorageError> {
        let r = self
            .objects
            .read(&paths::collection_info(prefix), Freshness::Latest)
            .await?;
        Ok((CollectionRoot::decode(&r.value)?, r.version))
    }

    /// Compare-and-swaps the collection root. Returns `false` on a precondition
    /// miss, `true` on success.
    pub async fn store_root(
        &self,
        prefix: &str,
        root: &CollectionRoot,
        expected: &backend::Version,
    ) -> Result<bool, StorageError> {
        match self
            .objects
            .write_if(
                &paths::collection_info(prefix),
                Arc::from(root.encode()),
                expected,
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(StorageError::Precondition | StorageError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Creates the collection root if absent, reporting whether this call won the
    /// create (`true`) or found it already present (`false`).
    pub async fn create_root(
        &self,
        prefix: &str,
        root: &CollectionRoot,
    ) -> Result<bool, StorageError> {
        match self
            .objects
            .write_if_not_exists(&paths::collection_info(prefix), Arc::from(root.encode()))
            .await
        {
            Ok(_) => Ok(true),
            Err(StorageError::Precondition) => Ok(false),
            Err(e) => Err(e),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};

    use crate::entry::SharedCache;

    const COLL: &str = "coll";

    fn store_over(backend: Arc<dyn Backend>) -> ShardStore {
        ShardStore::new(ObjectCache::new(backend, &SharedCache::new(1 << 20)))
    }

    fn count(log: &OpLog, op: &str) -> usize {
        log.lock().unwrap().iter().filter(|r| r.op == op).count()
    }

    // Seeds an empty node leaf at `path` through a separate store so a later
    // reader starts with a cold cache. Creates it with a bare `write_if_not_exists`
    // (no seeding read) so the shared op log reflects only the reader's traffic.
    async fn seed_empty_leaf(backend: &Arc<dyn Backend>, path: &str) {
        let kind = LeafKind::Node(Node::leaf(Shard::new()));
        assert!(
            store_over(backend.clone())
                .store_leaf(path, &Shard::new(), &kind, None)
                .await
                .unwrap()
        );
    }

    // A node object exists in the backend. A cold cache full-fetches it on the
    // first load; a subsequent hot load revalidates with `read_if_modified`
    // instead of re-fetching, and returns the same version (ADR-023).
    #[tokio::test]
    async fn hot_reload_revalidates_without_full_read() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let path = paths::from_node(COLL, "tok");
        seed_empty_leaf(&backend, &path).await;

        let reader = store_over(backend.clone());
        let v1 = reader
            .load_leaf(&path, Freshness::Latest)
            .await
            .unwrap()
            .version;
        assert_eq!(count(&log, "read"), 1, "cold load full-reads");
        assert_eq!(count(&log, "read_if_modified"), 0);

        let v2 = reader
            .load_leaf(&path, Freshness::Latest)
            .await
            .unwrap()
            .version;
        assert_eq!(count(&log, "read"), 1, "hot load must not full-read");
        assert_eq!(
            count(&log, "read_if_modified"),
            1,
            "hot load revalidates conditionally"
        );
        assert_eq!(v1, v2, "unchanged node keeps its version");
    }

    // A cached node loaded with `AllowStale` is served without any backend op
    // (neither a full read nor a revalidation), while an uncached node still
    // does one full read (ADR-030).
    #[tokio::test]
    async fn allow_stale_serves_cached_without_backend_op() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let path = paths::from_node(COLL, "tok");
        seed_empty_leaf(&backend, &path).await;

        let reader = store_over(backend.clone());
        // Warm the cache with one cold full read.
        reader.load_leaf(&path, Freshness::Latest).await.unwrap();
        assert_eq!(count(&log, "read"), 1);

        // A cached AllowStale load touches the backend for nothing.
        reader
            .load_leaf(&path, Freshness::AllowStale)
            .await
            .unwrap();
        assert_eq!(count(&log, "read"), 1, "cached AllowStale must not read");
        assert_eq!(
            count(&log, "read_if_modified"),
            0,
            "cached AllowStale must not revalidate"
        );

        // An uncached node has nothing to serve, so it falls through to a read.
        let other = paths::from_node(COLL, "tok2");
        reader
            .load_leaf(&other, Freshness::AllowStale)
            .await
            .unwrap();
        assert_eq!(count(&log, "read"), 2, "uncached AllowStale falls through");
    }

    // A write-through store updates the cache, so a load after a CAS observes the
    // freshly written content and its new version.
    #[tokio::test]
    async fn store_is_visible_to_next_load() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let path = paths::from_node(COLL, "tok");

        let loaded = store.load_leaf(&path, Freshness::Latest).await.unwrap();
        assert!(
            store
                .store_leaf(&path, &Shard::new(), loaded.kind(), None)
                .await
                .unwrap()
        );
        let loaded = store.load_leaf(&path, Freshness::Latest).await.unwrap();
        let v1 = loaded.version.clone().expect("node exists after create");

        // CAS a new generation over the loaded version, then confirm the next
        // load reflects it.
        assert!(
            store
                .store_leaf(&path, &Shard::new(), loaded.kind(), Some(&v1))
                .await
                .unwrap()
        );
        let v2 = store
            .load_leaf(&path, Freshness::Latest)
            .await
            .unwrap()
            .version;
        assert_ne!(Some(v1), v2, "a CAS store advances the version");
    }
}
