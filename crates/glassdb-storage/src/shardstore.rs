//! Compare-and-swap I/O for the v2 coordination objects (ADR-031).
//!
//! B-link nodes (`{prefix}/_n/<token>`) and collection roots (`{prefix}/_i`)
//! are the coordination units. Each mutation is a create-if-absent, a
//! version-conditional compare-and-swap, or an exact-revision delete
//! (ADR-023/ADR-042).
//!
//! Reads and mutations go through the decoded [`CachedStore`].

use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_data::paths;

use crate::cached_store::{
    CachedStore, CasResult, Codec, Observation, ObservationCheck, Requirement,
};
use crate::error::StorageError;
use crate::node::{Node, NodeLocks};
use crate::root::CollectionRoot;
use crate::shard::Shard;
use crate::structlog::StructuralLog;
use crate::timeline::SequencePoint;

const STRUCTURAL_LIST_PAGE_SIZE: usize = 128;

/// Reads and compare-and-swaps B-link node and collection-root objects.
#[derive(Clone)]
pub struct ShardStore {
    roots: crate::cached_store::TypedCachedStore<CollectionRoot>,
    nodes: crate::cached_store::TypedCachedStore<Node>,
    structural_logs: crate::cached_store::TypedCachedStore<StructuralLog>,
}

/// A B-link leaf loaded for one coordination round (ADR-031): its per-key
/// `entries`, the backing object's observation, and the private [`LeafKind`]
/// context needed to write the entries back into the right object kind. The leaf is the CAS unit for its keys; it
/// lives either in the collection root `_i` (a small collection's single leaf)
/// or in a standalone node `_n`.
pub struct LoadedLeaf {
    pub entries: Shard,
    /// Node-level coordination staged independently from topology.
    pub locks: NodeLocks,
    pub observation: LeafObservation,
    kind: LeafKind,
}

/// The exact root or node state from which a loaded leaf was decoded.
#[derive(Clone, Debug)]
pub enum LeafObservation {
    Root(Observation<CollectionRoot>),
    Node(Observation<Node>),
}

/// The outcome of checking whether a retained leaf observation is still current.
pub enum LeafObservationCheck {
    /// The retained leaf observation remains current after the required bound.
    Current,
    /// The backing root or node changed; here is its current observation.
    Changed(LeafObservation),
}

impl LeafObservation {
    /// Returns the decoded node when the backing object exists.
    pub fn node(&self) -> Option<&Node> {
        match self {
            LeafObservation::Root(observed) => observed.value().map(|root| root.node()),
            LeafObservation::Node(observed) => observed.value().map(Arc::as_ref),
        }
    }

    /// Reports whether the backing object was absent.
    pub fn is_absent(&self) -> bool {
        match self {
            LeafObservation::Root(observed) => observed.is_absent(),
            LeafObservation::Node(observed) => observed.is_absent(),
        }
    }

    /// Returns the backing object's portable revision token when present.
    pub fn revision(&self) -> Option<&crate::cached_store::Revision> {
        match self {
            LeafObservation::Root(observed) => observed.revision(),
            LeafObservation::Node(observed) => observed.revision(),
        }
    }

    /// Reports whether all physical state for this leaf came from cache.
    pub fn cache_hit(&self) -> bool {
        match self {
            LeafObservation::Root(observed) => observed.cache_hit(),
            LeafObservation::Node(observed) => observed.cache_hit(),
        }
    }

    /// Reports whether two observations describe the same exact leaf state.
    pub fn same_state(&self, other: &Self) -> bool {
        match (self, other) {
            (LeafObservation::Root(left), LeafObservation::Root(right)) => left.same_state(right),
            (LeafObservation::Node(left), LeafObservation::Node(right)) => left.same_state(right),
            _ => false,
        }
    }

    /// The watermark after which this exact leaf state was known to be current.
    pub fn current_after(&self) -> SequencePoint {
        match self {
            LeafObservation::Root(observed) => observed.current_after(),
            LeafObservation::Node(observed) => observed.current_after(),
        }
    }
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
    /// [`CollectionRoot`] is preserved (child directory and node metadata) with
    /// only its node replaced.
    Root(CollectionRoot),
    /// The leaf is a standalone node `_n`; its bounds are preserved.
    Node(Node),
}

impl Codec for CollectionRoot {
    type Value = CollectionRoot;

    fn decode(_path: &str, body: &[u8]) -> Result<Self::Value, StorageError> {
        CollectionRoot::decode(body)
    }

    fn encode(root: &Self::Value) -> Result<Vec<u8>, StorageError> {
        Ok(root.encode())
    }

    fn size(root: &Self::Value) -> usize {
        root.encode().len()
    }

    fn valid_path(path: &str) -> bool {
        paths::is_collection_info(path)
    }

    fn name() -> &'static str {
        "collection root"
    }
}

impl Codec for Node {
    type Value = Node;

    fn decode(_path: &str, body: &[u8]) -> Result<Self::Value, StorageError> {
        Node::decode(body)
    }

    fn encode(node: &Self::Value) -> Result<Vec<u8>, StorageError> {
        Ok(node.encode())
    }

    fn size(node: &Self::Value) -> usize {
        node.encode().len()
    }

    fn valid_path(path: &str) -> bool {
        paths::parse(path).is_ok_and(|parsed| parsed.typ == paths::Type::Node)
    }

    fn name() -> &'static str {
        "node"
    }
}

impl Codec for StructuralLog {
    type Value = StructuralLog;

    fn decode(_path: &str, body: &[u8]) -> Result<Self::Value, StorageError> {
        StructuralLog::decode(body)
    }

    fn encode(record: &Self::Value) -> Result<Vec<u8>, StorageError> {
        Ok(record.encode())
    }

    fn size(record: &Self::Value) -> usize {
        record.encode().len()
    }

    fn valid_path(path: &str) -> bool {
        paths::structural_log_id_of(path).is_ok()
    }

    fn name() -> &'static str {
        "structural log"
    }
}

impl ShardStore {
    /// Creates a shard store that reads and compare-and-swaps through `objects`.
    pub fn new(objects: CachedStore) -> Self {
        ShardStore {
            roots: objects.typed(),
            nodes: objects.typed(),
            structural_logs: objects.typed(),
        }
    }

    /// Checks whether a retained leaf observation is still current after `bound`.
    pub async fn check_leaf_current(
        &self,
        observed: &LeafObservation,
        bound: SequencePoint,
    ) -> Result<LeafObservationCheck, StorageError> {
        match observed {
            LeafObservation::Root(root) => match self.roots.check_current(root, bound).await? {
                ObservationCheck::Current => Ok(LeafObservationCheck::Current),
                ObservationCheck::Changed(changed) => Ok(LeafObservationCheck::Changed(
                    LeafObservation::Root(changed),
                )),
            },
            LeafObservation::Node(node) => match self.nodes.check_current(node, bound).await? {
                ObservationCheck::Current => Ok(LeafObservationCheck::Current),
                ObservationCheck::Changed(changed) => Ok(LeafObservationCheck::Changed(
                    LeafObservation::Node(changed),
                )),
            },
        }
    }

    /// Loads the B-link root node from the collection root `_i` under `prefix`,
    /// or `None` if the collection does not exist yet (ADR-031). The root object
    /// carries both the node and the collection metadata; this returns just the
    /// node (a leaf while small, an index once grown) and its exact observation.
    pub async fn load_root_node(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<Option<(Node, LeafObservation)>, StorageError> {
        let observation = self.load_root_state(prefix, requirement).await?;
        let node = observation.node().cloned();
        Ok(node.map(|node| (node, observation)))
    }

    /// Loads the root object's exact observation, including observed absence.
    pub async fn load_root_state(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        let observed = self
            .roots
            .read(&paths::collection_info(prefix), requirement)
            .await?;
        Ok(LeafObservation::Root(observed))
    }

    /// Loads the non-root node's exact observation.
    pub async fn load_node_state(
        &self,
        prefix: &str,
        token: &str,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        let observed = self
            .nodes
            .read(&paths::from_node(prefix, token), requirement)
            .await?;
        if observed.is_absent() {
            return Err(StorageError::NotFound);
        }
        Ok(LeafObservation::Node(observed))
    }

    /// Loads the non-root node named `token` (`{prefix}/_n/<token>`, ADR-031). A
    /// [`StorageError::NotFound`] means the node is missing — a dangling child or
    /// right-sibling reference, which a descent surfaces rather than silently
    /// skips.
    pub async fn load_node(
        &self,
        prefix: &str,
        token: &str,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        let observation = self.load_node_state(prefix, token, requirement).await?;
        let node = observation
            .node()
            .expect("load_node_state rejects absence")
            .clone();
        Ok((node, observation))
    }

    /// Compare-and-swaps the non-root node named `token`. `expected = None` means
    /// create-if-absent (a freshly split-out sibling). Returns `false` on a
    /// precondition miss, `true` on success.
    pub async fn store_node(
        &self,
        prefix: &str,
        token: &str,
        node: &Node,
        expected: Option<&LeafObservation>,
    ) -> Result<bool, StorageError> {
        let path = paths::from_node(prefix, token);
        let res = match expected {
            Some(LeafObservation::Node(observed)) => {
                self.nodes
                    .compare_and_swap(observed, Arc::new(node.clone()))
                    .await
            }
            Some(LeafObservation::Root(_)) => {
                return Err(StorageError::other(
                    "node write received a root observation",
                ));
            }
            None => self.nodes.create(&path, None, Arc::new(node.clone())).await,
        };
        match res {
            Ok(CasResult::Committed(_)) => Ok(true),
            Ok(CasResult::Conflict) | Err(StorageError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Deletes the exact observed standalone node, converging if it is missing.
    pub async fn delete_node(&self, expected: &Observation<Node>) -> Result<(), StorageError> {
        self.nodes.delete(expected).await?;
        Ok(())
    }

    /// Creates a split write-ahead record and returns its exact observation.
    pub async fn write_structural_log(
        &self,
        record_id: &str,
        record: &StructuralLog,
    ) -> Result<Observation<StructuralLog>, StorageError> {
        let path = paths::structural_log_record(paths::db_root_of(&record.prefix), record_id);
        match self
            .structural_logs
            .create(&path, None, Arc::new(record.clone()))
            .await
        {
            Ok(CasResult::Committed(observed)) => Ok(observed),
            Ok(CasResult::Conflict) => Err(StorageError::Precondition),
            Err(e) => Err(e),
        }
    }

    /// Lists exact observations of every unresolved structural record.
    pub async fn list_structural_logs(
        &self,
        db_root: &str,
        requirement: Requirement,
    ) -> Result<Vec<(String, Observation<StructuralLog>)>, StorageError> {
        let prefix = paths::structural_log_dir(db_root);
        let limit = backend::ListLimit::new(STRUCTURAL_LIST_PAGE_SIZE).unwrap();
        let mut cursor = None;
        let mut records = Vec::new();
        loop {
            let page = self
                .structural_logs
                .list(&prefix, cursor.as_ref(), limit)
                .await?;
            for path in page.objects {
                let record_id = paths::structural_log_id_of(&path)
                    .map_err(|e| StorageError::with_source("parsing structural-log path", e))?;
                let observed = self.structural_logs.read(&path, requirement).await?;
                if observed.exists() {
                    records.push((record_id, observed));
                }
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(records),
            }
        }
    }

    /// Deletes the exact observed structural record, converging if it is missing.
    pub async fn delete_structural_log(
        &self,
        expected: &Observation<StructuralLog>,
    ) -> Result<(), StorageError> {
        self.structural_logs.delete(expected).await?;
        Ok(())
    }

    /// Loads the leaf that lives at object `path` (ADR-031): the root leaf when
    /// `path` is a collection root `_i`, else a standalone node `_n`. Returns the
    /// [`StorageError::NotFound`] when the object is missing. Returns
    /// [`StorageError::Precondition`] if a concurrent split turned the routed
    /// path into an index, so the caller can re-descend.
    pub async fn load_leaf(
        &self,
        path: &str,
        requirement: Requirement,
    ) -> Result<LoadedLeaf, StorageError> {
        if paths::is_collection_info(path) {
            let observed = self.roots.read(path, requirement).await?;
            match observed.value() {
                Some(root) => {
                    let root = root.as_ref().clone();
                    let entries = root
                        .node()
                        .as_leaf()
                        .cloned()
                        .ok_or(StorageError::Precondition)?;
                    Ok(LoadedLeaf {
                        entries,
                        locks: root.node().locks().clone(),
                        observation: LeafObservation::Root(observed),
                        kind: LeafKind::Root(root),
                    })
                }
                None => Err(StorageError::NotFound),
            }
        } else {
            let observed = self.nodes.read(path, requirement).await?;
            match observed.value() {
                Some(node) => {
                    let node = node.as_ref().clone();
                    let entries = node.as_leaf().cloned().ok_or(StorageError::Precondition)?;
                    Ok(LoadedLeaf {
                        entries,
                        locks: node.locks().clone(),
                        observation: LeafObservation::Node(observed),
                        kind: LeafKind::Node(node),
                    })
                }
                None => Err(StorageError::NotFound),
            }
        }
    }

    /// Compare-and-swaps the leaf at object `path`, writing `entries` and node
    /// locks back while preserving root metadata and node topology.
    pub async fn store_leaf(
        &self,
        path: &str,
        entries: &Shard,
        locks: &NodeLocks,
        kind: &LeafKind,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        let res = match (kind, expected) {
            (LeafKind::Root(root), LeafObservation::Root(observed)) => {
                let mut root = root.clone();
                let mut node = root.node().clone();
                node.set_leaf(entries.clone())?;
                node.set_locks(locks.clone());
                root.set_node(node);
                if observed.is_absent() {
                    return Err(StorageError::NotFound);
                }
                self.roots
                    .compare_and_swap(observed, Arc::new(root))
                    .await
                    .map(|result| result.committed())
            }
            (LeafKind::Node(node), LeafObservation::Node(observed)) => {
                let mut node = node.clone();
                node.set_leaf(entries.clone())?;
                node.set_locks(locks.clone());
                if observed.is_absent() {
                    return Err(StorageError::NotFound);
                }
                self.nodes
                    .compare_and_swap(observed, Arc::new(node))
                    .await
                    .map(|result| result.committed())
            }
            _ => {
                return Err(StorageError::other(format!(
                    "leaf kind does not match observation for {path:?}"
                )));
            }
        };
        match res {
            Ok(committed) => Ok(committed),
            Err(StorageError::NotFound) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Loads the collection root under `prefix`, or [`StorageError::NotFound`] if
    /// the collection does not exist.
    pub async fn load_root(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<(CollectionRoot, LeafObservation), StorageError> {
        let observed = self
            .roots
            .read(&paths::collection_info(prefix), requirement)
            .await?;
        let root = observed
            .value()
            .map(|root| root.as_ref().clone())
            .ok_or(StorageError::NotFound)?;
        Ok((root, LeafObservation::Root(observed)))
    }

    /// Compare-and-swaps the collection root. Returns `false` on a precondition
    /// miss, `true` on success.
    pub async fn store_root(
        &self,
        _prefix: &str,
        root: &CollectionRoot,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        let LeafObservation::Root(expected) = expected else {
            return Err(StorageError::other(
                "root write received a node observation",
            ));
        };
        let res = self
            .roots
            .compare_and_swap(expected, Arc::new(root.clone()))
            .await;
        match res {
            Ok(CasResult::Committed(_)) => Ok(true),
            Ok(CasResult::Conflict) | Err(StorageError::NotFound) => Ok(false),
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
            .roots
            .create(
                &paths::collection_info(prefix),
                None,
                Arc::new(root.clone()),
            )
            .await
        {
            Ok(CasResult::Committed(_)) => Ok(true),
            Ok(CasResult::Conflict) => Ok(false),
            Err(e) => Err(e),
        }
    }

    /// Creates a collection root if absent and returns its exact installed
    /// observation. `None` means another object already occupies the path.
    pub async fn create_root_observed(
        &self,
        prefix: &str,
        root: &CollectionRoot,
    ) -> Result<Option<Observation<CollectionRoot>>, StorageError> {
        match self
            .roots
            .create(
                &paths::collection_info(prefix),
                None,
                Arc::new(root.clone()),
            )
            .await?
        {
            CasResult::Committed(observed) => Ok(Some(observed)),
            CasResult::Conflict => Ok(None),
        }
    }

    /// Deletes the exact observed collection root, converging if it is missing.
    pub async fn delete_root(
        &self,
        expected: &Observation<CollectionRoot>,
    ) -> Result<(), StorageError> {
        self.roots.delete(expected).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Timeline;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};

    const COLL: &str = "coll";

    struct TestStore {
        shards: ShardStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = ShardStore;

        fn deref(&self) -> &Self::Target {
            &self.shards
        }
    }

    fn store_over(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let shards = ShardStore::new(objects);
        TestStore { shards, timeline }
    }

    fn count(log: &OpLog, op: &str) -> usize {
        log.lock().unwrap().iter().filter(|r| r.op == op).count()
    }

    // Seeds an empty node leaf at `path` through a separate store so a later
    // reader starts with a cold cache. Creates it with a bare `write_if_not_exists`
    // (no seeding read) so the shared op log reflects only the reader's traffic.
    async fn seed_empty_leaf(backend: &Arc<dyn Backend>, path: &str) {
        let store = store_over(backend.clone());
        let token = paths::node_token_of(path).unwrap();
        assert!(
            store
                .store_node(COLL, &token, &Node::leaf(Shard::new()), None)
                .await
                .unwrap()
        );
    }

    // A node object exists in the backend. A cold cache full-fetches it on the
    // first load; a subsequent hot load checks with `read_if_modified`
    // instead of re-fetching, and returns the same version (ADR-023).
    #[tokio::test]
    async fn hot_reload_checks_current_without_full_read() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let path = paths::from_node(COLL, "tok");
        seed_empty_leaf(&backend, &path).await;

        let reader = store_over(backend.clone());
        let v1 = reader
            .load_leaf(&path, Requirement::AtLeast(reader.timeline.now()))
            .await
            .unwrap()
            .observation;
        assert_eq!(count(&log, "read"), 1, "cold load full-reads");
        assert_eq!(count(&log, "read_if_modified"), 0);

        let v2 = reader
            .load_leaf(&path, Requirement::AtLeast(reader.timeline.now()))
            .await
            .unwrap()
            .observation;
        assert_eq!(count(&log, "read"), 1, "hot load must not full-read");
        assert_eq!(
            count(&log, "read_if_modified"),
            1,
            "hot load checks conditionally"
        );
        assert_eq!(
            v1.revision(),
            v2.revision(),
            "unchanged node keeps its revision"
        );
    }

    // A cached node loaded with `Any` is served without any backend op
    // (neither a full read nor a currentness check), while an uncached node still
    // does one full read (ADR-030).
    #[tokio::test]
    async fn any_serves_cached_without_backend_op() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let path = paths::from_node(COLL, "tok");
        seed_empty_leaf(&backend, &path).await;

        let reader = store_over(backend.clone());
        // Warm the cache with one cold full read.
        reader
            .load_leaf(&path, Requirement::AtLeast(reader.timeline.now()))
            .await
            .unwrap();
        assert_eq!(count(&log, "read"), 1);

        // A cached `Any` load touches the backend for nothing.
        reader.load_leaf(&path, Requirement::Any).await.unwrap();
        assert_eq!(count(&log, "read"), 1, "cached Any must not read");
        assert_eq!(
            count(&log, "read_if_modified"),
            0,
            "cached Any must not check the backend"
        );

        // An uncached node has nothing to serve, so it falls through to a read.
        let other = paths::from_node(COLL, "tok2");
        assert!(matches!(
            reader.load_leaf(&other, Requirement::Any).await,
            Err(StorageError::NotFound)
        ));
        assert_eq!(count(&log, "read"), 2, "uncached Any falls through");
    }

    // A write-through store updates the cache, so a load after a CAS observes the
    // freshly written content and its new version.
    #[tokio::test]
    async fn store_is_visible_to_next_load() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let path = paths::from_node(COLL, "tok");
        assert!(
            store
                .store_node(COLL, "tok", &Node::leaf(Shard::new()), None)
                .await
                .unwrap()
        );

        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        assert!(
            store
                .store_leaf(
                    &path,
                    &Shard::new(),
                    &loaded.locks,
                    loaded.kind(),
                    &loaded.observation,
                )
                .await
                .unwrap()
        );
        let loaded = store
            .load_leaf(&path, Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        let v1 = loaded.observation.clone();

        // CAS a new generation over the loaded version, then confirm the next
        // load reflects it.
        assert!(
            store
                .store_leaf(&path, &Shard::new(), &loaded.locks, loaded.kind(), &v1,)
                .await
                .unwrap()
        );
        let v2 = store
            .load_leaf(&path, Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap()
            .observation;
        assert_ne!(
            v1.revision(),
            v2.revision(),
            "a CAS store advances the revision"
        );
    }

    #[tokio::test]
    async fn structural_log_listing_drains_backend_pages() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        for i in 0..=STRUCTURAL_LIST_PAGE_SIZE {
            store
                .write_structural_log(
                    &format!("record-{i:03}"),
                    &StructuralLog {
                        prefix: "db/coll".to_string(),
                        source_token: "source".to_string(),
                        source_version: "v1".to_string(),
                        created_tokens: vec![format!("node-{i:03}")],
                        split_key: vec![i as u8],
                        is_root: false,
                    },
                )
                .await
                .unwrap();
        }

        let records = store
            .list_structural_logs("db", Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        assert_eq!(records.len(), STRUCTURAL_LIST_PAGE_SIZE + 1);
    }
}
