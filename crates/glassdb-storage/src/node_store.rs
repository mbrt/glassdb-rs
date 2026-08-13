//! Typed persistence for B-link tree nodes.

use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_data::{CollectionAddress, NodeToken, ObjectPath};

use crate::cached_store::{
    CachedStore, CasResult, Codec, Observation, ObservationCheck, Requirement,
};
use crate::error::StorageError;
use crate::node::{Node, NodeLocks};
use crate::shard::Shard;
use crate::timeline::SequencePoint;

const NODE_LIST_PAGE_SIZE: usize = 128;

/// Reads and compare-and-swaps B-link nodes.
#[derive(Clone)]
pub struct NodeStore {
    nodes: crate::cached_store::TypedCachedStore<Node>,
}

/// A B-link leaf loaded for one coordination round.
pub struct LoadedLeaf {
    edit: LeafEdit,
}

/// A leaf mutation bound to the exact node state it will compare-and-swap.
pub struct LeafEdit {
    observation: LeafObservation,
    node: Node,
}

/// The exact node state from which a leaf was decoded.
pub type LeafObservation = Observation<Node>;

/// The outcome of checking a retained leaf observation.
pub type LeafObservationCheck = ObservationCheck<Node>;

impl LoadedLeaf {
    /// Returns the object path of this loaded leaf.
    pub fn path(&self) -> &ObjectPath {
        self.edit.path()
    }

    /// Returns the complete node carrying this leaf's entries and coordination.
    pub fn node(&self) -> &Node {
        self.edit.node()
    }

    /// Returns the loaded leaf entries.
    pub fn entries(&self) -> &Shard {
        self.edit.entries()
    }

    /// Returns the loaded node-level coordination state.
    pub fn locks(&self) -> &NodeLocks {
        self.edit.locks()
    }

    /// Returns the exact observation from which this leaf was loaded.
    pub fn observation(&self) -> &LeafObservation {
        self.edit.observation()
    }

    /// Reports whether this loaded leaf still owns `key` — i.e. `key` is below
    /// its high-key. A `false` result means a split moved `key` to a right
    /// sibling after the key was routed here, so a caller must re-descend.
    pub fn owns(&self, key: &[u8]) -> bool {
        self.edit.owns(key)
    }

    /// Converts this loaded leaf into an observation-bound mutation.
    pub fn into_edit(self) -> LeafEdit {
        self.edit
    }
}

impl LeafEdit {
    /// Returns the immutable object path to which this edit is bound.
    pub fn path(&self) -> &ObjectPath {
        self.observation.path()
    }

    /// Returns the complete node carrying this edit's topology and contents.
    pub fn node(&self) -> &Node {
        &self.node
    }

    /// Returns the staged leaf entries.
    pub fn entries(&self) -> &Shard {
        self.node
            .as_leaf()
            .expect("LeafEdit is always created from a leaf node")
    }

    /// Replaces the staged leaf entries without changing topology.
    pub fn set_entries(&mut self, entries: Shard) {
        self.node
            .set_leaf(entries)
            .expect("LeafEdit is always created from a leaf node");
    }

    /// Returns the staged node-level coordination state.
    pub fn locks(&self) -> &NodeLocks {
        self.node.locks()
    }

    /// Replaces the staged node-level coordination state without changing topology.
    pub fn set_locks(&mut self, locks: NodeLocks) {
        self.node.set_locks(locks);
    }

    /// Returns the exact observation against which this edit will commit.
    pub fn observation(&self) -> &LeafObservation {
        &self.observation
    }

    /// Reports whether this edited leaf still owns `key`.
    pub fn owns(&self, key: &[u8]) -> bool {
        self.node.owns(key)
    }
}

impl Codec for Node {
    type Value = Node;

    fn decode(_path: &ObjectPath, body: &[u8]) -> Result<Self::Value, StorageError> {
        Node::decode(body)
    }

    fn encode(_path: &ObjectPath, node: &Self::Value) -> Result<Vec<u8>, StorageError> {
        Ok(node.encode())
    }

    fn size(node: &Self::Value) -> usize {
        node.encoded_len()
    }

    fn accepts(path: &ObjectPath) -> bool {
        matches!(path, ObjectPath::TreeRoot { .. } | ObjectPath::Node { .. })
    }

    fn name() -> &'static str {
        "node"
    }
}

impl NodeStore {
    /// Creates a node store that reads and compare-and-swaps through `objects`.
    pub fn new(objects: CachedStore) -> Self {
        Self {
            nodes: objects.typed(),
        }
    }

    /// Checks whether a retained leaf observation is still current after `bound`.
    pub async fn check_leaf_current(
        &self,
        observed: &LeafObservation,
        bound: SequencePoint,
    ) -> Result<LeafObservationCheck, StorageError> {
        self.nodes.check_current(observed, bound).await
    }

    /// Loads the fixed B-link tree root under `prefix`.
    pub async fn load_root_node(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Option<(Node, LeafObservation)>, StorageError> {
        let observation = self.load_root_state(collection, requirement).await?;
        let node = observation.value().map(|node| node.as_ref().clone());
        Ok(node.map(|node| (node, observation)))
    }

    /// Loads the fixed tree root's exact observation, including absence.
    pub async fn load_root_state(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.load_node_at_state(
            &ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            requirement,
        )
        .await
    }

    /// Loads an exact `_r` or `_n/<token>` node observation, including absence.
    pub async fn load_node_at_state(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        validate_node_path(path)?;
        self.nodes.read(path, requirement).await
    }

    /// Loads an existing node at an exact `_r` or `_n/<token>` path.
    pub async fn load_node_at(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        let observed = self.load_node_at_state(path, requirement).await?;
        let node = observed
            .value()
            .map(|node| node.as_ref().clone())
            .ok_or(StorageError::NotFound)?;
        Ok((node, observed))
    }

    /// Loads the non-root node's exact observation.
    pub async fn load_node_state(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        let observed = self
            .nodes
            .read(
                ObjectPath::Node {
                    collection: collection.clone(),
                    token: token.clone(),
                },
                requirement,
            )
            .await?;
        if observed.is_absent() {
            return Err(StorageError::NotFound);
        }
        Ok(observed)
    }

    /// Loads the non-root node named `token` (`{prefix}/_n/<token>`, ADR-031). A
    /// [`StorageError::NotFound`] means the node is missing — a dangling child or
    /// right-sibling reference, which a descent surfaces rather than silently
    /// skips.
    pub async fn load_node(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        let observation = self.load_node_state(collection, token, requirement).await?;
        let node = observation
            .value()
            .expect("load_node_state rejects absence")
            .as_ref()
            .clone();
        Ok((node, observation))
    }

    /// Compare-and-swaps the non-root node named `token`. `expected = None` means
    /// create-if-absent (a freshly split-out sibling). Returns `false` on a
    /// precondition miss, `true` on success.
    pub async fn store_node(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        node: &Node,
        expected: Option<&LeafObservation>,
    ) -> Result<bool, StorageError> {
        let path = ObjectPath::Node {
            collection: collection.clone(),
            token: token.clone(),
        };
        let res = match expected {
            Some(observed) if observed.path() == &path => {
                self.nodes
                    .compare_and_swap(observed, Arc::new(node.clone()))
                    .await
            }
            Some(_) => return Err(StorageError::other("node observation path changed")),
            None => self.nodes.create(path, None, Arc::new(node.clone())).await,
        };
        match res {
            Ok(CasResult::Committed(_)) => Ok(true),
            Ok(CasResult::Conflict) | Err(StorageError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Compare-and-swaps an exact `_r` or `_n/<token>` node path.
    pub async fn store_node_at(
        &self,
        path: &ObjectPath,
        node: &Node,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        validate_node_path(path)?;
        if expected.path() != path {
            return Err(StorageError::other("node observation path changed"));
        }
        match self
            .nodes
            .compare_and_swap(expected, Arc::new(node.clone()))
            .await
        {
            Ok(CasResult::Committed(_)) => Ok(true),
            Ok(CasResult::Conflict) | Err(StorageError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Deletes the exact observed standalone node, converging if it is missing.
    pub async fn delete_node(&self, expected: &Observation<Node>) -> Result<(), StorageError> {
        self.nodes.delete(expected).await?;
        Ok(())
    }

    /// Lists every standalone node under one incarnation-unique collection
    /// prefix, including temporarily unreachable structural nodes.
    pub async fn list_nodes(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Vec<(NodeToken, Observation<Node>)>, StorageError> {
        let list_prefix = ObjectPath::nodes_prefix(collection);
        let limit = backend::ListLimit::new(NODE_LIST_PAGE_SIZE).unwrap();
        let mut cursor = None;
        let mut nodes = Vec::new();
        loop {
            let page = self
                .nodes
                .list(&list_prefix, cursor.as_ref(), limit)
                .await?;
            for path in page.objects {
                let ObjectPath::Node {
                    collection: listed_collection,
                    token,
                } = path.object_path()
                else {
                    return Err(StorageError::other("node listing returned a non-node path"));
                };
                if listed_collection != collection {
                    return Err(StorageError::other(
                        "node listing returned a different collection",
                    ));
                }
                let token = token.clone();
                let observed = self.nodes.read(path, requirement).await?;
                if observed.exists() {
                    nodes.push((token, observed));
                }
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(nodes),
            }
        }
    }

    /// Loads the leaf node at `_r` or `_n/<token>`.
    pub async fn load_leaf(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<LoadedLeaf, StorageError> {
        validate_node_path(path)?;
        let observed = self.nodes.read(path, requirement).await?;
        match observed.value() {
            Some(node) => {
                let node = node.as_ref().clone();
                node.as_leaf().ok_or(StorageError::Precondition)?;
                Ok(LoadedLeaf {
                    edit: LeafEdit {
                        observation: observed,
                        node,
                    },
                })
            }
            None => Err(StorageError::NotFound),
        }
    }

    /// Compare-and-swaps an observation-bound leaf edit.
    pub async fn commit_leaf(&self, edit: LeafEdit) -> Result<bool, StorageError> {
        let LeafEdit { observation, node } = edit;
        let result = self
            .nodes
            .compare_and_swap(&observation, Arc::new(node))
            .await
            .map(|result| result.committed());
        match result {
            Ok(committed) => Ok(committed),
            Err(StorageError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Loads the fixed tree root under `prefix`.
    pub async fn load_root(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.load_node_at(
            &ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            requirement,
        )
        .await
    }

    /// Compare-and-swaps the fixed tree root.
    pub async fn store_root(
        &self,
        collection: &CollectionAddress,
        root: &Node,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        self.store_node_at(
            &ObjectPath::TreeRoot {
                collection: collection.clone(),
            },
            root,
            expected,
        )
        .await
    }

    /// Creates the fixed tree root if absent.
    pub async fn create_root(
        &self,
        collection: &CollectionAddress,
        root: &Node,
    ) -> Result<bool, StorageError> {
        let path = ObjectPath::TreeRoot {
            collection: collection.clone(),
        };
        match self.nodes.create(path, None, Arc::new(root.clone())).await {
            Ok(CasResult::Committed(_)) => Ok(true),
            Ok(CasResult::Conflict) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Creates a fixed tree root if absent and returns its exact installed
    /// observation. `None` means another object already occupies the path.
    pub async fn create_root_observed(
        &self,
        collection: &CollectionAddress,
        root: &Node,
    ) -> Result<Option<Observation<Node>>, StorageError> {
        let path = ObjectPath::TreeRoot {
            collection: collection.clone(),
        };
        match self
            .nodes
            .create(path, None, Arc::new(root.clone()))
            .await?
        {
            CasResult::Committed(observed) => Ok(Some(observed)),
            CasResult::Conflict => Ok(None),
        }
    }

    /// Deletes the exact observed fixed tree root.
    pub async fn delete_root(&self, expected: &Observation<Node>) -> Result<(), StorageError> {
        self.delete_node(expected).await
    }
}

fn validate_node_path(path: &ObjectPath) -> Result<(), StorageError> {
    match path {
        ObjectPath::TreeRoot { .. } | ObjectPath::Node { .. } => Ok(()),
        _ => Err(StorageError::other("expected a tree-node object path")),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ShardEntry, Timeline};

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_data::TxId;

    struct TestStore {
        nodes: NodeStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = NodeStore;

        fn deref(&self) -> &Self::Target {
            &self.nodes
        }
    }

    fn store_over(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let nodes = NodeStore::new(objects);
        TestStore { nodes, timeline }
    }

    fn count(log: &OpLog, op: &str) -> usize {
        log.lock().unwrap().iter().filter(|r| r.op == op).count()
    }

    fn collection() -> CollectionAddress {
        CollectionAddress::root("coll")
    }

    fn token(byte: u8) -> NodeToken {
        NodeToken::from_bytes([byte; 16])
    }

    fn node_path(byte: u8) -> ObjectPath {
        ObjectPath::Node {
            collection: collection(),
            token: token(byte),
        }
    }

    // Use a separate store so the reader begins cold. Creating directly avoids
    // a seeding read, keeping the operation log limited to reader traffic.
    async fn seed_empty_leaf(backend: &Arc<dyn Backend>, token: &NodeToken) {
        let store = store_over(backend.clone());
        assert!(
            store
                .store_node(&collection(), token, &Node::leaf(Shard::new()), None)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn hot_reload_checks_current_without_full_read() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let path = node_path(1);
        seed_empty_leaf(&backend, &token(1)).await;

        let reader = store_over(backend);
        let first = reader
            .load_leaf(&path, Requirement::AtLeast(reader.timeline.now()))
            .await
            .unwrap()
            .observation()
            .clone();
        assert_eq!(count(&log, "read"), 1, "cold load full-reads");
        assert_eq!(count(&log, "read_if_modified"), 0);

        let second = reader
            .load_leaf(&path, Requirement::AtLeast(reader.timeline.now()))
            .await
            .unwrap()
            .observation()
            .clone();
        assert_eq!(count(&log, "read"), 1, "hot load must not full-read");
        assert_eq!(
            count(&log, "read_if_modified"),
            1,
            "hot load checks conditionally"
        );
        assert_eq!(first.revision(), second.revision());
    }

    #[tokio::test]
    async fn any_serves_cached_without_backend_op() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let backend: Arc<dyn Backend> = Arc::new(recorder);
        let path = node_path(1);
        seed_empty_leaf(&backend, &token(1)).await;

        let reader = store_over(backend);
        reader
            .load_leaf(&path, Requirement::AtLeast(reader.timeline.now()))
            .await
            .unwrap();
        assert_eq!(count(&log, "read"), 1);

        reader.load_leaf(&path, Requirement::Any).await.unwrap();
        assert_eq!(count(&log, "read"), 1, "cached Any must not read");
        assert_eq!(
            count(&log, "read_if_modified"),
            0,
            "cached Any must not check the backend"
        );

        assert!(matches!(
            reader.load_leaf(&node_path(2), Requirement::Any).await,
            Err(StorageError::NotFound)
        ));
        assert_eq!(count(&log, "read"), 2, "uncached Any falls through");
    }

    #[tokio::test]
    async fn committed_edit_is_visible_to_the_next_cached_load() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let path = node_path(1);
        assert!(
            store
                .store_node(&collection(), &token(1), &Node::leaf(Shard::new()), None,)
                .await
                .unwrap()
        );

        let loaded = store.load_leaf(&path, Requirement::Any).await.unwrap();
        let previous_revision = loaded.observation().revision().cloned();
        let mut edit = loaded.into_edit();
        edit.set_entries(Shard::from_entries([ShardEntry::new(b"new".as_slice())]));
        assert!(store.commit_leaf(edit).await.unwrap());

        let committed = store.load_leaf(&path, Requirement::Any).await.unwrap();
        assert!(committed.entries().lookup(b"new").is_some());
        assert_ne!(
            committed.observation().revision(),
            previous_revision.as_ref()
        );
    }

    #[tokio::test]
    async fn leaf_edit_commits_bounded_changes_without_changing_topology() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let path = node_path(1);
        let original = Node::leaf(Shard::new())
            .with_high_key(Some(b"m".to_vec()))
            .with_right_sibling(Some("right".to_string()));
        assert!(
            store
                .store_node(&collection(), &token(1), &original, None)
                .await
                .unwrap()
        );

        let loaded = store.load_leaf(&path, Requirement::Any).await.unwrap();
        let entries = Shard::from_entries([ShardEntry::new(b"key".as_slice())]);
        let mut locks = NodeLocks::default();
        let holder = TxId::from_bytes(b"holder".to_vec());
        locks.set_membership_writer(holder.clone());

        let mut edit = loaded.into_edit();
        assert_eq!(edit.path(), &path);
        edit.set_entries(entries.clone());
        edit.set_locks(locks.clone());
        assert!(store.commit_leaf(edit).await.unwrap());

        let committed = store.load_leaf(&path, Requirement::Any).await.unwrap();
        assert_eq!(committed.entries(), &entries);
        assert_eq!(committed.locks(), &locks);
        assert_eq!(committed.node().high_key(), Some(b"m".as_slice()));
        assert_eq!(committed.node().right_sibling(), Some("right"));
        assert_eq!(committed.node().membership_lock().holders(), &[holder]);
    }

    #[tokio::test]
    async fn leaf_edit_is_bound_to_its_observed_path() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let left_path = node_path(1);
        let right_path = node_path(2);
        for token in [token(1), token(2)] {
            assert!(
                store
                    .store_node(&collection(), &token, &Node::leaf(Shard::new()), None)
                    .await
                    .unwrap()
            );
        }

        let mut edit = store
            .load_leaf(&left_path, Requirement::Any)
            .await
            .unwrap()
            .into_edit();
        edit.set_entries(Shard::from_entries([ShardEntry::new(
            b"left-key".as_slice(),
        )]));
        assert_eq!(edit.path(), &left_path);
        assert!(store.commit_leaf(edit).await.unwrap());

        let left = store.load_leaf(&left_path, Requirement::Any).await.unwrap();
        let right = store
            .load_leaf(&right_path, Requirement::Any)
            .await
            .unwrap();
        assert!(left.entries().lookup(b"left-key").is_some());
        assert!(right.entries().is_empty());
    }

    #[tokio::test]
    async fn stale_leaf_edit_conflicts() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let path = node_path(1);
        assert!(
            store
                .store_node(&collection(), &token(1), &Node::leaf(Shard::new()), None,)
                .await
                .unwrap()
        );

        let mut winner = store
            .load_leaf(&path, Requirement::Any)
            .await
            .unwrap()
            .into_edit();
        let mut stale = store
            .load_leaf(&path, Requirement::Any)
            .await
            .unwrap()
            .into_edit();
        winner.set_entries(Shard::from_entries([ShardEntry::new(b"winner".as_slice())]));
        stale.set_entries(Shard::from_entries([ShardEntry::new(b"stale".as_slice())]));

        assert!(store.commit_leaf(winner).await.unwrap());
        assert!(!store.commit_leaf(stale).await.unwrap());

        let committed = store.load_leaf(&path, Requirement::Any).await.unwrap();
        assert!(committed.entries().lookup(b"winner").is_some());
        assert!(committed.entries().lookup(b"stale").is_none());
    }

    #[tokio::test]
    async fn node_listing_drains_backend_pages() {
        let recorder = RecordingBackend::new(Arc::new(MemoryBackend::new()));
        let log = recorder.log();
        let store = store_over(Arc::new(recorder));
        let expected: Vec<_> = (0..=NODE_LIST_PAGE_SIZE)
            .map(|index| token(index as u8))
            .collect();
        for token in &expected {
            assert!(
                store
                    .store_node(&collection(), token, &Node::leaf(Shard::new()), None)
                    .await
                    .unwrap()
            );
        }

        let listed = store
            .list_nodes(&collection(), Requirement::Any)
            .await
            .unwrap();

        assert_eq!(count(&log, "list"), 2);
        assert_eq!(listed.len(), expected.len());
        for token in expected {
            assert!(listed.iter().any(|(listed, _)| listed == &token));
        }
    }
}
