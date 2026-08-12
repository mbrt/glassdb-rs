//! Compare-and-swap storage for the v2 coordination objects (ADR-031).
//!
//! B-link nodes (`{prefix}/_r` and `{prefix}/_n/<token>`) are the coordination
//! units. Each mutation is a create-if-absent, a
//! version-conditional compare-and-swap, or an exact-revision delete
//! (ADR-023/ADR-042).
//!
//! Reads and mutations go through the decoded [`CachedStore`].

use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_data::{CollectionAddress, DbRoot, NodeToken, ObjectPath, StructuralRecordId, TxId};

use crate::cached_store::{CachedStore, CasResult, Codec, Observation, Requirement};
use crate::error::StorageError;
use crate::node::Node;
use crate::node_store::{LeafEdit, LeafObservation, LeafObservationCheck, LoadedLeaf, NodeStore};
use crate::structlog::StructuralLog;
use crate::timeline::SequencePoint;

const STRUCTURAL_LIST_PAGE_SIZE: usize = 128;

/// Reads and compare-and-swaps B-link nodes.
#[derive(Clone)]
pub struct ShardStore {
    nodes: NodeStore,
    structural_logs: crate::cached_store::TypedCachedStore<StructuralLog>,
}

impl Codec for StructuralLog {
    type Value = StructuralLog;

    fn decode(path: &ObjectPath, body: &[u8]) -> Result<Self::Value, StorageError> {
        let record = StructuralLog::decode(body)?;
        let ObjectPath::StructuralRecord { participant, .. } = path else {
            return Err(StorageError::other(
                "structural log has a non-structural path",
            ));
        };
        if participant != &record.participant_id {
            return Err(StorageError::other(
                "structural-log path does not match its participant",
            ));
        }
        Ok(record)
    }

    fn encode(path: &ObjectPath, record: &Self::Value) -> Result<Vec<u8>, StorageError> {
        let ObjectPath::StructuralRecord { participant, .. } = path else {
            return Err(StorageError::other(
                "structural log has a non-structural path",
            ));
        };
        if participant != &record.participant_id {
            return Err(StorageError::other(
                "structural-log path does not match its participant",
            ));
        }
        Ok(record.encode())
    }

    fn size(record: &Self::Value) -> usize {
        record.encode().len()
    }

    fn accepts(path: &ObjectPath) -> bool {
        matches!(path, ObjectPath::StructuralRecord { .. })
    }

    fn name() -> &'static str {
        "structural log"
    }
}

impl ShardStore {
    /// Creates a shard store that reads and compare-and-swaps through `objects`.
    pub fn new(objects: CachedStore) -> Self {
        ShardStore {
            nodes: NodeStore::new(objects.clone()),
            structural_logs: objects.typed(),
        }
    }

    /// Returns the store responsible for B-link tree nodes.
    pub fn nodes(&self) -> &NodeStore {
        &self.nodes
    }

    /// Checks whether a retained leaf observation is still current after `bound`.
    pub async fn check_leaf_current(
        &self,
        observed: &LeafObservation,
        bound: SequencePoint,
    ) -> Result<LeafObservationCheck, StorageError> {
        self.nodes.check_leaf_current(observed, bound).await
    }

    /// Loads the fixed B-link tree root under `prefix`.
    pub async fn load_root_node(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Option<(Node, LeafObservation)>, StorageError> {
        self.nodes.load_root_node(collection, requirement).await
    }

    /// Loads the fixed tree root's exact observation, including absence.
    pub async fn load_root_state(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.nodes.load_root_state(collection, requirement).await
    }

    /// Loads an exact `_r` or `_n/<token>` node observation, including absence.
    pub async fn load_node_at_state(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.nodes.load_node_at_state(path, requirement).await
    }

    /// Loads an existing node at an exact `_r` or `_n/<token>` path.
    pub async fn load_node_at(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.nodes.load_node_at(path, requirement).await
    }

    /// Loads the non-root node's exact observation.
    pub async fn load_node_state(
        &self,
        collection: &CollectionAddress,
        token: &NodeToken,
        requirement: Requirement,
    ) -> Result<LeafObservation, StorageError> {
        self.nodes
            .load_node_state(collection, token, requirement)
            .await
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
        self.nodes.load_node(collection, token, requirement).await
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
        self.nodes
            .store_node(collection, token, node, expected)
            .await
    }

    /// Compare-and-swaps an exact `_r` or `_n/<token>` node path.
    pub async fn store_node_at(
        &self,
        path: &ObjectPath,
        node: &Node,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        self.nodes.store_node_at(path, node, expected).await
    }

    /// Deletes the exact observed standalone node, converging if it is missing.
    pub async fn delete_node(&self, expected: &Observation<Node>) -> Result<(), StorageError> {
        self.nodes.delete_node(expected).await
    }

    /// Lists every standalone node under one incarnation-unique collection
    /// prefix, including temporarily unreachable structural nodes.
    pub async fn list_nodes(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Vec<(NodeToken, Observation<Node>)>, StorageError> {
        self.nodes.list_nodes(collection, requirement).await
    }

    /// Creates a split write-ahead record and returns its exact observation.
    pub async fn write_structural_log(
        &self,
        db_root: &DbRoot,
        record_id: &StructuralRecordId,
        record: &StructuralLog,
    ) -> Result<Observation<StructuralLog>, StorageError> {
        let path = ObjectPath::StructuralRecord {
            db_root: db_root.clone(),
            participant: record.participant_id.clone(),
            record_id: record_id.clone(),
        };
        match self
            .structural_logs
            .create(path, None, Arc::new(record.clone()))
            .await
        {
            Ok(CasResult::Committed(observed)) => Ok(observed),
            Ok(CasResult::Conflict) => Err(StorageError::Precondition),
            Err(e) => Err(e),
        }
    }

    /// Conditionally advances an exact split intent.
    pub async fn update_structural_log(
        &self,
        expected: &Observation<StructuralLog>,
        record: &StructuralLog,
    ) -> Result<Option<Observation<StructuralLog>>, StorageError> {
        match self
            .structural_logs
            .compare_and_swap(expected, Arc::new(record.clone()))
            .await
        {
            Ok(CasResult::Committed(observed)) => Ok(Some(observed)),
            Ok(CasResult::Conflict) | Err(StorageError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Lists exact observations of every unresolved structural record.
    pub async fn list_structural_logs(
        &self,
        db_root: &DbRoot,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralRecordId, Observation<StructuralLog>)>, StorageError> {
        let prefix = ObjectPath::structural_records_prefix(db_root);
        self.list_structural_logs_under(&prefix, requirement).await
    }

    /// Lists only the unresolved structural records owned by `participant`.
    pub async fn list_structural_logs_for_participant(
        &self,
        db_root: &DbRoot,
        participant: &TxId,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralRecordId, Observation<StructuralLog>)>, StorageError> {
        let prefix = ObjectPath::participant_structural_records_prefix(db_root, participant);
        self.list_structural_logs_under(&prefix, requirement).await
    }

    /// Deletes the exact observed structural record, converging if it is missing.
    pub async fn delete_structural_log(
        &self,
        expected: &Observation<StructuralLog>,
    ) -> Result<(), StorageError> {
        self.structural_logs.delete(expected).await?;
        Ok(())
    }

    /// Loads the leaf node at `_r` or `_n/<token>`.
    pub async fn load_leaf(
        &self,
        path: &ObjectPath,
        requirement: Requirement,
    ) -> Result<LoadedLeaf, StorageError> {
        self.nodes.load_leaf(path, requirement).await
    }

    /// Compare-and-swaps an observation-bound leaf edit.
    pub async fn commit_leaf(&self, edit: LeafEdit) -> Result<bool, StorageError> {
        self.nodes.commit_leaf(edit).await
    }

    /// Loads the fixed tree root under `prefix`.
    pub async fn load_root(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<(Node, LeafObservation), StorageError> {
        self.nodes.load_root(collection, requirement).await
    }

    /// Compare-and-swaps the fixed tree root.
    pub async fn store_root(
        &self,
        collection: &CollectionAddress,
        root: &Node,
        expected: &LeafObservation,
    ) -> Result<bool, StorageError> {
        self.nodes.store_root(collection, root, expected).await
    }

    /// Creates the fixed tree root if absent.
    pub async fn create_root(
        &self,
        collection: &CollectionAddress,
        root: &Node,
    ) -> Result<bool, StorageError> {
        self.nodes.create_root(collection, root).await
    }

    /// Creates a fixed tree root if absent and returns its exact installed
    /// observation. `None` means another object already occupies the path.
    pub async fn create_root_observed(
        &self,
        collection: &CollectionAddress,
        root: &Node,
    ) -> Result<Option<Observation<Node>>, StorageError> {
        self.nodes.create_root_observed(collection, root).await
    }

    /// Deletes the exact observed fixed tree root.
    pub async fn delete_root(&self, expected: &Observation<Node>) -> Result<(), StorageError> {
        self.nodes.delete_root(expected).await
    }

    async fn list_structural_logs_under(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralRecordId, Observation<StructuralLog>)>, StorageError> {
        let limit = backend::ListLimit::new(STRUCTURAL_LIST_PAGE_SIZE).unwrap();
        let mut cursor = None;
        let mut records = Vec::new();
        loop {
            let page = self
                .structural_logs
                .list(prefix, cursor.as_ref(), limit)
                .await?;
            for path in page.objects {
                let ObjectPath::StructuralRecord { record_id, .. } = path.object_path() else {
                    return Err(StorageError::other(
                        "structural listing returned a non-structural path",
                    ));
                };
                let record_id = record_id.clone();
                let observed = self.structural_logs.read(path, requirement).await?;
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Timeline;
    use crate::structlog::StructuralLogPhase;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;

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

    fn token(byte: u8) -> NodeToken {
        NodeToken::from_bytes([byte; 16])
    }

    fn db_root() -> DbRoot {
        DbRoot::try_from("db").unwrap()
    }

    fn record_id(byte: u8) -> StructuralRecordId {
        StructuralRecordId::from(token(byte))
    }

    #[test]
    fn structural_codec_rejects_a_different_path_participant() {
        let path = ObjectPath::StructuralRecord {
            db_root: db_root(),
            participant: TxId::from_bytes(b"path-participant".to_vec()),
            record_id: record_id(1),
        };
        let record = StructuralLog {
            collection: CollectionAddress::root("db"),
            source_token: None,
            source_version: String::new(),
            created_tokens: Vec::new(),
            split_key: Vec::new(),
            participant_id: TxId::from_bytes(b"body-participant".to_vec()),
            phase: StructuralLogPhase::Preparing,
        };

        assert!(<StructuralLog as Codec>::encode(&path, &record).is_err());
    }

    #[tokio::test]
    async fn structural_log_listing_drains_backend_pages() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let participant = TxId::from_bytes(b"participant".to_vec());
        for i in 0..=STRUCTURAL_LIST_PAGE_SIZE {
            store
                .write_structural_log(
                    &db_root(),
                    &record_id(i as u8),
                    &StructuralLog {
                        collection: CollectionAddress::root("db"),
                        source_token: Some(token(200)),
                        source_version: "v1".to_string(),
                        created_tokens: vec![token(i as u8)],
                        split_key: vec![i as u8],
                        participant_id: participant.clone(),
                        phase: StructuralLogPhase::Ready,
                    },
                )
                .await
                .unwrap();
        }

        let records = store
            .list_structural_logs(&db_root(), Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        assert_eq!(records.len(), STRUCTURAL_LIST_PAGE_SIZE + 1);
    }

    #[tokio::test]
    async fn structural_log_listing_is_scoped_to_one_participant() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let first = TxId::from_bytes(b"first".to_vec());
        let second = TxId::from_bytes(b"second".to_vec());
        for participant in [&first, &second] {
            store
                .write_structural_log(
                    &db_root(),
                    &record_id(1),
                    &StructuralLog {
                        collection: CollectionAddress::root("db"),
                        source_token: Some(token(200)),
                        source_version: String::new(),
                        created_tokens: vec![token(201)],
                        split_key: Vec::new(),
                        participant_id: participant.clone(),
                        phase: StructuralLogPhase::Preparing,
                    },
                )
                .await
                .unwrap();
        }

        let records = store
            .list_structural_logs_for_participant(
                &db_root(),
                &first,
                Requirement::AtLeast(store.timeline.now()),
            )
            .await
            .unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0].1.value().unwrap().participant_id,
            first,
            "a participant listing must not discover another participant's work"
        );
    }
}
