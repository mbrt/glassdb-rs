//! Collection record representation and compare-and-swap storage (ADR-050).
//!
//! The collection record (`{prefix}/_i`) contains the subcollection directory
//! and lifecycle/topology coordination. The B-link tree begins independently at
//! `{prefix}/_r`, so data-path mutations cannot conflict with this record.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use glassdb_data::{CollectionAddress, CollectionId, MAX_COLLECTION_NAME_BYTES, ObjectPath, TxId};
use glassdb_proto as pb;
use prost::Message;

use crate::cached_store::{CachedStore, CasResult, Codec, Observation, Requirement};
use crate::error::StorageError;
use crate::lock::LockType;
use crate::node::NodeLock;

/// A decoded collection record.
///
/// Child bindings are held in a sorted map so iteration and encoding are
/// canonical regardless of insertion order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionRecord {
    children: BTreeMap<Vec<u8>, CollectionId>,
    directory_lock: NodeLock,
    directory_version: u64,
    topology_freeze: Option<TxId>,
    topology_participants: BTreeSet<TxId>,
}

impl CollectionRecord {
    /// Creates an empty collection record with no child bindings.
    pub fn new() -> Self {
        CollectionRecord {
            children: BTreeMap::new(),
            directory_lock: NodeLock::default(),
            directory_version: 0,
            topology_freeze: None,
            topology_participants: BTreeSet::new(),
        }
    }

    /// Returns the incarnation bound to direct child `name`.
    pub fn child(&self, name: &[u8]) -> Option<CollectionId> {
        self.children.get(name).copied()
    }

    /// Adds a valid direct child binding, returning whether the name was vacant.
    pub fn add_child(
        &mut self,
        name: impl Into<Vec<u8>>,
        id: CollectionId,
    ) -> Result<bool, StorageError> {
        use std::collections::btree_map::Entry;

        let name = name.into();
        if name.is_empty() || name.len() > MAX_COLLECTION_NAME_BYTES {
            return Err(StorageError::other("invalid child collection name"));
        }
        if id.is_root() {
            return Err(StorageError::other(
                "a child cannot use the reserved root collection ID",
            ));
        }
        Ok(match self.children.entry(name) {
            Entry::Vacant(entry) => {
                entry.insert(id);
                true
            }
            Entry::Occupied(_) => false,
        })
    }

    /// Removes a direct child binding, returning its former incarnation.
    pub fn remove_child(&mut self, name: &[u8]) -> Option<CollectionId> {
        self.children.remove(name)
    }

    /// Iterates child bindings in canonical raw-name order.
    pub fn children(&self) -> impl Iterator<Item = (&[u8], CollectionId)> {
        self.children
            .iter()
            .map(|(name, id)| (name.as_slice(), *id))
    }

    /// Returns the lock coordinating the direct-child directory.
    pub fn directory_lock(&self) -> &NodeLock {
        &self.directory_lock
    }

    /// Returns the directory activity version.
    pub fn directory_version(&self) -> u64 {
        self.directory_version
    }

    /// Installs a shared directory holder.
    pub fn add_directory_reader(&mut self, id: TxId) {
        self.directory_lock.add_reader(id);
    }

    /// Installs an exclusive directory holder.
    pub fn set_directory_writer(&mut self, id: TxId) {
        self.directory_lock.set_writer(id);
    }

    /// Removes one directory holder.
    pub fn remove_directory_holder(&mut self, id: &TxId) -> bool {
        self.directory_lock.remove(id)
    }

    /// Records one committed directory mutation batch.
    pub fn advance_directory_version(&mut self) {
        self.directory_version = self.directory_version.wrapping_add(1);
    }

    /// Returns the transaction currently freezing collection topology.
    pub fn topology_freeze(&self) -> Option<&TxId> {
        self.topology_freeze.as_ref()
    }

    /// Installs a topology freeze if no other transaction owns it.
    pub fn set_topology_freeze(&mut self, id: TxId) -> bool {
        if self
            .topology_freeze
            .as_ref()
            .is_some_and(|holder| holder != &id)
        {
            return false;
        }
        self.topology_freeze = Some(id);
        true
    }

    /// Clears a topology freeze owned by `id`.
    pub fn remove_topology_freeze(&mut self, id: &TxId) -> bool {
        if self.topology_freeze.as_ref() != Some(id) {
            return false;
        }
        self.topology_freeze = None;
        true
    }

    /// Returns structural operations that joined collection topology.
    pub fn topology_participants(&self) -> impl Iterator<Item = &TxId> {
        self.topology_participants.iter()
    }

    /// Joins collection topology unless a different transaction froze it.
    pub fn add_topology_participant(&mut self, id: TxId) -> bool {
        if self
            .topology_freeze
            .as_ref()
            .is_some_and(|holder| holder != &id)
        {
            return false;
        }
        self.topology_participants.insert(id);
        true
    }

    /// Removes a structural topology participant.
    pub fn remove_topology_participant(&mut self, id: &TxId) -> bool {
        self.topology_participants.remove(id)
    }

    /// Encodes the record to its canonical protobuf body.
    pub fn encode(&self) -> Vec<u8> {
        self.to_pb().encode_to_vec()
    }

    /// Returns the canonical protobuf size without allocating the encoded body.
    pub fn encoded_len(&self) -> usize {
        self.to_pb().encoded_len()
    }

    /// Returns the encoded size without transient coordination holders.
    pub fn content_encoded_len(&self) -> usize {
        let mut record = self.clone();
        record.directory_lock = NodeLock::default();
        record.topology_freeze = None;
        record.topology_participants.clear();
        record.encoded_len()
    }

    /// Decodes a collection record from its protobuf body.
    pub fn decode(buf: &[u8]) -> Result<Self, StorageError> {
        let raw = pb::CollectionRecord::decode(buf)
            .map_err(|e| StorageError::with_source("unmarshalling collection record", e))?;
        let mut children = BTreeMap::new();
        for child in raw.children {
            if child.name.is_empty() || child.name.len() > MAX_COLLECTION_NAME_BYTES {
                return Err(StorageError::other(
                    "collection record contains an invalid child name",
                ));
            }
            let id = CollectionId::from_slice(&child.collection_id).ok_or_else(|| {
                StorageError::other("collection record contains an invalid child ID")
            })?;
            if id.is_root() {
                return Err(StorageError::other(
                    "collection record binds a child to the reserved root ID",
                ));
            }
            if children.insert(child.name, id).is_some() {
                return Err(StorageError::other(
                    "collection record contains a duplicate child name",
                ));
            }
        }
        Ok(CollectionRecord {
            children,
            directory_lock: {
                let lock = NodeLock::from_pb(raw.directory_lock);
                match (lock.lock_type(), lock.holders()) {
                    (LockType::None | LockType::Unknown, [])
                    | (LockType::Read, [_, ..])
                    | (LockType::Write, [_]) => lock,
                    _ => {
                        return Err(StorageError::other(
                            "collection record has an invalid directory lock",
                        ));
                    }
                }
            },
            directory_version: raw.directory_version,
            topology_freeze: (!raw.topology_freeze.is_empty())
                .then(|| TxId::from_bytes(raw.topology_freeze)),
            topology_participants: raw
                .topology_participants
                .into_iter()
                .filter(|id| !id.is_empty())
                .map(TxId::from_bytes)
                .collect(),
        })
    }

    fn to_pb(&self) -> pb::CollectionRecord {
        // Children are already canonical via the BTreeMap.
        pb::CollectionRecord {
            children: self
                .children
                .iter()
                .map(|(name, id)| pb::CollectionDirectoryEntry {
                    name: name.clone(),
                    collection_id: id.as_bytes().to_vec(),
                })
                .collect(),
            directory_lock: (!self.directory_lock.is_empty()).then(|| self.directory_lock.to_pb()),
            directory_version: self.directory_version,
            topology_freeze: self
                .topology_freeze
                .as_ref()
                .map(|id| id.as_bytes().to_vec())
                .unwrap_or_default(),
            topology_participants: self
                .topology_participants
                .iter()
                .map(|id| id.as_bytes().to_vec())
                .collect(),
        }
    }
}

impl Default for CollectionRecord {
    fn default() -> Self {
        CollectionRecord::new()
    }
}

impl Codec for CollectionRecord {
    type Value = CollectionRecord;

    fn decode(_path: &str, body: &[u8]) -> Result<Self::Value, StorageError> {
        CollectionRecord::decode(body)
    }

    fn encode(record: &Self::Value) -> Result<Vec<u8>, StorageError> {
        Ok(record.encode())
    }

    fn size(record: &Self::Value) -> usize {
        record.encoded_len()
    }

    fn valid_path(path: &str) -> bool {
        matches!(
            ObjectPath::try_from(path),
            Ok(ObjectPath::CollectionRecord { .. })
        )
    }

    fn name() -> &'static str {
        "collection record"
    }
}

/// Reads and compare-and-swaps collection metadata records.
#[derive(Clone)]
pub struct CollectionStore {
    records: crate::cached_store::TypedCachedStore<CollectionRecord>,
}

impl CollectionStore {
    /// Creates a collection store over the shared decoded-object cache.
    pub fn new(objects: CachedStore) -> Self {
        CollectionStore {
            records: objects.typed(),
        }
    }

    /// Loads a collection record, or returns [`StorageError::NotFound`].
    pub async fn load_record(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<(CollectionRecord, Observation<CollectionRecord>), StorageError> {
        let observed = self.load_record_state(collection, requirement).await?;
        let record = observed
            .value()
            .map(|record| record.as_ref().clone())
            .ok_or(StorageError::NotFound)?;
        Ok((record, observed))
    }

    /// Loads the exact record observation, including observed absence.
    pub async fn load_record_state(
        &self,
        collection: &CollectionAddress,
        requirement: Requirement,
    ) -> Result<Observation<CollectionRecord>, StorageError> {
        let path = ObjectPath::CollectionRecord {
            collection: collection.clone(),
        }
        .to_string();
        self.records.read(&path, requirement).await
    }

    /// Compare-and-swaps a collection record.
    pub async fn store_record(
        &self,
        record: &CollectionRecord,
        expected: &Observation<CollectionRecord>,
    ) -> Result<bool, StorageError> {
        match self
            .records
            .compare_and_swap(expected, Arc::new(record.clone()))
            .await
        {
            Ok(CasResult::Committed(_)) => Ok(true),
            Ok(CasResult::Conflict) | Err(StorageError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Creates a collection record if absent.
    pub async fn create_record(
        &self,
        collection: &CollectionAddress,
        record: &CollectionRecord,
    ) -> Result<bool, StorageError> {
        Ok(self
            .create_record_observed(collection, record)
            .await?
            .is_some())
    }

    /// Creates a record and returns its installed observation.
    pub async fn create_record_observed(
        &self,
        collection: &CollectionAddress,
        record: &CollectionRecord,
    ) -> Result<Option<Observation<CollectionRecord>>, StorageError> {
        let path = ObjectPath::CollectionRecord {
            collection: collection.clone(),
        }
        .to_string();
        match self
            .records
            .create(&path, None, Arc::new(record.clone()))
            .await?
        {
            CasResult::Committed(observed) => Ok(Some(observed)),
            CasResult::Conflict => Ok(None),
        }
    }

    /// Deletes an exact record observation, converging if it is missing.
    pub async fn delete_record(
        &self,
        expected: &Observation<CollectionRecord>,
    ) -> Result<(), StorageError> {
        self.records.delete(expected).await?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_data::TxId;

    use crate::{Node, Shard, ShardStore, Timeline};

    fn collection_id(byte: u8) -> CollectionId {
        CollectionId::from_slice(&[byte; 16]).unwrap()
    }

    #[test]
    fn round_trip() {
        let mut record = CollectionRecord::new();
        record
            .add_child(b"users".to_vec(), collection_id(1))
            .unwrap();
        record
            .add_child(b"settings".to_vec(), collection_id(2))
            .unwrap();

        let decoded = CollectionRecord::decode(&record.encode()).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn lifecycle_coordination_round_trips() {
        let directory_reader = TxId::from_bytes(vec![1]);
        let freeze = TxId::from_bytes(vec![2]);
        let participant = TxId::from_bytes(vec![3]);
        let mut record = CollectionRecord::new();
        record.add_directory_reader(directory_reader.clone());
        record.advance_directory_version();
        assert!(record.set_topology_freeze(freeze.clone()));
        assert!(record.add_topology_participant(freeze.clone()));

        let decoded = CollectionRecord::decode(&record.encode()).unwrap();
        assert_eq!(decoded.directory_version(), 1);
        assert!(decoded.directory_lock().contains(&directory_reader));
        assert_eq!(decoded.topology_freeze(), Some(&freeze));
        assert!(
            decoded
                .topology_participants()
                .any(|holder| holder == &freeze)
        );
        let mut unfrozen = CollectionRecord::new();
        assert!(unfrozen.add_topology_participant(participant.clone()));
        assert!(
            unfrozen
                .topology_participants()
                .any(|holder| holder == &participant)
        );
    }

    #[test]
    fn empty_round_trip() {
        let record = CollectionRecord::new();
        let decoded = CollectionRecord::decode(&record.encode()).unwrap();
        assert_eq!(decoded, record);
        assert_eq!(decoded.children().count(), 0);
    }

    #[test]
    fn child_directory_ops() {
        let mut record = CollectionRecord::new();
        assert!(record.add_child(b"a".to_vec(), collection_id(1)).unwrap());
        assert!(!record.add_child(b"a".to_vec(), collection_id(2)).unwrap());
        assert_eq!(record.child(b"a"), Some(collection_id(1)));
        assert_eq!(record.child(b"missing"), None);
        assert_eq!(record.remove_child(b"a"), Some(collection_id(1)));
        assert_eq!(record.remove_child(b"a"), None);
    }

    #[test]
    fn invalid_child_bindings_are_rejected_before_encoding() {
        let mut record = CollectionRecord::new();
        assert!(record.add_child(Vec::new(), collection_id(1)).is_err());
        assert!(
            record
                .add_child(vec![0; MAX_COLLECTION_NAME_BYTES + 1], collection_id(1))
                .is_err()
        );
        assert!(
            record
                .add_child(b"root".to_vec(), CollectionId::root())
                .is_err()
        );
        assert_eq!(record.children().count(), 0);
    }

    #[test]
    fn children_iterate_sorted() {
        let mut record = CollectionRecord::new();
        record.add_child(b"c".to_vec(), collection_id(3)).unwrap();
        record.add_child(b"a".to_vec(), collection_id(1)).unwrap();
        record.add_child(b"b".to_vec(), collection_id(2)).unwrap();
        let names: Vec<&[u8]> = record.children().map(|(name, _)| name).collect();
        assert_eq!(names, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn encoding_is_canonical_regardless_of_input_order() {
        let mk = |order: &[&[u8]]| {
            let mut r = CollectionRecord::new();
            for (i, n) in order.iter().enumerate() {
                r.add_child(n.to_vec(), collection_id(n[0] + i as u8))
                    .unwrap();
            }
            r
        };
        let a = mk(&[b"c", b"a", b"b"]);
        let b = {
            let mut record = CollectionRecord::new();
            record
                .add_child(b"a".to_vec(), collection_id(b'a' + 1))
                .unwrap();
            record
                .add_child(b"b".to_vec(), collection_id(b'b' + 2))
                .unwrap();
            record
                .add_child(b"c".to_vec(), collection_id(b'c'))
                .unwrap();
            record
        };
        assert_eq!(a.encode(), b.encode());
    }

    // Golden vector: a fixed record must always encode to these exact bytes.
    // Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let mut record = CollectionRecord::new();
        record
            .add_child(b"users".to_vec(), collection_id(1))
            .unwrap();
        let got = record.encode();
        let want = [
            0x0a, 0x19, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x73, 0x12, 0x10, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
        ];
        assert_eq!(record.encoded_len(), got.len());
        assert_eq!(got, want, "collection-record encoding drifted: {got:02x?}");
    }

    #[tokio::test]
    async fn record_and_tree_root_have_independent_cas_revisions() {
        let timeline = Timeline::new();
        let objects = CachedStore::new(
            Arc::new(MemoryBackend::new()),
            1 << 20,
            timeline.clone(),
            None,
        );
        let records = CollectionStore::new(objects.clone());
        let shards = ShardStore::new(objects);
        let collection = CollectionAddress::new("db", collection_id(9));

        assert!(
            records
                .create_record(&collection, &CollectionRecord::new())
                .await
                .unwrap()
        );
        assert!(
            shards
                .create_root(&collection, &Node::leaf(Shard::new()))
                .await
                .unwrap()
        );

        let (mut record, record_before) = records
            .load_record(&collection, Requirement::AtLeast(timeline.now()))
            .await
            .unwrap();
        let (mut root, root_before) = shards
            .load_root(&collection, Requirement::AtLeast(timeline.now()))
            .await
            .unwrap();

        let child = CollectionId::from_slice(&[1; 16]).unwrap();
        assert!(record.add_child(b"child".to_vec(), child).unwrap());
        assert!(records.store_record(&record, &record_before).await.unwrap());
        let (_, record_after) = records
            .load_record(&collection, Requirement::AtLeast(timeline.now()))
            .await
            .unwrap();
        let (_, root_after_record_write) = shards
            .load_root(&collection, Requirement::AtLeast(timeline.now()))
            .await
            .unwrap();
        assert_ne!(record_before.revision(), record_after.revision());
        assert_eq!(
            root_before.revision(),
            root_after_record_write.revision(),
            "metadata mutation must not advance the data-root revision"
        );

        root.add_membership_reader(TxId::from_bytes(vec![1]));
        assert!(
            shards
                .store_root(&collection, &root, &root_after_record_write)
                .await
                .unwrap()
        );
        let (_, record_after_root_write) = records
            .load_record(&collection, Requirement::AtLeast(timeline.now()))
            .await
            .unwrap();
        assert_eq!(
            record_after.revision(),
            record_after_root_write.revision(),
            "data-root mutation must not advance the record revision"
        );
    }
}
