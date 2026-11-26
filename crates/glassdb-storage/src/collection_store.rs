//! Collection record representation and compare-and-swap storage (ADR-050).
//!
//! The collection record (`{prefix}/_i`) contains the subcollection directory
//! and lifecycle/topology coordination. The B-link tree begins independently at
//! `{prefix}/_r`, so data-path mutations cannot conflict with this record.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use glassdb_data::{CollectionAddress, CollectionId, CollectionName, ObjectPath, TxId};
use glassdb_proto as pb;
use prost::Message;

use crate::cached_store::{
    CachedStore, CasResult, CasRetries, Codec, Converged, CutSearch, CutTarget, Decision,
    Observation, Requirement,
};
use crate::error::StorageError;
use crate::lock::SharedExclusiveLock;
use crate::wire_id::{decode_id, decode_optional_id};

/// A decoded collection record.
///
/// Child bindings are held in a sorted map so iteration and encoding are
/// canonical regardless of insertion order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CollectionRecord {
    children: BTreeMap<CollectionName, CollectionId>,
    directory_lock: SharedExclusiveLock,
    directory_generation: u64,
    topology_freeze: Option<TxId>,
    topology_participants: BTreeSet<TxId>,
    /// Changes on each change of the record, from a random start at its
    /// creation (ADR-106, ADR-109). Only the collection store sets it.
    record_sequence: u32,
}

impl CollectionRecord {
    /// Creates an empty collection record with no child bindings.
    pub fn new() -> Self {
        CollectionRecord {
            children: BTreeMap::new(),
            directory_lock: SharedExclusiveLock::default(),
            directory_generation: 0,
            topology_freeze: None,
            topology_participants: BTreeSet::new(),
            record_sequence: 0,
        }
    }

    /// Returns the record as a newly created one, with a random start of its
    /// record sequence.
    fn created(mut self) -> Self {
        self.record_sequence = crate::sequence::random_start();
        self
    }

    /// Returns the record as the change that follows `expected`.
    fn following(mut self, expected: &CollectionRecord) -> Self {
        self.record_sequence = expected.record_sequence.wrapping_add(1);
        self
    }

    /// Returns the collection ID bound to direct child `name`.
    pub fn child(&self, name: &CollectionName) -> Option<CollectionId> {
        self.children.get(name).copied()
    }

    /// Adds a valid direct child binding, returning whether the name was vacant.
    pub fn add_child(
        &mut self,
        name: CollectionName,
        id: CollectionId,
    ) -> Result<bool, StorageError> {
        use std::collections::btree_map::Entry;

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

    /// Removes a direct child binding, returning its former collection ID.
    pub fn remove_child(&mut self, name: &CollectionName) -> Option<CollectionId> {
        self.children.remove(name)
    }

    /// Iterates child bindings in canonical raw-name order.
    pub fn children(&self) -> impl Iterator<Item = (&CollectionName, CollectionId)> {
        self.children.iter().map(|(name, id)| (name, *id))
    }

    /// Returns the lock coordinating the direct-child directory.
    pub fn directory_lock(&self) -> &SharedExclusiveLock {
        &self.directory_lock
    }

    /// Returns the directory activity generation.
    pub fn directory_generation(&self) -> u64 {
        self.directory_generation
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
    pub fn advance_directory_generation(&mut self) {
        self.directory_generation = self.directory_generation.wrapping_add(1);
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
        record.directory_lock = SharedExclusiveLock::default();
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
            let name = CollectionName::new(&child.name).map_err(|_| {
                StorageError::other("collection record contains an invalid child name")
            })?;
            let id: CollectionId = decode_id(
                &child.collection_id,
                "collection record contains an invalid child ID",
            )?;
            if id.is_root() {
                return Err(StorageError::other(
                    "collection record binds a child to the reserved root ID",
                ));
            }
            if children.insert(name, id).is_some() {
                return Err(StorageError::other(
                    "collection record contains a duplicate child name",
                ));
            }
        }
        Ok(CollectionRecord {
            children,
            directory_lock: SharedExclusiveLock::from_pb(raw.directory_lock).map_err(|_| {
                StorageError::other("collection record has an invalid directory lock")
            })?,
            directory_generation: raw.directory_generation,
            topology_freeze: decode_optional_id(
                &raw.topology_freeze,
                "collection record has an invalid topology freeze",
            )?,
            topology_participants: raw
                .topology_participants
                .iter()
                .map(|id| decode_id(id, "collection record has an invalid topology participant"))
                .collect::<Result<_, _>>()?,
            record_sequence: raw.record_sequence,
        })
    }

    fn to_pb(&self) -> pb::CollectionRecord {
        // Children are already canonical via the BTreeMap.
        pb::CollectionRecord {
            children: self
                .children
                .iter()
                .map(|(name, id)| pb::CollectionDirectoryEntry {
                    name: name.as_bytes().to_vec(),
                    collection_id: id.as_bytes().to_vec(),
                })
                .collect(),
            directory_lock: (!self.directory_lock.is_empty()).then(|| self.directory_lock.to_pb()),
            directory_generation: self.directory_generation,
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
            record_sequence: self.record_sequence,
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

    fn decode(_path: &ObjectPath, body: &[u8]) -> Result<Self::Value, StorageError> {
        CollectionRecord::decode(body)
    }

    fn encode(_path: &ObjectPath, record: &Self::Value) -> Result<Vec<u8>, StorageError> {
        Ok(record.encode())
    }

    fn size(record: &Self::Value) -> usize {
        record.encoded_len()
    }

    fn accepts(path: &ObjectPath) -> bool {
        matches!(path, ObjectPath::CollectionRecord { .. })
    }

    fn name() -> &'static str {
        "collection record"
    }
}

/// Reads and compare-and-swaps collection metadata records.
#[derive(Clone)]
pub struct CollectionStore {
    records: crate::cached_store::TypedCachedStore<CollectionRecord>,
    /// The bound on the parallel checks of one batch of records.
    parallelism: NonZeroUsize,
}

impl CollectionStore {
    /// Creates a collection store over the shared decoded-object cache. A
    /// batch of record checks runs at most `parallelism` checks at a time.
    pub fn new(objects: CachedStore, parallelism: NonZeroUsize) -> Self {
        CollectionStore {
            records: objects.typed(),
            parallelism,
        }
    }

    /// Adds retained record observations to a cut search (ADR-097).
    pub fn add_records_to_cut(
        &self,
        search: &mut CutSearch,
        observations: &[Observation<CollectionRecord>],
    ) -> Result<(), StorageError> {
        self.records.add_to_cut(search, observations)
    }

    /// Checks the record observations that do not reach a cut target.
    pub async fn refresh_records_for_cut(
        &self,
        observations: &[Observation<CollectionRecord>],
        target: &CutTarget,
    ) -> Result<bool, StorageError> {
        self.records
            .refresh_for_cut(observations, target, self.parallelism)
            .await
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
        self.records
            .read(
                ObjectPath::CollectionRecord {
                    collection: collection.clone(),
                },
                requirement,
            )
            .await
    }

    /// Compare-and-swaps a collection record.
    pub async fn store_record(
        &self,
        record: &CollectionRecord,
        expected: &Observation<CollectionRecord>,
    ) -> Result<bool, StorageError> {
        let record = following(expected, record.clone())?;
        match self.records.replace(expected, Arc::new(record)).await {
            Ok(CasResult::Applied(_)) => Ok(true),
            Ok(CasResult::Rejected) | Err(StorageError::NotFound) => Ok(false),
            Err(error) => Err(error),
        }
    }

    /// Changes the record of `collection` with one writer until `decide` gives
    /// a result, as [`NodeStore::converge`](crate::NodeStore::converge) does
    /// for a node. Each CAS that comes back in doubt is fenced before the
    /// convergence decides again (ADR-106).
    ///
    /// Returns [`Fenced::InDoubt`] when the fence of a CAS is in doubt too, so
    /// that CAS can still land. Any other error comes from a read or from
    /// `decide`, and no CAS of the convergence can land after it.
    ///
    /// `retries` applies to each run between two fences. A fence starts a new
    /// run, so a [`CasRetries::Bounded`] budget can lose more CASes in total.
    pub async fn converge<T, E, F, Fut>(
        &self,
        collection: &CollectionAddress,
        proof: Requirement,
        retries: CasRetries,
        mut decide: F,
    ) -> Result<Fenced<T>, E>
    where
        E: From<StorageError>,
        F: FnMut(CollectionRecord, Observation<CollectionRecord>) -> Fut,
        Fut: Future<Output = Result<Decision<CollectionRecord, T>, E>>,
    {
        let path = ObjectPath::CollectionRecord {
            collection: collection.clone(),
        };
        loop {
            // The state that the last CAS of the convergence expected. A new
            // decision clears it: the convergence decides again only after its
            // last CAS was applied or lost.
            let expected = Mutex::new(None);
            let converged = self
                .records
                .converge(path.clone(), proof, retries, |record, observation| {
                    *expected.lock().unwrap() = None;
                    let decided = decide(record, observation.clone());
                    let expected = &expected;
                    async move {
                        let decision = decided.await.map_err(Failure::Decide)?;
                        let decision = match decision {
                            Decision::Write(record, value) => {
                                Decision::Write(following(&observation, record)?, value)
                            }
                            Decision::Advance(record) => {
                                Decision::Advance(following(&observation, record)?)
                            }
                            decision => return Ok(decision),
                        };
                        *expected.lock().unwrap() = Some(observation);
                        Ok(decision)
                    }
                })
                .await;
            let in_doubt = match converged {
                Ok(converged) => return Ok(Fenced::Converged(converged)),
                Err(Failure::Decide(error)) => return Err(error),
                Err(Failure::Storage(StorageError::Unavailable(error))) => {
                    match expected.into_inner().unwrap() {
                        Some(expected) => expected,
                        None => return Err(StorageError::Unavailable(error).into()),
                    }
                }
                Err(Failure::Storage(error)) => return Err(error.into()),
            };
            // An error of a read after an applied `Decision::Advance` also gets
            // here. The fence then finds a changed record sequence.
            if !self.fence(&path, &in_doubt).await {
                return Ok(Fenced::InDoubt);
            }
        }
    }

    /// Makes sure that a CAS that expected `expected` can never land. Returns
    /// `false` when the result is in doubt.
    ///
    /// The record sequence of `expected` changes with each change of the
    /// record, so a loaded record with another record sequence fences the CAS.
    /// A loaded record with the same one is in the state that the CAS
    /// expected: a write of that state again changes only the record sequence,
    /// and a lost write is a change too.
    async fn fence(&self, path: &ObjectPath, expected: &Observation<CollectionRecord>) -> bool {
        // Only a definitive result fences the CAS. A failed read or rewrite
        // tells nothing about the CAS, so it stays in doubt.
        let fresh = Requirement::after(self.records.currentness_barrier());
        let loaded = match self.records.read(path.clone(), fresh).await {
            Ok(loaded) => loaded,
            Err(error) => {
                tracing::debug!(%path, %error, "fence read failed, the CAS stays in doubt");
                return false;
            }
        };
        let (Some(expected), Some(current)) = (expected.value(), loaded.value()) else {
            // A CAS cannot land on a deleted record. A later create of the
            // path starts a new random record sequence, so it does not bring
            // the expected state back either (ADR-109), except for a retry of
            // one create request below GlassDB.
            return true;
        };
        if current.record_sequence != expected.record_sequence {
            return true;
        }
        let again = current.as_ref().clone().following(current);
        match self.records.replace(&loaded, Arc::new(again)).await {
            Ok(CasResult::Applied(_) | CasResult::Rejected) | Err(StorageError::NotFound) => true,
            Err(error) => {
                tracing::debug!(%path, %error, "fence rewrite failed, the CAS stays in doubt");
                false
            }
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
        };
        match self
            .records
            .create(path, None, Arc::new(record.clone().created()))
            .await?
        {
            CasResult::Applied(receipt) => Ok(Some(receipt.into_installed())),
            CasResult::Rejected => Ok(None),
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

/// The end of a [`CollectionStore::converge`].
// One value exists for each convergence, and only until its caller reads it.
#[allow(clippy::large_enum_variant)]
pub enum Fenced<T> {
    /// The convergence ended, and none of its CASes can land later.
    Converged(Converged<CollectionRecord, T>),
    /// A CAS of the convergence can still land.
    InDoubt,
}

impl<T> Fenced<T> {
    /// Returns the end of the convergence, or [`StorageError::Unavailable`]
    /// when a CAS can still land. A caller whose CAS adds no holder uses it.
    /// Such a CAS can land only on the state that it expected, and there it
    /// makes only the change that its caller decided, so no pin is necessary
    /// (ADR-106).
    pub fn or_unavailable(self) -> Result<Converged<CollectionRecord, T>, StorageError> {
        match self {
            Fenced::Converged(converged) => Ok(converged),
            Fenced::InDoubt => Err(StorageError::Unavailable(
                "a CAS on a collection record stays in doubt".into(),
            )),
        }
    }
}

/// An error of a convergence: from the store, or from its decision.
enum Failure<E> {
    Storage(StorageError),
    Decide(E),
}

impl<E> From<StorageError> for Failure<E> {
    fn from(error: StorageError) -> Self {
        Failure::Storage(error)
    }
}

/// Returns `record` as the change that follows the observed state, so that the
/// replace never restores an earlier state of the record (ADR-106).
fn following(
    expected: &Observation<CollectionRecord>,
    record: CollectionRecord,
) -> Result<CollectionRecord, StorageError> {
    let current = expected.value().ok_or_else(|| {
        StorageError::other("a collection record replace needs an observed record")
    })?;
    Ok(record.following(current))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::future::ready;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture};
    use glassdb_backend::{Backend, BackendError};
    use glassdb_data::TxId;

    use crate::{LeafBody, Node, NodeStore, Timeline};

    fn tx_id(prefix: &[u8]) -> TxId {
        TxId::with_priority(0, prefix)
    }

    fn collection_id(byte: u8) -> CollectionId {
        CollectionId::from_bytes([byte; 16])
    }

    fn name(name: impl AsRef<[u8]>) -> CollectionName {
        CollectionName::new(name).unwrap()
    }

    #[test]
    fn round_trip() {
        let mut record = CollectionRecord::new();
        record.add_child(name("users"), collection_id(1)).unwrap();
        record
            .add_child(name("settings"), collection_id(2))
            .unwrap();

        let decoded = CollectionRecord::decode(&record.encode()).unwrap();
        assert_eq!(decoded, record);
    }

    #[test]
    fn lifecycle_coordination_round_trips() {
        let directory_reader = tx_id(&[1]);
        let freeze = tx_id(&[2]);
        let participant = tx_id(&[3]);
        let mut record = CollectionRecord::new();
        record.add_directory_reader(directory_reader);
        record.advance_directory_generation();
        assert!(record.set_topology_freeze(freeze));
        assert!(record.add_topology_participant(freeze));

        let decoded = CollectionRecord::decode(&record.encode()).unwrap();
        assert_eq!(decoded.directory_generation(), 1);
        assert!(decoded.directory_lock().contains(&directory_reader));
        assert_eq!(decoded.topology_freeze(), Some(&freeze));
        assert!(
            decoded
                .topology_participants()
                .any(|holder| holder == &freeze)
        );
        let mut unfrozen = CollectionRecord::new();
        assert!(unfrozen.add_topology_participant(participant));
        assert!(
            unfrozen
                .topology_participants()
                .any(|holder| holder == &participant)
        );
    }

    #[test]
    fn decoding_rejects_invalid_directory_lock_states() {
        for lock in [
            pb::NodeLock {
                lock_type: pb::lock::LockType::Create as i32,
                locked_by: vec![vec![1; 16]],
            },
            pb::NodeLock {
                lock_type: pb::lock::LockType::Read as i32,
                locked_by: vec![vec![2; 16], vec![1; 16], vec![1; 16]],
            },
            pb::NodeLock {
                lock_type: pb::lock::LockType::Write as i32,
                locked_by: vec![vec![1; 15]],
            },
        ] {
            let raw = pb::CollectionRecord {
                directory_lock: Some(lock),
                ..pb::CollectionRecord::default()
            };

            let error = CollectionRecord::decode(&raw.encode_to_vec()).unwrap_err();
            assert_eq!(
                error.to_string(),
                "collection record has an invalid directory lock"
            );
        }
    }

    #[test]
    fn decoding_rejects_topology_ids_that_are_not_16_bytes() {
        for bad in [vec![1; 15], vec![1; 17]] {
            let freeze = pb::CollectionRecord {
                topology_freeze: bad.clone(),
                ..pb::CollectionRecord::default()
            };
            let error = CollectionRecord::decode(&freeze.encode_to_vec()).unwrap_err();
            assert_eq!(
                error.to_string(),
                "collection record has an invalid topology freeze"
            );
        }
        for bad in [Vec::new(), vec![1; 15], vec![1; 17]] {
            let participant = pb::CollectionRecord {
                topology_participants: vec![vec![2; 16], bad],
                ..pb::CollectionRecord::default()
            };
            let error = CollectionRecord::decode(&participant.encode_to_vec()).unwrap_err();
            assert_eq!(
                error.to_string(),
                "collection record has an invalid topology participant"
            );
        }
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
        assert!(record.add_child(name("a"), collection_id(1)).unwrap());
        assert!(!record.add_child(name("a"), collection_id(2)).unwrap());
        assert_eq!(record.child(&name("a")), Some(collection_id(1)));
        assert_eq!(record.child(&name("missing")), None);
        assert_eq!(record.remove_child(&name("a")), Some(collection_id(1)));
        assert_eq!(record.remove_child(&name("a")), None);
    }

    #[test]
    fn root_collection_id_cannot_be_bound_as_a_child() {
        let mut record = CollectionRecord::new();
        assert!(
            record
                .add_child(name("root"), CollectionId::root())
                .is_err()
        );
        assert_eq!(record.children().count(), 0);
    }

    #[test]
    fn decoding_rejects_invalid_child_names() {
        for bad in [
            Vec::new(),
            vec![0; glassdb_data::MAX_COLLECTION_NAME_BYTES + 1],
        ] {
            let raw = pb::CollectionRecord {
                children: vec![pb::CollectionDirectoryEntry {
                    name: bad,
                    collection_id: vec![1; 16],
                }],
                ..pb::CollectionRecord::default()
            };
            let error = CollectionRecord::decode(&raw.encode_to_vec()).unwrap_err();
            assert_eq!(
                error.to_string(),
                "collection record contains an invalid child name"
            );
        }
    }

    #[test]
    fn children_iterate_sorted() {
        let mut record = CollectionRecord::new();
        record.add_child(name("c"), collection_id(3)).unwrap();
        record.add_child(name("a"), collection_id(1)).unwrap();
        record.add_child(name("b"), collection_id(2)).unwrap();
        let names: Vec<&[u8]> = record.children().map(|(name, _)| name.as_bytes()).collect();
        assert_eq!(names, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn encoding_is_canonical_regardless_of_input_order() {
        let mk = |order: &[&[u8]]| {
            let mut r = CollectionRecord::new();
            for (i, n) in order.iter().enumerate() {
                r.add_child(name(n), collection_id(n[0] + i as u8)).unwrap();
            }
            r
        };
        let a = mk(&[b"c", b"a", b"b"]);
        let b = {
            let mut record = CollectionRecord::new();
            record
                .add_child(name("a"), collection_id(b'a' + 1))
                .unwrap();
            record
                .add_child(name("b"), collection_id(b'b' + 2))
                .unwrap();
            record.add_child(name("c"), collection_id(b'c')).unwrap();
            record
        };
        assert_eq!(a.encode(), b.encode());
    }

    // Golden vector: a fixed record must always encode to these exact bytes.
    // Changing the on-disk format must break this test.
    #[test]
    fn golden_encoding() {
        let mut record = CollectionRecord::new();
        record.add_child(name("users"), collection_id(1)).unwrap();
        record.record_sequence = 0x0403_0201;
        let got = record.encode();
        let want = [
            0x0a, 0x19, 0x0a, 0x05, 0x75, 0x73, 0x65, 0x72, 0x73, 0x12, 0x10, 0x01, 0x01, 0x01,
            0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01, 0x01,
            // field 6 (varint).
            0x30, 0x81, 0x84, 0x8c, 0x20,
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
        let records = CollectionStore::new(objects.clone(), std::num::NonZeroUsize::MIN);
        let nodes = NodeStore::new(objects, std::num::NonZeroUsize::MIN);
        let collection = CollectionAddress::new("db", collection_id(9));

        assert!(
            records
                .create_record(&collection, &CollectionRecord::new())
                .await
                .unwrap()
        );
        assert!(
            nodes
                .create_root(&collection, &Node::leaf(LeafBody::new()))
                .await
                .unwrap()
        );

        let (mut record, record_before) = records
            .load_record(
                &collection,
                Requirement::after(timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let (mut root, root_before) = nodes
            .load_root(
                &collection,
                Requirement::after(timeline.currentness_barrier()),
            )
            .await
            .unwrap();

        let child = CollectionId::from_bytes([1; 16]);
        assert!(record.add_child(name("child"), child).unwrap());
        assert!(records.store_record(&record, &record_before).await.unwrap());
        let (_, record_after) = records
            .load_record(
                &collection,
                Requirement::after(timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        let (_, root_after_record_write) = nodes
            .load_root(
                &collection,
                Requirement::after(timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert_ne!(record_before.revision(), record_after.revision());
        assert_eq!(
            root_before.revision(),
            root_after_record_write.revision(),
            "metadata mutation must not advance the data-root revision"
        );

        root.add_membership_reader(tx_id(&[1]));
        assert!(
            nodes
                .store_root(&collection, &root, &root_after_record_write)
                .await
                .unwrap()
        );
        let (_, record_after_root_write) = records
            .load_record(
                &collection,
                Requirement::after(timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert_eq!(
            record_after.revision(),
            record_after_root_write.revision(),
            "data-root mutation must not advance the record revision"
        );
    }

    // The step works on a collection record as on a node: a cached record
    // without the freeze proves nothing at a requirement after the freeze.
    #[tokio::test]
    async fn converge_proves_a_record_that_needs_no_change() {
        let backend: Arc<dyn glassdb_backend::Backend> = Arc::new(MemoryBackend::new());
        let local_timeline = Timeline::new();
        let store = |timeline| {
            let objects = CachedStore::new(backend.clone(), 1 << 20, timeline, None);
            CollectionStore::new(objects, NonZeroUsize::MIN)
        };
        let local = store(local_timeline.clone());
        let peer = store(Timeline::new());
        let collection = CollectionAddress::root("coll");
        assert!(
            local
                .create_record(&collection, &CollectionRecord::new())
                .await
                .unwrap()
        );
        let owner = tx_id(&[1]);
        let (mut record, observed) = peer
            .load_record(&collection, Requirement::ANY)
            .await
            .unwrap();
        assert!(record.set_topology_freeze(owner));
        assert!(peer.store_record(&record, &observed).await.unwrap());
        let unfreeze = async |proof| {
            local
                .converge(
                    &collection,
                    proof,
                    CasRetries::Bounded(8),
                    |mut record, _| {
                        std::future::ready(Ok::<_, StorageError>(
                            if record.remove_topology_freeze(&owner) {
                                Decision::Write(record, ())
                            } else {
                                Decision::Done(())
                            },
                        ))
                    },
                )
                .await
                .unwrap()
                .or_unavailable()
                .unwrap()
        };

        assert!(matches!(
            unfreeze(Requirement::ANY).await,
            Converged::Done(())
        ));
        let proof = Requirement::after(local_timeline.currentness_barrier());
        assert!(matches!(unfreeze(proof).await, Converged::Written { .. }));
    }

    /// A collection store over `backend`, with its own cache.
    fn store_over(backend: Arc<dyn glassdb_backend::Backend>) -> CollectionStore {
        let objects = CachedStore::new(backend, 1 << 20, Timeline::new(), None);
        CollectionStore::new(objects, std::num::NonZeroUsize::MIN)
    }

    /// Adds `reader` as a directory reader, unless it already reads.
    async fn add_reader(
        records: &CollectionStore,
        collection: &CollectionAddress,
        reader: TxId,
    ) -> Result<Fenced<()>, StorageError> {
        records
            .converge(
                collection,
                Requirement::ANY,
                CasRetries::Bounded(1),
                |mut record, _| async move {
                    if record.directory_lock().contains(&reader) {
                        return Ok::<_, StorageError>(Decision::Done(()));
                    }
                    record.add_directory_reader(reader);
                    Ok(Decision::Write(record, ()))
                },
            )
            .await
    }

    // ADR-106: a lock that is taken and released does not restore the earlier
    // state of the record.
    #[tokio::test]
    async fn each_change_changes_the_record_sequence() {
        let records = store_over(Arc::new(MemoryBackend::new()));
        let collection = CollectionAddress::new("db", collection_id(1));
        let reader = tx_id(&[1]);
        let mut seeded = CollectionRecord::new();
        seeded.add_directory_reader(reader);
        // A create stores a new start, also for a record copied from a changed
        // one.
        let prior = seeded.clone();
        seeded = seeded.following(&prior);
        assert!(records.create_record(&collection, &seeded).await.unwrap());
        let (created, first) = records
            .load_record(&collection, Requirement::ANY)
            .await
            .unwrap();
        assert_ne!(created.record_sequence, seeded.record_sequence);

        let mut released = created.clone();
        released.remove_directory_holder(&reader);
        assert!(records.store_record(&released, &first).await.unwrap());
        assert!(matches!(
            add_reader(&records, &collection, reader).await.unwrap(),
            Fenced::Converged(Converged::Written { .. })
        ));

        let (taken_again, last) = records
            .load_record(&collection, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(
            taken_again.record_sequence,
            created.record_sequence.wrapping_add(2)
        );
        assert!(!records.store_record(&released, &first).await.unwrap());
        assert_ne!(last.revision(), first.revision());
    }

    // ADR-109: a record that is created again by another create request does
    // not repeat the state of the first create, so a late CAS that expected
    // that state cannot land.
    #[tokio::test]
    async fn a_record_created_again_does_not_repeat_its_first_state() {
        let records = store_over(Arc::new(MemoryBackend::new()));
        let collection = CollectionAddress::new("db", collection_id(1));
        assert!(
            records
                .create_record(&collection, &CollectionRecord::new())
                .await
                .unwrap()
        );
        let (first, first_seen) = records
            .load_record(&collection, Requirement::ANY)
            .await
            .unwrap();
        records.delete_record(&first_seen).await.unwrap();
        assert!(
            records
                .create_record(&collection, &CollectionRecord::new())
                .await
                .unwrap()
        );

        let mut locked = first;
        locked.add_directory_reader(tx_id(&[1]));
        assert!(!records.store_record(&locked, &first_seen).await.unwrap());
    }

    // ADR-106: an in-doubt CAS that did not land yet is fenced, so it cannot
    // land later on the state that it expected.
    #[tokio::test]
    async fn an_in_doubt_cas_is_fenced_before_the_convergence_decides_again() {
        let inner = Arc::new(MemoryBackend::new());
        let hook = HookBackend::new(inner.clone());
        let records = store_over(hook.clone());
        let collection = CollectionAddress::new("db", collection_id(2));
        let path = ObjectPath::CollectionRecord {
            collection: collection.clone(),
        }
        .to_string();
        assert!(
            records
                .create_record(&collection, &CollectionRecord::new())
                .await
                .unwrap()
        );
        // The first CAS does not reach the backend now. The test sends it
        // later, as a backend can.
        let late = Arc::new(Mutex::new(None));
        hook.set_before({
            let late = late.clone();
            move |op| {
                let mut late = late.lock().unwrap();
                let held = match op {
                    BackendOp::WriteIf {
                        value, expected, ..
                    } if late.is_none() => {
                        *late = Some((value.to_vec(), (*expected).clone()));
                        true
                    }
                    _ => false,
                };
                Box::pin(ready(if held {
                    Err(BackendError::Unavailable("held".into()))
                } else {
                    Ok(())
                })) as HookFuture
            }
        });

        let reader = tx_id(&[1]);
        assert!(matches!(
            add_reader(&records, &collection, reader).await.unwrap(),
            Fenced::Converged(Converged::Written { .. })
        ));

        let (value, expected) = late.lock().unwrap().take().unwrap();
        assert!(matches!(
            inner.write_if(&path, value, &expected).await,
            Err(BackendError::Precondition)
        ));
        let (record, _) = records
            .load_record(&collection, Requirement::ANY)
            .await
            .unwrap();
        assert_eq!(
            record.directory_lock().holders(),
            std::slice::from_ref(&reader)
        );
    }

    // ADR-106: only a definitive result fences an in-doubt CAS. A fence whose
    // read fails, for any reason, leaves the CAS in doubt.
    #[tokio::test]
    async fn a_failed_fence_read_leaves_the_cas_in_doubt() {
        let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
        let records = store_over(hook.clone());
        let collection = CollectionAddress::new("db", collection_id(4));
        assert!(
            records
                .create_record(&collection, &CollectionRecord::new())
                .await
                .unwrap()
        );
        let in_doubt = Arc::new(std::sync::atomic::AtomicBool::new(false));
        hook.set_before({
            let in_doubt = in_doubt.clone();
            move |op| {
                let result = match op {
                    BackendOp::WriteIf { .. } => {
                        in_doubt.store(true, std::sync::atomic::Ordering::SeqCst);
                        Err(BackendError::Unavailable("lost".into()))
                    }
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                        if in_doubt.load(std::sync::atomic::Ordering::SeqCst) =>
                    {
                        Err(BackendError::other("rejected"))
                    }
                    _ => Ok(()),
                };
                Box::pin(ready(result)) as HookFuture
            }
        });

        assert!(matches!(
            add_reader(&records, &collection, tx_id(&[1]))
                .await
                .unwrap(),
            Fenced::InDoubt
        ));
    }

    // ADR-106: a fence whose rewrite fails, for any reason, leaves the CAS in
    // doubt too.
    #[tokio::test]
    async fn a_failed_fence_rewrite_leaves_the_cas_in_doubt() {
        let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
        let records = store_over(hook.clone());
        let collection = CollectionAddress::new("db", collection_id(5));
        assert!(
            records
                .create_record(&collection, &CollectionRecord::new())
                .await
                .unwrap()
        );
        // The CAS does not land and comes back in doubt. The fence then reads
        // the record unchanged, and its rewrite is rejected.
        let writes = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        hook.set_before({
            let writes = writes.clone();
            move |op| {
                let result = match op {
                    BackendOp::WriteIf { .. } => {
                        match writes.fetch_add(1, std::sync::atomic::Ordering::SeqCst) {
                            0 => Err(BackendError::Unavailable("lost".into())),
                            _ => Err(BackendError::other("rejected")),
                        }
                    }
                    _ => Ok(()),
                };
                Box::pin(ready(result)) as HookFuture
            }
        });

        assert!(matches!(
            add_reader(&records, &collection, tx_id(&[1]))
                .await
                .unwrap(),
            Fenced::InDoubt
        ));
        assert_eq!(writes.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    // ADR-106: when the fence is in doubt too, the owner learns that its CAS
    // can still land. A caller whose CAS adds no holder, for example a
    // release, reports it as unavailable.
    #[tokio::test]
    async fn a_cas_whose_fence_is_in_doubt_stays_in_doubt() {
        let hook = HookBackend::new(Arc::new(MemoryBackend::new()));
        let records = store_over(hook.clone());
        let collection = CollectionAddress::new("db", collection_id(3));
        let held = tx_id(&[9]);
        let mut record = CollectionRecord::new();
        record.add_directory_reader(held);
        assert!(records.create_record(&collection, &record).await.unwrap());
        hook.set_before(|op| {
            let refused = matches!(op, BackendOp::WriteIf { .. });
            Box::pin(ready(if refused {
                Err(BackendError::Unavailable("refused".into()))
            } else {
                Ok(())
            })) as HookFuture
        });

        assert!(matches!(
            add_reader(&records, &collection, tx_id(&[1]))
                .await
                .unwrap(),
            Fenced::InDoubt
        ));
        let release = records
            .converge(
                &collection,
                Requirement::ANY,
                CasRetries::Bounded(1),
                |mut record, _| async move {
                    record.remove_directory_holder(&held);
                    Ok::<_, StorageError>(Decision::Write(record, ()))
                },
            )
            .await
            .unwrap()
            .or_unavailable();
        assert!(matches!(release, Err(StorageError::Unavailable(_))));
    }
}
