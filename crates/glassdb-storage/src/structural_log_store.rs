//! Typed persistence for structural split recovery records.

use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_data::{DbRoot, ObjectPath, StructuralRecordId, TxId};

use crate::cached_store::{CachedStore, CasResult, Codec, Observation, Requirement};
use crate::error::StorageError;
use crate::structlog::StructuralLog;

const STRUCTURAL_LIST_PAGE_SIZE: usize = 128;

/// Reads and compare-and-swaps structural split recovery records.
#[derive(Clone)]
pub struct StructuralLogStore {
    structural_logs: crate::cached_store::TypedCachedStore<StructuralLog>,
}

impl Codec for StructuralLog {
    type Value = StructuralLog;

    fn decode(path: &ObjectPath, body: &[u8]) -> Result<Self::Value, StorageError> {
        let record = StructuralLog::decode(body)?;
        validate_structural_log_path(path, &record)?;
        Ok(record)
    }

    fn encode(path: &ObjectPath, record: &Self::Value) -> Result<Vec<u8>, StorageError> {
        validate_structural_log_path(path, record)?;
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

impl StructuralLogStore {
    /// Creates a structural-log store that reads and compare-and-swaps through `objects`.
    pub fn new(objects: CachedStore) -> Self {
        Self {
            structural_logs: objects.typed(),
        }
    }

    /// Creates a split write-ahead record and returns its exact observation.
    pub async fn write(
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
    pub async fn update(
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
    pub async fn list(
        &self,
        db_root: &DbRoot,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralRecordId, Observation<StructuralLog>)>, StorageError> {
        let prefix = ObjectPath::structural_records_prefix(db_root);
        self.list_under(&prefix, requirement).await
    }

    /// Lists only the unresolved structural records owned by `participant`.
    pub async fn list_for_participant(
        &self,
        db_root: &DbRoot,
        participant: &TxId,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralRecordId, Observation<StructuralLog>)>, StorageError> {
        let prefix = ObjectPath::participant_structural_records_prefix(db_root, participant);
        self.list_under(&prefix, requirement).await
    }

    /// Deletes the exact observed structural record, converging if it is missing.
    pub async fn delete(&self, expected: &Observation<StructuralLog>) -> Result<(), StorageError> {
        self.structural_logs.delete(expected).await?;
        Ok(())
    }

    async fn list_under(
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

fn validate_structural_log_path(
    path: &ObjectPath,
    record: &StructuralLog,
) -> Result<(), StorageError> {
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
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Timeline;
    use crate::structlog::StructuralLogPhase;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_data::{CollectionAddress, NodeToken};

    struct TestStore {
        structural_logs: StructuralLogStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = StructuralLogStore;

        fn deref(&self) -> &Self::Target {
            &self.structural_logs
        }
    }

    fn store_over(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let structural_logs = StructuralLogStore::new(objects);
        TestStore {
            structural_logs,
            timeline,
        }
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

    fn record(participant: &TxId, phase: StructuralLogPhase) -> StructuralLog {
        StructuralLog {
            collection: CollectionAddress::root("db"),
            source_token: Some(token(200)),
            source_version: "v1".to_string(),
            created_tokens: vec![token(201)],
            split_key: b"split".to_vec(),
            participant_id: participant.clone(),
            phase,
        }
    }

    #[test]
    fn structural_codec_rejects_a_different_path_participant() {
        let path = ObjectPath::StructuralRecord {
            db_root: db_root(),
            participant: TxId::from_bytes(b"path-participant".to_vec()),
            record_id: record_id(1),
        };
        let record = record(
            &TxId::from_bytes(b"body-participant".to_vec()),
            StructuralLogPhase::Preparing,
        );

        assert!(<StructuralLog as Codec>::encode(&path, &record).is_err());
    }

    #[tokio::test]
    async fn structural_log_lifecycle_rejects_a_stale_update_and_deletes() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let participant = TxId::from_bytes(b"participant".to_vec());
        let preparing = record(&participant, StructuralLogPhase::Preparing);
        let created = store
            .write(&db_root(), &record_id(1), &preparing)
            .await
            .unwrap();

        let ready = record(&participant, StructuralLogPhase::Ready);
        let updated = store.update(&created, &ready).await.unwrap().unwrap();
        assert!(
            store.update(&created, &preparing).await.unwrap().is_none(),
            "the superseded observation must not overwrite the current record"
        );

        store.delete(&updated).await.unwrap();
        assert!(
            store
                .list(&db_root(), Requirement::Any)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn structural_log_listing_drains_backend_pages() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let participant = TxId::from_bytes(b"participant".to_vec());
        for i in 0..=STRUCTURAL_LIST_PAGE_SIZE {
            let mut record = record(&participant, StructuralLogPhase::Ready);
            record.created_tokens = vec![token(i as u8)];
            record.split_key = vec![i as u8];
            store
                .write(&db_root(), &record_id(i as u8), &record)
                .await
                .unwrap();
        }

        let records = store
            .list(&db_root(), Requirement::AtLeast(store.timeline.now()))
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
                .write(
                    &db_root(),
                    &record_id(1),
                    &record(participant, StructuralLogPhase::Preparing),
                )
                .await
                .unwrap();
        }

        let records = store
            .list_for_participant(
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
