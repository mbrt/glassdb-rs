//! Typed persistence for structural intents.

use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_data::{DbRoot, ObjectPath, StructuralIntentId, TxId};

use crate::cached_store::{CachedStore, CasResult, Codec, Observation, Requirement};
use crate::error::StorageError;
use crate::structural_intent::StructuralIntent;

const STRUCTURAL_LIST_PAGE_SIZE: usize = 128;

/// Reads and compare-and-swaps structural intents for split recovery.
#[derive(Clone)]
pub struct StructuralIntentStore {
    structural_intents: crate::cached_store::TypedCachedStore<StructuralIntent>,
}

impl Codec for StructuralIntent {
    type Value = StructuralIntent;

    fn decode(path: &ObjectPath, body: &[u8]) -> Result<Self::Value, StorageError> {
        let intent = StructuralIntent::decode(body)?;
        validate_structural_intent_path(path, &intent)?;
        Ok(intent)
    }

    fn encode(path: &ObjectPath, intent: &Self::Value) -> Result<Vec<u8>, StorageError> {
        validate_structural_intent_path(path, intent)?;
        Ok(intent.encode())
    }

    fn size(intent: &Self::Value) -> usize {
        intent.encode().len()
    }

    fn accepts(path: &ObjectPath) -> bool {
        matches!(path, ObjectPath::StructuralIntent { .. })
    }

    fn name() -> &'static str {
        "structural intent"
    }
}

impl StructuralIntentStore {
    /// Creates a structural-intent store over `objects`.
    pub fn new(objects: CachedStore) -> Self {
        Self {
            structural_intents: objects.typed(),
        }
    }

    /// Creates a structural intent and returns its exact observation.
    pub async fn write(
        &self,
        db_root: &DbRoot,
        intent_id: &StructuralIntentId,
        intent: &StructuralIntent,
    ) -> Result<Observation<StructuralIntent>, StorageError> {
        let path = ObjectPath::StructuralIntent {
            db_root: db_root.clone(),
            participant: intent.participant_id.clone(),
            intent_id: intent_id.clone(),
        };
        match self
            .structural_intents
            .create(path, None, Arc::new(intent.clone()))
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
        expected: &Observation<StructuralIntent>,
        intent: &StructuralIntent,
    ) -> Result<Option<Observation<StructuralIntent>>, StorageError> {
        match self
            .structural_intents
            .compare_and_swap(expected, Arc::new(intent.clone()))
            .await
        {
            Ok(CasResult::Committed(observed)) => Ok(Some(observed)),
            Ok(CasResult::Conflict) | Err(StorageError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Lists exact observations of every unresolved structural intent.
    pub async fn list(
        &self,
        db_root: &DbRoot,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralIntentId, Observation<StructuralIntent>)>, StorageError> {
        let prefix = ObjectPath::structural_intents_prefix(db_root);
        self.list_under(&prefix, requirement).await
    }

    /// Lists only the unresolved structural intents owned by `participant`.
    pub async fn list_for_participant(
        &self,
        db_root: &DbRoot,
        participant: &TxId,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralIntentId, Observation<StructuralIntent>)>, StorageError> {
        let prefix = ObjectPath::participant_structural_intents_prefix(db_root, participant);
        self.list_under(&prefix, requirement).await
    }

    /// Deletes the exact observed structural intent, converging if it is missing.
    pub async fn delete(
        &self,
        expected: &Observation<StructuralIntent>,
    ) -> Result<(), StorageError> {
        self.structural_intents.delete(expected).await?;
        Ok(())
    }

    async fn list_under(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralIntentId, Observation<StructuralIntent>)>, StorageError> {
        let limit = backend::ListLimit::new(STRUCTURAL_LIST_PAGE_SIZE).unwrap();
        let mut cursor = None;
        let mut intents = Vec::new();
        loop {
            let page = self
                .structural_intents
                .list(prefix, cursor.as_ref(), limit)
                .await?;
            for path in page.objects {
                let ObjectPath::StructuralIntent { intent_id, .. } = path.object_path() else {
                    return Err(StorageError::other(
                        "structural listing returned a non-structural path",
                    ));
                };
                let intent_id = intent_id.clone();
                let observed = self.structural_intents.read(path, requirement).await?;
                if observed.exists() {
                    intents.push((intent_id, observed));
                }
            }
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(intents),
            }
        }
    }
}

fn validate_structural_intent_path(
    path: &ObjectPath,
    intent: &StructuralIntent,
) -> Result<(), StorageError> {
    let ObjectPath::StructuralIntent { participant, .. } = path else {
        return Err(StorageError::other(
            "structural intent has a non-structural path",
        ));
    };
    if participant != &intent.participant_id {
        return Err(StorageError::other(
            "structural-intent path does not match its participant",
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Timeline;
    use crate::structural_intent::StructuralIntentPhase;

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_data::{CollectionAddress, NodeToken};

    struct TestStore {
        structural_intents: StructuralIntentStore,
        timeline: Timeline,
    }

    impl std::ops::Deref for TestStore {
        type Target = StructuralIntentStore;

        fn deref(&self) -> &Self::Target {
            &self.structural_intents
        }
    }

    fn store_over(backend: Arc<dyn Backend>) -> TestStore {
        let timeline = Timeline::new();
        let objects = CachedStore::new(backend, 1 << 20, timeline.clone(), None);
        let structural_intents = StructuralIntentStore::new(objects);
        TestStore {
            structural_intents,
            timeline,
        }
    }

    fn token(byte: u8) -> NodeToken {
        NodeToken::from_bytes([byte; 16])
    }

    fn db_root() -> DbRoot {
        DbRoot::try_from("db").unwrap()
    }

    fn intent_id(byte: u8) -> StructuralIntentId {
        StructuralIntentId::from(token(byte))
    }

    fn intent(participant: &TxId, phase: StructuralIntentPhase) -> StructuralIntent {
        StructuralIntent {
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
        let path = ObjectPath::StructuralIntent {
            db_root: db_root(),
            participant: TxId::from_bytes(b"path-participant".to_vec()),
            intent_id: intent_id(1),
        };
        let intent = intent(
            &TxId::from_bytes(b"body-participant".to_vec()),
            StructuralIntentPhase::Preparing,
        );

        assert!(<StructuralIntent as Codec>::encode(&path, &intent).is_err());
    }

    #[tokio::test]
    async fn structural_intent_lifecycle_rejects_a_stale_update_and_deletes() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let participant = TxId::from_bytes(b"participant".to_vec());
        let preparing = intent(&participant, StructuralIntentPhase::Preparing);
        let created = store
            .write(&db_root(), &intent_id(1), &preparing)
            .await
            .unwrap();

        let ready = intent(&participant, StructuralIntentPhase::Ready);
        let updated = store.update(&created, &ready).await.unwrap().unwrap();
        assert!(
            store.update(&created, &preparing).await.unwrap().is_none(),
            "the superseded observation must not overwrite the current intent"
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
    async fn structural_intent_listing_drains_backend_pages() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let participant = TxId::from_bytes(b"participant".to_vec());
        for i in 0..=STRUCTURAL_LIST_PAGE_SIZE {
            let mut intent = intent(&participant, StructuralIntentPhase::Ready);
            intent.created_tokens = vec![token(i as u8)];
            intent.split_key = vec![i as u8];
            store
                .write(&db_root(), &intent_id(i as u8), &intent)
                .await
                .unwrap();
        }

        let intents = store
            .list(&db_root(), Requirement::AtLeast(store.timeline.now()))
            .await
            .unwrap();
        assert_eq!(intents.len(), STRUCTURAL_LIST_PAGE_SIZE + 1);
    }

    #[tokio::test]
    async fn structural_intent_listing_is_scoped_to_one_participant() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let first = TxId::from_bytes(b"first".to_vec());
        let second = TxId::from_bytes(b"second".to_vec());
        for participant in [&first, &second] {
            store
                .write(
                    &db_root(),
                    &intent_id(1),
                    &intent(participant, StructuralIntentPhase::Preparing),
                )
                .await
                .unwrap();
        }

        let intents = store
            .list_for_participant(
                &db_root(),
                &first,
                Requirement::AtLeast(store.timeline.now()),
            )
            .await
            .unwrap();
        assert_eq!(intents.len(), 1);
        assert_eq!(
            intents[0].1.value().unwrap().participant_id,
            first,
            "a participant listing must not discover another participant's work"
        );
    }
}
