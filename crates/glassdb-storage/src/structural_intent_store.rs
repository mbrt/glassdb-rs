//! Typed persistence for structural intents.

use std::sync::Arc;

use glassdb_backend as backend;
use glassdb_data::{DbPrefix, ObjectPath, StructuralIntentId, TxId};

use crate::cached_store::{CachedStore, CasResult, Codec, Observation, Requirement};
use crate::error::StorageError;
use crate::structural_intent::StructuralIntent;

const STRUCTURAL_LIST_PAGE_SIZE: usize = 128;

/// Reads and compare-and-swaps structural intents for split recovery.
#[derive(Clone)]
pub struct StructuralIntentStore {
    structural_intents: crate::cached_store::TypedCachedStore<StructuralIntent>,
}

/// One bounded page of structural recovery observations.
pub struct StructuralIntentPage {
    pub intents: Vec<(StructuralIntentId, Observation<StructuralIntent>)>,
    pub next: Option<backend::ListCursor>,
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
        db_prefix: &DbPrefix,
        intent_id: &StructuralIntentId,
        intent: &StructuralIntent,
    ) -> Result<Observation<StructuralIntent>, StorageError> {
        let path = ObjectPath::StructuralIntent {
            db_prefix: db_prefix.clone(),
            participant: intent.participant_id.clone(),
            intent_id: intent_id.clone(),
        };
        match self
            .structural_intents
            .create(path, None, Arc::new(intent.clone()))
            .await
        {
            Ok(CasResult::Applied(receipt)) => Ok(receipt.into_installed()),
            Ok(CasResult::Rejected) => Err(StorageError::Precondition),
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
            .replace(expected, Arc::new(intent.clone()))
            .await
        {
            Ok(CasResult::Applied(receipt)) => Ok(Some(receipt.into_installed())),
            Ok(CasResult::Rejected) | Err(StorageError::NotFound) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Discovers all structural recovery candidates.
    ///
    /// Uses the same evidence rules as [`Self::discover_page`]: present bodies
    /// are candidates, and only absence must meet `requirement`.
    pub async fn discover(
        &self,
        db_prefix: &DbPrefix,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralIntentId, Observation<StructuralIntent>)>, StorageError> {
        let prefix = ObjectPath::structural_intents_prefix(db_prefix);
        self.discover_under(&prefix, requirement).await
    }

    /// Discovers one page of structural recovery candidates.
    ///
    /// Present bodies may carry any cached evidence. A listed body observed as
    /// absent must satisfy `requirement`. This avoids indefinitely
    /// skipping a created intent when callers advance the bound between passes.
    /// Recovery must delete Preparing at its exact revision and classify Ready
    /// under a barrier captured after observing it. Discovery does not establish
    /// that a present candidate still exists or advance its evidence.
    pub async fn discover_page(
        &self,
        db_prefix: &DbPrefix,
        cursor: Option<&backend::ListCursor>,
        requirement: Requirement,
    ) -> Result<StructuralIntentPage, StorageError> {
        let prefix = ObjectPath::structural_intents_prefix(db_prefix);
        self.read_page(&prefix, cursor, requirement).await
    }

    /// Discovers recovery candidates owned by `participant`.
    ///
    /// Uses the same evidence rules as [`Self::discover_page`]: present bodies
    /// are candidates, and only absence must meet `requirement`.
    pub async fn discover_for_participant(
        &self,
        db_prefix: &DbPrefix,
        participant: &TxId,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralIntentId, Observation<StructuralIntent>)>, StorageError> {
        let prefix = ObjectPath::participant_structural_intents_prefix(db_prefix, participant);
        self.discover_under(&prefix, requirement).await
    }

    /// Deletes the exact observed structural intent, converging if it is missing.
    pub async fn delete(
        &self,
        expected: &Observation<StructuralIntent>,
    ) -> Result<(), StorageError> {
        self.structural_intents.delete(expected).await?;
        Ok(())
    }

    async fn discover_under(
        &self,
        prefix: &str,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralIntentId, Observation<StructuralIntent>)>, StorageError> {
        let mut cursor = None;
        let mut intents = Vec::new();
        loop {
            let page = self.read_page(prefix, cursor.as_ref(), requirement).await?;
            intents.extend(page.intents);
            match page.next {
                Some(next) => cursor = Some(next),
                None => return Ok(intents),
            }
        }
    }

    async fn read_page(
        &self,
        prefix: &str,
        cursor: Option<&backend::ListCursor>,
        requirement: Requirement,
    ) -> Result<StructuralIntentPage, StorageError> {
        let page = self
            .structural_intents
            .list(
                prefix,
                cursor,
                backend::ListLimit::new(STRUCTURAL_LIST_PAGE_SIZE).unwrap(),
            )
            .await?;
        let mut intents = Vec::new();
        for path in page.objects {
            let ObjectPath::StructuralIntent { intent_id, .. } = path.object_path() else {
                return Err(StorageError::other(
                    "structural listing returned a non-structural path",
                ));
            };
            let intent_id = intent_id.clone();
            let mut observed = self
                .structural_intents
                .read(path.clone(), Requirement::ANY)
                .await?;
            if !observed.exists() && !observed.satisfies(requirement) {
                // LIST can name an intent hidden by an older cached absence.
                // The caller's bound must also govern a decision to omit it.
                observed = self.structural_intents.read(path, requirement).await?;
            }
            if observed.exists() {
                intents.push((intent_id, observed));
            }
        }
        Ok(StructuralIntentPage {
            intents,
            next: page.next,
        })
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
    use crate::structural_intent::{StructuralChange, StructuralIntentPhase};

    use glassdb_backend::Backend;
    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{BackendOp, HookBackend, RecordingBackend};
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

    fn db_prefix() -> DbPrefix {
        DbPrefix::try_from("db").unwrap()
    }

    fn intent_id(byte: u8) -> StructuralIntentId {
        StructuralIntentId::from(token(byte))
    }

    fn intent(participant: &TxId, phase: StructuralIntentPhase) -> StructuralIntent {
        StructuralIntent {
            collection: CollectionAddress::root("db"),
            source_token: Some(token(200)),
            source_revision: "v1".to_string(),
            change: StructuralChange::Split {
                created_tokens: vec![token(201)],
                split_key: b"split".to_vec(),
            },
            participant_id: participant.clone(),
            phase,
        }
    }

    #[test]
    fn structural_codec_rejects_a_different_path_participant() {
        let path = ObjectPath::StructuralIntent {
            db_prefix: db_prefix(),
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
            .write(&db_prefix(), &intent_id(1), &preparing)
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
                .discover(&db_prefix(), Requirement::ANY)
                .await
                .unwrap()
                .is_empty()
        );
    }

    #[tokio::test]
    async fn structural_intent_discovery_drains_backend_pages() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let participant = TxId::from_bytes(b"participant".to_vec());
        for i in 0..=STRUCTURAL_LIST_PAGE_SIZE {
            let mut intent = intent(&participant, StructuralIntentPhase::Ready);
            intent.change = StructuralChange::Split {
                created_tokens: vec![token(i as u8)],
                split_key: vec![i as u8],
            };
            store
                .write(&db_prefix(), &intent_id(i as u8), &intent)
                .await
                .unwrap();
        }

        let intents = store
            .discover(
                &db_prefix(),
                Requirement::after(store.timeline.currentness_barrier()),
            )
            .await
            .unwrap();
        assert_eq!(intents.len(), STRUCTURAL_LIST_PAGE_SIZE + 1);
    }

    #[tokio::test]
    async fn structural_intent_discovery_is_scoped_to_one_participant() {
        let store = store_over(Arc::new(MemoryBackend::new()));
        let first = TxId::from_bytes(b"first".to_vec());
        let second = TxId::from_bytes(b"second".to_vec());
        for participant in [&first, &second] {
            store
                .write(
                    &db_prefix(),
                    &intent_id(1),
                    &intent(participant, StructuralIntentPhase::Preparing),
                )
                .await
                .unwrap();
        }

        let intents = store
            .discover_for_participant(
                &db_prefix(),
                &first,
                Requirement::after(store.timeline.currentness_barrier()),
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

    #[derive(Clone, Copy)]
    enum Discovery {
        All,
        Page,
        Participant,
    }

    async fn discover(
        store: &StructuralIntentStore,
        participant: &TxId,
        discovery: Discovery,
        requirement: Requirement,
    ) -> Result<Vec<(StructuralIntentId, Observation<StructuralIntent>)>, StorageError> {
        match discovery {
            Discovery::All => store.discover(&db_prefix(), requirement).await,
            Discovery::Page => store
                .discover_page(&db_prefix(), None, requirement)
                .await
                .map(|page| page.intents),
            Discovery::Participant => {
                store
                    .discover_for_participant(&db_prefix(), participant, requirement)
                    .await
            }
        }
    }

    #[tokio::test]
    async fn discovery_reuses_present_bodies_without_advancing_evidence() {
        for discovery in [Discovery::All, Discovery::Page, Discovery::Participant] {
            for phase in [
                StructuralIntentPhase::Preparing,
                StructuralIntentPhase::Ready,
            ] {
                let recorder = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
                let operations = recorder.log();
                let store = store_over(recorder);
                let participant = TxId::from_bytes(b"participant".to_vec());
                let body = intent(&participant, phase);
                store
                    .write(&db_prefix(), &intent_id(1), &body)
                    .await
                    .unwrap();
                let requirement = Requirement::after(store.timeline.currentness_barrier());
                operations.lock().unwrap().clear();

                let found = discover(&store, &participant, discovery, requirement)
                    .await
                    .unwrap();
                assert_eq!(found.len(), 1);
                assert_eq!(found[0].1.value().map(Arc::as_ref), Some(&body));
                assert!(operations.lock().unwrap().iter().all(|op| op.op == "list"));

                // An absence requirement must not relabel present evidence.
                assert!(!found[0].1.satisfies(requirement));
            }
        }
    }

    #[tokio::test]
    async fn discovery_refreshes_listed_bodies_cached_as_absent_and_reports_read_errors() {
        for discovery in [Discovery::All, Discovery::Page, Discovery::Participant] {
            let memory = Arc::new(MemoryBackend::new());
            let hooks = HookBackend::new(memory.clone());
            let recorder = Arc::new(RecordingBackend::new(hooks.clone()));
            let operations = recorder.log();
            let timeline = Timeline::new();
            let objects = CachedStore::new(recorder, 1 << 20, timeline.clone(), None);
            let store = StructuralIntentStore::new(objects.clone());
            let participant = TxId::from_bytes(b"participant".to_vec());
            let path = ObjectPath::StructuralIntent {
                db_prefix: db_prefix(),
                participant: participant.clone(),
                intent_id: intent_id(1),
            };
            // Seed absence through the cache interface before a peer creates
            // the intent. Listing must not discard its newly visible name.
            assert!(
                !objects
                    .typed::<StructuralIntent>()
                    .read(path, Requirement::ANY)
                    .await
                    .unwrap()
                    .exists()
            );
            let peer = store_over(memory);
            let body = intent(&participant, StructuralIntentPhase::Preparing);
            peer.write(&db_prefix(), &intent_id(1), &body)
                .await
                .unwrap();
            let requirement = Requirement::after(timeline.currentness_barrier());
            hooks.set_before(|op| {
                let fail = matches!(
                    op,
                    BackendOp::Read { .. } | BackendOp::ReadIfModified { .. }
                );
                Box::pin(async move {
                    if fail {
                        Err(backend::BackendError::Unavailable(
                            "body unavailable".into(),
                        ))
                    } else {
                        Ok(())
                    }
                })
            });
            let result = discover(&store, &participant, discovery, requirement).await;
            assert!(matches!(result, Err(StorageError::Unavailable(_))));

            hooks.clear_before();
            operations.lock().unwrap().clear();
            let found = discover(&store, &participant, discovery, requirement)
                .await
                .unwrap();
            assert_eq!(found.len(), 1);
            assert_eq!(found[0].1.value().map(Arc::as_ref), Some(&body));
            assert!(found[0].1.satisfies(requirement));
            let calls: Vec<_> = operations.lock().unwrap().iter().map(|op| op.op).collect();
            assert_eq!(calls, ["list", "read"]);
        }
    }

    #[tokio::test]
    async fn stale_preparing_discovery_cannot_delete_ready_and_does_not_pin_the_cache() {
        for discovery in [Discovery::All, Discovery::Page, Discovery::Participant] {
            let memory = Arc::new(MemoryBackend::new());
            let recorder = Arc::new(RecordingBackend::new(memory.clone()));
            let operations = recorder.log();
            let local = store_over(recorder);
            let peer = store_over(memory.clone());
            let participant = TxId::from_bytes(b"participant".to_vec());
            let preparing = intent(&participant, StructuralIntentPhase::Preparing);
            local
                .write(&db_prefix(), &intent_id(1), &preparing)
                .await
                .unwrap();
            let prior = peer.discover(&db_prefix(), Requirement::ANY).await.unwrap();
            let ready = intent(&participant, StructuralIntentPhase::Ready);
            assert!(peer.update(&prior[0].1, &ready).await.unwrap().is_some());

            let requirement = Requirement::after(local.timeline.currentness_barrier());
            operations.lock().unwrap().clear();
            let found = discover(&local, &participant, discovery, requirement)
                .await
                .unwrap();
            assert_eq!(found[0].1.value().map(Arc::as_ref), Some(&preparing));
            assert!(operations.lock().unwrap().iter().all(|op| op.op == "list"));
            assert!(matches!(
                local.delete(&found[0].1).await,
                Err(StorageError::Precondition)
            ));
            let verifier = store_over(memory);
            let current = verifier
                .discover(&db_prefix(), Requirement::ANY)
                .await
                .unwrap();
            assert_eq!(current[0].1.value().map(Arc::as_ref), Some(&ready));

            // A conflicting delete invalidates its exact cached revision, so
            // a later discovery can classify Ready instead of retrying forever.
            operations.lock().unwrap().clear();
            let found = discover(&local, &participant, discovery, requirement)
                .await
                .unwrap();
            assert_eq!(found[0].1.value().map(Arc::as_ref), Some(&ready));
            let calls: Vec<_> = operations.lock().unwrap().iter().map(|op| op.op).collect();
            assert_eq!(calls, ["list", "read"]);
            local.delete(&found[0].1).await.unwrap();
            assert!(
                peer.discover(&db_prefix(), Requirement::ANY)
                    .await
                    .unwrap()
                    .is_empty()
            );
        }
    }
}
