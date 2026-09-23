//! Durable structural-intent lifecycle and recovery.

use std::collections::{BTreeSet, VecDeque};
use std::mem;
use std::sync::{Arc, Mutex};

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, DbPrefix, NodeToken, ObjectPath, StructuralIntentId, TxId};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{
    CollectionStore, CurrentnessBarrier, LeafObservation, LockType, Node, NodeStore, Observation,
    Requirement, StorageError, StructuralIntent, StructuralIntentPhase, StructuralIntentStore,
    Timeline, TreeRouter,
};

use crate::error::TransError;
use crate::monitor::Monitor;

use super::{
    PARENT_RETRIES, ParentSplitContinuation, SeparatorPublication, SeparatorPublicationOutcome,
    SeparatorPublisher, SplitReason, StructuralNodeAccess,
};

/// Owns the durable structural-intent lifecycle and recovery policy.
#[derive(Clone)]
pub(super) struct StructuralRecovery {
    records: CollectionStore,
    nodes: NodeStore,
    intent_store: StructuralIntentStore,
    router: TreeRouter,
    mon: Monitor,
    structural_nodes: StructuralNodeAccess,
    publisher: SeparatorPublisher,
    timeline: Timeline,
    db_prefix: DbPrefix,
    retry: RetryConfig,
    scan_cursor: Arc<Mutex<Option<glassdb_backend::ListCursor>>>,
}

/// Proves that one structural intent is still in its cancellable state.
pub(super) struct PreparedIntent {
    observed: Observation<StructuralIntent>,
    intent: StructuralIntent,
}

/// Retains the exact cancellable observation after split coordination starts.
pub(super) struct PreparedIntentCleanup {
    observed: Observation<StructuralIntent>,
}

/// Proves that a structural intent may require durable recovery.
pub(super) struct ReadyIntent {
    expected: Observation<StructuralIntent>,
    intent: StructuralIntent,
    observed: Option<Observation<StructuralIntent>>,
}

/// The result of advancing one prepared intent to its recoverable state.
pub(super) enum ReadyIntentTransition {
    Ready(ReadyIntent),
    RetryCleanly(TransError),
    RecoveryRequired(ReadyIntent, TransError),
}

/// The result of removing one completed recoverable intent.
pub(super) enum ReadyIntentCompletion {
    Completed,
    // Keep the common completion result small through Box; ReadyIntent carries
    // two observations and the full durable intent.
    RecoveryRequired(Box<ReadyIntent>, TransError),
}

/// One opaque, resumable durable-recovery operation.
pub(super) struct RecoveryAction {
    kind: RecoveryActionKind,
    parent_split: ParentSplitState,
}

enum RecoveryActionKind {
    Sweep(SweepAction),
    Settlement(SettlementAction),
}

struct SweepAction {
    scanned: bool,
    active: bool,
    failed: bool,
    completed: bool,
    intents: VecDeque<(StructuralIntentId, DiscoveredIntent)>,
    participants: VecDeque<(CollectionAddress, TxId)>,
    intent_id: Option<StructuralIntentId>,
    intent: Option<IntentRecovery>,
    participant: Option<TxId>,
    settlement: Option<ParticipantSettlement>,
}

struct SettlementAction {
    completed: bool,
    settlement: ParticipantSettlement,
    intent: Option<IntentRecovery>,
}

enum ParentSplitState {
    Idle,
    Awaiting,
    Resumed(Result<(), TransError>),
}

/// Work that the split scheduler must perform before recovery can resume.
pub(super) enum RecoveryStep {
    Completed {
        active: bool,
        failed: bool,
    },
    SplitParent {
        path: ObjectPath,
        participant: TxId,
        reason: SplitReason,
    },
}

struct SweepFailure {
    intent: Option<StructuralIntentId>,
    participant: Option<TxId>,
    error: TransError,
}

/// One globally discovered batch and the participants represented in it.
struct RecoverySweep {
    intents: Vec<(StructuralIntentId, Observation<StructuralIntent>)>,
    participants: BTreeSet<(CollectionAddress, TxId)>,
}

/// One candidate and its post-discovery classification bound.
///
/// Do not replace the observation while retaining its barrier. A later Ready
/// observation needs a bound captured after its own discovery.
struct DiscoveredIntent {
    observed: Observation<StructuralIntent>,
    barrier: CurrentnessBarrier,
}

/// Resumable recovery of one exact structural intent.
struct IntentRecovery {
    observed: Observation<StructuralIntent>,
    phase: IntentRecoveryPhase,
}

enum IntentRecoveryPhase {
    Classify(CurrentnessBarrier),
    Publish {
        publication: SeparatorPublication,
        participant: TxId,
    },
    Delete,
}

enum IntentRecoveryStep {
    Completed,
    SplitParent {
        path: ObjectPath,
        participant: TxId,
        reason: SplitReason,
    },
}

/// Resumable settlement of one finalized topology participant.
struct ParticipantSettlement {
    collection: CollectionAddress,
    participant: TxId,
    status_checked: bool,
    intents: VecDeque<DiscoveredIntent>,
}

#[derive(Clone, Copy)]
enum SettlementMode {
    Background,
    Explicit,
}

enum ParticipantSettlementStep {
    Completed,
    Recover(DiscoveredIntent),
}

impl PreparedIntent {
    /// Retains the authority needed to cancel this exact prepared intent.
    pub(super) fn cleanup_witness(&self) -> PreparedIntentCleanup {
        PreparedIntentCleanup {
            observed: self.observed.clone(),
        }
    }

    /// Tests whether this intent belongs to the expected split source.
    pub(super) fn targets(
        &self,
        collection: &CollectionAddress,
        source_token: Option<&NodeToken>,
    ) -> bool {
        self.intent.collection == *collection && self.intent.source_token.as_ref() == source_token
    }

    /// Returns the two child tokens reserved for a root split.
    pub(super) fn root_children(&self) -> Option<(&NodeToken, &NodeToken)> {
        self.intent.is_root().then(|| {
            (
                &self.intent.created_tokens[0],
                &self.intent.created_tokens[1],
            )
        })
    }

    /// Returns the sibling token reserved for a non-root split.
    pub(super) fn nonroot_sibling(&self) -> Option<&NodeToken> {
        (!self.intent.is_root()).then(|| &self.intent.created_tokens[0])
    }

    fn from_observation(observed: Observation<StructuralIntent>) -> Result<Self, TransError> {
        let intent = observed
            .value()
            .filter(|intent| {
                intent.phase == StructuralIntentPhase::Preparing
                    && if intent.is_root() {
                        intent.created_tokens.len() == 2
                    } else {
                        intent.source_token.is_some() && intent.created_tokens.len() == 1
                    }
            })
            .ok_or_else(|| TransError::other("invalid prepared structural intent"))?
            .as_ref()
            .clone();
        Ok(Self { observed, intent })
    }

    fn into_ready(self, source_revision: String, split_key: Vec<u8>) -> ReadyIntent {
        let mut intent = self.intent;
        intent.source_revision = source_revision;
        intent.split_key = split_key;
        intent.phase = StructuralIntentPhase::Ready;
        ReadyIntent {
            expected: self.observed,
            intent,
            observed: None,
        }
    }
}

impl ReadyIntent {
    /// Returns the topology participant that owns this intent.
    pub(super) fn participant(&self) -> &TxId {
        &self.intent.participant_id
    }

    fn confirm(&mut self, observed: Observation<StructuralIntent>) -> Result<(), TransError> {
        let matches = observed
            .value()
            .is_some_and(|intent| intent.as_ref() == &self.intent);
        if !matches {
            return Err(TransError::other(
                "Ready transition returned an unexpected structural intent",
            ));
        }
        self.observed = Some(observed);
        Ok(())
    }

    fn observation(&self) -> &Observation<StructuralIntent> {
        self.observed
            .as_ref()
            .expect("only an acknowledged Ready intent may create nodes")
    }
}

impl RecoveryAction {
    /// Supplies the result of the parent split requested by the last step.
    pub(super) fn resume_parent_split(&mut self, result: Result<(), TransError>) {
        assert!(
            matches!(self.parent_split, ParentSplitState::Awaiting),
            "recovery can resume a parent split only after requesting one"
        );
        self.parent_split = ParentSplitState::Resumed(result);
    }

    fn take_parent_result(&mut self) -> Result<Option<Result<(), TransError>>, TransError> {
        match mem::replace(&mut self.parent_split, ParentSplitState::Idle) {
            ParentSplitState::Idle => Ok(None),
            ParentSplitState::Awaiting => {
                self.parent_split = ParentSplitState::Awaiting;
                Err(TransError::other(
                    "recovery requires the requested parent split result",
                ))
            }
            ParentSplitState::Resumed(result) => Ok(Some(result)),
        }
    }
}

impl StructuralRecovery {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        records: CollectionStore,
        nodes: NodeStore,
        intent_store: StructuralIntentStore,
        router: TreeRouter,
        mon: Monitor,
        structural_nodes: StructuralNodeAccess,
        publisher: SeparatorPublisher,
        timeline: Timeline,
        db_prefix: DbPrefix,
        retry: RetryConfig,
    ) -> Self {
        Self {
            records,
            nodes,
            intent_store,
            router,
            mon,
            structural_nodes,
            publisher,
            timeline,
            db_prefix,
            retry,
            scan_cursor: Arc::new(Mutex::new(None)),
        }
    }

    /// Persists one prepared structural intent before its source can change.
    pub(super) async fn prepare_intent(
        &self,
        collection: &CollectionAddress,
        source_token: Option<&NodeToken>,
        participant: &TxId,
    ) -> Result<PreparedIntent, TransError> {
        let created_tokens = if source_token.is_none() {
            vec![NodeToken::new_random(), NodeToken::new_random()]
        } else {
            vec![NodeToken::new_random()]
        };
        let intent_id = StructuralIntentId::from(
            created_tokens
                .last()
                .expect("a split always reserves at least one token"),
        );
        let observed = self
            .intent_store
            .write(
                collection.db_prefix_component(),
                &intent_id,
                &StructuralIntent {
                    collection: collection.clone(),
                    source_token: source_token.cloned(),
                    source_revision: String::new(),
                    created_tokens,
                    split_key: Vec::new(),
                    participant_id: participant.clone(),
                    phase: StructuralIntentPhase::Preparing,
                },
            )
            .await?;
        PreparedIntent::from_observation(observed)
    }

    /// Advances a prepared intent while the source structural gate is held.
    pub(super) async fn mark_ready(
        &self,
        prepared: PreparedIntent,
        worker: &TxId,
        observation: &LeafObservation,
        split_key: Vec<u8>,
    ) -> ReadyIntentTransition {
        let source_revision = match observation.revision() {
            Some(revision) => revision.serialize().to_string(),
            None => {
                return ReadyIntentTransition::RetryCleanly(TransError::other(
                    "split source is absent",
                ));
            }
        };
        let collection = prepared.intent.collection.clone();
        let source_token = prepared.intent.source_token.clone();
        let mut ready = prepared.into_ready(source_revision, split_key);
        match self
            .intent_store
            .update(&ready.expected, &ready.intent)
            .await
        {
            Ok(Some(observed)) => match ready.confirm(observed) {
                Ok(()) => ReadyIntentTransition::Ready(ready),
                Err(error) => ReadyIntentTransition::RecoveryRequired(ready, error),
            },
            Ok(None) => {
                let error = match self
                    .structural_nodes
                    .release_structural_gate(&collection, source_token.as_ref(), worker)
                    .await
                {
                    Ok(()) => TransError::Retry,
                    Err(error) => error,
                };
                ReadyIntentTransition::RetryCleanly(error)
            }
            Err(error) => ReadyIntentTransition::RecoveryRequired(ready, error.into()),
        }
    }

    /// Deletes one exact prepared intent after clean cancellation.
    pub(super) async fn discard_prepared(
        &self,
        cleanup: &PreparedIntentCleanup,
    ) -> Result<(), TransError> {
        self.intent_store
            .delete(&cleanup.observed)
            .await
            .map_err(Into::into)
    }

    /// Deletes one acknowledged Ready intent after its tree change completes.
    pub(super) async fn complete_ready(&self, ready: ReadyIntent) -> ReadyIntentCompletion {
        match self.intent_store.delete(ready.observation()).await {
            Ok(()) => ReadyIntentCompletion::Completed,
            Err(error) => ReadyIntentCompletion::RecoveryRequired(Box::new(ready), error.into()),
        }
    }

    /// Reports whether a structural-intent traversal remains incomplete.
    pub(super) fn has_continuation(&self) -> bool {
        self.scan_cursor.lock().unwrap().is_some()
    }

    /// Starts one bounded background sweep of unresolved structural intents.
    pub(super) fn begin_sweep(&self) -> RecoveryAction {
        RecoveryAction {
            kind: RecoveryActionKind::Sweep(SweepAction {
                scanned: false,
                active: false,
                failed: false,
                completed: false,
                intents: VecDeque::new(),
                participants: VecDeque::new(),
                intent_id: None,
                intent: None,
                participant: None,
                settlement: None,
            }),
            parent_split: ParentSplitState::Idle,
        }
    }

    /// Starts explicit settlement of one finalized topology participant.
    ///
    /// The caller must share the cache that admitted the participant or
    /// installed a topology freeze with the participant still present.
    pub(super) fn begin_participant_settlement(
        &self,
        collection: &CollectionAddress,
        participant: &TxId,
    ) -> RecoveryAction {
        RecoveryAction {
            kind: RecoveryActionKind::Settlement(SettlementAction {
                completed: false,
                settlement: Self::new_participant_settlement(collection, participant),
                intent: None,
            }),
            parent_split: ParentSplitState::Idle,
        }
    }

    /// Advances recovery until it completes or requests one recursive split.
    pub(super) async fn advance(
        &self,
        action: &mut RecoveryAction,
    ) -> Result<RecoveryStep, TransError> {
        let parent_result = action.take_parent_result()?;
        let RecoveryAction { kind, parent_split } = action;
        match kind {
            RecoveryActionKind::Sweep(sweep) => {
                self.advance_sweep(sweep, parent_split, parent_result).await
            }
            RecoveryActionKind::Settlement(settlement) => {
                self.advance_settlement(settlement, parent_split, parent_result)
                    .await
            }
        }
    }

    /// Removes one participant after all of its structural intents settle.
    ///
    /// A present record without the participant must satisfy `requirement`.
    /// Local admission or topology-freeze evidence permits `ANY`.
    pub(super) async fn leave_topology(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
        requirement: Requirement,
    ) -> Result<(), TransError> {
        let mut backoff = self.retry.backoff();
        let mut read_requirement = Requirement::ANY;
        loop {
            let (mut record, observed) =
                match self.records.load_record(collection, read_requirement).await {
                    Ok(record) => record,
                    // Published collections already have their record. Local
                    // preparation shares this cache, and deleted identities are
                    // not reused, so an absence cannot hide later admission.
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(error) => return Err(error.into()),
                };
            if !record.remove_topology_participant(id) {
                if observed.satisfies(requirement) {
                    return Ok(());
                }
                // Intent cleanup does not refresh the collection record. Only
                // a no-op without sufficient evidence needs a bounded reload.
                read_requirement = requirement;
                continue;
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(());
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn advance_sweep(
        &self,
        sweep: &mut SweepAction,
        parent_split: &mut ParentSplitState,
        mut parent_result: Option<Result<(), TransError>>,
    ) -> Result<RecoveryStep, TransError> {
        if sweep.completed {
            return Ok(RecoveryStep::Completed {
                active: sweep.active,
                failed: sweep.failed,
            });
        }
        if parent_result.is_some() && sweep.intent.is_none() {
            return Err(TransError::other(
                "recovery received a parent split result without a pending intent",
            ));
        }
        if !sweep.scanned {
            let discovered = self.scan().await?;
            sweep.scanned = true;
            sweep.intents = self.begin_intents(discovered.intents).collect();
            sweep.participants = discovered.participants.into_iter().collect();
        }

        loop {
            match self
                .advance_sweep_once(sweep, parent_split, parent_result.take())
                .await
            {
                Ok(Some(step)) => return Ok(step),
                Ok(None) => {}
                Err(SweepFailure {
                    intent,
                    participant,
                    error,
                }) => {
                    sweep.failed |= !matches!(error, TransError::Retry);
                    tracing::debug!(
                        target: "glassdb::splitter",
                        intent = ?intent,
                        participant = ?participant,
                        error = %error,
                        "structural recovery deferred"
                    );
                }
            }
        }
    }

    /// Advances one item in a best-effort structural-recovery sweep.
    async fn advance_sweep_once(
        &self,
        sweep: &mut SweepAction,
        parent_split: &mut ParentSplitState,
        parent_result: Option<Result<(), TransError>>,
    ) -> Result<Option<RecoveryStep>, SweepFailure> {
        if let Some(intent) = sweep.intent.as_mut() {
            match self.advance_intent(intent, parent_result).await {
                Ok(IntentRecoveryStep::Completed) => {
                    sweep.active = true;
                    sweep.intent = None;
                    if sweep.settlement.is_none() {
                        sweep.intent_id = None;
                    }
                    return Ok(None);
                }
                Ok(IntentRecoveryStep::SplitParent {
                    path,
                    participant,
                    reason,
                }) => {
                    *parent_split = ParentSplitState::Awaiting;
                    return Ok(Some(RecoveryStep::SplitParent {
                        path,
                        participant,
                        reason,
                    }));
                }
                Err(error) => {
                    let failure = SweepFailure {
                        intent: sweep.intent_id.clone(),
                        participant: sweep.participant.clone(),
                        error,
                    };
                    if sweep.participant.is_some() {
                        sweep.settlement = None;
                        sweep.participant = None;
                    } else {
                        sweep.intent_id = None;
                    }
                    sweep.intent = None;
                    return Err(failure);
                }
            }
        }
        debug_assert!(parent_result.is_none());

        if let Some(settlement) = sweep.settlement.as_mut() {
            match self
                .advance_participant(settlement, SettlementMode::Background)
                .await
            {
                Ok(ParticipantSettlementStep::Completed) => {
                    sweep.settlement = None;
                    sweep.participant = None;
                }
                Ok(ParticipantSettlementStep::Recover(intent)) => {
                    sweep.intent = Some(Self::begin_intent(intent));
                }
                Err(error) => {
                    let failure = SweepFailure {
                        intent: None,
                        participant: sweep.participant.clone(),
                        error,
                    };
                    sweep.settlement = None;
                    sweep.participant = None;
                    return Err(failure);
                }
            }
            return Ok(None);
        }

        if let Some((intent_id, intent)) = sweep.intents.pop_front() {
            sweep.intent_id = Some(intent_id);
            sweep.intent = Some(Self::begin_intent(intent));
            return Ok(None);
        }

        if let Some((collection, participant)) = sweep.participants.pop_front() {
            match self.participant_is_final(&participant).await {
                Ok(false) => {}
                Ok(true) => {
                    sweep.settlement =
                        Some(Self::new_participant_settlement(&collection, &participant));
                    sweep.participant = Some(participant);
                }
                Err(error) => {
                    return Err(SweepFailure {
                        intent: None,
                        participant: Some(participant),
                        error,
                    });
                }
            }
            return Ok(None);
        }

        sweep.completed = true;
        Ok(Some(RecoveryStep::Completed {
            active: sweep.active,
            failed: sweep.failed,
        }))
    }

    async fn advance_settlement(
        &self,
        action: &mut SettlementAction,
        parent_split: &mut ParentSplitState,
        mut parent_result: Option<Result<(), TransError>>,
    ) -> Result<RecoveryStep, TransError> {
        if action.completed {
            return Ok(RecoveryStep::Completed {
                active: false,
                failed: false,
            });
        }
        if parent_result.is_some() && action.intent.is_none() {
            return Err(TransError::other(
                "settlement received a parent split result without a pending intent",
            ));
        }

        loop {
            if let Some(intent) = action.intent.as_mut() {
                match self.advance_intent(intent, parent_result.take()).await? {
                    IntentRecoveryStep::Completed => {
                        action.intent = None;
                    }
                    IntentRecoveryStep::SplitParent {
                        path,
                        participant,
                        reason,
                    } => {
                        *parent_split = ParentSplitState::Awaiting;
                        return Ok(RecoveryStep::SplitParent {
                            path,
                            participant,
                            reason,
                        });
                    }
                }
                continue;
            }

            match self
                .advance_participant(&mut action.settlement, SettlementMode::Explicit)
                .await?
            {
                ParticipantSettlementStep::Completed => {
                    action.completed = true;
                    return Ok(RecoveryStep::Completed {
                        active: false,
                        failed: false,
                    });
                }
                ParticipantSettlementStep::Recover(intent) => {
                    action.intent = Some(Self::begin_intent(intent));
                }
            }
        }
    }

    async fn scan(&self) -> Result<RecoverySweep, StorageError> {
        // Only absent discovery bodies need this bound; present candidates
        // use their exact revisions and phase rules. Ready classification
        // still needs its own barrier after observing the intent.
        let recovery_start = Requirement::after(self.timeline.currentness_barrier());
        let cursor = self.scan_cursor.lock().unwrap().clone();
        let page = match self
            .intent_store
            .discover_page(&self.db_prefix, cursor.as_ref(), recovery_start)
            .await
        {
            Ok(page) => page,
            Err(StorageError::InvalidCursor) => {
                *self.scan_cursor.lock().unwrap() = None;
                return Err(StorageError::InvalidCursor);
            }
            Err(error) => return Err(error),
        };
        *self.scan_cursor.lock().unwrap() = page.next;
        let intents = page.intents;
        let participants = intents
            .iter()
            .filter_map(|(_, observed)| observed.value())
            .map(|intent| (intent.collection.clone(), intent.participant_id.clone()))
            .collect();
        Ok(RecoverySweep {
            intents,
            participants,
        })
    }

    async fn participant_is_final(&self, id: &TxId) -> Result<bool, TransError> {
        Ok(self.mon.tx_status(id).await?.is_final())
    }

    fn begin_intents(
        &self,
        intents: Vec<(StructuralIntentId, Observation<StructuralIntent>)>,
    ) -> impl Iterator<Item = (StructuralIntentId, DiscoveredIntent)> {
        // Every Ready body is already in hand and stays fixed until deletion.
        // One post-discovery barrier can therefore cover this entire batch.
        // Later discoveries must pass through here again, including intents
        // created by recursive recovery under an already-final participant.
        // Neither the discovery bound nor an observation's watermark proves
        // that a source read follows the gate recorded by a Ready intent.
        let barrier = self.timeline.currentness_barrier();
        intents
            .into_iter()
            .map(move |(id, observed)| (id, DiscoveredIntent { observed, barrier }))
    }

    fn begin_intent(intent: DiscoveredIntent) -> IntentRecovery {
        IntentRecovery {
            observed: intent.observed,
            phase: IntentRecoveryPhase::Classify(intent.barrier),
        }
    }

    async fn advance_intent(
        &self,
        recovery: &mut IntentRecovery,
        parent_result: Option<Result<(), TransError>>,
    ) -> Result<IntentRecoveryStep, TransError> {
        if let Some(result) = parent_result {
            result?;
        }
        loop {
            match &mut recovery.phase {
                IntentRecoveryPhase::Classify(barrier) => {
                    recovery.phase = self.classify_intent(&recovery.observed, *barrier).await?;
                }
                IntentRecoveryPhase::Publish {
                    publication,
                    participant,
                } => match self.publisher.publish(publication).await? {
                    SeparatorPublicationOutcome::Published => {
                        recovery.phase = IntentRecoveryPhase::Delete;
                    }
                    SeparatorPublicationOutcome::ParentRequiresSplit(action) => {
                        let path = action.path;
                        let participant = participant.clone();
                        if action.continuation == ParentSplitContinuation::CompletePublication {
                            recovery.phase = IntentRecoveryPhase::Delete;
                        }
                        return Ok(IntentRecoveryStep::SplitParent {
                            path,
                            participant,
                            reason: action.reason,
                        });
                    }
                },
                IntentRecoveryPhase::Delete => {
                    match self.intent_store.delete(&recovery.observed).await {
                        Ok(()) => {}
                        // Discovery can retain Preparing after a peer made it
                        // Ready. The failed deletion invalidates that exact
                        // cached revision; retry must discover and classify it
                        // again instead of exposing a storage precondition.
                        Err(StorageError::Precondition) => return Err(TransError::Retry),
                        Err(error) => return Err(error.into()),
                    }
                    return Ok(IntentRecoveryStep::Completed);
                }
            }
        }
    }

    fn new_participant_settlement(
        collection: &CollectionAddress,
        participant: &TxId,
    ) -> ParticipantSettlement {
        ParticipantSettlement {
            collection: collection.clone(),
            participant: participant.clone(),
            status_checked: false,
            intents: VecDeque::new(),
        }
    }

    async fn advance_participant(
        &self,
        settlement: &mut ParticipantSettlement,
        mode: SettlementMode,
    ) -> Result<ParticipantSettlementStep, TransError> {
        if !settlement.status_checked {
            if !self
                .mon
                .tx_status(&settlement.participant)
                .await?
                .is_final()
            {
                return Err(TransError::Retry);
            }
            settlement.status_checked = true;
        }

        loop {
            if let Some(recovery) = settlement.intents.pop_front() {
                let intent = recovery.observed.value().ok_or_else(|| {
                    TransError::other("structural intent disappeared after listing")
                })?;
                if intent.collection != settlement.collection {
                    return Err(TransError::other(
                        "topology participant owns intents for multiple collections",
                    ));
                }
                return Ok(ParticipantSettlementStep::Recover(recovery));
            }

            // Present discovery bodies already use ANY. Advancing this bound
            // rechecks insufficient absence after intervening recovery and
            // also supplies the background departure proof below.
            let requirement = Requirement::after(self.timeline.currentness_barrier());
            let intents = self
                .intent_store
                .discover_for_participant(
                    settlement.collection.db_prefix_component(),
                    &settlement.participant,
                    requirement,
                )
                .await?;
            if intents.is_empty() {
                // The listing bound follows final status and completed intent
                // recovery. Explicit settlement already has local record
                // evidence from admission or its topology freeze.
                let departure = match mode {
                    SettlementMode::Background => requirement,
                    SettlementMode::Explicit => Requirement::ANY,
                };
                self.leave_topology(&settlement.collection, &settlement.participant, departure)
                    .await?;
                return Ok(ParticipantSettlementStep::Completed);
            }
            settlement.intents = self
                .begin_intents(intents)
                .map(|(_, intent)| intent)
                .collect();
        }
    }

    async fn classify_intent(
        &self,
        observed: &Observation<StructuralIntent>,
        barrier: CurrentnessBarrier,
    ) -> Result<IntentRecoveryPhase, TransError> {
        let intent = observed
            .value()
            .ok_or_else(|| TransError::other("structural intent disappeared after listing"))?
            .clone();
        if intent.phase == StructuralIntentPhase::Preparing {
            // Cached Preparing can hide Ready while the participant is live;
            // its owner drives publication without waiting for this sweep.
            if self.mon.tx_status(&intent.participant_id).await? == TxCommitStatus::Pending {
                return Err(TransError::Retry);
            }
            // Unknown is cancellable too: the pending transaction is persisted
            // before this intent, and cancellation only makes the worker's
            // Ready CAS lose. This also reclaims an intent whose transaction
            // tombstone was collected before a late create appeared.
            return Ok(IntentRecoveryPhase::Delete);
        }

        if intent.source_revision.is_empty() {
            // A Ready transition records the revision its worker publishes
            // from. Without it there is nothing to fence against, so the intent
            // must not be classified at all.
            return Err(TransError::other(
                "Ready structural intent records no source revision",
            ));
        }

        // The batch barrier follows every Ready observation in this batch.
        // Each worker made its gated source durable before writing Ready, so
        // this bound excludes source evidence from before that gate.
        let collection = &intent.collection;
        let created_tokens = &intent.created_tokens;
        if !self
            .fence_source_writer(
                collection,
                intent.source_token.as_ref(),
                &intent.source_revision,
                barrier,
            )
            .await?
        {
            return Err(TransError::Retry);
        }

        let reachable = if intent.is_root() {
            if created_tokens.len() != 2 {
                return Err(TransError::InvalidInput(
                    "root split intent does not have two children".into(),
                ));
            }
            vec![
                self.router
                    .token_reachable_at_key(
                        collection,
                        &[],
                        &created_tokens[0],
                        Requirement::after(barrier),
                    )
                    .await?,
                self.router
                    .token_reachable_at_key(
                        collection,
                        &intent.split_key,
                        &created_tokens[1],
                        Requirement::after(barrier),
                    )
                    .await?,
            ]
        } else {
            if created_tokens.len() != 1 {
                return Err(TransError::InvalidInput(
                    "non-root split intent does not have one sibling".into(),
                ));
            }
            vec![
                self.router
                    .token_reachable_at_key(
                        collection,
                        &intent.split_key,
                        &created_tokens[0],
                        Requirement::after(barrier),
                    )
                    .await?,
            ]
        };
        let applied = reachable.iter().all(|reachable| *reachable);
        if applied && !intent.is_root() {
            return Ok(IntentRecoveryPhase::Publish {
                publication: self.publisher.begin_publication(
                    collection,
                    &intent.split_key,
                    &created_tokens[0],
                ),
                participant: intent.participant_id.clone(),
            });
        }
        if !applied {
            for (token, reachable) in created_tokens.iter().zip(reachable) {
                if !reachable {
                    match self
                        .nodes
                        .load_node_state(collection, token, Requirement::after(barrier))
                        .await
                    {
                        Ok(node) => self.nodes.delete_node(&node).await?,
                        Err(StorageError::NotFound) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
        Ok(IntentRecoveryPhase::Delete)
    }

    /// Fences the worker that recorded `source_revision` before classifying
    /// created-node reachability.
    ///
    /// That revision is the whole question. A worker publishes its split with
    /// one CAS expecting it and never re-reads the source in between, so while
    /// the source still carries it the publish can still land, and once it does
    /// not the publish can never land again. The structural gate answers a
    /// weaker question: it cannot tell this intent's worker from a later one
    /// that gated the same source after this intent was abandoned.
    async fn fence_source_writer(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        source_revision: &str,
        barrier: CurrentnessBarrier,
    ) -> Result<bool, TransError> {
        for _ in 0..PARENT_RETRIES {
            let Some((node, observed)) = self.load_source(collection, token, barrier).await? else {
                return Ok(true);
            };
            if !observed
                .revision()
                .is_some_and(|revision| revision.serialize() == source_revision)
            {
                return Ok(true);
            }
            // The worker recorded this revision while it held the gate, so a
            // source still at it must still carry that gate.
            let gate = node.structural_gate();
            let holder = (gate.lock_type() == LockType::Write)
                .then(|| gate.holders().first())
                .flatten()
                .ok_or_else(|| {
                    TransError::other("split source is at its recorded revision without a gate")
                })?;
            if self.mon.tx_status(holder).await? == TxCommitStatus::Pending {
                return Ok(false);
            }
            // A finalized holder can still have its publish CAS in flight. This
            // cleanup CAS either wins first and fences that publish, or loses
            // and the next iteration sees the source past `source_revision`.
            self.structural_nodes
                .release_structural_gate(collection, token, holder)
                .await?;
        }
        Err(TransError::Retry)
    }

    async fn load_source(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        barrier: CurrentnessBarrier,
    ) -> Result<Option<(Node, LeafObservation)>, TransError> {
        match token {
            Some(token) => match self
                .nodes
                .load_node(collection, token, Requirement::after(barrier))
                .await
            {
                Ok(source) => Ok(Some(source)),
                Err(StorageError::NotFound) => Ok(None),
                Err(error) => Err(error.into()),
            },
            None => Ok(self
                .nodes
                .load_root_node(collection, Requirement::after(barrier))
                .await?),
        }
    }
}
