//! Durable structural-split recovery.

use std::collections::{BTreeSet, VecDeque};

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, DbRoot, NodeToken, ObjectPath, StructuralRecordId, TxId};
use glassdb_storage::transaction::TxCommitStatus;
use glassdb_storage::{
    CollectionStore, LockType, NodeStore, Observation, Requirement, StorageError, StructuralLog,
    StructuralLogPhase, StructuralLogStore, Timeline, TreeRouter,
};

use crate::error::TransError;
use crate::monitor::Monitor;

use super::{
    PARENT_RETRIES, ParentSplitContinuation, SeparatorPublication, SeparatorPublicationOutcome,
    SeparatorPublisher, StructuralNodeAccess,
};

/// Owns durable split discovery, fencing, classification, and settlement.
#[derive(Clone)]
pub(super) struct StructuralRecovery {
    records: CollectionStore,
    shards: NodeStore,
    structural_logs: StructuralLogStore,
    router: TreeRouter,
    mon: Monitor,
    structural_nodes: StructuralNodeAccess,
    publisher: SeparatorPublisher,
    timeline: Timeline,
    db_root: DbRoot,
    retry: RetryConfig,
}

/// One globally discovered batch and the participants represented in it.
pub(super) struct RecoverySweep {
    pub(super) active: bool,
    pub(super) records: Vec<(StructuralRecordId, Observation<StructuralLog>)>,
    pub(super) participants: BTreeSet<(CollectionAddress, TxId)>,
}

/// Resumable recovery of one durable structural record.
pub(super) struct RecordRecovery {
    observed: Observation<StructuralLog>,
    phase: RecordRecoveryPhase,
}

enum RecordRecoveryPhase {
    Classify,
    Publish {
        publication: SeparatorPublication,
        participant: TxId,
    },
    Delete,
}

/// An action the splitter must perform before record recovery can resume.
pub(super) enum RecordRecoveryStep {
    Completed,
    SplitParent { path: ObjectPath, participant: TxId },
}

/// Resumable settlement of one finalized topology participant.
pub(super) struct ParticipantSettlement {
    collection: CollectionAddress,
    participant: TxId,
    status_checked: bool,
    records: VecDeque<Observation<StructuralLog>>,
}

/// Work produced by one participant-settlement step.
pub(super) enum ParticipantSettlementStep {
    Completed,
    Recover(Observation<StructuralLog>),
}

impl StructuralRecovery {
    #[allow(clippy::too_many_arguments)]
    pub(super) fn new(
        records: CollectionStore,
        shards: NodeStore,
        structural_logs: StructuralLogStore,
        router: TreeRouter,
        mon: Monitor,
        structural_nodes: StructuralNodeAccess,
        publisher: SeparatorPublisher,
        timeline: Timeline,
        db_root: DbRoot,
        retry: RetryConfig,
    ) -> Self {
        Self {
            records,
            shards,
            structural_logs,
            router,
            mon,
            structural_nodes,
            publisher,
            timeline,
            db_root,
            retry,
        }
    }

    /// Discovers one fresh batch of unresolved records and their participants.
    pub(super) async fn scan(&self) -> Result<RecoverySweep, StorageError> {
        // Recovery has no transaction validation or preceding tree CAS. Capture
        // one sweep epoch for log discovery; each record's own freshness then
        // gates its source fencing and reachability.
        let recovery_start = Requirement::AtLeast(self.timeline.now());
        let records = self
            .structural_logs
            .list(&self.db_root, recovery_start)
            .await?;
        let active = !records.is_empty();
        let participants = records
            .iter()
            .filter_map(|(_, observed)| observed.value())
            .map(|record| (record.collection.clone(), record.participant_id.clone()))
            .collect();
        Ok(RecoverySweep {
            active,
            records,
            participants,
        })
    }

    /// Checks whether a discovered participant is eligible for settlement.
    pub(super) async fn participant_is_final(&self, id: &TxId) -> Result<bool, TransError> {
        Ok(self.mon.tx_status(id).await?.is_final())
    }

    /// Starts recovery of one exact structural-record observation.
    pub(super) fn begin_record(&self, observed: Observation<StructuralLog>) -> RecordRecovery {
        RecordRecovery {
            observed,
            phase: RecordRecoveryPhase::Classify,
        }
    }

    /// Advances one record until it completes or needs a recursive parent split.
    pub(super) async fn advance_record(
        &self,
        recovery: &mut RecordRecovery,
    ) -> Result<RecordRecoveryStep, TransError> {
        loop {
            match &mut recovery.phase {
                RecordRecoveryPhase::Classify => {
                    recovery.phase = self.classify_record(&recovery.observed).await?;
                }
                RecordRecoveryPhase::Publish {
                    publication,
                    participant,
                } => match self.publisher.publish(publication).await? {
                    SeparatorPublicationOutcome::Published => {
                        recovery.phase = RecordRecoveryPhase::Delete;
                    }
                    SeparatorPublicationOutcome::ParentRequiresSplit(action) => {
                        let path = action.path;
                        let participant = participant.clone();
                        if action.continuation == ParentSplitContinuation::CompletePublication {
                            recovery.phase = RecordRecoveryPhase::Delete;
                        }
                        return Ok(RecordRecoveryStep::SplitParent { path, participant });
                    }
                },
                RecordRecoveryPhase::Delete => {
                    self.structural_logs.delete(&recovery.observed).await?;
                    return Ok(RecordRecoveryStep::Completed);
                }
            }
        }
    }

    /// Starts settlement of one finalized topology participant.
    pub(super) fn begin_participant_settlement(
        &self,
        collection: &CollectionAddress,
        participant: &TxId,
    ) -> ParticipantSettlement {
        ParticipantSettlement {
            collection: collection.clone(),
            participant: participant.clone(),
            status_checked: false,
            records: VecDeque::new(),
        }
    }

    /// Advances participant settlement by one record or through final removal.
    pub(super) async fn advance_participant_settlement(
        &self,
        settlement: &mut ParticipantSettlement,
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
            if let Some(observed) = settlement.records.pop_front() {
                let record = observed.value().ok_or_else(|| {
                    TransError::other("structural record disappeared after listing")
                })?;
                if record.collection != settlement.collection {
                    return Err(TransError::other(
                        "topology participant owns records for multiple collections",
                    ));
                }
                return Ok(ParticipantSettlementStep::Recover(observed));
            }

            let records = self
                .structural_logs
                .list_for_participant(
                    settlement.collection.db_root_component(),
                    &settlement.participant,
                    Requirement::AtLeast(self.timeline.now()),
                )
                .await?;
            if records.is_empty() {
                self.leave_topology(&settlement.collection, &settlement.participant)
                    .await?;
                return Ok(ParticipantSettlementStep::Completed);
            }
            settlement.records = records.into_iter().map(|(_, observed)| observed).collect();
        }
    }

    /// Removes one participant after all of its durable structural work settles.
    pub(super) async fn leave_topology(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) =
                match self.records.load_record(collection, Requirement::Any).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Ok(()),
                    Err(error) => return Err(error.into()),
                };
            if !record.remove_topology_participant(id) {
                return Ok(());
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(());
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    async fn classify_record(
        &self,
        observed: &Observation<StructuralLog>,
    ) -> Result<RecordRecoveryPhase, TransError> {
        let record = observed
            .value()
            .ok_or_else(|| TransError::other("structural record disappeared after listing"))?
            .clone();
        if record.phase == StructuralLogPhase::Preparing {
            if self.mon.tx_status(&record.participant_id).await? == TxCommitStatus::Pending {
                return Err(TransError::Retry);
            }
            // Unknown is cancellable too: the pending transaction is persisted
            // before this intent, and cancellation only makes the worker's
            // Ready CAS lose. This also reclaims an intent whose transaction
            // tombstone was already collected before a late create appeared.
            return Ok(RecordRecoveryPhase::Delete);
        }

        // Pin fencing and reachability to the record's own freshness rather than
        // the listing epoch. The Ready transition follows source-gate
        // acquisition, so its watermark is at least as fresh as that gate.
        let requirement = Requirement::AtLeast(observed.current_after());
        let collection = &record.collection;
        let created_tokens = &record.created_tokens;
        if !self
            .fence_source_writer(collection, record.source_token.as_ref(), requirement)
            .await?
        {
            return Err(TransError::Retry);
        }

        let reachable = if record.is_root() {
            if created_tokens.len() != 2 {
                return Err(TransError::InvalidInput(
                    "root split record does not have two children".into(),
                ));
            }
            vec![
                self.router
                    .token_reachable_at_key(collection, &[], &created_tokens[0], requirement)
                    .await?,
                self.router
                    .token_reachable_at_key(
                        collection,
                        &record.split_key,
                        &created_tokens[1],
                        requirement,
                    )
                    .await?,
            ]
        } else {
            if created_tokens.len() != 1 {
                return Err(TransError::InvalidInput(
                    "non-root split record does not have one sibling".into(),
                ));
            }
            vec![
                self.router
                    .token_reachable_at_key(
                        collection,
                        &record.split_key,
                        &created_tokens[0],
                        requirement,
                    )
                    .await?,
            ]
        };
        let applied = reachable.iter().all(|reachable| *reachable);
        if applied && !record.is_root() {
            return Ok(RecordRecoveryPhase::Publish {
                publication: self.publisher.begin_publication(
                    collection,
                    &record.split_key,
                    &created_tokens[0],
                ),
                participant: record.participant_id.clone(),
            });
        }
        if !applied {
            for (token, reachable) in created_tokens.iter().zip(reachable) {
                if !reachable {
                    match self
                        .shards
                        .load_node_state(collection, token, requirement)
                        .await
                    {
                        Ok(node) => self.shards.delete_node(&node).await?,
                        Err(StorageError::NotFound) => {}
                        Err(error) => return Err(error.into()),
                    }
                }
            }
        }
        Ok(RecordRecoveryPhase::Delete)
    }

    /// Fences the source writer before classifying created-node reachability.
    async fn fence_source_writer(
        &self,
        collection: &CollectionAddress,
        token: Option<&NodeToken>,
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        for _ in 0..PARENT_RETRIES {
            let node = match token {
                Some(token) => match self.shards.load_node(collection, token, requirement).await {
                    Ok((node, _)) => node,
                    Err(StorageError::NotFound) => return Ok(true),
                    Err(error) => return Err(error.into()),
                },
                None => match self.shards.load_root_node(collection, requirement).await? {
                    Some((node, _)) => node,
                    None => return Ok(true),
                },
            };
            if node.structural_gate().lock_type() != LockType::Write {
                return Ok(true);
            }
            let Some(holder) = node.structural_gate().holders().first() else {
                return Ok(true);
            };
            if self.mon.tx_status(holder).await? == TxCommitStatus::Pending {
                return Ok(false);
            }
            // A finalized holder may still have a shrink CAS in flight. This
            // cleanup CAS either wins first, fencing that shrink, or loses to
            // it and the next iteration observes the landed right-link.
            self.structural_nodes
                .release_structural_gate(collection, token, holder)
                .await?;
        }
        Err(TransError::Retry)
    }
}
