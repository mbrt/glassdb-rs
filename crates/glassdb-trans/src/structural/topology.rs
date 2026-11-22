//! Admission and departure of topology participants (ADR-049).

use std::future::ready;

use glassdb_concurr::RetrySchedule;
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxRecord};
use glassdb_storage::{CasRetries, CollectionStore, Converged, Decision, Requirement};

use crate::error::TransError;
use crate::monitor::{Monitor, OwnerAbortOutcome, TxRecoveryManifest};

/// Adds and removes the topology participants of collection records.
#[derive(Clone)]
pub(crate) struct TopologyMembership {
    records: CollectionStore,
    mon: Monitor,
    // Paces collection-record CAS retries. Transaction-status polling remains
    // entirely owned by Monitor.
    retry: RetrySchedule,
}

impl TopologyMembership {
    pub(crate) fn new(records: CollectionStore, mon: Monitor, retry: RetrySchedule) -> Self {
        Self {
            records,
            mon,
            retry,
        }
    }

    /// Persists the recovery manifest of a topology participant.
    pub(super) async fn begin(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        self.mon
            .begin_persisted_tx(
                id,
                TxRecoveryManifest {
                    locks: vec![TxLock::TopologyParticipant {
                        collection: collection.clone(),
                    }],
                    ..TxRecoveryManifest::default()
                },
            )
            .await
    }

    /// Admits `ids` as topology participants of `collection`, with one CAS
    /// for all of them.
    pub(super) async fn join(
        &self,
        collection: &CollectionAddress,
        ids: &[TxId],
    ) -> Result<(), TransError> {
        let joined = self
            .records
            .converge(
                collection,
                Requirement::ANY,
                CasRetries::Unbounded(self.retry),
                |mut record, _| async move {
                    // Proof: shared local knowledge (same cache, one
                    // acquisition, final before foreign removal). An admitted
                    // participant is the admission of this change. A new
                    // admission needs the CAS below.
                    let missing: Vec<TxId> = ids
                        .iter()
                        .filter(|id| {
                            !record
                                .topology_participants()
                                .any(|admitted| admitted == *id)
                        })
                        .copied()
                        .collect();
                    if missing.is_empty() {
                        return Ok(Decision::Done(()));
                    }
                    if let Some(holder) = record.topology_freeze().copied() {
                        return match self.mon.tx_status(&holder).await? {
                            TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                                record.remove_topology_freeze(&holder);
                                Ok(Decision::Advance(record))
                            }
                            TxCommitStatus::Committed => Err(TransError::StaleCollection),
                            TxCommitStatus::Pending | TxCommitStatus::Unknown => {
                                Err(TransError::Retry)
                            }
                        };
                    }
                    for id in missing {
                        if !record.add_topology_participant(id) {
                            return Err(TransError::Retry);
                        }
                    }
                    Ok::<_, TransError>(Decision::Write(record, ()))
                },
            )
            .await?;
        match joined {
            Converged::Written { .. } | Converged::Done(()) => Ok(()),
            // Proof: publication and lifecycle (created before seen). A cached
            // absence of the record comes after the drop of the collection.
            Converged::Missing => Err(TransError::StaleCollection),
            Converged::Stop(()) | Converged::Exhausted => Err(TransError::Retry),
        }
    }

    /// Removes participants whose structural intents have settled, with one
    /// CAS for all of them.
    ///
    /// Returns whether a removal CAS applied. A record without any of the
    /// participants, or a missing record, must satisfy `requirement`. Local
    /// admission or topology-freeze evidence permits `ANY`.
    pub(crate) async fn leave(
        &self,
        collection: &CollectionAddress,
        ids: &[TxId],
        requirement: Requirement,
    ) -> Result<bool, TransError> {
        let left = self
            .records
            .converge(
                collection,
                requirement,
                CasRetries::Unbounded(self.retry),
                |mut record, _| {
                    let mut removed = false;
                    for id in ids {
                        removed |= record.remove_topology_participant(id);
                    }
                    ready(Ok::<_, TransError>(if removed {
                        Decision::Write(record, ())
                    } else {
                        Decision::Done(())
                    }))
                },
            )
            .await?;
        match left {
            Converged::Written { .. } => Ok(true),
            Converged::Done(()) | Converged::Missing => Ok(false),
            Converged::Stop(()) | Converged::Exhausted => Err(TransError::Retry),
        }
    }

    /// Gives a topology participant its final status, so that recovery does
    /// not treat it as in-flight work. If a peer wounded the participant, this
    /// acknowledges the wound, so that GC can delete the record (ADR-059).
    pub(super) async fn finalize(&self, collection: &CollectionAddress, id: &TxId) {
        let mut record = TxRecord::new(*id, TxCommitStatus::Committed);
        record.locks.push(TxLock::TopologyParticipant {
            collection: collection.clone(),
        });
        let result = match self.mon.commit_tx(record).await {
            Err(TransError::AlreadyFinalized) => match self.mon.abort_owned_tx(id).await {
                Ok(OwnerAbortOutcome::Pinned) => {
                    tracing::debug!(
                        target: "glassdb::restructurer",
                        participant = %id,
                        "wound of topology participant stays pinned"
                    );
                    Ok(())
                }
                result => result.map(|_| ()),
            },
            result => result,
        };
        if let Err(e) = result {
            tracing::debug!(
                target: "glassdb::restructurer",
                error = %e,
                "finalizing topology participant failed"
            );
        }
    }
}
