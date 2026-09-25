//! Admission and departure of topology participants (ADR-049).

use glassdb_concurr::{RetryConfig, rt};
use glassdb_data::{CollectionAddress, TxId};
use glassdb_storage::transaction::{TxCommitStatus, TxLock, TxRecord};
use glassdb_storage::{CollectionStore, Requirement, StorageError};

use crate::error::TransError;
use crate::monitor::{Monitor, TxRecoveryManifest};

/// Adds and removes the topology participants of collection records.
#[derive(Clone)]
pub(super) struct TopologyMembership {
    records: CollectionStore,
    mon: Monitor,
    // Paces collection-record CAS retries. Transaction-status polling remains
    // entirely owned by Monitor.
    retry: RetryConfig,
}

impl TopologyMembership {
    pub(super) fn new(records: CollectionStore, mon: Monitor, retry: RetryConfig) -> Self {
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

    /// Admits `id` as a topology participant of `collection`.
    pub(super) async fn join(
        &self,
        collection: &CollectionAddress,
        id: &TxId,
    ) -> Result<(), TransError> {
        let mut backoff = self.retry.backoff();
        loop {
            let (mut record, observed) =
                match self.records.load_record(collection, Requirement::ANY).await {
                    Ok(record) => record,
                    Err(StorageError::NotFound) => return Err(TransError::StaleCollection),
                    Err(error) => return Err(error.into()),
                };
            if record
                .topology_participants()
                .any(|participant| participant == id)
            {
                // This is the same change's admission; its identity is not
                // reused after departure. New admission requires the CAS below.
                return Ok(());
            }
            if let Some(holder) = record.topology_freeze() {
                return match self.mon.tx_status(holder).await? {
                    TxCommitStatus::Aborted | TxCommitStatus::Wounded => {
                        let holder = *holder;
                        record.remove_topology_freeze(&holder);
                        if self.records.store_record(&record, &observed).await? {
                            continue;
                        }
                        rt::sleep(backoff.next_delay()).await;
                        continue;
                    }
                    TxCommitStatus::Committed => Err(TransError::StaleCollection),
                    TxCommitStatus::Pending | TxCommitStatus::Unknown => Err(TransError::Retry),
                };
            }
            if !record.add_topology_participant(*id) {
                return Err(TransError::Retry);
            }
            if self.records.store_record(&record, &observed).await? {
                return Ok(());
            }
            rt::sleep(backoff.next_delay()).await;
        }
    }

    /// Removes one participant after all of its structural intents settle.
    ///
    /// A present record without the participant must satisfy `requirement`.
    /// Local admission or topology-freeze evidence permits `ANY`.
    pub(super) async fn leave(
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

    /// Gives a topology participant its final status, so that recovery does
    /// not treat it as in-flight work.
    pub(super) async fn finalize(&self, collection: &CollectionAddress, id: &TxId) {
        let mut record = TxRecord::new(*id, TxCommitStatus::Committed);
        record.locks.push(TxLock::TopologyParticipant {
            collection: collection.clone(),
        });
        if let Err(e) = self.mon.commit_tx(record).await {
            tracing::debug!(
                target: "glassdb::restructurer",
                error = %e,
                "finalizing topology participant failed"
            );
        }
    }
}
