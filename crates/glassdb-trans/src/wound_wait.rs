//! Shared wound-wait policy for transaction lock holders.

use glassdb_data::TxId;
use glassdb_storage::transaction::TxCommitStatus;

use crate::error::TransError;
use crate::monitor::{HolderWait, Monitor, TxFinalStatus};

/// Result of applying wound-wait to one live holder.
pub(crate) enum Reclaim {
    Wounded,
    Wait,
}

/// Applies transaction priority to one pending holder without waiting.
pub(crate) async fn try_reclaim(
    monitor: &Monitor,
    requester: &TxId,
    holder: &TxId,
) -> Result<Reclaim, TransError> {
    if !requester.older(holder) {
        return Ok(Reclaim::Wait);
    }
    if monitor.preempt_tx(holder).await? == TxFinalStatus::Aborted {
        Ok(Reclaim::Wounded)
    } else {
        Ok(Reclaim::Wait)
    }
}

/// Resolves a conflicting holder according to transaction priority. Returns
/// `None` when the holder has no final status after one step of `wait`: the
/// caller then reads the object again.
pub(crate) async fn resolve_tx_conflict(
    wait: &HolderWait<'_>,
    requester: &TxId,
    holder: &TxId,
) -> Result<Option<TxFinalStatus>, TransError> {
    let monitor = wait.monitor();
    match monitor.tx_status(holder).await? {
        TxCommitStatus::Committed => Ok(Some(TxFinalStatus::Committed)),
        // The caller removes the holder with a CAS on the current object, so a
        // blind wound holds no committed effect there.
        TxCommitStatus::Aborted | TxCommitStatus::Wounded | TxCommitStatus::BlindWounded => {
            Ok(Some(TxFinalStatus::Aborted))
        }
        TxCommitStatus::Pending => {
            if matches!(
                try_reclaim(monitor, requester, holder).await?,
                Reclaim::Wounded
            ) {
                Ok(Some(TxFinalStatus::Aborted))
            } else {
                wait.final_status(holder).await
            }
        }
        TxCommitStatus::Unknown => wait.final_status(holder).await,
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use glassdb_backend::memory::MemoryBackend;
    use glassdb_backend::middleware::{OpLog, RecordingBackend};
    use glassdb_data::DbPrefix;

    use super::*;
    use crate::engine::{AssemblyFixture, EngineConfig};
    use crate::monitor::{OwnerAbortOutcome, TxRecoveryManifest};
    use crate::retry_timing::RetryTiming;

    /// A pending holder that one database instance owns, and a peer instance
    /// that records its backend operations.
    async fn pending_holder() -> (AssemblyFixture, AssemblyFixture, OpLog, TxId) {
        let recorded = Arc::new(RecordingBackend::new(Arc::new(MemoryBackend::new())));
        let ops = recorded.log();
        let fixture = |backend| {
            AssemblyFixture::new(
                backend,
                DbPrefix::try_from("db").unwrap(),
                &EngineConfig::default(),
            )
        };
        let owner = fixture(recorded.clone());
        let peer = fixture(recorded);
        let holder = TxId::with_priority(5, b"holder");
        owner
            .monitor
            .begin_persisted_tx(&holder, TxRecoveryManifest::default())
            .await
            .unwrap();
        ops.lock().unwrap().clear();
        (owner, peer, ops, holder)
    }

    fn op_names(ops: &OpLog) -> Vec<&'static str> {
        ops.lock().unwrap().iter().map(|op| op.op).collect()
    }

    // The wound expects the status read that found the holder pending.
    #[tokio::test]
    async fn an_older_requester_wounds_a_pending_holder_with_one_read() {
        let (_owner, peer, ops, holder) = pending_holder().await;
        let requester = TxId::with_priority(1, b"requester");

        let status = resolve_tx_conflict(&peer.monitor.holder_wait(), &requester, &holder)
            .await
            .unwrap();

        assert_eq!(status, Some(TxFinalStatus::Aborted));
        assert_eq!(op_names(&ops), ["read", "write_if"]);
    }

    // ADR-024: the poll of a holder on another instance stays short, so a
    // waiter learns of a final status soon also after a long wait. A wait
    // schedule capped at 5 s would learn of it up to 5 s late.
    #[tokio::test(start_paused = true)]
    async fn a_peer_learns_of_a_final_status_within_the_poll_cap_after_a_long_wait() {
        let (owner, peer, _ops, holder) = pending_holder().await;
        let monitor = peer.monitor.clone();
        let wait = tokio::spawn(async move { monitor.await_tx_final(&holder).await });
        tokio::time::sleep(Duration::from_secs(20)).await;
        assert!(!wait.is_finished(), "the holder is still pending");

        assert_eq!(
            owner.monitor.abort_owned_tx(&holder).await.unwrap(),
            OwnerAbortOutcome::Acknowledged
        );
        let cap = RetryTiming::default().holder_poll.max_interval;
        let status = tokio::time::timeout(cap + Duration::from_millis(10), wait)
            .await
            .expect("the peer learned of the final status after the poll cap")
            .unwrap()
            .unwrap();
        assert_eq!(status, TxFinalStatus::Aborted);
    }

    // The wait and its poller start from the status read that found the holder
    // pending. The next read comes after one backoff.
    #[tokio::test(start_paused = true)]
    async fn a_younger_requester_reads_a_pending_holder_once_before_it_polls() {
        let (_owner, peer, ops, holder) = pending_holder().await;
        let requester = TxId::with_priority(9, b"requester");

        let monitor = peer.monitor.clone();
        let wait = tokio::spawn(async move {
            resolve_tx_conflict(&monitor.holder_wait(), &requester, &holder).await
        });
        // Paused time advances only when every task waits. The poller then
        // waits for its first backoff, which is longer than this sleep.
        tokio::time::sleep(Duration::from_millis(1)).await;

        assert!(!wait.is_finished());
        assert_eq!(op_names(&ops), ["read"]);
        wait.abort();
    }
}
