//! Shared wound-wait policy for transaction lock holders.

use glassdb_data::TxId;
use glassdb_storage::transaction::TxCommitStatus;

use crate::error::TransError;
use crate::monitor::{Monitor, TxFinalStatus};

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

/// Resolves a conflicting holder according to transaction priority.
pub(crate) async fn resolve_tx_conflict(
    monitor: &Monitor,
    requester: &TxId,
    holder: &TxId,
) -> Result<TxFinalStatus, TransError> {
    match monitor.tx_status(holder).await? {
        TxCommitStatus::Ok => Ok(TxFinalStatus::Committed),
        TxCommitStatus::Aborted | TxCommitStatus::Wounded => Ok(TxFinalStatus::Aborted),
        TxCommitStatus::Pending => {
            if matches!(
                try_reclaim(monitor, requester, holder).await?,
                Reclaim::Wounded
            ) {
                Ok(TxFinalStatus::Aborted)
            } else {
                monitor.await_tx_final(holder).await
            }
        }
        TxCommitStatus::Unknown => monitor.await_tx_final(holder).await,
    }
}
