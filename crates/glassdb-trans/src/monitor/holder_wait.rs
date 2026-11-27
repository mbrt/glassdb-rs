//! Bounded waits for lock holders (ADR-024).

use std::sync::Mutex;

use glassdb_concurr::Backoff;
use glassdb_data::TxId;

use super::{Monitor, TxFinalStatus};
use crate::error::TransError;

/// Waits for lock holders in bounded steps, so that the waiter reads the held
/// object again between two steps (ADR-024). A holder can leave the object
/// before this instance learns its final status, for example when GC deletes
/// its transaction record first.
pub(crate) struct HolderWait<'a> {
    monitor: &'a Monitor,
    backoff: Mutex<Backoff>,
}

impl<'a> HolderWait<'a> {
    pub(super) fn new(monitor: &'a Monitor) -> Self {
        Self {
            monitor,
            backoff: Mutex::new(monitor.inner.holder_poll.backoff()),
        }
    }

    /// Returns the monitor that this wait uses.
    pub(crate) fn monitor(&self) -> &'a Monitor {
        self.monitor
    }

    /// Waits for the final status of `holder` for at most the next delay of
    /// the backoff. Returns `None` when the holder has no final status yet:
    /// the caller then reads the object again at a fresh barrier. The backoff
    /// starts again after a final status.
    pub(crate) async fn final_status(
        &self,
        holder: &TxId,
    ) -> Result<Option<TxFinalStatus>, TransError> {
        let delay = self.backoff.lock().unwrap().next_delay();
        let status = tokio::select! {
            status = self.monitor.await_tx_final(holder) => Some(status?),
            _ = glassdb_concurr::rt::sleep(delay) => None,
        };
        if status.is_some() {
            *self.backoff.lock().unwrap() = self.monitor.inner.holder_poll.backoff();
        }
        Ok(status)
    }
}
