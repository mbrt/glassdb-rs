//! Delayed garbage collection of finalized transaction logs. Ported from the
//! Go `internal/trans/gc.go`.

use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use glassdb_concurr::Background;
use glassdb_concurr::rt::{self, Instant};
use glassdb_data::TxId;
use glassdb_storage::TLogger;

const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const SIZE_LIMIT: usize = 1024;

struct CleanupItem {
    due: Instant,
    tid: TxId,
}

struct GcInner {
    // Weak so a `Gc` clone captured inside the cleanup loop does not keep
    // [`Background`] alive past DB shutdown.
    bg: Weak<Background>,
    tl: TLogger,
    items: Mutex<Vec<CleanupItem>>,
}

/// Periodically garbage-collects finalized transaction logs that are no longer
/// needed.
#[derive(Clone)]
pub struct Gc {
    inner: Arc<GcInner>,
}

impl Gc {
    /// Creates a GC using the given background executor and logger.
    pub fn new(bg: Weak<Background>, tl: TLogger) -> Self {
        Gc {
            inner: Arc::new(GcInner {
                bg,
                tl,
                items: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Starts the background cleanup loop. The loop is aborted when its
    /// owning [`Background`] is dropped.
    pub fn start(&self) {
        let Some(bg) = self.inner.bg.upgrade() else {
            return;
        };
        let g = self.clone();
        bg.spawn(async move {
            // First cleanup happens only after one full interval (matching Go's
            // ticker, whose immediate first tick is skipped). The loop runs
            // until the owning `Background` is dropped, which aborts this
            // task at its next `.await`.
            loop {
                rt::sleep(CLEANUP_INTERVAL).await;
                g.cleanup_round().await;
            }
        });
    }

    /// Enqueues a transaction log for deletion after a delay.
    pub fn schedule_tx_cleanup(&self, tid: TxId) {
        let due = Instant::now() + CLEANUP_INTERVAL;
        let mut items = self.inner.items.lock().unwrap();
        if items.len() > SIZE_LIMIT {
            // Avoid growing indefinitely.
            return;
        }
        items.push(CleanupItem { due, tid });
    }

    async fn cleanup_round(&self) {
        let now = Instant::now();
        let to_cleanup = self.filter_due_items(now);
        for item in to_cleanup {
            let _ = self.inner.tl.delete(&item.tid).await;
        }
    }

    fn filter_due_items(&self, now: Instant) -> Vec<CleanupItem> {
        let mut items = self.inner.items.lock().unwrap();
        let mut i = 0;
        while i < items.len() {
            if items[i].due > now {
                break;
            }
            i += 1;
        }
        items.drain(0..i).collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use glassdb_backend::{Backend, memory::MemoryBackend};
    use glassdb_storage::{Global, Local, TxCommitStatus, TxLog};

    #[tokio::test(start_paused = true)]
    async fn gc_deletes_scheduled_log() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let local = Local::new(1024);
        let global = Global::new(b, local.clone());
        let tl = TLogger::new(global, local, "test");
        let bg = Arc::new(Background::new());
        let gc = Gc::new(Arc::downgrade(&bg), tl.clone());
        gc.start();

        let tid = TxId::from_bytes(b"tx1".to_vec());
        tl.set(TxLog::new(tid.clone(), TxCommitStatus::Ok))
            .await
            .unwrap();
        assert!(tl.get(&tid).await.is_ok());

        gc.schedule_tx_cleanup(tid.clone());

        // Wait for several cleanup intervals; the log should be deleted.
        tokio::time::sleep(CLEANUP_INTERVAL * 3).await;
        let err = tl.get(&tid).await.unwrap_err();
        assert!(err.is_not_found(), "expected not-found, got {err:?}");
    }
}
