//! Delayed garbage collection of finalized transaction logs. Ported from the
//! Go `internal/trans/gc.go`.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use glassdb_concurr::rt::{self, Instant};
use glassdb_concurr::{Background, Ctx};
use glassdb_data::TxId;
use glassdb_storage::TLogger;

const CLEANUP_INTERVAL: Duration = Duration::from_secs(60);
const SIZE_LIMIT: usize = 1024;

struct CleanupItem {
    due: Instant,
    tid: TxId,
}

struct GcInner {
    bg: Arc<Background>,
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
    pub fn new(bg: Arc<Background>, tl: TLogger) -> Self {
        Gc {
            inner: Arc::new(GcInner {
                bg,
                tl,
                items: Mutex::new(Vec::new()),
            }),
        }
    }

    /// Starts the background cleanup loop.
    pub fn start(&self, ctx: &Ctx) {
        let g = self.clone();
        self.inner.bg.go(ctx, move |ctx| async move {
            // First cleanup happens only after one full interval (matching Go's
            // ticker, whose immediate first tick is skipped).
            loop {
                tokio::select! {
                    biased;
                    _ = ctx.cancelled() => return,
                    _ = rt::sleep(CLEANUP_INTERVAL) => g.cleanup_round(&ctx).await,
                }
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

    async fn cleanup_round(&self, ctx: &Ctx) {
        let now = Instant::now();
        let to_cleanup = self.filter_due_items(now);
        for item in to_cleanup {
            let _ = self.inner.tl.delete(ctx, &item.tid).await;
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
    use glassdb_backend::{memory::MemoryBackend, Backend};
    use glassdb_storage::{Global, Local, TxCommitStatus, TxLog};

    #[tokio::test(start_paused = true)]
    async fn gc_deletes_scheduled_log() {
        let b: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let local = Local::new(1024);
        let global = Global::new(b, local.clone());
        let tl = TLogger::new(global, local, "test");
        let bg = Arc::new(Background::new());
        let gc = Gc::new(bg, tl.clone());
        let ctx = Ctx::background();
        gc.start(&ctx);

        let tid = TxId::from_bytes(b"tx1".to_vec());
        tl.set(&ctx, &TxLog::new(tid.clone(), TxCommitStatus::Ok))
            .await
            .unwrap();
        assert!(tl.get(&ctx, &tid).await.is_ok());

        gc.schedule_tx_cleanup(tid.clone());

        // Wait for several cleanup intervals; the log should be deleted.
        tokio::time::sleep(CLEANUP_INTERVAL * 3).await;
        let err = tl.get(&ctx, &tid).await.unwrap_err();
        assert!(err.is_not_found(), "expected not-found, got {err:?}");
    }
}
