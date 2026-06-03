//! Background task management. Ported from the Go `concurr.Background`: spawns
//! tasks that are cancelled together on [`Background::close`].

use std::future::Future;
use std::sync::Mutex;

use tokio::task::JoinHandle;

use crate::cancel::CancelToken;
use crate::ctx::Ctx;

/// Manages a set of background tasks cancelled together on close.
pub struct Background {
    token: CancelToken,
    handles: Mutex<Vec<JoinHandle<()>>>,
    closed: Mutex<bool>,
}

impl Background {
    /// Creates a new background task manager.
    pub fn new() -> Self {
        Self {
            token: CancelToken::new(),
            handles: Mutex::new(Vec::new()),
            closed: Mutex::new(false),
        }
    }

    /// Spawns `f` as a background task, passing it a context whose cancellation
    /// is tied to this manager (but which preserves the parent's values).
    /// Returns `false` if the manager is already closed.
    pub fn go<F, Fut>(&self, ctx: &Ctx, f: F) -> bool
    where
        F: FnOnce(Ctx) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let closed = self.closed.lock().unwrap();
        if *closed {
            return false;
        }
        let child = ctx.with_new_cancel(self.token.clone());
        let handle = tokio::spawn(async move {
            f(child).await;
        });
        self.handles.lock().unwrap().push(handle);
        true
    }

    /// Signals all background tasks to stop and waits for them to finish.
    pub async fn close(&self) {
        {
            let mut c = self.closed.lock().unwrap();
            if *c {
                return;
            }
            *c = true;
        }
        self.token.cancel();
        let handles = std::mem::take(&mut *self.handles.lock().unwrap());
        for handle in handles {
            let _ = handle.await;
        }
    }
}

impl Default for Background {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
    use std::sync::Arc;

    #[tokio::test]
    async fn runs_and_rejects_after_close() {
        let b = Background::new();
        let ran = Arc::new(AtomicBool::new(false));
        let r = ran.clone();
        assert!(b.go(&Ctx::background(), move |_ctx| async move {
            r.store(true, Ordering::SeqCst);
        }));
        b.close().await;
        assert!(ran.load(Ordering::SeqCst));
        assert!(!b.go(&Ctx::background(), |_ctx| async {}));
    }

    #[tokio::test]
    async fn cancels_tasks_on_close() {
        let b = Arc::new(Background::new());
        let done = Arc::new(AtomicUsize::new(0));
        let d = done.clone();
        b.go(&Ctx::background(), move |ctx| async move {
            ctx.cancelled().await;
            d.fetch_add(1, Ordering::SeqCst);
        });
        // Give the task a chance to start.
        tokio::task::yield_now().await;
        b.close().await;
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }
}
