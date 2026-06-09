//! Background task management. Spawns tasks that share a [`CancelToken`] cancelled
//! together on [`Background::close`], shaped after `tokio_util::task::TaskTracker`.
//! Long-running spawned tasks observe the token (via [`CancelToken::cancelled`])
//! to learn that the parent is shutting down.

use std::future::Future;
use std::sync::Mutex;

use crate::cancel::CancelToken;
use crate::rt::{self, JoinHandle};

/// Manages a set of background tasks cancelled together on close. Each task
/// receives a shared [`CancelToken`] it can observe to react to shutdown.
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

    /// Spawns `f` as a background task, passing it the manager's shared
    /// [`CancelToken`]. Returns `false` if the manager is already closed.
    pub fn go<F, Fut>(&self, f: F) -> bool
    where
        F: FnOnce(CancelToken) -> Fut + Send + 'static,
        Fut: Future<Output = ()> + Send + 'static,
    {
        let closed = self.closed.lock().unwrap();
        if *closed {
            return false;
        }
        let token = self.token.clone();
        let handle = rt::spawn(async move {
            f(token).await;
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
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[tokio::test]
    async fn runs_and_rejects_after_close() {
        let b = Background::new();
        let ran = Arc::new(AtomicBool::new(false));
        let r = ran.clone();
        assert!(b.go(move |_tok| async move {
            r.store(true, Ordering::SeqCst);
        }));
        b.close().await;
        assert!(ran.load(Ordering::SeqCst));
        assert!(!b.go(|_tok| async {}));
    }

    #[tokio::test]
    async fn cancels_tasks_on_close() {
        let b = Arc::new(Background::new());
        let done = Arc::new(AtomicUsize::new(0));
        let d = done.clone();
        b.go(move |tok| async move {
            tok.cancelled().await;
            d.fetch_add(1, Ordering::SeqCst);
        });
        // Give the task a chance to start.
        tokio::task::yield_now().await;
        b.close().await;
        assert_eq!(done.load(Ordering::SeqCst), 1);
    }
}
