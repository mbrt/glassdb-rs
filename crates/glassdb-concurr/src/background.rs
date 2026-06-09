//! Background task management.
//!
//! [`Background`] is an owning collection of detached tasks. Each task is
//! spawned with [`Background::spawn`] and the only way it observes shutdown
//! is by being aborted at its next `.await` point: dropping `Background`
//! aborts every spawned task. Long-running tasks are expected to be written
//! as loops that `.await` between iterations, so abort is granular.

use std::future::Future;
use std::sync::Mutex;

use crate::rt::{self, JoinHandle};

/// Manages a set of background tasks. When the `Background` is dropped, every
/// tracked task is aborted; the abort fires at the task's next `.await`.
pub struct Background {
    handles: Mutex<Vec<JoinHandle<()>>>,
}

impl Background {
    /// Creates a new background task manager.
    pub fn new() -> Self {
        Self {
            handles: Mutex::new(Vec::new()),
        }
    }

    /// Spawns `f` as a background task. The task runs until it completes or
    /// the `Background` is dropped (at which point [`JoinHandle::abort`] is
    /// called on every tracked task).
    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = rt::spawn(f);
        self.handles.lock().unwrap().push(handle);
    }
}

impl Default for Background {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        // Take handles out so any `JoinHandle` still observed elsewhere (none
        // by construction here, but defensive) is left alone.
        let handles = std::mem::take(self.handles.get_mut().unwrap());
        for h in &handles {
            h.abort();
        }
        // Handles are dropped here, detaching the (now-aborted) tasks. The
        // sim runtime's wrapping `select!` and tokio's native abort both drop
        // the spawned future at its next `.await`.
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    #[tokio::test]
    async fn spawned_task_runs() {
        let b = Background::new();
        let ran = Arc::new(AtomicBool::new(false));
        let r = ran.clone();
        b.spawn(async move {
            r.store(true, Ordering::SeqCst);
        });
        // Give the task a chance to run before drop.
        for _ in 0..10 {
            if ran.load(Ordering::SeqCst) {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert!(ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn drop_aborts_tasks() {
        let b = Background::new();
        let done = Arc::new(AtomicUsize::new(0));
        let d = done.clone();
        b.spawn(async move {
            // Long sleep, never expected to complete.
            tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            d.fetch_add(1, Ordering::SeqCst);
        });
        tokio::task::yield_now().await;
        drop(b);
        // Yield enough times for the aborted task to be dropped before it
        // could increment the counter.
        for _ in 0..10 {
            tokio::task::yield_now().await;
        }
        assert_eq!(done.load(Ordering::SeqCst), 0);
    }
}
