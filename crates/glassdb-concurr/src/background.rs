//! Background task management.
//!
//! [`Background`] is an owning collection of detached tasks. Best-effort tasks
//! are spawned with [`Background::spawn`] and run until completion or until the
//! manager is dropped. Clean-shutdown tasks are spawned with
//! [`Background::spawn_waited`] and are also awaited by [`Background::shutdown`].
//! Dropping `Background` aborts every spawned task regardless of lane.

use std::future::Future;
use std::sync::Mutex;

use crate::rt::{self, JoinHandle};
use tokio::sync::Notify;

/// Manages a set of background tasks. When the `Background` is dropped, every
/// tracked task is aborted; the abort fires at the task's next `.await`.
pub struct Background {
    inner: Mutex<Inner>,
    shutdown_drained: Notify,
}

struct Inner {
    best_effort: Vec<JoinHandle<()>>,
    waited: Vec<JoinHandle<()>>,
    draining_waited: bool,
}

impl Background {
    /// Creates a new background task manager.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                best_effort: Vec::new(),
                waited: Vec::new(),
                draining_waited: false,
            }),
            shutdown_drained: Notify::new(),
        }
    }

    /// Spawns `f` as a best-effort background task. The task runs until it
    /// completes or the `Background` is dropped; [`Background::shutdown`] does
    /// not wait for it.
    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = rt::spawn(f);
        self.inner.lock().unwrap().best_effort.push(handle);
    }

    /// Spawns `f` as a clean-shutdown background task. The task runs until it
    /// completes, and [`Background::shutdown`] waits for it.
    pub fn spawn_waited<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let handle = rt::spawn(f);
        self.inner.lock().unwrap().waited.push(handle);
    }

    /// Waits for all tasks spawned with [`Background::spawn_waited`] to finish.
    ///
    /// Best-effort tasks are left running. Concurrent callers coordinate so
    /// every caller returns only after the current clean-shutdown drain has
    /// completed.
    pub async fn shutdown(&self) {
        loop {
            let mut wait = None;
            let handles = {
                let mut inner = self.inner.lock().unwrap();
                if inner.draining_waited {
                    wait = Some(self.shutdown_drained.notified());
                    None
                } else if inner.waited.is_empty() {
                    return;
                } else {
                    inner.draining_waited = true;
                    Some(std::mem::take(&mut inner.waited))
                }
            };
            let Some(handles) = handles else {
                wait.expect("missing drain notification").await;
                continue;
            };

            for handle in handles {
                let _ = handle.await;
            }

            let mut inner = self.inner.lock().unwrap();
            inner.draining_waited = false;
            self.shutdown_drained.notify_waiters();
        }
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
        let inner = self.inner.get_mut().unwrap();
        let best_effort = std::mem::take(&mut inner.best_effort);
        let waited = std::mem::take(&mut inner.waited);
        for h in best_effort.iter().chain(waited.iter()) {
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
    async fn shutdown_waits_for_waited_tasks() {
        let b = Background::new();
        let ran = Arc::new(AtomicBool::new(false));
        let r = ran.clone();
        b.spawn_waited(async move {
            tokio::task::yield_now().await;
            r.store(true, Ordering::SeqCst);
        });

        b.shutdown().await;

        assert!(ran.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_ignores_best_effort_tasks() {
        let b = Background::new();
        let done = Arc::new(AtomicBool::new(false));
        let d = done.clone();
        b.spawn(async move {
            std::future::pending::<()>().await;
            d.store(true, Ordering::SeqCst);
        });

        b.shutdown().await;

        assert!(!done.load(Ordering::SeqCst));
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
        let d = done.clone();
        b.spawn_waited(async move {
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
