//! Background task management.
//!
//! [`Background`] owns protocol producers and clean-shutdown work. Graceful
//! shutdown closes admission, aborts and joins best-effort producers, then
//! drains work spawned with [`Background::spawn_waited`]. Dropping `Background`
//! aborts every spawned task regardless of lane.

use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::rt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct TrackedTask {
    cancel: CancellationToken,
    completion: Arc<TaskCompletion>,
}

impl TrackedTask {
    fn spawn<F>(future: F) -> Self
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let cancel = CancellationToken::new();
        let completion = Arc::new(TaskCompletion::new());
        let guard = CompletionGuard(completion.clone());
        let task_cancel = cancel.clone();
        drop(rt::spawn(async move {
            let _guard = guard;
            tokio::select! {
                biased;
                _ = task_cancel.cancelled() => {}
                _ = future => {}
            }
        }));
        Self { cancel, completion }
    }

    fn cancel(&self) {
        self.cancel.cancel();
    }

    async fn wait(&self) {
        self.completion.wait().await;
    }
}

struct TaskCompletion {
    done: AtomicBool,
    notify: Notify,
}

impl TaskCompletion {
    fn new() -> Self {
        Self {
            done: AtomicBool::new(false),
            notify: Notify::new(),
        }
    }

    async fn wait(&self) {
        loop {
            let notified = self.notify.notified();
            if self.done.load(Ordering::Acquire) {
                return;
            }
            notified.await;
        }
    }
}

struct CompletionGuard(Arc<TaskCompletion>);

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        self.0.done.store(true, Ordering::Release);
        self.0.notify.notify_waiters();
    }
}

/// Manages a set of background tasks. When the `Background` is dropped, every
/// tracked task is aborted; the abort fires at the task's next `.await`.
pub struct Background {
    inner: Mutex<Inner>,
}

struct Inner {
    best_effort: Vec<TrackedTask>,
    waited: Vec<TrackedTask>,
    shutting_down: bool,
    complete: bool,
}

impl Background {
    /// Creates a new background task manager.
    pub fn new() -> Self {
        Self {
            inner: Mutex::new(Inner {
                best_effort: Vec::new(),
                waited: Vec::new(),
                shutting_down: false,
                complete: false,
            }),
        }
    }

    /// Spawns `f` as a best-effort background task. Graceful shutdown aborts
    /// and joins the task. Work submitted after shutdown starts is discarded.
    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut inner = self.inner.lock().unwrap();
        if inner.shutting_down {
            return;
        }
        inner.best_effort.push(TrackedTask::spawn(f));
    }

    /// Spawns `f` as clean-shutdown work. The task runs to completion and
    /// [`Background::shutdown`] waits for it. Work submitted after shutdown
    /// starts is discarded.
    pub fn spawn_waited<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let mut inner = self.inner.lock().unwrap();
        if inner.shutting_down {
            return;
        }
        inner.waited.push(TrackedTask::spawn(f));
    }

    /// Closes task admission, aborts and joins best-effort tasks, and waits for
    /// all clean-shutdown work. Concurrent calls are idempotent, and a later
    /// call resumes the drain if an earlier shutdown future was cancelled.
    pub async fn shutdown(&self) {
        let (best_effort, waited) = {
            let mut inner = self.inner.lock().unwrap();
            inner.shutting_down = true;
            if inner.complete {
                return;
            }
            (inner.best_effort.clone(), inner.waited.clone())
        };

        for task in &best_effort {
            task.cancel();
        }
        for task in best_effort {
            task.wait().await;
        }
        for task in waited {
            task.wait().await;
        }

        self.inner.lock().unwrap().complete = true;
    }
}

impl Default for Background {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        let inner = self.inner.get_mut().unwrap();
        for task in inner.best_effort.iter().chain(inner.waited.iter()) {
            task.cancel();
        }
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
    async fn shutdown_aborts_best_effort_tasks() {
        struct DropProbe(Arc<AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let b = Background::new();
        let done = Arc::new(AtomicBool::new(false));
        let dropped = Arc::new(AtomicBool::new(false));
        let d = done.clone();
        let probe = DropProbe(dropped.clone());
        b.spawn(async move {
            let _probe = probe;
            std::future::pending::<()>().await;
            d.store(true, Ordering::SeqCst);
        });

        b.shutdown().await;

        assert!(!done.load(Ordering::SeqCst));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn shutdown_rejects_new_tasks() {
        struct DropProbe(Arc<AtomicBool>);
        impl Drop for DropProbe {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let b = Background::new();
        b.shutdown().await;
        let dropped = Arc::new(AtomicBool::new(false));
        let probe = DropProbe(dropped.clone());
        b.spawn(async move {
            let _probe = probe;
            std::future::pending::<()>().await;
        });

        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test]
    async fn cancelled_shutdown_can_be_resumed() {
        let b = Arc::new(Background::new());
        let entered = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        b.spawn_waited({
            let entered = entered.clone();
            let release = release.clone();
            async move {
                entered.notify_one();
                release.notified().await;
            }
        });
        entered.notified().await;

        let first = tokio::spawn({
            let b = b.clone();
            async move { b.shutdown().await }
        });
        tokio::task::yield_now().await;
        first.abort();
        let _ = first.await;

        let resumed = tokio::spawn({
            let b = b.clone();
            async move { b.shutdown().await }
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!resumed.is_finished());
        release.notify_one();
        resumed.await.unwrap();
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
