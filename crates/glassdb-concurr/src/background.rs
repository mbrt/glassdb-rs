//! Background task management.
//!
//! [`Background`] owns protocol producers and clean-shutdown work. Graceful
//! shutdown closes admission, aborts and joins best-effort producers, then
//! drains work spawned with [`Background::spawn_waited`]. Dropping `Background`
//! aborts every spawned task regardless of lane.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};

use crate::rt;
use tokio::sync::Notify;
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct TrackedTask {
    cancel: CancellationToken,
    completion: Arc<TaskCompletion>,
}

impl TrackedTask {
    fn new() -> Self {
        Self {
            cancel: CancellationToken::new(),
            completion: Arc::new(TaskCompletion::new()),
        }
    }

    fn launch<F>(&self, future: F, guard: CompletionGuard)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let task_cancel = self.cancel.clone();
        drop(rt::spawn(async move {
            let _guard = guard;
            tokio::select! {
                biased;
                _ = task_cancel.cancelled() => {}
                _ = future => {}
            }
        }));
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

    fn finish(&self) {
        self.done.store(true, Ordering::Release);
        self.notify.notify_waiters();
    }
}

type TaskId = u64;

#[derive(Clone, Copy)]
enum TaskLane {
    BestEffort,
    Waited,
}

struct CompletionGuard {
    completion: Arc<TaskCompletion>,
    registry: Weak<Mutex<Inner>>,
    lane: TaskLane,
    id: TaskId,
}

impl Drop for CompletionGuard {
    fn drop(&mut self) {
        let Some(registry) = self.registry.upgrade() else {
            self.completion.finish();
            return;
        };

        let mut inner = registry.lock().unwrap();
        inner.remove(self.lane, self.id);
        // Publishing completion under the registry lock makes removal atomic
        // with a shutdown snapshot: shutdown either sees and waits for this task,
        // or observes it fully completed and absent.
        self.completion.finish();
    }
}

/// Manages a set of background tasks. When the `Background` is dropped, every
/// tracked task is aborted; the abort fires at the task's next `.await`.
pub struct Background {
    inner: Arc<Mutex<Inner>>,
}

struct Inner {
    best_effort: BTreeMap<TaskId, TrackedTask>,
    waited: BTreeMap<TaskId, TrackedTask>,
    next_task_id: TaskId,
    shutting_down: bool,
    complete: bool,
}

impl Inner {
    fn register(&mut self, lane: TaskLane, task: TrackedTask) -> TaskId {
        let id = self.next_task_id;
        self.next_task_id = self
            .next_task_id
            .checked_add(1)
            .expect("background task ID exhausted");
        let previous = self.tasks_mut(lane).insert(id, task);
        debug_assert!(previous.is_none());
        id
    }

    fn remove(&mut self, lane: TaskLane, id: TaskId) {
        let removed = self.tasks_mut(lane).remove(&id);
        debug_assert!(removed.is_some());
    }

    fn tasks_mut(&mut self, lane: TaskLane) -> &mut BTreeMap<TaskId, TrackedTask> {
        match lane {
            TaskLane::BestEffort => &mut self.best_effort,
            TaskLane::Waited => &mut self.waited,
        }
    }
}

impl Background {
    /// Creates a new background task manager.
    pub fn new() -> Self {
        Self {
            inner: Arc::new(Mutex::new(Inner {
                best_effort: BTreeMap::new(),
                waited: BTreeMap::new(),
                next_task_id: 0,
                shutting_down: false,
                complete: false,
            })),
        }
    }

    /// Spawns `f` as a best-effort background task. Graceful shutdown aborts
    /// and joins the task. Work submitted after shutdown starts is discarded.
    pub fn spawn<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_tracked(TaskLane::BestEffort, f);
    }

    /// Spawns `f` as clean-shutdown work. The task runs to completion and
    /// [`Background::shutdown`] waits for it. Work submitted after shutdown
    /// starts is discarded.
    pub fn spawn_waited<F>(&self, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        self.spawn_tracked(TaskLane::Waited, f);
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
            (
                inner.best_effort.values().cloned().collect::<Vec<_>>(),
                inner.waited.values().cloned().collect::<Vec<_>>(),
            )
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

        let mut inner = self.inner.lock().unwrap();
        debug_assert!(inner.best_effort.is_empty());
        debug_assert!(inner.waited.is_empty());
        inner.complete = true;
    }

    fn spawn_tracked<F>(&self, lane: TaskLane, f: F)
    where
        F: Future<Output = ()> + Send + 'static,
    {
        let (id, task) = {
            let mut inner = self.inner.lock().unwrap();
            if inner.shutting_down {
                return;
            }
            let task = TrackedTask::new();
            let id = inner.register(lane, task.clone());
            (id, task)
        };
        let guard = CompletionGuard {
            completion: task.completion.clone(),
            registry: Arc::downgrade(&self.inner),
            lane,
            id,
        };
        task.launch(f, guard);
    }

    #[cfg(test)]
    fn live_task_counts(&self) -> (usize, usize) {
        let inner = self.inner.lock().unwrap();
        (inner.best_effort.len(), inner.waited.len())
    }
}

impl Default for Background {
    fn default() -> Self {
        Self::new()
    }
}

impl Drop for Background {
    fn drop(&mut self) {
        let inner = self.inner.lock().unwrap();
        for task in inner.best_effort.values().chain(inner.waited.values()) {
            task.cancel();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

    use tokio::sync::oneshot;

    const TASKS_PER_LANE: usize = 2_048;

    struct DropSignal(Option<oneshot::Sender<()>>);

    impl Drop for DropSignal {
        fn drop(&mut self) {
            if let Some(sender) = self.0.take() {
                let _ = sender.send(());
            }
        }
    }

    async fn wait_for_live_task_counts(background: &Background, expected: (usize, usize)) {
        for _ in 0..100 {
            if background.live_task_counts() == expected {
                return;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(background.live_task_counts(), expected);
    }

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
        wait_for_live_task_counts(&b, (0, 0)).await;
    }

    #[tokio::test]
    async fn completed_tasks_are_pruned_from_both_lanes() {
        let b = Background::new();

        for _ in 0..TASKS_PER_LANE {
            b.spawn(async {});
            wait_for_live_task_counts(&b, (0, 0)).await;

            b.spawn_waited(async {});
            wait_for_live_task_counts(&b, (0, 0)).await;
        }
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
        assert_eq!(b.live_task_counts(), (0, 0));
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
        assert_eq!(b.live_task_counts(), (0, 0));
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
        assert_eq!(b.live_task_counts(), (0, 0));
    }

    #[tokio::test]
    async fn shutdown_handles_completion_before_during_and_after_start() {
        let b = Arc::new(Background::new());

        let (before_finished, before_done) = oneshot::channel();
        b.spawn_waited(async move {
            let _ = before_finished.send(());
        });
        before_done.await.unwrap();
        wait_for_live_task_counts(&b, (0, 0)).await;

        let (best_effort_entered, best_effort_started) = oneshot::channel();
        let (best_effort_dropped, best_effort_cancelled) = oneshot::channel();
        b.spawn(async move {
            let _drop_signal = DropSignal(Some(best_effort_dropped));
            let _ = best_effort_entered.send(());
            std::future::pending::<()>().await;
        });
        best_effort_started.await.unwrap();

        let (waited_entered, waited_started) = oneshot::channel();
        let (release_waited, waited_release) = oneshot::channel();
        let (waited_finished, waited_done) = oneshot::channel();
        b.spawn_waited(async move {
            let _ = waited_entered.send(());
            let _ = waited_release.await;
            let _ = waited_finished.send(());
        });
        waited_started.await.unwrap();
        assert_eq!(b.live_task_counts(), (1, 1));

        let shutdown = tokio::spawn({
            let b = b.clone();
            async move { b.shutdown().await }
        });

        best_effort_cancelled.await.unwrap();
        wait_for_live_task_counts(&b, (0, 1)).await;
        assert!(!shutdown.is_finished());

        release_waited.send(()).unwrap();
        waited_done.await.unwrap();
        shutdown.await.unwrap();
        assert_eq!(b.live_task_counts(), (0, 0));
    }

    #[tokio::test]
    async fn cancelled_shutdown_can_be_resumed() {
        let b = Arc::new(Background::new());
        let (best_effort_entered, best_effort_started) = oneshot::channel();
        let (best_effort_dropped, best_effort_cancelled) = oneshot::channel();
        b.spawn(async move {
            let _drop_signal = DropSignal(Some(best_effort_dropped));
            let _ = best_effort_entered.send(());
            std::future::pending::<()>().await;
        });
        best_effort_started.await.unwrap();

        let (waited_entered, waited_started) = oneshot::channel();
        let (release_waited, waited_release) = oneshot::channel();
        b.spawn_waited(async move {
            let _ = waited_entered.send(());
            let _ = waited_release.await;
        });
        waited_started.await.unwrap();

        let first = tokio::spawn({
            let b = b.clone();
            async move { b.shutdown().await }
        });
        best_effort_cancelled.await.unwrap();
        wait_for_live_task_counts(&b, (0, 1)).await;
        first.abort();
        let _ = first.await;
        assert_eq!(b.live_task_counts(), (0, 1));

        let resumed = tokio::spawn({
            let b = b.clone();
            async move { b.shutdown().await }
        });
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!resumed.is_finished());
        release_waited.send(()).unwrap();
        resumed.await.unwrap();
        assert_eq!(b.live_task_counts(), (0, 0));
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
