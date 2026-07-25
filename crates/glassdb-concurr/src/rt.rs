//! Runtime indirection seam.
//!
//! Production builds delegate to real `tokio`. Under `--cfg sim`, task spawning
//! and time route through the in-repo deterministic executor ([`crate::exec`])
//! when one is running on the current thread, and fall back to real `tokio`
//! otherwise (so ordinary `#[tokio::test]` unit tests still work under a `sim`
//! build).
//!
//! `tokio::sync` and `tokio::select!` are runtime-agnostic and are used directly
//! elsewhere (non-`biased` selects stay deterministic under sim via the seeded
//! branch-poll RNG; see `exec::block_on_with`).

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::FutureExt;
use tokio_util::sync::CancellationToken;

/// Error returned when a runtime-seam deadline expires.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct TimedOut;

impl std::fmt::Display for TimedOut {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "operation timed out")
    }
}

impl std::error::Error for TimedOut {}

/// Error returned when a dedicated task cannot be started.
#[derive(Debug)]
pub enum SpawnError {
    /// No Tokio runtime is active to drive the dedicated future.
    RuntimeUnavailable,
    /// The operating-system thread could not be created.
    Thread(std::io::Error),
}

impl std::fmt::Display for SpawnError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SpawnError::RuntimeUnavailable => write!(f, "no runtime is available"),
            SpawnError::Thread(error) => {
                write!(f, "dedicated thread could not be started: {error}")
            }
        }
    }
}

impl std::error::Error for SpawnError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SpawnError::RuntimeUnavailable => None,
            SpawnError::Thread(error) => Some(error),
        }
    }
}

/// Failure reported when joining a dedicated task.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DedicatedJoinError {
    /// The task was cooperatively cancelled.
    Cancelled,
    /// The task panicked while being polled.
    Panicked,
}

impl std::fmt::Display for DedicatedJoinError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DedicatedJoinError::Cancelled => write!(f, "dedicated task was cancelled"),
            DedicatedJoinError::Panicked => write!(f, "dedicated task panicked"),
        }
    }
}

impl std::error::Error for DedicatedJoinError {}

enum DedicatedOutcome<T> {
    Completed(T),
    Cancelled,
    Panicked,
}

/// A handle to a task driven by a dedicated production thread or a simulated
/// executor task.
///
/// Dropping the handle detaches the task. Call [`DedicatedJoinHandle::abort`]
/// to request cooperative cancellation.
pub struct DedicatedJoinHandle<T> {
    result: tokio::sync::oneshot::Receiver<DedicatedOutcome<T>>,
    abort: CancellationToken,
}

impl<T> DedicatedJoinHandle<T> {
    /// Requests cancellation. The future is dropped when its driver can next
    /// poll the cancellation signal.
    pub fn abort(&self) {
        self.abort.cancel();
    }
}

impl<T> Future for DedicatedJoinHandle<T> {
    type Output = Result<T, DedicatedJoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().result)
            .poll(cx)
            .map(|outcome| match outcome {
                Ok(DedicatedOutcome::Completed(value)) => Ok(value),
                Ok(DedicatedOutcome::Cancelled) => Err(DedicatedJoinError::Cancelled),
                Ok(DedicatedOutcome::Panicked) | Err(_) => Err(DedicatedJoinError::Panicked),
            })
    }
}

async fn drive_dedicated<F>(future: F, abort: CancellationToken) -> DedicatedOutcome<F::Output>
where
    F: Future,
{
    let future = std::panic::AssertUnwindSafe(future).catch_unwind();
    tokio::pin!(future);
    tokio::select! {
        biased;
        _ = abort.cancelled() => DedicatedOutcome::Cancelled,
        result = &mut future => {
            if abort.is_cancelled() {
                DedicatedOutcome::Cancelled
            } else {
                match result {
                    Ok(value) => DedicatedOutcome::Completed(value),
                    Err(_) => DedicatedOutcome::Panicked,
                }
            }
        },
    }
}

fn spawn_dedicated_native<N, F>(
    name: N,
    future: F,
) -> Result<DedicatedJoinHandle<F::Output>, SpawnError>
where
    N: Into<String>,
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let runtime =
        tokio::runtime::Handle::try_current().map_err(|_| SpawnError::RuntimeUnavailable)?;
    let abort = CancellationToken::new();
    let abort_inner = abort.clone();
    let (sender, result) = tokio::sync::oneshot::channel();
    std::thread::Builder::new()
        .name(name.into())
        .spawn(move || {
            let outcome = runtime.block_on(drive_dedicated(future, abort_inner));
            let _ = sender.send(outcome);
        })
        .map_err(SpawnError::Thread)?;
    Ok(DedicatedJoinHandle { result, abort })
}

#[cfg(not(sim))]
mod imp {
    use super::{
        DedicatedJoinHandle, Duration, Future, SpawnError, TimedOut, spawn_dedicated_native,
    };

    pub use tokio::task::JoinHandle;
    pub use tokio::task::yield_now;
    pub use tokio::time::{Instant, sleep};

    /// Reports whether the deterministic executor is active.
    pub fn in_sim() -> bool {
        false
    }

    /// The current wall-clock time. In production this is just the real clock.
    pub fn system_now() -> std::time::SystemTime {
        std::time::SystemTime::now()
    }

    /// Returns the host's available parallelism.
    pub fn available_parallelism() -> usize {
        std::thread::available_parallelism()
            .map(|parallelism| parallelism.get())
            .unwrap_or(1)
    }

    /// Spawns a task on the ambient tokio runtime.
    pub fn spawn<F>(f: F) -> JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(f)
    }

    /// Drives `future` on one newly created, named operating-system thread.
    pub fn spawn_dedicated<N, F>(
        name: N,
        future: F,
    ) -> Result<DedicatedJoinHandle<F::Output>, SpawnError>
    where
        N: Into<String>,
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        spawn_dedicated_native(name, future)
    }

    /// Runs `future` until it completes or `duration` elapses.
    pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, TimedOut>
    where
        F: Future,
    {
        tokio::time::timeout(duration, future)
            .await
            .map_err(|_| TimedOut)
    }
}

#[cfg(sim)]
mod imp {
    use std::ops::Add;
    use std::pin::Pin;
    use std::sync::OnceLock;
    use std::task::{Context, Poll};

    use crate::exec;

    use super::{
        DedicatedJoinHandle, Duration, Future, SpawnError, TimedOut, drive_dedicated,
        spawn_dedicated_native,
    };

    pub use crate::exec::{
        PctScheduler, RandomScheduler, Scheduler, TapeScheduler, TaskId, block_on_with, in_sim,
    };

    /// Fills `buf` with deterministic simulated entropy from the running
    /// executor's seeded RNG. Panics if called outside the executor.
    pub fn fill_random(buf: &mut [u8]) {
        exec::fill_random(buf)
    }

    fn now_nanos() -> u64 {
        if exec::in_sim() {
            exec::now_nanos()
        } else {
            // Fall-back clock for `#[tokio::test]` runs under a `sim` build (no
            // deterministic executor is active). `tokio::time::Instant::now`
            // requires a tokio runtime, which such tests provide; it also tracks
            // a paused clock under `start_paused`.
            static BASE: OnceLock<tokio::time::Instant> = OnceLock::new();
            BASE.get_or_init(tokio::time::Instant::now)
                .elapsed()
                .as_nanos() as u64
        }
    }

    /// A monotonic instant on the active clock: virtual time under the
    /// deterministic executor, tokio's (possibly paused) clock otherwise.
    /// Nanoseconds since the run/process start.
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    pub struct Instant(u64);

    impl Instant {
        pub fn now() -> Self {
            Instant(now_nanos())
        }

        pub fn elapsed(&self) -> Duration {
            Duration::from_nanos(now_nanos().saturating_sub(self.0))
        }

        pub fn duration_since(&self, earlier: Instant) -> Duration {
            Duration::from_nanos(self.0.saturating_sub(earlier.0))
        }

        pub fn saturating_duration_since(&self, earlier: Instant) -> Duration {
            self.duration_since(earlier)
        }
    }

    impl Add<Duration> for Instant {
        type Output = Instant;
        fn add(self, rhs: Duration) -> Instant {
            Instant(self.0.saturating_add(rhs.as_nanos() as u64))
        }
    }

    /// The current wall-clock time. Under the deterministic executor this is a
    /// fixed epoch plus virtual time, so persisted timestamps (e.g. transaction
    /// logs) are a pure function of the seed and schedule and replays are
    /// byte-identical. Outside the executor it is the real clock.
    pub fn system_now() -> std::time::SystemTime {
        use std::time::{SystemTime, UNIX_EPOCH};
        if exec::in_sim() {
            // Matches the harness's `deterministic_time` anchor
            // (`db.rs` `DETERMINISTIC_EPOCH_SECS`) so log timestamps and the
            // monitor's anchored clock share one timeline.
            const SIM_WALL_BASE_SECS: u64 = 1_700_000_000;
            UNIX_EPOCH + Duration::from_secs(SIM_WALL_BASE_SECS) + Duration::from_nanos(now_nanos())
        } else {
            SystemTime::now()
        }
    }

    /// Returns one in simulation builds so shard geometry is independent of
    /// host affinity, including in ordinary Tokio tests.
    pub fn available_parallelism() -> usize {
        1
    }

    /// Sleeps for `dur` on the active clock.
    pub async fn sleep(dur: Duration) {
        if exec::in_sim() {
            exec::det_sleep(dur).await
        } else {
            tokio::time::sleep(dur).await
        }
    }

    /// Yields once to the scheduler.
    pub async fn yield_now() {
        if exec::in_sim() {
            exec::DetYield::default().await
        } else {
            tokio::task::yield_now().await
        }
    }

    /// Runs `future` until it completes or the active clock advances by
    /// `duration`.
    pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, TimedOut>
    where
        F: Future,
    {
        if exec::in_sim() {
            tokio::pin!(future);
            tokio::select! {
                biased;
                value = &mut future => Ok(value),
                _ = exec::det_sleep(duration) => Err(TimedOut),
            }
        } else {
            tokio::time::timeout(duration, future)
                .await
                .map_err(|_| TimedOut)
        }
    }

    /// Error returned when a joined task did not produce a value (it was dropped
    /// or aborted).
    #[derive(Debug)]
    pub struct JoinError;

    impl std::fmt::Display for JoinError {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "joined task failed to complete")
        }
    }
    impl std::error::Error for JoinError {}

    use tokio_util::sync::CancellationToken;

    /// A handle to a spawned task. Backed by the deterministic executor when one
    /// is running, or by tokio otherwise. Dropping it detaches the task; call
    /// [`JoinHandle::abort`] to cancel it.
    pub enum JoinHandle<T> {
        Det {
            rx: tokio::sync::oneshot::Receiver<Option<T>>,
            abort: CancellationToken,
        },
        Tokio(tokio::task::JoinHandle<T>),
    }

    impl<T> JoinHandle<T> {
        /// Requests that the task be cancelled. The task is dropped at its next
        /// `.await` point and the handle will yield [`JoinError`].
        pub fn abort(&self) {
            match self {
                JoinHandle::Det { abort, .. } => abort.cancel(),
                JoinHandle::Tokio(h) => h.abort(),
            }
        }
    }

    impl<T> Future for JoinHandle<T> {
        type Output = Result<T, JoinError>;
        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            match self.get_mut() {
                JoinHandle::Det { rx, .. } => Pin::new(rx).poll(cx).map(|r| match r {
                    Ok(Some(v)) => Ok(v),
                    _ => Err(JoinError),
                }),
                JoinHandle::Tokio(h) => Pin::new(h).poll(cx).map(|r| r.map_err(|_| JoinError)),
            }
        }
    }

    /// Spawns a task on the deterministic executor (if running) or on tokio.
    ///
    /// Under `--cfg sim`, the spawned future is wrapped in a `select!` against
    /// an internal cancel signal so that [`JoinHandle::abort`] drops it at its
    /// next `.await`; the deterministic executor itself has no native abort.
    pub fn spawn<F>(f: F) -> JoinHandle<F::Output>
    where
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if exec::in_sim() {
            let abort = CancellationToken::new();
            let abort_inner = abort.clone();
            let rx = exec::det_spawn(async move {
                tokio::select! {
                    biased;
                    _ = abort_inner.cancelled() => None,
                    v = f => Some(v),
                }
            });
            JoinHandle::Det { rx, abort }
        } else {
            JoinHandle::Tokio(tokio::spawn(f))
        }
    }

    /// Drives `future` as an ordinary deterministic task when simulation is
    /// active, or on one named operating-system thread otherwise.
    pub fn spawn_dedicated<N, F>(
        name: N,
        future: F,
    ) -> Result<DedicatedJoinHandle<F::Output>, SpawnError>
    where
        N: Into<String>,
        F: Future + Send + 'static,
        F::Output: Send + 'static,
    {
        if exec::in_sim() {
            let abort = CancellationToken::new();
            let result = exec::det_spawn(drive_dedicated(future, abort.clone()));
            Ok(DedicatedJoinHandle { result, abort })
        } else {
            spawn_dedicated_native(name, future)
        }
    }
}

pub use imp::*;

#[cfg(test)]
mod dedicated_tests {
    use std::sync::mpsc;
    use std::time::Duration;

    use super::{DedicatedJoinError, sleep, spawn_dedicated};

    #[tokio::test]
    async fn dedicated_task_uses_a_named_native_thread() {
        let caller = std::thread::current().id();
        let task = spawn_dedicated("glassdb-dedicated-test", async move {
            sleep(Duration::from_millis(1)).await;
            (
                std::thread::current().id(),
                std::thread::current().name().map(str::to_owned),
            )
        })
        .unwrap();

        let (thread, name) = task.await.unwrap();
        assert_ne!(thread, caller);
        assert_eq!(name.as_deref(), Some("glassdb-dedicated-test"));
    }

    #[tokio::test]
    async fn dedicated_abort_waits_for_a_blocking_poll_to_return() {
        let (entered, entered_rx) = mpsc::channel();
        let (release, release_rx) = mpsc::channel();
        let mut task = spawn_dedicated("glassdb-dedicated-blocked", async move {
            entered.send(()).unwrap();
            release_rx.recv().unwrap();
        })
        .unwrap();
        entered_rx
            .recv_timeout(Duration::from_secs(1))
            .expect("dedicated task did not enter its blocking poll");

        task.abort();
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut task)
                .await
                .is_err()
        );
        release.send(()).unwrap();
        assert_eq!(task.await, Err(DedicatedJoinError::Cancelled));
    }

    #[tokio::test]
    async fn dedicated_panic_is_normalized() {
        let task = spawn_dedicated("glassdb-dedicated-panic", async move {
            panic!("dedicated test panic");
        })
        .unwrap();
        assert_eq!(task.await, Err(DedicatedJoinError::Panicked));
    }

    #[tokio::test]
    async fn dropping_dedicated_handle_detaches() {
        let (release, release_rx) = tokio::sync::oneshot::channel();
        let (finished, finished_rx) = tokio::sync::oneshot::channel();
        let task = spawn_dedicated("glassdb-dedicated-detach", async move {
            let _ = release_rx.await;
            let _ = finished.send(());
        })
        .unwrap();
        drop(task);
        release.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(1), finished_rx)
            .await
            .expect("detached task did not finish")
            .unwrap();
    }

    #[cfg(sim)]
    #[test]
    fn active_simulation_uses_an_executor_task() {
        let caller = std::thread::current().id();
        super::block_on_with(super::TapeScheduler::new(Vec::new()), 0, async move {
            let task = spawn_dedicated("not-a-native-thread", async move {
                super::yield_now().await;
                std::thread::current().id()
            })
            .unwrap();
            assert_eq!(task.await.unwrap(), caller);
        });
    }
}
