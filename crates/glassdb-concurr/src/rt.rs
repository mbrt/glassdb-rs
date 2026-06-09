//! Runtime indirection seam.
//!
//! Production builds re-export real `tokio` (`spawn`, `time::{sleep, Instant}`,
//! `task::{JoinHandle, yield_now}`) with zero overhead. Under `--cfg sim` these
//! route through the in-repo deterministic executor ([`crate::exec`]) when one is
//! running on the current thread, and fall back to real `tokio` otherwise (so
//! ordinary `#[tokio::test]` unit tests still work under a `sim` build).
//!
//! Only `spawn` and time need redirection: `tokio::sync` and `tokio::select!`
//! are runtime-agnostic and are used directly elsewhere (non-`biased` selects
//! stay deterministic under sim via the seeded branch-poll RNG; see
//! `exec::block_on_with`).

#[cfg(not(sim))]
mod imp {
    pub use tokio::task::JoinHandle;
    pub use tokio::task::yield_now;
    pub use tokio::time::{Instant, sleep};

    /// The current wall-clock time. In production this is just the real clock.
    pub fn system_now() -> std::time::SystemTime {
        std::time::SystemTime::now()
    }

    /// Spawns a task on the ambient tokio runtime.
    pub fn spawn<F>(f: F) -> JoinHandle<F::Output>
    where
        F: std::future::Future + Send + 'static,
        F::Output: Send + 'static,
    {
        tokio::spawn(f)
    }
}

#[cfg(sim)]
mod imp {
    use std::future::Future;
    use std::ops::Add;
    use std::pin::Pin;
    use std::sync::OnceLock;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use crate::exec;

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

    use std::sync::Arc;

    use crate::abort_signal::AbortSignal;

    /// A handle to a spawned task. Backed by the deterministic executor when one
    /// is running, or by tokio otherwise. Dropping it detaches the task; call
    /// [`JoinHandle::abort`] to cancel it.
    pub enum JoinHandle<T> {
        Det {
            rx: tokio::sync::oneshot::Receiver<Option<T>>,
            abort: Arc<AbortSignal>,
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
            let abort = Arc::new(AbortSignal::new());
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
}

pub use imp::*;
