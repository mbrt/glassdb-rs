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
use std::time::{Duration, SystemTime};

mod dedicated;

pub use dedicated::{DedicatedJoinError, DedicatedJoinHandle, SpawnError, spawn_dedicated};

/// Error returned when a runtime-seam deadline expires.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
#[error("operation timed out")]
pub struct TimedOut;

#[cfg(not(sim))]
mod imp {
    use std::ops::Add;
    use std::sync::OnceLock;

    use super::{Duration, Future, SystemTime, TimedOut};

    pub use tokio::task::JoinHandle;
    pub use tokio::task::yield_now;

    /// Error returned when process-wide model time cannot be configured.
    #[derive(Clone, Copy, Debug, PartialEq, thiserror::Error)]
    pub enum ModelTimeError {
        /// The requested speedup cannot represent a coherent time conversion.
        #[error("model-time speedup must be finite, positive, and representable, got {0}")]
        InvalidSpeedup(f64),
        /// Model time was already configured or observed by this process.
        #[error("model time was already initialized")]
        AlreadyInitialized,
    }

    /// Installs an immutable process-wide model-time speedup.
    ///
    /// A speedup of `N` makes one wall second advance model time by `N` seconds.
    /// The setting must be installed before any model-time observation or wait.
    /// Explicit configuration also anchors wall timestamps to model monotonic time;
    /// an unconfigured process defaults to live real time at speed `1`.
    pub fn set_model_time_speedup(speedup: f64) -> Result<(), ModelTimeError> {
        if !(speedup.is_finite()
            && speedup > 0.0
            && Duration::try_from_secs_f64(speedup).is_ok()
            && Duration::try_from_secs_f64(1.0 / speedup).is_ok())
        {
            return Err(ModelTimeError::InvalidSpeedup(speedup));
        }
        MODEL_TIME
            .set(ModelTime {
                speedup,
                anchored_wall: true,
            })
            .map_err(|_| ModelTimeError::AlreadyInitialized)
    }

    #[derive(Clone, Copy)]
    struct ModelTime {
        speedup: f64,
        anchored_wall: bool,
    }

    static MODEL_TIME: OnceLock<ModelTime> = OnceLock::new();

    fn model_time() -> &'static ModelTime {
        MODEL_TIME.get_or_init(|| ModelTime {
            speedup: 1.0,
            anchored_wall: false,
        })
    }

    fn scaled_duration(duration: Duration, factor: f64) -> Duration {
        if duration.is_zero() || factor == 1.0 {
            return duration;
        }
        Duration::try_from_secs_f64(duration.as_secs_f64() * factor).unwrap_or(Duration::MAX)
    }

    fn model_elapsed(duration: Duration) -> Duration {
        scaled_duration(duration, model_time().speedup)
    }

    fn model_wait(duration: Duration) -> Duration {
        if duration.is_zero() {
            return duration;
        }
        let wait = scaled_duration(duration, 1.0 / model_time().speedup);
        // Preserve an actual scheduling point even when acceleration rounds a
        // nominally nonzero duration below the runtime's nanosecond representation.
        wait.max(Duration::from_nanos(1))
    }

    /// A monotonic instant whose elapsed durations are expressed in model time.
    #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
    pub struct Instant(tokio::time::Instant);

    impl Instant {
        pub fn now() -> Self {
            // Observing an instant freezes the process-wide configuration even
            // though the raw instant itself needs no conversion yet.
            let _ = model_time();
            Instant(tokio::time::Instant::now())
        }

        pub fn elapsed(&self) -> Duration {
            model_elapsed(self.0.elapsed())
        }

        pub fn duration_since(&self, earlier: Instant) -> Duration {
            model_elapsed(self.0.duration_since(earlier.0))
        }

        pub fn saturating_duration_since(&self, earlier: Instant) -> Duration {
            model_elapsed(self.0.saturating_duration_since(earlier.0))
        }
    }

    impl Add<Duration> for Instant {
        type Output = Instant;

        fn add(self, rhs: Duration) -> Instant {
            Instant(self.0 + model_wait(rhs))
        }
    }

    /// Reports whether the deterministic executor is active.
    pub fn in_sim() -> bool {
        false
    }

    struct WallAnchor {
        system: SystemTime,
        instant: Instant,
    }

    /// The current wall-clock time in the process's model-time domain.
    pub fn system_now() -> SystemTime {
        if !model_time().anchored_wall {
            return SystemTime::now();
        }
        static ANCHOR: OnceLock<WallAnchor> = OnceLock::new();
        let anchor = ANCHOR.get_or_init(|| WallAnchor {
            system: SystemTime::now(),
            instant: Instant::now(),
        });
        anchor
            .system
            .checked_add(anchor.instant.elapsed())
            .expect("model wall time exceeded SystemTime's range")
    }

    /// Sleeps for `duration` in model time.
    pub async fn sleep(duration: Duration) {
        tokio::time::sleep(model_wait(duration)).await;
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

    /// Runs `future` until it completes or `duration` elapses.
    pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, TimedOut>
    where
        F: Future,
    {
        tokio::time::timeout(model_wait(duration), future)
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

    use super::{Duration, Future, SystemTime, TimedOut};

    pub use crate::exec::{
        PctScheduler, RandomScheduler, RuntimeEntropySource, RuntimeTraceEvent,
        RuntimeTraceObserver, Scheduler, TapeScheduler, TaskId, block_on_with, block_on_with_trace,
        in_sim,
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
            let nanos = rhs.as_nanos().min(u64::MAX as u128) as u64;
            Instant(self.0.saturating_add(nanos))
        }
    }

    /// The current wall-clock time. Under the deterministic executor this is a
    /// fixed epoch plus virtual time, so persisted timestamps (e.g. transaction
    /// logs) are a pure function of the seed and schedule and replays are
    /// byte-identical. Outside the executor it is the real clock.
    pub fn system_now() -> SystemTime {
        use std::time::UNIX_EPOCH;
        if exec::in_sim() {
            const SIM_WALL_BASE_SECS: u64 = 1_700_000_000;
            return UNIX_EPOCH
                + Duration::from_secs(SIM_WALL_BASE_SECS)
                + Duration::from_nanos(now_nanos());
        }
        SystemTime::now()
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
    #[derive(Debug, thiserror::Error)]
    #[error("joined task failed to complete")]
    pub struct JoinError;

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
    fn active_simulation_preserves_dedicated_lifecycle_and_spawn_order() {
        use std::sync::atomic::{AtomicBool, Ordering};
        use std::sync::{Arc, Mutex};

        struct DropNotice(Arc<AtomicBool>);

        impl Drop for DropNotice {
            fn drop(&mut self) {
                self.0.store(true, Ordering::SeqCst);
            }
        }

        let caller = std::thread::current().id();
        let dropped_at_shutdown = Arc::new(AtomicBool::new(false));
        let shutdown_notice = dropped_at_shutdown.clone();
        let trace = Arc::new(Mutex::new(Vec::new()));
        let trace_sink = trace.clone();
        super::block_on_with_trace(
            super::TapeScheduler::new(Vec::new()),
            0,
            Arc::new(move |event| trace_sink.lock().unwrap().push(event)),
            async move {
                let success = spawn_dedicated("not-a-native-thread", async move {
                    super::yield_now().await;
                    std::thread::current().id()
                })
                .unwrap();
                let panicked = spawn_dedicated("simulated-panic", async move {
                    panic!("dedicated test panic");
                })
                .unwrap();
                let cancelled = spawn_dedicated("simulated-cancellation", async move {
                    std::future::pending::<()>().await;
                })
                .unwrap();
                cancelled.abort();

                let notice = DropNotice(shutdown_notice);
                let detached = spawn_dedicated("simulated-shutdown", async move {
                    let _notice = notice;
                    std::future::pending::<()>().await;
                })
                .unwrap();
                drop(detached);

                assert_eq!(success.await.unwrap(), caller);
                assert!(matches!(panicked.await, Err(DedicatedJoinError::Panicked)));
                assert_eq!(cancelled.await, Err(DedicatedJoinError::Cancelled));
            },
        );

        assert!(dropped_at_shutdown.load(Ordering::SeqCst));
        let spawned: Vec<_> = trace
            .lock()
            .unwrap()
            .iter()
            .filter_map(|event| match event {
                super::RuntimeTraceEvent::TaskSpawned { task_id } => Some(*task_id),
                _ => None,
            })
            .collect();
        assert_eq!(spawned, [0, 1, 2, 3, 4]);
    }
}
