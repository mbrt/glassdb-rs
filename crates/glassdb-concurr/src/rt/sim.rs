//! Simulation-aware runtime adapters.

use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::sync::OnceLock;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use tokio_util::sync::CancellationToken;

use crate::exec;

use super::TimedOut;

pub use crate::exec::{
    PctScheduler, RandomScheduler, RuntimeEntropySource, RuntimeTraceEvent, RuntimeTraceObserver,
    Scheduler, TapeScheduler, TaskId, block_on_with, block_on_with_trace, in_sim,
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
