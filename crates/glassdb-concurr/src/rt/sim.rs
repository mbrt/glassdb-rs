#![cfg(sim)]

//! Simulation-aware runtime adapters.

use std::future::Future;
use std::ops::Add;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, SystemTime};

use futures::FutureExt;
use tokio_util::sync::CancellationToken;

use crate::exec::executor;

use super::TimedOut;

/// A monotonic instant on the deterministic executor's virtual clock.
/// Nanoseconds since the run started.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Debug)]
pub struct Instant(u64);

impl Instant {
    pub fn now() -> Self {
        Instant(executor::now_nanos())
    }

    pub fn elapsed(&self) -> Duration {
        Duration::from_nanos(executor::now_nanos().saturating_sub(self.0))
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

/// The current simulated wall-clock time.
///
/// This is a fixed epoch plus virtual time, so persisted timestamps are a pure
/// function of the seed and schedule and replays are byte-identical.
pub fn system_now() -> SystemTime {
    use std::time::UNIX_EPOCH;
    const SIM_WALL_BASE_SECS: u64 = 1_700_000_000;
    UNIX_EPOCH
        + Duration::from_secs(SIM_WALL_BASE_SECS)
        + Duration::from_nanos(executor::now_nanos())
}

/// Returns one in simulation builds so shard geometry is independent of
/// host affinity.
pub fn available_parallelism() -> usize {
    1
}

/// Sleeps for `dur` on the deterministic executor's virtual clock.
pub async fn sleep(dur: Duration) {
    executor::det_sleep(dur).await
}

/// Yields once to the deterministic scheduler.
pub async fn yield_now() {
    executor::DetYield::default().await
}

/// Runs `future` until it completes or virtual time advances by `duration`.
pub async fn timeout<F>(duration: Duration, future: F) -> Result<F::Output, TimedOut>
where
    F: Future,
{
    tokio::pin!(future);
    tokio::select! {
        biased;
        value = &mut future => Ok(value),
        _ = executor::det_sleep(duration) => Err(TimedOut),
    }
}

/// Error returned when a joined task did not produce a value (it was dropped
/// or aborted).
#[derive(Debug, thiserror::Error)]
#[error("joined task failed to complete")]
pub struct JoinError;

/// A handle to a task on the deterministic executor.
///
/// Dropping it detaches the task. Call [`JoinHandle::abort`] to cancel it.
pub struct JoinHandle<T> {
    rx: tokio::sync::oneshot::Receiver<Option<T>>,
    abort: CancellationToken,
}

impl<T> JoinHandle<T> {
    /// Requests that the task be cancelled. The task is dropped at its next
    /// `.await` point and the handle will yield [`JoinError`].
    pub fn abort(&self) {
        self.abort.cancel();
    }
}

impl<T> Future for JoinHandle<T> {
    type Output = Result<T, JoinError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        Pin::new(&mut self.get_mut().rx).poll(cx).map(|r| match r {
            Ok(Some(v)) => Ok(v),
            _ => Err(JoinError),
        })
    }
}

/// Spawns a task on the active deterministic executor.
///
/// Under `--cfg sim`, the spawned future is wrapped in a `select!` against
/// an internal cancel signal so that [`JoinHandle::abort`] drops it at its
/// next `.await`; the deterministic executor itself has no native abort.
/// Panics if no deterministic executor is active.
pub fn spawn<F>(f: F) -> JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    let abort = CancellationToken::new();
    let abort_inner = abort.clone();
    let rx = executor::det_spawn(async move {
        let future = std::panic::AssertUnwindSafe(f).catch_unwind();
        tokio::pin!(future);
        tokio::select! {
            biased;
            _ = abort_inner.cancelled() => None,
            result = &mut future => result.ok(),
        }
    });
    JoinHandle { rx, abort }
}
