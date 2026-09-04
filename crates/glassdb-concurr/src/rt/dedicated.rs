//! Dedicated-task lifecycle shared by native and simulated runtimes.

use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures::FutureExt;
use tokio_util::sync::CancellationToken;

/// Error returned when a dedicated task cannot be started.
#[derive(Debug, thiserror::Error)]
pub enum SpawnError {
    /// No Tokio runtime is active to drive the dedicated future.
    #[error("no runtime is available")]
    RuntimeUnavailable,
    /// The operating-system thread could not be created.
    #[error("dedicated thread could not be started: {0}")]
    Thread(#[source] std::io::Error),
}

/// Failure reported when joining a dedicated task.
#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum DedicatedJoinError {
    /// The task was cooperatively cancelled.
    #[error("dedicated task was cancelled")]
    Cancelled,
    /// The task panicked while being polled.
    #[error("dedicated task panicked")]
    Panicked,
}

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

async fn drive<F>(future: F, abort: CancellationToken) -> DedicatedOutcome<F::Output>
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

/// Drives `future` on one newly created, named operating-system thread.
#[cfg(not(sim))]
pub fn spawn_dedicated<N, F>(
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
            let outcome = runtime.block_on(drive(future, abort_inner));
            let _ = sender.send(outcome);
        })
        .map_err(SpawnError::Thread)?;
    Ok(DedicatedJoinHandle { result, abort })
}

/// Drives `future` as an ordinary task on the active deterministic executor.
///
/// Panics if no deterministic executor is active.
#[cfg(sim)]
pub fn spawn_dedicated<N, F>(
    _name: N,
    future: F,
) -> Result<DedicatedJoinHandle<F::Output>, SpawnError>
where
    N: Into<String>,
    F: Future + 'static,
    F::Output: 'static,
{
    let abort = CancellationToken::new();
    let result = crate::exec::executor::det_spawn(drive(future, abort.clone()));
    Ok(DedicatedJoinHandle { result, abort })
}
