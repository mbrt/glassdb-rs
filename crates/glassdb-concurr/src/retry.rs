//! Retry-with-backoff utilities. Ported from the Go `concurr` backoff helpers.
//! A closure returns [`RetryErr::Transient`] to be retried or
//! [`RetryErr::Permanent`] to stop immediately.

use std::future::Future;
use std::time::Duration;

use crate::ctx::Ctx;

const INITIAL_INTERVAL: Duration = Duration::from_millis(200);
const MAX_INTERVAL: Duration = Duration::from_secs(5);
const MULTIPLIER: f64 = 1.5;

/// Classifies a retryable operation's error.
#[derive(Debug)]
pub enum RetryErr<E> {
    /// The operation should be retried after a backoff.
    Transient(E),
    /// The operation must not be retried; the inner error is returned as-is.
    Permanent(E),
}

impl<E> RetryErr<E> {
    /// Convenience constructor for a transient error.
    pub fn transient(e: E) -> Self {
        RetryErr::Transient(e)
    }

    /// Convenience constructor for a permanent error.
    pub fn permanent(e: E) -> Self {
        RetryErr::Permanent(e)
    }
}

/// Retries `f` with default exponential backoff until it succeeds, returns a
/// permanent error, or `ctx` is cancelled.
pub async fn retry_with_backoff<T, E, F, Fut>(ctx: &Ctx, f: F) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RetryErr<E>>>,
{
    retry(ctx, INITIAL_INTERVAL, MAX_INTERVAL, f).await
}

/// Retries `f` with exponential backoff between `initial` and `max` intervals.
/// On cancellation during a backoff wait, returns the last transient error.
pub async fn retry<T, E, F, Fut>(
    ctx: &Ctx,
    initial: Duration,
    max: Duration,
    mut f: F,
) -> Result<T, E>
where
    F: FnMut() -> Fut,
    Fut: Future<Output = Result<T, RetryErr<E>>>,
{
    let mut interval = initial;
    loop {
        match f().await {
            Ok(v) => return Ok(v),
            Err(RetryErr::Permanent(e)) => return Err(e),
            Err(RetryErr::Transient(e)) => {
                tokio::select! {
                    biased;
                    _ = ctx.cancelled() => return Err(e),
                    _ = crate::rt::sleep(interval) => {}
                }
                interval = std::cmp::min(interval.mul_f64(MULTIPLIER), max);
            }
        }
    }
}
