//! Bounded concurrent fan-out. Ported from the Go `concurr.Fanout` (an
//! `errgroup` with a concurrency limit). Runs `num` tasks concurrently up to a
//! limit, returning the first error and cancelling the rest via the context.

use std::future::Future;

use futures::stream::{self, StreamExt};

use crate::ctx::Ctx;

/// Executes closures concurrently up to a configured limit.
#[derive(Clone, Copy)]
pub struct Fanout {
    limit: usize,
}

impl Fanout {
    /// Creates a fan-out with the given maximum concurrency.
    pub fn new(max_concurrent: usize) -> Self {
        Fanout {
            limit: max_concurrent.max(1),
        }
    }

    /// Runs `f` for each index in `[0, num)`, bounded by the concurrency limit.
    /// Returns the first error encountered; on error the per-task context is
    /// cancelled so siblings can stop early.
    pub async fn spawn<F, Fut, E>(&self, ctx: &Ctx, num: usize, f: F) -> Result<(), E>
    where
        F: Fn(Ctx, usize) -> Fut,
        Fut: Future<Output = Result<(), E>>,
    {
        if num == 0 {
            return Ok(());
        }
        // A token cancelled when the parent is cancelled or on first error.
        let (child_ctx, group_token) = ctx.child_cancel();

        let f = &f;
        let child_ctx = &child_ctx;
        let mut stream = stream::iter(0..num)
            .map(|i| {
                let c = child_ctx.clone();
                async move { f(c, i).await }
            })
            .buffer_unordered(self.limit);

        let mut result = Ok(());
        while let Some(r) = stream.next().await {
            if let Err(e) = r {
                if result.is_ok() {
                    result = Err(e);
                    group_token.cancel();
                }
            }
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use std::time::Duration;
    use tokio::time::Instant;

    #[tokio::test]
    async fn runs_all() {
        let counter = Arc::new(AtomicUsize::new(0));
        let f = Fanout::new(3);
        let c = counter.clone();
        let res: Result<(), ()> = f
            .spawn(&Ctx::background(), 3, |_ctx, _i| {
                let c = c.clone();
                async move {
                    c.fetch_add(1, Ordering::SeqCst);
                    Ok(())
                }
            })
            .await;
        assert!(res.is_ok());
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    }

    #[tokio::test]
    async fn first_error_is_returned() {
        let f = Fanout::new(3);
        let res: Result<(), i32> = f
            .spawn(&Ctx::background(), 3, |_ctx, i| async move {
                if i == 1 {
                    Err(42)
                } else {
                    Ok(())
                }
            })
            .await;
        assert_eq!(res, Err(42));
    }

    #[tokio::test(start_paused = true)]
    async fn respects_concurrency_limit() {
        // With a limit of 2, the third task can only start after one of the
        // first two finishes.
        let start = Instant::now();
        let f = Fanout::new(2);
        let times = Arc::new(std::sync::Mutex::new(vec![Duration::ZERO; 3]));
        let t = times.clone();
        let res: Result<(), ()> = f
            .spawn(&Ctx::background(), 3, |_ctx, i| {
                let t = t.clone();
                async move {
                    tokio::time::sleep(Duration::from_millis(50)).await;
                    t.lock().unwrap()[i] = start.elapsed();
                    Ok(())
                }
            })
            .await;
        assert!(res.is_ok());
        let times = times.lock().unwrap();
        assert!(times[0] < Duration::from_millis(90));
        assert!(times[1] < Duration::from_millis(90));
        assert!(times[2] >= Duration::from_millis(90));
    }
}
