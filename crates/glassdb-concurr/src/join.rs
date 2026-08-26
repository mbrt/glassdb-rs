//! Foreground bounded future collection.

use std::future::Future;
use std::num::NonZeroUsize;

use futures::stream::{self, StreamExt};

/// Runs all supplied futures with bounded admission and returns stable outputs.
///
/// At most `limit` futures are incomplete at one time. Futures are admitted in
/// input order and are polled in the caller's task. Waiting futures continue to
/// consume the limit. Dropping this future drops admitted and stored futures.
pub async fn join_all_bounded<I>(
    futures: I,
    limit: NonZeroUsize,
) -> Vec<<I::Item as Future>::Output>
where
    I: IntoIterator,
    I::Item: Future,
{
    let mut futures = futures.into_iter();
    let Some(first) = futures.next() else {
        return Vec::new();
    };
    let Some(second) = futures.next() else {
        return vec![first.await];
    };

    let mut outputs = stream::iter(
        std::iter::once(first)
            .chain(std::iter::once(second))
            .chain(futures),
    )
    .enumerate()
    .map(|(index, future)| async move { (index, future.await) })
    .buffer_unordered(limit.get())
    .collect::<Vec<_>>()
    .await;
    outputs.sort_unstable_by_key(|(index, _)| *index);
    outputs.into_iter().map(|(_, output)| output).collect()
}

/// Applies `operation` to every input with bounded join semantics.
///
/// Inputs are mapped in input order. The returned futures follow
/// [`join_all_bounded`]'s admission, output-order, and cancellation rules.
pub fn map_all_bounded<I, F, Fut>(
    inputs: I,
    limit: NonZeroUsize,
    operation: F,
) -> impl Future<Output = Vec<Fut::Output>>
where
    I: IntoIterator,
    F: FnMut(I::Item) -> Fut,
    Fut: Future,
{
    join_all_bounded(inputs.into_iter().map(operation), limit)
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::pin::Pin;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use futures::future::{self, BoxFuture, FutureExt};
    use tokio::sync::oneshot;

    #[tokio::test]
    async fn zero_and_one_input_return_directly() {
        let empty = join_all_bounded(Vec::<future::Ready<usize>>::new(), NonZeroUsize::MIN).await;
        assert!(empty.is_empty());

        let one = join_all_bounded([future::ready(7)], NonZeroUsize::new(16).unwrap()).await;
        assert_eq!(one, [7]);
    }

    #[tokio::test]
    async fn maps_inputs_and_returns_stable_outputs() {
        let outputs = map_all_bounded(
            [3, 1, 2],
            NonZeroUsize::new(2).unwrap(),
            |input| async move {
                tokio::task::yield_now().await;
                input * 2
            },
        )
        .await;

        assert_eq!(outputs, [6, 2, 4]);
    }

    #[tokio::test]
    async fn runs_every_input_and_returns_stable_outputs() {
        let (first_tx, first_rx) = oneshot::channel();
        let (second_tx, second_rx) = oneshot::channel();
        let (third_tx, third_rx) = oneshot::channel();
        let futures: Vec<BoxFuture<'static, Result<usize, usize>>> = vec![
            first_rx.map(|result| result.unwrap()).boxed(),
            second_rx.map(|result| result.unwrap()).boxed(),
            third_rx.map(|result| result.unwrap()).boxed(),
        ];
        let joined = tokio::spawn(join_all_bounded(futures, NonZeroUsize::new(2).unwrap()));

        second_tx.send(Err(2)).unwrap();
        tokio::task::yield_now().await;
        third_tx.send(Ok(3)).unwrap();
        first_tx.send(Ok(1)).unwrap();

        assert_eq!(joined.await.unwrap(), [Ok(1), Err(2), Ok(3)]);
    }

    struct CountedFuture {
        polls: Arc<AtomicUsize>,
        drops: Arc<AtomicUsize>,
    }

    impl Future for CountedFuture {
        type Output = ();

        fn poll(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Self::Output> {
            self.polls.fetch_add(1, Ordering::SeqCst);
            Poll::Pending
        }
    }

    impl Drop for CountedFuture {
        fn drop(&mut self) {
            self.drops.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn limits_incomplete_futures_and_drops_all_inputs() {
        let polls = Arc::new(AtomicUsize::new(0));
        let drops = Arc::new(AtomicUsize::new(0));
        let inputs = (0..17)
            .map(|_| CountedFuture {
                polls: polls.clone(),
                drops: drops.clone(),
            })
            .collect::<Vec<_>>();
        let mut joined = Box::pin(join_all_bounded(inputs, NonZeroUsize::new(16).unwrap()));

        assert!(futures::poll!(joined.as_mut()).is_pending());
        assert_eq!(polls.load(Ordering::SeqCst), 16);

        drop(joined);
        assert_eq!(drops.load(Ordering::SeqCst), 17);
    }
}
