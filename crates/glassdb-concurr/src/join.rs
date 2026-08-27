//! Foreground bounded future collection.

use std::future::Future;
use std::iter;
use std::num::NonZeroUsize;

use futures::FutureExt;
use futures::stream::{FuturesUnordered, StreamExt};

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
    let mut inputs = futures.into_iter();
    let Some(first) = inputs.next() else {
        return Vec::new();
    };
    let Some(second) = inputs.next() else {
        return vec![first.await];
    };

    let mut remaining = iter::once(first).chain(iter::once(second)).chain(inputs);
    let mut outputs: Vec<Option<<I::Item as Future>::Output>> = Vec::new();
    let mut running = FuturesUnordered::new();
    loop {
        while running.len() < limit.get()
            && let Some(future) = remaining.next()
        {
            let position = outputs.len();
            outputs.push(None);
            running.push(future.map(move |output| (position, output)));
        }
        let Some((position, output)) = running.next().await else {
            break;
        };
        outputs[position] = Some(output);
    }
    outputs
        .into_iter()
        .map(|output| output.expect("an admitted future deposits its output"))
        .collect()
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

    use std::cell::Cell;
    use std::pin::Pin;
    use std::rc::Rc;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::task::{Context, Poll};

    use futures::future::{self, BoxFuture, FutureExt};
    use tokio::sync::Notify;
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

    #[tokio::test]
    async fn completion_refills_the_bound_for_local_futures() {
        let admitted = Rc::new(Cell::new(0));
        let release = Arc::new(Notify::new());
        let inputs = (0..4)
            .map(|value| {
                let admitted = admitted.clone();
                let release = release.clone();
                async move {
                    admitted.set(admitted.get() + 1);
                    if value == 0 {
                        release.notified().await;
                    }
                    value
                }
            })
            .collect::<Vec<_>>();
        let mut joined = Box::pin(join_all_bounded(inputs, NonZeroUsize::new(2).unwrap()));

        assert!(futures::poll!(joined.as_mut()).is_pending());
        assert_eq!(admitted.get(), 4, "completed inputs refill the bound");
        release.notify_one();
        assert_eq!(joined.await, [0, 1, 2, 3]);
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
