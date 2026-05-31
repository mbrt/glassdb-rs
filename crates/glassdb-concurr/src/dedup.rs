//! Mergeable work deduplication. Ported from the Go `concurr.Dedup`.
//!
//! For a given key only one [`DedupWorker`] runs at a time. Concurrent requests
//! that can merge join the in-flight bundle; otherwise they are queued (FIFO)
//! or, if reorderable, parked so they can merge with later work. When the main
//! worker completes, its result is delivered to every merged caller and the
//! next queued request is promoted to main.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::{oneshot, Semaphore};

use crate::ctx::Ctx;
use crate::shard::Sharded;

/// A unit of work that may merge with another request for the same key.
pub trait MergeRequest: Clone + Send + Sync + 'static {
    /// Attempts to merge `self` with `other`, returning the combined request.
    fn merge(&self, other: &Self) -> Option<Self>;
    /// Whether this request may be reordered relative to queued work.
    fn can_reorder(&self) -> bool;
}

/// Performs the actual work for a deduplicated request, using `contr` to fetch
/// the (merged) request and to await newly arriving requests.
#[async_trait]
pub trait DedupWorker<R, E>: Send + Sync
where
    R: MergeRequest,
    E: Send + Sync + 'static,
{
    async fn work(&self, ctx: &Ctx, key: &str, contr: &Controller<R, E>) -> Result<(), E>;
}

/// Error returned by [`Dedup::run`].
#[derive(Debug)]
pub enum DedupError<E> {
    /// The caller's context was cancelled before completion.
    Cancelled,
    /// The work failed; the error is shared across all merged callers.
    Work(Arc<E>),
}

impl<E: std::fmt::Display> std::fmt::Display for DedupError<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DedupError::Cancelled => write!(f, "context canceled"),
            DedupError::Work(e) => write!(f, "{e}"),
        }
    }
}

impl<E: std::fmt::Debug + std::fmt::Display> std::error::Error for DedupError<E> {}

enum Notification<E> {
    /// The receiver has been promoted to the main worker.
    Next,
    /// The bundle completed with this result.
    Done(Result<(), Arc<E>>),
}

struct RequestCtx<R, E> {
    ctx: Ctx,
    request: R,
    notify: Option<oneshot::Sender<Notification<E>>>,
}

struct RequestBundle<R, E> {
    main: Option<RequestCtx<R, E>>,
    other: Vec<RequestCtx<R, E>>,
}

impl<R, E> Default for RequestBundle<R, E> {
    fn default() -> Self {
        RequestBundle {
            main: None,
            other: Vec::new(),
        }
    }
}

struct Call<R, E> {
    curr: RequestBundle<R, E>,
    pending: Vec<RequestCtx<R, E>>,
    queue: Vec<RequestCtx<R, E>>,
    /// Permits accumulate one per newly arrived request; the worker consumes
    /// them via [`Controller::on_next_do`].
    next: Arc<Semaphore>,
}

/// Coordinates deduplicated work for a set of keys. Implements the request
/// reconstruction and "wait for next request" hooks used by workers.
pub struct Controller<R, E> {
    calls: Mutex<HashMap<String, Call<R, E>>>,
}

impl<R, E> Controller<R, E>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
{
    fn new() -> Self {
        Controller {
            calls: Mutex::new(HashMap::new()),
        }
    }

    async fn run<W>(&self, ctx: &Ctx, key: &str, r: R, worker: &W) -> Result<(), DedupError<E>>
    where
        W: DedupWorker<R, E>,
    {
        enum Start<E> {
            Main,
            Wait(oneshot::Receiver<Notification<E>>),
        }

        let start = {
            let mut calls = self.calls.lock().unwrap();
            if let Some(c) = calls.get_mut(key) {
                let (tx, rx) = oneshot::channel();
                let rctx = RequestCtx {
                    ctx: ctx.clone(),
                    request: r.clone(),
                    notify: Some(tx),
                };
                if r.can_reorder() {
                    c.pending.push(rctx);
                } else {
                    c.queue.push(rctx);
                }
                // Signal the running worker that a new request arrived.
                c.next.add_permits(1);
                Start::Wait(rx)
            } else {
                calls.insert(
                    key.to_string(),
                    Call {
                        curr: RequestBundle {
                            main: Some(RequestCtx {
                                ctx: ctx.clone(),
                                request: r,
                                notify: None,
                            }),
                            other: Vec::new(),
                        },
                        pending: Vec::new(),
                        queue: Vec::new(),
                        next: Arc::new(Semaphore::new(0)),
                    },
                );
                Start::Main
            }
        };

        let work_res: Result<(), Arc<E>> = match start {
            Start::Main => worker.work(ctx, key, self).await.map_err(Arc::new),
            Start::Wait(rx) => {
                tokio::select! {
                    biased;
                    _ = ctx.cancelled() => return Err(DedupError::Cancelled),
                    n = rx => match n {
                        Ok(Notification::Next) => worker.work(ctx, key, self).await.map_err(Arc::new),
                        Ok(Notification::Done(res)) => return res.map_err(DedupError::Work),
                        Err(_) => return Err(DedupError::Cancelled),
                    }
                }
            }
        };

        {
            let mut calls = self.calls.lock().unwrap();
            if let Some(c) = calls.get_mut(key) {
                c.curr.main = None;
                // If the context expired, do not notify the whole bundle:
                // another request in there could pick up the work.
                if work_res.is_ok() || !ctx.is_cancelled() {
                    notify_bundle(&mut c.curr, &work_res);
                    c.curr = RequestBundle::default();
                }
                Self::wake_up_next(&mut calls, key);
            }
        }

        work_res.map_err(DedupError::Work)
    }

    /// Reconstructs the merged request for the in-flight bundle, absorbing any
    /// compatible pending/queued requests. Called by the worker.
    pub fn request(&self, key: &str) -> R {
        let mut calls = self.calls.lock().unwrap();
        let c = calls
            .get_mut(key)
            .expect("dedup: request() for unknown key");

        c.curr.other = filter_expired(std::mem::take(&mut c.curr.other));
        c.pending = filter_expired(std::mem::take(&mut c.pending));
        c.queue = filter_expired(std::mem::take(&mut c.queue));

        // Reconstruct the current bundle.
        let mut new_req = c
            .curr
            .main
            .as_ref()
            .expect("dedup: no main request")
            .request
            .clone();
        for r in &c.curr.other {
            new_req = new_req
                .merge(&r.request)
                .expect("dedup: unexpected non-mergeable request in the bundle");
        }

        // Absorb mergeable pending requests (order independent).
        let pending = std::mem::take(&mut c.pending);
        let mut remaining = Vec::new();
        for r in pending {
            match new_req.merge(&r.request) {
                Some(mr) => {
                    new_req = mr;
                    c.curr.other.push(r);
                }
                None => remaining.push(r),
            }
        }
        c.pending = remaining;

        // Absorb from the queue, stopping at the first non-mergeable request.
        let queue = std::mem::take(&mut c.queue);
        let mut leftover = Vec::new();
        let mut it = queue.into_iter();
        for r in it.by_ref() {
            match new_req.merge(&r.request) {
                Some(mr) => {
                    new_req = mr;
                    c.curr.other.push(r);
                }
                None => {
                    leftover.push(r);
                    break;
                }
            }
        }
        leftover.extend(it);
        c.queue = leftover;

        new_req
    }

    /// Returns a semaphore that gains a permit for each newly arrived request,
    /// letting the worker await additional work. Mirrors Go's `OnNextDo`.
    pub fn on_next_do(&self, key: &str) -> Arc<Semaphore> {
        let calls = self.calls.lock().unwrap();
        calls
            .get(key)
            .expect("dedup: on_next_do() for unknown key")
            .next
            .clone()
    }

    fn wake_up_next(calls: &mut HashMap<String, Call<R, E>>, key: &str) {
        let c = calls.get_mut(key).unwrap();
        c.curr.other = filter_expired(std::mem::take(&mut c.curr.other));
        c.pending = filter_expired(std::mem::take(&mut c.pending));
        c.queue = filter_expired(std::mem::take(&mut c.queue));

        let next_main = if !c.curr.other.is_empty() {
            Some(c.curr.other.remove(0))
        } else if !c.queue.is_empty() {
            Some(c.queue.remove(0))
        } else if !c.pending.is_empty() {
            Some(c.pending.remove(0))
        } else {
            None
        };

        match next_main {
            Some(mut rc) => {
                if let Some(tx) = rc.notify.take() {
                    let _ = tx.send(Notification::Next);
                }
                // A fresh signal source for the new main worker.
                c.next = Arc::new(Semaphore::new(0));
                c.curr.main = Some(rc);
            }
            None => {
                calls.remove(key);
            }
        }
    }
}

fn notify_bundle<R, E>(bundle: &mut RequestBundle<R, E>, res: &Result<(), Arc<E>>) {
    for r in &mut bundle.other {
        if let Some(tx) = r.notify.take() {
            let _ = tx.send(Notification::Done(res.clone()));
        }
    }
}

fn filter_expired<R, E>(rs: Vec<RequestCtx<R, E>>) -> Vec<RequestCtx<R, E>> {
    rs.into_iter().filter(|r| !r.ctx.is_cancelled()).collect()
}

/// Awaits a single signal on a "next request" semaphore, consuming the permit.
/// Intended to be used inside a `select!` in worker implementations.
pub async fn await_signal(sem: &Semaphore) {
    if let Ok(p) = sem.acquire().await {
        p.forget();
    }
}

/// Deduplicates and merges concurrent requests for the same key using `W`.
///
/// Requests are partitioned across independent controllers by key hash to
/// reduce lock contention.
pub struct Dedup<R, E, W> {
    worker: W,
    contr: Sharded<Controller<R, E>>,
}

impl<R, E, W> Dedup<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: DedupWorker<R, E>,
{
    /// Creates a new deduplicator backed by `worker`.
    pub fn new(worker: W) -> Self {
        Dedup {
            worker,
            contr: Sharded::new(|_| Controller::new()),
        }
    }

    /// Submits a request for `key`, merging with any in-flight work if possible.
    pub async fn run(&self, ctx: &Ctx, key: &str, r: R) -> Result<(), DedupError<E>> {
        self.contr
            .for_key(key.as_bytes())
            .run(ctx, key, r, &self.worker)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;

    #[derive(Clone)]
    struct TestRequest {
        counter: i64,
        can_merge: bool,
        can_reorder: bool,
    }

    fn mergeable(c: i64) -> TestRequest {
        TestRequest {
            counter: c,
            can_merge: true,
            can_reorder: false,
        }
    }
    fn unmergeable(c: i64) -> TestRequest {
        TestRequest {
            counter: c,
            can_merge: false,
            can_reorder: false,
        }
    }
    fn reorderable(c: i64) -> TestRequest {
        TestRequest {
            counter: c,
            can_merge: true,
            can_reorder: true,
        }
    }

    impl MergeRequest for TestRequest {
        fn merge(&self, other: &Self) -> Option<Self> {
            if !self.can_merge || !other.can_merge {
                return None;
            }
            Some(mergeable(self.counter + other.counter))
        }
        fn can_reorder(&self) -> bool {
            self.can_reorder
        }
    }

    #[derive(Default)]
    struct CounterWorker {
        counter: StdMutex<i64>,
    }

    #[async_trait]
    impl DedupWorker<TestRequest, ()> for CounterWorker {
        async fn work(
            &self,
            ctx: &Ctx,
            key: &str,
            contr: &Controller<TestRequest, ()>,
        ) -> Result<(), ()> {
            let _ = contr.request(key);
            *self.counter.lock().unwrap() += 1;
            ctx.err().map_err(|_| ())
        }
    }

    struct MergeWorker {
        wait_requests: StdMutex<i64>,
        res: StdMutex<Vec<i64>>,
    }

    #[async_trait]
    impl DedupWorker<TestRequest, ()> for MergeWorker {
        async fn work(
            &self,
            ctx: &Ctx,
            key: &str,
            contr: &Controller<TestRequest, ()>,
        ) -> Result<(), ()> {
            loop {
                let remaining = *self.wait_requests.lock().unwrap();
                if remaining <= 0 {
                    break;
                }
                let sem = contr.on_next_do(key);
                tokio::select! {
                    _ = ctx.cancelled() => return Err(()),
                    _ = await_signal(&sem) => {}
                }
                *self.wait_requests.lock().unwrap() -= 1;
            }
            let r = contr.request(key);
            self.res.lock().unwrap().push(r.counter);
            Ok(())
        }
    }

    #[tokio::test]
    async fn single_call() {
        let d = Dedup::new(CounterWorker::default());
        assert!(d.run(&Ctx::background(), "key", mergeable(0)).await.is_ok());
        assert_eq!(*d.worker.counter.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn context_expired() {
        let d = Dedup::new(CounterWorker::default());
        let (ctx, token) = Ctx::with_cancel();
        token.cancel();
        let err = d.run(&ctx, "key", mergeable(0)).await;
        assert!(matches!(err, Err(DedupError::Work(_))));
        assert_eq!(*d.worker.counter.lock().unwrap(), 1);
    }

    #[tokio::test]
    async fn merge_do() {
        let d = Arc::new(Dedup::new(MergeWorker {
            wait_requests: StdMutex::new(1),
            res: StdMutex::new(Vec::new()),
        }));
        let ctx = Ctx::background();
        let d2 = d.clone();
        let ctx2 = ctx.clone();
        let h = tokio::spawn(async move { d2.run(&ctx2, "key", mergeable(1)).await.is_ok() });
        // The current task's run() locks synchronously and becomes the main
        // worker (the spawned task only runs once we await), matching Go's
        // scheduler where the spawning goroutine proceeds first.
        assert!(d.run(&ctx, "key", mergeable(1)).await.is_ok());
        assert!(h.await.unwrap());
        assert_eq!(*d.worker.res.lock().unwrap(), vec![2]);
    }

    #[tokio::test]
    async fn sequential_do() {
        let d = Arc::new(Dedup::new(MergeWorker {
            wait_requests: StdMutex::new(1),
            res: StdMutex::new(Vec::new()),
        }));
        let ctx = Ctx::background();
        let d2 = d.clone();
        let ctx2 = ctx.clone();
        let h = tokio::spawn(async move { d2.run(&ctx2, "key", unmergeable(1)).await.is_ok() });
        assert!(d.run(&ctx, "key", mergeable(1)).await.is_ok());
        assert!(h.await.unwrap());
        assert_eq!(*d.worker.res.lock().unwrap(), vec![1, 1]);
    }

    #[tokio::test]
    async fn reorder_merge() {
        let d = Arc::new(Dedup::new(MergeWorker {
            wait_requests: StdMutex::new(2),
            res: StdMutex::new(Vec::new()),
        }));
        let ctx = Ctx::background();

        let da = d.clone();
        let ca = ctx.clone();
        let ha = tokio::spawn(async move { da.run(&ca, "key", unmergeable(2)).await.is_ok() });
        let db = d.clone();
        let cb = ctx.clone();
        let hb = tokio::spawn(async move { db.run(&cb, "key", reorderable(3)).await.is_ok() });
        assert!(d.run(&ctx, "key", mergeable(5)).await.is_ok());
        assert!(ha.await.unwrap());
        assert!(hb.await.unwrap());
        assert_eq!(*d.worker.res.lock().unwrap(), vec![8, 2]);
    }
}
