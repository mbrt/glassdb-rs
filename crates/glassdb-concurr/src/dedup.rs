//! Mergeable work deduplication.
//!
//! For a given key only one [`Worker`] batch runs at a time. Concurrent requests
//! that can merge join the in-flight batch; otherwise they are queued (FIFO) or,
//! if reorderable, parked so they can merge with later work. When a batch
//! completes, its result is delivered to every merged caller.
//!
//! # Driver model (inline fast path, spawn on handoff)
//!
//! A key is always driven by exactly one *driver*:
//!
//! - the **inline driver** - the first caller for an idle key runs the worker on
//!   its own task (the uncontended common case: no task spawn), or
//! - a **spawned owner task** ([`rt::spawn`]) created only when a handoff is
//!   actually required.
//!
//! The inline driver runs a single batch round, then either removes the key (no
//! more work) or hands the leftover queue off to a freshly spawned owner. A
//! caller dropping/cancelling its future can never strand the key: a queued
//! waiter only ever drops its receiver, and a dropped inline driver hands off to
//! a spawned owner via [`DriverGuard`]. Every handoff target is a fresh task
//! (which a caller cannot drop), so worker liveness is never coupled to a
//! caller-future's lifetime. This is the structural reason orphaned keys and
//! lost handoffs are impossible.

use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::{oneshot, Notify};

use crate::cancel::CancelToken;
use crate::ctx::Ctx;
use crate::rt;
use crate::shard::Sharded;

/// A unit of work that may merge with another request for the same key.
pub trait MergeRequest: Clone + Send + Sync + 'static {
    /// Attempts to merge `self` with `other`, returning the combined request.
    fn merge(&self, other: &Self) -> Option<Self>;
    /// Whether this request may be reordered relative to queued work.
    fn can_reorder(&self) -> bool;
}

/// Performs the actual work for a batch of deduplicated requests on a key.
///
/// `batch` exposes the current merged request ([`BatchHandle::merged`], which
/// absorbs newly-arrived compatible submissions) and a wakeup for fresh work
/// ([`BatchHandle::changed`]). `ctx` is an independent cancellation context owned
/// by the dedup machinery: it is cancelled when no live caller remains for the
/// batch or when the deduplicator is closed - never tied to a single caller.
#[async_trait]
pub trait Worker<R, E>: Send + Sync
where
    R: MergeRequest,
    E: Send + Sync + 'static,
{
    async fn run(&self, ctx: &Ctx, key: &str, batch: &BatchHandle<R, E>) -> Result<(), E>;
}

/// Error returned by [`Dedup::run`].
#[derive(Debug)]
pub enum DedupError<E> {
    /// The caller's context was cancelled (or its future dropped) before
    /// completion.
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

/// A single submitted request awaiting a batch result.
struct Member<R, E> {
    ctx: Ctx,
    request: R,
    done: oneshot::Sender<Result<(), Arc<E>>>,
}

impl<R, E> Member<R, E> {
    /// A member is live while its caller is still interested: the context is not
    /// cancelled and the result receiver has not been dropped.
    fn live(&self) -> bool {
        !self.ctx.is_cancelled() && !self.done.is_closed()
    }
}

/// Per-key coordination state. Lives in the shard map while the key has a
/// driver; removed (atomically, under the shard lock) once there is no more
/// outstanding work.
struct KeyState<R, E> {
    /// Members currently being served by the running worker round.
    batch: Vec<Member<R, E>>,
    /// Reorderable submissions waiting to merge with a future batch.
    pending: Vec<Member<R, E>>,
    /// FIFO submissions waiting their turn.
    queue: Vec<Member<R, E>>,
    /// The merged request for the current batch (kept valid: seeded on creation,
    /// recomputed on every reconstruction).
    merged: R,
    /// Cancellation token for the in-flight worker round, if any. Cancelled when
    /// the batch loses all live members so the worker bails early.
    op_token: Option<CancelToken>,
    /// Notified whenever new work arrives (or a waiter cancels), so the worker
    /// can re-evaluate. A single stored permit is enough: the worker re-absorbs
    /// everything via [`KeyState::reconstruct`] on each wake.
    changed: Arc<Notify>,
}

impl<R, E> KeyState<R, E>
where
    R: MergeRequest,
{
    /// Creates per-key state seeded with the inline driver's own submission.
    fn new(seed: Member<R, E>) -> Self {
        let merged = seed.request.clone();
        KeyState {
            batch: vec![seed],
            pending: Vec::new(),
            queue: Vec::new(),
            merged,
            op_token: None,
            changed: Arc::new(Notify::new()),
        }
    }

    /// Drops members whose callers are no longer interested.
    fn prune(&mut self) {
        self.batch.retain(|m| m.live());
        self.pending.retain(|m| m.live());
        self.queue.retain(|m| m.live());
    }

    /// Reports whether any queued (not yet batched) submission is still live.
    fn incoming_live(&self) -> bool {
        self.pending.iter().any(|m| m.live()) || self.queue.iter().any(|m| m.live())
    }

    /// If the batch is empty, promotes a new seed: the queue front (FIFO) if any,
    /// otherwise any pending (reorderable) submission. Assumes dead members were
    /// already pruned.
    fn promote_seed(&mut self) {
        if !self.queue.is_empty() {
            self.batch.push(self.queue.remove(0));
        } else if !self.pending.is_empty() {
            self.batch.push(self.pending.remove(0));
        }
    }

    /// Recomputes [`KeyState::merged`] from the live batch, absorbing compatible
    /// incoming submissions: every mergeable pending request (order-independent),
    /// then queued requests front-to-back up to the first non-mergeable one (so
    /// write ordering is preserved and reads cannot starve the queue). Returns
    /// `false` if no live member remains.
    fn reconstruct(&mut self) -> bool {
        self.prune();
        if self.batch.is_empty() {
            return false;
        }

        let mut req = self.batch[0].request.clone();
        for m in &self.batch[1..] {
            req = req
                .merge(&m.request)
                .expect("dedup: non-mergeable request inside a batch");
        }

        let pending = std::mem::take(&mut self.pending);
        let mut remaining = Vec::new();
        for m in pending {
            match req.merge(&m.request) {
                Some(mr) => {
                    req = mr;
                    self.batch.push(m);
                }
                None => remaining.push(m),
            }
        }
        self.pending = remaining;

        let queue = std::mem::take(&mut self.queue);
        let mut leftover = Vec::new();
        let mut it = queue.into_iter();
        for m in it.by_ref() {
            match req.merge(&m.request) {
                Some(mr) => {
                    req = mr;
                    self.batch.push(m);
                }
                None => {
                    leftover.push(m);
                    break;
                }
            }
        }
        leftover.extend(it);
        self.queue = leftover;

        self.merged = req;
        true
    }

    /// Prepares the next batch at the start of a round: promotes a seed if the
    /// batch is empty, then reconstructs the merged request. Returns `false` if
    /// there is no live work to do.
    fn build_batch(&mut self) -> bool {
        self.prune();
        if self.batch.is_empty() {
            self.promote_seed();
        }
        self.reconstruct()
    }

    /// Delivers `res` to every member of the current batch and clears it.
    fn deliver(&mut self, res: &Result<(), Arc<E>>) {
        for m in self.batch.drain(..) {
            let _ = m.done.send(res.clone());
        }
    }

    /// Moves still-live, undelivered batch members back into the incoming queues
    /// so a successor driver re-serves them. Used on the inline-driver drop path.
    fn requeue_batch(&mut self) {
        let batch = std::mem::take(&mut self.batch);
        let mut front = Vec::new();
        for m in batch {
            if !m.live() {
                continue;
            }
            if m.request.can_reorder() {
                self.pending.push(m);
            } else {
                front.push(m);
            }
        }
        front.extend(std::mem::take(&mut self.queue));
        self.queue = front;
    }
}

/// One key-space partition guarded by a single lock.
struct Shard<R, E> {
    map: Mutex<HashMap<String, KeyState<R, E>>>,
}

impl<R, E> Shard<R, E> {
    fn new() -> Self {
        Shard {
            map: Mutex::new(HashMap::new()),
        }
    }
}

/// Handle passed to a [`Worker`] for the in-flight batch on a key.
pub struct BatchHandle<R, E> {
    shard: Arc<Shard<R, E>>,
    key: String,
}

impl<R, E> BatchHandle<R, E>
where
    R: MergeRequest,
{
    /// Returns the current merged request, absorbing any newly-arrived
    /// compatible submissions. If every caller for the batch has gone away, the
    /// worker's context is cancelled (so it returns early) and the last known
    /// merged request is returned.
    pub fn merged(&self) -> R {
        let mut map = self.shard.map.lock().unwrap();
        let st = map
            .get_mut(&self.key)
            .expect("dedup: merged() for an unknown key");
        if !st.reconstruct() {
            if let Some(t) = &st.op_token {
                t.cancel();
            }
        }
        st.merged.clone()
    }

    /// Resolves when new work arrives for the key (or a waiter cancels). Intended
    /// for use inside a `select!` in worker implementations.
    pub async fn changed(&self) {
        let notify = {
            let map = self.shard.map.lock().unwrap();
            match map.get(&self.key) {
                Some(st) => st.changed.clone(),
                None => return,
            }
        };
        notify.notified().await;
    }
}

/// Outcome of a single worker round.
enum Round {
    /// A batch was served and its result delivered.
    Delivered,
    /// There was no live work (or the deduplicator is closing); the key entry
    /// was removed.
    Exit,
}

struct Inner<R, E, W> {
    worker: Arc<W>,
    shards: Sharded<Arc<Shard<R, E>>>,
    /// Cancelled by [`Dedup::close`]; every worker round's `op_token` is a child
    /// of this, so closing promptly unblocks in-flight workers.
    shutdown: CancelToken,
    /// Number of live spawned owner tasks, so [`Dedup::close`] can await them.
    active_owners: AtomicUsize,
    /// Notified when the last spawned owner exits.
    owners_idle: Notify,
}

impl<R, E, W> Inner<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    /// Runs one worker round for `key`: builds the batch under the lock, runs the
    /// worker outside it, then delivers the result. Removes the key (returning
    /// [`Round::Exit`]) when there is no live work or the deduplicator is closing.
    async fn drive_one_round(
        self: &Arc<Self>,
        shard: &Arc<Shard<R, E>>,
        key: &str,
        handle: &BatchHandle<R, E>,
    ) -> Round {
        let op_ctx = {
            let mut map = shard.map.lock().unwrap();
            let st = match map.get_mut(key) {
                Some(s) => s,
                None => return Round::Exit,
            };
            if self.shutdown.is_cancelled() || !st.build_batch() {
                map.remove(key);
                return Round::Exit;
            }
            let tok = self.shutdown.child_token();
            st.op_token = Some(tok.clone());
            Ctx::from_token(tok)
        };

        let res = self
            .worker
            .run(&op_ctx, key, handle)
            .await
            .map_err(Arc::new);

        {
            let mut map = shard.map.lock().unwrap();
            if let Some(st) = map.get_mut(key) {
                st.op_token = None;
                st.deliver(&res);
            }
        }
        Round::Delivered
    }

    /// After an inline driver's single round: hand the leftover queue off to a
    /// spawned owner, or remove the key if nothing live remains. Atomic under the
    /// shard lock w.r.t. concurrent submitters.
    fn finish_round(self: &Arc<Self>, shard: &Arc<Shard<R, E>>, key: &str) {
        let mut map = shard.map.lock().unwrap();
        let st = match map.get_mut(key) {
            Some(s) => s,
            None => return,
        };
        st.prune();
        if st.incoming_live() {
            self.spawn_owner(shard, key);
        } else {
            map.remove(key);
        }
    }

    /// Spawns a dedicated owner task to drain the key's queue. Synchronous (no
    /// await), so it is safe to call while holding the shard lock; the spawned
    /// task only runs once the lock is released.
    fn spawn_owner(self: &Arc<Self>, shard: &Arc<Shard<R, E>>, key: &str) {
        self.active_owners.fetch_add(1, Ordering::SeqCst);
        let inner = self.clone();
        let shard = shard.clone();
        let key = key.to_string();
        // Detached: the owner is tracked via `active_owners`/`owners_idle` for
        // `close`, not by joining a handle (which would accumulate per handoff).
        let owner = rt::spawn(async move {
            inner.run_owner(&shard, key).await;
            if inner.active_owners.fetch_sub(1, Ordering::SeqCst) == 1 {
                inner.owners_idle.notify_one();
            }
        });
        drop(owner);
    }

    /// Owner task loop: serves rounds until the queue drains, then exits.
    async fn run_owner(self: &Arc<Self>, shard: &Arc<Shard<R, E>>, key: String) {
        let handle = BatchHandle {
            shard: shard.clone(),
            key: key.clone(),
        };
        loop {
            match self.drive_one_round(shard, &key, &handle).await {
                Round::Delivered => continue,
                Round::Exit => return,
            }
        }
    }

    /// Drives the inline fast path for the first caller of an idle key. Runs one
    /// round on the caller's own task, responsive to the caller's own
    /// cancellation, and returns that caller's own result.
    async fn drive_inline(
        self: &Arc<Self>,
        ctx: &Ctx,
        shard: &Arc<Shard<R, E>>,
        key: &str,
        mut rx: oneshot::Receiver<Result<(), Arc<E>>>,
    ) -> Result<(), DedupError<E>> {
        let handle = BatchHandle {
            shard: shard.clone(),
            key: key.to_string(),
        };
        let mut guard = DriverGuard {
            inner: self.clone(),
            shard: shard.clone(),
            key: key.to_string(),
            armed: true,
        };

        let round = tokio::select! {
            biased;
            r = self.drive_one_round(shard, key, &handle) => Some(r),
            // The caller cancelled its own context: bail and let the (still
            // armed) guard hand off any live waiters to a spawned owner. This
            // keeps the inline driver responsive to its own cancellation without
            // abandoning merged waiters.
            _ = ctx.cancelled() => None,
        };

        match round {
            Some(round) => {
                guard.armed = false;
                if let Round::Delivered = round {
                    self.finish_round(shard, key);
                }
                match rx.try_recv() {
                    Ok(res) => res.map_err(DedupError::Work),
                    // Our own member was pruned (we were cancelled) before
                    // delivery.
                    Err(_) => Err(DedupError::Cancelled),
                }
            }
            None => Err(DedupError::Cancelled),
        }
    }
}

/// RAII handoff for a dropped inline driver. On drop while still armed (the
/// driver's future was dropped or cancelled mid-round), it cancels the in-flight
/// worker, requeues undelivered live members, and either spawns a fresh owner to
/// finish the work or removes the key. Because the successor is a spawned task,
/// not a caller future, the handoff cannot be lost.
struct DriverGuard<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    inner: Arc<Inner<R, E, W>>,
    shard: Arc<Shard<R, E>>,
    key: String,
    armed: bool,
}

impl<R, E, W> Drop for DriverGuard<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        let mut map = self.shard.map.lock().unwrap();
        let st = match map.get_mut(&self.key) {
            Some(s) => s,
            None => return,
        };
        if let Some(t) = st.op_token.take() {
            t.cancel();
        }
        st.requeue_batch();
        st.prune();
        if st.incoming_live() {
            self.inner.spawn_owner(&self.shard, &self.key);
        } else {
            map.remove(&self.key);
        }
    }
}

/// Deduplicates and merges concurrent requests for the same key using `W`.
///
/// Requests are partitioned across independent shards by key hash to reduce lock
/// contention. The uncontended path (one caller per key) runs the worker inline
/// with no task spawn; spawned owner tasks appear only on genuine contention
/// (a handoff to non-mergeable queued work) or when an inline driver is dropped
/// with live waiters.
pub struct Dedup<R, E, W> {
    inner: Arc<Inner<R, E, W>>,
}

impl<R, E, W> Dedup<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    /// Creates a new deduplicator backed by `worker`.
    pub fn new(worker: W) -> Self {
        Dedup {
            inner: Arc::new(Inner {
                worker: Arc::new(worker),
                shards: Sharded::new(|_| Arc::new(Shard::new())),
                shutdown: CancelToken::new(),
                active_owners: AtomicUsize::new(0),
                owners_idle: Notify::new(),
            }),
        }
    }

    /// Submits a request for `key`, merging with any in-flight work if possible.
    ///
    /// Dropping or cancelling the returned future is safe: a queued waiter simply
    /// drops its receiver, and a dropped inline driver hands its work off to a
    /// spawned owner, so neither orphans the key nor strands other callers.
    pub async fn run(&self, ctx: &Ctx, key: &str, r: R) -> Result<(), DedupError<E>> {
        let shard = self.inner.shards.for_key(key.as_bytes()).clone();
        let (tx, rx) = oneshot::channel();
        let reorder = r.can_reorder();
        let member = Member {
            ctx: ctx.clone(),
            request: r,
            done: tx,
        };

        let is_driver = {
            let mut map = shard.map.lock().unwrap();
            match map.get_mut(key) {
                Some(st) => {
                    if reorder {
                        st.pending.push(member);
                    } else {
                        st.queue.push(member);
                    }
                    st.changed.notify_one();
                    false
                }
                None => {
                    map.insert(key.to_string(), KeyState::new(member));
                    true
                }
            }
        };

        if is_driver {
            return self.inner.drive_inline(ctx, &shard, key, rx).await;
        }

        tokio::select! {
            biased;
            r = rx => match r {
                Ok(res) => res.map_err(DedupError::Work),
                Err(_) => Err(DedupError::Cancelled),
            },
            _ = ctx.cancelled() => {
                // Wake the driver so it prunes us promptly and can abandon the
                // batch if we were the last live caller.
                if let Some(st) = shard.map.lock().unwrap().get(key) {
                    st.changed.notify_one();
                }
                Err(DedupError::Cancelled)
            }
        }
    }

    /// Cancels all in-flight work and awaits any spawned owner tasks, so no owner
    /// leaks when the owning component shuts down.
    pub async fn close(&self) {
        self.inner.shutdown.cancel();
        while self.inner.active_owners.load(Ordering::SeqCst) > 0 {
            self.inner.owners_idle.notified().await;
        }
    }

    /// Number of live spawned owner tasks. Test-only behavioral assertion of the
    /// inline fast path (uncontended work spawns nothing).
    #[cfg(test)]
    fn active_owners(&self) -> usize {
        self.inner.active_owners.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex as StdMutex;
    use std::time::Duration;

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

    /// Records the merged counter of each batch it serves. Its first invocation
    /// blocks on `release`, so tests can register waiters before the batch is
    /// read; later invocations run straight through (honoring cancellation).
    struct GatedWorker {
        release: Arc<tokio::sync::Semaphore>,
        calls: StdMutex<i64>,
        done: StdMutex<Vec<i64>>,
    }

    impl GatedWorker {
        fn new() -> Self {
            GatedWorker {
                release: Arc::new(tokio::sync::Semaphore::new(0)),
                calls: StdMutex::new(0),
                done: StdMutex::new(Vec::new()),
            }
        }
    }

    #[async_trait]
    impl Worker<TestRequest, ()> for GatedWorker {
        async fn run(
            &self,
            ctx: &Ctx,
            _key: &str,
            batch: &BatchHandle<TestRequest, ()>,
        ) -> Result<(), ()> {
            let n = {
                let mut c = self.calls.lock().unwrap();
                *c += 1;
                *c
            };
            if n == 1 {
                tokio::select! {
                    biased;
                    _ = ctx.cancelled() => return Err(()),
                    _ = async {
                        if let Ok(p) = self.release.acquire().await {
                            p.forget();
                        }
                    } => {}
                }
            } else if ctx.is_cancelled() {
                return Err(());
            }
            let r = batch.merged();
            self.done.lock().unwrap().push(r.counter);
            Ok(())
        }
    }

    /// Worker that waits (via `changed`) until the merged counter reaches
    /// `target`, exercising mid-flight merging without counting wakeups.
    struct AccumWorker {
        target: i64,
        res: StdMutex<Vec<i64>>,
    }

    #[async_trait]
    impl Worker<TestRequest, ()> for AccumWorker {
        async fn run(
            &self,
            ctx: &Ctx,
            _key: &str,
            batch: &BatchHandle<TestRequest, ()>,
        ) -> Result<(), ()> {
            loop {
                let r = batch.merged();
                if r.counter >= self.target {
                    self.res.lock().unwrap().push(r.counter);
                    return Ok(());
                }
                tokio::select! {
                    biased;
                    _ = ctx.cancelled() => return Err(()),
                    _ = batch.changed() => {}
                }
            }
        }
    }

    #[derive(Default)]
    struct CounterWorker {
        counter: StdMutex<i64>,
    }

    #[async_trait]
    impl Worker<TestRequest, ()> for CounterWorker {
        async fn run(
            &self,
            ctx: &Ctx,
            _key: &str,
            batch: &BatchHandle<TestRequest, ()>,
        ) -> Result<(), ()> {
            let _ = batch.merged();
            *self.counter.lock().unwrap() += 1;
            ctx.err().map_err(|_| ())
        }
    }

    #[tokio::test]
    async fn single_call() {
        let d = Dedup::new(CounterWorker::default());
        assert!(d.run(&Ctx::background(), "key", mergeable(0)).await.is_ok());
        assert_eq!(*d.inner.worker.counter.lock().unwrap(), 1);
    }

    // Uncontended work runs inline: a lone caller per key never spawns an owner.
    #[tokio::test]
    async fn uncontended_runs_inline_without_spawn() {
        let d = Dedup::new(CounterWorker::default());
        for i in 0..5 {
            assert!(d.run(&Ctx::background(), "key", mergeable(i)).await.is_ok());
        }
        assert_eq!(d.active_owners(), 0, "uncontended work should not spawn");
    }

    #[tokio::test]
    async fn context_expired() {
        let d = Dedup::new(CounterWorker::default());
        let (ctx, token) = Ctx::with_cancel();
        token.cancel();
        let err = d.run(&ctx, "key", mergeable(0)).await;
        // The inline driver bails on its own cancellation before running.
        assert!(matches!(err, Err(DedupError::Cancelled)), "got {err:?}");
    }

    #[tokio::test]
    async fn merge_do() {
        let d = Arc::new(Dedup::new(AccumWorker {
            target: 2,
            res: StdMutex::new(Vec::new()),
        }));
        let ctx = Ctx::background();

        // A becomes the inline driver and waits for the merged total to reach 2.
        let mut a = Box::pin(d.run(&ctx, "key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        // B merges in.
        let mut b = Box::pin(d.run(&ctx, "key", mergeable(1)));
        assert!(futures::poll!(b.as_mut()).is_pending());

        assert!(a.await.is_ok());
        assert!(b.await.is_ok());
        assert_eq!(*d.inner.worker.res.lock().unwrap(), vec![2]);
        assert_eq!(d.active_owners(), 0);
    }

    #[tokio::test]
    async fn sequential_do() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));
        let release = d.inner.worker.release.clone();
        let ctx = Ctx::background();

        // A is the inline driver (gated); B queues behind it (non-mergeable).
        let mut a = Box::pin(d.run(&ctx, "key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut b = Box::pin(d.run(&ctx, "key", unmergeable(1)));
        assert!(futures::poll!(b.as_mut()).is_pending());

        // Release A: it serves its own batch, then hands B off to a spawned owner.
        release.add_permits(1);
        assert!(a.await.is_ok());
        assert!(b.await.is_ok());
        assert_eq!(*d.inner.worker.done.lock().unwrap(), vec![1, 1]);
    }

    // A non-mergeable leftover is handed off to a spawned owner.
    #[tokio::test]
    async fn handoff_spawns_owner() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));
        let release = d.inner.worker.release.clone();
        let ctx = Ctx::background();

        let mut a = Box::pin(d.run(&ctx, "key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut b = Box::pin(d.run(&ctx, "key", unmergeable(2)));
        assert!(futures::poll!(b.as_mut()).is_pending());

        release.add_permits(1);
        assert!(a.await.is_ok());
        // B is drained by a spawned owner.
        assert!(b.await.is_ok());
        // After draining, the owner exits.
        d.close().await;
        assert_eq!(d.active_owners(), 0);
        assert_eq!(*d.inner.worker.done.lock().unwrap(), vec![1, 2]);
    }

    #[tokio::test]
    async fn reorder_merge() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));
        let release = d.inner.worker.release.clone();
        let ctx = Ctx::background();

        // Seed (gated), a non-mergeable queued request, and a reorderable one.
        let mut a = Box::pin(d.run(&ctx, "key", mergeable(5)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut wa = Box::pin(d.run(&ctx, "key", unmergeable(2)));
        assert!(futures::poll!(wa.as_mut()).is_pending());
        let mut wb = Box::pin(d.run(&ctx, "key", reorderable(3)));
        assert!(futures::poll!(wb.as_mut()).is_pending());

        // Release: the seed merges the reorderable (5+3=8); the non-mergeable (2)
        // is handed off to an owner.
        release.add_permits(1);
        assert!(a.await.is_ok());
        assert!(wb.await.is_ok());
        assert!(wa.await.is_ok());
        assert_eq!(*d.inner.worker.done.lock().unwrap(), vec![8, 2]);
    }

    // Every merged caller receives the same batch result.
    #[tokio::test]
    async fn result_fans_out_to_all_mergeable_callers() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));
        let release = d.inner.worker.release.clone();
        let ctx = Ctx::background();

        let mut a = Box::pin(d.run(&ctx, "key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut waiters: Vec<_> = (0..4)
            .map(|_| Box::pin(d.run(&ctx, "key", mergeable(1))))
            .collect();
        for w in &mut waiters {
            assert!(futures::poll!(w.as_mut()).is_pending());
        }

        release.add_permits(1);
        assert!(a.await.is_ok());
        for w in waiters {
            assert!(w.await.is_ok());
        }
        // One batch served all five mergeable callers: 1 + 4 = 5.
        assert_eq!(*d.inner.worker.done.lock().unwrap(), vec![5]);
        assert_eq!(d.active_owners(), 0);
    }

    // Regression (defect A): dropping the inline driver mid-round must hand the
    // queued waiter off to a spawned owner instead of orphaning the key.
    #[tokio::test]
    async fn dropped_inline_driver_with_waiters_spawns_owner() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));
        let ctx = Ctx::background();

        let mut a = Box::pin(d.run(&ctx, "key", unmergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut b = Box::pin(d.run(&ctx, "key", unmergeable(2)));
        assert!(futures::poll!(b.as_mut()).is_pending());

        // Drop A mid-round: the guard hands B off to a fresh owner.
        drop(a);

        let r = tokio::time::timeout(Duration::from_secs(2), b).await;
        assert!(
            matches!(r, Ok(Ok(()))),
            "dropped driver orphaned the key: {r:?}"
        );
        assert_eq!(*d.inner.worker.done.lock().unwrap(), vec![2]);
        d.close().await;
        assert_eq!(d.active_owners(), 0);
    }

    // Regression (defect C): a queued waiter whose future is dropped (receiver
    // gone, context still live) must be pruned, not stranded.
    #[tokio::test]
    async fn dropped_waiter_future_does_not_orphan_key() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));
        let release = d.inner.worker.release.clone();
        let ctx = Ctx::background();

        let mut a = Box::pin(d.run(&ctx, "key", unmergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        {
            let mut b = Box::pin(d.run(&ctx, "key", unmergeable(2)));
            assert!(futures::poll!(b.as_mut()).is_pending());
        }

        release.add_permits(1);
        let ra = tokio::time::timeout(Duration::from_secs(2), a).await;
        assert!(matches!(ra, Ok(Ok(()))), "driver did not finish: {ra:?}");

        // The key is free for a fresh request.
        let rc =
            tokio::time::timeout(Duration::from_secs(2), d.run(&ctx, "key", unmergeable(3))).await;
        assert!(matches!(rc, Ok(Ok(()))), "key was orphaned: {rc:?}");
    }

    // A queued waiter that cancels its context is pruned and never orphans the
    // key.
    #[tokio::test]
    async fn cancelled_waiter_does_not_orphan_key() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));
        let release = d.inner.worker.release.clone();
        let ctx = Ctx::background();

        let mut a = Box::pin(d.run(&ctx, "key", unmergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let (ctx_b, token_b) = Ctx::with_cancel();
        let mut b = Box::pin(d.run(&ctx_b, "key", unmergeable(2)));
        assert!(futures::poll!(b.as_mut()).is_pending());

        // Cancel B before it is served.
        token_b.cancel();
        let rb = tokio::time::timeout(Duration::from_secs(2), b).await;
        assert!(
            matches!(rb, Ok(Err(DedupError::Cancelled))),
            "cancelled waiter: {rb:?}"
        );

        // Release A and confirm the key is reusable.
        release.add_permits(1);
        assert!(a.await.is_ok());
        let rc =
            tokio::time::timeout(Duration::from_secs(2), d.run(&ctx, "key", unmergeable(3))).await;
        assert!(matches!(rc, Ok(Ok(()))), "key was orphaned: {rc:?}");
        d.close().await;
        assert_eq!(d.active_owners(), 0);
    }

    // When every member of a batch is cancelled mid-flight, the worker's context
    // is cancelled so it abandons the work.
    #[tokio::test]
    async fn all_members_cancelled_abandons_batch() {
        let d = Arc::new(Dedup::new(AccumWorker {
            // Never reachable: forces the worker to wait on `changed` until its
            // context is cancelled.
            target: i64::MAX,
            res: StdMutex::new(Vec::new()),
        }));
        let (ctx, token) = Ctx::with_cancel();

        let mut a = Box::pin(d.run(&ctx, "key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut b = Box::pin(d.run(&ctx, "key", mergeable(1)));
        assert!(futures::poll!(b.as_mut()).is_pending());

        // Cancel everyone: the inline driver bails on its own context.
        token.cancel();
        let ra = tokio::time::timeout(Duration::from_secs(2), a).await;
        let rb = tokio::time::timeout(Duration::from_secs(2), b).await;
        assert!(matches!(ra, Ok(Err(DedupError::Cancelled))), "a: {ra:?}");
        assert!(matches!(rb, Ok(Err(DedupError::Cancelled))), "b: {rb:?}");

        // No work was recorded and nothing leaked.
        assert!(d.inner.worker.res.lock().unwrap().is_empty());
        d.close().await;
        assert_eq!(d.active_owners(), 0);
    }
}
