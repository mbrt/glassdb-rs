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

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use tokio::sync::{Notify, oneshot};
use tokio_util::sync::CancellationToken;

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
/// ([`BatchHandle::changed`]).
///
/// # Cancel-safety contract
///
/// `run` must be cancel-safe: the dedup machinery drops the future at its
/// current `.await` whenever it needs to abort the round (the deduplicator
/// closed, or no live caller remains for the batch). Implementations must
/// therefore hold no invariants across `.await` points that require running
/// an `Err`-arm to clean up; if there is per-iteration state to settle, do it
/// synchronously before the next `.await`.
#[async_trait]
pub trait Worker<R, E>: Send + Sync
where
    R: MergeRequest,
    E: Send + Sync + 'static,
{
    async fn run(&self, key: &str, batch: &BatchHandle<R, E>) -> Result<(), E>;
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
    request: R,
    done: oneshot::Sender<Result<(), Arc<E>>>,
}

impl<R, E> Member<R, E> {
    /// A member is live while its caller is still interested. With cancellation
    /// modelled as future-drop, that's identical to "the result receiver has
    /// not been dropped": dropping the `run` future drops the `oneshot`
    /// receiver, which `is_closed` observes.
    fn live(&self) -> bool {
        !self.done.is_closed()
    }
}

/// Queue and compatible-batch policy for one key.
struct KeyQueue<R, E> {
    /// Members currently being served by the running worker round.
    batch: Vec<Member<R, E>>,
    /// Reorderable submissions waiting to merge with a future batch.
    reorderable: VecDeque<Member<R, E>>,
    /// FIFO submissions waiting their turn.
    fifo: VecDeque<Member<R, E>>,
    /// The merged request for the current batch (kept valid: seeded on creation,
    /// recomputed on every reconstruction).
    merged: R,
}

impl<R, E> KeyQueue<R, E>
where
    R: MergeRequest,
{
    /// Creates a queue seeded with the inline driver's own submission.
    fn new(seed: Member<R, E>) -> Self {
        let merged = seed.request.clone();
        KeyQueue {
            batch: vec![seed],
            reorderable: VecDeque::new(),
            fifo: VecDeque::new(),
            merged,
        }
    }

    /// Queues a submission according to its ordering constraint.
    fn enqueue(&mut self, member: Member<R, E>, can_reorder: bool) {
        if can_reorder {
            self.reorderable.push_back(member);
        } else {
            self.fifo.push_back(member);
        }
    }

    /// Drops members whose callers are no longer interested without disturbing
    /// the order of the survivors.
    fn prune(&mut self) {
        self.batch.retain(|member| member.live());
        self.reorderable.retain(|member| member.live());
        self.fifo.retain(|member| member.live());
    }

    /// Reports whether a future batch has queued work.
    fn has_incoming_live(&mut self) -> bool {
        self.prune();
        !self.reorderable.is_empty() || !self.fifo.is_empty()
    }

    /// Refreshes the active batch with compatible queued work. An empty active
    /// batch stays empty so its in-flight operation can be cancelled before a
    /// queued seed starts a fresh round.
    fn refresh_batch(&mut self) -> bool {
        self.prune();
        self.merge_compatible()
    }

    /// Forms the batch for a worker round, preferring the FIFO front as its seed.
    fn form_batch(&mut self) -> bool {
        self.prune();
        if self.batch.is_empty() {
            let seed = self
                .fifo
                .pop_front()
                .or_else(|| self.reorderable.pop_front());
            if let Some(seed) = seed {
                self.batch.push(seed);
            }
        }
        // Preserve the second liveness boundary before a round starts: a
        // promoted caller can disappear while the queue is being rebuilt.
        self.refresh_batch()
    }

    /// Recomputes the merged request and absorbs compatible queued work.
    fn merge_compatible(&mut self) -> bool {
        if self.batch.is_empty() {
            return false;
        }

        let mut merged = self.batch[0].request.clone();
        for member in &self.batch[1..] {
            merged = merged
                .merge(&member.request)
                .expect("dedup: non-mergeable request inside a batch");
        }

        // Each reorderable candidate present on entry gets one chance. Failed
        // candidates retain their relative order for the next refresh.
        let reorderable = self.reorderable.len();
        for _ in 0..reorderable {
            let member = self
                .reorderable
                .pop_front()
                .expect("dedup: reorderable scan exceeded its bound");
            match merged.merge(&member.request) {
                Some(next) => {
                    merged = next;
                    self.batch.push(member);
                }
                None => self.reorderable.push_back(member),
            }
        }

        // An incompatible FIFO member is an ordering barrier: work behind it
        // cannot join this batch even if it would otherwise be compatible.
        while let Some(member) = self.fifo.front() {
            let Some(next) = merged.merge(&member.request) else {
                break;
            };
            merged = next;
            self.batch.push(
                self.fifo
                    .pop_front()
                    .expect("dedup: observed FIFO front disappeared"),
            );
        }

        self.merged = merged;
        true
    }

    /// Delivers a result to the active batch and returns its member count.
    fn close_batch(&mut self, res: &Result<(), Arc<E>>) -> usize {
        let delivered = self.batch.len();
        for member in self.batch.drain(..) {
            let _ = member.done.send(res.clone());
        }
        delivered
    }

    /// Abandons an active batch after every member has stopped waiting.
    fn abandon_batch(&mut self) {
        self.batch.clear();
    }

    /// Returns live undelivered members to the queues for a successor driver.
    fn requeue_batch(&mut self) {
        let mut strict = VecDeque::new();
        for member in self.batch.drain(..) {
            if !member.live() {
                continue;
            }
            if member.request.can_reorder() {
                self.reorderable.push_back(member);
            } else {
                strict.push_back(member);
            }
        }
        strict.append(&mut self.fifo);
        self.fifo = strict;
    }

    fn merged(&self) -> &R {
        &self.merged
    }

    fn batch_len(&self) -> usize {
        self.batch.len()
    }

    fn reorderable_len(&self) -> usize {
        self.reorderable.len()
    }

    fn fifo_len(&self) -> usize {
        self.fifo.len()
    }
}

/// Per-key coordination state. Lives in the shard map while the key has a
/// driver; removed (atomically, under the shard lock) once there is no more
/// outstanding work.
struct KeyState<R, E> {
    queue: KeyQueue<R, E>,
    /// Abort signal for the in-flight worker round, if any. Cancelled when the
    /// batch loses all live members so the [`Inner::drive_one_round`]
    /// `select!` drops the worker future at its next `.await`.
    op_signal: Option<CancellationToken>,
    /// Notified whenever new work arrives (or a waiter cancels), so the worker
    /// can re-evaluate. A single stored permit is enough: the worker re-absorbs
    /// everything via [`KeyQueue::refresh_batch`] on each wake.
    changed: Arc<Notify>,
}

impl<R, E> KeyState<R, E>
where
    R: MergeRequest,
{
    /// Creates per-key state seeded with the inline driver's own submission.
    fn new(seed: Member<R, E>) -> Self {
        KeyState {
            queue: KeyQueue::new(seed),
            op_signal: None,
            changed: Arc::new(Notify::new()),
        }
    }
}

/// One key-space partition guarded by a single lock.
struct Shard<R, E> {
    map: Mutex<HashMap<String, KeyState<R, E>>>,
    submissions: AtomicU64,
    rounds: AtomicU64,
}

impl<R, E> Shard<R, E> {
    fn new() -> Self {
        Shard {
            map: Mutex::new(HashMap::new()),
            submissions: AtomicU64::new(0),
            rounds: AtomicU64::new(0),
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
    /// compatible submissions. If every caller for the batch has gone away,
    /// the round's [`CancellationToken`] is fired so the outer `select!` in
    /// [`Inner::drive_one_round`] drops the worker future at its next
    /// `.await`. The (now-stale) merged request is still returned so the
    /// worker has something to inspect for the rest of its current poll.
    pub fn merged(&self) -> R {
        let mut map = self.shard.map.lock().unwrap();
        let st = map
            .get_mut(&self.key)
            .expect("dedup: merged() for an unknown key");
        if !st.queue.refresh_batch()
            && let Some(s) = &st.op_signal
        {
            s.cancel();
        }
        st.queue.merged().clone()
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

/// Diagnostic snapshot of one key's coordination state inside a [`Dedup`].
///
/// Returned by [`Dedup::snapshot`] for operators investigating hangs: a key
/// that stays in the snapshot with `has_active_op = true` and a non-zero queue
/// would indicate a stuck worker; a key with `has_active_op = false` and
/// non-zero `batch_count` would indicate the round delivered but post-round
/// cleanup did not run. Both are signatures of orphan-key hangs the dedup
/// driver model is designed to prevent.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DedupKeySnapshot {
    pub key: String,
    /// Members currently being served by an in-flight worker round (or, after
    /// delivery and before round-end cleanup, transiently zero).
    pub batch_count: usize,
    /// Reorderable submissions queued for a future round.
    pub pending_count: usize,
    /// FIFO submissions queued for a future round.
    pub queue_count: usize,
    /// `true` if a worker round is in flight for this key.
    pub has_active_op: bool,
}

/// Cumulative work accepted and rounds started by a [`Dedup`].
///
/// The ratio of `submissions` to `rounds` describes how much compatible work
/// was coalesced. Cancelled submissions remain submissions: they consumed
/// coordination work even if their caller stopped waiting for the result.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DedupStats {
    pub submissions: u64,
    pub rounds: u64,
}

struct Inner<R, E, W> {
    worker: Arc<W>,
    shards: Sharded<Arc<Shard<R, E>>>,
    /// Fired by [`Dedup::close`]. The outer `select!` in
    /// [`Inner::drive_one_round`] watches this and drops the worker future
    /// at its next `.await` when shutdown lands.
    shutdown: CancellationToken,
    /// Number of live spawned owner tasks, so [`Dedup::close`] can await them.
    active_owners: AtomicUsize,
    /// Notified when the last spawned owner exits.
    owners_idle: Notify,
}

/// What happened to the worker future inside [`Inner::drive_one_round`]'s
/// `select!`.
enum WorkerOutcome<E> {
    /// Worker ran to completion; this is the result it produced.
    Done(Result<(), Arc<E>>),
    /// `BatchHandle::merged` cancelled the per-round abort signal because no
    /// live batch member remained; the worker future was dropped. The owner
    /// loop continues so a fresh batch (built from the incoming queues, if any)
    /// gets its own round.
    Liveness,
    /// [`Dedup::close`] fired global shutdown; the worker future was dropped
    /// and we abandon the key entirely.
    Shutdown,
}

impl<R, E, W> Inner<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    /// Runs one worker round for `key`: builds the batch under the lock, races
    /// the worker future against the per-round and global abort signals
    /// outside it, then delivers (or abandons) the result. Removes the key
    /// (returning [`Round::Exit`]) when there is no live work, when the
    /// deduplicator is shutting down, or when the worker bailed mid-round
    /// because shutdown raced it.
    async fn drive_one_round(
        self: &Arc<Self>,
        shard: &Arc<Shard<R, E>>,
        key: &str,
        handle: &BatchHandle<R, E>,
    ) -> Round {
        let op_signal = {
            let mut map = shard.map.lock().unwrap();
            let st = match map.get_mut(key) {
                Some(s) => s,
                None => {
                    tracing::trace!(target: "glassdb::dedup", key, "round_exit_no_state");
                    return Round::Exit;
                }
            };
            if self.shutdown.is_cancelled() || !st.queue.form_batch() {
                map.remove(key);
                tracing::trace!(target: "glassdb::dedup", key, "key_removed");
                return Round::Exit;
            }
            shard.rounds.fetch_add(1, Ordering::Relaxed);
            let signal = CancellationToken::new();
            st.op_signal = Some(signal.clone());
            tracing::trace!(
                target: "glassdb::dedup",
                key,
                batch_count = st.queue.batch_len(),
                pending_count = st.queue.reorderable_len(),
                queue_count = st.queue.fifo_len(),
                "round_start",
            );
            signal
        };

        // Drop-the-future cancellation. The worker is a plain cancel-safe
        // async fn (see `Worker` trait contract); whichever arm wins, the
        // others are dropped at their current `.await` point. The worker
        // never observes a cancellation token in-band.
        let outcome = tokio::select! {
            biased;
            _ = self.shutdown.cancelled() => WorkerOutcome::Shutdown,
            _ = op_signal.cancelled() => WorkerOutcome::Liveness,
            res = self.worker.run(key, handle) => WorkerOutcome::Done(res.map_err(Arc::new)),
        };

        let mut map = shard.map.lock().unwrap();
        match outcome {
            WorkerOutcome::Done(res) => {
                if let Some(st) = map.get_mut(key) {
                    st.op_signal = None;
                    let delivered = st.queue.close_batch(&res);
                    tracing::trace!(
                        target: "glassdb::dedup",
                        key,
                        delivered,
                        is_err = res.is_err(),
                        "round_delivered",
                    );
                }
                Round::Delivered
            }
            WorkerOutcome::Liveness => {
                // Dead batch members go with their senders when we clear the
                // batch: every waiter the round was serving had already
                // dropped its receiver (that is what fired the signal), so
                // there is nothing to deliver. The caller of `drive_inline`
                // / `run_owner` will retry: another round will either build
                // a fresh batch from the incoming queues or remove the key.
                if let Some(st) = map.get_mut(key) {
                    st.op_signal = None;
                    st.queue.abandon_batch();
                    tracing::trace!(target: "glassdb::dedup", key, "round_liveness_abort");
                }
                Round::Delivered
            }
            WorkerOutcome::Shutdown => {
                // Tear down: dropping the `KeyState` drops every member's
                // `oneshot::Sender`, so each caller's `rx.await` resolves to
                // `RecvError` and `Dedup::run` returns `DedupError::Cancelled`.
                map.remove(key);
                tracing::trace!(target: "glassdb::dedup", key, "round_shutdown");
                Round::Exit
            }
        }
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
        if st.queue.has_incoming_live() {
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
        tracing::trace!(target: "glassdb::dedup", key = %key, "spawn_owner");
        // Detached: the owner is tracked via `active_owners`/`owners_idle` for
        // `close`, not by joining a handle (which would accumulate per handoff).
        let owner = rt::spawn(async move {
            inner.run_owner(&shard, key).await;
            if inner.active_owners.fetch_sub(1, Ordering::SeqCst) == 1 {
                inner.owners_idle.notify_waiters();
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
    /// round on the caller's own task and returns that caller's own result.
    ///
    /// If the surrounding `run` future is dropped mid-round, the
    /// [`DriverGuard`] (kept armed until success) runs on drop and hands any
    /// live waiters off to a spawned owner, so cancellation is just
    /// future-drop.
    async fn drive_inline(
        self: &Arc<Self>,
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

        let round = self.drive_one_round(shard, key, &handle).await;
        guard.armed = false;
        if let Round::Delivered = round {
            self.finish_round(shard, key);
        }
        match rx.try_recv() {
            Ok(res) => res.map_err(DedupError::Work),
            // Our own member was pruned (e.g. by shutdown) before delivery.
            Err(_) => Err(DedupError::Cancelled),
        }
    }
}

/// RAII handoff for a dropped inline driver. On drop while still armed (the
/// driver's future was dropped or cancelled mid-round), it requeues undelivered
/// live members, and either spawns a fresh owner to finish the work or removes
/// the key. Because the successor is a spawned task, not a caller future, the
/// handoff cannot be lost.
///
/// Note: the worker future is part of the inline driver's own future tree, so
/// it is already being dropped by the time `drop` runs. We just clear the
/// per-round `op_signal` for bookkeeping; there is no separate cancel to issue.
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
            None => {
                tracing::debug!(
                    target: "glassdb::dedup",
                    key = %self.key,
                    "inline_driver_dropped_no_state",
                );
                return;
            }
        };
        let had_op = st.op_signal.take().is_some();
        st.queue.requeue_batch();
        if st.queue.has_incoming_live() {
            tracing::debug!(
                target: "glassdb::dedup",
                key = %self.key,
                had_op,
                pending_count = st.queue.reorderable_len(),
                queue_count = st.queue.fifo_len(),
                "inline_driver_dropped_handoff",
            );
            self.inner.spawn_owner(&self.shard, &self.key);
        } else {
            tracing::debug!(
                target: "glassdb::dedup",
                key = %self.key,
                had_op,
                "inline_driver_dropped_key_removed",
            );
            map.remove(&self.key);
        }
    }
}

/// Disarms after the waiter receives its result. On drop while armed (the
/// `run` future was cancelled mid-wait), it pokes the per-key `changed`
/// notifier so the driver re-evaluates liveness and can abandon the batch if
/// no caller remains. Without it, the driver might sit indefinitely in
/// `BatchHandle::changed`.
struct WaiterDropGuard {
    changed: Arc<Notify>,
    armed: bool,
}

impl Drop for WaiterDropGuard {
    fn drop(&mut self) {
        if self.armed {
            self.changed.notify_one();
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
                shutdown: CancellationToken::new(),
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
    pub async fn run(&self, key: &str, r: R) -> Result<(), DedupError<E>> {
        let shard = self.inner.shards.for_key(key.as_bytes()).clone();
        shard.submissions.fetch_add(1, Ordering::Relaxed);
        let (tx, rx) = oneshot::channel();
        let can_reorder = r.can_reorder();
        let member = Member {
            request: r,
            done: tx,
        };

        let (is_driver, changed) = {
            let mut map = shard.map.lock().unwrap();
            match map.get_mut(key) {
                Some(st) => {
                    st.queue.enqueue(member, can_reorder);
                    st.changed.notify_one();
                    (false, st.changed.clone())
                }
                None => {
                    let st = KeyState::new(member);
                    let changed = st.changed.clone();
                    map.insert(key.to_string(), st);
                    (true, changed)
                }
            }
        };

        if is_driver {
            return self.inner.drive_inline(&shard, key, rx).await;
        }

        // If a queued waiter is dropped mid-wait, its `oneshot::Receiver` goes
        // with it; the guard wakes the driver so it can prune the dead member
        // promptly (and abandon the batch if no live caller remains).
        let mut guard = WaiterDropGuard {
            changed,
            armed: true,
        };
        let out = match rx.await {
            Ok(res) => res.map_err(DedupError::Work),
            Err(_) => Err(DedupError::Cancelled),
        };
        guard.armed = false;
        out
    }

    /// Aborts all in-flight worker rounds (by dropping their futures via the
    /// shutdown signal) and awaits any spawned owner tasks, so no owner leaks
    /// when the owning component shuts down.
    pub async fn close(&self) {
        self.inner.shutdown.cancel();
        loop {
            let owners_idle = self.inner.owners_idle.notified();
            if self.inner.active_owners.load(Ordering::SeqCst) == 0 {
                return;
            }
            owners_idle.await;
        }
    }

    /// Returns a per-key diagnostic snapshot of the deduplicator's coordination
    /// state. Pull-only and zero cost unless called: takes each shard's lock
    /// briefly, copies the keys and their (batch / pending / queue / op-token)
    /// counts, and returns. Output is sorted by key for stable display.
    pub fn snapshot(&self) -> Vec<DedupKeySnapshot> {
        let mut out = Vec::new();
        self.inner.shards.each(|shard| {
            let map = shard.map.lock().unwrap();
            for (key, st) in map.iter() {
                out.push(DedupKeySnapshot {
                    key: key.clone(),
                    batch_count: st.queue.batch_len(),
                    pending_count: st.queue.reorderable_len(),
                    queue_count: st.queue.fifo_len(),
                    has_active_op: st.op_signal.is_some(),
                });
            }
        });
        out.sort_by(|a, b| a.key.cmp(&b.key));
        out
    }

    /// Returns and resets cumulative submission and worker-round counts.
    pub fn stats_and_reset(&self) -> DedupStats {
        let mut out = DedupStats::default();
        self.inner.shards.each(|shard| {
            out.submissions += shard.submissions.swap(0, Ordering::Relaxed);
            out.rounds += shard.rounds.swap(0, Ordering::Relaxed);
        });
        out
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

    #[test]
    fn requeue_preserves_ordering_classes() {
        let mut results = Vec::new();
        let mut member = |request| {
            let (done, result) = oneshot::channel::<Result<(), Arc<()>>>();
            results.push(result);
            Member { request, done }
        };

        let mut queue = KeyQueue::new(member(mergeable(1)));
        queue.enqueue(member(reorderable(2)), true);
        assert!(queue.refresh_batch());
        queue.enqueue(member(unmergeable(3)), false);
        queue.enqueue(member(reorderable(4)), true);

        queue.requeue_batch();

        assert!(queue.batch.is_empty());
        assert_eq!(
            queue
                .fifo
                .iter()
                .map(|member| member.request.counter)
                .collect::<Vec<_>>(),
            vec![1, 3],
        );
        assert_eq!(
            queue
                .reorderable
                .iter()
                .map(|member| member.request.counter)
                .collect::<Vec<_>>(),
            vec![4, 2],
        );
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
        async fn run(&self, _key: &str, batch: &BatchHandle<TestRequest, ()>) -> Result<(), ()> {
            let n = {
                let mut c = self.calls.lock().unwrap();
                *c += 1;
                *c
            };
            // The first call gates on `release`; subsequent ones flow
            // through. Cancellation is by future-drop: if the dedup
            // machinery aborts the round, this `acquire().await` is
            // dropped at its await point.
            if n == 1
                && let Ok(p) = self.release.acquire().await
            {
                p.forget();
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
        async fn run(&self, _key: &str, batch: &BatchHandle<TestRequest, ()>) -> Result<(), ()> {
            loop {
                let r = batch.merged();
                if r.counter >= self.target {
                    self.res.lock().unwrap().push(r.counter);
                    return Ok(());
                }
                // No in-band cancel check: if the dedup machinery aborts the
                // round (no live members, or shutdown), this `.await` is
                // dropped.
                batch.changed().await;
            }
        }
    }

    #[derive(Default)]
    struct CounterWorker {
        counter: StdMutex<i64>,
    }

    #[async_trait]
    impl Worker<TestRequest, ()> for CounterWorker {
        async fn run(&self, _key: &str, batch: &BatchHandle<TestRequest, ()>) -> Result<(), ()> {
            let _ = batch.merged();
            *self.counter.lock().unwrap() += 1;
            Ok(())
        }
    }

    // Uncontended work runs inline: a lone caller per key never spawns an owner.
    #[tokio::test]
    async fn uncontended_runs_inline_without_spawn() {
        let d = Dedup::new(CounterWorker::default());
        for i in 0..5 {
            assert!(d.run("key", mergeable(i)).await.is_ok());
        }
        assert_eq!(*d.inner.worker.counter.lock().unwrap(), 5);
        assert_eq!(d.active_owners(), 0, "uncontended work should not spawn");
    }

    /// Closing the deduplicator cancels all in-flight work; any subsequent
    /// inline call observes `Cancelled` because the shutdown token propagates
    /// to the worker round.
    #[tokio::test]
    async fn close_surfaces_cancelled() {
        let d = Dedup::new(CounterWorker::default());
        d.close().await;
        let err = d.run("key", mergeable(0)).await;
        assert!(matches!(err, Err(DedupError::Cancelled)), "got {err:?}");
    }

    #[tokio::test]
    async fn concurrent_close_wakes_every_waiter() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));

        let mut driver = Box::pin(d.run("key", unmergeable(1)));
        assert!(futures::poll!(driver.as_mut()).is_pending());
        let mut queued = Box::pin(d.run("key", unmergeable(2)));
        assert!(futures::poll!(queued.as_mut()).is_pending());

        // Cancelling the inline driver hands the queued call to a spawned owner.
        drop(driver);
        assert_eq!(d.active_owners(), 1);

        // Register both close callers before the owner observes shutdown.
        let mut first = Box::pin(d.close());
        let mut second = Box::pin(d.close());
        assert!(futures::poll!(first.as_mut()).is_pending());
        assert!(futures::poll!(second.as_mut()).is_pending());

        tokio::time::timeout(Duration::from_secs(2), async {
            tokio::join!(first, second);
        })
        .await
        .expect("one concurrent close caller remained asleep");
        assert!(matches!(queued.await, Err(DedupError::Cancelled)));
    }

    #[tokio::test]
    async fn merge_do() {
        let d = Arc::new(Dedup::new(AccumWorker {
            target: 2,
            res: StdMutex::new(Vec::new()),
        }));

        // A becomes the inline driver and waits for the merged total to reach 2.
        let mut a = Box::pin(d.run("key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        // B merges in.
        let mut b = Box::pin(d.run("key", mergeable(1)));
        assert!(futures::poll!(b.as_mut()).is_pending());

        assert!(a.await.is_ok());
        assert!(b.await.is_ok());
        assert_eq!(*d.inner.worker.res.lock().unwrap(), vec![2]);
        assert_eq!(d.active_owners(), 0);
        assert_eq!(
            d.stats_and_reset(),
            DedupStats {
                submissions: 2,
                rounds: 1,
            }
        );
        assert_eq!(d.stats_and_reset(), DedupStats::default());
    }

    #[tokio::test]
    async fn sequential_do() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));
        let release = d.inner.worker.release.clone();

        // A is the inline driver (gated); B queues behind it (non-mergeable).
        let mut a = Box::pin(d.run("key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut b = Box::pin(d.run("key", unmergeable(1)));
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

        let mut a = Box::pin(d.run("key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut b = Box::pin(d.run("key", unmergeable(2)));
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

        // Seed (gated), a non-mergeable queued request, and a reorderable one.
        let mut a = Box::pin(d.run("key", mergeable(5)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut wa = Box::pin(d.run("key", unmergeable(2)));
        assert!(futures::poll!(wa.as_mut()).is_pending());
        let mut wb = Box::pin(d.run("key", reorderable(3)));
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

        let mut a = Box::pin(d.run("key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut waiters: Vec<_> = (0..4)
            .map(|_| Box::pin(d.run("key", mergeable(1))))
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

        let mut a = Box::pin(d.run("key", unmergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut b = Box::pin(d.run("key", unmergeable(2)));
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

        let mut a = Box::pin(d.run("key", unmergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        {
            let mut b = Box::pin(d.run("key", unmergeable(2)));
            assert!(futures::poll!(b.as_mut()).is_pending());
        }

        release.add_permits(1);
        let ra = tokio::time::timeout(Duration::from_secs(2), a).await;
        assert!(matches!(ra, Ok(Ok(()))), "driver did not finish: {ra:?}");

        // The key is free for a fresh request.
        let rc = tokio::time::timeout(Duration::from_secs(2), d.run("key", unmergeable(3))).await;
        assert!(matches!(rc, Ok(Ok(()))), "key was orphaned: {rc:?}");
    }

    /// When every caller drops its `run` future mid-flight, the worker's
    /// per-round context is cancelled (no live members remain) so it
    /// abandons the work and the key is removed without an orphan.
    #[tokio::test]
    async fn all_members_dropped_abandons_batch() {
        let d = Arc::new(Dedup::new(AccumWorker {
            // Never reachable: forces the worker to wait on `changed` until
            // every caller drops and the round's token is cancelled.
            target: i64::MAX,
            res: StdMutex::new(Vec::new()),
        }));

        let mut a = Box::pin(d.run("key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut b = Box::pin(d.run("key", mergeable(1)));
        assert!(futures::poll!(b.as_mut()).is_pending());

        // Drop everyone: the inline driver bails on its own future-drop;
        // the waiter pokes `changed` so the spawned owner re-evaluates.
        drop(a);
        drop(b);

        // Wait for the spawned owner to drain its empty batch and remove the
        // key entirely, signalling no orphan.
        let rc = tokio::time::timeout(Duration::from_secs(2), async {
            while !d.snapshot().is_empty() {
                tokio::task::yield_now().await;
            }
        })
        .await;
        assert!(rc.is_ok(), "key was not abandoned: {rc:?}");

        // No work was recorded and nothing leaked.
        assert!(d.inner.worker.res.lock().unwrap().is_empty());
        d.close().await;
        assert_eq!(d.active_owners(), 0);
    }

    // Diagnostics smoke test: while a batch is in flight, snapshot reports the
    // key with an active op and the queued/pending counts; after delivery, the
    // key is gone (no orphan).
    #[tokio::test]
    async fn snapshot_reflects_inflight_state_and_clears_after_delivery() {
        let d = Arc::new(Dedup::new(GatedWorker::new()));
        let release = d.inner.worker.release.clone();

        // No state when idle.
        assert!(d.snapshot().is_empty());

        // A is the inline driver (gated). B queues behind, C is reorderable.
        let mut a = Box::pin(d.run("key", mergeable(1)));
        assert!(futures::poll!(a.as_mut()).is_pending());
        let mut b = Box::pin(d.run("key", unmergeable(2)));
        assert!(futures::poll!(b.as_mut()).is_pending());
        let mut c = Box::pin(d.run("key", reorderable(3)));
        assert!(futures::poll!(c.as_mut()).is_pending());

        let snap = d.snapshot();
        assert_eq!(snap.len(), 1, "expected one keyed entry: {snap:?}");
        let s = &snap[0];
        assert_eq!(s.key, "key");
        assert!(s.has_active_op, "worker round should be in flight");
        assert_eq!(s.batch_count, 1, "only A is in the batch");
        assert_eq!(s.queue_count, 1, "B queued");
        assert_eq!(s.pending_count, 1, "C reorderable pending");

        // Drain to completion.
        release.add_permits(1);
        assert!(a.await.is_ok());
        assert!(b.await.is_ok());
        assert!(c.await.is_ok());
        d.close().await;

        // Snapshot is empty once all work is delivered (key removed).
        assert!(
            d.snapshot().is_empty(),
            "post-delivery snapshot: {:?}",
            d.snapshot()
        );
    }
}
