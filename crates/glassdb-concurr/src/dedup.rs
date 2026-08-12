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
    /// The merged request for the current batch. Starting a round moves it into
    /// that driver's handle as stale/close/liveness fallback; the next refresh
    /// recomputes it from the still-owned batch.
    merged: Option<R>,
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
            merged: Some(merged),
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
    fn prune(&mut self, discarded: &mut Vec<Member<R, E>>) {
        discarded.extend(self.batch.extract_if(.., |member| !member.live()));
        Self::prune_queue(&mut self.reorderable, discarded);
        Self::prune_queue(&mut self.fifo, discarded);
    }

    fn prune_queue(queue: &mut VecDeque<Member<R, E>>, discarded: &mut Vec<Member<R, E>>) {
        let count = queue.len();
        for _ in 0..count {
            let member = queue
                .pop_front()
                .expect("dedup: queue changed while pruning");
            if member.live() {
                queue.push_back(member);
            } else {
                discarded.push(member);
            }
        }
    }

    /// Reports whether a future batch has queued work.
    fn has_incoming_live(&mut self, discarded: &mut Vec<Member<R, E>>) -> bool {
        self.prune(discarded);
        !self.reorderable.is_empty() || !self.fifo.is_empty()
    }

    /// Refreshes the active batch with compatible queued work. An empty active
    /// batch stays empty so its in-flight operation can be cancelled before a
    /// queued seed starts a fresh round.
    fn refresh_batch(&mut self, discarded: &mut Vec<Member<R, E>>) -> bool {
        self.prune(discarded);
        self.merge_compatible()
    }

    /// Forms the batch for a worker round, preferring the FIFO front as its seed.
    fn form_batch(&mut self, discarded: &mut Vec<Member<R, E>>) -> bool {
        self.prune(discarded);
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
        self.refresh_batch(discarded)
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

        self.merged = Some(merged);
        true
    }

    /// Moves out the active batch so delivery can drain and return its
    /// allocation after releasing the shard lock.
    fn take_batch(&mut self) -> Vec<Member<R, E>> {
        std::mem::take(&mut self.batch)
    }

    /// Abandons an active batch after every member has stopped waiting.
    fn abandon_batch(&mut self, discarded: &mut Vec<Member<R, E>>) {
        discarded.append(&mut self.batch);
    }

    /// Returns live undelivered members to the queues for a successor driver.
    fn requeue_batch(&mut self, discarded: &mut Vec<Member<R, E>>) {
        let mut strict = VecDeque::new();
        for member in self.batch.drain(..) {
            if !member.live() {
                discarded.push(member);
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

    fn merged(&self) -> Option<&R> {
        self.merged.as_ref()
    }

    fn take_merged(&mut self) -> R {
        self.merged
            .take()
            .expect("dedup: formed batch has no merged request")
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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct DriverId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DriverKind {
    Inline,
    Owner,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Driver {
    id: DriverId,
    kind: DriverKind,
}

enum RoundPhase {
    Ready,
    Running(CancellationToken),
}

enum KeyPhase {
    Driven {
        driver: Driver,
        round: RoundPhase,
    },
    /// Reserves the key for `driver` while its result is delivered outside the
    /// shard lock. New submissions queue behind this phase.
    Completing {
        driver: Driver,
    },
    Handoff {
        reserved_owner: DriverId,
    },
}

#[derive(Debug, PartialEq, Eq)]
enum MachineAction {
    Keep,
    Remove,
    SpawnOwner(DriverId),
}

type BatchDelivery<R, E> = (Vec<Member<R, E>>, Result<(), Arc<E>>);

struct MachineEffects<R, E> {
    cancellation: Option<CancellationToken>,
    wake: Option<Arc<Notify>>,
    delivery: Option<BatchDelivery<R, E>>,
    discarded: Vec<Member<R, E>>,
    retired: Option<KeyMachine<R, E>>,
    discarded_outcome: Option<MachineRoundOutcome<E>>,
    retired_batch: Option<Vec<Member<R, E>>>,
}

impl<R, E> MachineEffects<R, E> {
    fn new() -> Self {
        Self {
            cancellation: None,
            wake: None,
            delivery: None,
            discarded: Vec::new(),
            retired: None,
            discarded_outcome: None,
            retired_batch: None,
        }
    }

    /// Applies observable effects in one fixed order after the shard lock has
    /// been released.
    fn apply(self) {
        drop(self.apply_recycling());
    }

    /// Applies effects and returns a drained batch allocation for the same
    /// completing driver to reuse in its successor round.
    fn apply_recycling(self) -> Option<Vec<Member<R, E>>> {
        let Self {
            cancellation,
            wake,
            delivery,
            discarded,
            retired,
            discarded_outcome,
            retired_batch,
        } = self;
        if let Some(cancellation) = cancellation {
            cancellation.cancel();
        }
        if let Some(wake) = wake {
            wake.notify_one();
        }
        let (recycled_batch, delivered_outcome) = match delivery {
            Some((mut batch, result)) => {
                for member in batch.drain(..) {
                    let _ = member.done.send(result.clone());
                }
                (Some(batch), Some(MachineRoundOutcome::Done(result)))
            }
            None => (None, None),
        };
        drop(discarded);
        drop(retired);
        drop(discarded_outcome);
        drop(delivered_outcome);
        drop(retired_batch);
        recycled_batch
    }

    fn delivery_len(&self) -> usize {
        self.delivery
            .as_ref()
            .map_or(0, |(batch, _result)| batch.len())
    }
}

struct MachineStep<R, E, T> {
    value: T,
    action: MachineAction,
    effects: MachineEffects<R, E>,
}

impl<R, E, T> MachineStep<R, E, T> {
    fn keep(value: T) -> Self {
        Self {
            value,
            action: MachineAction::Keep,
            effects: MachineEffects::new(),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DriverFlow {
    Continue,
    Exit,
}

enum MachineRoundOutcome<E> {
    Done(Result<(), Arc<E>>),
    Liveness,
    Shutdown,
}

/// The explicit lifecycle machine for one keyed queue. `Idle` and `Closed` are
/// not stored phases: transitions to either remove the entry from the shard map
/// while that map is still locked.
struct KeyMachine<R, E> {
    queue: KeyQueue<R, E>,
    phase: KeyPhase,
    changed: Arc<Notify>,
}

impl<R, E> KeyMachine<R, E>
where
    R: MergeRequest,
{
    fn new(seed: Member<R, E>, inline: DriverId) -> Self {
        Self {
            queue: KeyQueue::new(seed),
            phase: KeyPhase::Driven {
                driver: Driver {
                    id: inline,
                    kind: DriverKind::Inline,
                },
                round: RoundPhase::Ready,
            },
            changed: Arc::new(Notify::new()),
        }
    }

    fn submit(
        &mut self,
        member: Member<R, E>,
        can_reorder: bool,
    ) -> MachineStep<R, E, Arc<Notify>> {
        self.queue.enqueue(member, can_reorder);
        let mut step = MachineStep::keep(self.changed.clone());
        step.effects.wake = Some(self.changed.clone());
        step
    }

    fn start_round(
        &mut self,
        driver_id: DriverId,
        closing: bool,
    ) -> MachineStep<R, E, Option<(CancellationToken, R)>> {
        let matches_ready = matches!(
            &self.phase,
            KeyPhase::Driven {
                driver,
                round: RoundPhase::Ready,
            } if driver.id == driver_id
        );
        if !matches_ready {
            return MachineStep::keep(None);
        }

        let mut step = MachineStep::keep(None);
        if closing || !self.queue.form_batch(&mut step.effects.discarded) {
            step.action = MachineAction::Remove;
            return step;
        }

        let signal = CancellationToken::new();
        let fallback = self.queue.take_merged();
        if let KeyPhase::Driven { round, .. } = &mut self.phase {
            *round = RoundPhase::Running(signal.clone());
        }
        step.value = Some((signal, fallback));
        step
    }

    fn refresh(&mut self, driver_id: DriverId) -> MachineStep<R, E, Option<R>> {
        let signal = match &self.phase {
            KeyPhase::Driven {
                driver,
                round: RoundPhase::Running(signal),
            } if driver.id == driver_id => signal.clone(),
            _ => return MachineStep::keep(None),
        };

        let mut step = MachineStep::keep(None);
        if !self.queue.refresh_batch(&mut step.effects.discarded) {
            step.effects.cancellation = Some(signal);
        }
        step.value = self.queue.merged().cloned();
        step
    }

    fn round_finished(
        &mut self,
        driver_id: DriverId,
        outcome: MachineRoundOutcome<E>,
    ) -> MachineStep<R, E, bool> {
        let driver = match &self.phase {
            KeyPhase::Driven {
                driver,
                round: RoundPhase::Running(_),
            } if driver.id == driver_id => *driver,
            _ => {
                let mut step = MachineStep::keep(false);
                step.effects.discarded_outcome = Some(outcome);
                return step;
            }
        };

        let mut step = MachineStep::keep(false);
        match outcome {
            MachineRoundOutcome::Done(result) => {
                step.effects.delivery = Some((self.queue.take_batch(), result));
            }
            MachineRoundOutcome::Liveness => {
                self.queue.abandon_batch(&mut step.effects.discarded);
            }
            MachineRoundOutcome::Shutdown => {
                step.action = MachineAction::Remove;
                return step;
            }
        }

        self.phase = KeyPhase::Completing { driver };
        step.value = true;
        step
    }

    fn finalize_completion(
        &mut self,
        driver_id: DriverId,
        recycled_batch: Option<Vec<Member<R, E>>>,
        successor: impl FnOnce() -> DriverId,
    ) -> MachineStep<R, E, DriverFlow> {
        let driver = match self.phase {
            KeyPhase::Completing { driver } if driver.id == driver_id => driver,
            _ => {
                let mut step = MachineStep::keep(DriverFlow::Exit);
                step.effects.retired_batch = recycled_batch;
                return step;
            }
        };

        let mut step = MachineStep::keep(DriverFlow::Exit);
        if let Some(batch) = recycled_batch {
            debug_assert!(batch.is_empty());
            debug_assert!(self.queue.batch.is_empty());
            self.queue.batch = batch;
        }
        let has_more = self.queue.has_incoming_live(&mut step.effects.discarded);
        match (driver.kind, has_more) {
            (DriverKind::Inline, true) => {
                let successor = successor();
                self.phase = KeyPhase::Handoff {
                    reserved_owner: successor,
                };
                step.action = MachineAction::SpawnOwner(successor);
            }
            (DriverKind::Owner, true) => {
                self.phase = KeyPhase::Driven {
                    driver,
                    round: RoundPhase::Ready,
                };
                step.value = DriverFlow::Continue;
            }
            (_, false) => step.action = MachineAction::Remove,
        }
        step
    }

    fn driver_dropped(
        &mut self,
        driver_id: DriverId,
        closing: bool,
        successor: impl FnOnce() -> DriverId,
    ) -> MachineStep<R, E, ()> {
        let round_signal = match &self.phase {
            KeyPhase::Driven { driver, round } if driver.id == driver_id => match round {
                RoundPhase::Ready => None,
                RoundPhase::Running(signal) => Some(signal.clone()),
            },
            KeyPhase::Completing { driver } if driver.id == driver_id => None,
            _ => return MachineStep::keep(()),
        };

        let mut step = MachineStep::keep(());
        if let Some(signal) = round_signal {
            step.effects.cancellation = Some(signal);
        }
        self.queue.requeue_batch(&mut step.effects.discarded);
        if closing {
            step.action = MachineAction::Remove;
        } else if self.queue.has_incoming_live(&mut step.effects.discarded) {
            let successor = successor();
            self.phase = KeyPhase::Handoff {
                reserved_owner: successor,
            };
            step.action = MachineAction::SpawnOwner(successor);
        } else {
            step.action = MachineAction::Remove;
        }
        step
    }

    fn owner_started(&mut self, owner: DriverId) -> MachineStep<R, E, bool> {
        if !matches!(
            self.phase,
            KeyPhase::Handoff { reserved_owner } if reserved_owner == owner
        ) {
            return MachineStep::keep(false);
        }
        self.phase = KeyPhase::Driven {
            driver: Driver {
                id: owner,
                kind: DriverKind::Owner,
            },
            round: RoundPhase::Ready,
        };
        MachineStep::keep(true)
    }

    fn waiter_dropped(&self) -> MachineStep<R, E, ()> {
        let mut step = MachineStep::keep(());
        step.effects.wake = Some(self.changed.clone());
        step
    }

    fn close(&mut self) -> MachineStep<R, E, ()> {
        let mut step = MachineStep::keep(());
        if let KeyPhase::Driven {
            round: RoundPhase::Running(signal),
            ..
        } = &self.phase
        {
            step.effects.cancellation = Some(signal.clone());
        }
        step.action = MachineAction::Remove;
        step
    }

    fn changed_for(&self, driver_id: DriverId) -> Option<Arc<Notify>> {
        matches!(
            &self.phase,
            KeyPhase::Driven {
                driver,
                round: RoundPhase::Running(_),
            } if driver.id == driver_id
        )
        .then(|| self.changed.clone())
    }

    fn has_active_op(&self) -> bool {
        matches!(
            self.phase,
            KeyPhase::Driven {
                round: RoundPhase::Running(_),
                ..
            }
        )
    }
}

/// One key-space partition guarded by a single lock.
struct Shard<R, E> {
    map: Mutex<HashMap<String, KeyMachine<R, E>>>,
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
    driver: DriverId,
    fallback: Option<R>,
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
        let (merged, effects) = {
            let mut map = self.shard.map.lock().unwrap();
            match map.get_mut(&self.key) {
                Some(machine) => {
                    let step = machine.refresh(self.driver);
                    (step.value, step.effects)
                }
                None => (None, MachineEffects::new()),
            }
        };
        effects.apply();
        merged.unwrap_or_else(|| {
            self.fallback
                .as_ref()
                .expect("dedup: active batch has no round fallback")
                .clone()
        })
    }

    /// Resolves when new work arrives for the key (or a waiter cancels). Intended
    /// for use inside a `select!` in worker implementations.
    pub async fn changed(&self) {
        let notify = {
            let map = self.shard.map.lock().unwrap();
            match map.get(&self.key) {
                Some(machine) => match machine.changed_for(self.driver) {
                    Some(changed) => changed,
                    None => return,
                },
                None => return,
            }
        };
        notify.notified().await;
    }

    /// Retains the already-computed request when a stale, closed, or emptied
    /// batch cannot produce a current merge. Replacing the prior round's value
    /// happens after the shard unlocks.
    fn install_fallback(&mut self, fallback: R) {
        drop(self.fallback.replace(fallback));
    }
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
    next_driver: AtomicU64,
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

struct OwnerPermit<R, E, W> {
    inner: Arc<Inner<R, E, W>>,
}

impl<R, E, W> OwnerPermit<R, E, W> {
    fn reserve(inner: Arc<Inner<R, E, W>>) -> Self {
        inner
            .active_owners
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |owners| {
                owners.checked_add(1)
            })
            .expect("dedup: active owner count overflowed");
        Self { inner }
    }
}

impl<R, E, W> Drop for OwnerPermit<R, E, W> {
    fn drop(&mut self) {
        let owners = self
            .inner
            .active_owners
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |owners| {
                owners.checked_sub(1)
            })
            .expect("dedup: released an unreserved owner");
        if owners == 1 {
            self.inner.owners_idle.notify_waiters();
        }
    }
}

struct OwnerSpawn<R, E, W> {
    inner: Arc<Inner<R, E, W>>,
    shard: Arc<Shard<R, E>>,
    key: String,
    driver: DriverId,
    permit: OwnerPermit<R, E, W>,
}

impl<R, E, W> OwnerSpawn<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    fn apply(self) {
        let Self {
            inner,
            shard,
            key,
            driver,
            permit,
        } = self;
        tracing::trace!(target: "glassdb::dedup", key = %key, ?driver, "spawn_owner");
        let owner = rt::spawn(async move {
            let _permit = permit;
            inner.run_owner(&shard, key, driver).await;
        });
        drop(owner);
    }
}

struct DeferredEffects<R, E, W> {
    machine: MachineEffects<R, E>,
    spawn: Option<OwnerSpawn<R, E, W>>,
}

impl<R, E, W> DeferredEffects<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    fn apply(self) {
        drop(self.apply_recycling());
    }

    fn apply_recycling(self) -> Option<Vec<Member<R, E>>> {
        let recycled_batch = self.machine.apply_recycling();
        if let Some(spawn) = self.spawn {
            spawn.apply();
        }
        recycled_batch
    }
}

impl<R, E, W> Inner<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    fn next_driver(&self) -> DriverId {
        let id = self
            .next_driver
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |next| {
                next.checked_add(1)
            })
            .expect("dedup: exhausted driver identifiers");
        DriverId(id)
    }

    /// Commits removal or handoff while the shard is locked. A handoff reserves
    /// its owner count here; only the actual task spawn is deferred.
    fn commit_step<T>(
        self: &Arc<Self>,
        map: &mut HashMap<String, KeyMachine<R, E>>,
        shard: &Arc<Shard<R, E>>,
        key: &str,
        mut step: MachineStep<R, E, T>,
    ) -> (T, DeferredEffects<R, E, W>) {
        let spawn = match step.action {
            MachineAction::Keep => None,
            MachineAction::Remove => {
                debug_assert!(step.effects.retired.is_none());
                step.effects.retired = map.remove(key);
                None
            }
            MachineAction::SpawnOwner(driver) => Some(OwnerSpawn {
                inner: self.clone(),
                shard: shard.clone(),
                key: key.to_owned(),
                driver,
                permit: OwnerPermit::reserve(self.clone()),
            }),
        };
        (
            step.value,
            DeferredEffects {
                machine: step.effects,
                spawn,
            },
        )
    }

    /// Runs one worker round for the identified driver, applying every cancel,
    /// wake, delivery, drop, and spawn only after releasing the shard lock.
    async fn drive_one_round(
        self: &Arc<Self>,
        shard: &Arc<Shard<R, E>>,
        key: &str,
        driver: DriverId,
        handle: &mut BatchHandle<R, E>,
    ) -> DriverFlow {
        let (started, effects) = {
            let mut map = shard.map.lock().unwrap();
            let machine = match map.get_mut(key) {
                Some(machine) => machine,
                None => {
                    tracing::trace!(target: "glassdb::dedup", key, "round_exit_no_state");
                    return DriverFlow::Exit;
                }
            };
            let step = machine.start_round(driver, self.shutdown.is_cancelled());
            if step.value.is_some() {
                shard.rounds.fetch_add(1, Ordering::Relaxed);
                tracing::trace!(
                    target: "glassdb::dedup",
                    key,
                    ?driver,
                    batch_count = machine.queue.batch_len(),
                    pending_count = machine.queue.reorderable_len(),
                    queue_count = machine.queue.fifo_len(),
                    "round_start",
                );
            } else if matches!(step.action, MachineAction::Remove) {
                tracing::trace!(target: "glassdb::dedup", key, "key_removed");
            }
            self.commit_step(&mut map, shard, key, step)
        };
        effects.apply();
        let Some((op_signal, fallback)) = started else {
            return DriverFlow::Exit;
        };
        handle.install_fallback(fallback);

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

        let (completing, effects, delivered) = {
            let mut map = shard.map.lock().unwrap();
            let Some(machine) = map.get_mut(key) else {
                return DriverFlow::Exit;
            };
            let machine_outcome = match outcome {
                WorkerOutcome::Done(result) => MachineRoundOutcome::Done(result),
                WorkerOutcome::Liveness => MachineRoundOutcome::Liveness,
                WorkerOutcome::Shutdown => MachineRoundOutcome::Shutdown,
            };
            let step = machine.round_finished(driver, machine_outcome);
            let delivered = step.effects.delivery_len();
            let (completing, effects) = self.commit_step(&mut map, shard, key, step);
            (completing, effects, delivered)
        };
        if delivered != 0 {
            tracing::trace!(target: "glassdb::dedup", key, ?driver, delivered, "round_delivered");
        }
        let recycled_batch = effects.apply_recycling();
        if !completing {
            return DriverFlow::Exit;
        }

        let (flow, effects) = {
            let mut map = shard.map.lock().unwrap();
            let Some(machine) = map.get_mut(key) else {
                return DriverFlow::Exit;
            };
            let step = machine.finalize_completion(driver, recycled_batch, || self.next_driver());
            self.commit_step(&mut map, shard, key, step)
        };
        effects.apply();
        flow
    }

    /// Owner task loop: serves rounds until the queue drains, then exits.
    async fn run_owner(self: &Arc<Self>, shard: &Arc<Shard<R, E>>, key: String, driver: DriverId) {
        let (started, effects) = {
            let mut map = shard.map.lock().unwrap();
            let Some(machine) = map.get_mut(&key) else {
                return;
            };
            let step = machine.owner_started(driver);
            self.commit_step(&mut map, shard, &key, step)
        };
        effects.apply();
        if !started {
            return;
        }
        let mut guard = DriverGuard::new(self.clone(), shard.clone(), key.clone(), driver);
        let mut handle = BatchHandle {
            shard: shard.clone(),
            key: key.clone(),
            driver,
            fallback: None,
        };
        while let DriverFlow::Continue =
            self.drive_one_round(shard, &key, driver, &mut handle).await
        {}
        guard.disarm();
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
        driver: DriverId,
        mut rx: oneshot::Receiver<Result<(), Arc<E>>>,
    ) -> Result<(), DedupError<E>> {
        let mut guard = DriverGuard::new(self.clone(), shard.clone(), key.to_string(), driver);
        let mut handle = BatchHandle {
            shard: shard.clone(),
            key: key.to_owned(),
            driver,
            fallback: None,
        };
        let _ = self.drive_one_round(shard, key, driver, &mut handle).await;
        guard.disarm();
        match rx.try_recv() {
            Ok(res) => res.map_err(DedupError::Work),
            // Our own member was pruned (e.g. by shutdown) before delivery.
            Err(_) => Err(DedupError::Cancelled),
        }
    }

    fn driver_dropped(self: &Arc<Self>, shard: &Arc<Shard<R, E>>, key: &str, driver: DriverId) {
        let effects = {
            let mut map = shard.map.lock().unwrap();
            let Some(machine) = map.get_mut(key) else {
                return;
            };
            let step =
                machine.driver_dropped(driver, self.shutdown.is_cancelled(), || self.next_driver());
            self.commit_step(&mut map, shard, key, step).1
        };
        effects.apply();
    }

    fn waiter_dropped(
        self: &Arc<Self>,
        shard: &Arc<Shard<R, E>>,
        key: &str,
        changed: &Arc<Notify>,
    ) {
        let effects = {
            let mut map = shard.map.lock().unwrap();
            let Some(machine) = map.get_mut(key) else {
                return;
            };
            if !Arc::ptr_eq(&machine.changed, changed) {
                return;
            }
            let step = machine.waiter_dropped();
            self.commit_step(&mut map, shard, key, step).1
        };
        effects.apply();
    }

    fn close_keys(self: &Arc<Self>) {
        self.shards.each(|shard| {
            let effects = {
                let mut map = shard.map.lock().unwrap();
                let keys = map.keys().cloned().collect::<Vec<_>>();
                let mut effects = Vec::with_capacity(keys.len());
                for key in keys {
                    let step = map
                        .get_mut(&key)
                        .expect("dedup: key disappeared during close")
                        .close();
                    effects.push(self.commit_step(&mut map, shard, &key, step).1);
                }
                effects
            };
            for effect in effects {
                effect.apply();
            }
        });
    }
}

/// RAII handoff for a dropped driver. On drop while still armed (the driver's
/// future was dropped or cancelled mid-round), it requeues undelivered live
/// members, and either spawns a fresh owner to finish the work or removes the
/// key. Because the successor is a spawned task, not a caller future, the
/// handoff cannot be lost. The driver id makes the drop a no-op if ownership has
/// already moved on.
///
/// The worker future is part of the driver's own future tree, so it is already
/// being dropped by the time `drop` runs. The transition still defers token
/// cancellation so observers see every effect only after the shard unlocks.
struct DriverGuard<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    inner: Arc<Inner<R, E, W>>,
    shard: Arc<Shard<R, E>>,
    key: String,
    driver: DriverId,
    armed: bool,
}

impl<R, E, W> DriverGuard<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    fn new(
        inner: Arc<Inner<R, E, W>>,
        shard: Arc<Shard<R, E>>,
        key: String,
        driver: DriverId,
    ) -> Self {
        Self {
            inner,
            shard,
            key,
            driver,
            armed: true,
        }
    }

    fn disarm(&mut self) {
        self.armed = false;
    }
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
        self.inner
            .driver_dropped(&self.shard, &self.key, self.driver);
    }
}

/// Disarms after the waiter receives its result. On drop while armed (the
/// `run` future was cancelled mid-wait), it pokes the per-key `changed`
/// notifier so the driver re-evaluates liveness and can abandon the batch if
/// no caller remains. Without it, the driver might sit indefinitely in
/// `BatchHandle::changed`.
struct WaiterDropGuard<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    inner: Arc<Inner<R, E, W>>,
    shard: Arc<Shard<R, E>>,
    key: String,
    changed: Arc<Notify>,
    armed: bool,
}

impl<R, E, W> Drop for WaiterDropGuard<R, E, W>
where
    R: MergeRequest,
    E: Send + Sync + 'static,
    W: Worker<R, E> + 'static,
{
    fn drop(&mut self) {
        if self.armed {
            self.inner
                .waiter_dropped(&self.shard, &self.key, &self.changed);
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
                next_driver: AtomicU64::new(1),
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

        let (driver, changed, effects) = {
            let mut map = shard.map.lock().unwrap();
            match map.get_mut(key) {
                Some(machine) => {
                    let step = machine.submit(member, can_reorder);
                    let (changed, effects) = self.inner.commit_step(&mut map, &shard, key, step);
                    (None, changed, effects)
                }
                None => {
                    let driver = self.inner.next_driver();
                    let machine = KeyMachine::new(member, driver);
                    let changed = machine.changed.clone();
                    map.insert(key.to_string(), machine);
                    (
                        Some(driver),
                        changed,
                        DeferredEffects {
                            machine: MachineEffects::new(),
                            spawn: None,
                        },
                    )
                }
            }
        };
        effects.apply();

        if let Some(driver) = driver {
            return self.inner.drive_inline(&shard, key, driver, rx).await;
        }

        // If a queued waiter is dropped mid-wait, its `oneshot::Receiver` goes
        // with it; the guard wakes the driver so it can prune the dead member
        // promptly (and abandon the batch if no live caller remains).
        let mut guard = WaiterDropGuard {
            inner: self.inner.clone(),
            shard,
            key: key.to_owned(),
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
        self.inner.close_keys();
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
            for (key, machine) in map.iter() {
                out.push(DedupKeySnapshot {
                    key: key.clone(),
                    batch_count: machine.queue.batch_len(),
                    pending_count: machine.queue.reorderable_len(),
                    queue_count: machine.queue.fifo_len(),
                    has_active_op: machine.has_active_op(),
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

    type TestResult = oneshot::Receiver<Result<(), Arc<()>>>;

    fn test_member(request: TestRequest) -> (Member<TestRequest, ()>, TestResult) {
        let (done, result) = oneshot::channel();
        (Member { request, done }, result)
    }

    #[test]
    fn key_machine_transition_action_table() {
        fn phase(machine: &KeyMachine<TestRequest, ()>) -> &'static str {
            match &machine.phase {
                KeyPhase::Driven {
                    driver:
                        Driver {
                            kind: DriverKind::Inline,
                            ..
                        },
                    round: RoundPhase::Ready,
                } => "inline-ready",
                KeyPhase::Driven {
                    driver:
                        Driver {
                            kind: DriverKind::Inline,
                            ..
                        },
                    round: RoundPhase::Running(_),
                } => "inline-running",
                KeyPhase::Driven {
                    driver:
                        Driver {
                            kind: DriverKind::Owner,
                            ..
                        },
                    round: RoundPhase::Ready,
                } => "owner-ready",
                KeyPhase::Driven {
                    driver:
                        Driver {
                            kind: DriverKind::Owner,
                            ..
                        },
                    round: RoundPhase::Running(_),
                } => "owner-running",
                KeyPhase::Completing {
                    driver:
                        Driver {
                            kind: DriverKind::Inline,
                            ..
                        },
                } => "inline-completing",
                KeyPhase::Completing {
                    driver:
                        Driver {
                            kind: DriverKind::Owner,
                            ..
                        },
                } => "owner-completing",
                KeyPhase::Handoff { .. } => "handoff",
            }
        }
        fn action(action: &MachineAction) -> &'static str {
            match action {
                MachineAction::Keep => "keep",
                MachineAction::Remove => "remove",
                MachineAction::SpawnOwner(_) => "spawn-owner",
            }
        }

        let inline = DriverId(1);
        let owner = DriverId(2);
        let stale = DriverId(3);
        let (seed, _seed_result) = test_member(mergeable(1));
        let mut machine = KeyMachine::new(seed, inline);
        let mut actual = Vec::new();

        let (first_waiter, _first_result) = test_member(reorderable(2));
        let step = machine.submit(first_waiter, true);
        actual.push(("Submit", phase(&machine), action(&step.action)));
        step.effects.apply();

        let step = machine.start_round(inline, false);
        actual.push(("StartRound", phase(&machine), action(&step.action)));
        assert!(step.value.is_some());
        step.effects.apply();

        let step = machine.refresh(inline);
        actual.push(("Refresh", phase(&machine), action(&step.action)));
        assert!(step.value.is_some());
        step.effects.apply();

        let step = machine.waiter_dropped();
        actual.push(("WaiterDropped", phase(&machine), action(&step.action)));
        step.effects.apply();

        let step = machine.round_finished(inline, MachineRoundOutcome::Done(Ok(())));
        actual.push(("RoundFinished", phase(&machine), action(&step.action)));
        assert!(step.value);

        // No successor existed when the round finished. A submission arriving
        // before deferred delivery remains queued behind the completing round.
        let (barrier, _barrier_result) = test_member(unmergeable(4));
        let submitted = machine.submit(barrier, false);
        actual.push(("Submit", phase(&machine), action(&submitted.action)));
        submitted.effects.apply();
        let recycled_batch = step.effects.apply_recycling();

        let step = machine.finalize_completion(inline, recycled_batch, || owner);
        actual.push(("FinalizeCompletion", phase(&machine), action(&step.action)));
        step.effects.apply();

        let step = machine.driver_dropped(inline, false, || stale);
        actual.push(("DriverDropped", phase(&machine), action(&step.action)));
        step.effects.apply();

        let step = machine.owner_started(owner);
        actual.push(("OwnerStarted", phase(&machine), action(&step.action)));
        assert!(step.value);
        step.effects.apply();

        let step = machine.close();
        actual.push(("Close", phase(&machine), action(&step.action)));
        step.effects.apply();

        assert_eq!(
            actual,
            [
                ("Submit", "inline-ready", "keep"),
                ("StartRound", "inline-running", "keep"),
                ("Refresh", "inline-running", "keep"),
                ("WaiterDropped", "inline-running", "keep"),
                ("RoundFinished", "inline-completing", "keep"),
                ("Submit", "inline-completing", "keep"),
                ("FinalizeCompletion", "handoff", "spawn-owner"),
                ("DriverDropped", "handoff", "keep"),
                ("OwnerStarted", "owner-ready", "keep"),
                ("Close", "owner-ready", "remove"),
            ]
        );
    }

    #[tokio::test]
    async fn machine_effects_are_deferred() {
        let (seed, mut result) = test_member(mergeable(1));
        let mut delivery = KeyMachine::new(seed, DriverId(1));
        let started = delivery.start_round(DriverId(1), false);
        started.effects.apply();
        let finished = delivery.round_finished(DriverId(1), MachineRoundOutcome::Done(Ok(())));
        assert!(matches!(
            result.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));
        finished.effects.apply();
        assert!(matches!(result.try_recv(), Ok(Ok(()))));

        let (seed, result) = test_member(mergeable(1));
        let mut cancellation = KeyMachine::new(seed, DriverId(3));
        let started = cancellation.start_round(DriverId(3), false);
        let signal = started.value.unwrap().0;
        started.effects.apply();
        drop(result);
        let refreshed = cancellation.refresh(DriverId(3));
        assert!(!signal.is_cancelled());
        refreshed.effects.apply();
        assert!(signal.is_cancelled());

        let (seed, _result) = test_member(mergeable(1));
        let wake = KeyMachine::new(seed, DriverId(4));
        let mut notified = Box::pin(wake.changed.notified());
        assert!(futures::poll!(notified.as_mut()).is_pending());
        let dropped = wake.waiter_dropped();
        assert!(futures::poll!(notified.as_mut()).is_pending());
        dropped.effects.apply();
        assert!(futures::poll!(notified.as_mut()).is_ready());

        let (seed, _result) = test_member(mergeable(1));
        let mut stale = KeyMachine::new(seed, DriverId(5));
        let error = Arc::new(());
        let finished =
            stale.round_finished(DriverId(99), MachineRoundOutcome::Done(Err(error.clone())));
        assert_eq!(Arc::strong_count(&error), 2);
        finished.effects.apply();
        assert_eq!(Arc::strong_count(&error), 1);
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
        let mut discarded = Vec::new();
        assert!(queue.refresh_batch(&mut discarded));
        queue.enqueue(member(unmergeable(3)), false);
        queue.enqueue(member(reorderable(4)), true);

        queue.requeue_batch(&mut discarded);

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
