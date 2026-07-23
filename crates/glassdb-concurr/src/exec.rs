//! A minimal deterministic, single-threaded async executor for simulation
//! testing (`--cfg sim` only).
//!
//! The executor controls the order in which ready tasks are polled via a
//! pluggable [`Scheduler`], and drives time virtually (a sleep registers a timer
//! and "now" only advances when no task is runnable). Because scheduling and
//! time are pure functions of the scheduler's own state, a whole run replays
//! identically from the same seed/tape — the property the concurrency fuzzer
//! relies on (see ADR-010/011).
//!
//! Crucially, the executor reuses `tokio::sync` (which is runtime-agnostic) and
//! `tokio::select!` unchanged: those wake tasks through the standard `Waker`
//! API, which routes into this executor's ready set, so interleaving control is
//! obtained without re-implementing the synchronization surface. `biased`
//! selects poll top-to-bottom; non-`biased` ones stay deterministic because
//! [`block_on_with`] seeds tokio's branch-poll RNG from the run seed.

use std::cell::RefCell;
use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::future::Future;
use std::pin::Pin;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Wake, Waker};
use std::time::Duration;

use crate::rng::Rng;

/// Maximum number of scheduler steps (task polls plus virtual-time advances) a
/// single [`block_on_with`] run may take before it is declared non-terminating
/// and panics. A deterministic backstop (a *single-execution timeout* measured
/// in schedule steps rather than wall-clock, so it trips identically on every
/// replay) against livelock or an infinite retry loop (a bug class DST hunts):
/// a legitimate bounded workload uses orders of magnitude fewer steps, while a
/// run that never terminates trips this instead of hanging forever.
///
/// Set ~700x above the heaviest legitimate run observed across the whole sim
/// suite (~1.4k steps), so it cannot false-trip a bounded workload. Note this is
/// a *complement* to, not a replacement for, libFuzzer's wall-clock `-timeout`.
const DEFAULT_STEP_BUDGET: u64 = 1_000_000;

/// Unique id of a task within a single executor run. Assigned in spawn order, so
/// it is a deterministic function of the (deterministic) schedule.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct TaskId(pub u64);

/// Decides which ready task to poll next. Implementations must be deterministic
/// functions of their own state so the entire run replays from a seed/tape.
pub trait Scheduler: Send {
    /// Picks an index into `ready` (always sorted ascending by [`TaskId`]).
    /// The returned index is taken modulo `ready.len()` by the caller, so any
    /// value is safe.
    fn pick(&mut self, ready: &[TaskId]) -> usize;

    /// Notifies the scheduler that `id` was just created, in creation order.
    /// Used by priority-based policies (e.g. PCT) to assign a priority.
    fn on_spawn(&mut self, _id: TaskId) {}
}

/// Schedules by consuming a fuzzer-provided byte tape: at each decision the next
/// byte selects which ready task runs. When the tape is exhausted it falls back
/// to a fixed choice, so runs stay deterministic. This makes interleavings
/// directly mutable by the coverage-guided fuzzer (see ADR-010/011).
pub struct TapeScheduler {
    tape: Vec<u8>,
    pos: usize,
}

impl TapeScheduler {
    pub fn new(tape: Vec<u8>) -> Self {
        TapeScheduler { tape, pos: 0 }
    }
}

impl Scheduler for TapeScheduler {
    fn pick(&mut self, ready: &[TaskId]) -> usize {
        let b = self.tape.get(self.pos).copied().unwrap_or(0);
        self.pos = self.pos.wrapping_add(1);
        (b as usize) % ready.len()
    }
}

/// Schedules by picking a uniformly random ready task from a seeded PRNG. Used
/// for FoundationDB-style seed-breadth runs and as the base for PCT.
pub struct RandomScheduler {
    rng: Rng,
}

impl RandomScheduler {
    pub fn new(seed: u64) -> Self {
        RandomScheduler {
            rng: Rng::new(seed),
        }
    }
}

impl Scheduler for RandomScheduler {
    fn pick(&mut self, ready: &[TaskId]) -> usize {
        (self.rng.next_u64() % ready.len() as u64) as usize
    }
}

/// Probabilistic Concurrency Testing (Burckhardt et al., *A Randomized Scheduler
/// with Probabilistic Guarantees of Finding Bugs*).
///
/// Each task gets a distinct random priority and the scheduler always runs the
/// highest-priority runnable task, so by default a task runs uninterrupted until
/// it blocks. `depth - 1` random *change points* are drawn over an estimated
/// number of scheduling steps; when a step lands on a change point the running
/// task is demoted below all others, forcing a preemption there. This guarantees
/// a probability of at least `1 / (n * steps^(depth-1))` of hitting any bug that
/// requires `depth` ordering constraints among `n` tasks — a smarter,
/// seed-breadth complement to the byte-tape policy that needs no fuzzer feedback.
pub struct PctScheduler {
    rng: Rng,
    /// Priority per task; higher wins. Initial priorities sit in a high band so
    /// they always dominate the small priorities assigned at change points.
    priorities: BTreeMap<TaskId, u64>,
    /// Step indices (1-based) at which the running task is demoted.
    change_points: Vec<u64>,
    /// Scheduling steps taken so far.
    step: u64,
    /// Next (low) priority handed out at a change point; increasing so earlier
    /// change points demote more aggressively, matching the original algorithm.
    low_next: u64,
}

impl PctScheduler {
    /// Lowest value of the high priority band; any change-point priority is far
    /// below it, so a demoted task always yields to fresh tasks.
    const HIGH_BASE: u64 = 1 << 32;

    /// Builds a PCT scheduler for bug `depth` (number of ordering constraints to
    /// target; `depth = 1` never preempts) over an estimated `steps` scheduling
    /// decisions. Both the priorities and the change points are pure functions of
    /// `seed`, so a run replays exactly.
    pub fn new(seed: u64, depth: usize, steps: u64) -> Self {
        let mut rng = Rng::new(seed);
        let steps = steps.max(1);
        let n = depth.saturating_sub(1);
        let mut change_points = Vec::with_capacity(n);
        for _ in 0..n {
            change_points.push(1 + rng.next_u64() % steps);
        }
        PctScheduler {
            rng,
            priorities: BTreeMap::new(),
            change_points,
            step: 0,
            low_next: 0,
        }
    }
}

impl Scheduler for PctScheduler {
    fn pick(&mut self, ready: &[TaskId]) -> usize {
        self.step += 1;
        // Highest priority wins; ties break toward the lowest TaskId, which is
        // the first entry since `ready` is sorted ascending.
        let mut best_idx = 0;
        let mut best_prio = 0u64;
        for (i, tid) in ready.iter().enumerate() {
            let p = self.priorities.get(tid).copied().unwrap_or(0);
            if i == 0 || p > best_prio {
                best_prio = p;
                best_idx = i;
            }
        }
        // A change point at this step demotes the task we just chose, so it is
        // preempted on the next decision.
        if self.change_points.contains(&self.step) {
            self.low_next += 1;
            self.priorities.insert(ready[best_idx], self.low_next);
        }
        best_idx
    }

    fn on_spawn(&mut self, id: TaskId) {
        let p = Self::HIGH_BASE + (self.rng.next_u64() >> 1);
        self.priorities.insert(id, p);
    }
}

struct TimerEntry {
    deadline: u64,
    id: u64,
    waker: Waker,
}

impl PartialEq for TimerEntry {
    fn eq(&self, o: &Self) -> bool {
        self.deadline == o.deadline && self.id == o.id
    }
}
impl Eq for TimerEntry {}
impl PartialOrd for TimerEntry {
    fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(o))
    }
}
impl Ord for TimerEntry {
    fn cmp(&self, o: &Self) -> std::cmp::Ordering {
        (self.deadline, self.id).cmp(&(o.deadline, o.id))
    }
}

struct Task {
    future: Pin<Box<dyn Future<Output = ()> + Send>>,
}

struct Inner {
    next_task: u64,
    next_timer: u64,
    /// Virtual time, in nanoseconds since the run started.
    now: u64,
    tasks: BTreeMap<TaskId, Task>,
    ready: BTreeSet<TaskId>,
    timers: BinaryHeap<Reverse<TimerEntry>>,
    scheduler: Box<dyn Scheduler>,
    /// Simulated entropy source for `fill_random` (e.g. `TxId` prefixes), seeded
    /// so the run is reproducible.
    entropy: Rng,
}

/// Tasks woken via a `Waker`. The only state shared with wakers, so it is the
/// only part that must be `Send + Sync`.
#[derive(Default)]
struct WakeQueue {
    woken: Mutex<Vec<TaskId>>,
}

impl WakeQueue {
    fn push(&self, t: TaskId) {
        self.woken.lock().unwrap().push(t);
    }
    fn take(&self) -> Vec<TaskId> {
        std::mem::take(&mut *self.woken.lock().unwrap())
    }
}

struct TaskWaker {
    q: Arc<WakeQueue>,
    tid: TaskId,
}

impl Wake for TaskWaker {
    fn wake(self: Arc<Self>) {
        self.q.push(self.tid);
    }
    fn wake_by_ref(self: &Arc<Self>) {
        self.q.push(self.tid);
    }
}

/// A cheap handle to the running executor, kept in a thread-local so `spawn`,
/// `sleep`, and `Instant::now` can reach it without being threaded through every
/// call. The futures live behind `Rc<RefCell<..>>` (single-threaded), while the
/// wake queue is the `Arc` shared with wakers.
#[derive(Clone)]
pub(crate) struct Handle {
    inner: Rc<RefCell<Inner>>,
    wake: Arc<WakeQueue>,
}

thread_local! {
    static CURRENT: RefCell<Option<Handle>> = const { RefCell::new(None) };
}

pub(crate) fn current() -> Option<Handle> {
    CURRENT.with(|c| c.borrow().clone())
}

/// Reports whether a deterministic executor is running on this thread.
pub fn in_sim() -> bool {
    CURRENT.with(|c| c.borrow().is_some())
}

impl Handle {
    fn now(&self) -> u64 {
        self.inner.borrow().now
    }

    fn spawn_raw(&self, fut: Pin<Box<dyn Future<Output = ()> + Send>>) -> TaskId {
        let mut inner = self.inner.borrow_mut();
        let id = TaskId(inner.next_task);
        inner.next_task += 1;
        inner.tasks.insert(id, Task { future: fut });
        inner.ready.insert(id);
        inner.scheduler.on_spawn(id);
        id
    }

    fn register_timer(&self, deadline: u64, waker: Waker) {
        let mut inner = self.inner.borrow_mut();
        let id = inner.next_timer;
        inner.next_timer += 1;
        inner.timers.push(Reverse(TimerEntry {
            deadline,
            id,
            waker,
        }));
    }
}

/// Restores the previous `CURRENT` handle when dropped, even on panic.
struct CurrentGuard(Option<Handle>);

impl Drop for CurrentGuard {
    fn drop(&mut self) {
        CURRENT.with(|c| *c.borrow_mut() = self.0.take());
    }
}

/// Spawns `f` onto the running executor, returning a receiver for its result.
pub(crate) fn det_spawn<F>(f: F) -> tokio::sync::oneshot::Receiver<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    let h = current().expect("rt::spawn called outside the simulation executor");
    let (tx, rx) = tokio::sync::oneshot::channel();
    let fut = async move {
        let v = f.await;
        let _ = tx.send(v);
    };
    h.spawn_raw(Box::pin(fut));
    rx
}

/// A future that resolves once virtual time reaches its deadline.
pub(crate) struct DetSleep {
    deadline: u64,
    armed: bool,
}

pub(crate) fn det_sleep(d: Duration) -> DetSleep {
    let now = current()
        .expect("rt::sleep called outside the simulation executor")
        .now();
    DetSleep {
        deadline: now.saturating_add(d.as_nanos() as u64),
        armed: false,
    }
}

impl Future for DetSleep {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let h = current().expect("DetSleep polled outside the simulation executor");
        if h.now() >= self.deadline {
            return Poll::Ready(());
        }
        if !self.armed {
            h.register_timer(self.deadline, cx.waker().clone());
            self.armed = true;
        }
        Poll::Pending
    }
}

/// Current virtual time in nanoseconds since the run started. Panics if no
/// executor is running.
pub(crate) fn now_nanos() -> u64 {
    current()
        .expect("rt clock read outside the simulation executor")
        .now()
}

/// Fills `buf` with deterministic simulated entropy from the run's seeded RNG.
/// Panics if no executor is running.
pub(crate) fn fill_random(buf: &mut [u8]) {
    let h = current().expect("rt::fill_random called outside the simulation executor");
    h.inner.borrow_mut().entropy.fill(buf);
}

/// A future that yields once, then completes. Equivalent to
/// `tokio::task::yield_now` for the deterministic executor.
#[derive(Default)]
pub(crate) struct DetYield {
    yielded: bool,
}

impl Future for DetYield {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.yielded {
            Poll::Ready(())
        } else {
            self.yielded = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Runs `root` to completion on a fresh deterministic executor driven by
/// `scheduler`, returning its output. Background tasks still pending when `root`
/// completes are dropped (matching `tokio`'s `block_on`).
///
/// Panics deterministically if the system deadlocks: no task is runnable and no
/// timer is pending while `root` has not completed.
///
/// Panics deterministically if the run exceeds [`DEFAULT_STEP_BUDGET`] scheduler
/// steps, treating non-termination (livelock / infinite retry) as a failure
/// instead of hanging forever.
///
/// `entropy_seed` seeds the simulated entropy source read by [`fill_random`].
pub fn block_on_with<S, F, T>(scheduler: S, entropy_seed: u64, root: F) -> T
where
    S: Scheduler + 'static,
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    block_on_with_budget(scheduler, entropy_seed, DEFAULT_STEP_BUDGET, root)
}

/// As [`block_on_with`], but with an explicit per-run step `budget`. Private so
/// the public entry point always uses [`DEFAULT_STEP_BUDGET`]; a small budget
/// lets the tests exercise the non-termination guard without spinning millions
/// of steps.
fn block_on_with_budget<S, F, T>(scheduler: S, entropy_seed: u64, budget: u64, root: F) -> T
where
    S: Scheduler + 'static,
    F: Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    // Seed tokio's `select!` branch-poll RNG from `entropy_seed` so that even a
    // non-`biased` `select!` (ours or one inside a dependency) replays
    // identically across runs instead of drawing from tokio's OS-seeded
    // thread-local RNG. tokio swaps in the runtime's `RngSeed` only when it
    // *enters* the runtime, and on current-thread that enter happens inside
    // `block_on` (the public `Runtime::enter()` does not touch the select RNG).
    // So we drive our own synchronous `run_loop` from within `block_on`: the
    // seed is live for every task poll, and `tokio::select!` reads it on each
    // poll, making the branch order a pure function of the seed. A fresh runtime
    // per call is required - entering advances the seed generator, so reusing one
    // would hand successive runs different seeds and reintroduce divergence.
    // Requires `--cfg tokio_unstable` (paired with `--cfg sim`).
    let tokio_rt = tokio::runtime::Builder::new_current_thread()
        .rng_seed(tokio::runtime::RngSeed::from_bytes(
            &entropy_seed.to_le_bytes(),
        ))
        .build()
        .expect("build sim-local tokio runtime for select-rng seeding");

    let wake = Arc::new(WakeQueue::default());
    let inner = Rc::new(RefCell::new(Inner {
        next_task: 0,
        next_timer: 0,
        now: 0,
        tasks: BTreeMap::new(),
        ready: BTreeSet::new(),
        timers: BinaryHeap::new(),
        scheduler: Box::new(scheduler),
        entropy: Rng::new(entropy_seed),
    }));
    let handle = Handle {
        inner: inner.clone(),
        wake: wake.clone(),
    };

    let prev = CURRENT.with(|c| c.borrow_mut().replace(handle.clone()));
    let _guard = CurrentGuard(prev);

    let out: Arc<Mutex<Option<T>>> = Arc::new(Mutex::new(None));
    let o2 = out.clone();
    handle.spawn_raw(Box::pin(async move {
        let v = root.await;
        *o2.lock().unwrap() = Some(v);
    }));

    // Drive the synchronous executor loop inside `block_on` so the seeded select
    // RNG is installed for the duration. `unconstrained` disables tokio's
    // cooperative budget, which would otherwise force the `tokio::sync`
    // primitives our tasks use to yield after ~128 ops within our single
    // synthetic poll (we never re-enter `block_on`, so a yield would stall).
    tokio_rt.block_on(tokio::task::coop::unconstrained(std::future::poll_fn(
        |_cx| {
            run_loop(&handle, &out, budget);
            std::task::Poll::Ready(())
        },
    )));

    let result = out
        .lock()
        .unwrap()
        .take()
        .expect("root task did not complete");

    // Drop background tasks BEFORE the `CurrentGuard` clears `CURRENT`. Some
    // futures invoke `rt::spawn` from their `Drop` (e.g. `Dedup::DriverGuard`
    // hands the rest of a batch off to a freshly-spawned owner), and that
    // dispatch needs to see the simulation executor still installed. Re-entrant
    // spawns from a task's `Drop` would deadlock on the executor's `RefCell`
    // if we held it across the drop, so we repeatedly snapshot-and-clear the
    // task map until it stays empty.
    loop {
        let drained: Vec<_> = std::mem::take(&mut handle.inner.borrow_mut().tasks)
            .into_iter()
            .collect();
        if drained.is_empty() {
            break;
        }
        drop(drained);
    }
    drop(handle);

    result
}

fn run_loop<T>(handle: &Handle, out: &Arc<Mutex<Option<T>>>, budget: u64) {
    let mut steps: u64 = 0;
    loop {
        // Bound a single execution: a run that never terminates (livelock /
        // infinite retry) trips this deterministic step budget and panics rather
        // than hanging the fuzzer or a corpus replay forever.
        steps += 1;
        if steps > budget {
            panic!(
                "simulation executor exceeded its step budget ({budget} scheduler \
                 steps): the run is not terminating (livelock or infinite retry loop)"
            );
        }

        // 1. Fold woken tasks into the (sorted) ready set.
        let woken = handle.wake.take();
        {
            let mut inner = handle.inner.borrow_mut();
            for tid in woken {
                if inner.tasks.contains_key(&tid) {
                    inner.ready.insert(tid);
                }
            }
        }

        if out.lock().unwrap().is_some() {
            return;
        }

        // 2. If any task is ready, let the scheduler pick one and poll it.
        let picked = {
            let mut inner = handle.inner.borrow_mut();
            if inner.ready.is_empty() {
                None
            } else {
                let ready_vec: Vec<TaskId> = inner.ready.iter().copied().collect();
                let idx = inner.scheduler.pick(&ready_vec) % ready_vec.len();
                let tid = ready_vec[idx];
                inner.ready.remove(&tid);
                Some(tid)
            }
        };

        if let Some(tid) = picked {
            let taken = handle.inner.borrow_mut().tasks.remove(&tid);
            if let Some(mut task) = taken {
                let waker = Waker::from(Arc::new(TaskWaker {
                    q: handle.wake.clone(),
                    tid,
                }));
                let mut cx = Context::from_waker(&waker);
                // No executor borrow is held across the poll, so the task is free
                // to spawn, sleep, or read the clock re-entrantly.
                let poll = task.future.as_mut().poll(&mut cx);
                if poll.is_pending() {
                    handle.inner.borrow_mut().tasks.insert(tid, task);
                }
            }
            continue;
        }

        // 3. No task is runnable: advance virtual time to the next timer.
        let fired = {
            let mut inner = handle.inner.borrow_mut();
            match inner.timers.peek() {
                Some(Reverse(top)) => {
                    let deadline = top.deadline;
                    if deadline > inner.now {
                        inner.now = deadline;
                    }
                    let now = inner.now;
                    let mut fired = Vec::new();
                    while let Some(Reverse(t)) = inner.timers.peek() {
                        if t.deadline <= now {
                            fired.push(inner.timers.pop().unwrap().0.waker);
                        } else {
                            break;
                        }
                    }
                    Some(fired)
                }
                None => None,
            }
        };

        match fired {
            Some(wakers) => {
                for w in wakers {
                    w.wake();
                }
            }
            None => {
                if out.lock().unwrap().is_some() {
                    return;
                }
                panic!(
                    "simulation executor deadlock: no runnable task and no pending timer, \
                     but the root future has not completed"
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rng::Rng;
    use std::collections::BTreeMap;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// A fixed-order scheduler: always polls the lowest-id ready task. Useful as
    /// a determinism baseline.
    struct LowestFirst;
    impl Scheduler for LowestFirst {
        fn pick(&mut self, _ready: &[TaskId]) -> usize {
            0
        }
    }

    #[test]
    fn runs_a_simple_future() {
        let v = block_on_with(LowestFirst, 0, async { 1 + 2 });
        assert_eq!(v, 3);
    }

    #[test]
    #[should_panic(expected = "exceeded its step budget")]
    fn step_budget_bounds_a_runaway_execution() {
        // A run that takes far more steps than its budget allows must trip the
        // deterministic non-termination guard and panic, instead of spinning
        // forever and hanging the fuzzer or a corpus replay. A tiny budget keeps
        // the test fast: the workload below would otherwise take ~10k steps.
        block_on_with_budget(LowestFirst, 0, 100, async {
            for _ in 0..10_000 {
                DetYield::default().await;
            }
        });
    }

    #[test]
    fn sleep_advances_virtual_time_instantly() {
        // The sleep is for 10s of virtual time; the test returns immediately.
        let elapsed = block_on_with(LowestFirst, 0, async {
            let start = now_nanos();
            super::det_sleep(Duration::from_secs(10)).await;
            now_nanos() - start
        });
        assert_eq!(elapsed, Duration::from_secs(10).as_nanos() as u64);
    }

    #[test]
    fn runtime_timeout_uses_virtual_time() {
        let elapsed = block_on_with(LowestFirst, 0, async {
            let start = now_nanos();
            let result =
                crate::rt::timeout(Duration::from_secs(10), std::future::pending::<()>()).await;
            assert_eq!(result, Err(crate::rt::TimedOut));
            now_nanos() - start
        });
        assert_eq!(elapsed, Duration::from_secs(10).as_nanos() as u64);
    }

    #[test]
    fn seeded_select_order_is_deterministic_per_seed() {
        // Records the branch a non-biased `tokio::select!` picks for 32 decisions
        // where both branches are immediately ready, so the only thing choosing
        // is tokio's branch-poll RNG. `block_on_with` seeds that RNG from its
        // `entropy_seed` (by entering a `RngSeed`-built runtime), so the sequence
        // must be a pure function of the seed.
        fn draws(seed: u64) -> Vec<u32> {
            block_on_with(LowestFirst, seed, async {
                let mut out = Vec::new();
                for _ in 0..32 {
                    let w: u32 = tokio::select! {
                        _ = std::future::ready(()) => 0,
                        _ = std::future::ready(()) => 1,
                    };
                    out.push(w);
                }
                out
            })
        }
        // Same seed => identical decision sequence across independent runs.
        // Without seeding on runtime-enter, tokio's thread-local select RNG would
        // carry over between the two `block_on_with` calls and the sequences
        // would diverge (the bug this guards against).
        assert_eq!(draws(7), draws(7));
        assert_eq!(draws(123), draws(123));
        // The seed genuinely drives the order (so it is seeded, not fixed): some
        // sequence differs, proving the choice is not constant.
        assert!(
            draws(7) != draws(8) || draws(1) != draws(2),
            "select order did not vary with the seed; tokio's branch RNG may not \
             be seeded by our runtime"
        );
    }

    #[test]
    fn entropy_is_seed_reproducible() {
        fn draw(seed: u64) -> [u8; 16] {
            block_on_with(LowestFirst, seed, async {
                let mut buf = [0u8; 16];
                super::fill_random(&mut buf);
                buf
            })
        }
        assert_eq!(draw(7), draw(7));
        assert_ne!(draw(7), draw(8));
    }

    #[test]
    fn spawn_and_join_over_tokio_sync() {
        let sum = block_on_with(LowestFirst, 0, async {
            let (tx, rx) = tokio::sync::oneshot::channel::<u32>();
            let h = det_spawn(async move {
                let _ = tx.send(41);
                1u32
            });
            let got = rx.await.unwrap();
            let joined = h.await.unwrap();
            got + joined
        });
        assert_eq!(sum, 42);
    }

    #[test]
    fn notify_wakes_across_tasks() {
        let order = block_on_with(LowestFirst, 0, async {
            let n = Arc::new(tokio::sync::Notify::new());
            let seen = Arc::new(AtomicUsize::new(0));
            let n2 = n.clone();
            let s2 = seen.clone();
            let h = det_spawn(async move {
                n2.notified().await;
                s2.fetch_add(1, Ordering::SeqCst);
            });
            // Let the spawned task register its waiter, then notify.
            DetYield::default().await;
            n.notify_one();
            h.await.unwrap();
            seen.load(Ordering::SeqCst)
        });
        assert_eq!(order, 1);
    }

    #[test]
    fn same_tape_is_byte_identical() {
        fn run(tape: Vec<u8>) -> Vec<u32> {
            block_on_with(TapeScheduler::new(tape), 0, async {
                let log = Arc::new(Mutex::new(Vec::new()));
                let mut handles = Vec::new();
                for i in 0..4u32 {
                    let log = log.clone();
                    handles.push(det_spawn(async move {
                        for _ in 0..3 {
                            log.lock().unwrap().push(i);
                            DetYield::default().await;
                        }
                    }));
                }
                for h in handles {
                    h.await.unwrap();
                }
                Arc::try_unwrap(log).unwrap().into_inner().unwrap()
            })
        }
        let tape = vec![3, 1, 2, 0, 1, 3, 2, 0, 1, 2, 3, 0];
        let a = run(tape.clone());
        let b = run(tape);
        assert_eq!(a, b);
        // A different tape should generally produce a different interleaving.
        let c = run(vec![0; 16]);
        assert_ne!(a, c);
    }

    #[test]
    fn tape_scheduler_consumes_bytes_modulo_ready_set_then_falls_back() {
        let mut scheduler = TapeScheduler::new(vec![5, 4]);
        let ready = [TaskId(10), TaskId(20), TaskId(30)];

        assert_eq!(scheduler.pick(&ready), 2, "5 % 3 selects index 2");
        assert_eq!(scheduler.pick(&ready), 1, "4 % 3 selects index 1");
        assert_eq!(
            scheduler.pick(&ready),
            0,
            "exhausted tapes fall back to the deterministic lowest-ready choice"
        );
    }

    #[test]
    fn executor_presents_ready_tasks_in_sorted_order() {
        struct ReadyRecorder {
            seen: Arc<Mutex<Vec<Vec<TaskId>>>>,
        }
        impl Scheduler for ReadyRecorder {
            fn pick(&mut self, ready: &[TaskId]) -> usize {
                self.seen.lock().unwrap().push(ready.to_vec());
                0
            }
        }

        let seen = Arc::new(Mutex::new(Vec::new()));
        block_on_with(ReadyRecorder { seen: seen.clone() }, 0, async {
            let mut handles = Vec::new();
            for _ in 0..3 {
                handles.push(det_spawn(async {
                    DetYield::default().await;
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
        });

        let seen = seen.lock().unwrap();
        assert!(
            seen.iter().any(|ready| ready.len() > 1),
            "test never presented multiple ready tasks to the scheduler: {seen:?}"
        );
        for ready in seen.iter() {
            assert!(
                ready.windows(2).all(|pair| pair[0] < pair[1]),
                "ready set was not sorted: {ready:?}"
            );
        }
    }

    /// Drives four yielding tasks under a [`PctScheduler`] and returns the order
    /// in which their steps ran.
    fn pct_order(seed: u64) -> Vec<u32> {
        block_on_with(PctScheduler::new(seed, 3, 64), 0, async {
            let log = Arc::new(Mutex::new(Vec::new()));
            let mut handles = Vec::new();
            for i in 0..4u32 {
                let log = log.clone();
                handles.push(det_spawn(async move {
                    for _ in 0..3 {
                        log.lock().unwrap().push(i);
                        DetYield::default().await;
                    }
                }));
            }
            for h in handles {
                h.await.unwrap();
            }
            Arc::try_unwrap(log).unwrap().into_inner().unwrap()
        })
    }

    #[test]
    fn pct_is_seed_reproducible() {
        for seed in [0u64, 1, 42, 9999] {
            assert_eq!(pct_order(seed), pct_order(seed), "seed {seed} not stable");
        }
    }

    #[test]
    fn pct_explores_interleavings() {
        // Different seeds should generally yield different interleavings, or PCT
        // would not be sampling the schedule space.
        let baseline = pct_order(0);
        let differs = (1u64..32).any(|s| pct_order(s) != baseline);
        assert!(differs, "no PCT seed in 1..32 changed the interleaving");
    }

    #[test]
    fn pct_change_point_demotes_selected_task() {
        let mut scheduler = PctScheduler {
            rng: Rng::new(0),
            priorities: BTreeMap::from([(TaskId(1), 100), (TaskId(2), 90)]),
            change_points: vec![1],
            step: 0,
            low_next: 0,
        };
        let ready = [TaskId(1), TaskId(2)];

        assert_eq!(scheduler.pick(&ready), 0);
        assert_eq!(scheduler.priorities[&TaskId(1)], 1);
        assert_eq!(
            scheduler.pick(&ready),
            1,
            "the demoted task must yield to the next-highest priority"
        );
    }
}
