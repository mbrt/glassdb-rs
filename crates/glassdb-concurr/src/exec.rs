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
struct WakeQueue {
    woken: Mutex<Vec<TaskId>>,
}

impl WakeQueue {
    fn new() -> Self {
        WakeQueue {
            woken: Mutex::new(Vec::new()),
        }
    }
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
/// `entropy_seed` seeds the simulated entropy source read by [`fill_random`].
pub fn block_on_with<S, F, T>(scheduler: S, entropy_seed: u64, root: F) -> T
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

    let wake = Arc::new(WakeQueue::new());
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
            run_loop(&handle, &out);
            std::task::Poll::Ready(())
        },
    )));

    let result = out
        .lock()
        .unwrap()
        .take()
        .expect("root task did not complete");
    result
}

fn run_loop<T>(handle: &Handle, out: &Arc<Mutex<Option<T>>>) {
    loop {
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
}
