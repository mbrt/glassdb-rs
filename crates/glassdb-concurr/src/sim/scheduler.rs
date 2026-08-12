//! Scheduling policies for the deterministic simulation executor.

use std::collections::BTreeMap;

use crate::rng::Rng;
use crate::sim::executor::{RuntimeEntropySource, RuntimeTraceEvent, RuntimeTraceObserver, TaskId};

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
    trace: Option<RuntimeTraceObserver>,
}

impl TapeScheduler {
    /// Builds a scheduler that replays `tape`, then selects the lowest ready task.
    pub fn new(tape: Vec<u8>) -> Self {
        TapeScheduler {
            tape,
            pos: 0,
            trace: None,
        }
    }

    /// Builds a tape scheduler that observes each consumed schedule byte.
    pub fn new_traced(tape: Vec<u8>, trace: RuntimeTraceObserver) -> Self {
        TapeScheduler {
            tape,
            pos: 0,
            trace: Some(trace),
        }
    }
}

impl Scheduler for TapeScheduler {
    fn pick(&mut self, ready: &[TaskId]) -> usize {
        let input = self.tape.get(self.pos).copied();
        let b = input.unwrap_or(0);
        self.pos = self.pos.wrapping_add(1);
        if input.is_some() {
            trace_scheduler_draw(
                self.trace.as_ref(),
                RuntimeEntropySource::SchedulerInput,
                &[b],
            );
        }
        (b as usize) % ready.len()
    }
}

/// Schedules by picking a uniformly random ready task from a seeded PRNG. Used
/// for FoundationDB-style seed-breadth runs and as the base for PCT.
pub struct RandomScheduler {
    rng: Rng,
}

impl RandomScheduler {
    /// Builds a randomized scheduler whose decisions replay from `seed`.
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
    trace: Option<RuntimeTraceObserver>,
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
        Self::build(seed, depth, steps, None)
    }

    /// Builds a PCT scheduler that observes change-point and priority draws.
    pub fn new_traced(seed: u64, depth: usize, steps: u64, trace: RuntimeTraceObserver) -> Self {
        Self::build(seed, depth, steps, Some(trace))
    }

    fn build(seed: u64, depth: usize, steps: u64, trace: Option<RuntimeTraceObserver>) -> Self {
        let mut rng = Rng::new(seed);
        let steps = steps.max(1);
        let n = depth.saturating_sub(1);
        let mut change_points = Vec::with_capacity(n);
        for _ in 0..n {
            let draw = rng.next_u64();
            trace_scheduler_draw(
                trace.as_ref(),
                RuntimeEntropySource::SchedulerRng,
                &draw.to_le_bytes(),
            );
            change_points.push(1 + draw % steps);
        }
        PctScheduler {
            rng,
            trace,
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
        let draw = self.rng.next_u64();
        trace_scheduler_draw(
            self.trace.as_ref(),
            RuntimeEntropySource::SchedulerRng,
            &draw.to_le_bytes(),
        );
        let p = Self::HIGH_BASE + (draw >> 1);
        self.priorities.insert(id, p);
    }
}

fn trace_scheduler_draw(
    trace: Option<&RuntimeTraceObserver>,
    source: RuntimeEntropySource,
    bytes: &[u8],
) {
    if let Some(trace) = trace {
        trace(RuntimeTraceEvent::EntropyDraw {
            source,
            bytes: bytes.to_vec(),
        });
    }
}

/// Picks the lowest task id, the fixed-order baseline used by executor tests.
#[cfg(test)]
pub(crate) struct LowestFirst;

#[cfg(test)]
impl Scheduler for LowestFirst {
    fn pick(&mut self, _ready: &[TaskId]) -> usize {
        0
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::*;
    use crate::exec::{DetYield, block_on_with, det_spawn};

    #[test]
    fn random_scheduler_seeded_selection_matches_reviewed_vector() {
        let ready = [TaskId(10), TaskId(20), TaskId(30), TaskId(40)];
        let mut scheduler = RandomScheduler::new(0xF31A);
        let selected: Vec<_> = (0..8).map(|_| scheduler.pick(&ready)).collect();

        assert_eq!(selected, [0, 2, 3, 0, 1, 0, 1, 2]);
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
                for handle in handles {
                    handle.await.unwrap();
                }
                Arc::try_unwrap(log).unwrap().into_inner().unwrap()
            })
        }
        let tape = vec![3, 1, 2, 0, 1, 3, 2, 0, 1, 2, 3, 0];
        let first = run(tape.clone());
        let second = run(tape);
        assert_eq!(first, second);
        // A different tape should generally produce a different interleaving.
        let different = run(vec![0; 16]);
        assert_ne!(first, different);
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
            for handle in handles {
                handle.await.unwrap();
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
        let differs = (1u64..32).any(|seed| pct_order(seed) != baseline);
        assert!(differs, "no PCT seed in 1..32 changed the interleaving");
    }

    #[test]
    fn pct_change_point_demotes_selected_task() {
        let mut scheduler = PctScheduler {
            rng: Rng::new(0),
            trace: None,
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
