//! Scheduling policies for the deterministic simulation executor.

use std::collections::BTreeMap;

use crate::rng::Rng;

/// Unique id of a task within a single executor run. Assigned in spawn order,
/// so it is a deterministic function of the schedule.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug)]
pub struct TaskId(pub u64);

/// Decides which ready task to poll next. Implementations must be deterministic
/// functions of their own state so the entire run replays from a seed/tape.
/// Callbacks may observe or spawn work through [`crate::rt`]; spawn
/// notifications produced within a callback are delivered afterward in task
/// creation order.
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
/// byte selects which ready task runs. This makes interleavings directly
/// mutable by the coverage-guided fuzzer (see ADR-010/011). When the tape is
/// exhausted, the scheduler selects the lowest ready task, or a random ready
/// task from a seeded PRNG. Both fallbacks keep the run deterministic.
pub struct TapeScheduler {
    tape: Vec<u8>,
    pos: usize,
    fallback: Option<RandomScheduler>,
}

impl TapeScheduler {
    /// Seed offset of the fallback PRNG. A run seeds other streams with the
    /// same seed, so the offset keeps the scheduling draws independent of them.
    const FALLBACK_STREAM: u64 = 0x7A9E_5C4E_D0FA_11BA;

    /// Builds a scheduler that replays `tape`, then selects the lowest ready task.
    pub fn new(tape: Vec<u8>) -> Self {
        TapeScheduler {
            tape,
            pos: 0,
            fallback: None,
        }
    }

    /// Builds a scheduler that replays `tape`, then selects a random ready task
    /// with a PRNG seeded from `seed`. A fuzzer input can steer only the
    /// decisions that its tape covers, and a fixed choice after the tape always
    /// runs the lowest ready task, so it explores only one order. The random
    /// choice keeps the later decisions varied.
    pub fn with_random_fallback(tape: Vec<u8>, seed: u64) -> Self {
        TapeScheduler {
            tape,
            pos: 0,
            fallback: Some(RandomScheduler::new(seed ^ Self::FALLBACK_STREAM)),
        }
    }
}

impl Scheduler for TapeScheduler {
    fn pick(&mut self, ready: &[TaskId]) -> usize {
        let byte = self.tape.get(self.pos).copied();
        self.pos = self.pos.saturating_add(1);
        match (byte, &mut self.fallback) {
            (Some(b), _) => b as usize % ready.len(),
            (None, Some(random)) => random.pick(ready),
            (None, None) => 0,
        }
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
            let draw = rng.next_u64();
            change_points.push(1 + draw % steps);
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
        let draw = self.rng.next_u64();
        let p = Self::HIGH_BASE + (draw >> 1);
        self.priorities.insert(id, p);
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
mod sim_tests {
    use super::*;

    #[test]
    fn random_scheduler_seeded_selection_matches_reviewed_vector() {
        let ready = [TaskId(10), TaskId(20), TaskId(30), TaskId(40)];
        let mut scheduler = RandomScheduler::new(0xF31A);
        let selected: Vec<_> = (0..8).map(|_| scheduler.pick(&ready)).collect();

        assert_eq!(selected, [0, 2, 3, 0, 1, 0, 1, 2]);
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
    fn exhausted_tape_falls_back_to_seeded_random_choices() {
        let ready = [TaskId(10), TaskId(20), TaskId(30), TaskId(40)];
        let picks = |seed| {
            let mut scheduler = TapeScheduler::with_random_fallback(vec![5], seed);
            (0..16).map(|_| scheduler.pick(&ready)).collect::<Vec<_>>()
        };

        let picks_7 = picks(7);
        assert_eq!(
            picks_7[0], 1,
            "the tape decides first: 5 % 4 selects index 1"
        );
        assert_eq!(picks_7, picks(7), "the fallback replays from its seed");
        assert_ne!(picks_7, picks(8), "other seeds give other fallback choices");
        assert!(
            picks_7[1..].iter().any(|&pick| pick != 0),
            "the fallback does not always select the lowest ready task"
        );
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
