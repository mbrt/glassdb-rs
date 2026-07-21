//! One-shot slow mutation injection for deterministic simulation.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::Duration;

use glassdb_backend::Backend;
use glassdb_backend::middleware::{BackendOp, HookBackend, HookFuture};
use glassdb_concurr::{Tape, rt};
use glassdb_trans::ProtocolTiming;

const MUTATION_WINDOW: u64 = 8;
const BOUNDARY_EPSILON: Duration = Duration::from_millis(1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DelayPoint {
    Before,
    After,
}

#[derive(Debug, Clone, Copy)]
struct SlowMutationPlan {
    ordinal: usize,
    point: DelayPoint,
    duration: Duration,
}

impl SlowMutationPlan {
    fn from_tape(tape: &mut Tape, timing: ProtocolTiming) -> Self {
        let ordinal = tape.below(MUTATION_WINDOW) as usize;
        let point = if tape.roll(128) {
            DelayPoint::Before
        } else {
            DelayPoint::After
        };
        let pending = timing.pending_timeout();
        let duration = match tape.below(3) {
            0 => pending.saturating_sub(BOUNDARY_EPSILON),
            1 => pending + BOUNDARY_EPSILON,
            _ => pending + timing.max_clock_skew() + BOUNDARY_EPSILON,
        };
        Self {
            ordinal,
            point,
            duration,
        }
    }
}

struct SlowMutationController {
    plan: SlowMutationPlan,
    seen: AtomicUsize,
    injected: AtomicBool,
}

impl SlowMutationController {
    fn new(plan: SlowMutationPlan) -> Self {
        Self {
            plan,
            seen: AtomicUsize::new(0),
            injected: AtomicBool::new(false),
        }
    }

    fn claim(&self) -> Option<Duration> {
        let ordinal = self.seen.fetch_add(1, Ordering::SeqCst);
        if ordinal != self.plan.ordinal || self.injected.swap(true, Ordering::SeqCst) {
            return None;
        }
        Some(self.plan.duration)
    }

    #[cfg(test)]
    fn injected(&self) -> bool {
        self.injected.load(Ordering::SeqCst)
    }
}

fn is_mutation(operation: &BackendOp<'_>) -> bool {
    matches!(
        operation,
        BackendOp::WriteIf { .. } | BackendOp::WriteIfNotExists { .. } | BackendOp::DeleteIf { .. }
    )
}

fn delay_selected(
    controller: &Arc<SlowMutationController>,
    operation: &BackendOp<'_>,
) -> HookFuture {
    let duration = is_mutation(operation).then(|| controller.claim()).flatten();
    Box::pin(async move {
        if let Some(duration) = duration {
            rt::sleep(duration).await;
        }
        Ok(())
    })
}

fn with_plan(
    inner: Arc<dyn Backend>,
    plan: SlowMutationPlan,
) -> (Arc<HookBackend>, Arc<SlowMutationController>) {
    let backend = HookBackend::new(inner);
    let controller = Arc::new(SlowMutationController::new(plan));
    match plan.point {
        DelayPoint::Before => backend.set_before({
            let controller = controller.clone();
            move |operation| delay_selected(&controller, operation)
        }),
        DelayPoint::After => backend.set_after({
            let controller = controller.clone();
            // HookBackend has no per-call token, so selecting at completion
            // avoids assigning the delay to a different concurrent mutation.
            move |operation, _| delay_selected(&controller, operation)
        }),
    }
    (backend, controller)
}

/// Wraps a backend with a simulation-only hook that delays at most one
/// conditional mutation without serializing other operations.
pub(super) fn with_tape(
    inner: Arc<dyn Backend>,
    tape: Vec<u8>,
    seed: u64,
    timing: ProtocolTiming,
) -> Arc<dyn Backend> {
    let mut tape = Tape::new(tape, seed);
    let plan = SlowMutationPlan::from_tape(&mut tape, timing);
    with_plan(inner, plan).0
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;

    use glassdb_backend::{BackendError, Version, memory::MemoryBackend};

    use super::*;

    fn wrapper(
        inner: Arc<dyn Backend>,
        plan: SlowMutationPlan,
    ) -> (Arc<HookBackend>, Arc<SlowMutationController>) {
        with_plan(inner, plan)
    }

    #[tokio::test(start_paused = true)]
    async fn delays_before_forwarding() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (slow, _) = wrapper(
            inner.clone(),
            SlowMutationPlan {
                ordinal: 0,
                point: DelayPoint::Before,
                duration: Duration::from_secs(10),
            },
        );
        let task = tokio::spawn({
            let slow = slow.clone();
            async move { slow.write_if_not_exists("p", b"v".to_vec()).await }
        });

        tokio::task::yield_now().await;
        assert!(matches!(inner.read("p").await, Err(BackendError::NotFound)));
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_secs(10)).await;
        task.await.unwrap().unwrap();
        assert_eq!(inner.read("p").await.unwrap().contents, b"v");
    }

    #[tokio::test(start_paused = true)]
    async fn delays_after_the_mutation_lands() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let (slow, _) = wrapper(
            inner.clone(),
            SlowMutationPlan {
                ordinal: 0,
                point: DelayPoint::After,
                duration: Duration::from_secs(10),
            },
        );
        let task = tokio::spawn({
            let slow = slow.clone();
            async move { slow.write_if_not_exists("p", b"v".to_vec()).await }
        });

        tokio::task::yield_now().await;
        assert_eq!(inner.read("p").await.unwrap().contents, b"v");
        assert!(!task.is_finished());
        tokio::time::advance(Duration::from_secs(10)).await;
        task.await.unwrap().unwrap();
    }

    #[tokio::test(start_paused = true)]
    async fn only_mutations_consume_the_one_shot_ordinal() {
        let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let initial = inner
            .write_if_not_exists("p", b"v1".to_vec())
            .await
            .unwrap();
        let (slow, controller) = wrapper(
            inner.clone(),
            SlowMutationPlan {
                ordinal: 1,
                point: DelayPoint::Before,
                duration: Duration::from_secs(10),
            },
        );

        slow.read("p").await.unwrap();
        slow.read_if_modified("p", &Version::new("different"))
            .await
            .unwrap();
        slow.list("", None, NonZeroUsize::new(10).unwrap())
            .await
            .unwrap();
        let updated = slow.write_if("p", b"v2".to_vec(), &initial).await.unwrap();

        let delete = tokio::spawn({
            let slow = slow.clone();
            async move { slow.delete_if("p", &updated).await }
        });
        tokio::task::yield_now().await;
        assert!(controller.injected());
        assert!(!delete.is_finished());
        tokio::time::advance(Duration::from_secs(10)).await;
        delete.await.unwrap().unwrap();

        slow.write_if_not_exists("q", b"v".to_vec()).await.unwrap();
        assert_eq!(inner.read("q").await.unwrap().contents, b"v");
    }
}
