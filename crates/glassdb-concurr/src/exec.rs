//! Deterministic simulation execution.
//!
//! This module is the control plane used to start a simulation with a chosen
//! scheduling policy. Code running inside that simulation uses [`crate::rt`]
//! for spawning, time, and other runtime services.

pub(crate) mod executor;
mod scheduler;

pub use executor::block_on_with;
pub use scheduler::{PctScheduler, RandomScheduler, Scheduler, TapeScheduler, TaskId};

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use super::executor::{DetYield, det_spawn};
    use super::{PctScheduler, Scheduler, TapeScheduler, TaskId, block_on_with};

    fn yielding_task_order<S>(scheduler: S) -> Vec<u32>
    where
        S: Scheduler + 'static,
    {
        block_on_with(scheduler, 0, async {
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
    fn tape_execution_replays_and_varies() {
        let tape = vec![3, 1, 2, 0, 1, 3, 2, 0, 1, 2, 3, 0];
        let first = yielding_task_order(TapeScheduler::new(tape.clone()));
        let second = yielding_task_order(TapeScheduler::new(tape));
        assert_eq!(first, second);
        assert_ne!(first, yielding_task_order(TapeScheduler::new(vec![0; 16])));
    }

    #[test]
    fn pct_execution_replays_and_explores() {
        for seed in [0u64, 1, 42, 9999] {
            assert_eq!(
                yielding_task_order(PctScheduler::new(seed, 3, 64)),
                yielding_task_order(PctScheduler::new(seed, 3, 64)),
                "seed {seed} not stable"
            );
        }

        let baseline = yielding_task_order(PctScheduler::new(0, 3, 64));
        assert!(
            (1u64..32).any(|seed| yielding_task_order(PctScheduler::new(seed, 3, 64)) != baseline),
            "no PCT seed in 1..32 changed the interleaving"
        );
    }

    #[test]
    fn scheduler_callbacks_can_use_runtime_services() {
        struct DropAfterNotification {
            notifications: Arc<Mutex<Vec<TaskId>>>,
            id: TaskId,
        }

        impl Drop for DropAfterNotification {
            fn drop(&mut self) {
                assert!(self.notifications.lock().unwrap().contains(&self.id));
            }
        }

        struct ReentrantScheduler {
            notifications: Arc<Mutex<Vec<TaskId>>>,
            spawned_on_notification: bool,
            spawned_on_pick: bool,
        }

        impl Scheduler for ReentrantScheduler {
            fn pick(&mut self, _ready: &[TaskId]) -> usize {
                let _ = crate::rt::Instant::now();
                if !self.spawned_on_pick {
                    self.spawned_on_pick = true;
                    drop(crate::rt::spawn(async {}));
                }
                0
            }

            fn on_spawn(&mut self, id: TaskId) {
                let _ = crate::rt::Instant::now();
                self.notifications.lock().unwrap().push(id);
                if !self.spawned_on_notification {
                    self.spawned_on_notification = true;
                    drop(crate::rt::spawn(async {}));
                }
            }
        }

        let notifications = Arc::new(Mutex::new(Vec::new()));
        let final_notifications = notifications.clone();
        block_on_with(
            ReentrantScheduler {
                notifications: notifications.clone(),
                spawned_on_notification: false,
                spawned_on_pick: false,
            },
            0,
            async move {
                crate::rt::yield_now().await;
                let notice = DropAfterNotification {
                    notifications: final_notifications,
                    id: TaskId(3),
                };
                drop(crate::rt::spawn(async move {
                    let _notice = notice;
                    std::future::pending::<()>().await;
                }));
            },
        );

        assert_eq!(
            *notifications.lock().unwrap(),
            [TaskId(0), TaskId(1), TaskId(2), TaskId(3)]
        );
    }
}
