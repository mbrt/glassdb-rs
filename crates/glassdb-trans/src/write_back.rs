//! Database-local scheduling for bounded delayed write-back convergence.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use futures::future::join_all;
use glassdb_concurr::{Background, rt};
use glassdb_data::{ObjectPath, TxId};
use tokio::sync::Notify;

use crate::gc::Gc;
use crate::monitor::ProtocolTiming;
use crate::tlocker::{KeyLocker, WriteBackRetry, WriteBackRetrySink};

const WRITE_BACK_QUEUE_CAPACITY: usize = 4096;

#[derive(Clone)]
pub(crate) struct WriteBackScheduler {
    inner: Arc<SchedulerInner>,
}

struct SchedulerInner {
    background: Weak<Background>,
    locker: KeyLocker,
    gc: Gc,
    quiet_period: Duration,
    max_age: Duration,
    enabled: bool,
    queue: Mutex<Queue>,
    wake: Notify,
    closed: Notify,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Lifecycle {
    Open,
    Closing,
    Closed,
}

struct Queue {
    groups: BTreeMap<ObjectPath, PendingGroup>,
    queued: usize,
    capacity: usize,
    lifecycle: Lifecycle,
}

struct PendingGroup {
    first_enqueue: rt::Instant,
    last_activity: rt::Instant,
    retries: BTreeMap<TxId, WriteBackRetry>,
}

enum EnqueueResult {
    Accepted {
        forced: Option<(ObjectPath, PendingGroup)>,
    },
    Rejected(WriteBackRetry),
}

enum DriverAction {
    Drain {
        path: ObjectPath,
        group: PendingGroup,
        reason: DrainReason,
    },
    Wait(Option<rt::Instant>),
    Finished,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DrainReason {
    Quiet,
    MaximumAge,
    Capacity,
    Shutdown,
}

impl Queue {
    fn new(capacity: usize, enabled: bool) -> Self {
        Self {
            groups: BTreeMap::new(),
            queued: 0,
            capacity,
            lifecycle: if enabled {
                Lifecycle::Open
            } else {
                Lifecycle::Closed
            },
        }
    }

    fn enqueue(&mut self, now: rt::Instant, retry: WriteBackRetry) -> EnqueueResult {
        if self.lifecycle != Lifecycle::Open {
            return EnqueueResult::Rejected(retry);
        }

        let path = retry.leaf_hint().clone();
        let tx_id = retry.tx_id().clone();
        if let Some(existing) = self
            .groups
            .get_mut(&path)
            .and_then(|group| group.retries.get_mut(&tx_id))
        {
            existing.merge(retry);
            self.groups.get_mut(&path).unwrap().last_activity = now;
            return EnqueueResult::Accepted { forced: None };
        }

        let forced = (self.queued >= self.capacity)
            .then(|| self.take_oldest())
            .flatten();
        if self.queued >= self.capacity {
            return EnqueueResult::Rejected(retry);
        }

        let group = self.groups.entry(path).or_insert_with(|| PendingGroup {
            first_enqueue: now,
            last_activity: now,
            retries: BTreeMap::new(),
        });
        group.last_activity = now;
        group.retries.insert(tx_id, retry);
        self.queued += 1;
        EnqueueResult::Accepted { forced }
    }

    fn close(&mut self) {
        if self.lifecycle == Lifecycle::Open {
            self.lifecycle = Lifecycle::Closing;
        }
    }

    fn next_action(
        &mut self,
        now: rt::Instant,
        quiet_period: Duration,
        max_age: Duration,
    ) -> DriverAction {
        if self.lifecycle == Lifecycle::Closing {
            if let Some((path, group)) = self.take_oldest() {
                return DriverAction::Drain {
                    path,
                    group,
                    reason: DrainReason::Shutdown,
                };
            }
            self.lifecycle = Lifecycle::Closed;
            return DriverAction::Finished;
        }

        if self.lifecycle == Lifecycle::Closed {
            return DriverAction::Finished;
        }

        let next = self
            .groups
            .iter()
            .map(|(path, group)| {
                let quiet = group.last_activity + quiet_period;
                let maximum = group.first_enqueue + max_age;
                let (deadline, reason) = if maximum <= quiet {
                    (maximum, DrainReason::MaximumAge)
                } else {
                    (quiet, DrainReason::Quiet)
                };
                (deadline, path, reason)
            })
            .min_by(|left, right| left.0.cmp(&right.0).then_with(|| left.1.cmp(right.1)));
        let Some((deadline, path, reason)) = next else {
            return DriverAction::Wait(None);
        };
        if deadline > now {
            return DriverAction::Wait(Some(deadline));
        }
        let path = path.clone();
        let group = self.remove_group(&path).unwrap();
        DriverAction::Drain {
            path,
            group,
            reason,
        }
    }

    fn take_oldest(&mut self) -> Option<(ObjectPath, PendingGroup)> {
        let path = self
            .groups
            .iter()
            .min_by(|left, right| {
                left.1
                    .first_enqueue
                    .cmp(&right.1.first_enqueue)
                    .then_with(|| left.0.cmp(right.0))
            })
            .map(|(path, _)| path.clone())?;
        let group = self.remove_group(&path).unwrap();
        Some((path, group))
    }

    fn remove_group(&mut self, path: &ObjectPath) -> Option<PendingGroup> {
        let group = self.groups.remove(path)?;
        self.queued -= group.retries.len();
        Some(group)
    }
}

impl WriteBackScheduler {
    pub(crate) fn new(
        background: Option<Weak<Background>>,
        locker: KeyLocker,
        gc: Gc,
        timing: ProtocolTiming,
    ) -> Self {
        let quiet_period = timing.write_back_quiet_period();
        let max_age = timing.write_back_max_age();
        let background = background.unwrap_or_default();
        let background_owner = background.upgrade();
        let enabled = !quiet_period.is_zero() && !max_age.is_zero() && background_owner.is_some();
        let scheduler = Self {
            inner: Arc::new(SchedulerInner {
                background: background.clone(),
                locker,
                gc,
                quiet_period,
                max_age,
                enabled,
                queue: Mutex::new(Queue::new(WRITE_BACK_QUEUE_CAPACITY, enabled)),
                wake: Notify::new(),
                closed: Notify::new(),
            }),
        };
        if enabled && let Some(background) = background_owner {
            let driver = scheduler.clone();
            background.spawn(async move { driver.drive().await });
        }
        scheduler
    }

    pub(crate) fn enabled(&self) -> bool {
        self.inner.enabled
    }

    pub(crate) async fn close(&self) {
        if !self.inner.enabled {
            return;
        }
        {
            let mut queue = self.inner.queue.lock().unwrap();
            queue.close();
        }
        self.inner.wake.notify_one();
        loop {
            let closed = self.inner.closed.notified();
            if self.inner.queue.lock().unwrap().lifecycle == Lifecycle::Closed {
                return;
            }
            closed.await;
        }
    }

    async fn drive(&self) {
        loop {
            let wake = self.inner.wake.notified();
            tokio::pin!(wake);
            let action = self.inner.queue.lock().unwrap().next_action(
                rt::Instant::now(),
                self.inner.quiet_period,
                self.inner.max_age,
            );
            match action {
                DriverAction::Drain {
                    path,
                    group,
                    reason,
                } => self.dispatch(path, group, reason),
                DriverAction::Wait(Some(deadline)) => {
                    let delay = deadline.saturating_duration_since(rt::Instant::now());
                    tokio::select! {
                        _ = rt::sleep(delay) => {}
                        _ = &mut wake => {}
                    }
                }
                DriverAction::Wait(None) => wake.await,
                DriverAction::Finished => {
                    self.inner.closed.notify_waiters();
                    return;
                }
            }
        }
    }

    fn dispatch(&self, path: ObjectPath, group: PendingGroup, reason: DrainReason) {
        let Some(background) = self.inner.background.upgrade() else {
            return;
        };
        let retries = group.retries.into_values().collect::<Vec<_>>();
        tracing::debug!(
            target: "glassdb::write_back",
            leaf = %path,
            count = retries.len(),
            ?reason,
            "draining delayed write-back group"
        );
        let locker = self.inner.locker.clone();
        let gc = self.inner.gc.clone();
        background.spawn_waited(async move {
            let outcomes = join_all(retries.into_iter().map(|retry| {
                let locker = locker.clone();
                let tx_id = retry.tx_id().clone();
                async move { (tx_id, locker.retry_write_back(retry).await) }
            }))
            .await;
            for (tx_id, outcome) in outcomes {
                match outcome {
                    Ok(superseded) => {
                        for previous in superseded {
                            gc.schedule_tx_cleanup(previous);
                        }
                    }
                    Err(error) => tracing::debug!(
                        target: "glassdb::write_back",
                        tx = %tx_id,
                        %error,
                        "delayed write-back deferred"
                    ),
                }
            }
        });
    }
}

impl WriteBackRetrySink for WriteBackScheduler {
    fn try_schedule(&self, retry: WriteBackRetry) -> Result<(), WriteBackRetry> {
        if !self.inner.enabled || self.inner.background.upgrade().is_none() {
            return Err(retry);
        }
        let mut queue = self.inner.queue.lock().unwrap();
        let result = queue.enqueue(rt::Instant::now(), retry);
        match result {
            EnqueueResult::Accepted { forced } => {
                if let Some((path, group)) = forced {
                    // Capacity pressure hands the oldest group to background
                    // ownership before accepting more delayed work.
                    self.dispatch(path, group, DrainReason::Capacity);
                }
                drop(queue);
                self.inner.wake.notify_one();
                Ok(())
            }
            EnqueueResult::Rejected(retry) => {
                drop(queue);
                Err(retry)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use glassdb_data::CollectionAddress;

    use super::*;

    fn path(name: &str) -> ObjectPath {
        ObjectPath::TreeRoot {
            collection: CollectionAddress::root(name),
        }
    }

    fn retry(order: u64, leaf: &ObjectPath) -> WriteBackRetry {
        WriteBackRetry::empty(
            TxId::with_priority(order, format!("tx-{order}").as_bytes()),
            leaf.clone(),
        )
    }

    #[tokio::test(start_paused = true)]
    async fn quiet_activity_is_bounded_by_maximum_age() {
        let mut queue = Queue::new(8, true);
        let leaf = path("quiet");
        let start = rt::Instant::now();
        assert!(matches!(
            queue.enqueue(start, retry(1, &leaf)),
            EnqueueResult::Accepted { forced: None }
        ));

        rt::sleep(Duration::from_secs(8)).await;
        assert!(matches!(
            queue.enqueue(rt::Instant::now(), retry(1, &leaf)),
            EnqueueResult::Accepted { forced: None }
        ));
        assert!(matches!(
            queue.next_action(
                rt::Instant::now(),
                Duration::from_secs(10),
                Duration::from_secs(25)
            ),
            DriverAction::Wait(Some(deadline)) if deadline == start + Duration::from_secs(18)
        ));

        rt::sleep(Duration::from_secs(8)).await;
        queue.enqueue(rt::Instant::now(), retry(1, &leaf));
        assert!(matches!(
            queue.next_action(
                rt::Instant::now(),
                Duration::from_secs(10),
                Duration::from_secs(25)
            ),
            DriverAction::Wait(Some(deadline)) if deadline == start + Duration::from_secs(25)
        ));

        rt::sleep(Duration::from_secs(9)).await;
        assert!(matches!(
            queue.next_action(
                rt::Instant::now(),
                Duration::from_secs(10),
                Duration::from_secs(25)
            ),
            DriverAction::Drain {
                reason: DrainReason::MaximumAge,
                group,
                ..
            } if group.retries.len() == 1
        ));
    }

    #[test]
    fn capacity_forces_the_oldest_group_and_duplicates_are_free() {
        let mut queue = Queue::new(2, true);
        let now = rt::Instant::now();
        let first = path("first");
        let second = path("second");
        let third = path("third");

        queue.enqueue(now, retry(1, &first));
        assert!(matches!(
            queue.enqueue(now, retry(1, &first)),
            EnqueueResult::Accepted { forced: None }
        ));
        assert_eq!(queue.queued, 1);
        queue.enqueue(now, retry(2, &second));
        let forced = match queue.enqueue(now, retry(3, &third)) {
            EnqueueResult::Accepted {
                forced: Some((_, group)),
            } => group,
            _ => panic!("capacity did not force the oldest group"),
        };

        assert!(
            forced
                .retries
                .contains_key(&TxId::with_priority(1, b"tx-1"))
        );
        assert_eq!(queue.queued, 2);
        assert!(!queue.groups.contains_key(&first));
        assert!(queue.groups.contains_key(&second));
        assert!(queue.groups.contains_key(&third));
    }

    #[test]
    fn close_rejects_new_work_and_forces_pending_groups() {
        let mut queue = Queue::new(2, true);
        let now = rt::Instant::now();
        let first = path("first");
        let second = path("second");
        queue.enqueue(now, retry(1, &first));
        queue.close();

        assert!(matches!(
            queue.enqueue(now, retry(2, &second)),
            EnqueueResult::Rejected(_)
        ));
        assert!(matches!(
            queue.next_action(now, Duration::MAX, Duration::MAX),
            DriverAction::Drain {
                reason: DrainReason::Shutdown,
                ..
            }
        ));
        assert!(matches!(
            queue.next_action(now, Duration::MAX, Duration::MAX),
            DriverAction::Finished
        ));
    }
}
