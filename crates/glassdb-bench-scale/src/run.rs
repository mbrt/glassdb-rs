//! Shared lifecycle helpers for the scale benchmark binaries.

use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant as WallInstant};

use futures::stream::{FuturesUnordered, StreamExt};
use glassdb::{Database, Error};
use tokio::task::JoinHandle;
use tokio::time::Instant;

use crate::bench::Bench;

/// The completed-split count once setup splits became quiet, and the wall time
/// needed to get there.
#[derive(Clone, Copy)]
pub struct SplitSettlement {
    pub completed: u64,
    pub elapsed: Duration,
}

/// Waits until the completed-split counter of `database` stays unchanged for
/// `quiet` of wall time.
pub async fn wait_for_split_quiet(
    database: &Database,
    quiet: Duration,
    timeout: Duration,
) -> Result<SplitSettlement, Error> {
    let started = WallInstant::now();
    let mut tracker = SplitQuietTracker::new(database.stats().restructurer.splits, started);
    let poll = (quiet / 4).clamp(Duration::from_millis(20), Duration::from_millis(250));
    loop {
        let now = WallInstant::now();
        tracker.observe(database.stats().restructurer.splits, now);
        if tracker.is_quiet(now, quiet) {
            return Ok(SplitSettlement {
                completed: tracker.completed,
                elapsed: started.elapsed(),
            });
        }
        if deadline_expired(started, WallInstant::now(), timeout) {
            return Err(Error::internal(format!(
                "setup splits did not stay quiet for {quiet:?} within {timeout:?} \
                 (completed={})",
                tracker.completed
            )));
        }
        tokio::time::sleep(poll).await;
    }
}

/// Distributes workers across databases evenly, omitting empty slots.
pub fn split_workers(workers: usize, databases: usize) -> Vec<usize> {
    let databases = databases.max(1).min(workers.max(1));
    let base = workers / databases;
    let remainder = workers % databases;
    (0..databases)
        .map(|index| base + usize::from(index < remainder))
        .filter(|&count| count > 0)
        .collect()
}

/// Outcome of driving a cell toward its sampling target.
pub enum DriveOutcome {
    Converged,
    Capped,
    WorkerStopped,
}

/// Runs a cell until every bench has `target` samples after at least
/// `min_dur`, or until `max_dur`. Sets `stop` when the cell must end. A `stop`
/// set by a worker means that worker failed.
pub async fn drive_to_significance(
    benches: &[&Bench],
    stop: &AtomicBool,
    target: u64,
    min_dur: Duration,
    max_dur: Duration,
) -> DriveOutcome {
    let started = WallInstant::now();
    // Poll often enough to react promptly, coarsely enough to stay negligible.
    let step = (max_dur / 40).clamp(Duration::from_millis(20), Duration::from_millis(250));
    loop {
        tokio::time::sleep(step).await;
        if stop.load(Ordering::Relaxed) {
            return DriveOutcome::WorkerStopped;
        }
        let elapsed = started.elapsed();
        let ready = elapsed >= min_dur
            && (target == 0
                || benches
                    .iter()
                    .all(|bench| bench.sample_count() as u64 >= target));
        if ready {
            stop.store(true, Ordering::Relaxed);
            return DriveOutcome::Converged;
        }
        if elapsed >= max_dur {
            stop.store(true, Ordering::Relaxed);
            return DriveOutcome::Capped;
        }
    }
}

/// Waits for benchmark workers until `deadline`.
///
/// The first worker error or panic aborts its peers. Expiry also aborts every
/// worker, so dropping a benchmark cell cannot leave detached transactions
/// changing the backend or holding `Database` clones alive.
pub async fn join_tasks_until(
    handles: Vec<JoinHandle<Result<(), Error>>>,
    deadline: Instant,
) -> Result<(), Error> {
    let mut pending: FuturesUnordered<_> = handles.into_iter().collect();
    while !pending.is_empty() {
        let joined = match tokio::time::timeout_at(deadline, pending.next()).await {
            Ok(Some(joined)) => joined,
            Ok(None) => return Ok(()),
            Err(_) => {
                abort_all(&mut pending).await;
                return Err(Error::internal(
                    "benchmark workers exceeded the cell completion deadline",
                ));
            }
        };
        match joined {
            Ok(Ok(())) => {}
            Ok(Err(err)) => {
                abort_all(&mut pending).await;
                return Err(err);
            }
            Err(err) => {
                abort_all(&mut pending).await;
                return Err(Error::with_source("benchmark worker task failed", err));
            }
        }
    }
    Ok(())
}

/// Gracefully closes all databases without extending the cell deadline.
pub async fn shutdown_databases_until(
    databases: &[Database],
    deadline: Instant,
) -> Result<(), Error> {
    let shutdowns = futures::future::join_all(databases.iter().map(Database::shutdown));
    tokio::time::timeout_at(deadline, shutdowns)
        .await
        .map(|_| ())
        .map_err(|_| Error::internal("database shutdown exceeded the cell completion deadline"))
}

async fn abort_all(pending: &mut FuturesUnordered<JoinHandle<Result<(), Error>>>) {
    for handle in pending.iter() {
        handle.abort();
    }
    while pending.next().await.is_some() {}
}

struct SplitQuietTracker {
    completed: u64,
    unchanged_since: WallInstant,
}

impl SplitQuietTracker {
    fn new(completed: u64, now: WallInstant) -> Self {
        Self {
            completed,
            unchanged_since: now,
        }
    }

    fn observe(&mut self, completed: u64, now: WallInstant) {
        if completed != self.completed {
            self.completed = completed;
            self.unchanged_since = now;
        }
    }

    fn is_quiet(&self, now: WallInstant, quiet: Duration) -> bool {
        now.duration_since(self.unchanged_since) >= quiet
    }
}

fn deadline_expired(started: WallInstant, now: WallInstant, timeout: Duration) -> bool {
    now.duration_since(started) >= timeout
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::*;

    #[test]
    fn worker_distribution_keeps_every_open_database_active() {
        let cases = [
            (1, 5, vec![1]),
            (5, 5, vec![1, 1, 1, 1, 1]),
            (8, 5, vec![2, 2, 2, 1, 1]),
            (10, 5, vec![2, 2, 2, 2, 2]),
        ];
        for (workers, databases, expected) in cases {
            let distribution = split_workers(workers, databases);
            assert_eq!(distribution, expected);
            assert_eq!(distribution.iter().sum::<usize>(), workers);
            assert!(distribution.iter().all(|&count| count > 0));
        }
    }

    #[test]
    fn quiet_period_resets_and_deadline_is_inclusive() {
        let start = WallInstant::now();
        let quiet = Duration::from_secs(2);
        let mut tracker = SplitQuietTracker::new(3, start);

        assert!(!tracker.is_quiet(start + Duration::from_secs(1), quiet));
        tracker.observe(4, start + Duration::from_millis(1500));
        assert!(!tracker.is_quiet(start + Duration::from_secs(3), quiet));
        tracker.observe(4, start + Duration::from_millis(3200));
        assert!(tracker.is_quiet(start + Duration::from_millis(3500), quiet));
        assert!(!deadline_expired(
            start,
            start + Duration::from_millis(2999),
            Duration::from_secs(3)
        ));
        assert!(deadline_expired(
            start,
            start + Duration::from_secs(3),
            Duration::from_secs(3)
        ));
    }

    #[tokio::test]
    async fn worker_error_aborts_its_peers() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let marker = Dropped(dropped.clone());
        let pending = tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<Result<(), Error>>().await
        });
        let failed = tokio::spawn(async { Err(Error::NotFound) });

        let err = join_tasks_until(
            vec![pending, failed],
            Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap_err();

        assert!(matches!(err, Error::NotFound));
        assert!(dropped.load(Ordering::Relaxed));
    }

    #[tokio::test]
    async fn deadline_aborts_pending_workers() {
        struct Dropped(Arc<AtomicBool>);
        impl Drop for Dropped {
            fn drop(&mut self) {
                self.0.store(true, Ordering::Relaxed);
            }
        }

        let dropped = Arc::new(AtomicBool::new(false));
        let marker = Dropped(dropped.clone());
        let pending = tokio::spawn(async move {
            let _marker = marker;
            std::future::pending::<Result<(), Error>>().await
        });

        let err = join_tasks_until(vec![pending], Instant::now())
            .await
            .unwrap_err();

        assert!(matches!(err, Error::Internal { .. }));
        assert!(dropped.load(Ordering::Relaxed));
    }
}
