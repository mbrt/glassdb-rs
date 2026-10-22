//! The workers of a cell: the transactions of each shape, the gate between
//! the warm-up and the measurement, and the end of the measurement.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use glassdb::{Collection, Database, Error as GError, Stats};
use glassdb_bench_scale::backend_breakdown::BackendBreakdownHandle;
use glassdb_bench_scale::bench::Bench;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tokio::task::JoinHandle;

use super::Window;
use super::kinds::{Kind, Op};
use super::model::{DatabaseOptions, PoolSpec};
use super::seed::{open_collections_of, shutdown};
use super::sources::WorkerKeys;
use super::until::Until;

/// The state that all workers of a cell share.
pub(crate) struct Shared {
    /// Stops all workers at once.
    pub(crate) stop: AtomicBool,
    /// Set by a worker that stops with an error.
    pub(crate) failed: AtomicBool,
    /// Whether the transactions that start now are in the measurement.
    pub(crate) recording: AtomicBool,
    pub(crate) pause: Pause,
    /// The workers that end the measurement by their passes, and have not
    /// ended it yet.
    pub(crate) passes_left: AtomicUsize,
    /// Times the warm-up transactions, which are not reported. A bench that
    /// never starts records nothing, but retries in-doubt outcomes like the
    /// measurement does.
    pub(crate) warm_bench: Bench,
    pub(crate) affinity: u8,
    pub(crate) until: Until,
    /// The storage counters, for the start of a pass.
    pub(crate) breakdown: BackendBreakdownHandle,
    /// The start of the pass that ended the measurement, when the end
    /// condition leaves that pass out.
    pub(crate) boundary: Mutex<Option<Boundary>>,
}

/// The state at the start of the pass that ended the measurement.
#[derive(Clone, Copy)]
pub(crate) struct Boundary {
    /// The samples of the shape of the worker before the pass.
    pub(crate) samples: usize,
    pub(crate) window: Window,
    pub(crate) wall: Instant,
    pub(crate) model: glassdb_concurr::rt::Instant,
}

/// One shape of one group, as its workers run it.
pub(crate) struct ShapeRun {
    pub(crate) name: String,
    pub(crate) kind: Arc<dyn Kind>,
    pub(crate) pool: PoolSpec,
    pub(crate) bench: Arc<Bench>,
}

/// The windows of the database instances that workers open for single
/// transactions, which no group instance counts.
#[derive(Default)]
pub(crate) struct OwnInstances {
    pub(crate) workload: Mutex<Window>,
    pub(crate) shutdown: Mutex<Window>,
    /// From before each open to after each shutdown, so that the opens can be
    /// taken out of the windows of the storage.
    pub(crate) lifecycle: Mutex<Window>,
}

/// Opens new database instances over the storage of a cell.
#[derive(Clone)]
pub(crate) struct Opener {
    pub(crate) backend: Arc<dyn glassdb::Backend>,
    pub(crate) breakdown: BackendBreakdownHandle,
    pub(crate) database: String,
    pub(crate) options: DatabaseOptions,
    pub(crate) pool: String,
    pub(crate) collections: usize,
    pub(crate) drain_timeout: Duration,
}

/// Where a worker runs its transactions.
pub(crate) enum Target {
    /// An instance of its group, with its collections of the pool.
    Instance {
        db: Database,
        collections: Arc<[Collection]>,
    },
    /// A new instance for each transaction.
    Fresh {
        opener: Opener,
        own: Arc<OwnInstances>,
    },
}

/// One worker.
pub(crate) struct Worker {
    pub(crate) shared: Arc<Shared>,
    pub(crate) shape: Arc<ShapeRun>,
    pub(crate) target: Target,
    pub(crate) keys: WorkerKeys,
    pub(crate) home: usize,
    pub(crate) collections: usize,
    pub(crate) seed: u64,
}

/// A gate between the transactions of all workers of a cell.
///
/// The warm-up and the measurement share the workers, and a transaction that
/// runs while the measurement starts would count work of the warm-up. Closing
/// the gate lets each worker finish its transaction and wait, so that the
/// measurement starts with no transaction in flight.
#[derive(Default)]
pub(crate) struct Pause {
    state: Mutex<PauseState>,
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct PauseState {
    closed: bool,
    waiting: usize,
}

/// How the measurement ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Outcome {
    /// The end condition was met.
    Met,
    /// The maximum duration came first.
    Capped,
    /// A worker stopped with an error.
    WorkerStopped,
}

impl Pause {
    /// Returns a gate that is closed until the first [`Pause::open`], so that
    /// no worker starts a transaction before the warm-up or the measurement.
    pub(crate) fn closed() -> Self {
        let pause = Self::default();
        pause.state.lock().unwrap().closed = true;
        pause
    }

    /// Closes the gate and waits until `workers` wait at it, or until `stop`
    /// is set.
    pub(crate) async fn close_and_wait(&self, workers: usize, stop: &AtomicBool) {
        self.state.lock().unwrap().closed = true;
        loop {
            let changed = self.changed.notified();
            if self.state.lock().unwrap().waiting >= workers || stop.load(Ordering::Relaxed) {
                return;
            }
            // A worker that stops with an error sets `stop` and never waits.
            tokio::select! {
                () = changed => {}
                () = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
    }

    /// Opens the gate, so that the waiting workers continue.
    pub(crate) fn open(&self) {
        self.state.lock().unwrap().closed = false;
        self.changed.notify_waiters();
    }

    /// Waits while the gate is closed, unless `stop` is set.
    async fn pass(&self, stop: &AtomicBool) {
        let mut counted = false;
        loop {
            let changed = self.changed.notified();
            {
                let mut state = self.state.lock().unwrap();
                if !state.closed || stop.load(Ordering::Relaxed) {
                    if counted {
                        state.waiting -= 1;
                    }
                    return;
                }
                if !counted {
                    state.waiting += 1;
                    counted = true;
                    self.changed.notify_waiters();
                }
            }
            tokio::select! {
                () = changed => {}
                () = tokio::time::sleep(Duration::from_millis(20)) => {}
            }
        }
    }
}

impl Worker {
    /// Spawns the worker on the current runtime.
    pub(crate) fn spawn(self) -> JoinHandle<Result<(), GError>> {
        tokio::spawn(async move {
            let shared = self.shared.clone();
            let result = self.run().await;
            if result.is_err() {
                shared.failed.store(true, Ordering::Relaxed);
                shared.stop.store(true, Ordering::Relaxed);
            }
            result
        })
    }

    /// Runs transactions until the cell stops, or until the passes of the
    /// worker end the measurement.
    ///
    /// Only the cell stops the other workers: if one shape stopped first, the
    /// windows would end with an idle tail that lowers the last window.
    async fn run(self) -> Result<(), GError> {
        let shared = &self.shared;
        let mut rng = StdRng::seed_from_u64(self.seed);
        let kind = &self.shape.kind;
        let repeats = kind.repeats().max(1);
        let pass_len = self.keys.pass_len().filter(|_| shared.until.by_passes());
        let mut round = 0u64;
        let mut measured = 0u64;
        let mut pass_start: Option<(Stats, Boundary)> = None;
        let mut collection = self.home;
        let mut keys: Vec<Vec<u8>> = Vec::new();
        while !shared.stop.load(Ordering::Relaxed) {
            shared.pause.pass(&shared.stop).await;
            if shared.stop.load(Ordering::Relaxed) {
                break;
            }
            // Select the inputs outside the measurement, so that their work does
            // not count in the latency.
            if round.is_multiple_of(repeats) {
                collection =
                    pick_collection(&mut rng, self.home, self.collections, shared.affinity);
                keys = self
                    .keys
                    .select(&mut rng, round)
                    .into_iter()
                    .map(|index| self.shape.pool.key(index))
                    .collect();
            }
            let recording = shared.recording.load(Ordering::Relaxed);
            let bench: &Bench = if recording {
                &self.shape.bench
            } else {
                &shared.warm_bench
            };
            if recording
                && pass_len.is_some()
                && pass_start.is_none()
                && let Some(stats) = self.instance_stats()
            {
                let boundary = Boundary {
                    samples: self.shape.bench.sample_count(),
                    window: Window {
                        stats,
                        breakdown: shared.breakdown.snapshot(),
                    },
                    wall: Instant::now(),
                    model: glassdb_concurr::rt::Instant::now(),
                };
                pass_start = Some((stats, boundary));
            }
            self.once(bench, collection, &keys, round, recording)
                .await?;
            round += 1;
            let Some(pass_len) = pass_len.filter(|_| recording) else {
                continue;
            };
            measured += 1;
            if !measured.is_multiple_of(pass_len) {
                continue;
            }
            let start = pass_start.take();
            let pass = match (self.instance_stats(), start) {
                (Some(end), Some((start, _))) => end - start,
                _ => Stats::default(),
            };
            let ended = match &shared.until {
                Until::Passes => true,
                Until::Pass(condition) => condition.ends(&pass),
                Until::Converged | Until::Duration => false,
            };
            if ended {
                if shared.until.excludes_last_pass() {
                    *shared.boundary.lock().unwrap() = start.map(|(_, boundary)| boundary);
                }
                shared.passes_left.fetch_sub(1, Ordering::Relaxed);
                break;
            }
        }
        Ok(())
    }

    async fn once(
        &self,
        bench: &Bench,
        collection: usize,
        keys: &[Vec<u8>],
        round: u64,
        recording: bool,
    ) -> Result<(), GError> {
        match &self.target {
            Target::Instance { db, collections } => {
                let selected =
                    select_collections(collections, collection, self.shape.kind.collections());
                let op = Op {
                    db,
                    collections: &selected,
                    keys,
                    pool: &self.shape.pool,
                    round,
                };
                bench.measure(|| self.shape.kind.run(&op)).await
            }
            Target::Fresh { opener, own } => {
                let start = opener.breakdown.snapshot();
                let db = opener
                    .options
                    .apply(Database::builder(&opener.database, opener.backend.clone()))
                    .map_err(|e| GError::internal(e.to_string()))?
                    .open()
                    .await?;
                let collections =
                    open_collections_of(&db, &opener.pool, opener.collections).await?;
                let selected =
                    select_collections(&collections, collection, self.shape.kind.collections());
                let op = Op {
                    db: &db,
                    collections: &selected,
                    keys,
                    pool: &self.shape.pool,
                    round,
                };
                let before = Window::now(&db, &opener.breakdown);
                let result = bench.measure(|| self.shape.kind.run(&op)).await;
                let after = Window::now(&db, &opener.breakdown);
                drop(selected);
                drop(collections);
                shutdown(&db, opener.drain_timeout).await?;
                if recording {
                    let end = Window::now(&db, &opener.breakdown);
                    own.workload.lock().unwrap().add(after.since(&before));
                    own.shutdown.lock().unwrap().add(end.since(&after));
                    own.lifecycle.lock().unwrap().add(Window {
                        stats: end.stats,
                        breakdown: end.breakdown - start,
                    });
                }
                result
            }
        }
    }

    fn instance_stats(&self) -> Option<Stats> {
        match &self.target {
            Target::Instance { db, .. } => Some(db.stats()),
            Target::Fresh { .. } => None,
        }
    }
}

/// Runs the measurement until its end condition, or until `max`.
pub(crate) async fn drive(
    shared: &Shared,
    benches: &[Arc<Bench>],
    target: u64,
    min: Duration,
    max: Duration,
) -> Outcome {
    let started = Instant::now();
    // Poll often enough to react promptly, coarsely enough to stay negligible.
    let step = (max / 40).clamp(Duration::from_millis(20), Duration::from_millis(250));
    loop {
        tokio::time::sleep(step).await;
        if shared.failed.load(Ordering::Relaxed) {
            return Outcome::WorkerStopped;
        }
        let elapsed = started.elapsed();
        let met = match shared.until {
            Until::Converged => {
                elapsed >= min
                    && (target == 0
                        || benches
                            .iter()
                            .all(|bench| bench.sample_count() as u64 >= target))
            }
            Until::Duration => elapsed >= min,
            Until::Passes | Until::Pass(_) => shared.passes_left.load(Ordering::Relaxed) == 0,
        };
        if met {
            return Outcome::Met;
        }
        if elapsed >= max {
            return Outcome::Capped;
        }
    }
}

/// Distributes workers across instances evenly, omitting empty slots.
pub(crate) fn split_workers(workers: usize, instances: usize) -> Vec<usize> {
    let instances = instances.max(1).min(workers.max(1));
    let base = workers / instances;
    let remainder = workers % instances;
    (0..instances)
        .map(|index| base + usize::from(index < remainder))
        .collect()
}

/// Returns the random seed of the `index`th worker of a cell.
pub(crate) fn worker_seed(index: usize) -> u64 {
    0x9E37_79B9_7F4A_7C15_u64.wrapping_add((index as u64 + 1).wrapping_mul(0x1000_0000_0000_0001))
}

/// Selects the home collection with the configured affinity.
fn pick_collection(rng: &mut StdRng, home: usize, collections: usize, affinity: u8) -> usize {
    // Draw both at every affinity, so that cells with other affinities keep
    // the same later draws.
    let choose_home = rng.random_range(0..100u8) < affinity;
    let uniform = rng.random_range(0..collections.max(1));
    if choose_home { home } else { uniform }
}

/// Returns `count` collections from `first` on, wrapping around.
fn select_collections(collections: &[Collection], first: usize, count: usize) -> Vec<Collection> {
    (0..count.max(1))
        .map(|offset| collections[(first + offset) % collections.len()].clone())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn worker_distribution_keeps_every_open_instance_active() {
        let cases = [
            (1, 5, vec![1]),
            (5, 5, vec![1, 1, 1, 1, 1]),
            (8, 5, vec![2, 2, 2, 1, 1]),
            (10, 5, vec![2, 2, 2, 2, 2]),
        ];
        for (workers, instances, expected) in cases {
            assert_eq!(split_workers(workers, instances), expected);
        }
    }
}
