//! Mixed-workload worker planning, selection, and execution.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use futures::future::join_all;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tokio::task::JoinHandle;

use glassdb::{Collection, Database, Error as GError};
use glassdb_bench_scale::bench::Bench;

use super::result::ShapeMeasurement;
use super::{key_bytes, value};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    RwSingle,
    RwMany,
    RoSingle,
    RoMulti,
}

const SHAPES: [Shape; 4] = [
    Shape::RwSingle,
    Shape::RwMany,
    Shape::RoSingle,
    Shape::RoMulti,
];

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::RwSingle => "rwSingle",
            Shape::RwMany => "rwMany",
            Shape::RoSingle => "roSingle",
            Shape::RoMulti => "roMulti",
        }
    }

    fn is_write(self) -> bool {
        matches!(self, Shape::RwSingle | Shape::RwMany)
    }

    fn key_count(self, multi_keys: usize, pool_size: usize) -> usize {
        match self {
            Shape::RwMany | Shape::RoMulti => multi_keys.min(pool_size).max(1),
            Shape::RwSingle | Shape::RoSingle => 1,
        }
    }
}

/// One shape's timer and its workers per database.
pub(super) struct ShapePlan {
    shape: Shape,
    bench: Arc<Bench>,
    /// Indices into the cell's database vector, with the worker count for each.
    slots: Vec<(usize, usize)>,
}

#[derive(Clone, Copy)]
struct WorkerSpec<'a> {
    database: usize,
    shape: Shape,
    bench: &'a Arc<Bench>,
    seed: u64,
}

/// Builds all worker plans in their stable shape, database, and worker order.
pub(super) fn plans(
    workers_per_shape: usize,
    databases: usize,
    max_duration: Duration,
) -> Vec<ShapePlan> {
    let slots: Vec<(usize, usize)> = split_workers(workers_per_shape, databases)
        .into_iter()
        .enumerate()
        .collect();
    SHAPES
        .into_iter()
        .map(|shape| ShapePlan {
            shape,
            bench: Arc::new(Bench::new(max_duration)),
            slots: slots.clone(),
        })
        .collect()
}

/// Starts every shape's measurement timer.
pub(super) fn start_measurement(plans: &[ShapePlan]) {
    for plan in plans {
        plan.bench.start();
    }
}

/// Ends every shape's measurement timer.
pub(super) fn end_measurement(plans: &[ShapePlan]) {
    for plan in plans {
        plan.bench.end();
    }
}

/// Returns each shape's logical samples in stable reporting order.
pub(super) fn measurements(plans: &[ShapePlan]) -> impl Iterator<Item = ShapeMeasurement> + '_ {
    plans
        .iter()
        .map(|plan| ShapeMeasurement::new(plan.shape.name(), plan.bench.results()))
}

/// Cell-wide context shared by every worker.
#[derive(Clone)]
pub(super) struct WorkerCtx {
    /// Set by [`drive_to_significance`] to stop all shapes at once.
    stop: Arc<AtomicBool>,
    pool_size: usize,
    multi_keys: usize,
    affinity_pct: u8,
}

impl WorkerCtx {
    /// Creates the shared selection and stopping context for one cell.
    pub(super) fn new(
        stop: Arc<AtomicBool>,
        pool_size: usize,
        multi_keys: usize,
        affinity_pct: u8,
    ) -> Self {
        Self {
            stop,
            pool_size,
            multi_keys,
            affinity_pct,
        }
    }
}

/// Spawns every shape's workers across the cell's databases.
pub(super) fn spawn_workers(
    dbs: &[Database],
    collections: &[Arc<[Collection]>],
    plans: &[ShapePlan],
    ctx: &WorkerCtx,
) -> Vec<JoinHandle<Result<(), GError>>> {
    let mut handles: Vec<JoinHandle<Result<(), GError>>> = Vec::new();
    for spec in worker_specs(plans) {
        let db = dbs[spec.database].clone();
        let collections = collections[spec.database].clone();
        let bench = spec.bench.clone();
        let ctx = ctx.clone();
        handles.push(tokio::spawn(worker(
            db,
            collections,
            spec.database,
            spec.shape,
            bench,
            spec.seed,
            ctx,
        )));
    }
    handles
}

fn worker_specs(plans: &[ShapePlan]) -> impl Iterator<Item = WorkerSpec<'_>> + '_ {
    plans
        .iter()
        .flat_map(|plan| {
            plan.slots.iter().flat_map(move |&(database, count)| {
                std::iter::repeat_n((plan.shape, &plan.bench, database), count)
            })
        })
        .scan(
            0x9E37_79B9_7F4A_7C15_u64,
            |seed, (shape, bench, database)| {
                *seed = seed.wrapping_add(0x1000_0000_0000_0001);
                Some(WorkerSpec {
                    database,
                    shape,
                    bench,
                    seed: *seed,
                })
            },
        )
}

/// Outcome of driving a cell toward its sampling target.
pub(super) enum DriveOutcome {
    Converged,
    Capped,
    WorkerStopped,
}

/// Runs the cell until every shape is significant or its deadline is reached.
pub(super) async fn drive_to_significance(
    plans: &[ShapePlan],
    stop: &Arc<AtomicBool>,
    target: u64,
    min_dur: Duration,
    max_dur: Duration,
) -> DriveOutcome {
    let started = Instant::now();
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
                || plans
                    .iter()
                    .all(|plan| plan.bench.sample_count() as u64 >= target));
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

/// Distributes workers across databases evenly, omitting empty slots.
fn split_workers(workers: usize, databases: usize) -> Vec<usize> {
    let databases = databases.max(1).min(workers.max(1));
    let base = workers / databases;
    let remainder = workers % databases;
    (0..databases)
        .map(|index| base + usize::from(index < remainder))
        .filter(|&count| count > 0)
        .collect()
}

/// Runs one worker until the cell or its shape reaches its stopping condition.
async fn worker(
    db: Database,
    collections: Arc<[Collection]>,
    home: usize,
    shape: Shape,
    bench: Arc<Bench>,
    seed: u64,
    ctx: WorkerCtx,
) -> Result<(), GError> {
    let mut rng = StdRng::seed_from_u64(seed);
    let key_count = shape.key_count(ctx.multi_keys, ctx.pool_size);
    while !ctx.stop.load(Ordering::Relaxed) && !bench.is_finished() {
        let result = execute_once(
            &db,
            &collections,
            home,
            shape,
            &bench,
            &mut rng,
            &ctx,
            key_count,
        )
        .await;
        if let Err(err) = result {
            ctx.stop.store(true, Ordering::Relaxed);
            return Err(err);
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn execute_once(
    db: &Database,
    collections: &[Collection],
    home: usize,
    shape: Shape,
    bench: &Bench,
    rng: &mut StdRng,
    ctx: &WorkerCtx,
    key_count: usize,
) -> Result<(), GError> {
    // Select inputs outside measurement so entropy work and borrows do not span
    // the transaction future.
    let (collection, indices) = select_inputs(rng, home, collections.len(), ctx, key_count);
    let keys: Vec<Vec<u8>> = indices.iter().map(|&index| key_bytes(index)).collect();
    let keys = &keys;
    let collection = &collections[collection];
    bench
        .measure(|| async {
            if shape.is_write() {
                rmw_tx(db, collection, keys).await
            } else {
                ro_tx(db, collection, keys).await
            }
        })
        .await
}

fn select_inputs(
    rng: &mut StdRng,
    home: usize,
    collections: usize,
    ctx: &WorkerCtx,
    key_count: usize,
) -> (usize, Vec<usize>) {
    (
        pick_collection(rng, home, collections, ctx.affinity_pct),
        pick_keys(rng, ctx.pool_size, key_count),
    )
}

/// Selects the home collection with the configured affinity.
fn pick_collection(rng: &mut StdRng, home: usize, collections: usize, affinity_pct: u8) -> usize {
    debug_assert!(home < collections);
    // Consume both draws at every affinity so paired sweep cells retain the
    // same subsequent key-selection stream.
    let choose_home = rng.random_range(0..100u8) < affinity_pct;
    let uniform = rng.random_range(0..collections);
    if choose_home { home } else { uniform }
}

/// Picks distinct pool indices in stable ascending order.
fn pick_keys(rng: &mut StdRng, pool_size: usize, count: usize) -> Vec<usize> {
    let count = count.min(pool_size).max(1);
    if count >= pool_size {
        return (0..pool_size).collect();
    }
    let mut selected = HashSet::with_capacity(count);
    while selected.len() < count {
        selected.insert(rng.random_range(0..pool_size));
    }
    let mut selected: Vec<_> = selected.into_iter().collect();
    selected.sort_unstable();
    selected
}

async fn rmw_tx(db: &Database, collection: &Collection, keys: &[Vec<u8>]) -> Result<(), GError> {
    db.tx(|tx| async move {
        let values = join_all(keys.iter().map(|key| tx.read(collection, key))).await;
        for (key, read) in keys.iter().zip(values) {
            match read {
                Ok(_) => {}
                Err(err) => return Err(err),
            }
            tx.write(collection, key, &value())?;
        }
        Ok(())
    })
    .await
}

async fn ro_tx(db: &Database, collection: &Collection, keys: &[Vec<u8>]) -> Result<(), GError> {
    db.tx(|tx| async move {
        let values = join_all(keys.iter().map(|key| tx.read(collection, key))).await;
        for value in values {
            match value {
                Ok(_) => {}
                Err(err) => return Err(err),
            }
        }
        Ok(())
    })
    .await
}

#[cfg(test)]
mod tests {
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

    #[tokio::test]
    async fn seeded_selection_and_logical_counts_are_stable() {
        let worker_plans = plans(3, 2, Duration::from_secs(1));
        let selected: Vec<_> = worker_specs(&worker_plans)
            .map(|spec| {
                let mut rng = StdRng::seed_from_u64(spec.seed);
                let ctx = WorkerCtx::new(Arc::new(AtomicBool::new(false)), 17, 3, 60);
                let count = spec.shape.key_count(ctx.multi_keys, ctx.pool_size);
                let (collection, keys) = select_inputs(&mut rng, spec.database, 2, &ctx, count);
                (
                    spec.shape.name(),
                    spec.shape.is_write(),
                    spec.database,
                    spec.seed,
                    collection,
                    keys,
                )
            })
            .collect();
        assert_eq!(
            selected,
            vec![
                ("rwSingle", true, 0, 0xAE37_79B9_7F4A_7C16, 0, vec![10]),
                ("rwSingle", true, 0, 0xBE37_79B9_7F4A_7C17, 0, vec![8]),
                ("rwSingle", true, 1, 0xCE37_79B9_7F4A_7C18, 1, vec![11]),
                ("rwMany", true, 0, 0xDE37_79B9_7F4A_7C19, 0, vec![7, 9, 15]),
                ("rwMany", true, 0, 0xEE37_79B9_7F4A_7C1A, 0, vec![1, 9, 11]),
                ("rwMany", true, 1, 0xFE37_79B9_7F4A_7C1B, 1, vec![1, 10, 13]),
                ("roSingle", false, 0, 0x0E37_79B9_7F4A_7C1C, 1, vec![5]),
                ("roSingle", false, 0, 0x1E37_79B9_7F4A_7C1D, 0, vec![10]),
                ("roSingle", false, 1, 0x2E37_79B9_7F4A_7C1E, 1, vec![7]),
                (
                    "roMulti",
                    false,
                    0,
                    0x3E37_79B9_7F4A_7C1F,
                    1,
                    vec![4, 9, 12]
                ),
                ("roMulti", false, 0, 0x4E37_79B9_7F4A_7C20, 0, vec![1, 6, 9]),
                (
                    "roMulti",
                    false,
                    1,
                    0x5E37_79B9_7F4A_7C21,
                    0,
                    vec![7, 10, 14]
                ),
            ]
        );

        let plans = plans(1, 1, Duration::from_secs(1));
        start_measurement(&plans);
        for plan in &plans {
            plan.bench.measure(|| async { Ok(()) }).await.unwrap();
        }
        end_measurement(&plans);
        let counts: Vec<_> = plans
            .iter()
            .map(|plan| (plan.shape.name(), plan.bench.sample_count()))
            .collect();
        assert_eq!(
            counts,
            vec![
                ("rwSingle", 1),
                ("rwMany", 1),
                ("roSingle", 1),
                ("roMulti", 1),
            ]
        );

        let mut isolated = StdRng::seed_from_u64(7);
        assert!((0..100).all(|_| pick_collection(&mut isolated, 2, 4, 100) == 2));

        let mut uniform = StdRng::seed_from_u64(7);
        let selected: HashSet<_> = (0..100)
            .map(|_| pick_collection(&mut uniform, 2, 4, 0))
            .collect();
        assert_eq!(selected, HashSet::from([0, 1, 2, 3]));

        let mut no_affinity = StdRng::seed_from_u64(11);
        let mut full_affinity = StdRng::seed_from_u64(11);
        pick_collection(&mut no_affinity, 2, 4, 0);
        pick_collection(&mut full_affinity, 2, 4, 100);
        assert_eq!(no_affinity.random::<u64>(), full_affinity.random::<u64>());
    }
}
