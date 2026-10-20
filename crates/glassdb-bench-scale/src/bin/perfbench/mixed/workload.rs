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

/// Number of transaction shapes, each with `--workers-per-shape` workers.
pub(super) const SHAPE_COUNT: usize = 4;

const SHAPES: [Shape; SHAPE_COUNT] = [
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
    stripe: Stripe,
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

/// A gate between the transactions of all workers of a cell.
///
/// The warm-up and the measurement share the workers, and a transaction that
/// runs while the measurement starts would count work of the warm-up. Closing
/// the gate lets each worker finish its transaction and wait, so that the
/// measurement starts with no transaction in flight.
#[derive(Default)]
pub(super) struct Pause {
    state: std::sync::Mutex<PauseState>,
    changed: tokio::sync::Notify,
}

#[derive(Default)]
struct PauseState {
    closed: bool,
    waiting: usize,
}

impl Pause {
    /// Closes the gate and waits until `workers` wait at it, or until `stop`
    /// is set.
    pub(super) async fn close_and_wait(&self, workers: usize, stop: &AtomicBool) {
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
    pub(super) fn open(&self) {
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

/// How workers choose keys from the pool.
#[derive(Clone, Copy)]
pub(super) enum KeySelection {
    /// Every transaction draws from the whole pool.
    Shared,
    /// A transaction draws from its worker's stripe, except with probability
    /// `shared_pct` percent, when it draws from the whole pool.
    Stripes { shared_pct: u8 },
}

/// Cell-wide context shared by every worker.
#[derive(Clone)]
pub(super) struct WorkerCtx {
    /// Set by [`drive_to_significance`] to stop all shapes at once.
    stop: Arc<AtomicBool>,
    pause: Arc<Pause>,
    pool_size: usize,
    multi_keys: usize,
    affinity_pct: u8,
    key_selection: KeySelection,
    /// The worker's own stripe. The cell-wide context holds the whole pool.
    stripe: Stripe,
}

impl WorkerCtx {
    /// Creates the shared selection and stopping context for one cell.
    pub(super) fn new(
        stop: Arc<AtomicBool>,
        pool_size: usize,
        multi_keys: usize,
        affinity_pct: u8,
        key_selection: KeySelection,
    ) -> Self {
        Self {
            stop,
            pause: Arc::new(Pause::default()),
            pool_size,
            multi_keys,
            affinity_pct,
            key_selection,
            stripe: Stripe::WHOLE_POOL,
        }
    }

    /// Returns the gate between the transactions of the workers.
    pub(super) fn pause(&self) -> &Pause {
        &self.pause
    }

    fn for_stripe(&self, stripe: Stripe) -> Self {
        Self {
            stripe,
            ..self.clone()
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
        let home = home_collection(spec.database, collections.len());
        let bench = spec.bench.clone();
        let ctx = ctx.for_stripe(spec.stripe);
        handles.push(tokio::spawn(worker(
            db,
            collections,
            home,
            spec.shape,
            bench,
            spec.seed,
            ctx,
        )));
    }
    handles
}

/// Returns the home collection of `database`. Databases share home
/// collections when the cell has fewer collections than Databases.
fn home_collection(database: usize, collections: usize) -> usize {
    database % collections
}

/// The pool indices `index` for which `index % count == offset`.
///
/// Each worker of the cell owns a distinct stripe, so that two transactions
/// conflict only when one of them draws from the whole pool.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Stripe {
    offset: usize,
    count: usize,
}

impl Stripe {
    const WHOLE_POOL: Stripe = Stripe {
        offset: 0,
        count: 1,
    };

    /// Returns the number of pool indices in the stripe.
    fn len(self, pool_size: usize) -> usize {
        pool_size.saturating_sub(self.offset).div_ceil(self.count)
    }

    /// Returns the pool index of the stripe's `position`th member.
    fn index(self, position: usize) -> usize {
        position * self.count + self.offset
    }
}

fn worker_specs(plans: &[ShapePlan]) -> impl Iterator<Item = WorkerSpec<'_>> + '_ {
    // Workers can use any collection, so stripes are distinct across all
    // workers of the cell, not only within one database.
    let workers = plans
        .iter()
        .map(|plan| plan.slots.iter().map(|&(_, count)| count).sum::<usize>())
        .sum::<usize>();
    plans
        .iter()
        .flat_map(|plan| {
            plan.slots.iter().flat_map(move |&(database, count)| {
                std::iter::repeat_n((plan.shape, &plan.bench, database), count)
            })
        })
        .enumerate()
        .scan(
            0x9E37_79B9_7F4A_7C15_u64,
            move |seed, (offset, (shape, bench, database))| {
                *seed = seed.wrapping_add(0x1000_0000_0000_0001);
                let stripe = Stripe {
                    offset,
                    count: workers,
                };
                Some(WorkerSpec {
                    database,
                    shape,
                    bench,
                    seed: *seed,
                    stripe,
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

/// Runs one worker until the cell stops.
///
/// Only the cell stops the workers: if one shape stopped first, the windows
/// would end with an idle tail that lowers the last window.
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
    while !ctx.stop.load(Ordering::Relaxed) {
        ctx.pause.pass(&ctx.stop).await;
        if ctx.stop.load(Ordering::Relaxed) {
            break;
        }
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
    let collection = pick_collection(rng, home, collections, ctx.affinity_pct);
    let stripe = match ctx.key_selection {
        KeySelection::Shared => Stripe::WHOLE_POOL,
        KeySelection::Stripes { shared_pct } => {
            if rng.random_range(0..100u8) < shared_pct {
                Stripe::WHOLE_POOL
            } else {
                ctx.stripe
            }
        }
    };
    (collection, pick_keys(rng, ctx.pool_size, stripe, key_count))
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

/// Picks distinct pool indices of `stripe` in stable ascending order.
///
/// A stripe with fewer members than `count` gives all its members.
fn pick_keys(rng: &mut StdRng, pool_size: usize, stripe: Stripe, count: usize) -> Vec<usize> {
    let members = stripe.len(pool_size);
    let count = count.min(members).max(1);
    if count >= members {
        return (0..members)
            .map(|position| stripe.index(position))
            .collect();
    }
    let mut selected = HashSet::with_capacity(count);
    while selected.len() < count {
        selected.insert(rng.random_range(0..members));
    }
    let mut selected: Vec<_> = selected
        .into_iter()
        .map(|position| stripe.index(position))
        .collect();
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
                let ctx = WorkerCtx::new(
                    Arc::new(AtomicBool::new(false)),
                    17,
                    3,
                    60,
                    KeySelection::Shared,
                );
                let count = spec.shape.key_count(ctx.multi_keys, ctx.pool_size);
                let (collection, keys) = select_inputs(
                    &mut rng,
                    spec.database,
                    2,
                    &ctx.for_stripe(spec.stripe),
                    count,
                );
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

    /// Draws `draws` key sets for every worker of a cell, by shape.
    fn drawn_keys(
        keys: KeySelection,
        pool_size: usize,
        draws: usize,
    ) -> Vec<(Shape, Vec<HashSet<usize>>)> {
        let worker_plans = plans(4, 2, Duration::from_secs(1));
        let ctx = WorkerCtx::new(Arc::new(AtomicBool::new(false)), pool_size, 3, 100, keys);
        worker_specs(&worker_plans)
            .map(|spec| {
                let mut rng = StdRng::seed_from_u64(spec.seed);
                let count = spec.shape.key_count(ctx.multi_keys, ctx.pool_size);
                let sets = (0..draws)
                    .map(|_| {
                        let (_, keys) = select_inputs(
                            &mut rng,
                            spec.database,
                            2,
                            &ctx.for_stripe(spec.stripe),
                            count,
                        );
                        assert_eq!(keys.len(), count);
                        assert!(keys.iter().all(|&key| key < pool_size));
                        keys.into_iter().collect()
                    })
                    .collect();
                (spec.shape, sets)
            })
            .collect()
    }

    #[test]
    fn stripes_keep_workers_apart_unless_shared() {
        let pool_size = 161;
        let keys_by_worker = |shared_pct| -> Vec<HashSet<usize>> {
            drawn_keys(KeySelection::Stripes { shared_pct }, pool_size, 200)
                .into_iter()
                .map(|(_, sets)| sets.into_iter().flatten().collect())
                .collect()
        };

        let striped = keys_by_worker(0);
        assert_eq!(striped.len(), 16);
        for (index, keys) in striped.iter().enumerate() {
            for other in &striped[index + 1..] {
                assert!(keys.is_disjoint(other));
            }
        }
        let covered: HashSet<usize> = striped.iter().flatten().copied().collect();
        assert_eq!(covered.len(), pool_size);

        // Shared transactions reach keys outside the worker's stripe.
        let stripe_len = pool_size.div_ceil(16);
        assert!(
            keys_by_worker(100)
                .iter()
                .all(|keys| keys.len() > stripe_len)
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn closed_pause_waits_for_running_operations_to_end() {
        use std::sync::atomic::AtomicUsize;

        let stop = Arc::new(AtomicBool::new(false));
        let pause = Arc::new(Pause::default());
        let running = Arc::new(AtomicUsize::new(0));
        let workers: Vec<_> = (0..8)
            .map(|_| {
                let (stop, pause, running) = (stop.clone(), pause.clone(), running.clone());
                tokio::spawn(async move {
                    let mut operations = 0;
                    while !stop.load(Ordering::Relaxed) {
                        pause.pass(&stop).await;
                        running.fetch_add(1, Ordering::SeqCst);
                        tokio::time::sleep(Duration::from_millis(3)).await;
                        running.fetch_sub(1, Ordering::SeqCst);
                        operations += 1;
                    }
                    operations
                })
            })
            .collect();

        tokio::time::sleep(Duration::from_millis(20)).await;
        pause.close_and_wait(workers.len(), &stop).await;
        assert_eq!(running.load(Ordering::SeqCst), 0);
        tokio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(running.load(Ordering::SeqCst), 0);

        pause.open();
        tokio::time::sleep(Duration::from_millis(20)).await;
        stop.store(true, Ordering::Relaxed);
        for worker in workers {
            assert!(
                worker.await.unwrap() > 1,
                "workers continue after the pause"
            );
        }
    }

    #[test]
    fn databases_share_home_collections_when_collections_are_fewer() {
        let homes = |databases, collections| -> Vec<usize> {
            (0..databases)
                .map(|database| home_collection(database, collections))
                .collect()
        };
        assert_eq!(homes(3, 3), [0, 1, 2]);
        assert_eq!(homes(5, 2), [0, 1, 0, 1, 0]);
        assert_eq!(homes(5, 1), [0, 0, 0, 0, 0]);
    }

    #[test]
    fn small_stripes_give_all_their_members() {
        let stripe = Stripe {
            offset: 2,
            count: 4,
        };
        assert_eq!(stripe.len(11), 3);
        let mut rng = StdRng::seed_from_u64(3);
        assert_eq!(pick_keys(&mut rng, 11, stripe, 5), vec![2, 6, 10]);
        assert_eq!(pick_keys(&mut rng, 11, stripe, 3), vec![2, 6, 10]);
        let keys = pick_keys(&mut rng, 11, stripe, 2);
        assert_eq!(keys.len(), 2);
        assert!(keys.iter().all(|key| key % 4 == 2));
    }
}
