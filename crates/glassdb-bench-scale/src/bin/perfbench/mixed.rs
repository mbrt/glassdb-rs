//! Mixed-workload contention benchmark.
//!
//! Runs all four transaction shapes (`rwSingle`, `rwMany`, `roSingle`,
//! `roMulti`) concurrently over one selected storage backend, sweeping:
//!
//! - contention **mode** (`lo` = keys drawn from a large pool so overlaps are
//!   rare; `hi` = keys drawn from a small hot pool so they collide on the same
//!   leaves), and
//! - home-collection **affinity** (`0` = each `Database` chooses uniformly
//!   among every collection; `100` = each uses only its own collection).
//!
//! Every `Database` runs every shape and has one distinct home collection. An
//! intermediate affinity mixes home traffic with uniformly selected
//! collections, exposing cross-client overlap without hard-coding a small set
//! of discrete client-to-collection layouts. All clients wrap the same backend.
//!
//! The key set is seeded before the measurement clients open. The seeding
//! database then waits until its completed-split counter has not changed for
//! `--split-quiet`; a timeout fails the cell. Setup, structural convergence,
//! collection opening, and their backend operations are outside the measured
//! interval.
//!
//! Each cell uses **sequential (adaptive) sampling**: all shapes run
//! concurrently until every shape has committed enough transactions for its
//! throughput 95% confidence interval to reach `--target-ci` (or `--max-duration`
//! caps the run). Heavily-contended write shapes therefore run longer to earn
//! significance instead of returning a noisy fixed-window number, while cheap
//! read shapes stop being the reason the cell keeps going once they are precise.
//!
mod options;
mod result;
mod setup;

use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime};

use futures::future::join_all;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use glassdb::{Collection, Database, Error as GError};
use glassdb_backend::Backend;
use glassdb_bench_scale::bench::{Bench, samples_for_rel_ci};
use glassdb_bench_scale::run::join_tasks_until;

use super::backend;
use super::{Execution, cooldown};
pub(super) use options::Options;
use options::{CellDimension, Mode};
pub(super) use result::RunResult;
use result::{CellMetadata, CellResult, ShapeMeasurement};

/// Fixed opaque value written on every put; only op counts and contention
/// matter, not the payload.
fn value() -> Vec<u8> {
    vec![0x5a; 128]
}

/// The base key name for pool index `i`.
fn key_bytes(i: usize) -> Vec<u8> {
    format!("key{i}").into_bytes()
}

// ---------------------------------------------------------------------------
// Dimensions
// ---------------------------------------------------------------------------

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

    /// Whether the shape writes (drives lock/CAS contention) or only reads.
    fn is_write(self) -> bool {
        matches!(self, Shape::RwSingle | Shape::RwMany)
    }
}

pub(super) fn run(
    handle: &Handle,
    factory: &backend::Factory,
    options: &Options,
    execution: Execution,
) -> Result<Vec<RunResult>, Box<dyn Error>> {
    let dimensions = options.cell_dimensions()?;
    let invocation = SystemTime::UNIX_EPOCH.elapsed()?.as_millis();
    let mut runs = Vec::with_capacity(execution.runs);
    for run in 1..=execution.runs {
        handle.block_on(cooldown(execution, run));
        let mut cells = Vec::new();
        for &CellDimension { mode, affinity_pct } in &dimensions {
            eprintln!("{}", result::cell_started(run, mode.label(), affinity_pct));
            let database_name = format!(
                "perfbenchmixed{invocation}r{run}{}a{affinity_pct}",
                mode.label()
            );
            cells.push(run_cell(
                handle,
                factory.backend(),
                &database_name,
                options,
                execution,
                mode,
                affinity_pct,
            )?);
        }
        runs.push(RunResult::new(run, cells));
    }
    Ok(runs)
}

/// Distributes `w` workers across `k` Databases as evenly as possible, dropping
/// empty slots (so `k` is effectively clamped to `w`).
fn split_workers(w: usize, k: usize) -> Vec<usize> {
    let k = k.max(1).min(w.max(1));
    let base = w / k;
    let rem = w % k;
    (0..k)
        .map(|i| base + usize::from(i < rem))
        .filter(|&c| c > 0)
        .collect()
}

/// One shape's timer and its workers per Database.
struct ShapePlan {
    shape: Shape,
    bench: Arc<Bench>,
    /// Indices into the cell's `dbs` vector, with the worker count for each.
    slots: Vec<(usize, usize)>,
}

fn run_cell(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    database_name: &str,
    options: &Options,
    execution: Execution,
    mode: Mode,
    affinity_pct: u8,
) -> Result<CellResult, Box<dyn Error>> {
    let pool_size = mode.pool_size(options);
    let prepared = setup::prepare_cell(
        handle,
        backend,
        database_name,
        setup::CellConfig {
            databases: options.databases,
            pool_size,
            split_quiet: options.split_quiet,
            split_settle_timeout: options.split_settle_timeout,
            drain_timeout: execution.drain_timeout,
        },
    )?;
    let w = options.workers_per_shape;
    let slots: Vec<(usize, usize)> = split_workers(w, prepared.databases().len())
        .into_iter()
        .enumerate()
        .collect();
    let plans: Vec<ShapePlan> = SHAPES
        .into_iter()
        .map(|shape| ShapePlan {
            shape,
            bench: Arc::new(Bench::new(options.max_duration)),
            slots: slots.clone(),
        })
        .collect();

    // Collection binding and its record reads are setup, not transaction work.
    // Bracket stats only after every client has opened every possible target.
    let active = prepared.begin_measurement();
    for p in &plans {
        p.bench.start();
    }

    let stop = Arc::new(AtomicBool::new(false));
    let target = samples_for_rel_ci(options.target_ci);
    let ctx = WorkerCtx {
        stop: stop.clone(),
        pool_size,
        multi_keys: options.multi_keys,
        affinity_pct,
    };
    let (drive, run, deadline) = handle.block_on(async {
        let handles = spawn_workers(active.databases(), active.collections(), &plans, &ctx);
        let drive = drive_to_significance(
            &plans,
            &stop,
            target,
            options.duration,
            options.max_duration,
        )
        .await;
        let deadline = tokio::time::Instant::now() + execution.drain_timeout;
        let run = join_tasks_until(handles, deadline).await;
        (drive, run, deadline)
    });

    for p in &plans {
        p.bench.end();
    }
    let completed = active.teardown(handle, deadline, run)?;
    let cell_converged = match drive {
        DriveOutcome::Converged => true,
        DriveOutcome::Capped => false,
        DriveOutcome::WorkerStopped => {
            return Err("benchmark worker stopped without reporting its error".into());
        }
    };
    if !cell_converged {
        eprintln!(
            "{}",
            result::cell_capped(mode.label(), affinity_pct, options.target_ci)
        );
    }

    let measurements = plans
        .iter()
        .map(|plan| ShapeMeasurement::new(plan.shape.name(), plan.bench.results()));
    Ok(CellResult::summarize(
        CellMetadata::new(
            mode.label(),
            affinity_pct,
            completed.databases,
            completed.setup_splits,
            completed.split_settle_elapsed,
        ),
        measurements,
        &completed.deltas,
        target,
    ))
}

/// Cell-wide context shared by every worker.
#[derive(Clone)]
struct WorkerCtx {
    /// Set by [`drive_to_significance`] to stop all shapes at once.
    stop: Arc<AtomicBool>,
    pool_size: usize,
    multi_keys: usize,
    affinity_pct: u8,
}

/// Spawns every shape's workers across the cell's Databases, returning their
/// join handles. Workers loop until `ctx.stop` is set (by
/// [`drive_to_significance`]) or their `Bench` deadline (the `--max-duration`
/// cap) elapses.
fn spawn_workers(
    dbs: &[Database],
    collections: &[Arc<[Collection]>],
    plans: &[ShapePlan],
    ctx: &WorkerCtx,
) -> Vec<JoinHandle<Result<(), GError>>> {
    let mut handles: Vec<JoinHandle<Result<(), GError>>> = Vec::new();
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    for p in plans {
        for &(db_idx, count) in &p.slots {
            for _ in 0..count {
                let db = dbs[db_idx].clone();
                let collections = collections[db_idx].clone();
                let bench = p.bench.clone();
                let shape = p.shape;
                let ctx = ctx.clone();
                seed = seed.wrapping_add(0x1000_0000_0000_0001);
                handles.push(tokio::spawn(worker(
                    db,
                    collections,
                    db_idx,
                    shape,
                    bench,
                    seed,
                    ctx,
                )));
            }
        }
    }
    handles
}

/// Runs the cell to statistical significance. With every shape's workers already
/// spawned, it polls their committed-transaction counts and stops all shapes at
/// once once each has reached `target` (so its throughput 95% CI is within
/// `--target-ci`) — but never before `min_dur` and never past `max_dur`. Keeping
/// every shape running until the last one is precise avoids skewing contention by
/// letting readers drop out early.
///
/// `target == 0` disables adaptivity: the cell runs exactly `min_dur` and reports
/// as converged (the caller asked for a fixed window, not a significance target).
enum DriveOutcome {
    Converged,
    Capped,
    WorkerStopped,
}

async fn drive_to_significance(
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
                    .all(|p| p.bench.sample_count() as u64 >= target));
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

/// One worker: loops its shape's transaction until the cell's `stop` flag is set
/// (the significance controller) or the shape's `Bench` deadline elapses, keying
/// from the shared pool. [`Bench::measure`] absorbs unknown outcomes into the
/// logical sample; every definitive error stops and fails the cell.
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
    let n = match shape {
        Shape::RwMany | Shape::RoMulti => ctx.multi_keys.min(ctx.pool_size).max(1),
        Shape::RwSingle | Shape::RoSingle => 1,
    };
    while !ctx.stop.load(Ordering::Relaxed) && !bench.is_finished() {
        // Select all inputs before the measured region so RNG work and borrows
        // do not span the transaction future.
        let collection = pick_collection(&mut rng, home, collections.len(), ctx.affinity_pct);
        let idxs = pick_keys(&mut rng, ctx.pool_size, n);
        let keys: Vec<Vec<u8>> = idxs.iter().map(|&i| key_bytes(i)).collect();
        let keys = &keys;
        let coll = &collections[collection];
        let db = &db;
        let result = bench
            .measure(|| async {
                if shape.is_write() {
                    rmw_tx(db, coll, keys).await
                } else {
                    ro_tx(db, coll, keys).await
                }
            })
            .await;
        if let Err(err) = result {
            ctx.stop.store(true, Ordering::Relaxed);
            return Err(err);
        }
    }
    Ok(())
}

/// Selects the Database's home with probability `affinity_pct`; the remaining
/// probability is uniform over every collection, including the home. Thus 0%
/// has no client-specific preference and 100% isolates every client.
fn pick_collection(rng: &mut StdRng, home: usize, collections: usize, affinity_pct: u8) -> usize {
    debug_assert!(home < collections);
    // Consume both draws at every affinity so paired sweep cells retain the
    // same subsequent key-selection stream.
    let choose_home = rng.random_range(0..100u8) < affinity_pct;
    let uniform = rng.random_range(0..collections);
    if choose_home { home } else { uniform }
}

/// Picks `n` distinct pool indices (or the whole pool when `n >= pool_size`).
fn pick_keys(rng: &mut StdRng, pool_size: usize, n: usize) -> Vec<usize> {
    let n = n.min(pool_size).max(1);
    if n >= pool_size {
        return (0..pool_size).collect();
    }
    let mut set = HashSet::with_capacity(n);
    while set.len() < n {
        set.insert(rng.random_range(0..pool_size));
    }
    let mut selected: Vec<_> = set.into_iter().collect();
    selected.sort_unstable();
    selected
}

/// Read-modify-write of every key (parallel reads, then a write-back each).
async fn rmw_tx(db: &Database, coll: &Collection, keys: &[Vec<u8>]) -> Result<(), GError> {
    db.tx(|tx| async move {
        let vals = join_all(keys.iter().map(|k| tx.read(coll, k))).await;
        for (k, rv) in keys.iter().zip(vals) {
            match rv {
                Ok(_) => {}
                Err(e) => return Err(e),
            }
            tx.write(coll, k, &value())?;
        }
        Ok(())
    })
    .await
}

/// Read-only over every key (in parallel).
async fn ro_tx(db: &Database, coll: &Collection, keys: &[Vec<u8>]) -> Result<(), GError> {
    db.tx(|tx| async move {
        let vals = join_all(keys.iter().map(|k| tx.read(coll, k))).await;
        for rv in vals {
            match rv {
                Ok(_) => {}
                Err(e) => return Err(e),
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
    fn affinity_extremes_are_uniform_or_home_only() {
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
