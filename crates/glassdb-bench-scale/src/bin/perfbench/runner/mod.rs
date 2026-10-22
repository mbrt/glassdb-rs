//! The runner of all experiments: it reads the configuration, runs the loop of
//! cells, and writes their results.
//!
//! Each cell opens the database instances of its groups over the storage that
//! it starts from, runs the workers of its shapes, warms up until its splits
//! and merges stop, and then measures until its end condition.

pub(crate) mod config;
pub(crate) mod kinds;
pub(crate) mod model;
pub(crate) mod result;
pub(crate) mod seed;
pub(crate) mod sources;
pub(crate) mod until;
pub(crate) mod workers;

use std::error::Error;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime};

use glassdb::{Collection, Database, Stats};
use glassdb_bench_scale::backend_breakdown::{BackendBreakdown, BackendBreakdownHandle};
use glassdb_bench_scale::bench::Bench;
use glassdb_bench_scale::run::{join_tasks_until, shutdown_databases_until};
use glassdb_concurr::rt;
use indexmap::IndexMap;
use serde_json::{Map, Value as Json};

use super::backend::Factory;
use super::{Execution, cooldown, progress};
use config::{CellPoint, Resolved, Start};
use model::CellSpec;
use progress::Stage;
use result::{
    CellResult, GroupResult, InstanceResult, MeasureResult, OpsPerTx, ProtocolPerTx, RunResult,
    ShapeResult, THROUGHPUT_WINDOWS, TimelineEntry, WarmupResult,
};
use seed::{Seeds, Storage, gc_stats, millis, restructurer_stats, structural_changes};
use sources::Family;
use workers::{Opener, Outcome, OwnInstances, Pause, ShapeRun, Shared, Target, Worker};

/// The engine stats and the backend operations of the database instances of
/// a cell over one interval.
#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Window {
    pub(crate) stats: Stats,
    pub(crate) breakdown: BackendBreakdown,
}

impl Window {
    /// Returns the totals of `db` and of the storage now.
    pub(crate) fn now(db: &Database, breakdown: &BackendBreakdownHandle) -> Self {
        Self {
            stats: db.stats(),
            breakdown: breakdown.snapshot(),
        }
    }

    /// Returns the totals from `start` to this window.
    pub(crate) fn since(&self, start: &Window) -> Self {
        Self {
            stats: self.stats - start.stats,
            breakdown: self.breakdown - start.breakdown,
        }
    }

    pub(crate) fn add(&mut self, other: Window) {
        self.stats += other.stats;
        self.breakdown = self.breakdown + other.breakdown;
    }
}

/// The state that a cell leaves for the cell after it.
struct Carry {
    storage: Storage,
    /// The open instances of the cell and their collections, for
    /// `previous`. Other cells shut their instances down.
    instances: Option<Vec<OpenInstance>>,
    /// A copy of the storage after the warm-up, for `previous-warm-up`.
    after_warm_up: Option<Storage>,
}

struct OpenInstance {
    db: Database,
    collections: IndexMap<String, Arc<[Collection]>>,
}

/// Runs all cells of `resolved` for each run.
pub(crate) fn run(
    runtime: &tokio::runtime::Runtime,
    factory: &Factory,
    resolved: &Resolved,
    execution: Execution,
) -> Result<Vec<RunResult>, Box<dyn Error>> {
    let (points, specs) = plan(resolved)?;
    let invocation = SystemTime::UNIX_EPOCH.elapsed()?.as_millis();
    let mut runs = Vec::with_capacity(execution.runs);
    for run in 1..=execution.runs {
        runtime.block_on(cooldown(execution, run));
        let mut seeds = Seeds::new(
            factory,
            format!(
                "perfbench{}{invocation}r{run}",
                resolved.name.replace('-', "")
            ),
            execution.drain_timeout,
        );
        let mut carry: Option<Carry> = None;
        let mut cells = Vec::with_capacity(points.len());
        for (index, (point, spec)) in points.iter().zip(&specs).enumerate() {
            crate::progress::log!("{}: run={run} {}", resolved.name, describe(point));
            let next_continues = points
                .get(index + 1)
                .is_some_and(|next| next.start == Start::Previous);
            progress::start_cell();
            let (cell, next) = runtime.block_on(run_cell(
                &mut seeds,
                carry.take(),
                point,
                spec,
                next_continues,
                execution,
            ))?;
            progress::finish_cell();
            cells.push(cell);
            carry = next;
        }
        if let Some(instances) = carry.and_then(|carry| carry.instances) {
            runtime.block_on(close(instances, execution.drain_timeout))?;
        }
        runs.push(RunResult { run, cells });
    }
    Ok(runs)
}

/// Returns the cells of `resolved`, with one seed for each chain of cells that
/// continue each other, and checks that the chains can continue.
pub(crate) fn plan(resolved: &Resolved) -> Result<(Vec<CellPoint>, Vec<CellSpec>), Box<dyn Error>> {
    let points = resolved.points();
    let mut specs: Vec<CellSpec> = points
        .iter()
        .map(|point| CellSpec::from_table(&resolved.cell_table(point)?))
        .collect::<Result<_, _>>()?;
    let mut first = 0;
    for end in 1..=points.len() {
        if end < points.len() && points[end].start != Start::Seed {
            continue;
        }
        let largest = specs[first..end]
            .iter()
            .map(|spec| spec.collections)
            .max()
            .unwrap_or(1);
        for spec in &mut specs[first..end] {
            spec.seed_collections = largest;
        }
        first = end;
    }
    check_continuations(&points, &specs)?;
    Ok((points, specs))
}

/// Rejects a `previous` cell that changes what its open instances cannot
/// change: the instances of its groups, and their database options.
fn check_continuations(points: &[CellPoint], specs: &[CellSpec]) -> Result<(), Box<dyn Error>> {
    for (index, point) in points.iter().enumerate().skip(1) {
        let (before, after) = (&specs[index - 1], &specs[index]);
        if point.start == Start::Seed {
            continue;
        }
        if seed::SeedKey::of(before) != seed::SeedKey::of(after) {
            return Err(format!(
                "cell {}: an axis that continues from the cell before it changes the seed",
                describe(point)
            )
            .into());
        }
        let layout = |spec: &CellSpec| -> Vec<(String, usize)> {
            spec.groups
                .iter()
                .map(|group| (group.name.clone(), group.instances))
                .collect()
        };
        if point.start == Start::Previous
            && (layout(before) != layout(after) || before.params.database != after.params.database)
        {
            return Err(format!(
                "cell {}: `previous` keeps the open instances, so it cannot change the groups, \
                 their instances, or cell.database",
                describe(point)
            )
            .into());
        }
    }
    Ok(())
}

/// Runs one cell. `next_continues` tells if the next cell continues on the
/// open instances of this one.
async fn run_cell(
    seeds: &mut Seeds<'_>,
    carry: Option<Carry>,
    point: &CellPoint,
    spec: &CellSpec,
    next_continues: bool,
    execution: Execution,
) -> Result<(CellResult, Option<Carry>), Box<dyn Error>> {
    let (storage, instances) = match (point.start, carry) {
        (Start::Previous, Some(carry)) => (carry.storage, carry.instances),
        (Start::PreviousWarmUp, Some(carry)) => {
            let storage = carry
                .after_warm_up
                .ok_or("`previous-warm-up` needs storage that can fork")?;
            (storage, None)
        }
        (_, _) => (seeds.storage_for(spec).await?, None),
    };
    let instances = match instances {
        Some(instances) => instances,
        None => open_instances(&storage, spec).await?,
    };
    let databases: Vec<Database> = instances.iter().map(|i| i.db.clone()).collect();

    let until = spec.until()?;
    let shared = Arc::new(Shared {
        stop: AtomicBool::new(false),
        failed: AtomicBool::new(false),
        recording: AtomicBool::new(false),
        pause: Pause::closed(),
        passes_left: AtomicUsize::new(0),
        warm_bench: Bench::new(Duration::ZERO),
        affinity: spec.params.affinity,
        until: until.clone(),
        breakdown: storage.breakdown.clone(),
        boundary: Mutex::new(None),
    });
    let max = spec.params.measure.max_duration;
    let own = Arc::new(OwnInstances::default());
    let mut shapes = Vec::new();
    let mut handles = Vec::new();
    let mut slots: IndexMap<(String, Family), usize> = IndexMap::new();
    let mut first_instance = 0;
    let mut worker_index = 0;
    for group in &spec.groups {
        for shape in &group.shapes {
            let run = Arc::new(ShapeRun {
                name: shape.name.clone(),
                kind: shape.kind.clone(),
                pool: spec.pools[&shape.pool].clone(),
                bench: Arc::new(Bench::new(max)),
            });
            let family_workers = spec.source_workers(&shape.pool, shape.source.family());
            let pool_size = spec.pool_size(&shape.pool);
            let split = workers::split_workers(shape.workers, group.instances);
            for (offset, count) in split.into_iter().enumerate() {
                let instance = first_instance + offset;
                for _ in 0..count {
                    let slot = slots
                        .entry((shape.pool.clone(), shape.source.family()))
                        .or_default();
                    let keys =
                        shape
                            .source
                            .worker(pool_size, *slot, family_workers, shape.kind.keys());
                    *slot += 1;
                    if keys.pass_len().is_some() && until.by_passes() {
                        shared.passes_left.fetch_add(1, Ordering::Relaxed);
                    }
                    let target = if shape.kind.fresh_instance() {
                        Target::Fresh {
                            opener: Opener {
                                backend: storage.backend.clone(),
                                breakdown: storage.breakdown.clone(),
                                database: storage.database.clone(),
                                options: spec.params.database.clone(),
                                pool: shape.pool.clone(),
                                collections: spec.collections,
                                drain_timeout: execution.drain_timeout,
                            },
                            own: own.clone(),
                        }
                    } else {
                        Target::Instance {
                            db: instances[instance].db.clone(),
                            collections: instances[instance].collections[&shape.pool].clone(),
                        }
                    };
                    handles.push(
                        Worker {
                            shared: shared.clone(),
                            shape: run.clone(),
                            target,
                            keys,
                            home: instance % spec.collections,
                            collections: spec.collections,
                            seed: workers::worker_seed(worker_index),
                        }
                        .spawn(),
                    );
                    worker_index += 1;
                }
            }
            shapes.push((group.name.clone(), shape.workers, run));
        }
        first_instance += group.instances;
    }
    if until.by_passes() && shared.passes_left.load(Ordering::Relaxed) == 0 {
        return Err("the end condition needs passes, but no worker has a sequence source".into());
    }

    let mut timeline = Timeline::new(&databases);
    let warm_start = Instant::now();
    let changes_start = restructurer_stats(&databases);
    let gc_start = gc_stats(&databases);
    let warm_up = &spec.params.warm_up;
    let (warm_stage, mut measure_stage) = Stage::split(
        warm_start,
        warm_up.minimum.min(warm_up.timeout),
        spec.params.measure.duration,
    );
    let quiet = if warm_up.timeout.is_zero() {
        true
    } else {
        shared.pause.open();
        let minimum = async {
            tokio::select! {
                () = rt::sleep(warm_up.minimum) => {}
                () = tokio::time::sleep(warm_up.timeout) => {}
            }
        };
        timeline
            .sample_during(&databases, minimum, false, warm_stage)
            .await;
        let remaining = warm_up.timeout.saturating_sub(warm_start.elapsed());
        match warm_up.quiet {
            Some(quiet) => {
                let wait = seed::wait_for_quiet(
                    || structural_changes(&databases),
                    &shared.stop,
                    quiet,
                    remaining,
                );
                timeline
                    .sample_during(&databases, wait, false, warm_stage)
                    .await
            }
            None => {
                timeline
                    .sample_during(&databases, tokio::time::sleep(remaining), false, warm_stage)
                    .await;
                false
            }
        }
    };
    let changes_end = restructurer_stats(&databases);
    let warmup = WarmupResult {
        splits: changes_end.splits - changes_start.splits,
        merges: changes_end.merges - changes_start.merges,
        wall_ms: millis(warm_start.elapsed()),
        quiet,
        gc: (gc_stats(&databases) - gc_start).into(),
    };
    crate::progress::log!(
        "  warm-up took {}ms: {} splits and {} merges{}",
        warmup.wall_ms,
        warmup.splits,
        warmup.merges,
        if quiet { "" } else { " (not quiet)" }
    );

    // The baselines and the samples start with no transaction in flight.
    shared
        .pause
        .close_and_wait(handles.len(), &shared.stop)
        .await;
    let full_gc = if spec.params.gc {
        match seed::collect_garbage(&databases[0]).await {
            Ok(collected) => {
                crate::progress::log!(
                    "  full GC took {}ms: {} transaction records deleted",
                    collected.wall_ms,
                    collected.deleted
                );
                Some(collected)
            }
            Err(error) => {
                crate::progress::log!("  note: {error}");
                None
            }
        }
    } else {
        None
    };
    let after_warm_up = storage.fork();
    let baselines: Vec<Stats> = databases.iter().map(Database::stats).collect();
    let breakdown_start = storage.breakdown.snapshot();
    let measure_start = Instant::now();
    let measure_model_start = rt::Instant::now();
    measure_stage.started = measure_start;
    for (_, _, shape) in &shapes {
        shape.bench.start();
    }
    shared.recording.store(true, Ordering::Relaxed);
    shared.pause.open();
    let target = match until {
        until::Until::Converged => spec.params.measure.window_commits * THROUGHPUT_WINDOWS as u64,
        _ => 0,
    };
    let benches: Vec<_> = shapes
        .iter()
        .map(|(_, _, shape)| shape.bench.clone())
        .collect();
    let drive = workers::drive(&shared, &benches, target, spec.params.measure.duration, max);
    let outcome = timeline
        .sample_during(&databases, drive, true, measure_stage)
        .await;
    // The windows end when the workers stop, so the drain of the transactions
    // in flight does not lower the last window.
    for bench in &benches {
        bench.end();
    }
    let mut measure_wall = measure_start.elapsed();
    shared.stop.store(true, Ordering::Relaxed);
    let deadline = tokio::time::Instant::now() + execution.drain_timeout;
    let joined = join_tasks_until(handles, deadline).await;
    let drain = measure_start.elapsed().saturating_sub(measure_wall);
    if let Err(error) = joined {
        let _ = close(instances, execution.drain_timeout).await;
        return Err(error.into());
    }
    if outcome == Outcome::WorkerStopped {
        let _ = close(instances, execution.drain_timeout).await;
        return Err("benchmark worker stopped without reporting its error".into());
    }
    if outcome == Outcome::Capped {
        crate::progress::log!(
            "  note: the measurement reached its maximum duration before its end condition"
        );
    }
    let mut measured: Vec<Stats> = databases.iter().map(Database::stats).collect();
    let mut breakdown_measured = storage.breakdown.snapshot();
    // An end condition that leaves its last pass out ends the measurement at
    // the start of that pass. The cells with such a condition have one worker
    // on one instance, so the stats of that instance are the boundary.
    let boundary = *shared.boundary.lock().unwrap();
    let mut cut = None;
    if let Some(boundary) = boundary
        && let [_] = measured.as_slice()
    {
        measured = vec![boundary.window.stats];
        breakdown_measured = boundary.window.breakdown;
        measure_wall = boundary.wall.duration_since(measure_start);
        cut = Some(boundary);
    }
    // A cell that the next cell does not continue shuts its instances down,
    // and their shutdown work is part of the cell, as the work that it drains
    // belongs to the measured transactions.
    let carried = if next_continues {
        Some(instances)
    } else {
        close(instances, execution.drain_timeout).await?;
        None
    };
    let after_shutdown = storage.breakdown.snapshot();
    let deltas: Vec<Stats> = databases
        .iter()
        .zip(&baselines)
        .map(|(db, base)| db.stats() - *base)
        .collect();
    let shutdown_stats =
        databases
            .iter()
            .zip(&measured)
            .fold(Stats::default(), |mut sum, (db, measured)| {
                sum += db.stats() - *measured;
                sum
            });
    let lifecycle = *own.lifecycle.lock().unwrap();
    let own_workload = *own.workload.lock().unwrap();
    let own_shutdown = *own.shutdown.lock().unwrap();

    // A measurement cut at a pass has no shutdown: the next cell continues it.
    let deltas: Vec<Stats> = if cut.is_some() {
        measured
            .iter()
            .zip(&baselines)
            .map(|(measured, base)| *measured - *base)
            .collect()
    } else {
        deltas
    };
    let breakdown = if cut.is_some() {
        breakdown_measured - breakdown_start
    } else {
        after_shutdown - breakdown_start
    };
    let mut total = Stats::default();
    for delta in &deltas {
        total += *delta;
    }
    total += own_workload.stats;
    let shape_results: Vec<ShapeResult> = shapes
        .iter()
        .map(|(group, workers, shape)| {
            let mut results = shape.bench.results();
            if let Some(cut) = cut {
                results.samples.truncate(cut.samples);
                results.completions.truncate(cut.samples);
                results.tot_duration = cut.model.duration_since(measure_model_start);
            }
            ShapeResult::summarize(&shape.name, group, *workers, &results, target)
        })
        .collect();
    let logical: u64 = shape_results
        .iter()
        .map(|shape| shape.committed as u64)
        .sum();
    // The storage counts the opens of the instances that workers open for
    // single transactions, which are not in any window, so they are taken out.
    let mut workload = Window {
        stats: measured.iter().zip(&baselines).fold(
            Stats::default(),
            |mut sum, (measured, base)| {
                sum += *measured - *base;
                sum
            },
        ),
        breakdown: breakdown_measured - breakdown_start - lifecycle.breakdown,
    };
    workload.add(own_workload);
    let mut shutdown_window = Window {
        stats: shutdown_stats,
        breakdown: after_shutdown - breakdown_measured,
    };
    shutdown_window.add(own_shutdown);
    let costs = result::costs(workload, shutdown_window, logical);
    let mut groups = Vec::new();
    let mut index = 0;
    for group in &spec.groups {
        let members = &deltas[index..index + group.instances];
        groups.push(GroupResult {
            group: group.name.clone(),
            instances: members.iter().map(InstanceResult::of).collect(),
        });
        index += group.instances;
    }
    let mut labels = Map::new();
    for (axis, label) in &point.labels {
        labels.insert(axis.clone(), Json::String(label.clone()));
    }
    let mut parameters = Map::new();
    for (path, value) in &point.set {
        parameters.insert(path.clone(), config::toml_to_json(value));
    }
    for shape in spec.groups.iter().flat_map(|group| &group.shapes) {
        match &shape.source {
            sources::SourceSpec::Overlap { shared } => {
                parameters.insert("keys".into(), Json::from(shape.kind.keys()));
                parameters.insert("shared".into(), Json::from(*shared));
            }
            sources::SourceSpec::Stripe { shared_pct } => {
                parameters.insert("sharedPct".into(), Json::from(*shared_pct));
            }
            _ => {}
        }
    }
    let cell = CellResult {
        labels,
        parameters,
        start: match point.start {
            Start::Seed => "seed",
            Start::Previous => "previous",
            Start::PreviousWarmUp => "previous-warm-up",
        },
        workload: spec.params.workload.clone(),
        affinity_pct: spec.params.affinity,
        database_limit: spec.uses_cell_databases.then_some(spec.params.databases),
        databases: databases.len(),
        collections: spec.collections,
        workers_per_shape: spec.uses_cell_workers.then_some(spec.params.workers),
        failures: 0,
        seed: storage.seed,
        warmup,
        full_gc,
        measure: MeasureResult {
            wall_ms: millis(measure_wall),
            met: outcome == Outcome::Met,
            splits: total.restructurer.splits,
            merges: total.restructurer.merges,
            worker_drain_ms: drain.as_secs_f64() * 1000.0,
        },
        shapes: shape_results,
        groups,
        aggregate_ops: OpsPerTx::of(&total, logical, breakdown),
        aggregate_protocol: ProtocolPerTx::of(&total, logical),
        gc: total.gc.into(),
        timeline: timeline.entries,
        costs,
    };
    Ok((
        cell,
        Some(Carry {
            storage,
            instances: carried,
            after_warm_up,
        }),
    ))
}

async fn open_instances(
    storage: &Storage,
    spec: &CellSpec,
) -> Result<Vec<OpenInstance>, Box<dyn Error>> {
    let mut instances = Vec::with_capacity(spec.instances());
    for _ in 0..spec.instances() {
        let db = storage.open(&spec.params.database).await?;
        let collections = seed::open_collections(&db, spec).await?;
        instances.push(OpenInstance { db, collections });
    }
    Ok(instances)
}

async fn close(
    instances: Vec<OpenInstance>,
    drain_timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let databases: Vec<Database> = instances.into_iter().map(|i| i.db).collect();
    shutdown_databases_until(&databases, tokio::time::Instant::now() + drain_timeout).await?;
    Ok(())
}

fn describe(point: &CellPoint) -> String {
    if point.labels.is_empty() {
        return "cell".to_string();
    }
    point
        .labels
        .iter()
        .map(|(axis, label)| format!("{axis}={label}"))
        .collect::<Vec<_>>()
        .join(" ")
}

/// The time between two samples of the structural changes in the timeline.
const TIMELINE_INTERVAL: Duration = Duration::from_secs(1);

/// The structural changes of each instance in each wall second.
struct Timeline {
    last: Vec<Stats>,
    entries: Vec<TimelineEntry>,
}

impl Timeline {
    fn new(databases: &[Database]) -> Self {
        Self {
            last: databases.iter().map(Database::stats).collect(),
            entries: Vec::new(),
        }
    }

    /// Runs `future`, and samples the changes of `databases` once each
    /// interval until it ends.
    async fn sample_during<T>(
        &mut self,
        databases: &[Database],
        future: impl std::future::Future<Output = T>,
        measured: bool,
        stage: Stage,
    ) -> T {
        tokio::pin!(future);
        let mut ticks = tokio::time::interval(TIMELINE_INTERVAL);
        ticks.tick().await;
        loop {
            tokio::select! {
                output = &mut future => return output,
                _ = ticks.tick() => {
                    self.sample(databases, measured);
                    progress::advance(stage);
                }
            }
        }
    }

    fn sample(&mut self, databases: &[Database], measured: bool) {
        let now: Vec<Stats> = databases.iter().map(Database::stats).collect();
        let (splits, merges) = now
            .iter()
            .zip(&self.last)
            .map(|(now, last)| {
                let delta = *now - *last;
                (delta.restructurer.splits, delta.restructurer.merges)
            })
            .unzip();
        self.last = now;
        self.entries.push(TimelineEntry {
            second: self.entries.len() as u64 + 1,
            measured,
            splits,
            merges,
        });
    }
}

#[cfg(test)]
mod tests;
