//! Mixed-workload contention benchmark.
//!
//! Runs all four transaction shapes (`rwSingle`, `rwMany`, `roSingle`,
//! `roMulti`) concurrently over one selected storage backend, sweeping:
//!
//! - contention **mode** (`lo` = each worker draws from its own stripe of a
//!   large pool, except for `--shared-pct` of its transactions, so transactions
//!   rarely conflict but still share leaves; `hi` = keys drawn from a small hot
//!   pool so they collide on the same leaves), and
//! - home-collection **affinity** (`0` = each `Database` chooses uniformly
//!   among every collection; `100` = each uses only its own collection).
//! - client-`Database` limits and workers per shape. Each cell opens no more
//!   Databases than it has workers per shape, so every open client is active.
//!
//! Every `Database` runs every shape and has one distinct home collection. An
//! intermediate affinity mixes home traffic with uniformly selected
//! collections, exposing cross-client overlap without hard-coding a small set
//! of discrete client-to-collection layouts. All clients wrap the same backend.
//!
//! The key set is seeded before the measurement clients open. The seeding
//! database then waits until its splits, merges, and garbage collection have
//! not changed for `--quiet-period`; `--seed-timeout` fails the run. A storage that can
//! fork is seeded once for each mode, and each cell runs on its own fork. Any
//! other storage is seeded again for each cell. Each cell then warms up under
//! its own load for at least `--warmup-minimum`, then until splits and merges
//! stop for `--quiet-period`, or for at most `--warmup-timeout`. A cell
//! measures after the timeout even when splits continue, and its result says
//! so. Setup, warm-up, collection opening, and their backend operations are
//! outside the measured interval.
//!
//! On a storage that can fork, the state after the warm-up of a cell is the
//! checkpoint where the next cell starts, when only the workers per shape
//! differ between the two. Every cell of such a chain then starts near the
//! topology of its own load, so its warm-up stays short. A cell with another
//! mode, affinity, or Database limit starts from the seed instead, because the
//! topology of the other traffic does not fit it. The result of a cell
//! therefore depends on the cells before it in its chain.
//!
//! Each cell uses **sequential (adaptive) sampling**: all shapes run
//! concurrently until every shape has committed `--window-commits` per
//! throughput window on average (or `--max-duration` caps the run). The
//! measured interval is split into equal windows, and the commit counts of the
//! windows give the throughput distribution of each shape. Heavily-contended
//! write shapes therefore run longer to earn a stable distribution instead of
//! returning a noisy fixed-interval number.
//!
mod options;
mod result;
mod setup;
mod workload;

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;

use tokio::runtime::Handle;

use glassdb_backend::Backend;
use glassdb_bench_scale::run::join_tasks_until;

use super::backend;
use super::{Execution, cooldown};
use backend::ForkableBackend;
pub(super) use options::Options;
use options::{CellDimension, Mode};
pub(super) use result::RunResult;
use result::{CellMetadata, CellResult};
use setup::Seed;
use workload::DriveOutcome;

/// Fixed opaque value written on every put.
fn value() -> Vec<u8> {
    vec![0x5a; 128]
}

/// The base key name for one pool index.
fn key_bytes(index: usize) -> Vec<u8> {
    format!("key{index}").into_bytes()
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
        let mut seeds = Seeds::new(
            handle,
            factory,
            options,
            execution,
            &dimensions,
            invocation,
            run,
        );
        let mut cells = Vec::new();
        for &dimension in &dimensions {
            eprintln!("{}", result::cell_started(run, dimension));
            let start = seeds.for_cell(dimension)?;
            let (cell, checkpoint) = run_cell(handle, &start, options, execution, dimension)?;
            seeds.record_checkpoint(dimension, checkpoint);
            cells.push(cell);
        }
        runs.push(RunResult::new(run, cells));
    }
    Ok(runs)
}

/// The database a cell starts from, and the storage that holds it.
struct CellStart {
    seed: Seed,
    /// The storage of the cell, when it can fork.
    storage: Option<ForkableBackend>,
}

impl CellStart {
    /// Returns a start over its own fork of `storage`, which holds `seed`.
    fn fork_of(storage: &ForkableBackend, seed: &Seed) -> Self {
        let cell = storage.fork();
        Self {
            seed: seed.with_backend(cell.backend()),
            storage: Some(cell),
        }
    }

    /// Returns a copy of the current state of the storage, or none when the
    /// storage cannot fork.
    ///
    fn checkpoint(&self) -> Option<ForkableBackend> {
        self.storage.as_ref().map(ForkableBackend::fork)
    }
}

/// Provides each cell of a run with its own seeded database.
///
/// A storage that can fork is seeded once for each mode of the run, with the
/// largest collection count of that mode. A cell gets a fork of the checkpoint
/// that the cell before it recorded when both cells differ only in their
/// workers per shape. Any other cell gets a fork of the seed. A storage that
/// cannot fork is seeded again for each cell, in a new database.
struct Seeds<'a> {
    handle: &'a Handle,
    factory: &'a backend::Factory,
    options: &'a Options,
    execution: Execution,
    name_prefix: String,
    /// The largest collection count of the cells of each mode.
    collections: Vec<(Mode, usize)>,
    /// The seeded storage of the mode of the last cell. Cells come grouped by
    /// mode, so the storage of an earlier mode is not used again.
    forkable: Option<(Mode, ForkableBackend, Seed)>,
    /// The latest checkpoint, with the cell it follows from.
    checkpoint: Option<(Chain, ForkableBackend)>,
}

/// The dimensions of a cell that its checkpoint carries over to the next cell.
/// Only the workers per shape may change along a chain.
#[derive(Clone, Copy, PartialEq, Eq)]
struct Chain {
    mode: Mode,
    affinity_pct: u8,
    database_limit: usize,
}

impl Chain {
    fn of(cell: CellDimension) -> Self {
        Self {
            mode: cell.mode,
            affinity_pct: cell.affinity_pct,
            database_limit: cell.database_limit,
        }
    }
}

impl<'a> Seeds<'a> {
    fn new(
        handle: &'a Handle,
        factory: &'a backend::Factory,
        options: &'a Options,
        execution: Execution,
        dimensions: &[CellDimension],
        invocation: u128,
        run: usize,
    ) -> Self {
        let mut collections: Vec<(Mode, usize)> = Vec::new();
        for cell in dimensions {
            match collections.iter_mut().find(|(mode, _)| *mode == cell.mode) {
                Some((_, count)) => *count = (*count).max(cell.databases),
                None => collections.push((cell.mode, cell.databases)),
            }
        }
        Self {
            handle,
            factory,
            options,
            execution,
            name_prefix: format!("perfbenchmixed{invocation}r{run}"),
            collections,
            forkable: None,
            checkpoint: None,
        }
    }

    /// Returns a seeded database for the cell, which no other cell uses.
    fn for_cell(&mut self, dimension: CellDimension) -> Result<CellStart, Box<dyn Error>> {
        let mode = dimension.mode;
        if let Some((chain, storage)) = &self.checkpoint
            && *chain == Chain::of(dimension)
            && let Some((_, _, seed)) = &self.forkable
        {
            return Ok(CellStart::fork_of(storage, seed));
        }
        self.checkpoint = None;
        if let Some((seeded, storage, seed)) = &self.forkable
            && *seeded == mode
        {
            return Ok(CellStart::fork_of(storage, seed));
        }
        if let Some(storage) = self.factory.forkable() {
            self.forkable = None;
            let collections = self
                .collections
                .iter()
                .find(|(seeded, _)| *seeded == mode)
                .map_or(dimension.databases, |&(_, count)| count);
            let name = format!("{}{}", self.name_prefix, mode.label());
            let seed = self.seed(storage.backend(), name, collections, mode)?;
            let start = CellStart::fork_of(&storage, &seed);
            self.forkable = Some((mode, storage, seed));
            return Ok(start);
        }
        let name = format!(
            "{}{}a{}d{}l{}w{}",
            self.name_prefix,
            mode.label(),
            dimension.affinity_pct,
            dimension.databases,
            dimension.database_limit,
            dimension.workers_per_shape,
        );
        let seed = self.seed(self.factory.backend(), name, dimension.databases, mode)?;
        Ok(CellStart {
            seed,
            storage: None,
        })
    }

    /// Makes the checkpoint of `dimension` the start of the next cell of its
    /// chain. Does nothing when the storage cannot fork.
    fn record_checkpoint(&mut self, dimension: CellDimension, checkpoint: Option<ForkableBackend>) {
        self.checkpoint = checkpoint.map(|storage| (Chain::of(dimension), storage));
    }

    fn seed(
        &self,
        backend: Arc<dyn Backend>,
        name: String,
        collections: usize,
        mode: Mode,
    ) -> Result<Seed, Box<dyn Error>> {
        Seed::create(
            self.handle,
            backend,
            name,
            &setup::SeedConfig {
                collections,
                pool_size: mode.pool_size(self.options),
                quiet_period: self.options.quiet_period,
                timeout: self.options.seed_timeout,
                drain_timeout: self.execution.drain_timeout,
            },
        )
    }
}

/// Runs one cell and returns its result with the checkpoint taken after its
/// warm-up.
fn run_cell(
    handle: &Handle,
    start: &CellStart,
    options: &Options,
    execution: Execution,
    dimension: CellDimension,
) -> Result<(CellResult, Option<ForkableBackend>), Box<dyn Error>> {
    // Collection binding and its record reads are setup, not transaction work.
    let prepared = start.seed.open_cell(handle, dimension.databases)?;
    let plans = workload::plans(
        dimension.workers_per_shape,
        prepared.databases().len(),
        options.max_duration,
    );

    let stop = Arc::new(AtomicBool::new(false));
    let target = options.window_commits * result::THROUGHPUT_WINDOWS as u64;
    let key_selection = dimension.mode.key_selection(options);
    let ctx = workload::WorkerCtx::new(
        stop.clone(),
        dimension.mode.pool_size(options),
        options.multi_keys,
        dimension.affinity_pct,
        key_selection,
    );
    let (warmup, checkpoint, drive, active, run, deadline) = handle.block_on(async {
        let handles =
            workload::spawn_workers(prepared.databases(), prepared.collections(), &plans, &ctx);
        // The workload runs before the measurement, so that the splits and
        // merges that it causes happen before the counter baselines.
        let warmup = prepared
            .warm_up(
                &stop,
                options.warmup_minimum,
                options.quiet_period,
                options.warmup_timeout,
            )
            .await;
        // The checkpoint, the baselines and the samples then start with no
        // transaction in flight.
        ctx.pause().close_and_wait(handles.len(), &stop).await;
        let checkpoint = start.checkpoint();
        let active = prepared.begin_measurement();
        workload::start_measurement(&plans);
        ctx.pause().open();
        let drive = workload::drive_to_significance(
            &plans,
            &stop,
            target,
            options.duration,
            options.max_duration,
        )
        .await;
        // The windows end when the workers stop, so the drain of the
        // transactions in flight does not lower the last window.
        workload::end_measurement(&plans);
        let deadline = tokio::time::Instant::now() + execution.drain_timeout;
        let run = join_tasks_until(handles, deadline).await;
        (warmup, checkpoint, drive, active, run, deadline)
    });

    let completed = active.teardown(handle, deadline, run)?;
    let cell_converged = match drive {
        DriveOutcome::Converged => true,
        DriveOutcome::Capped => false,
        DriveOutcome::WorkerStopped => {
            return Err("benchmark worker stopped without reporting its error".into());
        }
    };
    eprintln!("{}", result::cell_warmed_up(&warmup));
    if !warmup.quiet {
        eprintln!(
            "{}",
            result::warmup_capped(dimension, options.warmup_timeout)
        );
    }
    if !cell_converged {
        eprintln!("{}", result::cell_capped(dimension, options.window_commits));
    }

    let result = CellResult::summarize(
        CellMetadata::new(
            dimension.mode.label(),
            dimension.affinity_pct,
            dimension.database_limit,
            completed.databases,
            dimension.workers_per_shape,
            completed.setup_splits,
            completed.split_settle_elapsed,
        )
        .with_key_selection(key_selection)
        .with_warmup(warmup),
        workload::measurements(&plans),
        &completed.deltas,
        target,
    );
    Ok((result, checkpoint))
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use clap::Parser;
    use glassdb::middleware::{DelayOptions, ProviderLatencyProfile, RateLimit, WriteRateLimits};

    use super::*;

    #[derive(Parser)]
    struct TestCli {
        #[command(flatten)]
        options: Options,
    }

    #[test]
    fn a_checkpoint_carries_over_only_to_cells_that_differ_in_workers() -> Result<(), Box<dyn Error>>
    {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?;
        let handle = runtime.handle();
        let factory = backend::Factory::Memory(DelayOptions {
            latency: ProviderLatencyProfile::zero(),
            rate_limits: WriteRateLimits {
                same_obj_write_ps: RateLimit::Unlimited,
                same_obj_write_retry_delay: Duration::ZERO,
                prefix_read_ps: RateLimit::Unlimited,
                prefix_write_ps: RateLimit::Unlimited,
                prefix_depth: 0,
            },
        });
        let options = TestCli::try_parse_from([
            "perfbench",
            "--modes=lo,hi",
            "--affinities=0,100",
            "--databases=2",
            "--workers-per-shape=1,2",
            "--num-keys=16",
            "--hot-keys=4",
            "--multi-keys=2",
            "--quiet-period=20ms",
            "--seed-timeout=5s",
        ])?
        .options;
        // Cells come in this order: `lo` with 0% affinity and 1 and 2 workers,
        // `lo` with 100% affinity and 1 and 2 workers, then the same for `hi`.
        let dimensions = options.cell_dimensions()?;
        let execution = Execution {
            runs: 1,
            run_cooldown: Duration::ZERO,
            drain_timeout: Duration::from_secs(5),
        };
        let mut seeds = Seeds::new(handle, &factory, &options, execution, &dimensions, 0, 1);

        // The first `lo` cell opens one collection, but the seed has the two
        // collections of the largest `lo` cell.
        let first_start = seeds.for_cell(dimensions[0])?;
        let first = first_start.seed.open_cell(handle, 2)?;
        let collection = first.collections()[0][1].clone();
        let keys: Vec<_> = handle.block_on(collection.iter_keys())?.collect();
        assert_eq!(keys.len(), 16);

        // The checkpoint holds the writes before it, and not the writes after.
        handle.block_on(collection.write(&key_bytes(0), b"first"))?;
        seeds.record_checkpoint(dimensions[0], first_start.checkpoint());
        handle.block_on(collection.write(&key_bytes(1), b"after"))?;

        // A cell that differs only in its workers starts from the checkpoint.
        let second_start = seeds.for_cell(dimensions[1])?;
        let second = second_start.seed.open_cell(handle, 2)?;
        let second_collection = second.collections()[0][1].clone();
        let read = handle.block_on(second_collection.read(&key_bytes(0)))?;
        assert_eq!(read, Some(b"first".to_vec()));
        let read = handle.block_on(second_collection.read(&key_bytes(1)))?;
        assert_eq!(read, Some(value()));
        seeds.record_checkpoint(dimensions[1], second_start.checkpoint());

        // Another affinity starts again from the seed.
        let other_affinity_start = seeds.for_cell(dimensions[2])?;
        let other_affinity = other_affinity_start.seed.open_cell(handle, 2)?;
        let read = handle.block_on(other_affinity.collections()[0][1].read(&key_bytes(0)))?;
        assert_eq!(read, Some(value()));
        seeds.record_checkpoint(dimensions[2], other_affinity_start.checkpoint());

        // The mode `hi` starts from its own seed, with its own pool, and
        // not from the checkpoint of the cell before it.
        let hot_start = seeds.for_cell(dimensions[4])?;
        let hot = hot_start.seed.open_cell(handle, 1)?;
        let keys: Vec<_> = handle
            .block_on(hot.collections()[0][0].iter_keys())?
            .collect();
        assert_eq!(keys.len(), 4);

        for cell in [first, second, other_affinity, hot] {
            cell.begin_measurement().teardown(
                handle,
                tokio::time::Instant::now() + Duration::from_secs(5),
                Ok(()),
            )?;
        }
        Ok(())
    }
}
