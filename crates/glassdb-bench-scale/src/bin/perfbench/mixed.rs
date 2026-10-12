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
mod workload;

use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::SystemTime;

use tokio::runtime::Handle;

use glassdb_backend::Backend;
use glassdb_bench_scale::bench::samples_for_rel_ci;
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
            let seed = seeds.for_cell(dimension)?;
            cells.push(run_cell(handle, &seed, options, execution, dimension)?);
        }
        runs.push(RunResult::new(run, cells));
    }
    Ok(runs)
}

/// Provides each cell of a run with its own seeded database.
///
/// A storage that can fork is seeded once for each mode of the run, with the
/// largest collection count of that mode, and every cell gets its own fork.
/// Any other storage is seeded again for each cell, in a new database.
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
        }
    }

    /// Returns a seeded database for the cell, which no other cell uses.
    fn for_cell(&mut self, dimension: CellDimension) -> Result<Seed, Box<dyn Error>> {
        let mode = dimension.mode;
        if let Some((seeded, storage, seed)) = &self.forkable
            && *seeded == mode
        {
            return Ok(seed.with_backend(storage.fork().backend()));
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
            let cell = seed.with_backend(storage.fork().backend());
            self.forkable = Some((mode, storage, seed));
            return Ok(cell);
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
        self.seed(self.factory.backend(), name, dimension.databases, mode)
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

fn run_cell(
    handle: &Handle,
    seed: &Seed,
    options: &Options,
    execution: Execution,
    dimension: CellDimension,
) -> Result<CellResult, Box<dyn Error>> {
    // Collection binding and its record reads are setup, not transaction work.
    let prepared = seed.open_cell(handle, dimension.databases)?;
    let plans = workload::plans(
        dimension.workers_per_shape,
        prepared.databases().len(),
        options.max_duration,
    );

    let stop = Arc::new(AtomicBool::new(false));
    let target = samples_for_rel_ci(options.target_ci);
    let key_selection = dimension.mode.key_selection(options);
    let ctx = workload::WorkerCtx::new(
        stop.clone(),
        dimension.mode.pool_size(options),
        options.multi_keys,
        dimension.affinity_pct,
        key_selection,
    );
    let (warmup, drive, active, run, deadline) = handle.block_on(async {
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
        // The baselines and the samples then start with no transaction in
        // flight.
        ctx.pause().close_and_wait(handles.len(), &stop).await;
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
        let deadline = tokio::time::Instant::now() + execution.drain_timeout;
        let run = join_tasks_until(handles, deadline).await;
        (warmup, drive, active, run, deadline)
    });

    workload::end_measurement(&plans);
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
        eprintln!("{}", result::cell_capped(dimension, options.target_ci));
    }

    Ok(CellResult::summarize(
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
    ))
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
    fn each_cell_gets_an_own_copy_of_one_seed_per_mode() -> Result<(), Box<dyn Error>> {
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
            "--affinities=100",
            "--databases=1,2",
            "--workers-per-shape=2",
            "--num-keys=16",
            "--hot-keys=4",
            "--multi-keys=2",
            "--quiet-period=20ms",
            "--seed-timeout=5s",
        ])?
        .options;
        let dimensions = options.cell_dimensions()?;
        let execution = Execution {
            runs: 1,
            run_cooldown: Duration::ZERO,
            drain_timeout: Duration::from_secs(5),
        };
        let mut seeds = Seeds::new(handle, &factory, &options, execution, &dimensions, 0, 1);

        // The first `lo` cell opens one collection, but the seed has the two
        // collections of the largest `lo` cell.
        let first = seeds.for_cell(dimensions[0])?.open_cell(handle, 2)?;
        let collection = first.collections()[0][1].clone();
        let keys: Vec<_> = handle.block_on(collection.iter_keys())?.collect();
        assert_eq!(keys.len(), 16);
        handle.block_on(collection.write(&key_bytes(0), b"first"))?;

        // A later cell of the same mode does not see the writes of earlier
        // cells.
        let second = seeds.for_cell(dimensions[1])?.open_cell(handle, 2)?;
        let read = handle.block_on(second.collections()[0][1].read(&key_bytes(0)))?;
        assert_eq!(read, Some(value()));
        handle.block_on(second.collections()[0][1].write(&key_bytes(0), b"second"))?;
        let third = seeds.for_cell(dimensions[1])?.open_cell(handle, 2)?;
        let read = handle.block_on(third.collections()[0][1].read(&key_bytes(0)))?;
        assert_eq!(read, Some(value()));

        // The `hi` mode has its own seed with its own pool.
        let hot = seeds.for_cell(dimensions[2])?.open_cell(handle, 1)?;
        let keys: Vec<_> = handle
            .block_on(hot.collections()[0][0].iter_keys())?
            .collect();
        assert_eq!(keys.len(), 4);

        for cell in [first, second, third, hot] {
            cell.begin_measurement().teardown(
                handle,
                tokio::time::Instant::now() + Duration::from_secs(5),
                Ok(()),
            )?;
        }
        Ok(())
    }
}
