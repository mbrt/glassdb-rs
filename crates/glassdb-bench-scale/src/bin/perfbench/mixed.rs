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
//! - client-`Database` limits and workers per shape. Each cell opens no more
//!   Databases than it has workers per shape, so every open client is active.
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
//! The measurement clients open with each topology policy of `--policies`.
//! All shapes can run for `--warmup` before measurement, so that the policy
//! changes the tree before it is measured.
//!
//! A counterfactual compares the same workload on different trees. The setup
//! Database can seed with a leaf entry limit, `--seed-leaf-entries`, and no
//! underfull merges, so that the tree has more leaves than the soft caps give.
//! The measurement clients keep the default node size policy, so only the
//! `fixed` policy keeps such a tree as seeded. `--key-layouts` gives the
//! single-key shapes of each Database their own keys, for example the keys of
//! one leaf. `--shadow-policies` and `--trace-windows` report what the
//! measurement clients measured in each window, and what other policies would
//! decide on it, without a change of the tree.
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
use std::time::{Duration, SystemTime};

use tokio::runtime::Handle;

use glassdb::{Database, Error as GError};
use glassdb_backend::Backend;
use glassdb_bench_scale::bench::samples_for_rel_ci;
use glassdb_bench_scale::run::{DriveOutcome, drive_to_significance, join_tasks_until};

use super::backend;
use super::policy::{PolicySpec, ShadowTally};
use super::{Execution, cooldown};
use options::CellDimension;
pub(super) use options::Options;
pub(super) use result::RunResult;
use result::{CellMetadata, CellResult, Restructure};

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
        let mut cells = Vec::new();
        for &dimension in &dimensions {
            eprintln!("{}", result::cell_started(run, dimension));
            let database_name = format!(
                "perfbenchmixed{invocation}r{run}{}a{}d{}l{}w{}s{}k{}p{}",
                dimension.mode.label(),
                dimension.affinity_pct,
                dimension.databases,
                dimension.database_limit,
                dimension.workers_per_shape,
                dimension.seed_leaf_entries.unwrap_or(0),
                dimension.key_layout.label(),
                dimension.policy_index,
            );
            cells.push(run_cell(
                handle,
                factory.backend(),
                &database_name,
                options,
                execution,
                dimension,
            )?);
        }
        runs.push(RunResult::new(run, cells));
    }
    Ok(runs)
}

fn run_cell(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    database_name: &str,
    options: &Options,
    execution: Execution,
    dimension: CellDimension,
) -> Result<CellResult, Box<dyn Error>> {
    let pool_size = dimension.mode.pool_size(options);
    let shapes = options.shapes()?;
    let tally = (options.observes_windows() && dimension.policy != PolicySpec::Engine).then(|| {
        Arc::new(ShadowTally::new(
            options.shadow_policies.iter().map(|policy| policy.label()),
            options.trace_windows,
        ))
    });
    let prepared = setup::prepare_cell(
        handle,
        backend,
        database_name,
        setup::CellConfig {
            databases: dimension.databases,
            policy: dimension.policy,
            shadows: options.shadow_policies.clone(),
            tally: tally.clone(),
            pool_size,
            seed_leaf_entries: dimension.seed_leaf_entries,
            split_quiet: options.split_quiet,
            split_settle_timeout: options.split_settle_timeout,
            drain_timeout: execution.drain_timeout,
        },
    )?;
    let ctx = |stop: &Arc<AtomicBool>| {
        workload::WorkerCtx::new(
            stop.clone(),
            pool_size,
            options.multi_keys,
            dimension.affinity_pct,
            dimension.key_layout,
            dimension.databases,
        )
    };
    let opened: Vec<_> = prepared.databases().iter().map(Database::stats).collect();
    if !options.warmup.is_zero() {
        let plans = workload::plans(
            &shapes,
            dimension.workers_per_shape,
            prepared.databases().len(),
            options.warmup,
        );
        let stop = Arc::new(AtomicBool::new(false));
        handle.block_on(warm_up(
            prepared.databases(),
            prepared.collections(),
            &plans,
            &ctx(&stop),
            options.warmup,
            execution,
        ))?;
    }
    let mut restructure = Restructure::default();
    for (database, opened) in prepared.databases().iter().zip(opened) {
        restructure.add_warmup(database.stats() - opened);
    }
    let plans = workload::plans(
        &shapes,
        dimension.workers_per_shape,
        prepared.databases().len(),
        options.max_duration,
    );

    // Collection binding and its record reads are setup, not transaction work.
    // Bracket stats only after every client has opened every possible target.
    let active = prepared.begin_measurement();
    if let Some(tally) = &tally {
        tally.take();
    }
    workload::start_measurement(&plans);

    let stop = Arc::new(AtomicBool::new(false));
    let target = samples_for_rel_ci(options.target_ci);
    let ctx = ctx(&stop);
    let (drive, run, deadline) = handle.block_on(async {
        let handles =
            workload::spawn_workers(active.databases(), active.collections(), &plans, &ctx);
        let drive = drive_to_significance(
            &workload::benches(&plans),
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

    workload::end_measurement(&plans);
    let shadow = tally.as_ref().map(|tally| tally.take());
    let completed = active.teardown(handle, deadline, run)?;
    let cell_converged = match drive {
        DriveOutcome::Converged => true,
        DriveOutcome::Capped => false,
        DriveOutcome::WorkerStopped => {
            return Err("benchmark worker stopped without reporting its error".into());
        }
    };
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
        .with_layout(dimension.seed_leaf_entries, dimension.key_layout.label())
        .with_policy(dimension.policy.label(), restructure)
        .with_shadow(shadow),
        workload::measurements(&plans),
        &completed.deltas,
        target,
    ))
}

/// Runs every shape for `duration` without measuring it.
async fn warm_up(
    databases: &[Database],
    collections: &[Arc<[glassdb::Collection]>],
    plans: &[workload::ShapePlan],
    ctx: &workload::WorkerCtx,
    duration: Duration,
    execution: Execution,
) -> Result<(), GError> {
    // The benches never start, so they never finish, and only the stop flag
    // of `ctx` ends the workers.
    let handles = workload::spawn_workers(databases, collections, plans, ctx);
    tokio::time::sleep(duration).await;
    ctx.stop();
    join_tasks_until(
        handles,
        tokio::time::Instant::now() + execution.drain_timeout,
    )
    .await
}
