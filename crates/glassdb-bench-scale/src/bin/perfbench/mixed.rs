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
pub(super) use options::Options;
use options::{CellDimension, Mode};
pub(super) use result::RunResult;
use result::{CellMetadata, CellResult};
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
    let plans = workload::plans(
        options.workers_per_shape,
        prepared.databases().len(),
        options.max_duration,
    );

    // Collection binding and its record reads are setup, not transaction work.
    // Bracket stats only after every client has opened every possible target.
    let active = prepared.begin_measurement();
    workload::start_measurement(&plans);

    let stop = Arc::new(AtomicBool::new(false));
    let target = samples_for_rel_ci(options.target_ci);
    let ctx = workload::WorkerCtx::new(stop.clone(), pool_size, options.multi_keys, affinity_pct);
    let (drive, run, deadline) = handle.block_on(async {
        let handles =
            workload::spawn_workers(active.databases(), active.collections(), &plans, &ctx);
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
        (drive, run, deadline)
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
    if !cell_converged {
        eprintln!(
            "{}",
            result::cell_capped(mode.label(), affinity_pct, options.target_ci)
        );
    }

    Ok(CellResult::summarize(
        CellMetadata::new(
            mode.label(),
            affinity_pct,
            completed.databases,
            completed.setup_splits,
            completed.split_settle_elapsed,
        ),
        workload::measurements(&plans),
        &completed.deltas,
        target,
    ))
}
