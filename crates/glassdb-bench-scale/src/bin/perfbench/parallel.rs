//! A run of the chains of cells in parallel, each in its own process.
//!
//! Model time is state of the process, so each chain runs in a new process of
//! this binary, with the same arguments and `--chain`. The process writes the
//! cells of its chain to stdout, and this process puts them in the order of a
//! run of all cells.

use std::error::Error;
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::progress;
use crate::runner::result::CellResult;
use crate::runner::{self, Chain, config::Resolved};

/// What the process of a chain writes to stdout.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(crate) enum ChainOutput {
    Cells(Vec<CellResult>),
    /// The error of the chain, with the name of its experiment.
    Error(String),
}

/// A [`ChainOutput`], as the parent reads it.
#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
enum ReadOutput {
    Cells(Vec<Value>),
    Error(String),
}

/// The cells of a chain, or the error of its process.
type JobOutput = Result<Vec<Value>, String>;

/// One chain of one experiment.
pub(crate) struct Job {
    /// The index of the experiment in the selection.
    pub(crate) experiment: usize,
    pub(crate) chain: Chain,
}

/// Returns the chains of all experiments, in the order of a run of all cells.
/// The index of a job is its value of `--chain`.
pub(crate) fn chains(resolved: &[Resolved], runs: usize) -> Result<Vec<Job>, Box<dyn Error>> {
    let mut jobs = Vec::new();
    for (experiment, resolved) in resolved.iter().enumerate() {
        let chains =
            runner::chains(resolved, runs).map_err(|e| format!("{}: {e}", resolved.name))?;
        jobs.extend(chains.into_iter().map(|chain| Job { experiment, chain }));
    }
    Ok(jobs)
}

/// Runs `jobs` at most `parallel` at a time, and adds the description of each
/// experiment with its runs to `experiments`, until one fails.
///
/// After a job fails, no new job starts. The jobs before it in the order
/// already started, so the experiments before the one that failed complete.
pub(crate) fn run_all(
    resolved: &[Resolved],
    jobs: &[Job],
    parallel: usize,
    runs: usize,
    experiments: &mut Vec<Value>,
) -> Result<(), Box<dyn Error>> {
    let next = AtomicUsize::new(0);
    let failed = AtomicBool::new(false);
    let outputs: Mutex<Vec<Option<JobOutput>>> =
        Mutex::new((0..jobs.len()).map(|_| None).collect());
    std::thread::scope(|scope| {
        for _ in 0..parallel.min(jobs.len()) {
            scope.spawn(|| {
                while !failed.load(Ordering::SeqCst) {
                    let index = next.fetch_add(1, Ordering::SeqCst);
                    let Some(job) = jobs.get(index) else {
                        return;
                    };
                    let output = run_job(index);
                    match &output {
                        Ok(_) => progress::finish_cells(job.chain.cells.len()),
                        Err(_) => failed.store(true, Ordering::SeqCst),
                    }
                    outputs.lock().unwrap()[index] = Some(output);
                }
            });
        }
    });
    let mut outputs = outputs.into_inner().unwrap().into_iter();
    let mut jobs = jobs.iter().peekable();
    for (index, experiment) in resolved.iter().enumerate() {
        let mut cells: Vec<Vec<Value>> = vec![Vec::new(); runs];
        while let Some(job) = jobs.next_if(|job| job.experiment == index) {
            match outputs.next().flatten() {
                Some(Ok(chain)) => cells[job.chain.run - 1].extend(chain),
                // The error of a chain names its experiment.
                Some(Err(error)) => return Err(error.into()),
                // A job that did not start comes after a job that failed.
                None => return Err(format!("{}: a chain before it failed", experiment.name).into()),
            }
        }
        let mut described = experiment.describe();
        described["runs"] = Value::Array(
            cells
                .into_iter()
                .enumerate()
                .map(|(run, cells)| json!({ "run": run + 1, "cells": cells }))
                .collect(),
        );
        experiments.push(described);
    }
    Ok(())
}

/// Runs job `index` in a new process, prints its log when it ends, and
/// returns its cells.
fn run_job(index: usize) -> JobOutput {
    let output = std::env::current_exe()
        .and_then(|program| {
            Command::new(program)
                .args(std::env::args_os().skip(1))
                .arg(format!("--chain={index}"))
                .stdin(Stdio::null())
                .output()
        })
        .map_err(|e| format!("cannot start the process of chain {index}: {e}"))?;
    let log = String::from_utf8_lossy(&output.stderr);
    // The lines of one chain stay together.
    for line in log.lines() {
        progress::log!("{line}");
    }
    match serde_json::from_slice(&output.stdout) {
        Ok(ReadOutput::Cells(cells)) if output.status.success() => Ok(cells),
        Ok(ReadOutput::Error(error)) => Err(error),
        // A process that panics or is killed writes no output.
        _ => Err(format!(
            "the process of chain {index} failed: {}",
            output.status
        )),
    }
}
