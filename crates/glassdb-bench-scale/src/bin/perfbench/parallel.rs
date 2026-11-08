//! A run of the chains of cells in parallel, each in its own process.
//!
//! Model time is state of the process, so each chain runs in a new process of
//! this binary, with the same arguments and `--chain`. The process writes its
//! events to stdout as JSON lines: its log, its progress, and at the end its
//! cells or its error. This process shows the log and the progress, and puts
//! the cells in the order of a run of all cells.

use std::error::Error;
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Command, Stdio};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::progress;
use crate::runner::result::CellResult;
use crate::runner::{self, Chain, config::Resolved};

/// One line that the process of a chain writes to stdout.
#[derive(Serialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub(crate) enum Event<'a> {
    /// A line of the log. The first line of a cell names it.
    Log {
        text: &'a str,
    },
    CellStart,
    /// The part of the current cell that is done, from 0 to 1.
    Progress {
        fraction: f64,
    },
    CellEnd,
    /// The cells of the chain. It is the last event.
    Cells {
        cells: &'a [CellResult],
    },
    /// The error of the chain, with the name of its experiment. It is the
    /// last event.
    Error {
        message: &'a str,
    },
}

/// An [`Event`], as this process reads it.
#[derive(Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
enum ReadEvent {
    Log { text: String },
    CellStart,
    Progress { fraction: f64 },
    CellEnd,
    Cells { cells: Vec<Value> },
    Error { message: String },
}

/// Writes `event` as one line to stdout.
pub(crate) fn emit(event: &Event<'_>) -> std::io::Result<()> {
    let mut stdout = std::io::stdout().lock();
    serde_json::to_writer(&mut stdout, event)?;
    writeln!(stdout)?;
    stdout.flush()
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
                    if index >= jobs.len() {
                        return;
                    }
                    let output = run_job(index);
                    if output.is_err() {
                        failed.store(true, Ordering::SeqCst);
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

/// Runs job `index` in a new process, shows its log and progress while it
/// runs, and returns its cells.
fn run_job(index: usize) -> JobOutput {
    let mut child = program()
        .and_then(|program| {
            let mut command = Command::new(program);
            // Process listings and usage messages show the name of the binary,
            // not `/proc/self/exe`.
            #[cfg(unix)]
            if let Some(name) = std::env::args_os().next() {
                std::os::unix::process::CommandExt::arg0(&mut command, name);
            }
            command
                .args(std::env::args_os().skip(1))
                .arg(format!("--chain={index}"))
                .stdin(Stdio::null())
                .stdout(Stdio::piped())
                .stderr(Stdio::piped())
                .spawn()
        })
        .map_err(|e| format!("cannot start the process of chain {index}: {e}"))?;
    let stdout = child.stdout.take().expect("stdout is piped");
    let mut stderr = child.stderr.take().expect("stderr is piped");
    // A process that writes much to stderr would block if nothing read it.
    let errors = std::thread::spawn(move || {
        let mut text = String::new();
        let _ = stderr.read_to_string(&mut text);
        text
    });
    progress::chain_started(index);
    let mut output = None;
    // The lines of a cell print together, when the cell ends.
    let mut block = Vec::new();
    for line in BufReader::new(stdout).lines() {
        let Ok(line) = line else { break };
        match serde_json::from_str(&line) {
            Ok(ReadEvent::Log { text }) => block.push(text),
            Ok(ReadEvent::CellStart) => progress::chain_progress(index, 0.0),
            Ok(ReadEvent::Progress { fraction }) => progress::chain_progress(index, fraction),
            Ok(ReadEvent::CellEnd) => {
                progress::lines(&std::mem::take(&mut block));
                progress::chain_cell_done(index);
            }
            Ok(ReadEvent::Cells { cells }) => output = Some(Ok(cells)),
            Ok(ReadEvent::Error { message }) => output = Some(Err(message)),
            // Not an event, for example a line of a dependency.
            Err(_) => block.push(line),
        }
    }
    let status = child
        .wait()
        .map_err(|e| format!("the process of chain {index}: {e}"))?;
    progress::chain_finished(index);
    // The lines of a cell that did not end, as the cell that failed.
    block.extend(
        errors
            .join()
            .unwrap_or_default()
            .lines()
            .map(str::to_string),
    );
    progress::lines(&block);
    match output {
        Some(Ok(cells)) if status.success() => Ok(cells),
        Some(Err(message)) => Err(message),
        // A process that panics or is killed writes no last event.
        _ => Err(format!("the process of chain {index} failed: {status}")),
    }
}

/// Returns the program of the chain processes: the binary of this process.
/// On Linux, `/proc/self/exe` stays valid when a build replaces the file
/// during a run, so that a run does not mix two binaries.
fn program() -> std::io::Result<std::path::PathBuf> {
    if cfg!(target_os = "linux") {
        Ok("/proc/self/exe".into())
    } else {
        std::env::current_exe()
    }
}
