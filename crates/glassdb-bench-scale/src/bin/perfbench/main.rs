//! Database-level GlassDB performance experiments.
//!
//! One runner runs every experiment. An experiment is a TOML file over layers
//! of shared configuration; the README of this crate explains it.

mod backend;
mod progress;
mod runner;
mod scenarios;
mod time;

#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::error::Error;
use std::fs::File;
use std::io::{self, BufWriter, IsTerminal, Write};
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, ValueEnum};
use glassdb::ProtocolTiming;
use serde::Serialize;
use time::TimeMode;

const SCHEMA_VERSION: u32 = 3;

/// The protocol timing of every database instance.
///
/// All instances run on one machine and read one clock, so they need no
/// clock skew allowance, and GC can check the records of a load 30 s sooner.
/// The pending timeout keeps the production value, so that leases, refreshes,
/// and wounds behave as in production.
const TIMING: ProtocolTiming = ProtocolTiming::new(Duration::from_secs(15), Duration::ZERO);

#[derive(Parser)]
#[command(about = "Run GlassDB database-level performance experiments")]
struct Cli {
    #[command(flatten)]
    backend: backend::Options,
    /// A configuration file, or a directory of them, for every experiment. It
    /// can be given more than once, and a later file changes the values of an
    /// earlier one. The built-in defaults are always the first.
    #[arg(long = "config")]
    configs: Vec<PathBuf>,
    /// An experiment file, or a directory of them. It can be given more than
    /// once, and the experiments run one after the other.
    #[arg(long = "experiment")]
    experiments: Vec<PathBuf>,
    /// Give a parameter of each experiment a fixed value, as `PATH=VALUE` with
    /// a TOML value. It removes the axis that sets `PATH`. It can be given more
    /// than once, and the last one wins.
    #[arg(long = "set")]
    set: Vec<String>,
    /// Keep only one value of an axis, as `AXIS=LABEL`, in each experiment
    /// that has the axis.
    #[arg(long = "only")]
    only: Vec<String>,
    /// How model time advances. `virtual` needs `--backend=memory` and no
    /// `--delay-scale`, and the watchdogs `--drain-timeout` and the limit of
    /// the full GCs then use virtual time too.
    #[arg(long, value_enum, default_value = "real")]
    time: TimeMode,
    /// Repeat the complete experiment.
    #[arg(long, default_value_t = 1)]
    runs: usize,
    /// Real wall time between repeated runs.
    #[arg(long, default_value = "0s", value_parser = glassdb_bench_scale::parse_duration)]
    run_cooldown: Duration,
    /// Maximum time for in-flight work and Database shutdown.
    #[arg(long, default_value = "30s", value_parser = glassdb_bench_scale::parse_duration)]
    drain_timeout: Duration,
    /// Show a progress bar on stderr, with the estimated time left. Without a
    /// value, it is always shown. By default, it is shown only on a terminal.
    #[arg(
        long,
        value_enum,
        default_value = "auto",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "always"
    )]
    progress: ProgressMode,
    /// Write JSON to this path instead of stdout.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Clone, Copy, ValueEnum)]
enum ProgressMode {
    /// Show the bar if stderr is a terminal.
    Auto,
    Always,
    Never,
}

impl ProgressMode {
    fn enabled(self) -> bool {
        match self {
            Self::Auto => io::stderr().is_terminal(),
            Self::Always => true,
            Self::Never => false,
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report {
    schema_version: u32,
    backend: String,
    time_mode: &'static str,
    model_time_speedup: f64,
    latency_jitter: bool,
    /// The command-line options of the backend and of the run.
    options: serde_json::Value,
    /// Each experiment as it ran, with its runs.
    experiments: Vec<serde_json::Value>,
}

#[derive(Clone, Copy)]
struct Execution {
    runs: usize,
    run_cooldown: Duration,
    drain_timeout: Duration,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    if cli.runs == 0 {
        return Err("--runs must be greater than zero".into());
    }
    let selection = runner::config::Selection {
        configs: cli.configs.clone(),
        experiments: cli.experiments.clone(),
        set: cli.set.clone(),
        only: cli.only.clone(),
    };
    let resolved = selection.resolve()?;
    let model_time_speedup = cli.time.install(&cli.backend)?;
    let runtime = cli.time.runtime()?;
    let factory = runtime.block_on(cli.backend.initialize())?;
    let execution = Execution {
        runs: cli.runs,
        run_cooldown: cli.run_cooldown,
        drain_timeout: cli.drain_timeout,
    };
    let mut cells = 0;
    for experiment in &resolved {
        cells += runner::plan(experiment)?.0.len() * cli.runs;
    }
    if cli.progress.enabled() {
        progress::enable(cells);
    }
    let mut experiments = Vec::with_capacity(resolved.len());
    let outcome = run_all(&runtime, &factory, &resolved, execution, &mut experiments);
    progress::finish();
    if outcome.is_err() {
        if experiments.is_empty() {
            return outcome;
        }
        // A run of all experiments can take hours, so the experiments that
        // completed before the error are kept.
        crate::progress::log!(
            "the result has only the {} experiments that completed",
            experiments.len()
        );
    }
    let report = Report {
        schema_version: SCHEMA_VERSION,
        backend: cli.backend.label().to_string(),
        time_mode: cli.time.label(),
        model_time_speedup,
        latency_jitter: cli.backend.latency_jitter,
        options: cli.backend.describe(cli.time),
        experiments,
    };
    write_json(cli.output, &serde_json::to_value(report)?)?;
    outcome
}

/// Runs each experiment, and adds its description with its runs to
/// `experiments`, until one fails.
fn run_all(
    runtime: &tokio::runtime::Runtime,
    factory: &backend::Factory,
    resolved: &[runner::config::Resolved],
    execution: Execution,
    experiments: &mut Vec<serde_json::Value>,
) -> Result<(), Box<dyn Error>> {
    for experiment in resolved {
        let runs = runner::run(runtime, factory, experiment, execution)
            .map_err(|e| format!("{}: {e}", experiment.name))?;
        let mut described = experiment.describe();
        described["runs"] = serde_json::to_value(runs)?;
        experiments.push(described);
    }
    Ok(())
}

fn write_json(path: Option<PathBuf>, value: &serde_json::Value) -> Result<(), Box<dyn Error>> {
    let mut writer: Box<dyn Write> = match path {
        Some(path) => Box::new(BufWriter::new(File::create(path)?)),
        None => Box::new(BufWriter::new(io::stdout().lock())),
    };
    serde_json::to_writer_pretty(&mut writer, value)?;
    writeln!(writer)?;
    Ok(())
}

async fn cooldown(execution: Execution, run: usize) {
    if run > 1 && !execution.run_cooldown.is_zero() {
        tokio::time::sleep(execution.run_cooldown).await;
    }
}
