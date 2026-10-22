//! Database-level GlassDB performance experiments.
//!
//! One runner runs every experiment. An experiment is a named entry of a
//! layered TOML configuration; the README of this crate explains it.

mod backend;
mod runner;
mod scenarios;

#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::error::Error;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::time::Duration;

use clap::Parser;
use glassdb::ProtocolTiming;
use glassdb_concurr::rt;
use serde::Serialize;

const SCHEMA_VERSION: u32 = 2;

/// The protocol timing of the database instances, unless an experiment asks
/// for the engine default.
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
    /// A configuration file. It can be given more than once, and a later file
    /// changes the values of an earlier one. The built-in configuration is
    /// always the first.
    #[arg(long = "config")]
    configs: Vec<PathBuf>,
    /// The experiment to run.
    #[arg(long, default_value = "mixed")]
    experiment: String,
    /// Give a parameter of the experiment a fixed value, as `PATH=VALUE` with a
    /// TOML value. It removes the axis that sets `PATH`. It can be given more
    /// than once, and the last one wins.
    #[arg(long = "set")]
    set: Vec<String>,
    /// Keep only one value of an axis, as `AXIS=LABEL`.
    #[arg(long = "only")]
    only: Vec<String>,
    /// List the experiments of the configuration and exit.
    #[arg(long)]
    list: bool,
    /// Repeat the complete experiment.
    #[arg(long, default_value_t = 1)]
    runs: usize,
    /// Real wall time between repeated runs.
    #[arg(long, default_value = "0s", value_parser = glassdb_bench_scale::parse_duration)]
    run_cooldown: Duration,
    /// Maximum time for in-flight work and Database shutdown.
    #[arg(long, default_value = "30s", value_parser = glassdb_bench_scale::parse_duration)]
    drain_timeout: Duration,
    /// Write JSON to this path instead of stdout.
    #[arg(long)]
    output: Option<PathBuf>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report<T> {
    schema_version: u32,
    /// The experiment.
    scenario: String,
    backend: String,
    model_time_speedup: f64,
    latency_jitter: bool,
    /// The command-line options of the backend and of the run.
    options: serde_json::Value,
    experiment: serde_json::Value,
    runs: Vec<T>,
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
        experiment: cli.experiment.clone(),
        set: cli.set.clone(),
        only: cli.only.clone(),
    };
    if cli.list {
        for name in selection.experiment_names()? {
            println!("{name}");
        }
        return Ok(());
    }
    let resolved = selection.resolve()?;
    let configured_speedup = cli.backend.model_time_speedup()?;
    if let Some(speedup) = configured_speedup {
        rt::set_model_time_speedup(speedup)?;
    }
    let model_time_speedup = configured_speedup.unwrap_or(1.0);
    let runtime = match resolved.runtime()? {
        runner::config::Runtime::MultiThread => tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .build()?,
        runner::config::Runtime::CurrentThread => tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?,
    };
    let factory = runtime.block_on(cli.backend.initialize())?;
    let execution = Execution {
        runs: cli.runs,
        run_cooldown: cli.run_cooldown,
        drain_timeout: cli.drain_timeout,
    };
    let runs = runner::run(&runtime, &factory, &resolved, execution)?;
    let report = Report {
        schema_version: SCHEMA_VERSION,
        scenario: resolved.name.clone(),
        backend: cli.backend.label().to_string(),
        model_time_speedup,
        latency_jitter: cli.backend.latency_jitter,
        options: cli.backend.describe(),
        experiment: resolved.describe(),
        runs,
    };
    write_json(cli.output, &serde_json::to_value(report)?)
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
