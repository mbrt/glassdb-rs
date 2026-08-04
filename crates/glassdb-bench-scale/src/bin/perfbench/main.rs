//! Database-level GlassDB performance scenarios.

mod backend;
mod contention;
mod inline_pressure;
mod mixed;

#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::error::Error;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use std::path::PathBuf;
use std::time::Duration;

use clap::{Parser, Subcommand};
use glassdb_concurr::rt;
use serde::Serialize;

const SCHEMA_VERSION: u32 = 1;

#[derive(Parser)]
#[command(about = "Run GlassDB database-level performance scenarios")]
struct Cli {
    #[command(flatten)]
    backend: backend::Options,
    /// Repeat the complete selected scenario.
    #[arg(long, default_value_t = 1, global = true)]
    runs: usize,
    /// Real wall time between repeated scenario runs.
    #[arg(long, default_value = "0s", value_parser = glassdb_bench_scale::parse_duration, global = true)]
    run_cooldown: Duration,
    /// Maximum time for in-flight work and Database shutdown.
    #[arg(long, default_value = "30s", value_parser = glassdb_bench_scale::parse_duration, global = true)]
    drain_timeout: Duration,
    /// Write JSON to this path instead of stdout.
    #[arg(long, global = true)]
    output: Option<PathBuf>,
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Sweep mixed transaction shapes across contention and collection affinity.
    Mixed(mixed::Options),
    /// Measure overlapping multi-key read-modify-write contention.
    Contention(contention::Options),
    /// Exercise demand-driven splits after inline-admission pressure.
    InlinePressure(inline_pressure::Options),
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Report<T> {
    schema_version: u32,
    scenario: &'static str,
    backend: String,
    model_time_speedup: f64,
    runs: Vec<T>,
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();
    if cli.runs == 0 {
        return Err("--runs must be greater than zero".into());
    }
    let configured_speedup = cli.backend.model_time_speedup()?;
    if let Some(speedup) = configured_speedup {
        rt::set_model_time_speedup(speedup)?;
    }
    let model_time_speedup = configured_speedup.unwrap_or(1.0);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let factory = runtime.block_on(cli.backend.initialize())?;
    let handle = runtime.handle();
    let backend = cli.backend.label().to_string();
    let execution = execution(&cli);

    let value = match &cli.command {
        Command::Mixed(options) => serde_json::to_value(Report {
            schema_version: SCHEMA_VERSION,
            scenario: "mixed",
            backend: backend.clone(),
            model_time_speedup,
            runs: mixed::run(handle, &factory, options, execution)?,
        })?,
        Command::Contention(options) => serde_json::to_value(Report {
            schema_version: SCHEMA_VERSION,
            scenario: "contention",
            backend: backend.clone(),
            model_time_speedup,
            runs: contention::run(handle, &factory, options, execution)?,
        })?,
        Command::InlinePressure(options) => serde_json::to_value(Report {
            schema_version: SCHEMA_VERSION,
            scenario: "inline-pressure",
            backend,
            model_time_speedup,
            runs: inline_pressure::run(handle, &factory, options, execution)?,
        })?,
    };
    write_json(cli.output, &value)
}

#[derive(Clone, Copy)]
struct Execution {
    runs: usize,
    run_cooldown: Duration,
    drain_timeout: Duration,
}

fn execution(cli: &Cli) -> Execution {
    Execution {
        runs: cli.runs,
        run_cooldown: cli.run_cooldown,
        drain_timeout: cli.drain_timeout,
    }
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
