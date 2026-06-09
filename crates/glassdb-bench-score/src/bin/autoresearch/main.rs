//! The autoresearch scoring harness for glassdb-rs.
//!
//! It runs a fixed suite of single-client workloads against the in-memory
//! backend and reports a single primary score plus secondary axes. The primary
//! score is a weighted count of backend operations per transaction (lower is
//! better) and is deterministic for these single-client workloads, so it is
//! comparable across machines and runs. The secondary axes (memory, CPU /
//! runtime) are softer, noisier signals used as tie-breakers.
//!
//! Ported from the Go `hack/autoresearch/bench`. Go's `mutexWaitNsPerTx`
//! (from `runtime/metrics`) has no portable Rust equivalent and is dropped.
//!
//! This file is part of the autoresearch fixed infrastructure: it defines the
//! metric and must NOT be modified by autoresearch experiments.

mod metrics;
mod workloads;

use std::cmp::Ordering;
use std::error::Error;
use std::fs::OpenOptions;
use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use serde::Serialize;

use glassdb::backend::memory::MemoryBackend;
use glassdb::{Backend, DB, Stats};

use crate::metrics::Sample;

/// Path (relative to the repo root, where the harness is run from) of the log
/// the `--record` flag appends a score line to.
const LOG_PATH: &str = "hack/autoresearch/log.md";

/// Converts backend operation counts into a single cost. The values are the
/// mean object-storage latencies (in milliseconds): object read ~57ms, object
/// write ~70ms, metadata ~31ms. List is treated as a metadata-class operation.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct Weights {
    obj_read: f64,
    obj_write: f64,
    meta_read: f64,
    meta_write: f64,
    obj_list: f64,
}

const WEIGHTS: Weights = Weights {
    obj_read: 57.0,
    obj_write: 70.0,
    meta_read: 31.0,
    meta_write: 31.0,
    obj_list: 31.0,
};

impl Weights {
    fn cost(&self, s: &Stats) -> f64 {
        self.obj_read * s.obj_reads as f64
            + self.obj_write * s.obj_writes as f64
            + self.meta_read * s.meta_reads as f64
            + self.meta_write * s.meta_writes as f64
            + self.obj_list * s.obj_lists as f64
    }
}

/// The measured metrics for a single workload.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct WorkloadResult {
    name: String,
    txn: i64,
    obj_reads: i64,
    obj_writes: i64,
    meta_reads: i64,
    meta_writes: i64,
    obj_lists: i64,
    retries: i64,
    cost_per_tx: f64,
    alloc_bytes_per_tx: f64,
    allocs_per_tx: f64,
    ns_per_tx: f64,
    cpu_ns_per_tx: f64,
}

/// The secondary axes aggregated across all workloads.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Secondary {
    alloc_bytes_per_tx: f64,
    allocs_per_tx: f64,
    ns_per_tx: f64,
    cpu_ns_per_tx: f64,
}

/// The outcome of running the full workload suite once.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SuiteResult {
    score: f64,
    secondary: Secondary,
    weights: Weights,
    workloads: Vec<WorkloadResult>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    scores: Vec<f64>,
}

#[global_allocator]
static GLOBAL: metrics::CountingAlloc = metrics::CountingAlloc;

#[derive(Parser)]
#[command(about = "Autoresearch scoring harness for glassdb-rs")]
struct Args {
    /// Emit machine-readable JSON instead of a human-readable table.
    #[arg(long)]
    json: bool,
    /// Run the suite this many times and report the median.
    #[arg(long, default_value_t = 1)]
    count: usize,
    /// Append a score-record line to the log.
    #[arg(long)]
    record: bool,
}

fn main() {
    let args = Args::parse();
    let count = args.count.max(1);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("build tokio runtime");

    let mut runs = Vec::with_capacity(count);
    let mut scores = Vec::with_capacity(count);
    for _ in 0..count {
        let res = rt.block_on(run_suite()).unwrap_or_else(|e| {
            eprintln!("error: {e}");
            std::process::exit(1);
        });
        scores.push(res.score);
        runs.push(res);
    }

    let mut final_run = median_run(runs);
    final_run.scores = scores;

    if args.record
        && let Err(e) = append_record(&final_run)
    {
        eprintln!("warning: could not record: {e}");
    }

    if args.json {
        println!(
            "{}",
            serde_json::to_string_pretty(&final_run).expect("serialize suite result")
        );
    } else {
        emit_text(&final_run);
    }
}

/// Runs every workload once (each on a fresh DB and backend) and aggregates the
/// per-workload results into a single suite score.
async fn run_suite() -> Result<SuiteResult, Box<dyn Error>> {
    let mut results = Vec::with_capacity(workloads::NAMES.len());
    for &name in workloads::NAMES {
        let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
        let db = DB::open("autoresearch", backend).await?;
        let sample = workloads::run(name, &db).await?;
        db.shutdown().await;
        results.push(to_result(sample)?);
    }

    let costs: Vec<f64> = results.iter().map(|r| r.cost_per_tx).collect();
    let alloc_bytes: Vec<f64> = results.iter().map(|r| r.alloc_bytes_per_tx).collect();
    let allocs: Vec<f64> = results.iter().map(|r| r.allocs_per_tx).collect();
    let ns: Vec<f64> = results.iter().map(|r| r.ns_per_tx).collect();
    let cpu: Vec<f64> = results.iter().map(|r| r.cpu_ns_per_tx).collect();

    Ok(SuiteResult {
        score: geomean(&costs),
        secondary: Secondary {
            alloc_bytes_per_tx: geomean(&alloc_bytes),
            allocs_per_tx: geomean(&allocs),
            ns_per_tx: geomean(&ns),
            cpu_ns_per_tx: geomean(&cpu),
        },
        weights: WEIGHTS,
        workloads: results,
        scores: Vec::new(),
    })
}

/// Turns a raw [`Sample`] into a per-transaction-normalized [`WorkloadResult`].
fn to_result(s: Sample) -> Result<WorkloadResult, Box<dyn Error>> {
    let txn = s.stats.tx_n;
    if txn <= 0 {
        return Err(format!("workload {}: no transactions recorded", s.name).into());
    }
    let n = txn as f64;
    Ok(WorkloadResult {
        name: s.name,
        txn,
        obj_reads: s.stats.obj_reads,
        obj_writes: s.stats.obj_writes,
        meta_reads: s.stats.meta_reads,
        meta_writes: s.stats.meta_writes,
        obj_lists: s.stats.obj_lists,
        retries: s.stats.tx_retries,
        cost_per_tx: WEIGHTS.cost(&s.stats) / n,
        alloc_bytes_per_tx: s.alloc_bytes as f64 / n,
        allocs_per_tx: s.alloc_count as f64 / n,
        ns_per_tx: s.wall_ns as f64 / n,
        cpu_ns_per_tx: s.cpu_ns as f64 / n,
    })
}

/// Geometric mean, so workloads of very different magnitudes contribute
/// proportionally. A small floor avoids `ln(0)`.
fn geomean(xs: &[f64]) -> f64 {
    if xs.is_empty() {
        return 0.0;
    }
    const FLOOR: f64 = 1.0;
    let sum: f64 = xs.iter().map(|&x| x.max(FLOOR).ln()).sum();
    (sum / xs.len() as f64).exp()
}

/// Returns the run whose score is the median across runs.
fn median_run(mut runs: Vec<SuiteResult>) -> SuiteResult {
    let mut idx: Vec<usize> = (0..runs.len()).collect();
    idx.sort_by(|&a, &b| {
        runs[a]
            .score
            .partial_cmp(&runs[b].score)
            .unwrap_or(Ordering::Equal)
    });
    let mid = idx[idx.len() / 2];
    runs.swap_remove(mid)
}

fn emit_text(res: &SuiteResult) {
    println!("primary score (lower is better): {:.2}", res.score);
    if res.scores.len() > 1 {
        println!("per-run scores: {:?}", res.scores);
    }
    println!();
    println!(
        "{:<14} {:>6} {:>10} {:>10} {:>10}",
        "workload", "txn", "cost/tx", "allocB/tx", "ns/tx"
    );
    for w in &res.workloads {
        println!(
            "{:<14} {:>6} {:>10.1} {:>10.0} {:>10.0}",
            w.name, w.txn, w.cost_per_tx, w.alloc_bytes_per_tx, w.ns_per_tx
        );
    }
    println!();
    println!("secondary (geomean over workloads):");
    println!("  alloc bytes/tx: {:.0}", res.secondary.alloc_bytes_per_tx);
    println!("  allocs/tx:      {:.1}", res.secondary.allocs_per_tx);
    println!("  ns/tx:          {:.0}", res.secondary.ns_per_tx);
    println!("  cpu ns/tx:      {:.0}", res.secondary.cpu_ns_per_tx);
}

fn append_record(res: &SuiteResult) -> std::io::Result<()> {
    let ts = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0);
    let mut f = OpenOptions::new()
        .append(true)
        .create(true)
        .open(LOG_PATH)?;
    writeln!(
        f,
        "- score-record unix={ts} primary={:.2} allocBytesPerTx={:.0} allocsPerTx={:.1} \
         nsPerTx={:.0} cpuNsPerTx={:.0}",
        res.score,
        res.secondary.alloc_bytes_per_tx,
        res.secondary.allocs_per_tx,
        res.secondary.ns_per_tx,
        res.secondary.cpu_ns_per_tx,
    )
}
