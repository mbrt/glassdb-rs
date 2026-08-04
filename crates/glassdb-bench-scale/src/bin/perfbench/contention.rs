//! Focused overlapping read-modify-write contention.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use clap::Args;
use futures::future::join_all;
use glassdb::{Collection, Database, Error as GError};
use glassdb_backend::Backend;
use glassdb_bench_scale::bench::Bench;
use glassdb_bench_scale::run::{join_tasks_until, shutdown_databases_until};
use serde::Serialize;
use tokio::runtime::Handle;

use super::backend;
use super::{Execution, cooldown};

const WRITERS: usize = 5;
const VALUE_BYTES: usize = 1024;

#[derive(Clone, Args)]
pub(super) struct Options {
    /// Key counts to sweep. The default visits one through six.
    #[arg(long, value_delimiter = ',')]
    keys: Vec<usize>,
    /// Measured wall-clock duration of each contention cell.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    duration: Duration,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RunResult {
    run: usize,
    cells: Vec<CellResult>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct CellResult {
    num_keys: usize,
    overlap: usize,
    overlap_pct: usize,
    committed: usize,
    duration_ms: f64,
    tx_per_sec: f64,
    p50_ms: f64,
    p90_ms: f64,
    samples_ms: Vec<f64>,
    retries: u64,
    direct_candidates: u64,
    direct_landed: u64,
    worker_drain_ms: f64,
    failures: u64,
}

pub(super) fn run(
    handle: &Handle,
    factory: &backend::Factory,
    options: &Options,
    execution: Execution,
) -> Result<Vec<RunResult>, Box<dyn Error>> {
    let key_steps = key_steps(options)?;
    let invocation = SystemTime::UNIX_EPOCH.elapsed()?.as_millis();
    let mut runs = Vec::with_capacity(execution.runs);
    for run in 1..=execution.runs {
        handle.block_on(cooldown(execution, run));
        let mut cells = Vec::new();
        for &num_keys in &key_steps {
            for overlap in 1..=num_keys {
                eprintln!("contention: run={run} keys={num_keys} overlap={overlap}");
                let name = format!("perfbenchcontention{invocation}r{run}k{num_keys}o{overlap}");
                cells.push(run_cell(
                    handle,
                    factory.backend(),
                    &name,
                    num_keys,
                    overlap,
                    options.duration,
                    execution,
                )?);
            }
        }
        runs.push(RunResult { run, cells });
    }
    Ok(runs)
}

fn run_cell(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    database_name: &str,
    num_keys: usize,
    overlap: usize,
    duration: Duration,
    execution: Execution,
) -> Result<CellResult, Box<dyn Error>> {
    let db = handle.block_on(Database::open(database_name, backend))?;
    let collection = handle.block_on(
        db.root_collection()
            .create_collection_if_absent(b"contention"),
    )?;
    let all_keys = contention_keys(WRITERS, num_keys, overlap);
    seed(handle, &db, &collection, &all_keys)?;

    let base = db.stats();
    let bench = Arc::new(Bench::new(duration));
    let wall_start = Instant::now();
    bench.start();
    let deadline = tokio::time::Instant::now() + duration + execution.drain_timeout;
    let handles = spawn_workers(
        handle,
        &db,
        &collection,
        &bench,
        &all_keys,
        num_keys,
        overlap,
    );
    let workers = handle.block_on(join_tasks_until(handles, deadline));
    bench.end();
    let worker_wall = wall_start.elapsed();
    let shutdown = handle.block_on(shutdown_databases_until(
        std::slice::from_ref(&db),
        deadline,
    ));
    workers?;
    shutdown?;

    let stats = db.stats() - base;
    let result = bench.results();
    let duration_secs = result.tot_duration.as_secs_f64();
    let samples_ms: Vec<_> = result
        .samples
        .iter()
        .map(|sample| sample.as_secs_f64() * 1000.0)
        .collect();
    Ok(CellResult {
        num_keys,
        overlap,
        overlap_pct: 100 * overlap / num_keys,
        committed: samples_ms.len(),
        duration_ms: duration_secs * 1000.0,
        tx_per_sec: if duration_secs > 0.0 {
            samples_ms.len() as f64 / duration_secs
        } else {
            0.0
        },
        p50_ms: result.percentile(0.5).as_secs_f64() * 1000.0,
        p90_ms: result.percentile(0.9).as_secs_f64() * 1000.0,
        samples_ms,
        retries: stats.transactions.retries,
        direct_candidates: stats.direct_commit.candidates,
        direct_landed: stats.direct_commit.landed,
        worker_drain_ms: worker_wall.saturating_sub(duration).as_secs_f64() * 1000.0,
        failures: 0,
    })
}

fn contention_keys(writers: usize, keys_per_writer: usize, overlap: usize) -> Vec<Vec<u8>> {
    let unique_per_writer = keys_per_writer - overlap;
    let total = overlap + writers * unique_per_writer;
    (0..total)
        .map(|index| format!("key{index}").into_bytes())
        .collect()
}

fn worker_keys(
    all: &[Vec<u8>],
    worker: usize,
    keys_per_worker: usize,
    overlap: usize,
) -> Vec<Vec<u8>> {
    let unique = keys_per_worker - overlap;
    let mut keys = Vec::with_capacity(keys_per_worker);
    keys.extend_from_slice(&all[..overlap]);
    let start = overlap + worker * unique;
    keys.extend_from_slice(&all[start..start + unique]);
    keys
}

fn seed(
    handle: &Handle,
    db: &Database,
    collection: &Collection,
    keys: &[Vec<u8>],
) -> Result<(), GError> {
    handle.block_on(db.tx(|tx| async move {
        for key in keys {
            tx.write(collection, key, &[0x5a; VALUE_BYTES])?;
        }
        Ok(())
    }))
}

fn spawn_workers(
    handle: &Handle,
    db: &Database,
    collection: &Collection,
    bench: &Arc<Bench>,
    all_keys: &[Vec<u8>],
    keys_per_worker: usize,
    overlap: usize,
) -> Vec<tokio::task::JoinHandle<Result<(), GError>>> {
    (0..WRITERS)
        .map(|worker| {
            let db = db.clone();
            let collection = collection.clone();
            let bench = bench.clone();
            let keys = worker_keys(all_keys, worker, keys_per_worker, overlap);
            handle.spawn(async move {
                while !bench.is_finished() {
                    bench.measure(|| mutate(&db, &collection, &keys)).await?;
                }
                Ok(())
            })
        })
        .collect()
}

async fn mutate(db: &Database, collection: &Collection, keys: &[Vec<u8>]) -> Result<(), GError> {
    db.tx(|tx| async move {
        let values = join_all(keys.iter().map(|key| tx.read(collection, key))).await;
        for (key, value) in keys.iter().zip(values) {
            let value = value?.ok_or(GError::NotFound)?;
            tx.write(collection, key, &value)?;
        }
        Ok(())
    })
    .await
}

fn key_steps(options: &Options) -> Result<Vec<usize>, Box<dyn Error>> {
    if options.keys.is_empty() {
        return Ok((1..=6).collect());
    }
    if options.keys.contains(&0) {
        return Err("--keys values must be greater than zero".into());
    }
    Ok(options.keys.clone())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn overlapping_layout_has_shared_and_disjoint_keys() {
        let all = contention_keys(3, 4, 2);
        assert_eq!(all.len(), 8);
        assert_eq!(worker_keys(&all, 0, 4, 2), all[0..4]);
        assert_eq!(
            worker_keys(&all, 1, 4, 2),
            [&all[0..2], &all[4..6]].concat()
        );
    }
}
