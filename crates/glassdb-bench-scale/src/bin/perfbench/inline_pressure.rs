//! Measures how the leaves of the default topology policy adapt when their
//! aggregate inline budget is full (ADR-056, ADR-074): the locked commits that
//! the pressure causes until the splits land, and the direct commits after.

use std::error::Error;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use clap::Args;
use glassdb::{Collection, Database, Error as GError, InlinePolicy, Stats};
use glassdb_backend::Backend;
use glassdb_bench_scale::backend_breakdown::{BackendBreakdown, BackendBreakdownHandle, wrap};
use glassdb_bench_scale::bench::{Bench, Results};
use glassdb_bench_scale::run::shutdown_databases_until;
use serde::Serialize;
use tokio::runtime::Handle;

use super::backend;
use super::{Execution, cooldown};

const COLLECTION: &[u8] = b"inline-pressure";
const KEY_COUNT: usize = 192;
const VALUE_BYTES: usize = 1024;
const SATURATION_KEY_COUNT: usize = 64;
const SATURATION_KEYS: std::ops::Range<usize> = 0..SATURATION_KEY_COUNT;
const PRESSURED_KEY_COUNT: usize = 64;
// Keep the workload stable when product defaults are retuned.
const INLINE_POLICY: InlinePolicy = InlinePolicy {
    max_value_bytes: VALUE_BYTES,
    max_leaf_bytes: SATURATION_KEY_COUNT * VALUE_BYTES,
};
const _: () = {
    assert!(KEY_COUNT < 256);
    assert!(INLINE_POLICY.max_leaf_bytes == 64 * 1024);
    assert!(SATURATION_KEY_COUNT + PRESSURED_KEY_COUNT <= KEY_COUNT);
};

#[derive(Clone, Args)]
pub(super) struct Options {
    /// Maximum time of the pressure phase, until one pass over the pressured
    /// keys commits without locks.
    #[arg(long, default_value = "30s", value_parser = glassdb_bench_scale::parse_duration)]
    settle_timeout: Duration,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub(super) struct RunResult {
    run: usize,
    phases: Vec<PhaseResult>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct PhaseResult {
    phase: String,
    logical_tx: usize,
    wall_ms: f64,
    tx_per_sec: Option<f64>,
    p50_ms: Option<f64>,
    p90_ms: Option<f64>,
    replays: u64,
    lock_calls: u64,
    direct_candidates: u64,
    direct_landed: u64,
    backend_ops: u64,
    write_bytes: u64,
    split_candidates: u64,
    split_completed: u64,
    split_deferred: u64,
    merges: u64,
}

pub(super) fn run(
    handle: &Handle,
    factory: &backend::Factory,
    options: &Options,
    execution: Execution,
) -> Result<Vec<RunResult>, Box<dyn Error>> {
    let invocation = SystemTime::UNIX_EPOCH.elapsed()?.as_millis();
    let mut runs = Vec::with_capacity(execution.runs);
    for run in 1..=execution.runs {
        handle.block_on(cooldown(execution, run));
        eprintln!("inline-pressure: run={run}");
        let (backend, backend_stats) = wrap(factory.backend());
        let phases = run_once(
            handle,
            &backend,
            &backend_stats,
            options,
            execution,
            invocation,
            run,
        )?;
        runs.push(RunResult { run, phases });
    }
    Ok(runs)
}

fn run_once(
    handle: &Handle,
    backend: &Arc<dyn Backend>,
    backend_stats: &BackendBreakdownHandle,
    options: &Options,
    execution: Execution,
    invocation: u128,
    run: usize,
) -> Result<Vec<PhaseResult>, Box<dyn Error>> {
    let name = format!("perfbenchinlinepressure{invocation}{run}");
    seed_collection(handle, backend, &name, execution)?;

    let db = open_database(handle, backend, &name)?;
    let collection = handle.block_on(db.open_collection("inline-pressure"))?;
    let mut cursor = Cursor::new(&db, backend_stats);
    let total_start = cursor;
    let total_wall_start = Instant::now();
    let mut phases = Vec::new();

    let saturation = handle.block_on(measure_keys(&db, &collection, SATURATION_KEYS))?;
    let mut count = saturation.count;
    phases.push(cursor.record("saturation", saturation, &db, backend_stats));

    let (pressure, recovery) = handle.block_on(measure_until_direct(
        &db,
        &collection,
        backend_stats,
        options.settle_timeout,
    ))?;
    count += pressure.measured.count + recovery.count;
    phases.push(cursor.record_until("pressure", pressure.measured, pressure.until));
    phases.push(cursor.record("recovery", recovery, &db, backend_stats));

    handle.block_on(shutdown_databases_until(
        std::slice::from_ref(&db),
        tokio::time::Instant::now() + execution.drain_timeout,
    ))?;

    let final_cursor = Cursor::new(&db, backend_stats);
    let total = Measured::idle(total_wall_start.elapsed()).with_count(count);
    phases.push(result(
        "total",
        total,
        final_cursor.stats - total_start.stats,
        final_cursor.backend - total_start.backend,
    ));
    Ok(phases)
}

fn seed_collection(
    handle: &Handle,
    backend: &Arc<dyn Backend>,
    name: &str,
    execution: Execution,
) -> Result<(), Box<dyn Error>> {
    let db = open_database(handle, backend, name)?;
    let collection =
        handle.block_on(db.root_collection().create_collection_if_absent(COLLECTION))?;
    let collection = &collection;
    handle.block_on(db.tx(|tx| async move {
        for index in 0..KEY_COUNT {
            tx.write(collection, &key(index), &[0; VALUE_BYTES])?;
        }
        Ok(())
    }))?;
    handle.block_on(shutdown_databases_until(
        std::slice::from_ref(&db),
        tokio::time::Instant::now() + execution.drain_timeout,
    ))?;
    Ok(())
}

fn open_database(
    handle: &Handle,
    backend: &Arc<dyn Backend>,
    name: &str,
) -> Result<Database, GError> {
    handle.block_on(
        Database::builder(name, backend.clone())
            .inline_policy(INLINE_POLICY)
            .open(),
    )
}

async fn measure_keys(
    db: &Database,
    collection: &Collection,
    keys: impl IntoIterator<Item = usize>,
) -> Result<Measured, GError> {
    let bench = Bench::new(Duration::from_secs(1));
    let wall_start = Instant::now();
    bench.start();
    for index in keys {
        let key = key(index);
        bench.measure(|| mutate(db, collection, &key)).await?;
    }
    bench.end();
    Ok(Measured {
        count: bench.sample_count(),
        wall: wall_start.elapsed(),
        results: Some(bench.results()),
    })
}

/// Repeats passes over the pressured keys until one pass commits without
/// locks, and returns the passes before it and that pass.
async fn measure_until_direct(
    db: &Database,
    collection: &Collection,
    backend: &BackendBreakdownHandle,
    timeout: Duration,
) -> Result<(Pressure, Measured), Box<dyn Error>> {
    let start = Instant::now();
    let splits_before = db.stats().restructurer.splits;
    let mut pressure = Measured::idle(Duration::ZERO);
    loop {
        let before = Cursor::new(db, backend);
        let pass = measure_keys(db, collection, pressured_keys()).await?;
        if db.stats().locker.calls == before.stats.locker.calls {
            pressure.wall = start.elapsed() - pass.wall;
            let pressure = Pressure {
                measured: pressure,
                until: before,
            };
            return Ok((pressure, pass));
        }
        pressure.append(pass);
        if start.elapsed() >= timeout {
            let splits = db.stats().restructurer.splits - splits_before;
            return Err(format!(
                "pressured keys still took locked commits after {timeout:?} (splits={splits})"
            )
            .into());
        }
    }
}

async fn mutate(db: &Database, collection: &Collection, key: &[u8]) -> Result<(), GError> {
    db.tx(|tx| async move {
        let mut value = tx.read(collection, key).await?.ok_or(GError::NotFound)?;
        if value.len() != VALUE_BYTES {
            return Err(GError::internal(format!(
                "inline-pressure value has {} bytes, expected {VALUE_BYTES}",
                value.len()
            )));
        }
        value[0] = value[0].wrapping_add(1);
        tx.write(collection, key, &value)?;
        Ok(())
    })
    .await
}

#[derive(Clone, Copy)]
struct Cursor {
    stats: Stats,
    backend: BackendBreakdown,
}

impl Cursor {
    fn new(db: &Database, backend: &BackendBreakdownHandle) -> Self {
        Self {
            stats: db.stats(),
            backend: backend.snapshot(),
        }
    }

    fn record(
        &mut self,
        phase: &str,
        measured: Measured,
        db: &Database,
        backend: &BackendBreakdownHandle,
    ) -> PhaseResult {
        self.record_until(phase, measured, Self::new(db, backend))
    }

    /// Records the phase that ended at `until`.
    fn record_until(&mut self, phase: &str, measured: Measured, until: Cursor) -> PhaseResult {
        let row = result(
            phase,
            measured,
            until.stats - self.stats,
            until.backend - self.backend,
        );
        *self = until;
        row
    }
}

/// The passes over the pressured keys that took locked commits.
struct Pressure {
    measured: Measured,
    /// The stats when the passes ended.
    until: Cursor,
}

struct Measured {
    count: usize,
    wall: Duration,
    results: Option<Results>,
}

impl Measured {
    fn idle(wall: Duration) -> Self {
        Self {
            count: 0,
            wall,
            results: None,
        }
    }

    fn with_count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }

    /// Adds the transactions of `other` to this measurement.
    fn append(&mut self, other: Measured) {
        self.count += other.count;
        let Some(other) = other.results else {
            return;
        };
        match &mut self.results {
            Some(results) => {
                results.samples.extend(other.samples);
                results.tot_duration += other.tot_duration;
            }
            None => self.results = Some(other),
        }
    }
}

#[derive(Clone, Copy)]
struct ProtocolStats {
    replays: u64,
    lock_calls: u64,
    direct_candidates: u64,
    direct_landed: u64,
    split_candidates: u64,
    split_completed: u64,
    split_deferred: u64,
    merges: u64,
}

fn protocol_stats(stats: Stats) -> ProtocolStats {
    ProtocolStats {
        replays: stats.transactions.replays,
        lock_calls: stats.locker.calls,
        direct_candidates: stats.direct_commit.candidates,
        direct_landed: stats.direct_commit.landed,
        split_candidates: stats.restructurer.candidates,
        split_completed: stats.restructurer.splits,
        split_deferred: stats.restructurer.deferred,
        merges: stats.restructurer.merges,
    }
}

fn result(phase: &str, measured: Measured, stats: Stats, backend: BackendBreakdown) -> PhaseResult {
    let protocol = protocol_stats(stats);
    let (tx_per_sec, p50_ms, p90_ms) = measured
        .results
        .as_ref()
        .map(|results| {
            let rate = if results.tot_duration.is_zero() {
                0.0
            } else {
                results.samples.len() as f64 / results.tot_duration.as_secs_f64()
            };
            (
                Some(rate),
                Some(results.percentile(0.5).as_secs_f64() * 1000.0),
                Some(results.percentile(0.9).as_secs_f64() * 1000.0),
            )
        })
        .unwrap_or_default();
    PhaseResult {
        phase: phase.to_string(),
        logical_tx: measured.count,
        wall_ms: measured.wall.as_secs_f64() * 1000.0,
        tx_per_sec,
        p50_ms,
        p90_ms,
        replays: protocol.replays,
        lock_calls: protocol.lock_calls,
        direct_candidates: protocol.direct_candidates,
        direct_landed: protocol.direct_landed,
        backend_ops: backend.total(),
        write_bytes: backend.write_bytes(),
        split_candidates: protocol.split_candidates,
        split_completed: protocol.split_completed,
        split_deferred: protocol.split_deferred,
        merges: protocol.merges,
    }
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:03}").into_bytes()
}

/// Returns the keys after the saturated ones, alternating between their two
/// halves, so that the writes spread over the leaves that the splits make.
fn pressured_keys() -> impl Iterator<Item = usize> {
    const HALF: usize = PRESSURED_KEY_COUNT / 2;
    (0..HALF).flat_map(|offset| {
        [
            SATURATION_KEY_COUNT + offset,
            SATURATION_KEY_COUNT + HALF + offset,
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pressured_keys_alternate_between_their_halves() {
        let pressured: Vec<_> = pressured_keys().collect();
        assert_eq!(pressured.len(), PRESSURED_KEY_COUNT);
        assert_eq!(&pressured[..4], &[64, 96, 65, 97]);
    }

    #[test]
    fn idle_phase_has_no_latency_summary() {
        let phase = result(
            "settle",
            Measured::idle(Duration::from_millis(10)),
            Stats::default(),
            BackendBreakdown::default(),
        );
        assert_eq!(phase.logical_tx, 0);
        assert_eq!(phase.tx_per_sec, None);
        assert_eq!(phase.p50_ms, None);
    }
}
