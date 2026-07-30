//! Focused coverage for ADR-056's demand-driven inline-pressure splits.

use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};

use glassdb::{Collection, Database, Error as GError, Stats};
use glassdb_backend::Backend;
use glassdb_bench_scale::backend_breakdown::{BackendBreakdown, BackendBreakdownHandle, wrap};
use glassdb_bench_scale::bench::{Bench, Results};
use glassdb_bench_scale::run::shutdown_databases_until;
use tokio::runtime::Handle;

const COLLECTION: &[u8] = b"inline-pressure";
const KEY_COUNT: usize = 192;
const VALUE_BYTES: usize = 1024;
const SATURATION_KEY_COUNT: usize = 64;
const SATURATION_KEYS: std::ops::Range<usize> = 0..SATURATION_KEY_COUNT;
const ROOT_PRESSURED_KEY: usize = 64;
const LEAF_PRESSURED_KEY: usize = 65;
const RECOVERY_KEY_COUNT: usize = 64;
const _: () = assert!(KEY_COUNT < 256);
const _: () = assert!(SATURATION_KEY_COUNT * VALUE_BYTES == 64 * 1024);
const _: () = assert!(ROOT_PRESSURED_KEY != LEAF_PRESSURED_KEY);
const _: () = assert!(LEAF_PRESSURED_KEY < KEY_COUNT);

const HEADER: &str = "\
run,phase,logical-tx,wall-ms,tx-per-sec,p50-ms,p90-ms,\
retries,lock-calls,direct-candidates,direct-landed,backend-ops,write-bytes,\
split-candidates,split-completed,split-deferred,\
pressure-candidates,pressure-completed,pressure-deferred,pressure-discarded";

pub(super) struct Options<'a> {
    pub out: &'a str,
    pub time_scale: f64,
    pub num_runs: usize,
    pub run_cooldown: Duration,
    pub drain_timeout: Duration,
    pub settle_timeout: Duration,
}

pub(super) fn run(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    options: Options<'_>,
) -> Result<(), Box<dyn Error>> {
    let (backend, backend_stats) = wrap(backend);
    let mut out = BufWriter::new(File::create(options.out)?);
    writeln!(out, "{HEADER}")?;
    let invocation = SystemTime::UNIX_EPOCH.elapsed()?.as_millis();

    for run in 1..=options.num_runs.max(1) {
        if run > 1 {
            handle.block_on(tokio::time::sleep(options.run_cooldown));
        }
        eprintln!("Inline-pressure run {run}/{}", options.num_runs.max(1));
        run_once(
            handle,
            &backend,
            &backend_stats,
            &options,
            invocation,
            run,
            &mut out,
        )?;
        out.flush()?;
    }
    Ok(())
}

fn run_once(
    handle: &Handle,
    backend: &Arc<dyn Backend>,
    backend_stats: &BackendBreakdownHandle,
    options: &Options<'_>,
    invocation: u128,
    run: usize,
    out: &mut impl Write,
) -> Result<(), Box<dyn Error>> {
    let name = format!("benchinlinepressure{invocation}{run}");
    seed_collection(handle, backend, &name, options.drain_timeout)?;

    let db = handle.block_on(Database::open(&name, backend.clone()))?;
    let collection = handle.block_on(db.open_collection("inline-pressure"))?;
    let mut cursor = Cursor::new(&db, backend_stats);
    let total_start = cursor;
    let total_wall_start = Instant::now();

    let measured = handle.block_on(measure_keys(
        &db,
        &collection,
        SATURATION_KEYS,
        options.time_scale,
    ))?;
    cursor.record(run, "saturation", measured, &db, backend_stats, out)?;

    let measured = handle.block_on(measure_keys(
        &db,
        &collection,
        std::iter::once(ROOT_PRESSURED_KEY),
        options.time_scale,
    ))?;
    cursor.record(run, "root-trigger", measured, &db, backend_stats, out)?;

    let pressure_base = protocol_stats(total_start.stats).pressure_completed;
    let wall = handle.block_on(wait_for_pressure_split(
        &db,
        pressure_base + 1,
        options.settle_timeout,
    ));
    cursor.record(
        run,
        "root-settle",
        Measured::idle(wall),
        &db,
        backend_stats,
        out,
    )?;

    let measured = handle.block_on(measure_keys(
        &db,
        &collection,
        std::iter::once(LEAF_PRESSURED_KEY),
        options.time_scale,
    ))?;
    cursor.record(run, "leaf-trigger", measured, &db, backend_stats, out)?;

    let wall = handle.block_on(wait_for_pressure_split(
        &db,
        pressure_base + 2,
        options.settle_timeout,
    ));
    cursor.record(
        run,
        "leaf-settle",
        Measured::idle(wall),
        &db,
        backend_stats,
        out,
    )?;

    let measured = handle.block_on(measure_keys(
        &db,
        &collection,
        recovery_keys(),
        options.time_scale,
    ))?;
    cursor.record(run, "recovery", measured, &db, backend_stats, out)?;

    handle.block_on(shutdown_databases_until(
        std::slice::from_ref(&db),
        tokio::time::Instant::now() + options.drain_timeout,
    ))?;

    let final_cursor = Cursor::new(&db, backend_stats);
    let total = Measured::idle(total_wall_start.elapsed())
        .with_count(SATURATION_KEYS.len() + 2 + RECOVERY_KEY_COUNT);
    write_row(
        out,
        run,
        "total",
        total,
        final_cursor.stats - total_start.stats,
        final_cursor.backend - total_start.backend,
    )?;
    Ok(())
}

fn seed_collection(
    handle: &Handle,
    backend: &Arc<dyn Backend>,
    name: &str,
    drain_timeout: Duration,
) -> Result<(), Box<dyn Error>> {
    let db = handle.block_on(Database::open(name, backend.clone()))?;
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
        tokio::time::Instant::now() + drain_timeout,
    ))?;
    Ok(())
}

async fn measure_keys(
    db: &Database,
    collection: &Collection,
    keys: impl IntoIterator<Item = usize>,
    time_scale: f64,
) -> Result<Measured, GError> {
    let bench = Bench::with_time_scale(Duration::from_secs(1), time_scale);
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

async fn wait_for_pressure_split(
    db: &Database,
    completed_target: u64,
    timeout: Duration,
) -> Duration {
    let start = Instant::now();
    loop {
        if protocol_stats(db.stats()).pressure_completed >= completed_target
            || start.elapsed() >= timeout
        {
            return start.elapsed();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
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
        run: usize,
        phase: &str,
        measured: Measured,
        db: &Database,
        backend: &BackendBreakdownHandle,
        out: &mut impl Write,
    ) -> std::io::Result<()> {
        let after = Self::new(db, backend);
        write_row(
            out,
            run,
            phase,
            measured,
            after.stats - self.stats,
            after.backend - self.backend,
        )?;
        *self = after;
        Ok(())
    }
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
}

#[derive(Clone, Copy)]
struct ProtocolStats {
    retries: u64,
    lock_calls: u64,
    direct_candidates: u64,
    direct_landed: u64,
    split_candidates: u64,
    split_completed: u64,
    split_deferred: u64,
    pressure_candidates: u64,
    pressure_completed: u64,
    pressure_deferred: u64,
    pressure_discarded: u64,
}

fn protocol_stats(stats: Stats) -> ProtocolStats {
    ProtocolStats {
        retries: stats.transactions.retries,
        lock_calls: stats.locker.calls,
        direct_candidates: stats.direct_commit.candidates,
        direct_landed: stats.direct_commit.landed,
        split_candidates: stats.splitter.candidates,
        split_completed: stats.splitter.completed,
        split_deferred: stats.splitter.deferred,
        pressure_candidates: stats.splitter.inline_pressure.candidates,
        pressure_completed: stats.splitter.inline_pressure.completed,
        pressure_deferred: stats.splitter.inline_pressure.deferred,
        pressure_discarded: stats.splitter.inline_pressure.discarded,
    }
}

fn write_row(
    out: &mut impl Write,
    run: usize,
    phase: &str,
    measured: Measured,
    stats: Stats,
    backend: BackendBreakdown,
) -> std::io::Result<()> {
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
                format!("{rate:.4}"),
                format!("{:.4}", results.percentile(0.5).as_secs_f64() * 1000.0),
                format!("{:.4}", results.percentile(0.9).as_secs_f64() * 1000.0),
            )
        })
        .unwrap_or_default();

    writeln!(
        out,
        "{run},{phase},{},{:.4},{tx_per_sec},{p50_ms},{p90_ms},\
         {},{},{},{},{},{},{},{},{},{},{},{},{}",
        measured.count,
        measured.wall.as_secs_f64() * 1000.0,
        protocol.retries,
        protocol.lock_calls,
        protocol.direct_candidates,
        protocol.direct_landed,
        backend.total(),
        backend.write_bytes(),
        protocol.split_candidates,
        protocol.split_completed,
        protocol.split_deferred,
        protocol.pressure_candidates,
        protocol.pressure_completed,
        protocol.pressure_deferred,
        protocol.pressure_discarded,
    )
}

fn key(index: usize) -> Vec<u8> {
    format!("key-{index:03}").into_bytes()
}

fn recovery_keys() -> impl Iterator<Item = usize> {
    (0..32).flat_map(|offset| [64 + offset, 96 + offset])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recovery_wave_interleaves_the_two_new_capacity_ranges() {
        let recovery: Vec<_> = recovery_keys().collect();
        assert_eq!(recovery.len(), RECOVERY_KEY_COUNT);
        assert_eq!(&recovery[..4], &[64, 96, 65, 97]);
    }

    #[test]
    fn csv_header_names_each_protocol_and_cost_signal_once() {
        let columns: Vec<_> = HEADER.split(',').collect();
        assert_eq!(columns.len(), 20);
        for required in [
            "direct-candidates",
            "direct-landed",
            "backend-ops",
            "write-bytes",
            "pressure-completed",
        ] {
            assert_eq!(
                columns.iter().filter(|column| **column == required).count(),
                1
            );
        }
        assert!(!columns.contains(&"node-reads"));
        assert!(!columns.contains(&"leaf-count"));
    }
}
