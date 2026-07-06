//! Mixed-workload contention benchmark.
//!
//! Runs all four transaction shapes (`rwSingle`, `rwMany`, `roSingle`,
//! `roMulti`) **concurrently over a shared in-memory backend** with a simulated
//! cloud-latency profile, sweeping a 2x2 grid of:
//!
//! - contention **mode** (`lo` = keys drawn from a large pool so overlaps are
//!   rare; `hi` = keys drawn from a small hot pool so they collide on the same
//!   shards), and
//! - Database **topology** (`shared` = one `Database` hosts every shape;
//!   `per-shape` = `K` client `Database`s per shape, each hosting one shape).
//!
//! All `Database`s in a cell wrap the *same* underlying backend, so they contend
//! through the object protocol exactly like `rtbench`'s `rw9010` — while each
//! `Database`'s own `StatsBackend` lets `per-shape` cells attribute backend ops
//! to a single shape. `shared` cells can only report a whole-DB op aggregate,
//! but let the in-process request deduplication (ADR-025/026) batch across
//! shapes; comparing the two topologies exposes how much that batching helps.
//!
//! ```text
//! cargo run --release -p glassdb-bench-scale --bin mixbench
//! cargo run --release -p glassdb-bench-scale --bin mixbench -- --modes hi --topologies per-shape --json
//! ```

// musl's default allocator serializes multi-threaded allocation on a coarse
// lock; mimalloc's per-thread caches remove that contention (see rtbench).
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::collections::HashSet;
use std::error::Error;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use clap::Parser;
use futures::future::join_all;
use rand::rngs::StdRng;
use rand::{RngExt, SeedableRng};
use serde::Serialize;
use tokio::runtime::Handle;
use tokio::task::JoinHandle;

use glassdb::backend::memory::MemoryBackend;
use glassdb::middleware::{DelayBackend, DelayOptions, gcs_delays, s3_delays};
use glassdb::{Collection, Database, Error as GError, Stats};
use glassdb_backend::Backend;
use glassdb_bench_scale::bench::Bench;

/// The shared collection every shape reads and writes, so all shapes contend on
/// the same key pool.
const COLL: &str = "mix";

/// Fixed opaque value written on every put; only op counts and contention
/// matter, not the payload.
fn value() -> Vec<u8> {
    vec![0x5a; 128]
}

/// The base key name for pool index `i`.
fn key_bytes(i: usize) -> Vec<u8> {
    format!("key{i}").into_bytes()
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

#[derive(Parser)]
#[command(about = "Mixed-workload contention grid for glassdb")]
struct Args {
    /// Measured duration of each shape within a cell.
    #[arg(long, default_value = "2s", value_parser = glassdb_bench_scale::parse_duration)]
    duration: Duration,
    /// Total concurrent workers per shape (held constant across topologies: all
    /// on the one DB for `shared`, split evenly across the shape's K DBs for
    /// `per-shape`).
    #[arg(long, default_value_t = 8)]
    workers_per_shape: usize,
    /// Client `Database`s per shape in the `per-shape` topology (4*K DBs total).
    #[arg(long, default_value_t = 4)]
    clients_per_shape: usize,
    /// Keys touched by the multi-key shapes (`rwMany`, `roMulti`); clamped to
    /// the pool size in the `hi` mode.
    #[arg(long, default_value_t = 10)]
    multi_keys: usize,
    /// Key-pool size for the `lo` (spread) mode. Keys hash across the 1024
    /// shards, so a few thousand already populate every shard and keep overlaps
    /// rare; larger pools add seeding cost without lowering contention further.
    #[arg(long, default_value_t = 5000)]
    num_keys: usize,
    /// Key-pool size for the `hi` (hot) mode.
    #[arg(long, default_value_t = 8)]
    hot_keys: usize,
    /// Simulated backend latency profile.
    #[arg(long, default_value = "s3", value_parser = ["gcs", "s3"])]
    delays: String,
    /// Compresses the simulated latencies/rate-limits by this factor (`1.0` =
    /// real-time; smaller runs faster). Must be > 0.
    #[arg(long, default_value_t = 0.02)]
    delay_scale: f64,
    /// Contention modes to sweep.
    #[arg(long, value_delimiter = ',', default_value = "lo,hi")]
    modes: Vec<String>,
    /// Database topologies to sweep.
    #[arg(long, value_delimiter = ',', default_value = "shared,per-shape")]
    topologies: Vec<String>,
    /// Emit the full grid as JSON instead of a human-readable table.
    #[arg(long)]
    json: bool,
}

// ---------------------------------------------------------------------------
// Dimensions
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq)]
enum Shape {
    RwSingle,
    RwMany,
    RoSingle,
    RoMulti,
}

const SHAPES: [Shape; 4] = [
    Shape::RwSingle,
    Shape::RwMany,
    Shape::RoSingle,
    Shape::RoMulti,
];

impl Shape {
    fn name(self) -> &'static str {
        match self {
            Shape::RwSingle => "rwSingle",
            Shape::RwMany => "rwMany",
            Shape::RoSingle => "roSingle",
            Shape::RoMulti => "roMulti",
        }
    }

    /// Whether the shape writes (drives lock/CAS contention) or only reads.
    fn is_write(self) -> bool {
        matches!(self, Shape::RwSingle | Shape::RwMany)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Mode {
    Lo,
    Hi,
}

impl Mode {
    fn label(self) -> &'static str {
        match self {
            Mode::Lo => "lo",
            Mode::Hi => "hi",
        }
    }

    /// Key-pool size for the mode (small = high contention).
    fn pool_size(self, args: &Args) -> usize {
        match self {
            Mode::Lo => args.num_keys.max(1),
            Mode::Hi => args.hot_keys.max(1),
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Topology {
    Shared,
    PerShape,
}

impl Topology {
    fn label(self) -> &'static str {
        match self {
            Topology::Shared => "shared",
            Topology::PerShape => "per-shape",
        }
    }
}

fn parse_modes(v: &[String]) -> Result<Vec<Mode>, Box<dyn Error>> {
    v.iter()
        .map(|s| match s.trim() {
            "lo" => Ok(Mode::Lo),
            "hi" => Ok(Mode::Hi),
            other => Err(format!("unknown mode {other:?} (expected lo|hi)").into()),
        })
        .collect()
}

fn parse_topologies(v: &[String]) -> Result<Vec<Topology>, Box<dyn Error>> {
    v.iter()
        .map(|s| match s.trim() {
            "shared" => Ok(Topology::Shared),
            "per-shape" | "pershape" => Ok(Topology::PerShape),
            other => Err(format!("unknown topology {other:?} (expected shared|per-shape)").into()),
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Results
// ---------------------------------------------------------------------------

/// Backend-op counters normalized per transaction.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
struct OpsPerTx {
    txn: u64,
    obj_reads_per_tx: f64,
    obj_writes_per_tx: f64,
    obj_lists_per_tx: f64,
    total_ops_per_tx: f64,
    retries_per_tx: f64,
}

/// Raw counter deltas summed across a shape's Databases.
#[derive(Default, Clone, Copy)]
struct RawOps {
    reads: u64,
    writes: u64,
    lists: u64,
    txn: u64,
    retries: u64,
}

impl RawOps {
    fn of(delta: Stats) -> Self {
        RawOps {
            reads: delta.obj_reads,
            writes: delta.obj_writes,
            lists: delta.obj_lists,
            txn: delta.tx_n,
            retries: delta.tx_retries,
        }
    }

    fn add(self, o: RawOps) -> RawOps {
        RawOps {
            reads: self.reads + o.reads,
            writes: self.writes + o.writes,
            lists: self.lists + o.lists,
            txn: self.txn + o.txn,
            retries: self.retries + o.retries,
        }
    }

    fn per_tx(self) -> OpsPerTx {
        let d = self.txn.max(1) as f64;
        OpsPerTx {
            txn: self.txn,
            obj_reads_per_tx: self.reads as f64 / d,
            obj_writes_per_tx: self.writes as f64 / d,
            obj_lists_per_tx: self.lists as f64 / d,
            total_ops_per_tx: (self.reads + self.writes + self.lists) as f64 / d,
            retries_per_tx: self.retries as f64 / d,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct ShapeResult {
    shape: String,
    committed: usize,
    tx_per_sec: f64,
    p50_ms: f64,
    p90_ms: f64,
    /// Present only in the `per-shape` topology (each DB hosts one shape).
    #[serde(skip_serializing_if = "Option::is_none")]
    ops: Option<OpsPerTx>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct CellResult {
    mode: String,
    topology: String,
    databases: usize,
    failures: u64,
    shapes: Vec<ShapeResult>,
    /// Present only in the `shared` topology (one StatsBackend counts all
    /// shapes, so ops cannot be attributed per shape).
    #[serde(skip_serializing_if = "Option::is_none")]
    aggregate_ops: Option<OpsPerTx>,
}

// ---------------------------------------------------------------------------
// Main / grid sweep
// ---------------------------------------------------------------------------

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    if !(args.delay_scale > 0.0 && args.delay_scale.is_finite()) {
        return Err(format!("--delay-scale must be > 0, got {}", args.delay_scale).into());
    }
    if args.workers_per_shape == 0 {
        return Err("--workers-per-shape must be >= 1".into());
    }
    let modes = parse_modes(&args.modes)?;
    let topologies = parse_topologies(&args.topologies)?;

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let handle = rt.handle().clone();

    let mut cells = Vec::new();
    for &mode in &modes {
        for &topo in &topologies {
            eprintln!(
                "running mode={} topology={} ...",
                mode.label(),
                topo.label()
            );
            cells.push(run_cell(&handle, &args, mode, topo)?);
        }
    }

    if args.json {
        println!("{}", serde_json::to_string_pretty(&cells)?);
    } else {
        emit_text(&args, &cells);
    }
    Ok(())
}

/// Selects and scales the simulated-latency profile.
fn delay_profile(args: &Args) -> DelayOptions {
    let mut d = match args.delays.as_str() {
        "gcs" => gcs_delays(),
        // clap's value_parser guarantees "s3" otherwise.
        _ => s3_delays(),
    };
    d.scale = args.delay_scale;
    d
}

fn open_db(handle: &Handle, backend: Arc<dyn Backend>) -> Database {
    handle
        .block_on(Database::open("mix", backend))
        .expect("open db")
}

/// Distributes `w` workers across `k` Databases as evenly as possible, dropping
/// empty slots (so `k` is effectively clamped to `w`).
fn split_workers(w: usize, k: usize) -> Vec<usize> {
    let k = k.max(1).min(w.max(1));
    let base = w / k;
    let rem = w % k;
    (0..k)
        .map(|i| base + usize::from(i < rem))
        .filter(|&c| c > 0)
        .collect()
}

/// One shape's plan within a cell: its timer plus the `(database, workers)`
/// slots it drives. In `shared` every shape's slots point at the one DB; in
/// `per-shape` a shape owns a disjoint set of DBs.
struct ShapePlan {
    shape: Shape,
    bench: Arc<Bench>,
    /// Indices into the cell's `dbs` vector, with the worker count for each.
    slots: Vec<(usize, usize)>,
}

fn run_cell(
    handle: &Handle,
    args: &Args,
    mode: Mode,
    topo: Topology,
) -> Result<CellResult, Box<dyn Error>> {
    // Fresh backend per cell so cells are independent and comparable.
    let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let backend: Arc<dyn Backend> = Arc::new(DelayBackend::new(inner, delay_profile(args)));
    let pool_size = mode.pool_size(args);

    // Seed the shared pool once on the shared backend (unmeasured), via a
    // throwaway Database whose stats do not touch the measurement DBs.
    seed_pool(handle, backend.clone(), pool_size)?;

    // Open the cell's Databases and build each shape's worker plan.
    // The DelayBackend compresses wall-clock by `delay_scale`; report latency
    // and throughput in the simulated (real-time-equivalent) domain by dividing
    // it back out (`--delay-scale` is validated > 0 in `main`).
    let time_scale = 1.0 / args.delay_scale;
    let w = args.workers_per_shape;
    let mut dbs: Vec<Database> = Vec::new();
    let mut plans: Vec<ShapePlan> = Vec::new();
    match topo {
        Topology::Shared => {
            let db = open_db(handle, backend.clone());
            dbs.push(db); // index 0
            for shape in SHAPES {
                plans.push(ShapePlan {
                    shape,
                    bench: Arc::new(Bench::with_time_scale(args.duration, time_scale)),
                    slots: vec![(0, w)],
                });
            }
        }
        Topology::PerShape => {
            for shape in SHAPES {
                let mut slots = Vec::new();
                for count in split_workers(w, args.clients_per_shape) {
                    let idx = dbs.len();
                    dbs.push(open_db(handle, backend.clone()));
                    slots.push((idx, count));
                }
                plans.push(ShapePlan {
                    shape,
                    bench: Arc::new(Bench::with_time_scale(args.duration, time_scale)),
                    slots,
                });
            }
        }
    }

    // Bracket each Database's stats around the measured window.
    let base: Vec<Stats> = dbs.iter().map(|d| d.stats()).collect();
    for p in &plans {
        p.bench.start();
    }

    let failures = Arc::new(AtomicU64::new(0));
    let run = handle.block_on(spawn_and_join(
        &dbs,
        &plans,
        pool_size,
        args.multi_keys,
        &failures,
    ));

    for p in &plans {
        p.bench.end();
    }
    let deltas: Vec<Stats> = dbs
        .iter()
        .enumerate()
        .map(|(i, d)| d.stats() - base[i])
        .collect();
    for d in &dbs {
        handle.block_on(d.shutdown());
    }
    run?;

    // Build per-shape rows; attribute ops per shape only in `per-shape`.
    let mut shapes = Vec::with_capacity(plans.len());
    for p in &plans {
        let res = p.bench.results();
        let count = res.samples.len();
        let secs = res.tot_duration.as_secs_f64();
        let (p50, p90) = if count > 0 {
            (
                res.percentile(0.5).as_secs_f64() * 1000.0,
                res.percentile(0.9).as_secs_f64() * 1000.0,
            )
        } else {
            (0.0, 0.0)
        };
        let ops = match topo {
            Topology::PerShape => {
                let raw = p.slots.iter().fold(RawOps::default(), |acc, &(idx, _)| {
                    acc.add(RawOps::of(deltas[idx]))
                });
                Some(raw.per_tx())
            }
            Topology::Shared => None,
        };
        shapes.push(ShapeResult {
            shape: p.shape.name().to_string(),
            committed: count,
            tx_per_sec: if secs > 0.0 { count as f64 / secs } else { 0.0 },
            p50_ms: p50,
            p90_ms: p90,
            ops,
        });
    }

    let aggregate_ops = match topo {
        Topology::Shared => Some(RawOps::of(deltas[0]).per_tx()),
        Topology::PerShape => None,
    };

    Ok(CellResult {
        mode: mode.label().to_string(),
        topology: topo.label().to_string(),
        databases: dbs.len(),
        failures: failures.load(Ordering::Relaxed),
        shapes,
        aggregate_ops,
    })
}

/// Spawns every shape's workers across the cell's Databases and awaits them.
async fn spawn_and_join(
    dbs: &[Database],
    plans: &[ShapePlan],
    pool_size: usize,
    multi_keys: usize,
    failures: &Arc<AtomicU64>,
) -> Result<(), GError> {
    let mut handles: Vec<JoinHandle<Result<(), GError>>> = Vec::new();
    let mut seed: u64 = 0x9E37_79B9_7F4A_7C15;
    for p in plans {
        for &(db_idx, count) in &p.slots {
            for _ in 0..count {
                let db = dbs[db_idx].clone();
                let bench = p.bench.clone();
                let shape = p.shape;
                let failures = failures.clone();
                seed = seed.wrapping_add(0x1000_0000_0000_0001);
                handles.push(tokio::spawn(worker(
                    db, shape, bench, pool_size, multi_keys, seed, failures,
                )));
            }
        }
    }
    join_tasks(handles).await
}

/// One worker: loops its shape's transaction until the shape's `Bench` window
/// closes, keying from the shared pool. Errors are counted, not fatal (a single
/// contention timeout must not derail the run).
async fn worker(
    db: Database,
    shape: Shape,
    bench: Arc<Bench>,
    pool_size: usize,
    multi_keys: usize,
    seed: u64,
    failures: Arc<AtomicU64>,
) -> Result<(), GError> {
    let coll = db.collection(COLL.as_bytes());
    let mut rng = StdRng::seed_from_u64(seed);
    let n = match shape {
        Shape::RwMany | Shape::RoMulti => multi_keys.min(pool_size).max(1),
        Shape::RwSingle | Shape::RoSingle => 1,
    };
    while !bench.is_finished() {
        // Pick keys before the measured region so the RNG borrow does not span
        // the transaction future.
        let idxs = pick_keys(&mut rng, pool_size, n);
        let keys: Vec<Vec<u8>> = idxs.iter().map(|&i| key_bytes(i)).collect();
        let keys = &keys;
        let coll = &coll;
        let db = &db;
        let res = bench
            .measure(|| async move {
                if shape.is_write() {
                    rmw_tx(db, coll, keys).await
                } else {
                    ro_tx(db, coll, keys).await
                }
            })
            .await;
        if res.is_err() {
            failures.fetch_add(1, Ordering::Relaxed);
        }
    }
    Ok(())
}

/// Picks `n` distinct pool indices (or the whole pool when `n >= pool_size`).
fn pick_keys(rng: &mut StdRng, pool_size: usize, n: usize) -> Vec<usize> {
    let n = n.min(pool_size).max(1);
    if n >= pool_size {
        return (0..pool_size).collect();
    }
    let mut set = HashSet::with_capacity(n);
    while set.len() < n {
        set.insert(rng.random_range(0..pool_size));
    }
    set.into_iter().collect()
}

/// Read-modify-write of every key (parallel reads, then a write-back each).
async fn rmw_tx(db: &Database, coll: &Collection, keys: &[Vec<u8>]) -> Result<(), GError> {
    db.tx(|tx| async move {
        let vals = join_all(keys.iter().map(|k| tx.read(coll, k))).await;
        for (k, rv) in keys.iter().zip(vals) {
            match rv {
                Ok(_) | Err(GError::NotFound) => {}
                Err(e) => return Err(e),
            }
            tx.write(coll, k, &value())?;
        }
        Ok(())
    })
    .await
}

/// Read-only over every key (in parallel).
async fn ro_tx(db: &Database, coll: &Collection, keys: &[Vec<u8>]) -> Result<(), GError> {
    db.tx(|tx| async move {
        let vals = join_all(keys.iter().map(|k| tx.read(coll, k))).await;
        for rv in vals {
            match rv {
                Ok(_) | Err(GError::NotFound) => {}
                Err(e) => return Err(e),
            }
        }
        Ok(())
    })
    .await
}

/// Seeds the shared collection with `pool_size` keys (batched, unmeasured).
fn seed_pool(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    pool_size: usize,
) -> Result<(), Box<dyn Error>> {
    let db = open_db(handle, backend);
    handle.block_on(async {
        let coll = db.collection(COLL.as_bytes());
        coll.create().await?;
        let mut i = 0;
        while i < pool_size {
            let end = (i + 100).min(pool_size);
            let batch: Vec<Vec<u8>> = (i..end).map(key_bytes).collect();
            let coll = &coll;
            let batch = &batch;
            db.tx(|tx| async move {
                for k in batch {
                    tx.write(coll, k, &value())?;
                }
                Ok(())
            })
            .await?;
            i = end;
        }
        Ok::<(), GError>(())
    })?;
    handle.block_on(db.shutdown());
    Ok(())
}

/// Awaits spawned worker tasks, returning the first error encountered.
async fn join_tasks(handles: Vec<JoinHandle<Result<(), GError>>>) -> Result<(), GError> {
    let mut result = Ok(());
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if result.is_ok() => result = Err(e),
            Ok(Err(_)) => {}
            Err(_) if result.is_ok() => result = Err(GError::internal("worker task panicked")),
            Err(_) => {}
        }
    }
    result
}

// ---------------------------------------------------------------------------
// Text output
// ---------------------------------------------------------------------------

fn emit_text(args: &Args, cells: &[CellResult]) {
    println!(
        "mixbench: duration={:?} workers/shape={} clients/shape(K)={} delays={} scale={} \
         num-keys={} hot-keys={} multi-keys={}",
        args.duration,
        args.workers_per_shape,
        args.clients_per_shape,
        args.delays,
        args.delay_scale,
        args.num_keys,
        args.hot_keys,
        args.multi_keys,
    );
    println!(
        "(latency & tx/s are simulated-time, compensated for --delay-scale; ops/tx are counts)"
    );
    for c in cells {
        println!();
        println!(
            "=== mode={} topology={} (dbs={}) ===",
            c.mode, c.topology, c.databases
        );
        let per_shape_ops = c.shapes.iter().any(|s| s.ops.is_some());
        if per_shape_ops {
            println!(
                "{:<10} {:>10} {:>9} {:>9} {:>9} {:>9} {:>9} {:>8} {:>10}",
                "shape",
                "tx/s",
                "p50ms",
                "p90ms",
                "reads/tx",
                "writes/tx",
                "lists/tx",
                "ops/tx",
                "retries/tx",
            );
            for s in &c.shapes {
                let o = s.ops.expect("per-shape ops present");
                println!(
                    "{:<10} {:>10.2} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>9.2} {:>8.2} {:>10.3}",
                    s.shape,
                    s.tx_per_sec,
                    s.p50_ms,
                    s.p90_ms,
                    o.obj_reads_per_tx,
                    o.obj_writes_per_tx,
                    o.obj_lists_per_tx,
                    o.total_ops_per_tx,
                    o.retries_per_tx,
                );
            }
        } else {
            println!(
                "{:<10} {:>10} {:>9} {:>9}",
                "shape", "tx/s", "p50ms", "p90ms"
            );
            for s in &c.shapes {
                println!(
                    "{:<10} {:>10.2} {:>9.2} {:>9.2}",
                    s.shape, s.tx_per_sec, s.p50_ms, s.p90_ms
                );
            }
            if let Some(o) = c.aggregate_ops {
                println!(
                    "aggregate ops/tx: reads={:.2} writes={:.2} lists={:.2} total={:.2} \
                     retries/tx={:.3} (txn={})",
                    o.obj_reads_per_tx,
                    o.obj_writes_per_tx,
                    o.obj_lists_per_tx,
                    o.total_ops_per_tx,
                    o.retries_per_tx,
                    o.txn,
                );
            }
        }
        if c.failures > 0 {
            println!("WARNING: {} transaction failures in this cell", c.failures);
        }
    }
}
