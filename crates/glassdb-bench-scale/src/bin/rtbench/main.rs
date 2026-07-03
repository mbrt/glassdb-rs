//! Measures database transaction performance under various concurrency
//! scenarios. Ported from the Go `hack/rtbench`.
//!
//! Runs against a simulated backend (in-memory + a GCS/S3 latency profile) or a
//! real cloud bucket:
//!
//! ```text
//! cargo run -p glassdb-bench-scale --bin rtbench -- --backend memory --test-name simple
//! BUCKET=my-bucket cargo run -p glassdb-bench-scale --bin rtbench -- --backend s3 --test-name rw9010
//! ```
//!
//! ## Concurrency model
//!
//! `Database::tx` takes the transaction body by value (`|tx| async move { ... }`), so
//! its future is `Send` and every worker is a `tokio::spawn`-ed task on a shared
//! multi-thread runtime. The runtime multiplexes all workers over its worker
//! threads (rather than one OS thread per worker), so a single shared S3 client
//! and reactor serve everyone — matching the Go design where all goroutines
//! share one client.
//!
//! ## Reported units
//!
//! For the simulated backends (`memory`, `fakes3`) `--delay-scale` compresses
//! wall-clock time. Reported transaction latency and throughput are rescaled
//! back to the simulated (real-time-equivalent) domain (see
//! [`report_time_scale`]), so they are comparable across `--delay-scale` values
//! and to the real `s3`/`gcs` backends. Per-transaction *counts* (retries,
//! backend ops) are scale-free already. The `client-stats.csv` diagnostics
//! (wall time, CPU, HTTP) stay in real wall-clock: they measure the actual
//! client process, not the simulated storage timeline.

mod clientmetrics;
mod cpu;

// musl's default allocator serializes multi-threaded allocation on a coarse
// lock, which collapses into a futex/`sys`-CPU storm under the benchmark's
// hundreds of concurrent workers (each S3 op churns HTTP/TLS buffers). mimalloc
// uses per-thread caches and removes that contention, matching glibc/Go.
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::error::Error;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use aws_smithy_runtime_api::client::http::SharedHttpClient;
use clap::Parser;
use rand::rngs::StdRng;
use rand::{Rng, RngExt, SeedableRng};
use tokio::runtime::Handle;

use glassdb::backend::memory::MemoryBackend;
use glassdb::middleware::{DelayBackend, DelayOptions, gcs_delays, s3_delays};
use glassdb::s3::{FakeS3, FakeS3Options, tuned_http_client};
use glassdb::{Collection, Database, Error as GError, Stats};
use glassdb_backend::Backend;
use glassdb_bench_scale::bench::{Bench, Results};

use clientmetrics::{HttpCounter, HttpMetrics, HttpSnapshot, ThreadSampler};

const READ_WRITE_9010_CNAME: &str = "read-write-9010";
const READ_WRITE_9010_NUM_CONCURR_TX: usize = 10;
const DEADLOCK_NUM_WRITERS: usize = 5;

#[derive(Parser)]
#[command(about = "Measure GlassDB transaction performance under concurrency")]
struct Args {
    /// Backend type. `fakes3` runs the real aws-sdk-s3 client against an
    /// in-process fake S3 server with simulated S3 latency (no AWS account
    /// required), so the real client transport can be profiled locally.
    #[arg(long, default_value = "memory", value_parser = ["memory", "gcs", "s3", "fakes3"])]
    backend: String,
    /// Delay profile for the memory backend.
    #[arg(long, default_value = "gcs", value_parser = ["gcs", "s3"])]
    delays: String,
    /// Enable throttling (per-object write limit) for the memory backend.
    #[arg(long, default_value_t = true, action = clap::ArgAction::Set)]
    enable_throttling: bool,
    /// Override per-prefix throttling depth for the memory backend (0 = profile
    /// default; higher = more partitions, more throughput).
    #[arg(long, default_value_t = 0)]
    prefix_depth: usize,
    /// Compresses the memory backend's simulated latencies and rate limits by
    /// this factor (`1.0` = real-time; e.g. `0.01` runs ~100x faster). The
    /// simulated request rates scale with it, so relative behavior is
    /// preserved; useful for quick local iteration. Must be > 0.
    #[arg(long, default_value_t = 1.0)]
    delay_scale: f64,
    /// Which test to run.
    #[arg(long, default_value = "simple", value_parser = ["simple", "rw9010", "deadlock"])]
    test_name: String,
    /// Transaction mix for the `rw9010` test: a named preset (`balanced` =
    /// 1,6,3, the default 10%-write mix; `readheavy` = 1,14,5; `writeheavy` =
    /// 6,3,1) or explicit `W,SR,WR` counts (writers, strong-readers,
    /// weak-readers). At least one writer is required (each worker's loop
    /// terminates on the write bench reaching its sample floor).
    #[arg(long, default_value = "balanced")]
    rw_mix: String,
    /// Output file with raw samples data.
    #[arg(long, default_value = "samples.csv")]
    samples_out: String,
    /// Output file with db stats.
    #[arg(long, default_value = "stats.csv")]
    stats_out: String,
    /// Output file with per-db throughput data.
    #[arg(long, default_value = "throughput.csv")]
    throughput_out: String,
    /// Output file with per-step client resource metrics.
    #[arg(long, default_value = "client-stats.csv")]
    client_stats_out: String,
    /// Output file with deadlock latency samples.
    #[arg(long, default_value = "deadlock.csv")]
    deadlock_out: String,
    /// Max concurrent Databases for the rw9010 test.
    #[arg(long, default_value_t = 50)]
    max_dbs: usize,
    /// Number of keys for the rw9010 test.
    #[arg(long, default_value_t = 50000)]
    num_keys: usize,
    /// Duration of each rw9010 / deadlock step.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    duration: Duration,
    /// Repeat the parameter sweep this many times. The expensive setup (e.g.
    /// rw9010's 50k-key init) happens once and is shared across runs; only
    /// the measured sweeps are repeated. All runs append to the same CSV
    /// outputs, which the plot script aggregates into tighter percentile
    /// bands. Useful against real S3, where back-to-back repeats give the
    /// service time to settle and reduce per-run variance.
    #[arg(long, default_value_t = 1)]
    num_runs: usize,
    /// Sleep this long between repeated sweeps (only when --num-runs > 1).
    /// Lets S3's prefix throttling / connection state quiesce before the
    /// next run begins.
    #[arg(long, default_value = "0s", value_parser = glassdb_bench_scale::parse_duration)]
    run_cooldown: Duration,
    /// Explicit concurrency points (number of Databases) for the rw9010 sweep,
    /// e.g. `--db-list=20,30,40`. Overrides the default 1,5,10,..,max-dbs sweep
    /// so a focused run can hammer just the concurrency band of interest, for a
    /// much tighter iterate-and-measure loop when tuning S3 performance.
    #[arg(long, value_delimiter = ',')]
    db_list: Vec<usize>,
    /// HTTP connection-pool strategy for the `s3` / `fakes3` backends. `tuned`
    /// (default) keeps idle connections warm for reuse (the `tuned_http_client`
    /// path); `default` uses the SDK's stock client; `churn` (fakes3 only) reaps
    /// idle connections after 1ms to exaggerate connection churn.
    #[arg(long, default_value = "tuned", value_parser = ["default", "tuned", "churn"])]
    http_pool: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let handle = rt.handle().clone();

    let setup = rt.block_on(init_backend(&args))?;
    let time_scale = report_time_scale(&args);

    match args.test_name.as_str() {
        "simple" => run_simple(&handle, setup.backend, time_scale)?,
        "rw9010" => run_read_write_9010(
            &handle,
            &args,
            setup.backend,
            setup.http,
            setup.server_conns,
            time_scale,
        )?,
        "deadlock" => run_deadlock(&handle, &args, setup.backend, time_scale)?,
        other => return Err(format!("unknown test name {other:?}").into()),
    }
    Ok(())
}

/// Multiplier applied to measured latencies and throughput so the reported
/// numbers are in the simulated (real-time-equivalent) domain rather than the
/// compressed wall-clock one. The simulated backends (`memory` via
/// `DelayBackend`, `fakes3` via the injected `FakeS3` latency) sleep at
/// `delay_scale * real`, so we divide it back out; the real cloud backends
/// (`s3`, `gcs`) run in real time and need no compensation.
fn report_time_scale(args: &Args) -> f64 {
    match args.backend.as_str() {
        "memory" | "fakes3" if args.delay_scale > 0.0 && args.delay_scale.is_finite() => {
            1.0 / args.delay_scale
        }
        _ => 1.0,
    }
}

/// What [`init_backend`] returns: the backend plus the optional client-side HTTP
/// metrics and, for `fakes3`, the server-side accepted-connection counter that
/// stands in for the `new-conns` column the SDK does not surface.
struct BackendSetup {
    backend: Arc<dyn Backend>,
    http: Option<Arc<HttpMetrics>>,
    server_conns: Option<Arc<AtomicU64>>,
}

// ---------------------------------------------------------------------------
// Backend / Database setup
// ---------------------------------------------------------------------------

async fn init_backend(args: &Args) -> Result<BackendSetup, Box<dyn Error>> {
    match args.backend.as_str() {
        "memory" => {
            let mut delays = memory_delay_profile(args)?;
            if !args.enable_throttling {
                // Effectively disable the per-object write limit.
                delays.same_obj_write_ps = 100_000;
            }
            let inner: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
            let backend: Arc<dyn Backend> = Arc::new(DelayBackend::new(inner, delays));
            Ok(BackendSetup {
                backend,
                http: None,
                server_conns: None,
            })
        }
        "gcs" => {
            let bucket = env_var("BUCKET")?;
            let backend: Arc<dyn Backend> = Arc::new(glassdb::gcs::GcsBackend::new(bucket));
            Ok(BackendSetup {
                backend,
                http: None,
                server_conns: None,
            })
        }
        "s3" => {
            let bucket = env_var("BUCKET")?;
            let metrics = Arc::new(HttpMetrics::default());
            // The tuned HTTP client steers the SDK toward connection reuse
            // (rustls + ALPN-negotiated HTTP/2), so high-concurrency steps
            // don't pile DNS+TLS handshakes onto tokio's blocking pool.
            let mut loader = aws_config::defaults(aws_config::BehaviorVersion::latest());
            if let Some(hc) = s3_http_client(&args.http_pool) {
                loader = loader.http_client(hc);
            }
            let base = loader.load().await;
            let conf = aws_sdk_s3::config::Builder::from(&base)
                .interceptor(HttpCounter::new(metrics.clone()))
                .build();
            let client = aws_sdk_s3::Client::from_conf(conf);
            let backend: Arc<dyn Backend> = Arc::new(glassdb::s3::S3Backend::new(client, bucket));
            Ok(BackendSetup {
                backend,
                http: Some(metrics),
                server_conns: None,
            })
        }
        "fakes3" => init_fakes3(args).await,
        other => Err(format!("unknown backend type {other:?}").into()),
    }
}

/// Wires the real aws-sdk-s3 client to an in-process [`FakeS3`] server with
/// simulated S3 latency. This exercises the full client transport (SDK → smithy
/// → hyper connection pool → loopback TCP) without an AWS account, so the
/// connection-pool / head-of-line behavior under concurrency can be iterated on
/// locally. The fake serves plain HTTP/1.1, so the client must too (see
/// `--http-pool`); a TLS + ALPN-h2 listener for the exact `tuned_http_client`
/// path is a possible follow-up.
async fn init_fakes3(args: &Args) -> Result<BackendSetup, Box<dyn Error>> {
    if !(args.delay_scale > 0.0 && args.delay_scale.is_finite()) {
        return Err(format!("--delay-scale must be > 0, got {}", args.delay_scale).into());
    }
    let mut delays = s3_delays();
    delays.scale = args.delay_scale;

    let conns = Arc::new(AtomicU64::new(0));
    let fake = FakeS3::start_with(FakeS3Options {
        latency: Some(delays),
        conn_counter: Some(conns.clone()),
    })
    .await;

    let metrics = Arc::new(HttpMetrics::default());
    let mut conf = fake
        .client_config()
        .interceptor(HttpCounter::new(metrics.clone()));
    if let Some(hc) = fakes3_http_client(&args.http_pool) {
        conf = conf.http_client(hc);
    }
    let client = aws_sdk_s3::Client::from_conf(conf.build());
    let backend: Arc<dyn Backend> = Arc::new(glassdb::s3::S3Backend::new(client, "bench"));
    Ok(BackendSetup {
        backend,
        http: Some(metrics),
        server_conns: Some(conns),
    })
}

/// HTTP client override for the real `s3` backend. `tuned` returns the
/// connection-reusing TLS client; everything else returns `None` (SDK default).
fn s3_http_client(pool: &str) -> Option<SharedHttpClient> {
    match pool {
        "tuned" => Some(tuned_http_client()),
        // "default" (and "churn", which only applies to the plaintext fake).
        _ => None,
    }
}

/// HTTP client override for the plaintext `fakes3` backend. `default` returns
/// `None` (SDK stock client); the others return a plaintext client whose pool
/// idle timeout governs connection reuse vs. churn.
fn fakes3_http_client(pool: &str) -> Option<SharedHttpClient> {
    match pool {
        "default" => None,
        "churn" => Some(plaintext_http_client(Some(Duration::from_millis(1)))),
        // "tuned": keep idle connections warm, mirroring `tuned_http_client`.
        _ => Some(plaintext_http_client(Some(Duration::from_secs(90)))),
    }
}

/// A plaintext (no-TLS) HTTP client whose `pool_idle_timeout` governs how long
/// idle keep-alive connections are retained: a long value maximizes reuse, a
/// very short one forces the pool to drop and re-open connections (churn). This
/// is the TLS-free analog of the s3 backend's `tuned_http_client`, used to A/B
/// the connection pool locally against the plaintext `fakes3` server.
fn plaintext_http_client(pool_idle_timeout: Option<Duration>) -> SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .pool_idle_timeout(pool_idle_timeout)
        .build_http()
}

/// Selects which simulated-latency profile the in-memory backend emulates,
/// applying the optional prefix-depth override.
fn memory_delay_profile(args: &Args) -> Result<DelayOptions, Box<dyn Error>> {
    let mut delays = match args.delays.as_str() {
        "gcs" => gcs_delays(),
        "s3" => s3_delays(),
        other => return Err(format!("unknown delay profile {other:?}").into()),
    };
    if args.prefix_depth > 0 {
        delays.prefix_depth = args.prefix_depth;
    }
    if !(args.delay_scale > 0.0 && args.delay_scale.is_finite()) {
        return Err(format!("--delay-scale must be > 0, got {}", args.delay_scale).into());
    }
    delays.scale = args.delay_scale;
    Ok(delays)
}

fn env_var(k: &str) -> Result<String, Box<dyn Error>> {
    match std::env::var(k) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(format!("environment variable ${k} is required").into()),
    }
}

fn open_db(handle: &Handle, backend: Arc<dyn Backend>) -> Database {
    handle
        .block_on(Database::open("bench", backend))
        .expect("open db")
}

// ---------------------------------------------------------------------------
// Value helpers
// ---------------------------------------------------------------------------

fn rand_1k() -> Vec<u8> {
    let mut b = vec![0u8; 1024];
    rand::rng().fill_bytes(&mut b);
    b
}

/// In-place Fisher-Yates shuffle driven by a seeded RNG (so a run is
/// reproducible without depending on rand's `SliceRandom` API surface).
fn shuffle<T>(rng: &mut StdRng, v: &mut [T]) {
    for i in (1..v.len()).rev() {
        let j = rng.random_range(0..=i);
        v.swap(i, j);
    }
}

// ---------------------------------------------------------------------------
// Benchmarker (shared by `simple` and `deadlock`)
// ---------------------------------------------------------------------------

struct Benchmarker {
    bench: Arc<Bench>,
    num_keys: usize,
    num_workers: usize,
    num_keys_per_worker: usize,
    base_stats: Stats,
    delta_stats: Stats,
}

impl Benchmarker {
    fn new(duration: Duration, time_scale: f64) -> Self {
        Benchmarker {
            bench: Arc::new(Bench::with_time_scale(duration, time_scale)),
            num_keys: 0,
            num_workers: 0,
            num_keys_per_worker: 0,
            base_stats: Stats::default(),
            delta_stats: Stats::default(),
        }
    }

    fn start(
        &mut self,
        db: &Database,
        num_keys: usize,
        num_workers: usize,
        num_keys_per_worker: usize,
    ) {
        self.num_keys = num_keys;
        self.num_workers = num_workers;
        self.num_keys_per_worker = num_keys_per_worker;
        self.base_stats = db.stats();
        self.bench.start();
    }

    fn end(&mut self, db: &Database) {
        self.bench.end();
        self.delta_stats = db.stats() - self.base_stats;
    }

    fn results_row(&self) -> String {
        let res = self.bench.results();
        let txs = if res.tot_duration.is_zero() {
            0.0
        } else {
            res.samples.len() as f64 / res.tot_duration.as_secs_f64()
        };
        [
            fmt_ms(res.tot_duration),
            self.num_keys.to_string(),
            self.num_workers.to_string(),
            self.num_keys_per_worker.to_string(),
            self.delta_stats.tx_n.to_string(),
            self.delta_stats.tx_retries.to_string(),
            fmt_float(txs),
            fmt_ms(res.avg()),
            fmt_ms(res.percentile(0.25)),
            fmt_ms(res.percentile(0.5)),
            fmt_ms(res.percentile(0.9)),
            fmt_ms(res.percentile(0.95)),
        ]
        .join(",")
    }
}

// ---------------------------------------------------------------------------
// `simple` scenario
// ---------------------------------------------------------------------------

fn run_simple(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    time_scale: f64,
) -> Result<(), Box<dyn Error>> {
    println!(
        "{}",
        [
            "test name",
            "tot time",
            "num keys",
            "num workers",
            "num keys/worker",
            "num tx",
            "num retries",
            "tx/sec",
            "avg tx time",
            "25% tx time",
            "50% tx time",
            "90% tx time",
            "95% tx time",
        ]
        .join(",")
    );

    let mut i = 0;
    while i <= 100 {
        let numw = i.max(1);
        let name = format!("IndepSingleRMW ({numw}w)");
        run_test(
            handle,
            &name,
            backend.clone(),
            Duration::ZERO,
            time_scale,
            |b, db, h| independent_single_rmw(b, db, h, numw),
        )?;
        i += 5;
    }

    let mut i = 0;
    while i <= 50 {
        let numk = i.max(2);
        let mut j = 0;
        while j <= 100 {
            let numw = j.max(1);
            let name = format!("IndepMultiRMW ({numw}w{numk}k)");
            run_test(
                handle,
                &name,
                backend.clone(),
                Duration::ZERO,
                time_scale,
                |b, db, h| independent_multi_rmw(b, db, h, numw, numk),
            )?;
            j += 5;
        }
        i += 10;
    }

    for numw in 2..=5 {
        let mut i = 0;
        while i <= 6 {
            let numkpw = i.max(1);
            let mut j = 0;
            while j <= numkpw {
                let nover = j.max(1);
                let name = format!("OverlapMultiRMW ({numw}w{numkpw}kpw{nover}ko)");
                run_test(
                    handle,
                    &name,
                    backend.clone(),
                    Duration::ZERO,
                    time_scale,
                    |b, db, h| overlapping_multi_rmw(b, db, h, numw, numkpw, nover),
                )?;
                j += 2;
            }
            i += 2;
        }
    }

    Ok(())
}

fn run_test<F>(
    handle: &Handle,
    name: &str,
    backend: Arc<dyn Backend>,
    duration: Duration,
    time_scale: f64,
    f: F,
) -> Result<(), Box<dyn Error>>
where
    F: FnOnce(&mut Benchmarker, &Database, &Handle) -> Result<(), GError>,
{
    let db = open_db(handle, backend);
    let mut ben = Benchmarker::new(duration, time_scale);
    let res = f(&mut ben, &db, handle);
    handle.block_on(db.shutdown());
    res?;
    println!("{name},{}", ben.results_row());
    Ok(())
}

fn independent_single_rmw(
    b: &mut Benchmarker,
    db: &Database,
    handle: &Handle,
    nwriters: usize,
) -> Result<(), GError> {
    b.start(db, nwriters, nwriters, 1);
    let handles: Vec<_> = (0..nwriters)
        .map(|i| {
            let db = db.clone();
            let bench = b.bench.clone();
            handle.spawn(async move {
                let coll = db.collection(format!("c{i}").as_bytes());
                coll.create().await?;
                let coll = &coll;
                let db = &db;
                while !bench.is_finished() {
                    bench
                        .measure(|| async move {
                            db.tx(|tx| async move {
                                let v = match tx.read(coll, b"key").await {
                                    Ok(v) => v,
                                    Err(GError::NotFound) => Vec::new(),
                                    Err(e) => return Err(e),
                                };
                                tx.write(coll, b"key", &v)
                            })
                            .await
                        })
                        .await?;
                }
                Ok::<(), GError>(())
            })
        })
        .collect();
    let result = handle.block_on(join_tasks(handles));
    b.end(db);
    result
}

fn independent_multi_rmw(
    b: &mut Benchmarker,
    db: &Database,
    handle: &Handle,
    nwriters: usize,
    numkeys: usize,
) -> Result<(), GError> {
    b.start(db, numkeys * nwriters, nwriters, numkeys);
    let handles: Vec<_> = (0..nwriters)
        .map(|i| {
            let db = db.clone();
            let bench = b.bench.clone();
            handle.spawn(async move {
                let coll = db.collection(format!("c{i}").as_bytes());
                coll.create().await?;
                let keys: Vec<Vec<u8>> = (0..numkeys)
                    .map(|j| format!("key{j}").into_bytes())
                    .collect();
                let coll = &coll;
                let db = &db;
                let keys = &keys;
                while !bench.is_finished() {
                    bench
                        .measure(|| async move {
                            db.tx(|tx| async move {
                                // Read all keys in parallel, then write each back.
                                let vals = futures::future::join_all(
                                    keys.iter().map(|k| tx.read(coll, k)),
                                )
                                .await;
                                for (k, rv) in keys.iter().zip(vals) {
                                    let v = match rv {
                                        Ok(v) => v,
                                        Err(GError::NotFound) => Vec::new(),
                                        Err(e) => return Err(e),
                                    };
                                    tx.write(coll, k, &v)?;
                                }
                                Ok(())
                            })
                            .await
                        })
                        .await?;
                }
                Ok::<(), GError>(())
            })
        })
        .collect();
    let result = handle.block_on(join_tasks(handles));
    b.end(db);
    result
}

fn overlapping_multi_rmw(
    b: &mut Benchmarker,
    db: &Database,
    handle: &Handle,
    n_writers: usize,
    n_keys_per_writer: usize,
    n_overlap: usize,
) -> Result<(), GError> {
    let coll = db.collection(b"omrmw");
    handle.block_on(async {
        let _ = coll.create().await;
    });

    let n_keys = n_writers * n_keys_per_writer - n_overlap;
    let all_keys: Vec<Vec<u8>> = (0..n_keys)
        .map(|i| format!("key{i}").into_bytes())
        .collect();
    // Initialize all keys beforehand.
    handle.block_on(async {
        let coll = &coll;
        let all_keys = &all_keys;
        db.tx(|tx| async move {
            for k in all_keys {
                tx.write(coll, k.as_slice(), &rand_1k())?;
            }
            Ok(())
        })
        .await
    })?;

    b.start(db, n_keys, n_writers, n_keys_per_writer);
    let n_unique = n_keys_per_writer - n_overlap;
    let handles: Vec<_> = (0..n_writers)
        .map(|i| {
            // Overlapping keys first, then this worker's unique slice.
            let mut keys: Vec<Vec<u8>> = Vec::with_capacity(n_keys_per_writer);
            keys.extend_from_slice(&all_keys[..n_overlap]);
            let start = n_overlap + i * n_unique;
            keys.extend_from_slice(&all_keys[start..start + n_unique]);
            let db = db.clone();
            let coll = coll.clone();
            let bench = b.bench.clone();
            handle.spawn(async move {
                let coll = &coll;
                let db = &db;
                let keys = &keys;
                while !bench.is_finished() {
                    bench
                        .measure(|| async move {
                            db.tx(|tx| async move {
                                let vals = futures::future::join_all(
                                    keys.iter().map(|k| tx.read(coll, k)),
                                )
                                .await;
                                for (k, rv) in keys.iter().zip(vals) {
                                    let v = rv?;
                                    tx.write(coll, k, &v)?;
                                }
                                Ok(())
                            })
                            .await
                        })
                        .await?;
                }
                Ok::<(), GError>(())
            })
        })
        .collect();
    let result = handle.block_on(join_tasks(handles));
    b.end(db);
    result
}

/// Awaits spawned worker tasks, returning the first error encountered.
async fn join_tasks(
    handles: Vec<tokio::task::JoinHandle<Result<(), GError>>>,
) -> Result<(), GError> {
    let mut result = Ok(());
    for h in handles {
        match h.await {
            Ok(Ok(())) => {}
            Ok(Err(e)) if result.is_ok() => result = Err(e),
            Ok(Err(_)) => {}
            Err(_) if result.is_ok() => {
                result = Err(GError::internal("worker task panicked"));
            }
            Err(_) => {}
        }
    }
    result
}

// ---------------------------------------------------------------------------
// `rw9010` scenario
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
enum TransactionType {
    Write,
    ReadStrong,
    ReadWeak,
}

fn make_tx_series(num_w: usize, num_strong_r: usize, num_weak_r: usize) -> Vec<TransactionType> {
    let mut v = Vec::with_capacity(num_w + num_strong_r + num_weak_r);
    v.extend(std::iter::repeat_n(TransactionType::Write, num_w));
    v.extend(std::iter::repeat_n(
        TransactionType::ReadStrong,
        num_strong_r,
    ));
    v.extend(std::iter::repeat_n(TransactionType::ReadWeak, num_weak_r));
    v
}

/// The per-worker (writers, strong-readers, weak-readers) counts for the
/// `rw9010` test, as set by `--rw-mix`.
type RwMix = (usize, usize, usize);

/// Parses the `--rw-mix` value: either a named preset or explicit
/// `W,SR,WR` counts. At least one writer is required because each worker's loop
/// runs until the *write* bench reaches its sample floor; a zero-writer mix
/// would never terminate.
fn parse_rw_mix(s: &str) -> Result<RwMix, String> {
    let mix = match s.trim() {
        "balanced" | "rw9010" => (1, 6, 3),
        "readheavy" => (1, 14, 5),
        "writeheavy" => (6, 3, 1),
        other => {
            let parts: Vec<&str> = other.split(',').collect();
            if parts.len() != 3 {
                return Err(format!(
                    "invalid --rw-mix {s:?}: expected a preset \
                     (balanced|readheavy|writeheavy) or W,SR,WR"
                ));
            }
            let n = |p: &str| {
                p.trim()
                    .parse::<usize>()
                    .map_err(|_| format!("invalid --rw-mix count {p:?}"))
            };
            (n(parts[0])?, n(parts[1])?, n(parts[2])?)
        }
    };
    if mix.0 == 0 {
        return Err(format!(
            "invalid --rw-mix {s:?}: at least one writer is required"
        ));
    }
    Ok(mix)
}

/// The three per-DB benches (one per transaction type).
struct DbBench {
    write: Bench,
    strong: Bench,
    weak: Bench,
}

impl DbBench {
    fn new(duration: Duration, time_scale: f64) -> Self {
        DbBench {
            write: Bench::with_time_scale(duration, time_scale),
            strong: Bench::with_time_scale(duration, time_scale),
            weak: Bench::with_time_scale(duration, time_scale),
        }
    }

    fn start(&self) {
        self.write.start();
        self.strong.start();
        self.weak.start();
    }

    fn end(&self) {
        self.write.end();
        self.strong.end();
        self.weak.end();
    }
}

struct DbResults {
    stats: Stats,
    write: Results,
    strong: Results,
    weak: Results,
}

/// The concurrency points (number of Databases) the rw9010 sweep visits:
/// the explicit `--db-list` when given, otherwise the default
/// `1,5,10,..,max-dbs` ramp.
fn db_steps(args: &Args) -> Vec<usize> {
    if !args.db_list.is_empty() {
        return args.db_list.clone();
    }
    let mut v = vec![1usize];
    let mut i = 5;
    while i <= args.max_dbs {
        v.push(i);
        i += 5;
    }
    v
}

fn run_read_write_9010(
    handle: &Handle,
    args: &Args,
    backend: Arc<dyn Backend>,
    metrics: Option<Arc<HttpMetrics>>,
    server_conns: Option<Arc<AtomicU64>>,
    time_scale: f64,
) -> Result<(), Box<dyn Error>> {
    // Parse the transaction mix up front so a bad --rw-mix fails before the
    // expensive key initialization.
    let mix = parse_rw_mix(&args.rw_mix)?;
    eprintln!(
        "Transaction mix (writers,strong-readers,weak-readers): {},{},{}",
        mix.0, mix.1, mix.2
    );

    eprintln!("Initialize keys");
    let keys = init_keys(handle, backend.clone(), args.num_keys)?;
    eprintln!("End of keys initialization");

    let mut samples = create_csv(&args.samples_out, "num-db,db,tx-type,ops,latency\n")?;
    let mut stats = create_csv(
        &args.stats_out,
        "num-db,db,num-tx,num-retries,obj-write,obj-read\n",
    )?;
    let mut throughput = create_csv(
        &args.throughput_out,
        "num-db,db,tx-type,count,duration-ms,tx-per-sec\n",
    )?;
    let mut client = create_csv(
        &args.client_stats_out,
        "num-db,wall-ms,num-cpu,cpu-user-ms,cpu-sys-ms,cpu-util-pct,\
         http-requests,http-throttle,http-5xx,http-2xx,new-conns,max-goroutines,tx-failures\n",
    )?;

    // Deterministic random source, for reproducibility. Re-seeded from the
    // same value at the start of each repeated sweep so every run picks the
    // same per-DB seeds and key access patterns.
    let num_runs = args.num_runs.max(1);
    for run in 0..num_runs {
        if run > 0 {
            eprintln!(
                "Sleeping {:?} before run {}/{num_runs}",
                args.run_cooldown,
                run + 1
            );
            handle.block_on(async { tokio::time::sleep(args.run_cooldown).await });
        }
        eprintln!("Run {}/{num_runs}", run + 1);
        let mut rnd = StdRng::seed_from_u64(42);
        for &numdb in &db_steps(args) {
            eprintln!("Testing {numdb} dbs");
            run_read_write_9010_step(
                handle,
                args,
                &backend,
                metrics.as_ref(),
                server_conns.as_ref(),
                &keys,
                numdb,
                mix,
                time_scale,
                &mut rnd,
                &mut samples,
                &mut stats,
                &mut throughput,
                &mut client,
            )?;
        }
    }

    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_read_write_9010_step(
    handle: &Handle,
    args: &Args,
    backend: &Arc<dyn Backend>,
    metrics: Option<&Arc<HttpMetrics>>,
    server_conns: Option<&Arc<AtomicU64>>,
    keys: &[Vec<u8>],
    numdb: usize,
    mix: RwMix,
    time_scale: f64,
    rnd: &mut StdRng,
    samples: &mut impl Write,
    stats: &mut impl Write,
    throughput: &mut impl Write,
    client: &mut impl Write,
) -> Result<(), Box<dyn Error>> {
    let http_before = metrics.map(|m| m.snapshot()).unwrap_or_default();
    let conns_before = server_conns.map(|c| c.load(Ordering::Relaxed)).unwrap_or(0);
    let (user_before, sys_before) = cpu::process_cpu_time();
    let sampler = ThreadSampler::start();
    let wall_start = std::time::Instant::now();

    let (results, failures) =
        match read_write_9010_all_dbs(handle, args, backend, keys, numdb, mix, time_scale, rnd) {
            Ok(r) => r,
            Err(e) => {
                // Individual failed transactions are counted and dropped, not
                // propagated. Reaching here means an unexpected fatal error (e.g. a
                // worker panic): warn and skip just this concurrency point rather
                // than discarding the whole sweep.
                let _ = sampler.stop_and_peak();
                eprintln!("WARNING: step num-db={numdb} failed, skipping: {e}");
                return Ok(());
            }
        };

    let wall = wall_start.elapsed();
    let (user_after, sys_after) = cpu::process_cpu_time();
    let peak_threads = sampler.stop_and_peak();
    let mut http_delta = metrics
        .map(|m| m.snapshot().sub(http_before))
        .unwrap_or_default();
    if let Some(c) = server_conns {
        // The SDK does not surface new connections; the fake S3 server counts
        // accepted TCP connections instead, so `new-conns` is meaningful here.
        http_delta.new_conns = c.load(Ordering::Relaxed).saturating_sub(conns_before) as i64;
    }

    dump_samples(samples, &results, numdb)?;
    dump_stats(stats, &results, numdb)?;
    dump_throughput(throughput, &results, numdb)?;
    dump_client_stats(
        client,
        numdb,
        wall,
        user_after - user_before,
        sys_after - sys_before,
        http_delta,
        peak_threads,
        failures,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn read_write_9010_all_dbs(
    handle: &Handle,
    args: &Args,
    backend: &Arc<dyn Backend>,
    keys: &[Vec<u8>],
    numdb: usize,
    mix: RwMix,
    time_scale: f64,
    rnd: &mut StdRng,
) -> Result<(Vec<DbResults>, u64), Box<dyn Error>> {
    // One seed per Database, derived up front from the shared source.
    let seeds: Vec<u64> = (0..numdb).map(|_| rnd.random()).collect();

    // Open all Databases and create their per-type benches.
    let dbs: Vec<Database> = (0..numdb)
        .map(|_| open_db(handle, backend.clone()))
        .collect();
    let benches: Vec<Arc<DbBench>> = (0..numdb)
        .map(|_| Arc::new(DbBench::new(args.duration, time_scale)))
        .collect();
    for db_bench in &benches {
        db_bench.start();
    }
    let keys: Arc<[Vec<u8>]> = Arc::from(keys);
    // Counts transactions that errored out during the step (shared across all
    // workers). Such transactions are dropped from the latency samples but
    // tracked here so the failure noise is reported per step.
    let failures = Arc::new(AtomicU64::new(0));

    // Run every worker of every Database concurrently as spawned tasks, so the shared
    // runtime multiplexes them over its worker threads.
    let mut worker_handles = Vec::new();
    for (di, db) in dbs.iter().enumerate() {
        let mut db_rng = StdRng::seed_from_u64(seeds[di]);
        let wseeds: Vec<u64> = (0..READ_WRITE_9010_NUM_CONCURR_TX)
            .map(|_| db_rng.random())
            .collect();
        for &wseed in &wseeds {
            let db = db.clone();
            let db_bench = benches[di].clone();
            let keys = keys.clone();
            worker_handles.push(handle.spawn(read_write_9010_worker(
                db,
                db_bench,
                keys,
                failures.clone(),
                mix,
                wseed,
            )));
        }
    }
    let run = handle.block_on(join_tasks(worker_handles));

    for db_bench in &benches {
        db_bench.end();
    }

    // Collect results and close Databases.
    let mut results = Vec::with_capacity(numdb);
    for (di, db) in dbs.iter().enumerate() {
        results.push(DbResults {
            stats: db.stats(),
            write: benches[di].write.results(),
            strong: benches[di].strong.results(),
            weak: benches[di].weak.results(),
        });
        handle.block_on(db.shutdown());
    }

    run?;
    Ok((results, failures.load(Ordering::Relaxed)))
}

async fn read_write_9010_worker(
    db: Database,
    db_bench: Arc<DbBench>,
    keys: Arc<[Vec<u8>]>,
    failures: Arc<AtomicU64>,
    mix: RwMix,
    seed: u64,
) -> Result<(), GError> {
    let mut rng = StdRng::seed_from_u64(seed);
    let coll = db.collection(READ_WRITE_9010_CNAME.as_bytes());
    // Per-worker transaction mix (writers, strong-readers, weak-readers),
    // selected by `--rw-mix` (default 1,6,3 = the 10%-write rw9010 mix).
    let (num_w, num_strong_r, num_weak_r) = mix;
    let mut series = make_tx_series(num_w, num_strong_r, num_weak_r);

    while !db_bench.write.is_finished() {
        shuffle(&mut rng, &mut series);
        for tt in &series {
            // Pick keys before the measured region so the RNG borrow does not
            // span the transaction future.
            let i0 = rng.random_range(0..keys.len());
            let i1 = rng.random_range(0..keys.len());
            let res = match tt {
                TransactionType::Write => {
                    db_bench
                        .write
                        .measure(|| write_tx(&db, &coll, &keys[i0], &keys[i1]))
                        .await
                }
                TransactionType::ReadStrong => {
                    db_bench
                        .strong
                        .measure(|| read_tx(&db, &coll, &keys[i0], &keys[i1]))
                        .await
                }
                TransactionType::ReadWeak => {
                    db_bench
                        .weak
                        .measure(|| weak_read_tx(&coll, &keys[i0]))
                        .await
                }
            };
            if res.is_err() {
                // Count transaction failures but keep going, so the step's overall
                // progress is not derailed by individual errors.
                failures.fetch_add(1, Ordering::Relaxed);
            }
        }
    }
    Ok(())
}

async fn write_tx(db: &Database, coll: &Collection, k0: &[u8], k1: &[u8]) -> Result<(), GError> {
    db.tx(|tx| async move {
        // Read both keys in parallel, then swap their values.
        let (r0, r1) = tokio::join!(tx.read(coll, k0), tx.read(coll, k1));
        let v0 = r0?;
        let v1 = r1?;
        tx.write(coll, k0, &v1)?;
        tx.write(coll, k1, &v0)?;
        Ok(())
    })
    .await
}

async fn read_tx(db: &Database, coll: &Collection, k0: &[u8], k1: &[u8]) -> Result<(), GError> {
    db.tx(|tx| async move {
        let (r0, r1) = tokio::join!(tx.read(coll, k0), tx.read(coll, k1));
        r0?;
        r1?;
        Ok(())
    })
    .await
}

async fn weak_read_tx(coll: &Collection, k: &[u8]) -> Result<(), GError> {
    coll.read_stale(k, Duration::from_secs(10))
        .await
        .map(|_| ())
}

fn init_keys(
    handle: &Handle,
    backend: Arc<dyn Backend>,
    num: usize,
) -> Result<Vec<Vec<u8>>, Box<dyn Error>> {
    if !num.is_multiple_of(100) {
        return Err("num must be multiple of 100".into());
    }
    let db = open_db(handle, backend);
    let keys = handle.block_on(async {
        let coll = db.collection(READ_WRITE_9010_CNAME.as_bytes());
        coll.create().await?;
        let mut keys: Vec<Vec<u8>> = vec![Vec::new(); num];
        // Initialize in batches of 100 keys.
        let mut i = 0;
        while i < num {
            eprintln!("keys {i} - {}", i + 100);
            // Build the batch up front so the (rerun-on-conflict) tx closure
            // only borrows it, rather than mutating a captured vector.
            let batch: Vec<Vec<u8>> = (0..100)
                .map(|j| format!("key{}", i + j).into_bytes())
                .collect();
            let batch_ref = &batch;
            let coll_ref = &coll;
            db.tx(|tx| async move {
                for k in batch_ref {
                    tx.write(coll_ref, k, &rand_1k())?;
                }
                Ok(())
            })
            .await?;
            for (j, k) in batch.into_iter().enumerate() {
                keys[i + j] = k;
            }
            i += 100;
        }
        Ok::<Vec<Vec<u8>>, GError>(keys)
    })?;
    // Give the db some time to flush the keys.
    handle.block_on(async {
        tokio::time::sleep(Duration::from_secs(1)).await;
    });
    handle.block_on(db.shutdown());
    Ok(keys)
}

// ---------------------------------------------------------------------------
// `deadlock` scenario
// ---------------------------------------------------------------------------

fn run_deadlock(
    handle: &Handle,
    args: &Args,
    backend: Arc<dyn Backend>,
    time_scale: f64,
) -> Result<(), Box<dyn Error>> {
    let mut out = create_csv(
        &args.deadlock_out,
        "num-keys,overlap,overlap-pct,latency-ms\n",
    )?;

    let num_runs = args.num_runs.max(1);
    for run in 0..num_runs {
        if run > 0 {
            eprintln!(
                "Sleeping {:?} before run {}/{num_runs}",
                args.run_cooldown,
                run + 1
            );
            handle.block_on(async { tokio::time::sleep(args.run_cooldown).await });
        }
        eprintln!("Run {}/{num_runs}", run + 1);
        for k in 1..=6usize {
            for overlap in 1..=k {
                let db = open_db(handle, backend.clone());
                let mut ben = Benchmarker::new(args.duration, time_scale);
                let res =
                    overlapping_multi_rmw(&mut ben, &db, handle, DEADLOCK_NUM_WRITERS, k, overlap);
                handle.block_on(db.shutdown());
                res?;

                let results = ben.bench.results();
                let overlap_pct = 100 * overlap / k;
                for s in &results.samples {
                    let lat_ms = s.as_secs_f64() * 1000.0;
                    writeln!(out, "{k},{overlap},{overlap_pct},{lat_ms:.2}")?;
                }
                eprintln!(
                    "deadlock: keys={k} overlap={overlap} ({overlap_pct}%) samples={} \
                     p50={:?} p90={:?}",
                    results.samples.len(),
                    results.percentile(0.5),
                    results.percentile(0.9),
                );
            }
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------------
// CSV output
// ---------------------------------------------------------------------------

fn create_csv(path: &str, header: &str) -> Result<BufWriter<File>, Box<dyn Error>> {
    let mut f = BufWriter::new(File::create(path)?);
    f.write_all(header.as_bytes())?;
    Ok(f)
}

fn dump_samples(
    out: &mut impl Write,
    results: &[DbResults],
    numdb: usize,
) -> Result<(), Box<dyn Error>> {
    for (i, res) in results.iter().enumerate() {
        for s in &res.strong.samples {
            dump_sample_row(out, numdb, i, "strong-read", 2, *s)?;
        }
        for s in &res.weak.samples {
            dump_sample_row(out, numdb, i, "weak-read", 1, *s)?;
        }
        for s in &res.write.samples {
            dump_sample_row(out, numdb, i, "write", 2, *s)?;
        }
    }
    Ok(())
}

fn dump_sample_row(
    out: &mut impl Write,
    numdb: usize,
    db: usize,
    tp: &str,
    ops: usize,
    lat: Duration,
) -> Result<(), Box<dyn Error>> {
    writeln!(
        out,
        "{numdb},{db},{tp},{ops},{:.2}",
        lat.as_secs_f64() * 1000.0
    )?;
    Ok(())
}

fn dump_stats(
    out: &mut impl Write,
    results: &[DbResults],
    numdb: usize,
) -> Result<(), Box<dyn Error>> {
    for (i, res) in results.iter().enumerate() {
        let s = &res.stats;
        // Total backend round-trips: the single comparable efficiency number.
        // Summing every backend-op class keeps it meaningful across engine
        // versions that categorize ops differently (e.g. v1's tag/metadata ops
        // vs v2 folding all coordination into object reads/writes).
        let backend_ops = s.obj_writes + s.obj_reads + s.obj_lists;
        writeln!(
            out,
            "{numdb},{i},{},{},{},{},{},{}",
            res.stats.tx_n,
            res.stats.tx_retries,
            res.stats.obj_writes,
            res.stats.obj_reads,
            res.stats.obj_lists,
            backend_ops
        )?;
    }
    Ok(())
}

fn dump_throughput(
    out: &mut impl Write,
    results: &[DbResults],
    numdb: usize,
) -> Result<(), Box<dyn Error>> {
    for (i, res) in results.iter().enumerate() {
        for (name, r) in [
            ("strong-read", &res.strong),
            ("weak-read", &res.weak),
            ("write", &res.write),
        ] {
            let count = r.samples.len();
            let dur_ms = r.tot_duration.as_secs_f64() * 1000.0;
            let tps = if r.tot_duration.is_zero() {
                0.0
            } else {
                count as f64 / r.tot_duration.as_secs_f64()
            };
            writeln!(out, "{numdb},{i},{name},{count},{dur_ms:.2},{tps:.4}")?;
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn dump_client_stats(
    out: &mut impl Write,
    numdb: usize,
    wall: Duration,
    cpu_user: Duration,
    cpu_sys: Duration,
    http: HttpSnapshot,
    peak_threads: usize,
    failures: u64,
) -> Result<(), Box<dyn Error>> {
    let num_cpu = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(1);
    let cpu_total = cpu_user + cpu_sys;
    let (util_pct, req_per_sec) = if wall.is_zero() {
        (0.0, 0.0)
    } else {
        (
            100.0 * cpu_total.as_secs_f64() / (wall.as_secs_f64() * num_cpu as f64),
            http.requests as f64 / wall.as_secs_f64(),
        )
    };

    eprintln!(
        "clientmetrics num-db={numdb} cpu-util={util_pct:.0}% (user={cpu_user:?} sys={cpu_sys:?} \
         wall={wall:?} cores={num_cpu}) http-req={} ({req_per_sec:.0}/s) throttle={} 5xx={} \
         new-conns={} peak-threads={peak_threads} tx-fail={failures}",
        http.requests, http.throttle, http.server_err, http.new_conns,
    );

    if failures > 0 {
        eprintln!(
            "WARNING: num-db={numdb} saw {failures} failed transactions, dropped \
             from latency stats — this step is unreliable. The likely cause is a \
             either CPU-contention, or low open-file limits. See \
             hack/aws-bench/README.md)"
        );
    }

    writeln!(
        out,
        "{numdb},{:.2},{num_cpu},{:.2},{:.2},{util_pct:.2},{},{},{},{},{},{peak_threads},{failures}",
        wall.as_secs_f64() * 1000.0,
        cpu_user.as_secs_f64() * 1000.0,
        cpu_sys.as_secs_f64() * 1000.0,
        http.requests,
        http.throttle,
        http.server_err,
        http.success,
        http.new_conns,
    )?;
    Ok(())
}

// ---------------------------------------------------------------------------
// Formatting helpers (match the Go output)
// ---------------------------------------------------------------------------

fn fmt_ms(d: Duration) -> String {
    fmt_float(d.as_secs_f64() * 1000.0)
}

fn fmt_float(f: f64) -> String {
    if f > 1000.0 {
        format!("{f:.2}")
    } else {
        format!("{f:.4}")
    }
}
