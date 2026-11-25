//! Benchmarks raw backend storage-operation latencies. Ported from the Go
//! `hack/backendbench`.
//!
//! It measures the requests that GlassDB sends, one at a time, and prints a
//! markdown table of their latencies to stdout. Run it against a simulated
//! backend (in-memory with the S3 latency profile) or a real cloud bucket:
//!
//! ```text
//! cargo run -p glassdb-bench-scale --bin backendbench -- --backend memory
//! BUCKET=my-bucket cargo run -p glassdb-bench-scale --bin backendbench -- --backend s3
//! ```
//!
//! A real bucket gives the latency of the network path to it, so run it in the
//! region of the bucket, for example with `hack/aws-bench`.

// See the note in `perfbench`: musl's default allocator contends badly under the
// concurrent workload, so use mimalloc for musl (static EC2) builds.
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rand::Rng;

use glassdb::backend::memory::MemoryBackend;
use glassdb::middleware::{DelayBackend, s3_delays};
use glassdb::s3::tuned_http_client;
use glassdb::{InlinePolicy, NodeSizePolicy, TransactionLimits};
use glassdb_backend::{Backend, BackendError, ListLimit, Revision};
use glassdb_bench_scale::bench::{Bench, Results};

const TEST_ROOT: &str = "backend-bench";
const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

#[derive(Parser)]
#[command(about = "Benchmark backend storage-operation latencies")]
struct Args {
    /// Backend to benchmark.
    #[arg(long, default_value = "memory", value_parser = ["memory", "gcs", "s3"])]
    backend: String,
    /// How long to run each operation benchmark. 60 s gives about 1,000
    /// samples of a real S3 write for its p99.
    #[arg(long, default_value = "60s", value_parser = glassdb_bench_scale::parse_duration)]
    duration: Duration,
}

type BenchFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), BackendError>>>>;

/// A backend-operation benchmark: given a backend, a [`Bench`] timer, and the
/// value of its [`Size`], it repeats the operation until the timer is finished.
type BenchFn = fn(Arc<dyn Backend>, Arc<Bench>, usize) -> BenchFuture;

/// What a case transfers in each request.
#[derive(Clone, Copy)]
enum Size {
    /// The body of one object.
    Bytes(usize),
    /// The paths of one list page. A list does not transfer the objects.
    Paths(usize),
}

/// One row of the result table.
struct Case {
    operation: &'static str,
    size: Size,
    run: BenchFn,
}

/// The cases, at the sizes of the objects that GlassDB writes with the default
/// settings.
fn cases() -> Vec<Case> {
    let inline = InlinePolicy::default();
    let nodes = NodeSizePolicy::builder()
        .build()
        .expect("the default node sizes are valid");
    let sizes = [
        // A leaf with one value at the inline limit, or a small transaction
        // record.
        inline.max_value_bytes,
        // A leaf that is full at its inline budget, or the record of a
        // transaction that writes several keys.
        inline.max_leaf_bytes,
        // A leaf at its byte split threshold.
        nodes.node_soft_max_bytes(),
        // A transaction record that holds the largest value.
        TransactionLimits::default().max_value_bytes,
    ];
    let mut cases = Vec::new();
    for (operation, run) in [("Read", run_read as BenchFn), ("Write", run_write)] {
        cases.extend(sizes.map(|n| Case {
            operation,
            size: Size::Bytes(n),
            run,
        }));
    }
    let small = Size::Bytes(inline.max_value_bytes);
    cases.extend([
        Case {
            operation: "Read unchanged",
            size: small,
            run: run_read_unchanged,
        },
        Case {
            operation: "Write precondition failure",
            size: small,
            run: run_write_fail_pre,
        },
        Case {
            operation: "List",
            size: Size::Paths(10),
            run: run_list,
        },
    ]);
    cases
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let backend = init_backend(&args.backend).await?;

    let mut rows = Vec::new();
    for case in cases() {
        eprintln!("running {} {}", case.operation, case.size);
        let value = match case.size {
            Size::Bytes(n) | Size::Paths(n) => n,
        };
        let bench = Arc::new(Bench::new(args.duration));
        bench.start();
        (case.run)(backend.clone(), bench.clone(), value).await?;
        bench.end();
        rows.push((case, bench.results()));
    }

    println!("| Operation | Size | Mean (ms) | p50 (ms) | p90 (ms) | p99 (ms) | Samples |");
    println!("| --------- | ---- | --------- | -------- | -------- | -------- | ------- |");
    for (case, res) in rows {
        println!("{}", fmt_row(&case, &res));
    }
    Ok(())
}

async fn init_backend(kind: &str) -> Result<Arc<dyn Backend>, Box<dyn Error>> {
    match kind {
        "memory" => Ok(Arc::new(DelayBackend::new(
            Arc::new(MemoryBackend::new()),
            s3_delays(),
        )?)),
        "s3" => {
            let bucket = env_var("BUCKET")?;
            // The client of perfbench, so that both tools measure through the
            // same transport.
            let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
                .http_client(tuned_http_client())
                .load()
                .await;
            let client = aws_sdk_s3::Client::new(&cfg);
            Ok(Arc::new(glassdb::s3::S3Backend::new(client, bucket)))
        }
        "gcs" => {
            let bucket = env_var("BUCKET")?;
            Ok(Arc::new(glassdb::gcs::GcsBackend::new(bucket)))
        }
        other => Err(format!("unknown backend type {other:?}").into()),
    }
}

fn env_var(k: &str) -> Result<String, Box<dyn Error>> {
    match std::env::var(k) {
        Ok(v) if !v.is_empty() => Ok(v),
        _ => Err(format!("environment variable ${k} is required").into()),
    }
}

fn fmt_row(case: &Case, res: &Results) -> String {
    format!(
        "| {} | {} | {} | {} | {} | {} | {} |",
        case.operation,
        case.size,
        fmt_ms(res.avg()),
        fmt_ms(res.percentile(0.5)),
        fmt_ms(res.percentile(0.9)),
        fmt_ms(res.percentile(0.99)),
        res.samples.len(),
    )
}

impl std::fmt::Display for Size {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match *self {
            Size::Bytes(n) if n >= MIB => write!(f, "{} MiB", n / MIB),
            Size::Bytes(n) => write!(f, "{} KiB", n / KIB),
            Size::Paths(n) => write!(f, "{n} paths"),
        }
    }
}

fn fmt_ms(d: Duration) -> String {
    format!("{:.1}", d.as_secs_f64() * 1000.0)
}

fn random_data(size: usize) -> Vec<u8> {
    let mut b = vec![0u8; size];
    rand::rng().fill_bytes(&mut b);
    b
}

async fn replace_or_create(
    backend: &dyn Backend,
    path: &str,
    value: Vec<u8>,
) -> Result<Revision, BackendError> {
    loop {
        match backend.read(path).await {
            Ok(current) => match backend
                .write_if(path, value.clone(), &current.revision)
                .await
            {
                Ok(revision) => return Ok(revision),
                Err(BackendError::Precondition | BackendError::NotFound) => continue,
                Err(err) => return Err(err),
            },
            Err(BackendError::NotFound) => {
                match backend.write_if_not_exists(path, value.clone()).await {
                    Ok(revision) => return Ok(revision),
                    Err(BackendError::Precondition) => continue,
                    Err(err) => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
    }
}

fn run_write(b: Arc<dyn Backend>, bench: Arc<Bench>, size: usize) -> BenchFuture {
    Box::pin(async move {
        let p = format!("{TEST_ROOT}/write-{size}");
        let mut revision = replace_or_create(b.as_ref(), &p, random_data(size)).await?;
        while !bench.is_finished() {
            // Vary the content so each overwrite is a genuine state change,
            // including on providers whose revision is content-derived.
            let data = random_data(size);
            bench
                .measure_once(|| async {
                    revision = b.write_if(&p, data, &revision).await?;
                    Ok(())
                })
                .await?;
        }
        Ok(())
    })
}

fn run_write_fail_pre(b: Arc<dyn Backend>, bench: Arc<Bench>, size: usize) -> BenchFuture {
    Box::pin(async move {
        let data = random_data(size);
        let p = format!("{TEST_ROOT}/write-fail-pre-{size}");
        replace_or_create(b.as_ref(), &p, data.clone()).await?;
        // A clearly-bogus revision so the CAS always fails its
        // precondition; the error is ignored, the latency is what we measure.
        let expected = Revision::new("0/0");
        while !bench.is_finished() {
            bench
                .measure_once(|| async {
                    let _ = b.write_if(&p, data.clone(), &expected).await;
                    Ok(())
                })
                .await?;
        }
        Ok(())
    })
}

fn run_read(b: Arc<dyn Backend>, bench: Arc<Bench>, size: usize) -> BenchFuture {
    Box::pin(async move {
        let p = format!("{TEST_ROOT}/read-{size}");
        replace_or_create(b.as_ref(), &p, random_data(size)).await?;
        while !bench.is_finished() {
            bench
                .measure_once(|| async {
                    b.read(&p).await?;
                    Ok(())
                })
                .await?;
        }
        Ok(())
    })
}

fn run_read_unchanged(b: Arc<dyn Backend>, bench: Arc<Bench>, size: usize) -> BenchFuture {
    Box::pin(async move {
        let p = format!("{TEST_ROOT}/read-{size}");
        // The write returns the object's current revision; a conditional read
        // against it short-circuits (304 / Precondition).
        let revision = replace_or_create(b.as_ref(), &p, random_data(size)).await?;
        while !bench.is_finished() {
            bench
                .measure_once(|| async {
                    // The object is unchanged, so the backend returns a
                    // precondition error; that is the fast path we are timing,
                    // not a failure.
                    match b.read_if_modified(&p, &revision).await {
                        Ok(_) | Err(BackendError::Precondition) => Ok(()),
                        Err(e) => Err(e),
                    }
                })
                .await?;
        }
        Ok(())
    })
}

fn run_list(b: Arc<dyn Backend>, bench: Arc<Bench>, paths: usize) -> BenchFuture {
    Box::pin(async move {
        let prefix = format!("{TEST_ROOT}/list/");
        for i in 0..paths {
            replace_or_create(b.as_ref(), &format!("{prefix}{i}"), Vec::new()).await?;
        }
        // One page holds all the paths.
        let limit = ListLimit::new(paths).unwrap();
        while !bench.is_finished() {
            bench
                .measure_once(|| async {
                    b.list(&prefix, None, limit).await?;
                    Ok(())
                })
                .await?;
        }
        Ok(())
    })
}
