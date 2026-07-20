//! Benchmarks raw backend storage-operation latencies. Ported from the Go
//! `hack/backendbench`.
//!
//! Run against a simulated backend (in-memory + GCS-profile latency) or a real
//! cloud bucket:
//!
//! ```text
//! cargo run -p glassdb-bench-scale --bin backendbench -- --backend memory
//! BUCKET=my-bucket cargo run -p glassdb-bench-scale --bin backendbench -- --backend s3
//! ```

// See the note in `rtbench`: musl's default allocator contends badly under the
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
use glassdb::middleware::{DelayBackend, gcs_delays};
use glassdb_backend::{Backend, BackendError, Version};
use glassdb_bench_scale::bench::Bench;
const TEST_ROOT: &str = "backend-bench";

#[derive(Parser)]
#[command(about = "Benchmark backend storage-operation latencies")]
struct Args {
    /// Backend to benchmark.
    #[arg(long, default_value = "memory", value_parser = ["memory", "gcs", "s3"])]
    backend: String,
    /// How long to run each operation benchmark.
    #[arg(long, default_value = "20s", value_parser = glassdb_bench_scale::parse_duration)]
    duration: Duration,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    let backend = init_backend(&args.backend).await?;

    let tests: &[(&str, BenchFn)] = &[
        ("WriteSame", run_write_same),
        ("WriteFailPre", run_write_fail_pre),
        ("Read", run_read),
        ("ReadUnchanged", run_read_unchanged),
    ];

    for (name, f) in tests {
        run_bench(name, backend.clone(), args.duration, *f).await?;
    }
    Ok(())
}

type BenchFuture = std::pin::Pin<Box<dyn std::future::Future<Output = Result<(), BackendError>>>>;

/// A named backend-operation benchmark: given a backend and a [`Bench`] timer,
/// it loops the operation until the timer is finished.
type BenchFn = fn(Arc<dyn Backend>, Arc<Bench>) -> BenchFuture;

async fn init_backend(kind: &str) -> Result<Arc<dyn Backend>, Box<dyn Error>> {
    match kind {
        "memory" => Ok(Arc::new(DelayBackend::new(
            Arc::new(MemoryBackend::new()),
            gcs_delays(),
        ))),
        "s3" => {
            let bucket = env_var("BUCKET")?;
            let cfg = aws_config::defaults(aws_config::BehaviorVersion::latest())
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

async fn run_bench(
    name: &str,
    backend: Arc<dyn Backend>,
    duration: Duration,
    f: BenchFn,
) -> Result<(), Box<dyn Error>> {
    let bench = Arc::new(Bench::new(duration));
    bench.start();
    f(backend, bench.clone()).await?;
    bench.end();
    let res = bench.results();

    let mut ts = Duration::ZERO;
    for x in &res.samples {
        println!("{name},{},{}", fmt_ms(ts), fmt_ms(*x));
        ts += *x;
    }
    eprintln!(
        "{name}: 50pc: {:?}, 90pc: {:?}, 95pc: {:?}",
        res.percentile(0.5),
        res.percentile(0.9),
        res.percentile(0.95)
    );
    Ok(())
}

fn random_data(size: usize) -> Vec<u8> {
    let mut b = vec![0u8; size];
    rand::rng().fill_bytes(&mut b);
    b
}

fn fmt_ms(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms > 1000.0 {
        format!("{ms:.2}")
    } else {
        format!("{ms:.4}")
    }
}

async fn replace_or_create(
    backend: &dyn Backend,
    path: &str,
    value: Vec<u8>,
) -> Result<Version, BackendError> {
    loop {
        match backend.read(path).await {
            Ok(current) => match backend
                .write_if(path, value.clone(), &current.version)
                .await
            {
                Ok(version) => return Ok(version),
                Err(BackendError::Precondition | BackendError::NotFound) => continue,
                Err(err) => return Err(err),
            },
            Err(BackendError::NotFound) => {
                match backend.write_if_not_exists(path, value.clone()).await {
                    Ok(version) => return Ok(version),
                    Err(BackendError::Precondition) => continue,
                    Err(err) => return Err(err),
                }
            }
            Err(err) => return Err(err),
        }
    }
}

fn run_write_same(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let p = format!("{TEST_ROOT}/write-same");
        let mut version = replace_or_create(b.as_ref(), &p, random_data(1024)).await?;
        let mut count = 0u64;
        while !bench.is_finished() {
            // Vary the content so each overwrite is a genuine state change,
            // including on providers whose CAS token is content-derived.
            let data = random_data(1024);
            bench
                .measure(|| async {
                    version = b.write_if(&p, data, &version).await?;
                    Ok(())
                })
                .await?;
            count += 1;
        }
        let _ = count;
        Ok(())
    })
}

fn run_write_fail_pre(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let data = random_data(1024);
        let p = format!("{TEST_ROOT}/write-same");
        replace_or_create(b.as_ref(), &p, data.clone()).await?;
        // A clearly-bogus version so the conditional write always fails its
        // precondition; the error is ignored, the latency is what we measure.
        let expected = Version::new("0/0");
        while !bench.is_finished() {
            bench
                .measure(|| async {
                    let _ = b.write_if(&p, data.clone(), &expected).await;
                    Ok(())
                })
                .await?;
        }
        Ok(())
    })
}

fn run_read(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let data = random_data(1024);
        let p = format!("{TEST_ROOT}/read");
        replace_or_create(b.as_ref(), &p, data).await?;
        while !bench.is_finished() {
            bench
                .measure(|| async {
                    b.read(&p).await?;
                    Ok(())
                })
                .await?;
        }
        Ok(())
    })
}

fn run_read_unchanged(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let data = random_data(1024);
        let p = format!("{TEST_ROOT}/read");
        // The version returned by the write is the object's current CAS token;
        // a conditional read against it short-circuits (304 / Precondition).
        let version = replace_or_create(b.as_ref(), &p, data).await?;
        while !bench.is_finished() {
            bench
                .measure(|| async {
                    // The object is unchanged, so the backend returns a
                    // precondition error; that is the fast path we are timing,
                    // not a failure.
                    match b.read_if_modified(&p, &version).await {
                        Ok(_) | Err(BackendError::Precondition) => Ok(()),
                        Err(e) => Err(e),
                    }
                })
                .await?;
        }
        Ok(())
    })
}
