//! Benchmarks raw backend storage-operation latencies. Ported from the Go
//! `hack/backendbench`.
//!
//! Run against a simulated backend (in-memory + GCS-profile latency) or a real
//! cloud bucket:
//!
//! ```text
//! cargo run -p glassdb-bench --bin backendbench -- --backend memory
//! BUCKET=my-bucket cargo run -p glassdb-bench --bin backendbench -- --backend s3
//! ```

// See the note in `rtbench`: musl's default allocator contends badly under the
// concurrent workload, so use mimalloc for musl (static EC2) builds.
#[cfg(target_env = "musl")]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use std::cell::RefCell;
use std::error::Error;
use std::sync::Arc;
use std::time::Duration;

use clap::Parser;
use rand::Rng;

use glassdb::backend::memory::MemoryBackend;
use glassdb::middleware::{gcs_delays, DelayBackend};
use glassdb_backend::{
    encode_writer_tag, Backend, BackendError, Metadata, Tags, Version, WriterId, LAST_WRITER_TAG,
};
use glassdb_bench::bench::Bench;
use glassdb_concurr::Ctx;

const TEST_ROOT: &str = "backend-bench";

#[derive(Parser)]
#[command(about = "Benchmark backend storage-operation latencies")]
struct Args {
    /// Backend to benchmark.
    #[arg(long, default_value = "memory", value_parser = ["memory", "gcs", "s3"])]
    backend: String,
    /// How long to run each operation benchmark.
    #[arg(long, default_value = "20s", value_parser = glassdb_bench::parse_duration)]
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
        ("SetMetaSame", run_set_meta_same),
        ("GetMeta", run_get_meta),
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

fn one_tag(key: &str, val: String) -> Tags {
    let mut t = Tags::new();
    t.insert(key.to_string(), val);
    t
}

fn fmt_ms(d: Duration) -> String {
    let ms = d.as_secs_f64() * 1000.0;
    if ms > 1000.0 {
        format!("{ms:.2}")
    } else {
        format!("{ms:.4}")
    }
}

fn run_write_same(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let ctx = Ctx::background();
        let data = random_data(1024);
        let p = format!("{TEST_ROOT}/write-same");
        let mut count = 0u64;
        while !bench.is_finished() {
            let c = count;
            bench
                .measure(|| async {
                    b.write(&ctx, &p, data.clone(), one_tag("key", format!("val{c}")))
                        .await?;
                    Ok(())
                })
                .await?;
            count += 1;
        }
        Ok(())
    })
}

fn run_write_fail_pre(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let ctx = Ctx::background();
        let data = random_data(1024);
        let p = format!("{TEST_ROOT}/write-same");
        b.write(&ctx, &p, data.clone(), one_tag("key", "val".into()))
            .await?;
        // A clearly-bogus version so the conditional write always fails its
        // precondition; the error is ignored, the latency is what we measure.
        let expected = Version::new("0/0");
        let mut count = 0u64;
        while !bench.is_finished() {
            let c = count;
            bench
                .measure(|| async {
                    let _ = b
                        .write_if(
                            &ctx,
                            &p,
                            data.clone(),
                            &expected,
                            one_tag("key", format!("val{c}")),
                        )
                        .await;
                    Ok(())
                })
                .await?;
            count += 1;
        }
        Ok(())
    })
}

fn run_read(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let ctx = Ctx::background();
        let data = random_data(1024);
        let p = format!("{TEST_ROOT}/read");
        b.write(&ctx, &p, data, one_tag("key", "val".into()))
            .await?;
        while !bench.is_finished() {
            bench
                .measure(|| async {
                    b.read(&ctx, &p).await?;
                    Ok(())
                })
                .await?;
        }
        Ok(())
    })
}

fn run_read_unchanged(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let ctx = Ctx::background();
        let data = random_data(1024);
        let p = format!("{TEST_ROOT}/read");
        let writer = WriterId::new(b"benchmark-writer".to_vec());
        let mut tags = one_tag("key", "val".into());
        tags.insert(LAST_WRITER_TAG.to_string(), encode_writer_tag(&writer));
        b.write(&ctx, &p, data, tags).await?;
        while !bench.is_finished() {
            bench
                .measure(|| async {
                    // The object is unchanged for `writer`, so the backend
                    // returns a precondition error; that is the fast path we
                    // are timing, not a failure.
                    match b.read_if_modified(&ctx, &p, &writer).await {
                        Ok(_) | Err(BackendError::Precondition) => Ok(()),
                        Err(e) => Err(e),
                    }
                })
                .await?;
        }
        Ok(())
    })
}

fn run_set_meta_same(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let ctx = Ctx::background();
        let data = random_data(1024);
        let p = format!("{TEST_ROOT}/set-meta");
        let meta0 = b
            .write(&ctx, &p, data, one_tag("key", "val".into()))
            .await?;
        let meta: RefCell<Metadata> = RefCell::new(meta0);
        let mut count = 0u64;
        while !bench.is_finished() {
            let c = count;
            bench
                .measure(|| async {
                    let version = meta.borrow().version.clone();
                    let m = b
                        .set_tags_if(&ctx, &p, &version, one_tag("key", format!("val{c}")))
                        .await?;
                    *meta.borrow_mut() = m;
                    Ok(())
                })
                .await?;
            count += 1;
        }
        Ok(())
    })
}

fn run_get_meta(b: Arc<dyn Backend>, bench: Arc<Bench>) -> BenchFuture {
    Box::pin(async move {
        let ctx = Ctx::background();
        let data = random_data(1024);
        let p = format!("{TEST_ROOT}/get-meta");
        b.write(&ctx, &p, data, one_tag("key", "val".into()))
            .await?;
        while !bench.is_finished() {
            bench
                .measure(|| async {
                    b.get_metadata(&ctx, &p).await?;
                    Ok(())
                })
                .await?;
        }
        Ok(())
    })
}
