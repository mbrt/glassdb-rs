//! Transaction microbenchmarks ported from the Go `bench_test.go`.
//!
//! Each workload runs over three backends, matching the Go suite:
//! - `memory`: a bare in-memory backend.
//! - `gcs` / `s3`: the same in-memory backend wrapped in [`DelayBackend`] with
//!   the GCS/S3 latency profile, compressed 1000x (`scale = 1/1000`) so a
//!   wall-clock `cargo bench` run stays fast — exactly like the Go benchmarks.
//!
//! Alongside the criterion timing, each (workload, backend) pair prints the
//! per-operation backend counters derived from [`glassdb::Stats`] (the analog
//! of Go's `benchStats` custom metrics: retries/op, w/op, r/op, metaw/op,
//! metar/op).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use criterion::{criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

use glassdb::backend::memory::MemoryBackend;
use glassdb::middleware::{gcs_delays, s3_delays, DelayBackend, DelayOptions};
use glassdb::{Backend, Collection, Ctx, Error, FqKey, Tx, DB};

// Number of iterations used for the one-off stats summary printed per backend.
const STATS_ITERS: i64 = 30;

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build tokio runtime")
}

/// Wraps a fresh in-memory backend in a [`DelayBackend`] using `profile`,
/// compressed 1000x so bench runs stay fast (mirrors the Go bench scaling).
fn simulated(profile: fn() -> DelayOptions) -> Arc<dyn Backend> {
    let mut opts = profile();
    opts.scale = 1.0 / 1000.0;
    Arc::new(DelayBackend::new(Arc::new(MemoryBackend::new()), opts))
}

/// The three backends used by every workload, each backed by fresh state.
fn backends() -> Vec<(&'static str, Arc<dyn Backend>)> {
    vec![
        ("memory", Arc::new(MemoryBackend::new())),
        ("gcs", simulated(gcs_delays)),
        ("s3", simulated(s3_delays)),
    ]
}

fn write_int(n: i64) -> Vec<u8> {
    n.to_le_bytes().to_vec()
}

fn read_int(b: &[u8]) -> i64 {
    if b.len() < 8 {
        return 0;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(&b[..8]);
    i64::from_le_bytes(arr)
}

async fn read_int_or_zero(tx: &mut Tx, coll: &Collection, key: &[u8]) -> Result<i64, Error> {
    match tx.read(coll, key).await {
        Ok(v) => Ok(read_int(&v)),
        Err(e) if e.is_not_found() => Ok(0),
        Err(e) => Err(e),
    }
}

async fn open_db(backend: Arc<dyn Backend>) -> DB {
    DB::open(&Ctx::background(), "bench", backend)
        .await
        .expect("open db")
}

async fn open_coll(backend: Arc<dyn Backend>, name: &[u8]) -> (DB, Collection) {
    let db = open_db(backend).await;
    let coll = db.collection(name);
    coll.create(&Ctx::background()).await.expect("create coll");
    (db, coll)
}

fn make_keys(coll: &Collection, n: usize) -> Vec<FqKey> {
    (0..n)
        .map(|i| FqKey {
            collection: coll.clone(),
            key: format!("key{i}").into_bytes(),
        })
        .collect()
}

/// Runs `body` `STATS_ITERS` times and prints the per-op backend counters,
/// the analog of Go's `benchStats`.
async fn report_stats<F: AsyncFnMut()>(label: &str, db: &DB, mut body: F) {
    let start = db.stats();
    for _ in 0..STATS_ITERS {
        body().await;
    }
    let s = db.stats().sub(&start);
    let n = STATS_ITERS.max(1) as f64;
    println!(
        "  stats {label}: retries/op={:.3} w/op={:.2} r/op={:.2} metaw/op={:.2} metar/op={:.2}",
        s.tx_retries as f64 / n,
        s.obj_writes as f64 / n,
        s.obj_reads as f64 / n,
        s.meta_writes as f64 / n,
        s.meta_reads as f64 / n,
    );
}

// --- Workload bodies (one transaction each) -------------------------------

async fn single_rmw(db: &DB, coll: &Collection) {
    db.tx(&Ctx::background(), async |tx| {
        let num = read_int_or_zero(tx, coll, b"key").await?;
        tx.write(coll, b"key", &write_int(num + 1))
    })
    .await
    .expect("single rmw");
}

async fn multi_rmw(db: &DB, coll: &Collection, keys: &[FqKey]) {
    db.tx(&Ctx::background(), async |tx| {
        let res = tx.read_multi(keys).await;
        for (i, rv) in res.iter().enumerate() {
            let val = read_int(&rv.value);
            tx.write(coll, keys[i].key.as_slice(), &write_int(val + 1))?;
        }
        Ok(())
    })
    .await
    .expect("multi rmw");
}

async fn multi_read(db: &DB, keys: &[FqKey]) {
    let _ = db
        .tx(&Ctx::background(), async |tx| {
            let _ = tx.read_multi(keys).await;
            Ok::<(), Error>(())
        })
        .await;
}

async fn hundred_writes(db: &DB, coll: &Collection, base: usize) {
    db.tx(&Ctx::background(), async |tx| {
        for j in 0..100 {
            let k = format!("k{}", base * 100 + j);
            tx.write(coll, k.as_bytes(), &write_int(j as i64))?;
        }
        Ok(())
    })
    .await
    .expect("hundred writes");
}

async fn update_two_keys(db: &DB, coll: &Collection, ctx: &Ctx) -> Result<(), Error> {
    db.tx(ctx, async |tx| {
        let n1 = read_int_or_zero(tx, coll, b"key1").await?;
        tx.write(coll, b"key1", &write_int(n1 + 1))?;
        let n2 = read_int_or_zero(tx, coll, b"key2").await?;
        tx.write(coll, b"key2", &write_int(n2 + 1))
    })
    .await
}

async fn update_shared(db: &DB, coll: &Collection, key_w: &[u8], ctx: &Ctx) -> Result<(), Error> {
    db.tx(ctx, async |tx| {
        let num = read_int_or_zero(tx, coll, b"key-r").await?;
        tx.write(coll, key_w, &write_int(num + 1))
    })
    .await
}

// --- Benchmark groups ------------------------------------------------------

fn bench_single_rmw(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("single_rmw");
    group.sample_size(10);
    for (name, backend) in backends() {
        let (db, coll) = rt.block_on(open_coll(backend, b"single-rmw"));
        rt.block_on(report_stats(&format!("single_rmw/{name}"), &db, || {
            single_rmw(&db, &coll)
        }));
        group.bench_function(name, |bch| {
            bch.iter(|| rt.block_on(single_rmw(&db, &coll)));
        });
        rt.block_on(db.close());
    }
    group.finish();
}

fn bench_multi_rmw(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("multi_rmw_10");
    group.sample_size(10);
    for (name, backend) in backends() {
        let (db, coll) = rt.block_on(open_coll(backend, b"rmw-mb"));
        let keys = make_keys(&coll, 10);
        rt.block_on(report_stats(&format!("multi_rmw_10/{name}"), &db, || {
            multi_rmw(&db, &coll, &keys)
        }));
        group.bench_function(name, |bch| {
            bch.iter(|| rt.block_on(multi_rmw(&db, &coll, &keys)));
        });
        rt.block_on(db.close());
    }
    group.finish();
}

fn bench_multi_read(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("multi_read_10");
    group.sample_size(10);
    for (name, backend) in backends() {
        let (db, coll) = rt.block_on(open_coll(backend, b"rmw-mb"));
        let keys = make_keys(&coll, 10);
        // Pre-write the values once.
        rt.block_on(async {
            db.tx(&Ctx::background(), async |tx| {
                for (i, k) in keys.iter().enumerate() {
                    tx.write(&coll, k.key.as_slice(), &write_int(i as i64))?;
                }
                Ok(())
            })
            .await
            .expect("seed values");
        });
        rt.block_on(report_stats(&format!("multi_read_10/{name}"), &db, || {
            multi_read(&db, &keys)
        }));
        group.bench_function(name, |bch| {
            bch.iter(|| rt.block_on(multi_read(&db, &keys)));
        });
        rt.block_on(db.close());
    }
    group.finish();
}

fn bench_hundred_writes(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("write_100");
    group.sample_size(10);
    for (name, backend) in backends() {
        let (db, coll) = rt.block_on(open_coll(backend, b"mw"));
        let ctr = AtomicUsize::new(0);
        rt.block_on(report_stats(&format!("write_100/{name}"), &db, || {
            let base = ctr.fetch_add(1, Ordering::Relaxed);
            hundred_writes(&db, &coll, base)
        }));
        group.bench_function(name, |bch| {
            bch.iter(|| {
                let base = ctr.fetch_add(1, Ordering::Relaxed);
                rt.block_on(hundred_writes(&db, &coll, base));
            });
        });
        rt.block_on(db.close());
    }
    group.finish();
}

fn bench_concurr_multi_rmw(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("concurr_multi_rmw");
    group.sample_size(10);
    for (name, backend) in backends() {
        // Two databases over the same backend; one runs a background contender.
        let (db1, coll1) = rt.block_on(open_coll(backend.clone(), b"rmw-b"));
        let db2 = rt.block_on(open_db(backend));
        let coll2 = db2.collection(b"rmw-b");

        // The contender runs on its own OS thread but drives its futures with
        // `Handle::block_on` on the *shared* measured runtime (like `rtbench`),
        // so it time-slices the same worker pool as the measured workload
        // instead of saturating a dedicated core. (`tokio::spawn` is not an
        // option: the `db.tx(async |tx| ...)` future trips the higher-ranked-
        // lifetime `Send` limitation noted in PORTING.md; `block_on` has no
        // `Send` bound.)
        let (cctx, cancel) = Ctx::with_cancel();
        let cdb = db1.clone();
        let ccoll = coll1.clone();
        let chandle = rt.handle().clone();
        let handle = std::thread::spawn(move || {
            chandle.block_on(async {
                while !cctx.is_cancelled() {
                    let _ = update_two_keys(&cdb, &ccoll, &cctx).await;
                }
            });
        });

        rt.block_on(report_stats(
            &format!("concurr_multi_rmw/{name}"),
            &db2,
            || async {
                let _ = update_two_keys(&db2, &coll2, &Ctx::background()).await;
            },
        ));
        group.bench_function(name, |bch| {
            bch.iter(|| {
                rt.block_on(async {
                    let _ = update_two_keys(&db2, &coll2, &Ctx::background()).await;
                });
            });
        });

        cancel.cancel();
        let _ = handle.join();
        rt.block_on(db1.close());
        rt.block_on(db2.close());
    }
    group.finish();
}

fn bench_shared_read(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("shared_read");
    group.sample_size(10);
    for (name, backend) in backends() {
        let (db, coll) = rt.block_on(open_coll(backend, b"shr-b"));
        rt.block_on(async {
            db.tx(&Ctx::background(), async |tx| {
                tx.write(&coll, b"key-r", &write_int(1))?;
                tx.write(&coll, b"key-w1", &write_int(0))?;
                tx.write(&coll, b"key-w2", &write_int(0))
            })
            .await
            .expect("seed shared keys");
        });

        // Background contender on its own thread, sharing the measured runtime
        // (see `bench_concurr_multi_rmw` for why).
        let (cctx, cancel) = Ctx::with_cancel();
        let cdb = db.clone();
        let ccoll = coll.clone();
        let chandle = rt.handle().clone();
        let handle = std::thread::spawn(move || {
            chandle.block_on(async {
                while !cctx.is_cancelled() {
                    let _ = update_shared(&cdb, &ccoll, b"key-w2", &cctx).await;
                }
            });
        });

        rt.block_on(report_stats(
            &format!("shared_read/{name}"),
            &db,
            || async {
                let _ = update_shared(&db, &coll, b"key-w1", &Ctx::background()).await;
            },
        ));
        group.bench_function(name, |bch| {
            bch.iter(|| {
                rt.block_on(async {
                    let _ = update_shared(&db, &coll, b"key-w1", &Ctx::background()).await;
                });
            });
        });

        cancel.cancel();
        let _ = handle.join();
        rt.block_on(db.close());
    }
    group.finish();
}

fn benches(c: &mut Criterion) {
    let rt = runtime();
    bench_single_rmw(c, &rt);
    bench_multi_rmw(c, &rt);
    bench_multi_read(c, &rt);
    bench_hundred_writes(c, &rt);
    bench_concurr_multi_rmw(c, &rt);
    bench_shared_read(c, &rt);
}

criterion_group!(transactions, benches);
criterion_main!(transactions);
