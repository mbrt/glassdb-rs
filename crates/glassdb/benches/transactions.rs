//! Transaction microbenchmarks ported from the Go `bench_test.go`.
//!
//! Each workload runs over three backends, matching the Go suite:
//! - `memory`: a bare in-memory backend.
//! - `gcs` / `s3`: the same in-memory backend wrapped in [`DelayBackend`] with
//!   the GCS/S3 latency profile. Process-wide model time is accelerated 1000x
//!   so a wall-clock `cargo bench` run stays fast.
//!
//! Alongside the criterion timing, each (workload, backend) pair prints the
//! per-operation backend counters derived from [`glassdb::Stats`] (the analog
//! of Go's `benchStats` custom metrics: retries/op, w/op, r/op, metaw/op,
//! metar/op).

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use criterion::{Criterion, criterion_group, criterion_main};
use tokio::runtime::Runtime;

use glassdb::backend::memory::MemoryBackend;
use glassdb::middleware::{DelayBackend, DelayOptions, gcs_delays, s3_delays};
use glassdb::{
    Backend, Collection, CollectionPath, Database, Error, InlinePolicy, Stats, Transaction,
};

// Number of iterations used for the one-off stats summary printed per backend.
const STATS_ITERS: i64 = 30;

fn runtime() -> Runtime {
    glassdb_concurr::rt::set_model_time_speedup(1000.0)
        .expect("configure benchmark model time before creating the runtime");
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(4)
        .enable_all()
        .build()
        .expect("build tokio runtime")
}

/// Wraps a fresh in-memory backend in a [`DelayBackend`] using `profile`.
fn simulated(profile: fn() -> DelayOptions) -> Arc<dyn Backend> {
    Arc::new(
        DelayBackend::new(Arc::new(MemoryBackend::new()), profile())
            .expect("built-in delay profile is valid"),
    )
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

fn read_int(key: &[u8], value: &[u8]) -> Result<i64, Error> {
    value
        .get(..8)
        .and_then(|bytes| bytes.try_into().ok())
        .map(i64::from_le_bytes)
        .ok_or_else(|| Error::internal(format!("key {key:?} has invalid integer value {value:?}")))
}

fn incremented_value(key: &[u8], current: i64) -> Result<Vec<u8>, Error> {
    current
        .checked_add(1)
        .map(write_int)
        .ok_or_else(|| Error::internal(format!("integer overflow for key {key:?}")))
}

async fn read_int_or_zero(tx: &Transaction, coll: &Collection, key: &[u8]) -> Result<i64, Error> {
    match tx.read(coll, key).await {
        Ok(Some(value)) => read_int(key, &value),
        Ok(None) => Ok(0),
        Err(e) => Err(e),
    }
}

async fn open_db(backend: Arc<dyn Backend>) -> Database {
    Database::open("bench", backend).await.expect("open db")
}

async fn open_coll(backend: Arc<dyn Backend>, name: &[u8]) -> (Database, Collection) {
    let db = open_db(backend).await;
    let coll = db
        .root_collection()
        .create_collection_if_absent(name)
        .await
        .expect("create coll");
    (db, coll)
}

async fn open_coll_with_inline(
    backend: Arc<dyn Backend>,
    name: &[u8],
    inline: InlinePolicy,
) -> (Database, Collection) {
    let db = Database::builder("bench", backend)
        .inline_policy(inline)
        .open()
        .await
        .expect("open db with inline policy");
    let coll = db
        .root_collection()
        .create_collection_if_absent(name)
        .await
        .expect("create coll");
    (db, coll)
}

fn make_keys(n: usize) -> Vec<Vec<u8>> {
    (0..n).map(|i| format!("key{i}").into_bytes()).collect()
}

/// Runs `body` `STATS_ITERS` times and prints the per-op backend counters,
/// the analog of Go's `benchStats`.
async fn report_stats<F: AsyncFnMut()>(label: &str, db: &Database, mut body: F) -> Stats {
    let start = db.stats();
    for _ in 0..STATS_ITERS {
        body().await;
    }
    let s = db.stats() - start;
    let n = STATS_ITERS.max(1) as f64;
    println!(
        "  stats {label}: retries/op={:.3} w/op={:.2} r/op={:.2} direct-candidates/op={:.2} direct-landed/op={:.2} locks/op={:.2}",
        s.transactions.retries as f64 / n,
        s.backend.obj_writes as f64 / n,
        s.backend.obj_reads as f64 / n,
        s.direct_commit.candidates as f64 / n,
        s.direct_commit.landed as f64 / n,
        s.locker.calls as f64 / n,
    );
    s
}

// --- Workload bodies (one transaction each) -------------------------------

async fn single_rmw(db: &Database, coll: &Collection) {
    db.tx(|tx| async move {
        let num = read_int_or_zero(&tx, coll, b"key").await?;
        tx.write(coll, b"key", &incremented_value(b"key", num)?)
    })
    .await
    .expect("single rmw");
}

async fn multi_rmw(db: &Database, coll: &Collection, keys: &[Vec<u8>]) {
    db.tx(|tx| async move {
        // Read every key in parallel, then write each incremented value.
        let vals = futures::future::join_all(keys.iter().map(|k| tx.read(coll, k))).await;
        for (k, rv) in keys.iter().zip(vals) {
            let val = match rv {
                Ok(Some(value)) => read_int(k, &value)?,
                Ok(None) => 0,
                Err(e) => return Err(e),
            };
            tx.write(coll, k, &incremented_value(k, val)?)?;
        }
        Ok(())
    })
    .await
    .expect("multi rmw");
}

async fn multi_read(db: &Database, coll: &Collection, keys: &[Vec<u8>]) {
    let _ = db
        .tx(|tx| async move {
            let _ = futures::future::join_all(keys.iter().map(|k| tx.read(coll, k))).await;
            Ok::<(), Error>(())
        })
        .await;
}

async fn hundred_writes(db: &Database, coll: &Collection, base: usize) {
    db.tx(|tx| async move {
        for j in 0..100 {
            let k = format!("k{}", base * 100 + j);
            tx.write(coll, k.as_bytes(), &write_int(j as i64))?;
        }
        Ok(())
    })
    .await
    .expect("hundred writes");
}

async fn update_two_keys(db: &Database, coll: &Collection) -> Result<(), Error> {
    db.tx(|tx| async move {
        let n1 = read_int_or_zero(&tx, coll, b"key1").await?;
        tx.write(coll, b"key1", &incremented_value(b"key1", n1)?)?;
        let n2 = read_int_or_zero(&tx, coll, b"key2").await?;
        tx.write(coll, b"key2", &incremented_value(b"key2", n2)?)
    })
    .await
}

async fn update_shared(db: &Database, coll: &Collection, key_w: &[u8]) -> Result<(), Error> {
    db.tx(|tx| async move {
        let num = read_int_or_zero(&tx, coll, b"key-r").await?;
        tx.write(coll, key_w, &incremented_value(key_w, num)?)
    })
    .await
}

#[derive(Clone, Copy)]
enum DirectWorkload {
    BlindPut,
    MixedPutDelete,
    CrossKeyRmw,
}

impl DirectWorkload {
    fn label(self) -> &'static str {
        match self {
            DirectWorkload::BlindPut => "blind-put",
            DirectWorkload::MixedPutDelete => "mixed-put-delete",
            DirectWorkload::CrossKeyRmw => "cross-key-rmw",
        }
    }
}

fn make_prefixed_keys(prefix: &str, n: usize) -> Vec<Vec<u8>> {
    (0..n)
        .map(|index| format!("{prefix}-{index:02}").into_bytes())
        .collect()
}

async fn run_direct_workload(
    workload: DirectWorkload,
    db: &Database,
    coll: &Collection,
    keys: &[Vec<u8>],
    sequence: usize,
    value_len: usize,
) {
    db.tx(|tx| async move {
        match workload {
            DirectWorkload::BlindPut => {
                let value = vec![(sequence & 0xff) as u8; value_len];
                for key in keys {
                    tx.write(coll, key, &value)?;
                }
            }
            DirectWorkload::MixedPutDelete => {
                let value = vec![(sequence & 0xff) as u8; value_len];
                for (index, key) in keys.iter().enumerate() {
                    if (index + sequence).is_multiple_of(2) {
                        tx.write(coll, key, &value)?;
                    } else {
                        tx.delete(coll, key)?;
                    }
                }
            }
            DirectWorkload::CrossKeyRmw => {
                let values =
                    futures::future::join_all(keys.iter().map(|key| tx.read(coll, key))).await;
                for (index, result) in values.into_iter().enumerate() {
                    let source = &keys[index];
                    let destination = &keys[(index + 1) % keys.len()];
                    let value = match result {
                        Ok(Some(value)) => read_int(source, &value)?,
                        Ok(None) => 0,
                        Err(error) => return Err(error),
                    };
                    tx.write(coll, destination, &incremented_value(destination, value)?)?;
                }
            }
        }
        Ok(())
    })
    .await
    .expect("ADR-061 direct workload");
}

async fn seed_direct_workload(
    workload: DirectWorkload,
    db: &Database,
    coll: &Collection,
    keys: &[Vec<u8>],
    value_len: usize,
) -> usize {
    match workload {
        DirectWorkload::MixedPutDelete => {
            run_direct_workload(workload, db, coll, keys, 0, value_len).await;
            1
        }
        DirectWorkload::BlindPut | DirectWorkload::CrossKeyRmw => {
            run_direct_workload(DirectWorkload::BlindPut, db, coll, keys, 0, value_len).await;
            1
        }
    }
}

fn verify_direct_gate(label: &str, stats: &Stats, uncontended: bool) {
    // This escape hatch supports temporarily applying the harness to a pre-ADR
    // worktree for paired measurements; its old protocol cannot meet the gate.
    if std::env::var_os("GLASSDB_ADR061_BASELINE").is_some() {
        return;
    }
    let completed = STATS_ITERS as u64;
    assert_eq!(stats.transactions.completed, completed, "{label}: failures");
    assert_eq!(
        stats.direct_commit.candidates, completed,
        "{label}: every transaction should be a direct candidate"
    );
    if uncontended {
        assert_eq!(
            stats.direct_commit.landed, completed,
            "{label}: every transaction should land directly"
        );
        assert_eq!(stats.locker.calls, 0, "{label}: no transaction may lock");
        assert_eq!(
            stats.backend.obj_writes, completed,
            "{label}: an uncontended transaction must issue one object write"
        );
    }
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
        rt.block_on(db.shutdown());
    }
    group.finish();
}

fn bench_multi_rmw(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("multi_rmw_10");
    group.sample_size(10);
    for (name, backend) in backends() {
        let (db, coll) = rt.block_on(open_coll(backend, b"rmw-mb"));
        let keys = make_keys(10);
        rt.block_on(report_stats(&format!("multi_rmw_10/{name}"), &db, || {
            multi_rmw(&db, &coll, &keys)
        }));
        group.bench_function(name, |bch| {
            bch.iter(|| rt.block_on(multi_rmw(&db, &coll, &keys)));
        });
        rt.block_on(db.shutdown());
    }
    group.finish();
}

fn bench_multi_read(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("multi_read_10");
    group.sample_size(10);
    for (name, backend) in backends() {
        let (db, coll) = rt.block_on(open_coll(backend, b"rmw-mb"));
        let keys = make_keys(10);
        // Pre-write the values once.
        rt.block_on(async {
            let coll_ref = &coll;
            let keys_ref = &keys;
            db.tx(|tx| async move {
                for (i, k) in keys_ref.iter().enumerate() {
                    tx.write(coll_ref, k, &write_int(i as i64))?;
                }
                Ok(())
            })
            .await
            .expect("seed values");
        });
        rt.block_on(report_stats(&format!("multi_read_10/{name}"), &db, || {
            multi_read(&db, &coll, &keys)
        }));
        group.bench_function(name, |bch| {
            bch.iter(|| rt.block_on(multi_read(&db, &coll, &keys)));
        });
        rt.block_on(db.shutdown());
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
        rt.block_on(db.shutdown());
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
        let coll2 = rt
            .block_on(db2.open_collection(&CollectionPath::new(b"rmw-b").unwrap()))
            .expect("open coll");

        // The contender is a spawned task on the *shared* measured runtime, so
        // it multiplexes over the same worker pool as the measured workload
        // (the `db.tx` future is `Send`, so no dedicated OS thread is needed).
        // The benchmark stops it by aborting the join handle: dropping the
        // future is equivalent to cancellation.
        let cdb = db1.clone();
        let ccoll = coll1.clone();
        let handle = rt.spawn(async move {
            loop {
                let _ = update_two_keys(&cdb, &ccoll).await;
            }
        });

        rt.block_on(report_stats(
            &format!("concurr_multi_rmw/{name}"),
            &db2,
            || async {
                let _ = update_two_keys(&db2, &coll2).await;
            },
        ));
        group.bench_function(name, |bch| {
            bch.iter(|| {
                rt.block_on(async {
                    let _ = update_two_keys(&db2, &coll2).await;
                });
            });
        });

        handle.abort();
        let _ = rt.block_on(handle);
        rt.block_on(db1.shutdown());
        rt.block_on(db2.shutdown());
    }
    group.finish();
}

fn bench_shared_read(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("shared_read");
    group.sample_size(10);
    for (name, backend) in backends() {
        let (db, coll) = rt.block_on(open_coll(backend, b"shr-b"));
        rt.block_on(async {
            let coll_ref = &coll;
            db.tx(|tx| async move {
                tx.write(coll_ref, b"key-r", &write_int(1))?;
                tx.write(coll_ref, b"key-w1", &write_int(0))?;
                tx.write(coll_ref, b"key-w2", &write_int(0))
            })
            .await
            .expect("seed shared keys");
        });

        // Background contender spawned on the shared measured runtime (see
        // `bench_concurr_multi_rmw`).
        let cdb = db.clone();
        let ccoll = coll.clone();
        let handle = rt.spawn(async move {
            loop {
                let _ = update_shared(&cdb, &ccoll, b"key-w2").await;
            }
        });

        rt.block_on(report_stats(
            &format!("shared_read/{name}"),
            &db,
            || async {
                let _ = update_shared(&db, &coll, b"key-w1").await;
            },
        ));
        group.bench_function(name, |bch| {
            bch.iter(|| {
                rt.block_on(async {
                    let _ = update_shared(&db, &coll, b"key-w1").await;
                });
            });
        });

        handle.abort();
        let _ = rt.block_on(handle);
        rt.block_on(db.shutdown());
    }
    group.finish();
}

fn bench_adr061_low_contention(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("adr061_direct_low");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(250));

    for workload in [
        DirectWorkload::BlindPut,
        DirectWorkload::MixedPutDelete,
        DirectWorkload::CrossKeyRmw,
    ] {
        for count in [2usize, 8, 32] {
            for (backend_name, backend) in backends() {
                let collection_name = format!("a61-l-{}-{count}-{backend_name}", workload.label());
                let (db, coll) = rt.block_on(open_coll(backend, collection_name.as_bytes()));
                let keys = make_prefixed_keys("measured", count);
                let initial = rt.block_on(seed_direct_workload(workload, &db, &coll, &keys, 8));
                let sequence = AtomicUsize::new(initial);
                let label = format!("{}/{count}/{backend_name}", workload.label());
                let stats = rt.block_on(report_stats(&format!("adr061_low/{label}"), &db, || {
                    let next = sequence.fetch_add(1, Ordering::Relaxed);
                    run_direct_workload(workload, &db, &coll, &keys, next, 8)
                }));
                verify_direct_gate(&label, &stats, true);

                group.bench_function(&label, |bch| {
                    bch.iter(|| {
                        let next = sequence.fetch_add(1, Ordering::Relaxed);
                        rt.block_on(run_direct_workload(workload, &db, &coll, &keys, next, 8));
                    });
                });
                rt.block_on(db.shutdown());
            }
        }
    }
    group.finish();
}

fn bench_adr061_same_leaf_contention(c: &mut Criterion, rt: &Runtime) {
    let mut group = c.benchmark_group("adr061_direct_same_leaf_contention");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(250));

    for workload in [
        DirectWorkload::BlindPut,
        DirectWorkload::MixedPutDelete,
        DirectWorkload::CrossKeyRmw,
    ] {
        for count in [2usize, 8, 32] {
            for (backend_name, backend) in backends() {
                let collection_name = format!("a61-c-{}-{count}-{backend_name}", workload.label());
                let (background_db, background_coll) =
                    rt.block_on(open_coll(backend.clone(), collection_name.as_bytes()));
                let measured_db = rt.block_on(open_db(backend));
                let measured_coll =
                    rt.block_on(measured_db.open_collection(
                        &CollectionPath::new(collection_name.as_bytes()).unwrap(),
                    ))
                    .expect("open contended collection");
                let background_keys = make_prefixed_keys("background", count);
                let measured_keys = make_prefixed_keys("measured", count);
                let background_initial = rt.block_on(seed_direct_workload(
                    workload,
                    &background_db,
                    &background_coll,
                    &background_keys,
                    8,
                ));
                let measured_initial = rt.block_on(seed_direct_workload(
                    workload,
                    &measured_db,
                    &measured_coll,
                    &measured_keys,
                    8,
                ));

                let background_sequence = Arc::new(AtomicUsize::new(background_initial));
                let task_sequence = background_sequence.clone();
                let task_db = background_db.clone();
                let task_coll = background_coll.clone();
                let task_keys = background_keys.clone();
                let contender = rt.spawn(async move {
                    loop {
                        let next = task_sequence.fetch_add(1, Ordering::Relaxed);
                        run_direct_workload(workload, &task_db, &task_coll, &task_keys, next, 8)
                            .await;
                    }
                });

                let measured_sequence = AtomicUsize::new(measured_initial);
                let label = format!("{}/{count}/{backend_name}", workload.label());
                let stats = rt.block_on(report_stats(
                    &format!("adr061_same_leaf/{label}"),
                    &measured_db,
                    || {
                        let next = measured_sequence.fetch_add(1, Ordering::Relaxed);
                        run_direct_workload(
                            workload,
                            &measured_db,
                            &measured_coll,
                            &measured_keys,
                            next,
                            8,
                        )
                    },
                ));
                verify_direct_gate(&label, &stats, false);

                group.bench_function(&label, |bch| {
                    bch.iter(|| {
                        let next = measured_sequence.fetch_add(1, Ordering::Relaxed);
                        rt.block_on(run_direct_workload(
                            workload,
                            &measured_db,
                            &measured_coll,
                            &measured_keys,
                            next,
                            8,
                        ));
                    });
                });

                contender.abort();
                let _ = rt.block_on(contender);
                rt.block_on(background_db.shutdown());
                rt.block_on(measured_db.shutdown());
            }
        }
    }
    group.finish();
}

fn bench_adr061_inline_boundaries(c: &mut Criterion, rt: &Runtime) {
    const PER_VALUE_MAX: usize = 1024;
    const AGGREGATE_MAX: usize = 16 * 1024;
    let mut group = c.benchmark_group("adr061_direct_inline_boundaries");
    group.sample_size(10);
    group.warm_up_time(Duration::from_millis(100));
    group.measurement_time(Duration::from_millis(250));

    for count in [2usize, 8, 32] {
        for (boundary, policy, value_len) in [
            (
                "per-value",
                InlinePolicy {
                    max_value_bytes: PER_VALUE_MAX,
                    max_leaf_bytes: count * PER_VALUE_MAX,
                },
                PER_VALUE_MAX - 1,
            ),
            (
                "aggregate",
                InlinePolicy {
                    max_value_bytes: AGGREGATE_MAX,
                    max_leaf_bytes: AGGREGATE_MAX,
                },
                (AGGREGATE_MAX - 128) / count,
            ),
        ] {
            let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
            let collection_name = format!("a61-boundary-{boundary}-{count}");
            let (db, coll) = rt.block_on(open_coll_with_inline(
                backend,
                collection_name.as_bytes(),
                policy,
            ));
            let keys = make_prefixed_keys("boundary", count);
            let initial = rt.block_on(seed_direct_workload(
                DirectWorkload::BlindPut,
                &db,
                &coll,
                &keys,
                value_len,
            ));
            let sequence = AtomicUsize::new(initial);
            let label = format!("{boundary}/{count}/memory");
            let stats = rt.block_on(report_stats(
                &format!("adr061_boundary/{label}"),
                &db,
                || {
                    let next = sequence.fetch_add(1, Ordering::Relaxed);
                    run_direct_workload(
                        DirectWorkload::BlindPut,
                        &db,
                        &coll,
                        &keys,
                        next,
                        value_len,
                    )
                },
            ));
            verify_direct_gate(&label, &stats, true);

            group.bench_function(&label, |bch| {
                bch.iter(|| {
                    let next = sequence.fetch_add(1, Ordering::Relaxed);
                    rt.block_on(run_direct_workload(
                        DirectWorkload::BlindPut,
                        &db,
                        &coll,
                        &keys,
                        next,
                        value_len,
                    ));
                });
            });
            rt.block_on(db.shutdown());
        }
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
    bench_adr061_low_contention(c, &rt);
    bench_adr061_same_leaf_contention(c, &rt);
    bench_adr061_inline_boundaries(c, &rt);
}

criterion_group!(transactions, benches);
criterion_main!(transactions);
