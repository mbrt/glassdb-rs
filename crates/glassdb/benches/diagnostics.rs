//! Bounded, named conditions for revision comparisons. Wall time is statistical.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use criterion::{Criterion, SamplingMode, Throughput, criterion_group, criterion_main};
use futures::future::try_join_all;
use glassdb::{Backend, Collection, Database, Error, Stats};
use glassdb_backend::memory::MemoryBackend;
use glassdb_backend::{BackendError, ListCursor, ListLimit, ListPage, ReadReply, Version};
use serde_json::{Value, json};

const COST_ITERATIONS: usize = 30;

#[derive(Clone, Copy, PartialEq, Eq)]
enum Case {
    WarmRead,
    FreshRead,
    InlineRmw,
    ExternalRmw,
    MultiLeafRmw,
    LargeRead,
    SharedLeafRmw,
}

impl Case {
    fn name(self) -> &'static str {
        match self {
            Self::WarmRead => "warm_read",
            Self::FreshRead => "fresh_client_read",
            Self::InlineRmw => "rmw_inline_1024",
            Self::ExternalRmw => "rmw_external_1025",
            Self::MultiLeafRmw => "rmw_five_leaves",
            Self::LargeRead => "read_long_keys_large_collection",
            Self::SharedLeafRmw => "rmw_shared_leaf_three_transactions",
        }
    }

    fn transactions(self) -> u64 {
        if self == Self::SharedLeafRmw { 3 } else { 1 }
    }

    fn value_size(self) -> usize {
        match self {
            Self::InlineRmw => 1024,
            Self::ExternalRmw => 1025,
            _ => 256,
        }
    }
}

const CASES: [Case; 7] = [
    Case::WarmRead,
    Case::FreshRead,
    Case::InlineRmw,
    Case::ExternalRmw,
    Case::MultiLeafRmw,
    Case::LargeRead,
    Case::SharedLeafRmw,
];

struct Fixture {
    db: Database,
    backend: Arc<dyn Backend>,
    members: Vec<(Collection, Vec<u8>)>,
    setup_splits: u64,
}

impl Fixture {
    async fn prepare(case: Case, backend: Arc<dyn Backend>) -> Self {
        let db = Database::open("diagnostics", backend.clone())
            .await
            .expect("open fixture");
        let collection_count = if case == Case::MultiLeafRmw { 5 } else { 1 };
        let key_count = match case {
            Case::LargeRead => 1024,
            Case::SharedLeafRmw => 3,
            _ => 1,
        };
        let value = vec![0; case.value_size()];
        for index in 0..collection_count {
            let coll = db
                .root_collection()
                .create_collection_if_absent(format!("c{index}").as_bytes())
                .await
                .expect("create fixture collection");
            for chunk in (0..key_count).collect::<Vec<_>>().chunks(64) {
                db.tx(|tx| {
                    let coll = &coll;
                    let value = &value;
                    async move {
                        for &key in chunk {
                            tx.write(coll, &key_bytes(case, key), value)?;
                        }
                        Ok(())
                    }
                })
                .await
                .expect("seed fixture");
            }
        }
        if case == Case::LargeRead {
            let start = Instant::now();
            let mut quiet = Instant::now();
            let mut completed = 0;
            loop {
                let current = db.stats().splitter.completed;
                if current != completed {
                    completed = current;
                    quiet = Instant::now();
                }
                if completed > 0 && quiet.elapsed() >= Duration::from_millis(1200) {
                    break;
                }
                assert!(
                    start.elapsed() < Duration::from_secs(15),
                    "large fixture did not split and settle"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        let setup_splits = db.stats().splitter.completed;
        db.shutdown().await;
        let mut fixture = Self {
            db,
            backend,
            members: Vec::new(),
            setup_splits,
        };
        fixture.reopen(case).await;
        if case != Case::FreshRead {
            // Preparation uses the same public reads that applications use to warm caches.
            for (coll, key) in &fixture.members {
                coll.read(key).await.expect("warm fixture");
            }
        }
        fixture
    }

    async fn reopen(&mut self, case: Case) {
        self.db = Database::open("diagnostics", self.backend.clone())
            .await
            .expect("reopen fixture");
        self.members.clear();
        let count = match case {
            Case::MultiLeafRmw => 5,
            Case::SharedLeafRmw => 3,
            _ => 1,
        };
        for index in 0..count {
            let coll_index = if case == Case::MultiLeafRmw { index } else { 0 };
            let coll = self
                .db
                .open_collection(format!("c{coll_index}").as_str())
                .await
                .expect("open fixture collection");
            let key = if case == Case::SharedLeafRmw {
                index
            } else if case == Case::LargeRead {
                512
            } else {
                0
            };
            self.members.push((coll, key_bytes(case, key)));
        }
    }

    async fn run(&self, case: Case) -> Result<(), Error> {
        match case {
            Case::WarmRead | Case::FreshRead | Case::LargeRead => {
                let (coll, key) = &self.members[0];
                let value = coll
                    .read(key)
                    .await?
                    .ok_or_else(|| Error::internal("missing fixture key"))?;
                std::hint::black_box(value);
                Ok(())
            }
            Case::SharedLeafRmw => {
                try_join_all(
                    self.members
                        .iter()
                        .map(|member| update(&self.db, std::slice::from_ref(member))),
                )
                .await?;
                Ok(())
            }
            _ => update(&self.db, &self.members).await,
        }
    }
}

fn key_bytes(case: Case, index: usize) -> Vec<u8> {
    if case == Case::LargeRead {
        format!("{index:0256}").into_bytes()
    } else {
        format!("k{index:04}").into_bytes()
    }
}

async fn update(db: &Database, members: &[(Collection, Vec<u8>)]) -> Result<(), Error> {
    db.tx(|tx| async move {
        let values = try_join_all(members.iter().map(|(coll, key)| tx.read(coll, key))).await?;
        for ((coll, key), value) in members.iter().zip(values) {
            let mut value = value.ok_or_else(|| Error::internal("missing fixture key"))?;
            let first = value
                .first_mut()
                .ok_or_else(|| Error::internal("empty fixture value"))?;
            *first = first.wrapping_add(1);
            tx.write(coll, key, &value)?;
        }
        Ok(())
    })
    .await
}

#[derive(Default)]
struct Window {
    stats: Stats,
    bytes: [u64; 2],
    elapsed: Duration,
}

impl Window {
    fn record(&mut self, before: (Stats, [u64; 2], Instant), db: &Database, bytes: &BodyBytes) {
        self.elapsed += before.2.elapsed();
        self.stats += db.stats() - before.0;
        let after = bytes.snapshot();
        for (index, value) in after.iter().enumerate() {
            self.bytes[index] += value - before.1[index];
        }
    }

    fn json(&self, transactions: u64) -> Value {
        let n = transactions as f64;
        json!({
            "reads": self.stats.backend.obj_reads as f64 / n,
            "writes": self.stats.backend.obj_writes as f64 / n,
            "lists": self.stats.backend.obj_lists as f64 / n,
            "readBodyBytes": self.bytes[0] as f64 / n,
            "writeBodyBytes": self.bytes[1] as f64 / n,
            "coordinatorSubmissions": self.stats.coordinator.submissions as f64 / n,
            "coordinatorRounds": self.stats.coordinator.rounds as f64 / n,
            "elapsedMs": self.elapsed.as_secs_f64() * 1000.0,
        })
    }
}

async fn measure_cost(case: Case) -> Value {
    let bytes = Arc::new(BodyBytes::new(Arc::new(MemoryBackend::new())));
    let mut fixture = Fixture::prepare(case, bytes.clone()).await;
    let mut workload = Window::default();
    let mut shutdown = Window::default();
    for iteration in 0..COST_ITERATIONS {
        if case == Case::FreshRead && iteration > 0 {
            fixture.reopen(case).await;
        }
        let before = (fixture.db.stats(), bytes.snapshot(), Instant::now());
        fixture.run(case).await.expect("cost workload");
        workload.record(before, &fixture.db, &bytes);
        if case == Case::FreshRead || iteration + 1 == COST_ITERATIONS {
            let before = (fixture.db.stats(), bytes.snapshot(), Instant::now());
            fixture.db.shutdown().await;
            shutdown.record(before, &fixture.db, &bytes);
        }
    }
    let n = COST_ITERATIONS as u64 * case.transactions();
    assert_eq!(
        workload.stats.transactions.completed, n,
        "all measured transactions must complete"
    );
    let mut combined = Window {
        stats: workload.stats,
        bytes: workload.bytes,
        elapsed: workload.elapsed,
    };
    combined.stats += shutdown.stats;
    combined.bytes[0] += shutdown.bytes[0];
    combined.bytes[1] += shutdown.bytes[1];
    combined.elapsed += shutdown.elapsed;
    json!({"name": case.name(), "transactions": n, "valueBytes": case.value_size(),
        "setupSplits": fixture.setup_splits, "workload": workload.json(n),
        "shutdown": shutdown.json(n), "combined": combined.json(n)})
}

fn benches(c: &mut Criterion) {
    glassdb_concurr::rt::set_model_time_speedup(20.0)
        .expect("configure diagnostic model time before creating the runtime");
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("diagnostic runtime");
    let mut costs = Vec::new();
    let mut group = c.benchmark_group("diagnostic");
    group
        .sample_size(20)
        .sampling_mode(SamplingMode::Flat)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(2))
        .noise_threshold(0.05);
    for case in CASES {
        let mut fixture = None;
        group.throughput(Throughput::Elements(case.transactions()));
        group.bench_function(case.name(), |b| {
            // Unselected cases do no setup; selected cases keep one fixture across samples.
            let fixture = fixture.get_or_insert_with(|| {
                costs.push(rt.block_on(measure_cost(case)));
                rt.block_on(Fixture::prepare(case, Arc::new(MemoryBackend::new())))
            });
            b.iter_custom(|iterations| {
                rt.block_on(async {
                    let mut elapsed = Duration::ZERO;
                    for _ in 0..iterations {
                        if case == Case::FreshRead {
                            fixture.db.shutdown().await;
                            fixture.reopen(case).await;
                        }
                        let start = Instant::now();
                        fixture.run(case).await.expect("timed workload");
                        elapsed += start.elapsed();
                    }
                    elapsed
                })
            })
        });
        if let Some(fixture) = fixture {
            rt.block_on(fixture.db.shutdown());
        }
    }
    group.finish();
    if !costs.is_empty() {
        println!(
            "diagnostic-costs: {}",
            json!({"schemaVersion": 1, "cases": costs})
        );
    }
}

criterion_group!(diagnostics, benches);
criterion_main!(diagnostics);

/// Counts successful read bodies and attempted write bodies, not wire traffic.
struct BodyBytes {
    inner: Arc<dyn Backend>,
    read: AtomicU64,
    written: AtomicU64,
}

impl BodyBytes {
    fn new(inner: Arc<dyn Backend>) -> Self {
        Self {
            inner,
            read: AtomicU64::new(0),
            written: AtomicU64::new(0),
        }
    }

    fn snapshot(&self) -> [u64; 2] {
        [
            self.read.load(Ordering::Relaxed),
            self.written.load(Ordering::Relaxed),
        ]
    }

    fn count_read(&self, reply: &ReadReply) {
        self.read
            .fetch_add(reply.contents.len() as u64, Ordering::Relaxed);
    }
}

#[async_trait]
impl Backend for BodyBytes {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError> {
        let reply = self.inner.read(path).await?;
        self.count_read(&reply);
        Ok(reply)
    }

    async fn read_if_modified(
        &self,
        path: &str,
        expected: &Version,
    ) -> Result<ReadReply, BackendError> {
        let reply = self.inner.read_if_modified(path, expected).await?;
        self.count_read(&reply);
        Ok(reply)
    }

    async fn write_if(
        &self,
        path: &str,
        value: Vec<u8>,
        expected: &Version,
    ) -> Result<Version, BackendError> {
        self.written
            .fetch_add(value.len() as u64, Ordering::Relaxed);
        self.inner.write_if(path, value, expected).await
    }

    async fn write_if_not_exists(
        &self,
        path: &str,
        value: Vec<u8>,
    ) -> Result<Version, BackendError> {
        self.written
            .fetch_add(value.len() as u64, Ordering::Relaxed);
        self.inner.write_if_not_exists(path, value).await
    }

    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError> {
        self.inner.delete_if(path, expected).await
    }

    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError> {
        self.inner.list(prefix, cursor, limit).await
    }
}
