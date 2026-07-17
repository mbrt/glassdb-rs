//! Microbenchmarks for the byte-weighted LRU cache.
//!
//! All keys are generated to hash to a single shard so the per-shard
//! data-structure cost (LRU ordering on access, delete-by-key, insert) is
//! measured directly instead of being diluted across shards.
//!
//! Run with `cargo bench -p glassdb-storage --bench cache`. To A/B two
//! implementations, use criterion baselines:
//!
//! ```text
//! cargo bench -p glassdb-storage --bench cache -- --save-baseline vec
//! # change the implementation, then:
//! cargo bench -p glassdb-storage --bench cache -- --baseline vec
//! ```

use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, criterion_group, criterion_main};
use glassdb_concurr::shard;
use glassdb_storage::{Cache, Weighable};

const WORKING_SET_SIZES: [usize; 3] = [1_000, 10_000, 100_000];
const ENTRY_BYTES: usize = 64;

#[derive(Clone)]
struct Payload {
    bytes: Arc<[u8]>,
}

impl Payload {
    fn new() -> Self {
        Payload {
            bytes: Arc::from(vec![0u8; ENTRY_BYTES].into_boxed_slice()),
        }
    }
}

impl Weighable for Payload {
    fn size(&self) -> usize {
        self.bytes.len()
    }
}

/// Returns `count` distinct keys that all hash to `target`, so a single `Cache`
/// shard holds the entire working set.
fn keys_for_shard(target: usize, n_shards: usize, count: usize) -> Vec<String> {
    let mut res = Vec::with_capacity(count);
    let mut i = 0u64;
    while res.len() < count {
        let k = format!("key{i}");
        if shard::index(k.as_bytes(), n_shards) == target {
            res.push(k);
        }
        i += 1;
    }
    res
}

/// Builds a cache whose targeted shard comfortably holds `keys.len()` entries
/// without eviction, pre-filled with `keys`.
fn filled_cache(keys: &[String]) -> Cache<Payload> {
    let n = keys.len();
    let n_shards = shard::count();
    // `Cache::new` divides the budget evenly across shards, so size the total so
    // the single shard under test has ~2x headroom over the working set.
    let max_size = n * ENTRY_BYTES * n_shards * 2 + n_shards * 1024;
    let cache = Cache::new(max_size);
    for k in keys {
        cache.set(k, Payload::new());
    }
    cache
}

/// `get` of a hot key refreshes its LRU recency on every call.
fn bench_get(c: &mut Criterion) {
    let n_shards = shard::count();
    let mut group = c.benchmark_group("cache_get_hot");
    group.sample_size(10);
    for &n in &WORKING_SET_SIZES {
        let keys = keys_for_shard(0, n_shards, n);
        let cache = filled_cache(&keys);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut i = 0usize;
            b.iter(|| {
                let k = &keys[i % n];
                i = i.wrapping_add(1);
                cache.get(k)
            });
        });
    }
    group.finish();
}

/// `update` on an existing key refreshes its LRU recency and replaces the value.
fn bench_update(c: &mut Criterion) {
    let n_shards = shard::count();
    let mut group = c.benchmark_group("cache_update_existing");
    group.sample_size(10);
    for &n in &WORKING_SET_SIZES {
        let keys = keys_for_shard(0, n_shards, n);
        let cache = filled_cache(&keys);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut i = 0usize;
            b.iter(|| {
                let k = &keys[i % n];
                i = i.wrapping_add(1);
                cache.update(k, |old| old.or_else(|| Some(Payload::new())));
            });
        });
    }
    group.finish();
}

/// Steady-state churn: each iteration inserts/refreshes one key and deletes a
/// different one, keeping membership bounded (no eviction) so the insert and
/// delete bookkeeping cost is what is measured.
fn bench_churn(c: &mut Criterion) {
    let n_shards = shard::count();
    let mut group = c.benchmark_group("cache_insert_delete_churn");
    group.sample_size(10);
    for &n in &WORKING_SET_SIZES {
        let pool = 2 * n;
        let keys = keys_for_shard(0, n_shards, pool);
        let cache = filled_cache(&keys[..n]);
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let mut i = 0usize;
            b.iter(|| {
                cache.set(&keys[i % pool], Payload::new());
                cache.delete(&keys[(i + n) % pool]);
                i = i.wrapping_add(1);
            });
        });
    }
    group.finish();
}

fn benches(c: &mut Criterion) {
    bench_get(c);
    bench_update(c);
    bench_churn(c);
}

criterion_group!(cache, benches);
criterion_main!(cache);
