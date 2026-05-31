# ADR-001: Sharded DB-level maps to reduce lock contention

## Status

Accepted

## Context

Several DB-level data structures are shared across the whole database instance
and were each guarded by a single `std::sync::Mutex`. Every key- or
transaction-keyed operation across all tasks serialized on that one lock, making
it a contention bottleneck under concurrent load. The affected structures were:

- `glassdb-storage::cache::Cache` - the LRU object/metadata cache, hit on every
  read/write through `storage::Local`.
- `glassdb-concurr::Dedup` - the singleflight-style request de-duplicator used by
  the locker.
- `glassdb-trans::tlocker::Locker` `tlocks` - per-transaction held-lock tracking.
- `glassdb-trans::monitor::Monitor` - the `local_tx`, `waiters`, and `unknown_tx`
  transaction-state maps.

All four are keyed by a single key (object path) or transaction ID per operation
and are never iterated globally, which makes them amenable to partitioning.

## Decision

Partition each structure into `N` independent shards selected by a hash of the
key, mirroring [tarndt/shardedsingleflight](https://github.com/tarndt/shardedsingleflight).
`N = next_pow2(available_parallelism())` is computed once at construction (the
Rust analog of Go's `GOMAXPROCS`), so the shard index reduces to a bit mask.

A new `glassdb-concurr::shard` module provides the shared primitives:

- `count()` - shard count (next power of two `>= available_parallelism()`).
- `index(key, n)` - inline FNV-1a hash masked to `[0, n)`, taking bytes so it
  serves both `&str` paths and `TxId` bytes.
- A thin generic `Sharded<T>` container (`new`/`for_key`/`each`/`len`) that owns
  the shards and routes keys, leaving each component free to define its own
  bespoke per-shard struct.

Each component keeps its own shard type and per-shard mutex:

- `CacheShard` (LRU list + entries + byte budget),
- `Controller` (the existing dedup controller, now one per shard),
- the `tlocks` map (one `Mutex<HashMap<..>>` per shard),
- the monitor's three transaction maps, grouped under one lock per shard so
  their cross-map updates for a given transaction stay atomic.

The cache's byte budget is split evenly across shards (`max_size_b / count()`).
To avoid a freshly written entry larger than the per-shard budget being evicted
immediately - which previously starved the locker that reads back its own writes
and could livelock - the per-shard LRU never evicts the most-recently-used entry.
Overshoot is therefore bounded to one entry per shard, and the effective maximum
cacheable entry size returns to roughly `max_size_b`.

## Consequences

- Lock contention on the hot DB-level maps drops roughly proportionally to the
  shard count, since unrelated keys/transactions no longer share a lock.
- The routing and shard-count logic lives in one tested place
  (`glassdb-concurr::shard`) and is reused by all four components, avoiding
  duplicated boilerplate.
- Cache eviction is now per-shard (approximate LRU) rather than global, and the
  global byte budget is approximate (bounded overshoot of one entry per shard).
  This is acceptable for a best-effort cache.
- Public constructors (`Cache::new`, `Dedup::new`, `Locker::new`,
  `Monitor::new`) keep their signatures, so callers are unaffected.
- Behavior depends on `available_parallelism()`: very small cache sizes are split
  into very small per-shard budgets, so tests that need deterministic LRU
  semantics drive a single `CacheShard` directly.
