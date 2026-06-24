# ADR-017: Shard object — model, mapping, and encoding

## Status

Proposed

## Context

[ADR-016](016-object-storage-native-layout.md) moves coordination state from
object tags into object content: a fixed set of `C` **shard** objects per
collection forms the coordination directory — simultaneously the lock table, the
MVCC version index (current-writer txid per key), and the per-shard key
directory. The shard is the unit of compare-and-swap.

We want to build and verify v2 incrementally, without first settling the whole
protocol. The shard's **data model**, its **key→shard mapping**, and its
**on-disk encoding** are the foundation every later piece builds on, and — exactly
like today's pure `compute_lock_update` (`glassdb-storage/src/locker.rs`) and the
tx-log marshalling (`glassdb-storage/src/tlogger.rs`) — they can be defined and
unit-tested as pure, I/O-free units. The encoding in particular is worth fixing
first: it is persisted and compared across processes, so it should be canonical
and golden-vector-anchored before anything depends on it.

This ADR therefore fixes **only the shard's structure, mapping, representation,
and pure read-side interpretation**. Everything else is out of scope and decided
in later ADRs.

In other words, this ADR defines an **inert data type**: no mutation policy, no
I/O: the largest piece we can land and verify in isolation.

## Decision

### Key → shard mapping

- Each collection has a **fixed `C` shards**, a compile-time constant, with
  `C = 1024` for the MVP. Rationale: at the 50k-key benchmark that is ~50
  keys/shard and few-KB shards (cheap CAS) while giving 1024-way write
  parallelism across shards; the ceiling is `C × per-shard soft cap`. `C` is
  **part of the on-disk format**: it must be a power of two (so the modulo is a
  mask), and changing it remaps every key, so it is a format-version constant,
  never a runtime option.
- `shard_index(key) = fnv1a(key) & (C - 1)`, reusing the inline FNV-1a already
  used for in-memory sharding (`glassdb-concurr::shard::index`,
  [ADR-001](001-sharded-db-maps.md)). It is dependency-free, deterministic, and
  stable across processes and under `--cfg sim` — all required for correctness
  (every client must agree on the mapping) and for DST replay. The hash is taken
  over the **raw user key bytes**, not the base64 path encoding. FNV-1a's low
  bits are adequate for bucketing (the existing distribution test confirms a
  reasonable spread); if distribution becomes a problem it can be changed behind
  the same `shard_index` function, but only as a format migration.

### Path

- Shard path: `{prefix}/_s/{index}`, adding a new `_s` path-type marker to
  `glassdb-data::paths` (alongside `_k`, `_c`, `_t`, `_i`). The index is a
  fixed-width zero-padded decimal (width = digits of `C - 1`, i.e. 4 for
  `C = 1024`), so paths are stable and lexicographically ordered. Shards are
  addressed by *computing* the index, so the encoding need only be a stable
  function of `(prefix, index)`; `parse` is extended to recognize `_s`.

### Shard contents

A shard holds one **entry per key it owns** that currently exists, is locked, or
is tombstoned. The **minimal** entry is:

- `key` — the key identity (raw key bytes); the entry's sort key.
- `lock_type` — `none | read | write | create`.
- `locked_by` — the set of txids holding the lock (more than one only for `read`).
- `current_writer` — the txid of the transaction object holding the committed
  value (the MVCC pointer); unset if the key has no committed value yet.
- `deleted` — tombstone flag.

Derived: a key **exists** iff it has an entry with `current_writer` set and
`deleted == false`. The set of a shard's entries is its slice of the
collection's key directory.

Only fields needed to express the lock table + version index are included.
Fields required solely by the protocol, wound-wait, or GC (e.g. a previous-writer
for reclamation, lease back-references) are added by those ADRs, not here.

### Encoding

- The shard **body** is the CAS unit; it changes on every mutation, so ETag /
  generation `If-Match` is real compare-and-swap with **no nonce**.
- Encoded as **protobuf** via a new `glassdb-proto` message, reusing the existing
  toolchain. Entries are serialized **sorted by key** so the encoding is
  canonical, deterministic, and anchored by golden vectors — the same
  encoding-fidelity practice used for paths and tx logs.
- Soft size budget per shard (~256 keys / tens of KB) to keep CAS cheap;
  exceeding it is an accepted MVP limitation (resharding deferred, ADR-016).

### Read-side interpretation (pure)

- Decoding a shard yields a queryable view: `lookup(key) -> Option<ShardEntry>`
  exposing existence, lock state, and current-writer. This is the read path used
  later by validation and reads. It is pure and unit-testable. **No mutation API**
  is defined here.

### Placement

The `Shard` type and its encoding live in `glassdb-storage` (next to `locker` /
`tlogger`); the `_s` marker and `shard_index` in `glassdb-data` (`paths` plus a
small mapping function reusing the FNV-1a from `glassdb-concurr`). Exact module
layout is an implementation detail.

## Consequences

- A self-contained first increment can land immediately: a `Shard` type with
  `shard_index`, the `_s` path, protobuf encode/decode, and `lookup`, covered by
  round-trip, determinism, distribution, and golden-vector tests — with **no
  dependency on any other v2 ADR**. This is the fastest path to verified code.
- The on-disk encoding and the key→shard mapping are pinned and golden-anchored
  early, before consumers exist — the highest-leverage things to stabilize.
- The shard is the CAS unit, so all coordination for its keys serializes on it:
  the shard-granularity false-sharing trade from ADR-016, with `C` as the knob.
- `C` and the FNV-1a mapping are load-bearing format constants; changing either
  is a format migration (and v2 resharding is out of scope).
- The type is inert until later ADRs add mutation: this increment delivers the
  data layer only, no end-to-end behavior. That is the explicit cost of the
  minimal scope.
