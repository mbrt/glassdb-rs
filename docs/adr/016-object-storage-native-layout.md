# ADR-016: Object-storage-native layout (MVCC + S2PL on a sharded directory)

## Status

Accepted

## Context

GlassDB stores per-key **lock state** (`lock-type`, `locked-by`, `last-writer`)
in each value object's **metadata tags**, and acquires locks with a conditional
metadata-only update (`set_tags_if`, in `glassdb-storage/src/locker.rs`). This
maps cleanly onto GCS — a `PATCH` with `ifMetagenerationMatch` touches metadata
without rewriting the body — but **S3 has no metadata-only update**. The S3
backend therefore emulates `set_tags_if` by re-uploading the whole object
(GET full body + PUT full body) on every lock change.

On S3 this is pathological:

- A multi-key write transaction pays, per key, a `set_tags_if` to take the lock
  (GET + PUT of the *entire value*) plus a `write_if` on unlock (another full
  PUT). Value bytes move ~3× their size just to flip lock bits.
- An 8-byte nonce is prepended to every body solely to force a fresh ETag on a
  metadata-only rewrite, so compare-and-swap still works.
- `delete_if` is a non-atomic HEAD-then-DELETE with a documented TOCTOU window.

The only object-storage primitive the engine needs that S3 lacks is **conditional
metadata mutation**. Content CAS (`If-Match` / `If-None-Match`) is native on both
S3 (conditional writes, GA 2024) and GCS (generation preconditions). The layout
is the problem, not the engine's correctness.

## Decision

Replace the tag-based layout with an **object-storage-native** one that relies
only on primitives both stores have: **MVCC for values + S2PL for isolation**,
with a sharded coordination directory whose state is mutated by **content CAS**.
No object tags anywhere.

Three object kinds (details in the follow-on ADRs):

- **Shard** — a fixed number `C` of objects per collection form the coordination
  directory. Each shard owns a hash-range of keys and is simultaneously the
  **lock table**, the **MVCC version index** (current-writer txid per key), and
  the **per-shard key directory** (which of its keys exist). It is the unit of
  CAS; reading or writing an *existing* key touches only its shard.
- **Transaction object** — unified; small while pending (lease + lock
  intentions), fat once committed (it then carries the transaction's written
  values). Values live *only* here; there are no per-key value objects.
- **Collection root** — small; records collection existence, the (constant)
  shard count, and the **list of subcollections**. It is the
  **membership-coordination point**: key creation and deletion take a write lock
  on it so the key set changes consistently (phantom prevention), and key /
  subcollection listing validates against its version optimistically, taking a
  read lock only under contention.

Isolation remains **strict serializable**, enforced by the same S2PL +
wound-wait protocol ([ADR-002](002-wound-wait-locking.md)) relocated to shard
granularity. Commit is the CAS that flips the transaction object to committed;
write-back is an async per-shard CAS that publishes current-writer pointers and
releases locks together. The in-doubt reasoning of
[ADR-009](009-in-doubt-conditional-writes.md) carries over to the new CAS sites.

This **replaces the on-storage format wholesale**. Both the S3 and GCS backends
adopt the new layout, and on-disk / commit-protocol compatibility with the Go
original is **dropped**.

This ADR records only the umbrella direction. The sub-decisions each get their
own ADR — sharded directory, unified transaction objects, commit/write-back,
wound-wait at shard granularity, mark-sweep GC, and the slimmed `Backend` trait
— and the overall effort, staging, and open questions are tracked in
[`docs/historical/algo-v2.md`](../historical/algo-v2.md).

## Consequences

- The engine depends only on content CAS, which S3 and GCS both provide
  natively. The worst S3 cost — rewriting whole values to flip lock bits —
  disappears, and a value is uploaded exactly once (into its transaction object).
- The `Backend` trait sheds the GCS-shaped primitives S3 lacks (`get_metadata`,
  `set_tags_if`, `read_if_modified`) along with all tags, the S3 nonce, and
  `delete_if` (and its TOCTOU window). Target surface: `read`, `write`,
  `write_if`, `write_if_not_exists`, `delete`, `list`.
- New cost: **shard-granularity false sharing**. Transactions touching the same
  shard serialize on its CAS even when their keys differ, so `C` becomes the
  write-parallelism knob and bounds per-shard throughput (roughly `1 / RTT`).
- Reads change shape: a strong read consults the (cached, conditionally-GET'd)
  shard for the current writer, then materializes the value from that immutable
  transaction object. Co-located keys share one shard read, and immutable value
  blobs are cacheable indefinitely.
- Key creation and deletion serialize on the collection root (the membership
  lock), as in the current design; reads and writes of *existing* keys do not, so
  the hot path is unaffected. Listing is optimistic against the root version,
  with a read-lock fallback under contention.
- Garbage collection becomes a reachability problem — a transaction object is
  live while any shard references its txid — handled by mark-sweep in the MVP.
- Dropping Go format compatibility means regenerating the golden vectors and
  `RecordingBackend` byte-stream expectations; the layout-independent DST oracles
  (RMW serializability, the cycle ring) carry over unchanged as the safety net.
- A fixed compile-time `C` caps collection size at `C × keys-per-shard`;
  unbounded growth needs v2 split-resharding. Compaction of fragmented fat blobs
  is likewise deferred.
