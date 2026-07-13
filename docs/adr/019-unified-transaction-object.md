# ADR-019: Values in the unified transaction object

## Status

Accepted — implemented (`glassdb-storage::txobject` codec + the transaction
engine)

The flat `_t/<txid>` path is refined by
[ADR-034](034-paginated-listing-and-sharded-transaction-logs.md) into a
deterministically sharded transaction-log layout. The unified object and
lifecycle decided here are unchanged.

## Context

[ADR-016](016-object-storage-native-layout.md) decided that values live **only**
in transaction objects and that a key's current version is a *pointer*
(`current_writer` txid) held in its shard ([ADR-017](017-shard-object.md)). This
ADR fixes what that object *is*: its contents, why status and values stay in one
object, and the pending → committed → aborted lifecycle with its commit point.

Today (v1) values live in two places. The per-key value object is the durable
home: the locker writes the staged value into it on unlock/write-back, tagged
with the writer's id (`glassdb-storage/src/locker.rs`). The transaction log
(`glassdb-storage/src/tlogger.rs`, `_t/<txid>`) is the transient home: it already
carries the written values (`Write.value`), the lock set, and a status — with the
commit `status` and `timestamp` *duplicated into object tags* so a peer can check
commit state with a cheap metadata read instead of downloading the body. Readers
resolve a locked key by reading the locker transaction's status and then its
`committed_value` from that log (`Algo::validate_locked_read`).

Two things force a redesign. First, ADR-016 removes per-key value objects
entirely, so the transaction object becomes the *only* home for values. Second,
it removes tags, so status can no longer live in metadata. The natural question
is whether to keep status and values together or split the object into a small
status header plus a separate value blob.

This ADR settles the object model and lifecycle. The validate/lock/write-back
*sequencing* is ADR-020; lease/expiry *mechanics* are ADR-021; reclamation is
ADR-022.

## Decision

### Values live only in the transaction object

There are no per-key value objects. A key's version *is* the value recorded in
the transaction object of the txid that wrote it; the shard's `current_writer` is
the pointer to it. A strong read is: shard → `current_writer` txid → GET that
transaction object → take the key's entry from its value map. Because a committed
transaction object never changes, the materialized value is **immutable and
cacheable indefinitely**, and keys co-written by one transaction share a single
object (one GET).

Consequently **write-back no longer copies values anywhere**: it only publishes
the `current_writer` pointer and releases locks in the shard (ADR-020). The value
was already uploaded once, when the transaction committed.

### One unified object, not a split header + value blob

The transaction object at `_t/<txid>` is **unified**: small while pending, fat
once committed (it then carries the transaction's value map). We rejected
splitting it into a tiny status object plus a separate value blob:

- **The hot consumer needs both.** Resolving a key reads the writer's *status*
  and then its *value*; with a split that is two GETs on the read path. Unified
  serves both from one (cacheable) object.
- **Committed objects are immutable**, so status and values cache together as one
  entry; a split adds a second object and a second cache entry for no gain.
- **The flip rewrites the object anyway.** Object stores have no partial update,
  so moving pending → committed re-PUTs the whole object regardless; splitting
  saves no write, it only adds an object and a second write to keep consistent.
- **Status-only consumers don't read the fat body.** Wound-wait orders by the
  txid's own timestamp ([ADR-002](002-wound-wait-locking.md)) and only needs the
  small *pending* object for liveness/lease; GC decides reachability from shard
  references (`current_writer ∪ locked_by`, ADR-022), never by downloading
  values. So the "check status without downloading values" argument for splitting
  is largely moot.

Status and timestamp therefore move from tags **into the body**; tags disappear
(ADR-016).

### Lifecycle: pending → committed | aborted

- **Pending** (created with `write_if_not_exists` at first lock acquisition, so
  peers can resolve a shard `locked_by` entry to a live transaction): status
  `pending`, the wound-wait timestamp, the **lock intentions** (the shards/keys
  and lock types it is taking — the successor of v1's tx-log `locks`), and the
  lease/expiry state. It carries **no authoritative values** yet. Its role is to
  let peers and recovery reason about an in-flight transaction (is it alive? what
  is it locking?).
- **Committed**: a single CAS replaces the pending object with status
  `committed` **and the full value map** — every write as a (key, value) or
  (key, tombstone). This write is the only place values become durable.
- **Aborted**: status `aborted` (small, no value map), reached from pending by a
  wound or a self-abort.

### The commit point

The pending → committed CAS **is** the commit point and the transaction's
linearization point. Because status and values are written by the *same* CAS,
there is never a "committed but values missing" window: any reader that observes
`committed` can read every value the transaction wrote. Before the flip the
transaction has no effect — shards merely reference its txid as a locker; its
values are invisible. After it, write-back asynchronously and idempotently
publishes `current_writer` pointers and releases locks (ADR-020). A reader or
recoverer that finds a shard pointing at a txid consults its object: `committed`
→ use the value; `pending` → not yet effective (wait/help/fall back to the prior
writer, per ADR-020); `aborted` → ignore.

The commit CAS is the single **in-doubt** point, and
[ADR-009](009-in-doubt-conditional-writes.md)'s reasoning carries over: the object
is keyed by txid and its committed body is a deterministic function of the
transaction's writes, so a re-issued commit after an `Unavailable` outcome is
idempotent — it either lands or finds the object already committed by the same
txid (the v2 analog of v1's `set_final_log`). Exact retry sequencing is ADR-020.

### Encoding

Reuse the `glassdb-proto` toolchain, evolving the existing `TransactionLog`
message, which already has the `status` enum (including `PENDING`), a
`timestamp`, the per-collection `writes` (each a key suffix + value/`deleted` +
`prev_tid`), and the lock intentions (`CollectionLocks`). v2 makes `status` and
`timestamp` authoritative in the body (no tags), treats `writes` as the committed
value map and the locks as the pending intentions, and adds the lease/expiry
field defined by ADR-021. Encoding stays canonical and golden-anchored, like the
shard and the path encodings.

## Consequences

- A value is uploaded **exactly once**, into its transaction object; ADR-016's
  worst S3 cost — rewriting whole values to flip lock bits — is gone, and reads
  materialize from immutable, indefinitely-cacheable objects.
- **Read amplification for multi-key writes**: reading one key downloads the
  whole writing transaction's value map, not just that key. Fine for the common
  small/single-key write; large batch writes inflate later single-key reads.
  Immutability + caching amortize it; splitting fat blobs (compaction) is deferred
  to ADR-022.
- **Fat-blob liveness**: a committed object stays live while any shard references
  its txid, so one cold key can pin a blob full of otherwise-dead values. No
  compaction in the MVP — an accepted limitation (ADR-016/022).
- A status check is now a body GET rather than a cheap tag/metadata read. This is
  acceptable because status consumers almost always want the value too, and the
  *pending* object (the one peers poll for liveness) is small.
- The class of anomaly [ADR-007](007-single-rw-cache-lost-update.md) guards
  against — a committed value with no discoverable committed status (the v1
  logless single-RW writer) — **cannot arise**, because a value and its committed
  status are now the *same* object; any fast path must still produce a committed
  transaction object.
- Write-back shrinks to publishing a pointer and releasing locks in the shard,
  removing the value copy, the last-writer tag, and the S3 nonce (ADR-016/023).
- The encoding evolves `TransactionLog`; new golden vectors are needed, and Go
  on-disk compatibility is already dropped (ADR-016).
- This ADR is the object model only; it is behavior-complete only with the
  commit/write-back protocol (ADR-020) and lease mechanics (ADR-021).
