# Architecture

This document describes the architecture and design choices of GlassDB. For the
full design narrative and motivation, see the companion blog post:
[Transactional Object
Storage](https://blog.mbrt.dev/posts/transactional-object-storage) (written for
the original Go version, but the design is identical). For the Rust-specific
porting decisions — the concurrency model, time/determinism, error handling, and
encoding fidelity — see [PORTING.md](../PORTING.md). For usage, performance
benchmarks, and examples, see the [README](../README.md).

## Design Goals & Tradeoffs

GlassDB is designed around a specific set of constraints:

- **Stateless clients, no server component.** The entire database is a
  client-side Rust library. There is no server to deploy, no coordinator, and no
  direct communication between clients. All coordination happens through object
  storage.
- **Optimistic locking.** Optimized for workloads where conflicts between
  transactions are rare. Readers are rarely blocked.
- **Strict serializability.** The strongest isolation level — transactions
  behave as if executed one at a time, in an order consistent with real time.
- **Throughput over latency.** Object storage is slow (50–150 ms per
  operation), but highly scalable. GlassDB leverages that parallelism.
- **Object storage as the only dependency.** Requires strong consistency and
  conditional writes (available in GCS and S3).

The explicit tradeoffs are:

- When transactions race, it's better to be slow than incorrect.
- High throughput is preferred over low latency.
- Values are expected in the 1 KB – 1 MB range.
- Stale reads are allowed if explicitly requested, but strong consistency is the
  default.

## High-Level Architecture

```
┌─────────────┐  ┌─────────────┐  ┌─────────────┐
│  Client A   │  │  Client B   │  │  Client C   │
│ ┌─────────┐ │  │ ┌─────────┐ │  │ ┌─────────┐ │
│ │ App     │ │  │ │ App     │ │  │ │ App     │ │
│ │ Code    │ │  │ │ Code    │ │  │ │ Code    │ │
│ ├─────────┤ │  │ ├─────────┤ │  │ ├─────────┤ │
│ │ GlassDB │ │  │ │ GlassDB │ │  │ │ GlassDB │ │
│ │ Library │ │  │ │ Library │ │  │ │ Library │ │
│ └────┬────┘ │  │ └────┬────┘ │  │ └────┬────┘ │
└──────┼──────┘  └──────┼──────┘  └──────┼──────┘
       │                │                │
       └────────────────┼────────────────┘
                        │
                        ▼
              ┌───────────────────┐
              │  Object Storage   │
              │  (e.g. GCS, S3)   │
              └───────────────────┘
```

Each client embeds GlassDB as a library. Clients are completely independent and
ephemeral — they can scale to zero and back without any coordination. The only
shared state is the object storage bucket, which provides strong consistency for
single-object operations and conditional writes for atomic state transitions.

## Crate Structure

The port is a Cargo workspace whose crates mirror the original Go `internal/`
and `backend/` package boundaries, so the mapping between the two codebases is
one-to-one. The dependency DAG is enforced at compile time (e.g. `storage`
cannot reach into `trans`):

```
glassdb-data → glassdb-backend → glassdb-storage → glassdb-trans → glassdb
glassdb-proto ─┘                  ↑                      ↑
glassdb-concurr ──────────────────┴──────────────────────┘
glassdb-backend-s3, glassdb-backend-gcs → glassdb (optional, feature-gated)
```

| Crate                 | Key modules                                                                  | Responsibility                                                                                                                                |
| --------------------- | ---------------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------------------- |
| `glassdb`             | `db.rs`, `tx.rs`, `collection.rs`, `iter.rs`, `stats.rs`                     | Public API: `Database`, `Transaction`, `Collection`, iterators, statistics                                                                    |
| `glassdb-backend`     | `lib.rs`, `memory.rs`, `stats.rs`, `middleware/`                             | The `Backend` trait, in-memory backend, stats decorator, and middleware (delay, scheduler, logger, fault, recording)                          |
| `glassdb-backend-s3`  | —                                                                            | Amazon S3 backend (`aws-sdk-s3`), enabled via the `s3` feature                                                                                |
| `glassdb-backend-gcs` | —                                                                            | Google Cloud Storage backend (GCS JSON API), enabled via the `gcs` feature                                                                    |
| `glassdb-trans`       | `algo.rs`, `tlocker.rs`, `monitor.rs`, `reader.rs`, `gc.rs`                  | Transaction engine: commit algorithm, distributed locker, lifecycle monitor, read path, log GC                                                |
| `glassdb-storage`     | `global.rs`, `local.rs`, `locker.rs`, `tlogger.rs`, `version.rs`, `cache.rs` | Backend read/write-through cache, local cache with staleness, lock-state encoding, transaction-log persistence, version tracking, generic LRU |
| `glassdb-data`        | `txid.rs`, `paths.rs`, `base64.rs`, `gopath.rs`                              | Core types: `TxId`, `TxIdSet`, order-preserving path encoding                                                                                 |
| `glassdb-proto`       | —                                                                            | `prost`-generated transaction-log protobuf messages                                                                                           |
| `glassdb-concurr`     | `background.rs`, `retry.rs`, `dedup.rs`, `clock.rs`                          | Concurrency utilities: `Background` tasks, retry/backoff, request deduplication, the `Clock` abstraction                                      |

Only the top-level `glassdb` crate is intended for direct use; the rest are
implementation detail. Its public API surface is small: `Database`,
`Transaction`, and `Collection`, plus the re-exported `Backend` trait and the
in-memory backend and middleware. The deterministic-simulation runtime (the
`rt`/`exec` seam in `glassdb-concurr`) is compiled only under `--cfg sim`; see
[PORTING.md](../PORTING.md) and [dst-approach.md](dst-approach.md).

## Backend Abstraction

The `Backend` trait (`glassdb-backend`) defines the contract with object
storage. It is an `async_trait`, and every method is cancellable by dropping the
returned future:

```rust
#[async_trait]
pub trait Backend: Send + Sync {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError>;
    async fn read_if_modified(
        &self, path: &str, expected_writer: &WriterId,
    ) -> Result<ReadReply, BackendError>;
    async fn get_metadata(&self, path: &str) -> Result<Metadata, BackendError>;
    async fn set_tags_if(
        &self, path: &str, expected: &Version, tags: Tags,
    ) -> Result<Metadata, BackendError>;
    async fn write(&self, path: &str, value: Vec<u8>, tags: Tags)
        -> Result<Metadata, BackendError>;
    async fn write_if(
        &self, path: &str, value: Vec<u8>, expected: &Version, tags: Tags,
    ) -> Result<Metadata, BackendError>;
    async fn write_if_not_exists(
        &self, path: &str, value: Vec<u8>, tags: Tags,
    ) -> Result<Metadata, BackendError>;
    async fn delete(&self, path: &str) -> Result<(), BackendError>;
    async fn delete_if(&self, path: &str, expected: &Version) -> Result<(), BackendError>;
    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError>;
}
```

### Key concepts

**Versions.** Every object has an opaque CAS token (`Version { token: Arc<str> }`),
assigned by the backend and used only for conditional operations. The format is
backend-specific: GCS encodes `"{generation}/{metageneration}"`, while S3 uses
the object's ETag. Consumers never interpret it — they pass it back unchanged to
`write_if` / `set_tags_if` / `delete_if`.

**Change detection.** To decide whether a value changed since it was read, the
algorithm compares the `last-writer` tag (the transaction that last wrote the
key) rather than the storage version. `read_if_modified` takes the expected
writer (`WriterId`) and returns `Precondition` when it still matches. This keeps
the backend abstraction independent of GCS-style monotonic versions and is what
allows the single-object S3 layout (see the Cloud backends section of
[PORTING.md](../PORTING.md)).

**Tags.** Key-value string pairs stored in object metadata (`Tags` is a
`BTreeMap<String, String>`, so iteration order is deterministic). GlassDB uses
tags to store lock state (`lock-type`, `locked-by`, `last-writer`) without
modifying the object's contents. Tags can be updated atomically and
conditionally via `set_tags_if`.

**Conditional operations.** `write_if`, `write_if_not_exists`, `set_tags_if`,
and `delete_if` all take an expected version and fail with
`BackendError::Precondition` if the object has been modified since. This is the
fundamental building block for distributed coordination — it provides
compare-and-swap (CAS) semantics.

**Error semantics** (`BackendError`):

- `NotFound` — object does not exist.
- `Precondition` — conditional operation failed (version mismatch).
- `Unavailable(_)` — the operation's outcome is _in doubt_: the request may or
  may not have been applied (e.g. a conditional write whose acknowledgement was
  lost and whose retry then saw a precondition failure, or an outage that
  exhausts the retry budget). Because the outcome is unknown, a non-idempotent
  operation must not be blindly retried. See
  [ADR-009](adr/009-in-doubt-conditional-writes.md).
- `Other(_)` — any other backend error.

`is_not_found`, `is_precondition`, and `is_unavailable` predicates preserve
the original sentinel-error matching semantics.

### Implementations

| Backend                       | Purpose             | Notes                                                                                                                                              |
| ----------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `glassdb-backend-gcs`         | Production          | GCS JSON API over `reqwest`; encodes `generation`/`metageneration` in the version token and stores tags in object custom metadata                  |
| `glassdb-backend-s3`          | Production          | One S3 object per key (`aws-sdk-s3`); user value with an 8-byte nonce in the body, tags in `x-amz-meta-*` user metadata, ETag as the version token |
| `glassdb-backend::memory`     | Testing             | In-process `MemoryBackend` simulating GCS semantics                                                                                                |
| `glassdb-backend::middleware` | Debugging / testing | Wrappers for logging, latency injection, byte-driven scheduling, fault injection, and op-stream recording                                          |

The cloud backends are feature-gated (`s3`, `gcs`) so their heavy SDK
dependencies are only pulled in when needed; each is tested against a pure-Rust
in-process fake of its API. See the Cloud backends section of
[PORTING.md](../PORTING.md).

## Transaction Algorithm

### Isolation & Consistency

GlassDB targets **strict serializability** — the combination of serializable
isolation and linearizable consistency. This is the strongest guarantee: all
transactions appear to execute one at a time, in an order consistent with real
time. No anomalies of any kind are possible.

This is achieved by combining two properties:

1. **Linearizable consistency**, provided natively by object storage (GCS, S3):
   any read initiated after a successful write returns that write's contents.
2. **Serializable isolation**, enforced by a modified Strict Two-Phase Locking
   (S2PL) protocol: all locks are held until after commit, preventing
   interleaving.

For a deeper discussion of isolation vs. consistency levels — including
comparisons with Postgres, Spanner, CockroachDB, and others — see the
[blog post](https://blog.mbrt.dev/posts/transactional-object-storage).

### Transaction Lifecycle

```
    ┌───────┐
    │ Begin │  Assign transaction ID, create handle
    └───┬───┘
        │
        ▼
    ┌─────────┐
    │ Execute │  User code runs: reads (tracked), writes (staged locally)
    └───┬─────┘
        │
        ▼
    ┌──────────┐
    │ Validate │  Acquire locks, verify read versions unchanged
    └───┬──────┘
        │
     conflict?
     ╱       ╲
   yes        no
    │          │
    ▼          ▼
 ┌───────┐  ┌────────┐
 │ Retry │  │ Commit │  Write transaction log atomically
 └───────┘  └───┬────┘
                │
                ▼
           ┌─────────┐
           │ Cleanup │  Async: write values back to keys, unlock, GC log
           └─────────┘
```

A transaction progresses through three internal states (`Status` in
`glassdb-trans/src/algo.rs`):

- **New** — transaction is executing user code.
- **Validating** — locks are being acquired and reads verified.
- **Committed** — transaction log written; commit is durable.

During **Execute**, reads go through the cache and are tracked (path + version).
Writes are staged in memory. No locks are held in this phase.

During **Validate**, the algorithm acquires locks and checks that every read
version still matches the current state. If any key was modified by a concurrent
transaction, the current transaction retries — but crucially, it retries with
locks still held, so the second attempt is guaranteed to succeed (at most one
retry).

After **Commit**, the transaction log is the durable record. The async cleanup
phase writes the new values back to keys, releases locks, and schedules the
transaction log for garbage collection.

Because `Database::tx` takes the body by value (`|tx| async move { ... }`) and
the framework owns the retry loop, a conflict simply reruns the closure. Dropping
the transaction future at any point is equivalent to a crash: the commit protocol
recovers any in-flight state (see [PORTING.md](../PORTING.md), "Cancel-safety
contract").

### Optimistic Concurrency Control

The core idea: **transactions run without locks until commit time.** This means
non-conflicting transactions never interfere with each other.

```
Transaction A (keys 1, 2)         Transaction B (keys 3, 4)
─────────────────────────         ─────────────────────────
Read key 1                        Read key 3
Read key 2                        Read key 4
Stage write to key 1              Stage write to key 3
  ── validate ──                    ── validate ──
Lock key 1, key 2                 Lock key 3, key 4
Verify versions                   Verify versions
Write tx log                      Write tx log
  ── commit ──                      ── commit ──
```

Since A and B touch different keys, they proceed fully in parallel — no waiting,
no retries. Locks are held only for the brief validate-and-commit window.

When transactions _do_ conflict:

1. Both reach the validate phase and try to lock overlapping keys.
2. One wins the lock; the other detects a version mismatch.
3. The loser retries with its locks held (pessimistic fallback), guaranteeing
   progress.

### Distributed Locks

Lock state is stored in the **object metadata tags** of each key
(`glassdb-storage/src/locker.rs`):

| Tag           | Values                          | Purpose                                       |
| ------------- | ------------------------------- | --------------------------------------------- |
| `lock-type`   | `r`, `w`, `c`, `-`              | Current lock type (read, write, create, none) |
| `locked-by`   | base64 tx IDs (comma-separated) | Which transactions hold the lock              |
| `last-writer` | base64 tx ID                    | Transaction that last wrote this key          |

Lock acquisition is a compare-and-swap on the metadata: read the current tags
and version, compute the new lock state, and conditionally write the updated tags
using `set_tags_if`. If the version changed (another transaction modified the
tags), the operation retries.

**Compatibility rules** (`LockType`: `None`, `Read`, `Write`, `Create`):

| Requested | Current: None |     Current: Read      | Current: Write | Current: Create |
| --------- | :-----------: | :--------------------: | :------------: | :-------------: |
| Read      |       ✓       |           ✓            |      wait      |      wait       |
| Write     |       ✓       | upgrade if sole holder |      wait      |      wait       |
| Create    |       ✓       |          wait          |      wait      |      wait       |

- Multiple transactions can hold **read** locks simultaneously.
- **Write** locks are exclusive. A read lock can be upgraded to write only if
  the requesting transaction is the sole holder.
- **Create** locks are used when a key doesn't yet exist, to prevent concurrent
  creation.

### Transaction Logs

Each transaction gets its own log object, stored at a deterministic path based
on the transaction ID:

```
<db-prefix>/_t/<base64-encoded-tx-id>
```

The transaction ID (`glassdb-data::TxId`) is `[8 bytes random prefix][8 bytes
big-endian UnixNano timestamp]`. The timestamp suffix encodes the wound-wait
priority (earlier = older), while the random prefix leads so that log keys keep
a high-entropy prefix and spread across object-store partitions instead of
clustering sequential commits into one hot partition.

The log is serialized as a Protocol Buffer (`glassdb-proto`, `prost`-generated
from a copy of `transaction.proto`) and contains:

- **Status**: pending, committed, or aborted.
- **Timestamp**: when the log was last updated.
- **Writes**: list of (path, value, deleted, previous writer) entries (the
  `oneof val_delete` layout is preserved byte-for-byte).
- **Locks**: list of (path, lock type) entries.

The transaction log serves two critical purposes:

1. **Atomic commit point.** A transaction is committed if and only if its log
   object exists with status "committed". All the multi-key writes become
   durable in a single object write.
2. **Crash recovery synchronization.** Other transactions can inspect a log to
   determine whether a lock holder is still active, and can attempt to abort
   an expired transaction by conditionally writing to its log.

### Commit Protocol

The validate-and-commit sequence:

1. **Parallel lock acquisition.** Lock all read and written keys in parallel.
   Conflicts are resolved by the wound-wait rule (see [Deadlock
   Handling](#deadlock-handling)): an older transaction aborts younger holders,
   a younger one waits. A 5-second timeout (`MAX_DEADLOCK_TIMEOUT`) falls back
   to serial locking only if contention prevents progress.

2. **Version verification.** For each read key, check that the version hasn't
   changed since the transaction read it. If it has, the transaction retries
   with locks held.

3. **Write transaction log.** Write the log object atomically. After this point,
   the transaction is considered committed.

4. **Async write-back.** Write the new values to each modified key and release
   locks. This can happen asynchronously because the transaction log is the
   source of truth. If the client crashes, another transaction can read the log
   and complete the write-back (or just observe the committed values from the
   log).

### Optimizations

#### Read-only transactions

If a transaction only reads, it can skip locking entirely on the happy path:

1. Read all keys, tracking their versions.
2. After the last read, verify that all versions are still current and no keys
   are write-locked.
3. If verification passes: return immediately. No locks acquired, no log
   written.
4. If verification fails (concurrent write detected): retry once with the full
   locking protocol as a fallback.

This makes read-heavy workloads very efficient — the happy path requires only
one value read plus one metadata read per key, with zero writes.

#### Single read-modify-write

Transactions that read and write exactly one key use a native CAS shortcut:

1. Read the key's contents and metadata.
2. If the key is locked, fall back to the full protocol.
3. Otherwise, do a conditional write (`write_if`) with the version as a
   precondition.

This avoids the lock-verify-log-writeback cycle entirely for the common case
of updating a single key. (The change-detection fix that keeps this path
lost-update-safe is documented in
[ADR-007](adr/007-single-rw-cache-lost-update.md).)

If the conditional write's outcome is in doubt (`Unavailable` — it may or may
not have landed), the fast path re-issues the *same* write unchanged. The write
is idempotent under its own precondition, so the retry lands only if the object
is still untouched (recovering an in-doubt write that never landed) and is
rejected by the precondition otherwise. Only an irreducible in-doubt — a
precondition seen after a re-issue, where our earlier attempt may have committed
— surfaces to the caller as `Error::InDoubt`. See
[ADR-009](adr/009-in-doubt-conditional-writes.md).

#### Retry with locks held

When a transaction fails validation and must retry, it does so with its locks
still held. This means the second attempt runs under pessimistic locking and is
guaranteed to succeed — no further conflicts are possible. This bounds the
maximum number of retries to one per conflict.

### Deadlock Handling

GlassDB prevents deadlocks proactively with the **wound-wait** rule. Each
transaction has a priority derived from its ID (an earlier timestamp means an
older, higher-priority transaction). When a transaction requests a lock that
conflicts with current holders:

- If the requester is **older** than a holder, it **wounds** it: the holder's
  log is durably aborted and the requester takes the lock.
- If the requester is **younger**, it **waits** for the holder to finish.

Since an older transaction never waits for a younger one, the wait-for graph
stays acyclic and no cycle can form. A wounded transaction observes
`TransError::Wounded` and is restarted by the database retry loop with a renewed
ID (`TxId::renew`) that preserves its original priority, so it is not starved.

**Serial locking is kept as a safety net.** Parallel validation still arms a
5-second timeout (`MAX_DEADLOCK_TIMEOUT`); if it fires — meaning sustained
contention, or two equal-priority transactions that wound-wait does not order —
the transaction falls back to **serial validation**, acquiring locks one at a
time in sorted path order. Total ordering cannot deadlock, guaranteeing
progress.

Priority depends only on the ID's timestamp, never on its random prefix, because
`TxId::renew` keeps the timestamp but changes the prefix on each restart;
ordering on the prefix would let equal-timestamp transactions flip order every
restart and livelock. See [ADR-002](adr/002-wound-wait-locking.md).

### Crash Recovery

If a client crashes mid-transaction (or its transaction future is dropped),
other clients can recover. The lifecycle monitor (`glassdb-trans/src/monitor.rs`)
drives this:

1. **Lock TTLs.** While holding locks, a transaction periodically refreshes its
   transaction log with a new timestamp (`PENDING_TX_TIMEOUT` = 15 s, refreshed
   at half that interval). If the timestamp becomes stale (the transaction
   hasn't refreshed within the timeout, allowing for `MAX_CLOCK_SKEW`),
   competing transactions consider the lock expired.

2. **Transaction log as arbiter.** To take over an expired lock, a competing
   transaction attempts to **conditionally write** to the expired transaction's
   log, marking it as aborted. If this CAS succeeds, the old transaction is
   officially aborted and its locks are invalid. If the CAS fails (the
   transaction refreshed or committed in the meantime), the competitor waits
   longer.

3. **Safe even with races.** If a crashed transaction's commit write races with
   a competitor's abort write, the CAS semantics ensure exactly one succeeds.
   The loser observes the version mismatch and backs off.

## Storage & Caching

GlassDB uses a three-layer caching architecture to minimize backend calls:

```
┌───────────────────────────────────────┐
│           Transaction Code            │
└─────────────────┬─────────────────────┘
                  │ tx.read / tx.write
                  ▼
┌───────────────────────────────────────┐
│         Local Cache (per-DB)          │
│  Staleness tracking, outdated flags   │
│  Separate caches for values & metadata│
└─────────────────┬─────────────────────┘
                  │ cache miss or stale
                  ▼
┌───────────────────────────────────────┐
│     Global Cache (read-through)       │
│  Uses read_if_modified to avoid full  │
│  downloads if the writer is unchanged │
└─────────────────┬─────────────────────┘
                  │ writer changed or absent
                  ▼
┌───────────────────────────────────────┐
│         Backend (Object Storage)      │
└───────────────────────────────────────┘
```

**LRU Cache** (`glassdb-storage/src/cache.rs`). A thread-safe, byte-weighted LRU
cache (default 512 MiB, configurable via `DatabaseBuilder::cache_size`). Entries
are evicted least-recently-used first when the total size exceeds the limit.

**Local Cache** (`glassdb-storage/src/local.rs`). Wraps the LRU cache with
staleness awareness. Each entry tracks when it was last updated and whether it
has been marked outdated (e.g., because a concurrent transaction invalidated
it). Separate entries are maintained for values and metadata. Relative staleness
uses `tokio::time::Instant` so it stays deterministic under paused time (see
[PORTING.md](../PORTING.md), "Time and determinism").

**Global Cache** (`glassdb-storage/src/global.rs`). A read-through and
write-through layer over the backend. On reads, it uses `read_if_modified` to
avoid re-downloading objects whose `last-writer` hasn't changed. On writes, it
updates the local cache with the new value and version immediately.

After a transaction commits, its written values are cached locally. Subsequent
transactions on the same client can read them without hitting the backend,
unless another client modifies the same keys.

## Data Model

### Path Encoding

Storage paths follow a hierarchical scheme with type markers
(`glassdb-data/src/paths.rs`):

```
<db-prefix>/<type>/<base64-encoded-name>
```

| Type Marker | Meaning                     | Example                    |
| ----------- | --------------------------- | -------------------------- |
| `_k`        | Key (user data)             | `mydb/coll/_k/dXNlcl9rZXk` |
| `_c`        | Collection (sub-collection) | `mydb/root/_c/c2V0dGluZ3M` |
| `_t`        | Transaction log             | `mydb/root/_t/dHhfMTIz`    |
| `_i`        | Collection info (metadata)  | `mydb/root/_i`             |

Key names and collection names are base64-encoded in the path to avoid
conflicts with the type markers and to support arbitrary byte sequences. The
encoding uses a custom **order-preserving** base64 alphabet
(`glassdb-data/src/base64.rs`), and `path.Clean`/`path.Join` are reimplemented
faithfully in `glassdb-data/src/gopath.rs`, so storage keys are byte-identical
to the Go implementation (anchored by golden vectors).

### Collections

A `Collection` is a scoped namespace for keys, similar to a table or a prefix.
Each collection has a pseudo-key (its collection info object, `_i`) that
represents the "list of keys" in the collection. This pseudo-key is locked when
keys are created or deleted, ensuring consistency for key enumeration:

- **Create a key**: lock the collection info in write + lock the new key in
  create.
- **Delete a key**: lock the collection info in write + lock the key in write.
- **Iterate keys**: lock the collection info in read.
- **Read/write an existing key**: no collection lock needed.

The `Collection` helpers (`read`, `read_stale`, `write`, `delete`, `update`,
`create`) each run a one-shot transaction via the same retry loop as
`Database::tx`.

### Versioning

The `Version` type in `glassdb-storage/src/version.rs` combines two sources of
truth:

- **Backend version** (`backend::Version`): the opaque CAS token assigned by
  object storage, used for conditional operations.
- **Local writer** (`data::TxId`): the transaction that last wrote the value,
  taken from the `last-writer` tag (or tracked locally for not-yet-committed
  writes).

During validation, the algorithm detects concurrent modifications by comparing
the last writer against what it observed when reading (`equal_meta_contents`).
The backend version is used purely as the CAS token for the conditional write
that takes the lock.

## Garbage Collection

Committed and aborted transaction logs are no longer needed once all their locks
are released and values written back. The `Gc` component
(`glassdb-trans/src/gc.rs`) handles cleanup:

- **Scheduled cleanup.** After a transaction completes, its log is queued for
  deletion with a 1-minute delay (`CLEANUP_INTERVAL`). The delay ensures
  in-flight readers can still inspect the log.
- **Bounded queue.** The cleanup queue holds at most 1024 items
  (`SIZE_LIMIT`). If the queue is full, further items are dropped (they'll be
  cleaned up eventually by future transactions or restarts).
- **Background execution.** Cleanup runs asynchronously on the `Background` task
  manager and does not block transaction processing. Background loops are torn
  down via `Drop` when the last `Database` clone is dropped.
