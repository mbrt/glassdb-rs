# Architecture

This document describes the architecture and design choices of GlassDB. For the
full design narrative and motivation, see the companion blog post:
[Transactional Object
Storage](https://blog.mbrt.dev/posts/transactional-object-storage) (written for
the original Go version, but the design is identical). For the Rust-specific
porting decisions — the concurrency model, time/determinism, error handling, and
encoding fidelity — see [porting-go.md](porting-go.md). For usage, performance
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
| `glassdb-storage`     | `object_cache.rs`, `value_cache.rs`, `shardstore.rs`, `lock.rs`, `tlogger.rs`, `version.rs`, `cache.rs` | Object cache (read/write-through, version-keyed), value cache (writer-keyed, staleness), shard/root CAS store, lock-state value type, transaction-log persistence, version tracking, generic LRU |
| `glassdb-data`        | `txid.rs`, `paths.rs`, `base64.rs`, `gopath.rs`                              | Core types: `TxId`, `TxIdSet`, order-preserving path encoding                                                                                 |
| `glassdb-proto`       | —                                                                            | `prost`-generated transaction-log protobuf messages                                                                                           |
| `glassdb-concurr`     | `background.rs`, `retry.rs`, `dedup.rs`, `clock.rs`                          | Concurrency utilities: `Background` tasks, retry/backoff, request deduplication, the `Clock` abstraction                                      |

Only the top-level `glassdb` crate is intended for direct use; the rest are
implementation detail. Its public API surface is small: `Database`,
`Transaction`, and `Collection`, plus the re-exported `Backend` trait and the
in-memory backend and middleware. The deterministic-simulation runtime (the
`rt`/`exec` seam in `glassdb-concurr`) is compiled only under `--cfg sim`; see
[dst-approach.md](dst-approach.md).

## Component Responsibilities

Inside the transaction engine (`glassdb-trans`) the division of labour follows a
deliberate **policy vs. mechanism** split, with one structural invariant: the
**shard concept never leaks above the locker**. `Algo` decides *what* must happen
to commit a transaction — purely in terms of logical keys (paths), the version
tokens observed at read time, and staged writes — while the `Locker` decides
*how* to acquire those locks efficiently, owning the mapping from keys to shard
objects and the parallel/serial CAS. (`Reader` is likewise shard-aware
internally but exposes a path-based API.)

> The v2 shard / content-CAS model shown here supersedes the tag-based
> description in the older *Distributed Locks* and *Single read-modify-write*
> sections further below, which are pending a refresh.

```
                         glassdb  (public API)
        Database · Transaction · Collection · tx_impl retry loop
               runs the user body, collects accesses by path
                                │  Data = reads(path, token) + writes(path, op)
                                ▼
═══════════════════════════ glassdb-trans ═══════════════════════════

  Algo — commit POLICY  (shard-agnostic)
    · lifecycle:        begin / rebegin / end
    · orchestrates:     lock → validate reads → commit point → write-back
    · conflict policy:  wound · deadlock-timeout · serial · backoff
    · read validation:  effective-writer token vs. observed (post-lock)
    · speaks:           Data · TxId · LockOutcome{Locked|Conflict}

      │ validate           │ lock(Data, serial)  │ status        │ schedule
      │                    │  ▲ LockedTx (opaque) │               │
      ▼                    ▼  │                    ▼               ▼
 ┌─────────┐   ┌───────────────────────┐   ┌──────────┐   ┌─────────┐
 │ Reader  │   │ Locker — MECHANISM    │   │ Monitor  │   │   Gc    │
 │ effctv. │   │ owns the SHARD model: │   │ tx-log   │   │ tx-log  │
 │ writer/ │   │ · path → shard groups │   │ lifecycle│   │ cleanup │
 │ snapshot│   │ · parallel/serial CAS │   │ wound /  │   │         │
 │ reads + │   │ · wound-wait holders  │   │ wait /   │   │         │
 │ validate│   │ · write-back, release │   │ refresh  │   │         │
 └────┬────┘   └───────────┬───────────┘   └────┬─────┘   └────┬────┘
      │                    │                    │              │
      ▼                    ▼                    ▼              ▼
══════════════════════════ glassdb-storage ══════════════════════════
  ShardStore (_s shards · _i roots) · TLogger (_t logs)
  ObjectCache (read/write-through) · ValueCache (staleness LRU)
                                │
                                ▼
            glassdb-backend  (content-CAS object store: GCS / S3)
```

`Algo` is shard-agnostic by construction: in non-test code it never imports
`ShardStore`, calls `shard_index`, or sees a `ShardEntry`. It hands the `Locker`
a `Data` value plus a `serial: bool`, and receives a logical `LockOutcome` plus
an opaque `LockedTx` it only passes back to `write_back`. Everything
shard-shaped — `{prefix}/_s/<i>` objects, `ShardEntry`, `CollectionRoot`, the
per-shard read-modify-write CAS — lives below the locker boundary.

| Component             | Layer            | Speaks                       | Owns                                                                                                                  | Must not know                       |
| --------------------- | ---------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `glassdb` (`tx_impl`) | API / retry      | closures, `Error`            | user body, retry loop, cancel-safety                                                                                  | locks, shards, tx logs              |
| `Algo`                | commit **policy** | `Data`, `TxId`, `LockOutcome` | lifecycle, lock→validate→commit→write-back orchestration, **read-version validation** (post-lock), conflict policy (wound, deadlock-timeout, parallel↔serial, backoff, same-id retry) | **shards**, CAS details, caching    |
| `Locker`              | lock **mechanism** | `Data`, `TxId`, shard objects | key→shard grouping, parallel & serial acquisition, wound-wait, write-back, release             | retry *policy*, **read validation** (reports `Conflict` only) |
| `Reader`              | read mechanism   | paths                        | effective-writer resolution, snapshot reads                                                                           | commit / lock policy                |
| `Monitor`             | tx lifecycle     | `TxId`, tx logs              | status, wound/abort, lease refresh, waits                                                                             | shards                              |
| `Gc`                  | maintenance      | `TxId`                       | scheduled tx-log cleanup                                                                                              | shards, commit policy               |

### The lock boundary

The single call across the policy/mechanism seam carries no shard vocabulary:

```rust
// Algo → Locker: acquire every lock the access set needs.
async fn lock(&self, id: &TxId, data: &Data, serial: bool)
    -> Result<LockOutcome, TransError>;
```

- **Down**: `Data` (the transaction's reads and its staged writes), plus
  `serial` — the *only* policy signal the locker needs. The locker reads each
  access's *path* to decide which lock to install (a read lock for a read-only
  key, write/create/delete for a written one); it does **not** look at the
  version token — that is validation, which is `Algo`'s job. It groups keys by
  shard and locks them in parallel by default, or one shard at a time in sorted
  path order when `Algo` decides contention warrants the serial fallback.
- **Up**: `LockOutcome::Locked(LockedTx)` on success, or `LockOutcome::Conflict`
  when a CAS race was lost — both logical, never shards. `Algo` maps `Conflict`
  onto its policy: release and re-acquire under the same id, escalating to serial
  and backing off.

Read-version validation is **not** at this seam. Once `Locked` comes back, every
touched key is locked and its value frozen, so `Algo` re-resolves each read's
effective writer (via `Reader`, path-based) and compares it to the token the body
observed. A mismatch means the value moved before the lock landed: `Algo` re-runs
the body **holding its locks** (`Retry`). This is optimistic-concurrency policy
over the shard-agnostic read set, and it reuses the same routine as the read-only
fast path — so validation lives in exactly one place, never in the locker.

Because the deadlock timeout, serial-escalation decision, and backoff are
*policy*, they live in `Algo`; the locker is bounded only by an internal
CAS-retry budget and reports sustained contention back as `Conflict` rather than
looping forever. This keeps efficient batch acquisition — which is inherently
shard-shaped (many keys collapse into one shard CAS) — in the one component that
understands shards, without ever surfacing shards to the commit algorithm.

## Backend Abstraction

The `Backend` trait (`glassdb-backend`) defines the contract with object
storage. It is an `async_trait`, and every method is cancellable by dropping the
returned future:

```rust
#[async_trait]
pub trait Backend: Send + Sync {
    async fn read(&self, path: &str) -> Result<ReadReply, BackendError>;
    async fn read_if_modified(
        &self, path: &str, expected: &Version,
    ) -> Result<ReadReply, BackendError>;
    async fn write(&self, path: &str, value: Vec<u8>) -> Result<Version, BackendError>;
    async fn write_if(
        &self, path: &str, value: Vec<u8>, expected: &Version,
    ) -> Result<Version, BackendError>;
    async fn write_if_not_exists(
        &self, path: &str, value: Vec<u8>,
    ) -> Result<Version, BackendError>;
    async fn delete(&self, path: &str) -> Result<(), BackendError>;
    async fn list(&self, dir_path: &str) -> Result<Vec<String>, BackendError>;
}
```

This is the slimmed, content-CAS-only surface of
[ADR-023](adr/023-slimmed-backend-trait.md): seven methods, each mapping to a
primitive S3 and GCS both provide natively. All coordination state lives in
object *content* and is mutated *only* by content CAS — there are no tags,
metadata, or writer ids.

### Key concepts

**Versions.** Every object has an opaque CAS token (`Version { token: Arc<str> }`),
assigned by the backend and used only for conditional operations. The format is
backend-specific: GCS encodes the object `generation`, while S3 uses the
object's ETag. Consumers never interpret it — they pass it back unchanged to
`write_if` / `read_if_modified`.

**Change detection.** All coordination state lives in object *content* and
changes only by content CAS, so an object's version (ETag / generation) changes
on exactly every write — precisely when a cached copy must be invalidated. To
revalidate a cached object the cache issues a *version-conditional* read:
`read_if_modified` takes the expected `Version` and returns `Precondition` when
the stored version still matches (the body is not re-transferred), or the full
object when it changed. This maps to a native conditional GET on every backend
(`If-None-Match` on S3, `ifGenerationNotMatch` on GCS) and lets a hot, unchanged
shard revalidate without a body transfer
([ADR-023](adr/023-slimmed-backend-trait.md)).

**Conditional operations.** `write_if` and `write_if_not_exists` take an
expected version (or "must not exist") and fail with
`BackendError::Precondition` if the object has been modified since. This content
compare-and-swap (CAS) is the only coordination primitive — the fundamental
building block for distributed coordination.

**Error semantics** (`BackendError`):

- `NotFound` — object does not exist.
- `Precondition` — conditional operation failed (version mismatch).
- `Unavailable(_)` — the operation could not be confirmed. For a *conditional
  write* this means the outcome is _in doubt_: it may or may not have been
  applied (e.g. an acknowledgement was lost and the retry then saw a precondition
  failure, or an outage exhausted the retry budget), so a non-idempotent
  operation must not be blindly retried
  ([ADR-009](adr/009-in-doubt-conditional-writes.md)). For an *idempotent*
  request (read, `read_if_modified`, unconditional write/delete, list) it is just
  a transient failure (`5xx`, timeout, transport error) that is always safe to
  retry; the engine retries reads in place and surfaces an unrecoverable one as
  `Error::Unavailable` ([ADR-015](adr/015-read-unavailability.md)).
- `Other(_)` — any other backend error.

`is_not_found`, `is_precondition`, and `is_unavailable` predicates preserve
the original sentinel-error matching semantics.

### Implementations

| Backend                       | Purpose             | Notes                                                                                                                                              |
| ----------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `glassdb-backend-gcs`         | Production          | GCS JSON API over `reqwest`; encodes the object `generation` in the version token; `read_if_modified` via `ifGenerationNotMatch`                    |
| `glassdb-backend-s3`          | Production          | One object per path (`aws-sdk-s3`); stores the value verbatim as the body, ETag as the version token; `read_if_modified` via `If-None-Match`        |
| `glassdb-backend::memory`     | Testing             | In-process `MemoryBackend` simulating GCS semantics                                                                                                |
| `glassdb-backend::middleware` | Debugging / testing | Wrappers for logging, latency injection, byte-driven scheduling, fault injection, and op-stream recording                                          |

The cloud backends are feature-gated (`s3`, `gcs`) so their heavy SDK
dependencies are only pulled in when needed; each is tested against a pure-Rust
in-process fake of its API.

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
recovers any in-flight state (see [porting-go.md](porting-go.md), "Cancel-safety
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

A read is idempotent, so a transient backend outage (`Unavailable`) during a
read is retried in place with backoff by the reader — recovering a blip
transparently without re-running the user closure. A sustained outage surfaces as
`Error::Unavailable` (distinct from the in-doubt `Error::InDoubt`, which only a
mutation can produce), which the caller may safely retry. See
[ADR-015](adr/015-read-unavailability.md).

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
│           ValueCache (per-DB)         │
│  Staleness tracking, outdated flags   │
│  Caches values keyed by their writer  │
└─────────────────┬─────────────────────┘
                  │ cache miss or stale
                  ▼
┌───────────────────────────────────────┐
│      ObjectCache (read-through)       │
│  Uses read_if_modified to avoid full  │
│  downloads if the version is unchanged│
└─────────────────┬─────────────────────┘
                  │ version changed or absent
                  ▼
┌───────────────────────────────────────┐
│         Backend (Object Storage)      │
└───────────────────────────────────────┘
```

Two facades share **one** byte-weighted LRU (a single `cache_size` budget),
keyed by two disjoint identities (ADR-023): user values by their **writer**, and
coordination objects by their **backend version**. Both are built from a
`SharedCache` handle rather than one depending on the other.

**LRU Cache** (`glassdb-storage/src/cache.rs`). A thread-safe, byte-weighted LRU
cache (default 512 MiB, configurable via `DatabaseBuilder::cache_size`). Entries
are evicted least-recently-used first when the total size exceeds the limit. A
`SharedCache` wraps one instance and hands it to both facades below.

**ValueCache** (`glassdb-storage/src/value_cache.rs`). The writer-keyed facade
for user values, with staleness awareness. A value lives in the transaction
object of whichever transaction last committed it, so it is identified by that
**writer**, not a backend object version. Each entry tracks when it was last
updated and whether it has been marked outdated (e.g., because a concurrent
transaction invalidated it). Relative staleness uses `tokio::time::Instant` so it
stays deterministic under paused time (see [porting-go.md](porting-go.md), "Time and
determinism").

**ObjectCache** (`glassdb-storage/src/object_cache.rs`). The backend-version-keyed,
read-through / write-through facade for coordination objects (shards, roots,
transaction logs). On reads it uses the version-conditional `read_if_modified` to
avoid re-downloading objects whose backend version hasn't changed; on writes it
updates the cache with the new bytes and version immediately. `ShardStore` and
`TLogger` read and compare-and-swap through it, so a hot unchanged shard/root/log
revalidates without re-transferring its body.

After a transaction commits, its written values are cached in the `ValueCache`.
Subsequent transactions on the same client can read them without hitting the
backend, unless another client modifies the same keys.

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

Two version identities are kept separate (ADR-023):

- **Writer** — the storage-layer `Version` in `glassdb-storage/src/version.rs` is
  writer-only (`data::TxId`): the transaction that last committed the value. A
  value lives in that transaction object's body (ADR-019), so the writer *is* the
  value's identity. This is what the `ValueCache` keys on.
- **Backend version** (`backend::Version`): the opaque CAS token assigned by
  object storage, used for conditional writes and for cache revalidation via the
  version-conditional `read_if_modified`. It identifies a coordination object's
  content, so it is tracked in the `ObjectCache` entries (not in the storage
  `Version`).

During validation, the algorithm detects concurrent modifications by comparing
the observed writer against the current state; the backend version is the CAS
token for the conditional write that takes the lock.

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
