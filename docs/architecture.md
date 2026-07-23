# Architecture

This document describes the architecture and design choices of GlassDB. For the
full design narrative and motivation, see the companion blog post:
[Transactional Object
Storage](https://blog.mbrt.dev/posts/transactional-object-storage) (written for
the original Go version, but the design is identical). For the Rust-specific
porting decisions — the concurrency model, time/determinism, error handling, and
encoding fidelity — see [porting-go.md](archive/porting-go.md). For usage,
performance benchmarks, and examples, see the [README](../README.md).

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
  conditional mutations (available in GCS and S3).

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
single-object operations and conditional mutations for atomic state transitions.

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
| `glassdb-trans`       | `algo.rs`, `tlocker.rs`, `shard_coord.rs`, `resolver.rs`, `monitor.rs`, `reader.rs`, `gc.rs` | Transaction engine: commit algorithm, distributed locker, shard-mutation coordinator, holder/effective-writer resolver, lifecycle monitor, read path, log GC |
| `glassdb-storage`     | `cached_store.rs`, `shardstore.rs`, `shard.rs`, `root.rs`, `txobject.rs`, `lock.rs`, `tlogger.rs`, `version.rs`, `cache.rs` | Shared decoded object store with bounded-freshness evidence, shard/root CAS store, shard & collection-root codecs, unified transaction-object codec, lock-state value type, transaction-log persistence, version tracking, generic LRU |
| `glassdb-data`        | `txid.rs`, `paths.rs`, `base64.rs`                                           | Core types: `TxId`, `TxIdSet`, order-preserving path encoding                                                                                 |
| `glassdb-proto`       | —                                                                            | `prost`-generated transaction-log protobuf messages                                                                                           |
| `glassdb-concurr`     | `background.rs`, `retry.rs`, `dedup.rs`, `clock.rs`                          | Concurrency utilities: `Background` tasks, retry/backoff, request deduplication, the `Clock` abstraction                                      |

Only the top-level `glassdb` crate is intended for direct use; the rest are
implementation detail. Its public API surface is small: `Database`,
`Transaction`, and `Collection`, plus the re-exported `Backend` trait and the
in-memory backend and middleware. The deterministic-simulation runtime (the
`rt`/`exec` seam in `glassdb-concurr`) is compiled only under `--cfg sim`; see
[testing-dst.md](guides/testing-dst.md).

## Component Responsibilities

Inside the transaction engine (`glassdb-trans`) the division of labour follows a
deliberate **policy vs. mechanism** split, with one structural invariant: the
**shard concept never leaks above the locker**. `Algo` decides *what* must happen
to commit a transaction — purely in terms of logical keys (paths), the version
tokens observed at read time, and staged writes — while the `Locker` decides
*how* to acquire those locks efficiently, owning the mapping from keys to shard
objects and the parallel/serial CAS. (`Reader` is likewise shard-aware
internally but exposes a path-based API.)

Every shard/root entry mutation — lock acquire, single read-write commit-install,
write-back, release, and GC reclamation — flows through **one shard-mutation
coordinator** that loads the object once, folds the round's operations in
wound-wait order, and CASes once (ADR-028/029). `Algo` and the `Locker` supply
 the
  policy as *resolvers*; the coordinator is the shared mechanism and knows
  nothing of locks or transaction ids. For the full design see
  [designs/object-storage-native.md](designs/object-storage-native.md).

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
 │ Reader  │   │ Locker — lock POLICY  │   │ Monitor  │   │   Gc    │
 │ effctv. │   │ owns the SHARD model: │   │ tx-log   │   │ tx-log  │
 │ writer/ │   │ · path → shard groups │   │ lifecycle│   │ reverse │
 │ validate│   │ · parallel/serial     │   │ wound /  │   │ liveness│
 │ reads + │   │ · hold-and-wait loop  │   │ wait /   │   │ release │
 │ validate│   │ · installs resolvers  │   │ refresh  │   │ →Locker │
 └────┬────┘   └───────────┬───────────┘   └────┬─────┘   └────┬────┘
      │                    │ acquire / write-back / release     │
      │                    │ + Algo CommitInstall + Gc release  │
      │                    ▼                                    │
      │       ┌───────────────────────────────┐                │
      │       │ ShardCoordinator — MECHANISM  │                │
      │       │ one round/object: load once · │                │
      │       │ fold (wound-wait order) · CAS │                │
      │       │ once · reload-recover in-doubt│                │
      │       └───────────────┬───────────────┘                │
      ▼                       ▼            ▼ (tx logs)          ▼
══════════════════════════ glassdb-storage ══════════════════════════
  ShardStore (_s shards · _i roots) · TLogger (_t logs)
  CachedStore (decoded, path-keyed, bounded-freshness LRU)
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

Beneath the locker boundary, one further split separates **policy from
mechanism**: every shard/root entry mutation flows through a single
`ShardCoordinator`, which owns the *mechanism* (one single-flight round per
object: load once, fold the round's operations, CAS once, recover
precondition/in-doubt by reload) while the callers supply *policy* as installed
resolvers — `Locker` installs acquire / write-back / release, `Algo` installs the
single read-write `CommitInstall`, and `Gc` reclaims through the `Locker`'s unlock
methods (ADR-028/029). The coordinator is ignorant of locks, transaction ids, and
wound-wait; the fold visits members oldest-first so it never has to backtrack.

| Component             | Layer            | Speaks                       | Owns                                                                                                                  | Must not know                       |
| --------------------- | ---------------- | ---------------------------- | --------------------------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `glassdb` (`tx_impl`) | API / retry      | closures, `Error`            | user body, retry loop, cancel-safety                                                                                  | locks, shards, tx logs              |
| `Algo`                | commit **policy** | `Data`, `TxId`, `LockOutcome` | lifecycle, lock→validate→commit→write-back orchestration, **read-version validation** (post-lock), conflict policy (wound, deadlock-timeout, parallel↔serial, backoff, same-id retry), single read-write `CommitInstall` | **shards**, CAS details, caching    |
| `Locker`              | lock **policy** | `Data`, `TxId`, shard objects | key→shard grouping, parallel & serial acquisition strategy, hold-and-wait, installs acquire / write-back / release resolvers | retry *policy*, **read validation** (reports `Conflict` only) |
| `ShardCoordinator`    | shard/root **mechanism** | object path, resolvers | one round per object: single-flight, load-once, monotonic fold, single CAS, reload-recover, vestigial-entry pruning | locks, `TxId`, wound-wait, commit |
| `Reader`              | read mechanism   | paths                        | effective-writer resolution                                                                                            | commit / lock policy                |
| `Monitor`             | tx lifecycle     | `TxId`, tx logs              | status, wound/abort, lease refresh, waits                                                                             | shards                              |
| `Gc`                  | maintenance      | `TxId`, shard objects        | mark-sweep GC: reverse liveness check, force-abort dead tx, paged shuffled `_t/<ss>/` walks, reclaims via the `Locker`'s coordinator-backed unlock | commit policy                       |

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
    async fn write_if(
        &self, path: &str, value: Vec<u8>, expected: &Version,
    ) -> Result<Version, BackendError>;
    async fn write_if_not_exists(
        &self, path: &str, value: Vec<u8>,
    ) -> Result<Version, BackendError>;
    async fn delete_if(
        &self, path: &str, expected: &Version,
    ) -> Result<(), BackendError>;
    async fn list(
        &self,
        prefix: &str,
        cursor: Option<&ListCursor>,
        limit: ListLimit,
    ) -> Result<ListPage, BackendError>;
}
```

This is the six-method, conditional-only surface established by
[ADR-042](adr/042-conditional-only-backend-mutations.md), refining the slimmed
backend of [ADR-023](adr/023-slimmed-backend-trait.md). Each method maps to a
primitive S3 and GCS provide natively. All coordination state lives in object
*content*, and every mutation names either absence or an exact content revision
— there are no tags, metadata, writer ids, or unconditional mutations.

Correctness assumes that each backend provides linearizable single-object reads
and conditional mutations, including read-after-definitive-completion. An
eventually consistent backend is therefore not supported. A definitive response
establishes an ordering edge; an `Unavailable` result does not. Provider retries
remain inside one logical backend invocation so that attempts do not manufacture
ordering edges between themselves.

`list` returns one recursive prefix page of actual object paths. Its cursor is
an opaque provider token valid only for the same backend and prefix, and only a
page without a next cursor completes the traversal. A rejected token returns
`InvalidCursor`, allowing the caller to restart that prefix. S3 and GCS map this
contract directly to their native continuation tokens without a delimiter
([ADR-035](adr/035-paginated-listing-and-sharded-transaction-logs.md)).

### Key concepts

**Versions.** Every object has an opaque CAS token (`Version { token: Arc<str> }`),
assigned by the backend and used only for conditional operations. The format is
backend-specific: GCS encodes the object `generation`, while S3 uses the
object's ETag. Consumers never interpret it — they pass it back unchanged to
`write_if`, `delete_if`, or `read_if_modified`.

**Change detection.** All coordination state lives in object *content* and
changes only by content CAS. The version (ETag / generation) identifies that
content state; rewriting equivalent content may retain the same token. To check
whether a cached object is current, the cache issues a
*version-conditional* read:
`read_if_modified` takes the expected `Version` and returns `Precondition` when
the stored version still matches (the body is not re-transferred), or the full
object when it changed. This maps to a native conditional GET on every backend
(`If-None-Match` on S3, `ifGenerationNotMatch` on GCS) and lets a hot, unchanged
shard check its currentness without a body transfer
([ADR-023](adr/023-slimmed-backend-trait.md)).

**Conditional operations.** `write_if`, `write_if_not_exists`, and `delete_if`
name an expected version (or "must not exist") and fail with
`BackendError::Precondition` if that state is no longer current. A missing
object during `delete_if` is successful convergence whether represented as
success or `NotFound`. Content compare-and-swap (CAS) is the only coordination
primitive — the fundamental building block for distributed coordination.

**Error semantics** (`BackendError`):

- `NotFound` — object does not exist.
- `Precondition` — conditional operation failed (version mismatch).
- `Unavailable(_)` — the operation could not be confirmed. For a *mutation*
  this means the outcome is _in doubt_: it may or may not have been applied
  (e.g. an acknowledgement was lost or an outage exhausted the retry budget),
  so it must not be blindly retried
  ([ADR-009](adr/009-in-doubt-conditional-writes.md)). For an idempotent read or
  list it is a transient failure (`5xx`, timeout, transport error) that is safe
  to retry; the engine retries reads in place and surfaces an unrecoverable one
  as `Error::Unavailable` ([ADR-015](adr/015-read-unavailability.md)).
- `Other(_)` — any other backend error.

`is_not_found`, `is_precondition`, and `is_unavailable` predicates preserve
the original sentinel-error matching semantics.

### Implementations

| Backend                       | Purpose             | Notes                                                                                                                                              |
| ----------------------------- | ------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| `glassdb-backend-gcs`         | Production          | GCS JSON API over `reqwest`; generation versions; conditional read/write/delete through native generation preconditions                            |
| `glassdb-backend-s3`          | Production          | One object per path (`aws-sdk-s3`); ETag versions; conditional read/write/delete through native HTTP preconditions                                 |
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
the framework owns the retry loop, a conflict simply reruns the closure.
Dropping the transaction future at any point is equivalent to a crash: the
commit protocol recovers any in-flight state (see
[porting-go.md](archive/porting-go.md), "Cancel-safety contract").

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

Lock state lives in the **content** of the shard objects (`_s/<i>`), not in object
tags. Each shard holds a directory of per-key entries; a locked key's entry
records its lock type, the set of holding transactions, and the current writer
(`glassdb-storage/src/shard.rs`, `lock.rs`):

| Field            | Values             | Purpose                                        |
| ---------------- | ------------------ | ---------------------------------------------- |
| `lock-type`      | `r`, `w`, `c`, `-` | Current lock type (read, write, create, none)  |
| `locked-by`      | tx IDs             | Which transactions hold the lock               |
| `current-writer` | tx ID              | Transaction that last wrote this key           |

Lock acquisition is a compare-and-swap on the shard *object*: read the current
shard and its version, compute the new lock state for every requested key that
maps to it, and conditionally rewrite the shard with `write_if` (the
version/ETag as the precondition). If the version changed (another transaction
mutated the shard), the operation retries. Keys are grouped by shard so many
keys collapse into a single GET + CAS (ADR-017/020), and contending transactions
on the same shard batch through the shard-mutation coordinator into one
owner-driven CAS (ADR-025/026/028) rather than racing separate ones.

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
<db-prefix>/_t/<first-two-encoded-symbols>/<base64-encoded-tx-id>
```

The transaction ID (`glassdb-data::TxId`) is `[8 bytes random prefix][8 bytes
big-endian UnixNano timestamp]`. The timestamp suffix encodes the wound-wait
priority (earlier = older), while the random prefix leads so that log keys keep
a high-entropy prefix and spread across object-store partitions instead of
clustering sequential commits into one hot partition. The first two encoded
symbols select one of 4,096 independently listable transaction-log shards
([ADR-035](adr/035-paginated-listing-and-sharded-transaction-logs.md)).

The log is serialized as a Protocol Buffer (`glassdb-proto`, `prost`-generated
from a copy of `transaction.proto`) and contains:

- **Status**: pending, committed, or aborted.
- **Timestamp**: when the log was last updated.
- **Writes**: list of (path, value, deleted, previous writer) entries (the
  `oneof val_delete` layout is preserved byte-for-byte). Committed values live
  here; lock state lives in the shard objects, not in the log.

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

A transaction that overwrites exactly one existing key commits with two
**parallel** writes instead of the full sequence (ADR-027):

1. Load the shard and resolve the key's holders. A committed-but-not-written-back
   holder is help-forwarded to its effective writer; a *live pending* holder, a
   create/delete, a missing key, or a read whose version has moved makes the
   transaction ineligible — nothing has been written yet, so it falls back to the
   full locked path under the same id.
2. Issue in parallel: the committed transaction object (`_t/<ss>/<txid>`,
   recording its held lock so GC can prune it) **and** one shard CAS that
   installs a write lock and help-forwards the resolved predecessor into
   `current_writer`.
3. Asynchronously, write-back converts the lock into `current_writer = txid` and
   releases it (through the same deduplicated coordinator path).

The transaction is committed iff both writes land (the committed object exists
and the lock is in the shard's chain). Because it holds a lock during the short
pre-commit window it is a full wound-wait participant — an older concurrent
writer may wound it, and it renews (priority preserved) and re-runs. The install
CAS routes through the shard-mutation coordinator as a `CommitInstall` resolver
(ADR-028), and the shard it already cached during the read is reused for the
first fold attempt with `Requirement::Any` (ADR-036), so a
steady-state single read-write commits with its shard loaded only once. (The
change-detection reasoning that keeps this path lost-update-safe is in
[ADR-007](adr/007-single-rw-cache-lost-update.md).)

One irreducible in-doubt remains: if the install CAS returns `Unavailable` and
the shard has moved past the transaction by the time it reads back, whether the
lock landed (committed, help-forwarded into the chain) or never landed (an
orphan the transaction renews away from) is unknowable, so it surfaces as
`Error::InDoubt` rather than risk a double-apply. See
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

## Storage, Caching & Consistency

The decoded object cache is also the coordination boundary for point
operations, not just a performance optimization. Its design combines the
unified typed cache from
[ADR-036](adr/036-decoded-object-cache-with-bounded-freshness.md) with the causal
ordering protocol from
[ADR-043](adr/043-causally-coordinated-backend-operations.md):

```
┌───────────────────────────────────────┐
│           Transaction Code            │
└─────────────────┬─────────────────────┘
                  │ tx.read / tx.write
                  ▼
┌───────────────────────────────────────┐
│   Reader / Resolver / Monitor         │
│ Derive values from decoded leaves and │
│       transaction objects             │
└─────────────────┬─────────────────────┘
                  │ Any read / AtLeast currentness
                  ▼
┌───────────────────────────────────────┐
│       CachedStore (per database)      │
│ Decoded L1, retained observations,    │
│ evidence, and per-path coordination   │
└─────────────────┬─────────────────────┘
                  │ miss or insufficient evidence
                  ▼
┌───────────────────────────────────────┐
│ Optional persistent encoded-body L2   │
│ Fixed-capacity, unverified candidates │
└─────────────────┬─────────────────────┘
                  │ miss or validation
                  ▼
┌───────────────────────────────────────┐
│         Backend (Object Storage)      │
└───────────────────────────────────────┘
```

All typed physical objects share one byte-weighted, path-keyed LRU under a
single `cache_size` budget. Codecs provide encoding, decoding, and decoded-size
accounting. A physical path has one decoded type; accessing the same path
through another codec is an internal error. Key values are not cached
separately: the reader derives a value from its leaf's effective writer and that
writer's decoded transaction object.

The LRU (`glassdb-storage/src/cache.rs`) has a 512 MiB default budget,
configurable through `DatabaseBuilder::cache_size`, and evicts least-recently
used entries first. Eviction removes discoverable cache state but does not
revoke observations already retained by readers or transactions.

`DatabaseBuilder::persistent_cache` optionally adds a fixed-capacity L2 in a
caller-selected directory. Its public configuration contains only the directory
and capacity; GlassDB derives the identity from the database name and persistent
database UUID. Production geometry uses 64 MiB segments and requires at least
131 MiB. L2 stores exact encoded present bodies and opaque revisions, while L1
continues to own decoded values and currentness evidence.

Filesystem work does not run on Tokio's blocking pool. Cache lookups and
write-behind work share one bounded cache-owned worker, so overload bypasses L2
instead of creating an unbounded blocking-task backlog. Opening and shutdown
are deadline-bounded and fail open; shutdown detaches a stuck worker after its
deadline. The deterministic executor disables L2 until filesystem behavior has
a replayable simulation model.

### Knowledge and causal evidence

`CachedStore` (`glassdb-storage/src/cached_store.rs`) stores only usable
knowledge for a path:

- `Present`, with the decoded value, opaque CAS revision, and currentness
  evidence
- `Absent`, with evidence that non-existence was established definitively

Uncertainty is represented by the absence of a cache entry. There is no stored
`Missing` variant that an ordinary lookup can accidentally reuse. An
`Observation` may retain an exact historical present or absent state and its
evidence after the shared cache entry has been evicted or invalidated. Uncertain
state is not returned as an observation.

Causal evidence is a `SequencePoint`: a strictly ordered event allocated by one
open `Database`. Sequence points are neither persisted nor shared between
independent database opens. Their numeric distance has no semantic meaning,
except that the allocator is coupled to a monotonic elapsed-time floor for the
intentionally approximate `read_stale` age policy.

Callers express the minimum acceptable evidence as a `Requirement`:

| Requirement | Cache state it accepts |
| --- | --- |
| `Any` | Any usable present or absent entry |
| `AtLeast(t)` | Present or absent state proven current at or after `t` |

An observation's optional `current_after` point is evidence that the observed
state was current at that point. A persisted L2 body starts with no point and
cannot satisfy `AtLeast`; `Any` may still use it as optimistic state. It is never
a claim about response time. A definitive backend operation contributes its
invocation point, allocated immediately before dispatch. If the same backend
state is observed again, its evidence watermark advances monotonically; a
different state replaces the old discoverable knowledge.

### Per-path operation ordering

`CachedStore` owns a clone-shared `PathCoordinator`. For causally coordinated
point operations, the implementation follows this order:

```text
check cache
-> acquire the path lane
-> check cache again
-> allocate invocation point
-> invoke backend
-> reconcile cache and observations
-> release lane
-> make the future ready
```

The second cache check prevents a waiter from issuing a backend request that an
earlier operation made unnecessary. The invocation point is allocated only
after admission to the lane, so local causal order and backend invocation order
agree. Reconciliation happens before the lane is released and before the
operation can be observed as complete.

Actual backend point calls for the same path do not overlap within one open
database. Calls for different paths remain concurrent, and code must not hold
two path lanes simultaneously. Compatible reads can share one in-flight backend
read; a read may join only when that flight's invocation point satisfies its
requirement. A stricter reader queues and rechecks the cache after the current
flight completes.

An `Any` cache hit deliberately bypasses the lane. It may return older usable
state while a same-path mutation is in flight, but never state already marked
obsolete or uncertain. Code requiring a causal cut uses `AtLeast(t)` instead.

The protocol covers typed single-object reads and conditional mutations.
Listing is not path-coordinated: each page receives its own invocation point,
and a multi-page listing is not a backend snapshot. Database metadata is the
narrow startup-only exception; it uses raw backend operations because it is
created or validated once before normal concurrent access begins.

### Reconciliation and cancellation

Definitive outcomes are published while the path lane is held:

- A successful read installs the exact observed present or absent state.
- A successful create, compare-and-swap, or delete installs the exact resulting
  observation.
- A successful precondition check advances the retained expected observation to
  the mutation's invocation point.
- A clean precondition failure invalidates only matching expected knowledge. It
  proves that state obsolete but normally does not identify its replacement. A
  definitive `NotFound` establishes absence where the operation's semantics make
  that conclusion exact.
- If a changed object's body cannot be decoded, its prior cache entry is
  invalidated; malformed new bytes must not leave stale state discoverable.
- An indeterminate mutation removes all usable knowledge for the path before
  returning `Unavailable`.

Cancellation is part of the protocol. Cancellation while waiting for a lane has
no cache effect because no backend call was invoked. After mutation dispatch, a
`MutationGuard` owns the conservative fallback: if the future is cancelled,
panics, or otherwise exits without definitive reconciliation, it invalidates
the entire path before releasing the lane. The remote mutation may still take
effect later, so local coordination does not pretend to order a subsequent call
after an abandoned remote call. Read cancellation requires no invalidation
because reads cannot change backend state; other readers retry admission if a
shared read flight disappears.

### Assumptions and invariants

The cache and coordinator rely on, and preserve, these properties:

1. Backend single-object reads and conditional mutations are linearizable, and
   a read invoked after a definitive mutation completion observes that mutation
   or a later state.
2. Conditional mutations remain semantically safe if their original predicate
   becomes true again. Revisions describe state and may exhibit ABA;
   create-if-absent is restricted to permanent idempotent paths or fresh
   identity paths whose existence alone cannot publish newer live state.
3. For one open database, no two actual backend point calls for the same
   physical path overlap, except that an abandoned mutation may still be
   executing remotely after local cancellation.
4. A same-path operation is not invoked after an earlier definitive local
   completion until that earlier outcome has been reconciled. Different paths
   have no artificial ordering dependency.
5. A discoverable cache entry always represents usable knowledge. Clean
   conflicts cannot overwrite newer knowledge, while indeterminate or abandoned
   mutations leave the path with no discoverable knowledge.
6. `current_after` never exceeds the invocation point that established it, and
   evidence for an unchanged state advances monotonically.
7. Successful mutations publish the exact installed state. Their callers can
   therefore use the returned observations without immediate verification
   reads.
8. Per-path lanes and sequence points are database-local coordination.
   Independent database opens and external writers are governed by backend
   linearizability and conditional revisions, not by a shared in-memory
   timeline.

Transaction execution may use cached state freely before commit. Transaction
validation captures one lower bound and propagates it through leaf and
transaction-object dependencies. A post-bound lock CAS can satisfy that bound
without another read. If a physical leaf changed, validation compares the
observed logical writer or membership with the newer consistent state;
post-bound evidence can therefore save I/O without being mistaken for logical
finality. A typed `TLogger` may serve cached final transaction objects
indefinitely because their immutability is a transaction-object invariant; the
generic store does not interpret finality. The monitor separately keeps a small
count-bounded status cache for finalized transactions.

## Data Model

### Path Encoding

Logical collection paths are structured as a database root plus raw collection
name segments. Logical keys pair one of those collection paths with raw key
bytes. They are not encoded as physical object paths.

Only backend objects have type markers (`glassdb-data/src/paths.rs`):

| Type Marker | Meaning                         | Example                           |
| ----------- | ------------------------------- | --------------------------------- |
| `_c`        | Physical collection namespace   | `mydb/_c/RqKoS6_iOrB`             |
| `_i`        | Collection root                  | `mydb/_c/RqKoS6_iOrB/_i`          |
| `_n`        | Standalone B-link node           | `mydb/_c/RqKoS6_iOrB/_n/<token>`  |
| `_t`        | Sharded transaction object       | `mydb/_t/<shard>/<transaction-id>`|
| `_s`        | Structural recovery record       | `mydb/_s/<record-id>`             |

Collection names are base64-encoded only when rendering a physical collection
namespace. Keys live inside leaf objects and remain raw bytes. Transaction
objects likewise store raw key bytes and root-relative collection-name segments;
the database root comes from the transaction object's location, so moving a
database does not invalidate its logs. The physical namespace encoding uses a
custom **order-preserving** base64 alphabet (`glassdb-data/src/base64.rs`).

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
  value's identity; the reader uses it to locate the decoded transaction object.
- **Backend version** (`backend::Version`): the opaque CAS token assigned by
  object storage, used for conditional mutations and cache currentness checks
  via the version-conditional `read_if_modified`. It identifies a coordination
  object's content, so the object store wraps it in an opaque `Revision`
  attached to each observation (not in the storage `Version`).

During validation, the algorithm detects concurrent modifications by comparing
the observed writer against the current state; the backend version is the CAS
token for the conditional write that takes the lock.

## Garbage Collection

A transaction object is **live** exactly while some shard or root still
references its txid (`current-writer`, `locked-by`, or the root's
`membership-locked-by`), so garbage collection is a reachability problem rather
than a timer. The `Gc` component (`glassdb-trans/src/gc.rs`) implements a
candidate-driven **reverse mark-sweep** ([ADR-022](adr/022-garbage-collection-mark-sweep.md)):

- **Reverse liveness check.** A forward mark (list every shard, union the
  referenced txids) would cost the whole database per cycle. Instead each
  candidate `_t/` object records its own back-references (its `locks ∪ writes`),
  so GC reads a batch of candidates and confirms each one dead by GET-ing only
  the handful of shards/root it names — never a database-wide scan.
- **Candidate feed.** Candidates come from the write-back hint queue (the
  `current-writer` a fresh commit just superseded, capped at `HINT_QUEUE_CAP`)
  and shuffled passes over the 4,096 `{db}/_t/<ss>/` prefixes, which make the
  candidate set complete regardless of lost hints. Each cycle stops after one
  non-empty page or a bounded number of listing requests; an invalid provider
  cursor restarts only its current shard.
- **Safety horizon.** The ADR-021 lease acts as the sweep horizon: a candidate
  within the horizon is always kept, because the non-atomic reverse check can
  race a lock a live transaction has taken but not yet published (ADR-024's lazy
  object materialization). A dead *pending* object is first force-aborted
  (`pending → aborted` CAS) so its death is durable before any lock moves.
- **Reclamation through the coordinator.** GC releases a dead transaction's locks
  not with its own CAS but by calling the `Locker`'s per-object unlock methods,
  so the release batches through the same shard-mutation coordinator as live
  traffic (ADR-029); the entry left behind is pruned as a fold property when it
  becomes vestigial (no holder, no `current-writer`). It retains the candidate
  log observation and conditionally deletes only that exact revision.
- **Background execution.** Sweeps run every `GC_INTERVAL` on the `Background`
  task manager and do not block transaction processing. Background loops are torn
  down via `Drop` when the last `Database` clone is dropped.
