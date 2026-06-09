# Porting GlassDB from Go to Rust

This document records the design decisions made while porting GlassDB from Go to
Rust, and why. It complements the [README](README.md), which covers usage and
layout. The original implementation was written in Go; this repository is the
standalone Rust port.

## Goals and scope

- **Behavioral fidelity over idiom.** Where a Go choice encoded externally
  observable behavior (on-disk encodings, the commit protocol, sentinel-error
  control flow), the Rust port reproduces it exactly. Where Go choices are
  incidental (mutex granularity, goroutine plumbing), the port uses idiomatic
  Rust.
- **In-memory, S3, and GCS backends.** All three backends are ported, each
  tested against a pure-Rust in-process fake of its API (see
  [Cloud backends](#cloud-backends-and-in-process-fakes)).
- **Async on tokio.** The whole stack is `async`, built on tokio.

## Workspace structure

The port is a Cargo workspace of nine internal crates (`publish = false`) that
mirror Go's `internal/` and `backend/` package boundaries:

```
glassdb-data → glassdb-backend → glassdb-storage → glassdb-trans → glassdb
glassdb-proto ─┘                  ↑                      ↑
glassdb-concurr ──────────────────┴──────────────────────┘
glassdb-backend-s3, glassdb-backend-gcs → glassdb (optional, feature-gated)
```

Rationale:

- **Mirrors the original module boundaries**, so the mapping between the two
  codebases is one-to-one and easy to cross-reference.
- **Enables clean parallel development.** The port was executed in waves by
  multiple agents owning disjoint crates; crate boundaries gave conflict-free
  ownership and independent `cargo test`.
- **Enforces the dependency DAG** at compile time (e.g. `storage` cannot reach
  into `trans`).

Only the top-level `glassdb` crate is intended for direct use; the rest are
implementation details.

## Concurrency model

Go's runtime primitives map onto tokio as follows.

| Go | Rust |
| --- | --- |
| `context.Context` (cancellation) | dropped futures (`tokio::time::timeout`, `tokio::select!`, `JoinHandle::abort`). Public APIs take no context. `Background` aborts spawned tasks via [`JoinHandle::abort`] from its `Drop` impl. The one residual primitive is `AbortSignal` (`AtomicBool` + `Notify`), used to drop a specific in-flight future from outside (sim `JoinHandle::abort`, `Dedup::close`, sim crash nemesis) — never plumbed into worker bodies |
| `context.Context` (values) | not used. The one Go consumer (a deterministic tx-id override) was dropped: under `--cfg sim` the same determinism falls out of `TxId::new_at` drawing its random prefix from the seeded executor RNG and its timestamp from the anchored clock |
| goroutine | `tokio::spawn` |
| `concurr.Background` (managed goroutines, cancelled together) | `Background` is a [`JoinHandle`] collection: `spawn(fut)` tracks the handle; `Drop` calls `abort()` on every tracked handle. Subsystems hold `Weak<Background>` so the captured-task cycle does not pin it alive |
| `db.Close` (graceful shutdown) | [`DB::shutdown`] — flips a shutting-down flag so new `DB::tx` calls return `Error::ShuttingDown`, awaits in-flight transactions to drain, then awaits dedup owners; background loops are torn down via `Drop` on the last `DB` clone |
| Transaction cancellation cleanup | `DbInner::tx_impl` owns a `TxAbortGuard` RAII helper armed after `algo.begin`/`algo.rebegin` and disarmed after `algo.end`. If the surrounding future is dropped in-between, the guard's `Drop` schedules `Algo::async_abort` so the engine-side tx is marked aborted promptly instead of lingering until lease expiry |
| `errgroup` / bounded fan-out | `futures::stream::iter(...).buffer_unordered(n)` (see `Algo::run_limited`) and, in tests, `tokio::join!` |
| `sync.Mutex` over small state | `std::sync::Mutex`, **never held across `.await`** |
| channels | `tokio::sync` channels |
| `select` on ctx + timers | `tokio::select!` on `ctx.cancelled()` + `tokio::time::sleep` (internal cancellation only); for caller-facing timeouts, `tokio::time::timeout` around the future |

### Synchronous mutexes, not async mutexes

Internal shared state (cache maps, monitor/locker tables) uses
`std::sync::Mutex`, and guards are always dropped before any `.await`. This
matches the original code, which holds `sync.Mutex` only for short critical
sections, and avoids the overhead and deadlock-footguns of an async mutex. This
invariant is enforced in CI by `clippy::await_holding_lock` under `-D warnings`.

### The `Dedup` state machine

`concurr.Dedup` (the mergeable-request deduplicator used by the distributed
locker) has no off-the-shelf Rust equivalent. The Go port originally ran the
worker *on a caller's goroutine* and used `defer` to hand the role to the next
waiter; in Rust that coupling made worker liveness depend on a caller-future's
lifetime, which a *dropped* future violates (orphaned keys / lost handoffs). It
was reworked into a driver model where worker liveness is never tied to a single
caller future:

- A key is driven by exactly one **driver**. The first caller for an idle key is
  the **inline driver**: it runs the worker on its own task with no spawn, so the
  uncontended common case (one locker per key) keeps the original zero-overhead
  hot path.
- A driver hands off only when it genuinely must: its batch is done but
  non-mergeable queued work remains, or its own future is dropped with live
  waiters. Both hand off by **`rt::spawn`-ing a fresh owner task** — never by
  promoting a waiter's future-that-must-be-polled. A spawned task cannot be
  dropped by a caller, so the handoff cannot be lost. A dropped inline driver is
  caught by a `DriverGuard` RAII guard that requeues undelivered live members and
  spawns the successor.
- All driver/owner/exit transitions happen under the same per-key lock that
  submitters use, so "the key entry exists iff it has a driver" holds without
  races. Waiters simply `await` their `oneshot::Receiver`; dropping that future
  closes the corresponding `Sender`, and a `WaiterDropGuard` pings the per-key
  `changed` notifier so the driver re-evaluates liveness and prunes the dead
  member (abandoning the batch if no live caller remains).
- `BatchHandle::merged()` reconstructs the merged request (absorbing newly
  arrived compatible work); `BatchHandle::changed()` (a `Notify`) wakes the
  worker on new work. When a batch loses all live members the worker's context is
  cancelled so it bails (best-effort: at worst one wasted round, never a hang).
- `Dedup::close()` cancels a shutdown token (parent of every round's context) and
  awaits any spawned owners, so none leak; it is wired through
  `Locker::close` → `Algo::close` → `DB::close`.

This was one of the highest-risk pieces; its behavior tests and drop/cancel
regression tests live in [`dedup.rs`](crates/glassdb-concurr/src/dedup.rs).

### Cancel-safety contract

Public futures are durability-safe to cancel: a mid-flight drop is equivalent to
a crash and is recovered by the commit protocol. **Dropping the future is the
only cancellation mechanism**: there is no `Ctx` argument to cancel separately.
Wrap a future with `tokio::time::timeout`, race it in a `tokio::select!`, or
`abort` the `JoinHandle` to stop it. The deduplicator makes dropping safe for
in-memory liveness too: a queued caller is pruned via `WaiterDropGuard`, and a
dropped inline driver hands its batch off to a spawned owner, so neither
orphans a key nor strands other callers. On-storage locks left by a drop are
reclaimed by wait/lease timeouts as before. This contract is documented on the
public API (`glassdb` `lib.rs` and `DB::tx`).

## Time and determinism

The original tests use Go's `synctest` to make time deterministic. Rust's
equivalent is `tokio::time::pause`/`advance`, but it only controls **tokio's**
clock — not `std::time`. Two distinct notions of time exist in the codebase, and
each is mapped deliberately:

- **Relative staleness** (cache freshness, lock-wait backoff) uses
  `tokio::time::Instant`. This was a key fix: the cache originally used
  `std::time::Instant`, which ignores `tokio::time::pause` and made a
  cross-process lock-wait test spend 10s of real time. Switching to
  `tokio::time::Instant` made those paths deterministic and dropped the whole
  test suite from ~13s to ~3s of wall-clock, with no public API change.
- **Absolute wall-clock timestamps** (transaction-log `last_update`, stored as
  unix-millis tags) need `SystemTime`. The `Clock` abstraction provides this:
  `Clock::real()` in production, and `Clock::anchored()` in tests, which anchors
  a `SystemTime` base to a `tokio::time::Instant` base so "now" advances together
  with the mocked tokio clock.

This split lets every time-sensitive path — staleness, transaction expiry,
lock waits, GC delays — be driven deterministically under
`#[tokio::test(start_paused = true)]`.

## Error handling and sentinel parity

Go drives control flow with `errors.Is(err, ErrRetry / ErrNotFound /
ErrPrecondition / …)`. Rust replaces these with typed `thiserror` enums, one per
layer, and preserves the exact matching semantics via `is_*` predicates:

- `BackendError`: `NotFound`, `Precondition`, `Other`. (Go's `ctx.Err()` has no
  equivalent: a dropped future is the cancel; backends never observe a context.)
- `StorageError`: wraps `BackendError`, plus `KeyNotFound`; `is_not_found` /
  `is_precondition` delegate to the backend.
- `TransError`: the engine's control-flow errors — `Retry` (Go `ErrRetry`),
  `AlreadyFinalized`, `Wounded` (Go `ErrWounded`; a wound-wait abort, consumed by
  the DB retry loop which restarts the victim with a renewed ID), and the
  **internal** `ValidateRetry` (Go `errValidateRetry`) and
  `LockTimeout` / `NoSingleWrite`. There is no `Cancelled` variant.
- `glassdb::Error`: the public surface — `NotFound`, `Aborted`, `Precondition`,
  `AlreadyFinalized`, `Other`.

Each layer converts into the next with `From`, mapping sentinels losslessly.
Care was taken that **internal** control-flow errors never leak: e.g.
`ValidateRetry` is consumed by `Algo::commit`'s validation loop, and the public
retry loop only treats `TransError::Retry` (not `ValidateRetry`) as a retry —
matching the original transaction loop, which only reacts to `ErrRetry`.

## Encoding fidelity

These must be byte-identical to the original format because they are persisted
or compared across processes; they are anchored by golden vectors generated from
the Go code:

- **Paths**: a custom order-preserving base64 alphabet with type markers, ported
  in `glassdb-data::paths`. Go's `path.Clean`/`path.Join` are reimplemented
  faithfully in `glassdb-data::gopath` so storage keys match exactly.
- **`TxId`**: `[8 random bytes][8 bytes big-endian UnixNano timestamp]`, lower-hex
  `Display`. The random prefix leads so transaction-log keys keep a
  high-entropy prefix (object stores partition by key prefix), while the
  timestamp suffix encodes the wound-wait priority (see
  [ADR-002](docs/adr/002-wound-wait-locking.md)).
- **Transaction log protobuf**: `prost`-generated from a copy of
  `transaction.proto`, keeping identical field numbers and the `oneof val_delete`
  layout, so logs written by either implementation are mutually readable.

## Cloud backends and in-process fakes

Both cloud backends implement the same `Backend` trait and live in their own
feature-gated crates so the AWS SDK / reqwest dependencies are opt-in.

- **S3** (`glassdb-backend-s3`, `aws-sdk-s3`). The opaque `Version` token is the
  object ETag (quotes included). Because an ETag is the MD5 of the content, an
  8-byte random nonce is prepended to every object body so that re-uploading
  identical bytes still yields a fresh ETag — restoring real compare-and-swap
  for metadata-only updates. Lock/last-writer tags are stored as `x-amz-meta-*`
  user metadata; conditional writes use `If-Match` / `If-None-Match`. Two retry
  layers compose: the SDK retryer rides out `503 SlowDown` within each
  `PutObject` (the default is the adaptive retryer with a raised attempt count),
  while a small outer loop re-issues on the `409 ConditionalRequestConflict`
  that S3 returns when concurrent conditional writes race.
- **GCS** (`glassdb-backend-gcs`, JSON API over `reqwest`). GCS has native
  preconditions, so the token encodes `"{generation}/{metageneration}"` and maps
  directly onto `ifGenerationMatch` / `ifMetagenerationMatch` (with
  `ifGenerationMatch=0` for create-if-absent). Values are uploaded with a
  `multipart/related` insert; tags are object custom metadata. One subtlety: a
  metadata patch *replaces* the whole metadata map, but the locker calls
  `set_tags_if` with only the lock tags, so the backend reads the current
  metadata and overlays the new tags to preserve `last-writer` — matching the
  merge semantics of the memory and S3 backends. Authentication uses
  Application Default Credentials via `gcp_auth`, resolved lazily on first
  request; an injectable `TokenProvider` and an unauthenticated `with_endpoint`
  constructor support emulators and tests.

### In-process fakes instead of Docker

The Go tests use `gofakes3` and `fake-gcs-server`. The Rust port keeps the
"no external services" property by compiling a minimal `hyper` server of the
relevant REST subset into each crate under `#[cfg(test)]`, so the real SDK /
HTTP client talks to a loopback socket. This exercises the actual request
encoding, conditional headers, and error mapping without Docker or live
credentials. The S3 fake can also inject a bounded run of `503 SlowDown`
responses (the analog of the Go `SlowDownTransport`) to drive the retry tests
deterministically.

## Middleware decorators

The latency, scheduler, and logger decorators wrap any `Backend`:

- `DelayBackend` injects per-operation latency from a lognormal distribution
  plus an optional per-object token-bucket rate limiter and an optional
  per-prefix request-rate limiter (modeling S3's documented 5,500 GET / 3,500
  PUT-per-prefix ceiling), all driven by `tokio::time` so tests stay
  deterministic under paused time (presets mirror the Go GCS/S3 latency
  profiles).
- `ScheduledBackend` / `Scheduler` inject byte-sequence-driven delays, the
  building block needed to replay `FuzzConcurrentTx` interleavings.
- `BackendLogger` traces every operation via the `tracing` crate.

## Public API choices

- **`DB::tx` takes the body by value: `FnMut(Tx) -> impl Future + Send`.** Write
  it as `|tx| async move { ... }`. `Tx` is a cheap, `Send` handle over
  interior-mutable state, passed by value rather than `&mut`, so the transaction
  future is `Send` and can be `tokio::spawn`-ed for true multiplexing. (An
  `async |tx|` closure cannot: bounding its returned future as `Send` is not
  expressible for `AsyncFn*` on stable Rust.) The framework owns the retry loop
  and reruns the closure on conflict, mirroring Go's `db.Tx(ctx, func(tx) error)`
  while letting the body `.await` reads.
- **One read primitive: `Tx::read`.** Because `read` takes `&self`, several
  reads can be polled concurrently for parallel fetches
  (`futures::future::join_all(keys.iter().map(|k| tx.read(coll, k)))`), so the
  old batched `read_multi` (and its `FqKey`/`ReadResult` types) was dropped.
- **`Collection` helpers** (`read_strong`, `write`, `delete`, `update`, …) each
  run a one-shot transaction via the same retry loop, matching the original.
- **Iterators return in-memory snapshots.** The memory backend returns the full
  listing up front, so `KeysIter`/`CollectionsIter` iterate a decoded `Vec` and
  expose a terminal `err()` accessor (rather than Go's `Next() (v, ok)` plus
  `Err()` pattern, adapted to Rust's `Iterator`).
- **Re-exports for ergonomics.** `glassdb` re-exports `Backend`, the `memory`
  backend, and `middleware`, so callers need only depend on the one crate; the
  `s3` / `gcs` backends are re-exported under their cargo features.

## Testing strategy

- **Determinism via paused time.** Integration tests use
  `#[tokio::test(start_paused = true)]`; contended paths resolve via virtual time
  with no real sleeps.
- **`join!` instead of `spawn` for concurrency tests.** Transaction futures are
  `Send` and could be spawned, but the integration/property tests run on a
  current-thread `start_paused` runtime where `tokio::join!` keeps every workload
  on one task — giving real interleaving (tasks yield at `.await` points under
  contention) while keeping virtual-time control simple and deterministic.
- **Deterministic simulation fuzzer (in-repo executor).** The original
  `FuzzConcurrentTx` uses a byte-driven backend-delay scheduler. The Rust port
  goes further with a DST in which scheduling, time, and randomness are all
  functions of the input. The runtime started on `madsim`
  ([ADR-008](docs/adr/008-deterministic-simulation-fuzzer.md)) but now runs on a
  minimal in-repo deterministic executor that controls task poll order via a
  pluggable scheduler — a **schedule-tape** (the libFuzzer-guidable primary) and
  **PCT** (seed-breadth) — with faults injected at the `Backend` trait
  (`FaultBackend`) rather than a simulated network: see
  [ADR-011](docs/adr/011-guided-interleaving-executor.md) (and
  [ADR-010](docs/adr/010-fuzzer-coverage-guidance.md) for why guidance needs a
  tape). A `cargo-fuzz` target (`fuzz/`) turns libFuzzer bytes into
  `(seed, Workload, FaultConfig, schedule_tape, fault_tape)` — a second tape that
  makes the fault schedule coverage-guidable too — runs an N-client RMW mix on a
  shared `MemoryBackend`, where each client reaches the store over its own faulty
  *transport* (delays, dropped-request / lost-ack faults on either side, and
  sustained per-client outages) and crashed clients restart on the same backend,
  and asserts the serializability invariant. The
  self-check additionally asserts that two same-tape/seed runs issue a
  **byte-for-byte identical backend-call stream** (`RecordingBackend` +
  `tests/concurrent_sim.rs`), which required the previously-deferred commit-path
  ordering (now justified) and a fixed-base deterministic clock. Run with
  `make test-sim` / `make fuzz`. The `proptest_concurrent` test is kept as a fast
  non-sim sanity check. Go's byte-schedule approach was inspiration only.
- **Cycle serializability oracle (ported from FoundationDB's `Cycle.cpp`).** A
  second fuzz target (`cycle`) lays down a ring `key(i) -> (i+1) % N` and has
  clients rotate three consecutive edges per transaction. The rotation does not
  commute, so any isolation or atomicity break splits/shrinks/grows the ring —
  catching serializability anomalies the commutative RMW-increment workload
  cannot. It reuses the same backbone/transports/nemeses and runs under both the
  schedule-tape and PCT schedulers. A concurrent read-only observer also
  snapshots the whole ring in one transaction (all `N` pointer reads issued
  concurrently via `try_join_all`) and asserts the snapshot is itself a valid
  ring: a committed read-only tx must see a single committed state, so this adds
  a read-side oracle and is the only workload exercising `Tx`'s concurrent-read
  path. See `tests/cycle_sim.rs` and `fuzz/corpus/cycle`.
- **Behavioral tests, ported wholesale.** The unit tests of the hard pieces
  (`dedup`, `algo`, `tlocker`, `monitor`, `gc`, `cache`) were ported from their
  Go counterparts to lock in equivalent behavior.

## Deviations from the original plan

- **Retry backoff is hand-rolled, not `backon`.** The plan suggested the `backon`
  crate; the implementation uses a small exponential-backoff helper
  (`retry_with_backoff`) with `Transient`/`Permanent` error classification,
  avoiding an extra dependency for a few dozen lines.
- **GCS rate-limit retry simplified.** The Go GCS backend retries a `429`
  by rechunking the upload; the Rust port surfaces `429` as a generic error for
  now, since the transaction engine already retries at a higher level.

## Benchmarks

The Go benchmark surface is ported so performance work is reproducible on both
a **simulated** backend (in-memory + `DelayBackend`) and **real Amazon S3**.

- **Criterion microbenchmarks** (`crates/glassdb/benches/transactions.rs`, run
  with `make bench`). Port the six `bench_test.go` workloads — single-key RMW,
  10-key RMW, 10-key read, 100-key write, and two contended workloads (a
  background-contender multi-RMW and a shared-read) — over `{memory, sim-gcs,
  sim-s3}`. The simulated profiles compress latency 1000× (`scale = 1/1000`),
  matching the Go bench scaling. Each pairing also prints the per-operation
  backend counters (`retries/op`, `w/op`, `r/op`, …) derived from
  `DB::stats()`, the analog of Go's `benchStats` custom metrics.
- **`glassdb-bench-scale` crate** — concurrency / throughput benchmarks against
  real or simulated cloud backends (the AWS / GCS SDKs live here), ported from
  `hack/rtbench` and `hack/backendbench`:
  - `rtbench` — the `simple`, `rw9010`, and `deadlock` scenarios, with the same
    flags and CSV schema as the Go tool (so the plotting scripts are shared),
    deterministic per-DB/per-worker seeds, and a real-S3 client wired up via
    `aws_config` + a `BUCKET` env var (faithful to the Go tooling), or real GCS
    via Application Default Credentials.
  - `backendbench` — raw backend-operation latencies (`WriteSame`,
    `WriteFailPre`, `Read`, `ReadUnchanged`, `SetMetaSame`, `GetMeta`).
- **`glassdb-bench-score` crate** — lightweight, in-memory local benchmarks (no
  cloud SDKs, so it builds fast); backs the autoresearch loop and CI
  perf-regression checks (`make bench-score`):
  - `autoresearch` — the autoresearch scoring harness (single-client,
    deterministic); see [`hack/autoresearch/`](hack/autoresearch).
- **`hack/aws-bench/`** — the real-S3 harness: `cloudformation.yaml` (private
  VPC + SSM + S3 gateway endpoint), `deploy.sh` (now cross-compiles a static
  `x86_64-unknown-linux-musl` `rtbench` instead of a Go binary), and the
  unchanged `plot.py` / `compare.py` (the CSV schema is preserved).

Two parts deviate from the Go original by necessity:

- **Worker concurrency uses `tokio::spawn`, like goroutines.** Because the
  `db.tx` future is `Send` (the body is taken by value, see [Public API
  choices](#public-api-choices)), `rtbench` spawns each worker as a task on a
  shared multi-thread runtime, which multiplexes them over its worker threads.
  All I/O is registered with one reactor, so a single shared S3 client serves
  every worker — matching the Go design where all goroutines share one client.
- **Two `client-stats.csv` columns are best-effort.** `new-conns` is always `0`
  (the aws-sdk HTTP stack does not surface TLS handshakes) and `max-goroutines`
  holds the peak OS-thread count (sampled from `/proc/self/status`). The CSV
  schema and headers are otherwise byte-identical to the Go tool. HTTP request /
  throttle / 5xx / 2xx counts come from an aws-smithy `Intercept`, and CPU time
  from `getrusage(RUSAGE_SELF)`.

The simulated S3 path tracks real S3 because `DelayBackend` now also models the
per-prefix request-rate ceiling (see [Middleware decorators](#middleware-decorators)).

## Out of scope

- Autoresearch perf experiments from upstream (`9b00d94` … `6bd75ea`), including
  their `hack/autoresearch/bench` tooling.

## Upstream sync log

| Field | Value |
| --- | --- |
| Upstream repo | `github.com/mbrt/glassdb` |
| Last ported commit | `ed5ec47` (partial) + `fe03218` test (partial) |
| Ported on | 2026-06-03 |
| Deferred — autoresearch | `9b00d94` … `6bd75ea` |
| Determinism/fuzz | Rust-native DST on an in-repo deterministic executor (schedule-tape + PCT) + cargo-fuzz built (ADR-011, superseding the madsim runtime of ADR-008); Go determinism commits `790c1a7`/`0b2c609` were inspiration only |
| Next port | autoresearch batch, or next non-autoresearch commit after HEAD |

### What was ported (`c1471c3`..`62dab6f`, autoresearch excluded)

- ADR-007 lost-update fix in `validate_locked_read` / `validate_read_not_found`
  ([docs/adr/007-single-rw-cache-lost-update.md](docs/adr/007-single-rw-cache-lost-update.md))
- `single_rw_lost_update` regression test in `glassdb-trans`

### Determinism work (built Rust-native, see ADR-008 and ADR-011)

- Stable path ordering of commit-path slices (`Tx::collect_accesses`,
  `init_validation`, `collections_locks`, `Locker::locked_paths`)
- In-repo deterministic executor (`glassdb-concurr` `rt`/`exec`) with schedule-tape
  and PCT schedulers; seeded scheduling/time/randomness; `TxId` prefix from the
  run's seeded RNG (`#[cfg(sim)]`); `DbBuilder::deterministic_time` fixed-base clock
  and `rt::system_now` for persisted timestamps
- `FaultBackend` (Backend-level fault injection) + `RecordingBackend` op-stream
  self-check + cargo-fuzz `concurrent_tx` target (schedule-tape)

### What was not ported (use as inspiration only)

- Injectable TxId prefix source / DB `Rand` option (the `#[cfg(sim)]` seeded-RNG
  shim supersedes)
- Configurable retry + jitter / `DisableJitter`
- `Monitor` Retrier refactor
- Go fuzz workload + `TestConcurrentTxDeterministicOutcome` (replaced by the
  in-repo-executor DST)
- Autoresearch perf experiments

### How to port the next batch

1. In the upstream repo: `git log <last-ported>..HEAD --oneline`
2. Skip commits whose subject starts with `autoresearch:`
3. Map Go packages to Rust crates using the workspace table above
4. Run `make test` before committing
