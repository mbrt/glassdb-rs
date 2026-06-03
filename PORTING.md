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
| `context.Context` (cancellation + values) | `glassdb_concurr::Ctx` wrapping an in-house `CancelToken` (hierarchical, over `tokio::sync::watch`), plus an optional tx-id value |
| goroutine | `tokio::spawn` |
| `concurr.Background` (managed goroutines, cancelled together) | `Background` over `CancelToken` + tracked `JoinHandle`s |
| `errgroup` / bounded fan-out | `Fanout` (a `Semaphore`-bounded join of futures) and, in tests, `tokio::join!` |
| `sync.Mutex` over small state | `std::sync::Mutex`, **never held across `.await`** |
| channels | `tokio::sync` channels; `make_chan_inf_cap` for the unbounded case |
| `select` on ctx + timers | `tokio::select!` on `ctx.cancelled()` + `tokio::time::sleep` |

### Synchronous mutexes, not async mutexes

Internal shared state (cache maps, monitor/locker tables) uses
`std::sync::Mutex`, and guards are always dropped before any `.await`. This
matches the original code, which holds `sync.Mutex` only for short critical
sections, and avoids the overhead and deadlock-footguns of an async mutex. This
invariant is enforced in CI by `clippy::await_holding_lock` under `-D warnings`.

### The `Dedup` state machine

`concurr.Dedup` (the mergeable-request deduplicator used by the distributed
locker) has no off-the-shelf Rust equivalent, so it was ported directly as a
`Controller` driving per-key worker state (pending / queue / merge), with
`await_signal` over a `Semaphore` for "wake the next waiter". This was one of the
highest-risk pieces and its behavior tests were ported first.

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

- `BackendError`: `NotFound`, `Precondition`, `Cancelled`, `Other`.
- `StorageError`: wraps `BackendError`, plus `KeyNotFound`; `is_not_found` /
  `is_precondition` delegate to the backend.
- `TransError`: the engine's control-flow errors — `Retry` (Go `ErrRetry`),
  `AlreadyFinalized`, `Wounded` (Go `ErrWounded`; a wound-wait abort, consumed by
  the DB retry loop which restarts the victim with a renewed ID), `Cancelled`,
  and the **internal** `ValidateRetry` (Go `errValidateRetry`) and
  `LockTimeout` / `NoSingleWrite`.
- `glassdb::Error`: the public surface — `NotFound`, `Aborted`, `Precondition`,
  `Cancelled`, `AlreadyFinalized`, `Other`.

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
  plus an optional per-object token-bucket rate limiter, driven by
  `tokio::time` so tests stay deterministic under paused time (presets mirror
  the Go GCS/S3 latency profiles).
- `ScheduledBackend` / `Scheduler` inject byte-sequence-driven delays, the
  building block needed to replay `FuzzConcurrentTx` interleavings.
- `BackendLogger` traces every operation via the `tracing` crate.

## Public API choices

- **`DB::tx` takes an `AsyncFnMut` closure.** The transaction body is
  `async |tx| -> Result<T, Error>`; the framework owns the retry loop and reruns
  the closure on conflict. This mirrors Go's `db.Tx(ctx, func(tx) error)` while
  letting the body `.await` reads.
- **`Collection` helpers** (`read_strong`, `write`, `delete`, `update`, …) each
  run a one-shot transaction via the same retry loop, matching the original.
- **Iterators return in-memory snapshots.** The memory backend returns the full
  listing up front, so `KeysIter`/`CollectionsIter` iterate a decoded `Vec` and
  expose a terminal `err()` accessor (rather than Go's `Next() (v, ok)` plus
  `Err()` pattern, adapted to Rust's `Iterator`).
- **Re-exports for ergonomics.** `glassdb` re-exports `Backend`, the `memory`
  backend, `middleware`, and `Ctx`, so callers need only depend on the one
  crate; the `s3` / `gcs` backends are re-exported under their cargo features.

## Testing strategy

- **Determinism via paused time.** Integration tests use
  `#[tokio::test(start_paused = true)]`; contended paths resolve via virtual time
  with no real sleeps.
- **`join!` instead of `spawn` for concurrency tests.** Async closures captured
  into `tokio::spawn` hit "implementation of `Send` is not general enough" (a
  higher-ranked-lifetime limitation). Running concurrent workloads with
  `tokio::join!` on the current-thread paused runtime gives real interleaving
  (tasks yield at `.await` points under contention) without the `Send` bound.
- **Deterministic simulation fuzzer (madsim).** The original `FuzzConcurrentTx`
  uses a byte-driven backend-delay scheduler. The Rust port goes further with a
  madsim-based DST in which scheduling, time, and randomness are all functions of
  a single seed: see [ADR-008](docs/adr/008-deterministic-simulation-fuzzer.md).
  A `cargo-fuzz` target (`fuzz/`) turns libFuzzer bytes into `(seed, Workload)`,
  runs an N-client RMW mix on a shared `MemoryBackend`, and asserts the
  serializability invariant. The self-check additionally asserts that two
  same-seed runs issue a **byte-for-byte identical backend-call stream**
  (`RecordingBackend` + `tests/concurrent_sim.rs`), which required the
  previously-deferred commit-path ordering (now justified) and a fixed-base
  deterministic clock. Run with `make sim-test` / `make fuzz`. The
  `proptest_concurrent` test is kept as a fast non-madsim sanity check.
  Go's byte-schedule approach was inspiration only; the Rust design is
  madsim-native.
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

## Out of scope

- Benchmark tooling and demos from the original repository.
- Autoresearch perf experiments from upstream (`9b00d94` … `6bd75ea`).

## Upstream sync log

| Field | Value |
| --- | --- |
| Upstream repo | `~/priv/glassdb` |
| Last ported commit | `ed5ec47` (partial) + `fe03218` test (partial) |
| Ported on | 2026-06-03 |
| Deferred — autoresearch | `9b00d94` … `6bd75ea` |
| Determinism/fuzz | Rust-native madsim DST + cargo-fuzz built (ADR-008); Go determinism commits `790c1a7`/`0b2c609` were inspiration only |
| Next port | autoresearch batch, or next non-autoresearch commit after HEAD |

### What was ported (`c1471c3`..`62dab6f`, autoresearch excluded)

- ADR-007 lost-update fix in `validate_locked_read` / `validate_read_not_found`
  ([docs/adr/007-single-rw-cache-lost-update.md](docs/adr/007-single-rw-cache-lost-update.md))
- `single_rw_lost_update` regression test in `glassdb-trans`

### Determinism work (built Rust-native, see ADR-008)

- Stable path ordering of commit-path slices (`Tx::collect_accesses`,
  `init_validation`, `collections_locks`, `Locker::locked_paths`)
- madsim deterministic runtime (seeded scheduling/time/randomness); `TxId`
  prefix from `madsim::rand`; `Options::deterministic_time` fixed-base clock
- `RecordingBackend` op-stream self-check + cargo-fuzz `concurrent_tx` target

### What was not ported (use as inspiration only)

- Injectable TxId prefix source / DB `Rand` option (madsim shim supersedes)
- Configurable retry + jitter / `DisableJitter`
- `Monitor` Retrier refactor
- Go fuzz workload + `TestConcurrentTxDeterministicOutcome` (replaced by the
  madsim DST)
- Autoresearch perf experiments

### How to port the next batch

1. In the upstream repo: `git log <last-ported>..HEAD --oneline`
2. Skip commits whose subject starts with `autoresearch:`
3. Map Go packages to Rust crates using the workspace table above
4. Run `make test` before committing
