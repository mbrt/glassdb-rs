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
- **Memory backend only.** Cloud backends (S3/GCS) and their fake-cloud test
  harness are out of scope for now (see [below](#out-of-scope)).
- **Async on tokio.** The whole stack is `async`, built on tokio.

## Workspace structure

The port is a Cargo workspace of seven internal crates (`publish = false`) that
mirror Go's `internal/` package boundaries:

```
glassdb-data → glassdb-backend → glassdb-storage → glassdb-trans → glassdb
glassdb-proto ─┘                  ↑                      ↑
glassdb-concurr ──────────────────┴──────────────────────┘
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
| `context.Context` (cancellation + values) | `glassdb_concurr::Ctx` wrapping `tokio_util::sync::CancellationToken`, plus an optional tx-id value |
| goroutine | `tokio::spawn` |
| `concurr.Background` (managed goroutines, cancelled together) | `Background` over `CancellationToken` + `TaskTracker` |
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
  `AlreadyFinalized`, `Cancelled`, and the **internal** `ValidateRetry`
  (Go `errValidateRetry`) and `LockTimeout` / `NoSingleWrite`.
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
- **`TxId`**: 16 random bytes, lower-hex `Display`.
- **Transaction log protobuf**: `prost`-generated from a copy of
  `transaction.proto`, keeping identical field numbers and the `oneof val_delete`
  layout, so logs written by either implementation are mutually readable.

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
  backend, and `Ctx`, so callers need only depend on the one crate.

## Testing strategy

- **Determinism via paused time.** Integration tests use
  `#[tokio::test(start_paused = true)]`; contended paths resolve via virtual time
  with no real sleeps.
- **`join!` instead of `spawn` for concurrency tests.** Async closures captured
  into `tokio::spawn` hit "implementation of `Send` is not general enough" (a
  higher-ranked-lifetime limitation). Running concurrent workloads with
  `tokio::join!` on the current-thread paused runtime gives real interleaving
  (tasks yield at `.await` points under contention) without the `Send` bound.
- **Property test in place of fuzzing.** The original `FuzzConcurrentTx` uses a
  byte-driven scheduler middleware to replay interleavings. That middleware is
  not yet ported, so the Rust mirror is a `proptest` that randomizes per-key
  increment counts across two concurrent DBs and asserts the serializability
  invariant (each key's final value equals the total successful increments).
- **Behavioral tests, ported wholesale.** The unit tests of the hard pieces
  (`dedup`, `algo`, `tlocker`, `monitor`, `gc`, `cache`) were ported from their
  Go counterparts to lock in equivalent behavior.

## Deviations from the original plan

- **Retry backoff is hand-rolled, not `backon`.** The plan suggested the `backon`
  crate; the implementation uses a small exponential-backoff helper
  (`retry_with_backoff`) with `Transient`/`Permanent` error classification,
  avoiding an extra dependency for a few dozen lines.
- **No scheduler/delay/logger middleware yet.** Only the memory backend and the
  stats decorator were ported. The deterministic scheduler middleware (needed to
  fully reproduce `FuzzConcurrentTx`) is deferred; the proptest above covers the
  invariant in the meantime.

## Out of scope

- S3/GCS backends and the fake-cloud test harness (`gofakes3`, fake-GCS).
- Benchmark tooling and demos from the original repository.
