# ADR-012: Reinstate `tokio_util` for cancellation primitives

## Status

Accepted

Follow-up to [ADR-008](008-deterministic-simulation-fuzzer.md) and
[ADR-011](011-guided-interleaving-executor.md).

## Context

[ADR-008](008-deterministic-simulation-fuzzer.md) §1 (*Resolving
non-determinism seams*) removed `tokio_util` from the dependency graph because
it could not run under `madsim`: `madsim-tokio` aliased the runtime, but
`tokio_util` was not redirected and still pulled real `tokio`. Two `tokio_util`
primitives were in use at the time — `CancellationToken` and `TaskTracker` —
and both were replaced by hand-rolled equivalents (`CancelToken` over
`tokio::sync::Notify`, and a `Vec<JoinHandle>` in `Background`). `CancelToken`
later collapsed further into `AbortSignal` (`AtomicBool` + `Notify`) once the
engine moved entirely to future-drop cancellation (`tokio::select!`,
`JoinHandle::abort`).

[ADR-011](011-guided-interleaving-executor.md) then replaced the `madsim`
runtime with an in-repo deterministic executor (`DetExecutor`) routed through
the `rt` seam. The seam redirects **only** `tokio::spawn` and `tokio::time`;
`tokio::sync` (and everything built on it) is reused unchanged. The original
blocker for `tokio_util` is therefore gone.

The remaining hand-rolled `AbortSignal` is a thin wrapper around an
`AtomicBool` and `tokio::sync::Notify`. `tokio_util::sync::CancellationToken`
has the same shape and the same internals: a cancellation-state flag plus
`tokio::sync::Notify` for the wakeup (see the `TreeNode` in
`tokio-util/src/sync/cancellation_token/tree_node.rs`). It is cheap to clone
(`Arc`-backed), sticky, supports both sync `is_cancelled()` and async
`cancelled().await`, and — crucially for the determinism gates — uses
`Notify::notify_waiters` (not `watch`, which would draw from `tokio`'s
thread-local `FastRand`; see ADR-008 §5).

## Decision

Replace `glassdb_concurr::AbortSignal` with
`tokio_util::sync::CancellationToken` everywhere
(`rt::JoinHandle::Det::abort`, `dedup::Inner::shutdown`, dedup per-round
`op_signal`, sim `crash_nemesis` per-client signal). Delete
`crates/glassdb-concurr/src/abort_signal.rs`.

Add `tokio-util = "0.7"` (default features, no extra cargo features needed —
`pub mod sync` is unconditional) to `glassdb-concurr` and `glassdb`.

`Background` is **not** migrated to `tokio_util::task::AbortOnDropHandle` or
`TaskTracker`: it owns the rt-seam `JoinHandle` enum (`Det` / `Tokio`), so the
abort-on-drop wrapper would need a parallel implementation that mirrors the
seam. The existing `Drop` impl is a 7-line loop calling `JoinHandle::abort()`
and is clearer than a wrapper would be.

### On the bespoke RAII guards

A side question: should the hand-rolled `Drop` guards
(`DriverGuard`, `WaiterDropGuard` in `dedup.rs`, `InFlightGuard`,
`TxAbortGuard` in `db.rs`, `PushGuard` in `tlocker.rs`, `CurrentGuard` in the
sim executor) be replaced by something standard (`scopeguard`,
`tokio_util::sync::DropGuard`)?

They are kept as-is. Their `drop` bodies are domain-specific cleanup —
locking a shard, requeuing batch members, possibly spawning an owner task
(`DriverGuard`); incrementing/decrementing an in-flight counter and notifying
on zero (`InFlightGuard`); marking a lock as `Unknown` in a shard map
(`PushGuard`); calling `Algo::async_abort` for a per-tx id (`TxAbortGuard`).
`tokio_util::sync::DropGuard` only covers "cancel a `CancellationToken` on
drop" — none of these are that shape. A `scopeguard` closure would have to
capture the same fields the named struct already names, and the named struct
plus `Drop` impl is the more idiomatic Rust pattern at this size. The
`armed: bool` / `armed: Option<T>` disarming convention is consistent across
all of them.

## Consequences

- **One fewer primitive to maintain.** `abort_signal.rs` (~70 lines plus a
  re-export) is gone; cancellation throughout the engine is now a single
  well-known type from a widely-used crate.
- **No behaviour change.** `CancellationToken` and `AbortSignal` have
  identical observable semantics (sticky flag + broadcast wakeup over
  `Notify`); the only difference is that `CancellationToken` is cheaply
  cloneable, so call sites lose the explicit `Arc<…>` wrapping.
- **Determinism is preserved.** The byte-for-byte op-stream self-checks
  (`concurrent_sim` and `cycle_sim`, with and without faults; tape and PCT
  scheduler) and the committed fuzz-corpus replay pass after the migration,
  including back-to-back repeats; a quick 30 s libFuzzer pass on both
  `concurrent_tx` and `cycle` targets ran cleanly with growing coverage.
- **Dependency cost is small.** `tokio-util 0.7` was already in the lockfile
  transitively (pulled by `hyper-util` and `gcp_auth`); no new transitive
  crate enters the graph.
- **`tokio_util` remains usable in future seams.** If we later want
  `TaskTracker`, `AbortOnDropHandle`, hierarchical `child_token()`, or
  `mpsc`-style cancellation, the dependency is now established.
