# ADR-008: Deterministic simulation fuzzer (madsim)

## Status

Accepted. The `madsim` runtime and the simulated-network topology described here
are **superseded by [ADR-011](011-guided-interleaving-executor.md)** for the
engine DST (a minimal in-repo deterministic executor + `Backend`-level fault
injection). The acked-bounds invariant, the in-doubt reasoning, and the
`RecordingBackend` op-stream self-check below still apply.

## Context

GlassDB's correctness rests on a serializable concurrency-control protocol.
Concurrency bugs (like the lost update in
[ADR-007](007-single-rw-cache-lost-update.md)) only appear under specific
interleavings, so we need a way to (a) explore many interleavings and (b)
reproduce any failure *exactly* from a compact input — FoundationDB-style
deterministic simulation testing (DST).

The Go implementation drives interleavings with a byte-sequence scheduler
middleware (`ScheduledBackend`) that inserts delays before each backend call,
plus a `go test` fuzz target. That approach only perturbs *backend* timing; it
does not control task scheduling, the clock, or randomness, so a "deterministic"
replay still depends on the OS scheduler.

We want stronger guarantees in Rust: scheduling, time, and randomness should all
be pure functions of a single `u64` seed, so the entire async execution replays
identically.

## Decision

Run the engine on [`madsim`](https://github.com/madsim-rs/madsim), a
deterministic simulator for `tokio`, and drive it from
[`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz).

```
libFuzzer bytes ──arbitrary──▶ (u64 seed, Workload, FaultConfig)
                                   │
        seed ─▶ Runtime::with_seed_and_config ──▶ block_on(
                    run_and_assert_with_faults(workload, faults))
                                   │
        A storage node serves a MemoryBackend over the simulated network;
        each client opens its own DB on its own node and reaches the store
        via a NetBackend RPC client; a seeded nemesis injects network and
        node faults. Assert each key's acked <= final <= started (exact
        equality with faults off). Violation ⇒ panic ⇒ libFuzzer crash +
        reproducing input.
```

Why madsim over a hand-rolled scheduler: it deterministically controls
`tokio::spawn`, `tokio::time`, and `tokio::sync`, plus a real simulated network
(`madsim::net` with an RPC layer) and node lifecycle (`Handle::kill`/`restart`/
`pause`), with no custom executor. It activates only under
`RUSTFLAGS="--cfg madsim"`; in normal builds the `madsim-tokio` alias re-exports
real tokio, so production behavior is unchanged.

### Topology: one DB per node over a simulated network

Clients only ever coordinate through the object store, so faults are meaningful
only if the backend path crosses the simulated network. The harness therefore
builds a small cluster (all `#[cfg(madsim)]`, with the original single-process
shared-`Arc` path kept as the `#[cfg(not(madsim))]` fallback so the lib still
builds without madsim):

- a **storage node** binds an `Endpoint` and runs `serve_backend` over a
  harness-owned `MemoryBackend` (or `RecordingBackend`) whose state survives
  every node fault (madsim nodes share process memory);
- each **client node** opens its own `DB` over a `NetBackend` — a `Backend`
  implementation that forwards each call as an RPC. madsim's network *drops*
  clogged/partitioned packets, so `NetBackend` retries (bounded by `MAX_ATTEMPTS`,
  mirroring a real object-store client's adaptive retryer): a brief fault appears
  as latency, but a sustained outage exhausts the budget and surfaces a transient
  error that fails the transaction in-doubt. There is deliberately **no** dedup —
  `serve_backend` applies every request, like real object storage with no
  at-most-once request id, so a retry after a dropped *response* re-runs the op.
  That is sound because the engine's writes are conditional (CAS): a re-delivered
  write observes a `Precondition` for its own already-applied write, exactly as S3
  returns when the SDK retries a conditional `PUT` whose ack was lost. Crucially,
  `NetBackend` does *not* pass that ambiguous `Precondition` up as a confident
  conflict: once a response has been lost, a `Precondition` on a *conditional
  write* is indistinguishable from "my own write landed", so it is converted to
  the in-doubt `BackendError::Unavailable` (the in-doubt backend contract is
  [ADR-009](009-in-doubt-conditional-writes.md)). This exposes the engine's
  "did my commit land?" handling rather than masking it;
- a **nemesis node** drives a seeded sequence of faults via `NetSim`/`Handle`
  (`clog_link`/`disconnect`/`clog_node`, `pause`/`resume`, `kill`), eventually
  healing every network fault (some long enough to outlast the retry budget) and
  leaving the storage node up (it models durable cloud storage); and
- a **verifier node** reads every key with `read_strong` (which drives recovery
  of any crashed client's locks via virtual-time lease expiry) and checks the
  invariant.

### Correctness under faults: the acked-bounds invariant

Node kills make the exact `final == sum of increments` check invalid: an
in-flight transaction may or may not have committed. The harness tracks two
per-key counters in shared state: `started[k]` (increments that entered a
`db.tx`) and `acked[k]` (those whose commit returned `Ok`). The invariant is
`acked[k] <= final[k] <= started[k]` (plus the read-only `v >= 0` checks). An
acked commit is durable (`acked <= final`); every committed increment came from
some started op committing at most once (`final <= started`). The harness never
retries an op itself, so an increment is left in-doubt (counted in `started`, not
`acked`) when a client crashes mid-commit or a sustained outage exhausts
`NetBackend`'s retry budget and fails the transaction; conditional writes (CAS)
keep each in-doubt op applied at most once even when a retry re-delivers it. With
`FaultConfig` disabled, `started == acked == final == expected`, so the original
exact check is also asserted.

The in-doubt outcome that `NetBackend` surfaces (and the at-most-once contract it
exercises) is a backend-wide decision documented separately in
[ADR-009](009-in-doubt-conditional-writes.md); this ADR only covers how the
simulator provokes and observes it.

### Integration

- **tokio alias.** Every crate in the engine graph depends on
  `tokio = { package = "madsim-tokio" }`. Cloud backends (s3/gcs) use real
  tokio/reqwest/aws-sdk and cannot build under madsim, so they are excluded from
  the simulated build (`make test-sim` lists packages explicitly).
- **`--cfg madsim` is a known cfg.** Declared once in the workspace
  `[workspace.lints.rust]` (`check-cfg`) so `#[cfg(madsim)]` does not trip the
  `unexpected_cfgs` lint in normal builds.

### Resolving non-determinism seams

For a replay to be identical, every source of non-determinism must be a function
of the seed:

1. **`tokio_util` cannot run under madsim** (it depends on real tokio and is not
   redirected by the alias). It was used for `CancellationToken` (in `Ctx`,
   `Background`, and the deadlock guard) and `TaskTracker` (in `Background`).
   Both were replaced with in-house equivalents built on `tokio::sync`, which
   *is* madsim-mapped: `CancelToken` (a hierarchical token over
   `tokio::sync::Notify`) and a `JoinHandle` vector in `Background`. The public
   API is unchanged, so call sites were untouched.
2. **Randomness.** `rand 0.10` pulls `getrandom 0.4`, which madsim's `getrandom`
   patch does not cover, so we do not rely on it. Instead `TxId` prefixes are
   drawn from `madsim::rand` under `#[cfg(madsim)]` (a one-line shim in
   `txid.rs`), which is seeded by the runtime.
3. **Time.** `DbBuilder::deterministic_time` makes the monitor's `Clock` anchor a
   *fixed* wall-clock base to `tokio::time::Instant` (`Clock::anchored_at`).
   Since `TxId` timestamps come from this clock, transaction-log object keys
   become deterministic. (Default stays `Clock::real()` in production.)
4. **`HashMap` iteration order.** `std`'s `RandomState` is reseeded per process,
   so any commit-path slice built by iterating a `HashMap` would differ between
   runs. The four such sites now emit a path-sorted order: `Tx::collect_accesses`
   (writes/reads), `init_validation`, `collections_locks`, and
   `Locker::locked_paths`. This is harmless in production and is what makes the
   op stream below byte-identical.
5. **`tokio`'s own select/watch RNG.** madsim re-exports the real `select!`
   macro and `tokio::sync::watch`. Both randomize with `tokio`'s *thread-local*
   `FastRand` (`select!` picks a starting branch; `watch` picks a notifier
   shard), which madsim does not seed and which persists across runs on the same
   thread. That made scheduling depend on a hidden, run-position-dependent RNG —
   the dominant source of non-determinism once clients talked over the network.
   Two fixes remove the dependence: every long-lived `select!` in the engine
   uses `biased;` (a fixed poll order — also slightly better for prompt
   cancellation), and `CancelToken` is built on `tokio::sync::Notify` (which
   draws no randomness) rather than `watch` (whose sharded notifier does). After
   this, behavior is independent of `tokio`'s thread-local RNG.

### The self-check asserts a byte-identical backend-call stream

The self-check verifies more than a matching final result. Two different
interleavings can reach the same final state while issuing different backend
operations, so only an identical *operation stream* proves the schedule itself
replayed deterministically.

`RecordingBackend` (a new `Backend` middleware) appends a canonical binary
encoding of every forwarded call — method tag plus all argument bytes (path,
value, sorted tags, expected version, writer id) — to a shared ordered log, in
call-issue order. On the storage node it records every call that crosses the
network. The `concurrent_sim` integration test runs the same `(workload, seed)`
twice, each on its own `RecordingBackend(MemoryBackend)`, and asserts the two
logs are equal, reporting the first diverging index and both records on
mismatch. It checks this both with faults off and with the nemesis active
(`op_stream_is_byte_identical_with_faults`), since determinism must survive fault
injection. `Runtime::check_determinism` is kept as a second, cheaper guard on
the workload result.

## Consequences

- A failing schedule reproduces exactly from its libFuzzer input:
  `RUSTFLAGS="--cfg madsim" cargo +nightly fuzz run concurrent_tx <crash-file>`.
- `make test-sim` runs the whole engine suite (and the op-stream self-check)
  under the simulator; `make test` keeps the normal build as the default gate so
  production paths stay covered both ways.
- The `proptest_concurrent` test remains as a fast, non-madsim sanity check.
- Production builds are unaffected: `madsim` is only compiled under
  `#[cfg(madsim)]` / in the `fuzz` crate, and the `sim` harness is behind a
  `glassdb` feature that pulls in `arbitrary`.

### Layout

- `crates/glassdb/src/sim.rs` — the `Workload`/`Op`/`FaultConfig` model and the
  `run_and_assert[_with_faults]` / `run_and_record[_with_faults]` node harness
  and nemesis (feature `sim`).
- `crates/glassdb-backend/src/net.rs` — the RPC transport: `serve_backend`
  (server, applies every request) and `NetBackend` (client, bounded retry +
  in-doubt conversion for ambiguous conditional writes, see ADR-009).
  `#[cfg(madsim)]`.
- `crates/glassdb-backend/src/middleware/recording.rs` — `RecordingBackend`.
- The `CancelToken` referenced in the *Decision* above was later removed when cancellation throughout the engine became future-drop (`tokio::select!`, `JoinHandle::abort`). The remaining outside-the-future wakeup primitive is `tokio_util::sync::CancellationToken` (over `tokio::sync::Notify`), used in `JoinHandle::abort`, `Dedup::close`, and the sim crash nemesis. `tokio_util` is once again usable now that ADR-011 replaced madsim with the in-repo `DetExecutor`, which redirects only `tokio::spawn` and `tokio::time`.
- `crates/glassdb/tests/concurrent_sim.rs` — the op-stream self-checks (with and
  without faults), only under `--cfg madsim` + `--features sim`.
- `fuzz/` — the cargo-fuzz crate (its own workspace), target `concurrent_tx`,
  with a starter corpus.

## Notes / open items

- `cargo fuzz` builds with a sanitizer + coverage on nightly. It sets its own
  `RUSTFLAGS`, which overrides `[build] rustflags` in `fuzz/.cargo/config.toml`,
  so `--cfg madsim` is passed via the `RUSTFLAGS` environment variable instead
  (cargo-fuzz appends its sanitizer flags to it). `make fuzz` does this. The
  `config.toml` cfg still applies to plain `cargo build`/`cargo test` inside
  `fuzz/`.
- `db.rs` still times `Stats.tx_time` with `std::time::Instant`; it is not part
  of any assertion or of madsim's determinism log, so it is left as-is.
- This is the Rust-native counterpart to the Go byte-schedule fuzzer
  (`FuzzConcurrentTx`) and outcome test (`TestConcurrentTxDeterministicOutcome`),
  which served as inspiration only.
