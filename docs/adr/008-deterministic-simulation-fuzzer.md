# ADR-008: Deterministic simulation fuzzer (madsim)

## Status

Accepted

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
libFuzzer bytes ──arbitrary──▶ (u64 seed, Workload)
                                   │
        seed ─▶ Runtime::with_seed_and_config ──▶ block_on(run_and_assert(workload))
                                   │
                 N clients (RMW / multi-RMW / read-only) over one shared
                 MemoryBackend, run concurrently; assert each key's final
                 value == total committed increments. Violation ⇒ panic ⇒
                 libFuzzer crash + reproducing input.
```

Why madsim over a hand-rolled scheduler: it deterministically controls
`tokio::spawn`, `tokio::time`, and `tokio::sync`, across the existing tokio
surface (`select!`, `oneshot`/`Semaphore`/`mpsc`, `buffer_unordered`) with no
custom executor. It activates only under `RUSTFLAGS="--cfg madsim"`; in normal
builds the `madsim-tokio` alias re-exports real tokio, so production behavior is
unchanged.

### Integration

- **tokio alias.** Every crate in the engine graph depends on
  `tokio = { package = "madsim-tokio" }`. Cloud backends (s3/gcs) use real
  tokio/reqwest/aws-sdk and cannot build under madsim, so they are excluded from
  the simulated build (`make sim-test` lists packages explicitly).
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
   `tokio::sync::watch`) and a `JoinHandle` vector in `Background`. The public
   API is unchanged, so call sites were untouched.
2. **Randomness.** `rand 0.10` pulls `getrandom 0.4`, which madsim's `getrandom`
   patch does not cover, so we do not rely on it. Instead `TxId` prefixes are
   drawn from `madsim::rand` under `#[cfg(madsim)]` (a one-line shim in
   `txid.rs`), which is seeded by the runtime.
3. **Time.** `Options::deterministic_time` makes the monitor's `Clock` anchor a
   *fixed* wall-clock base to `tokio::time::Instant` (`Clock::anchored_at`).
   Since `TxId` timestamps come from this clock, transaction-log object keys
   become deterministic. (Default stays `Clock::real()` in production.)
4. **`HashMap` iteration order.** `std`'s `RandomState` is reseeded per process,
   so any commit-path slice built by iterating a `HashMap` would differ between
   runs. The four such sites now emit a path-sorted order: `Tx::collect_accesses`
   (writes/reads), `init_validation`, `collections_locks`, and
   `Locker::locked_paths`. This is harmless in production and is what makes the
   op stream below byte-identical.

### The self-check asserts a byte-identical backend-call stream

The self-check verifies more than a matching final result. Two different
interleavings can reach the same final state while issuing different backend
operations, so only an identical *operation stream* proves the schedule itself
replayed deterministically.

`RecordingBackend` (a new `Backend` middleware) appends a canonical binary
encoding of every forwarded call — method tag plus all argument bytes (path,
value, sorted tags, expected version, writer id) — to a shared ordered log, in
call-issue order. The `concurrent_sim` integration test runs the same
`(workload, seed)` twice, each on its own `RecordingBackend(MemoryBackend)`, and
asserts the two logs are equal, reporting the first diverging index and both
records on mismatch. `Runtime::check_determinism` is kept as a second, cheaper
guard on the workload result.

## Consequences

- A failing schedule reproduces exactly from its libFuzzer input:
  `RUSTFLAGS="--cfg madsim" cargo +nightly fuzz run concurrent_tx <crash-file>`.
- `make sim-test` runs the whole engine suite (and the op-stream self-check)
  under the simulator; `make test` keeps the normal build as the default gate so
  production paths stay covered both ways.
- The `proptest_concurrent` test remains as a fast, non-madsim sanity check.
- Production builds are unaffected: `madsim` is only compiled under
  `#[cfg(madsim)]` / in the `fuzz` crate, and the `sim` harness is behind a
  `glassdb` feature that pulls in `arbitrary`.

### Layout

- `crates/glassdb/src/sim.rs` — the `Workload`/`Op` model and the
  `run_and_assert` / `run_and_record` harness (feature `sim`).
- `crates/glassdb-backend/src/middleware/recording.rs` — `RecordingBackend`.
- `crates/glassdb/tests/concurrent_sim.rs` — the op-stream self-check (only
  under `--cfg madsim` + `--features sim`).
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
