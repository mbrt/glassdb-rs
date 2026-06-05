# Autoresearch loop for glassdb-rs

An autonomous performance-research loop in the spirit of
[karpathy/autoresearch](https://github.com/karpathy/autoresearch), driven by the
[Cursor CLI](https://cursor.com/docs/cli). A single long-lived `cursor-agent`
session self-loops, guided entirely by [`program.md`](program.md): it forms a
hypothesis, edits the glassdb-rs implementation, proves correctness, measures a
score, then keeps or discards the change - and repeats.

You program the loop by editing `program.md`, not by touching the database code.

This is the Rust port of the Go `hack/autoresearch` loop. The mechanics are the
same; the harness, gate, and frozen-file set are adapted to the Cargo workspace
and the `madsim` serializability fuzzer.

## Pieces

| File | Role | Editable by the agent? |
|------|------|------------------------|
| [`program.md`](program.md) | The instructions the agent follows (the "brain") | No - the human edits this |
| [`crates/glassdb-bench-score`](../../crates/glassdb-bench-score) | Lightweight in-memory benchmark crate; the `autoresearch` binary is the scoring harness that defines the primary + secondary metrics (`glassdb-bench-scale` holds the separate cloud throughput tools) | No (whole crate off-limits) |
| [`check.sh`](check.sh) | Correctness gate (workspace tests + serializability fuzzer) | No (off-limits) |
| [`evaluate.sh`](evaluate.sh) + [`evaluator.md`](evaluator.md) | Read-only judge sub-agent enforcing the off-limits set | No (off-limits) |
| Verification/oracle tests (`crates/glassdb/src/sim.rs`, `crates/glassdb/tests/{concurrent_sim,proptest_concurrent,integration}.rs`, `crates/glassdb/benches/transactions.rs`, `fuzz/**`) | The correctness contract | No (off-limits) |
| `log.md` | The running experiment log (the "morning log") | Yes - appended every experiment (kept or discarded) |
| `baseline.json`, `best.json` | Baseline and best-kept scores (gitignored) | Yes - bookkeeping |

The implementation files (everything under `crates/**/src/**`, except the frozen
`glassdb-bench-score`/`glassdb-bench-scale` crates) are what the agent actually
optimizes; changes may be large and span multiple files. The
agent may also rewrite the **unit tests** that live alongside that code (inline
`#[cfg(test)]` modules and non-frozen `crates/**/tests/` files) to match. Only
the verification/oracle tests and the infrastructure above are frozen.

## The metric

The primary score is a weighted count of backend operations per transaction
(object/metadata reads and writes), aggregated as a geomean over a fixed suite
of single-client workloads. It is deterministic and reflects the real cost
driver - object-storage round-trips. Secondary axes (memory, CPU/runtime) are
tie-breakers. Lower is better.

```bash
cargo run --release -p glassdb-bench-score --bin autoresearch -- --json --count 3   # machine-readable median
cargo run --release -p glassdb-bench-score --bin autoresearch                       # human-readable table
```

(Go's `mutexWaitNsPerTx` axis is dropped: Rust's std exposes no portable
mutex-contention metric.)

## Correctness

Every experiment must pass the gate before it can be kept; this is what protects
strict serializability. The fast tier builds the workspace and runs
`cargo test --workspace` (which includes the `proptest_concurrent`
serializability property test). The full tier additionally runs `make test`
(fmt + `clippy -D warnings`), `make sim-test` (the `madsim` determinism /
serializability / fault-injection self-checks), and the deterministic
concurrency fuzzer, so the correctness contract holds even when the
implementation and its unit tests change substantially.

```bash
hack/autoresearch/check.sh          # fast: build + workspace tests
hack/autoresearch/check.sh --full   # full: make test + sim-test + fuzz (before keeping)
```

Tunable via `FUZZTIME` / `FULL_FUZZTIME` (seconds). Set `RUN_FAST_FUZZ=1` to add
a short fuzz to the fast tier.

## Running it

Prerequisites: `cursor-agent` authenticated (`cursor-agent login`), a stable
Rust toolchain, and `jq`. The full gate's fuzzer additionally needs the
**nightly** toolchain and `cargo-fuzz` (`cargo install cargo-fuzz`). Work on a
dedicated branch (`autoresearch`) so kept experiments accumulate as commits.

### Interactive (recommended to start)

```bash
cursor-agent --force
```

Then prompt:

```
Follow hack/autoresearch/program.md exactly. Do the setup, then run the full experiment budget:
25 experiments or 3 hours, whichever comes first, and do not stop early.
Keep only improvements; log every experiment.
```

### Headless / unattended

```bash
cursor-agent -p --force \
  "Follow hack/autoresearch/program.md exactly. Do the setup, then run the full experiment budget:
  25 experiments or 3 hours, whichever comes first, and do not stop early.
  Keep only improvements; log every experiment."
```

Resume a previous session to keep going:

```bash
cursor-agent --resume        # pick a session, or
cursor-agent -p --continue "Continue the autoresearch loop."
```

You wake up to a series of commits (the kept improvements) plus `log.md`, which
explains what was tried and why.
