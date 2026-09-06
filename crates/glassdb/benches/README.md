# Transaction diagnostics

`make bench-diagnostics` runs a small Criterion suite and prints a JSON cost
record on a line starting with `diagnostic-costs: `. CI reads this record from
the captured benchmark log; no output-path setting is needed. `make bench`
runs these cases and the wider transaction microbenchmarks. Local and CI revision comparisons use
`hack/aws-bench/compare-refs.sh`.

The selected cases are self-contained in `diagnostics.rs`. The existing
`transactions.rs` suite is independent. Select a target with Cargo; Criterion
filters within `diagnostics` skip setup and cost measurements for excluded cases:

```sh
cargo bench -p glassdb --bench diagnostics
cargo bench -p glassdb --bench diagnostics -- '^diagnostic/warm_read$'
cargo bench -p glassdb --bench transactions
```

## Conditions

All selected cases use the default engine policies and an in-memory backend
without injected latency or persistent cache. Both targets use the same 20×
model-clock setting: engine waits, expiry, and background schedules advance
20 times faster, while CPU work does not. Criterion still reports wall time.
Results compare revisions under this model, not production latency at 1×.
Inputs and preparation are controlled; timings and task scheduling are not
deterministic.

| Case | Condition | Transactions per iteration |
| --- | --- | ---: |
| `warm_read` | One key, 256-byte value, warmed client caches | 1 |
| `fresh_client_read` | Same contents; reopen client and collection before each read | 1 |
| `rmw_inline_1024` | One key; 1,024-byte value at the default inline limit | 1 |
| `rmw_external_1025` | One key; 1,025-byte value above that limit | 1 |
| `rmw_five_leaves` | One key in each of five collections; 256-byte values | 1 |
| `read_long_keys_large_collection` | 1,024 keys of 256 bytes; 256-byte values; warmed caches | 1 |
| `rmw_shared_leaf_three_transactions` | Three concurrent updates to distinct keys in one leaf through one Database | 3 |

Seeding uses a separate client that shuts down before measurement. The large
collection must complete at least one split and remain quiet for 1.2 seconds
before the measurement client opens. Its split count is recorded. Updates
reuse the same keys and retain value length, so timed loops do not grow the
tree. Fresh-client setup and collection opening are excluded from read timing;
this is not an end-to-end database startup measurement.

Criterion measures mean time per iteration. For the concurrent case, this is
completion time for all three transactions, not individual transaction latency.
Its samples cannot provide transaction p90; `perfbench` provides that metric.
Each case uses a 500 ms warmup and 20 flat samples over two seconds.
Current-thread Tokio creates a batching opportunity, not an exact batch-size
guarantee. Coordinator submissions and rounds record the combining achieved.

## Backend costs

The separate pass performs 30 iterations on its own prepared database. It
does not instrument Criterion timing. Requests, successful read-body bytes,
attempted write-body bytes, and coordinator counters are normalized by the
number of completed transactions (90 for the concurrent case).
The engine counts DELETE requests as writes; they add no write-body bytes.

Workload, shutdown, and combined windows are separate. Fresh-client reads
close their client after each measured read; other cases close after the pass.
Setup calls are excluded; background work that overlaps a measured window is
included, even if setup started it. Shutdown drains managed work but cancels GC
and split loops, so combined cost is not full lifecycle or reclamation cost.
Body bytes exclude paths, headers, LIST response bodies, and transport overhead.

There is no combined score or automatic performance gate. Timing results
depend on the host. Real-provider costs require separate measurements.
Exact protocol guarantees belong in integration/simulation tests, not timing
assertions. Fixture preparation and transaction-completion checks run with the
benchmarks, not through `make test-all`.

## Comparison artifacts

The driver copies the candidate's benchmark sources and Cargo benchmark
declarations into the baseline snapshot. It records harness identity, compiler
version, both resolved lockfile hashes, and fixed workload settings. Each
revision keeps its engine dependency graph; identical harnesses do not imply
identical engine dependencies.
Criterion 0.8.2 is pinned in the benchmark dependencies. The report reads its
private `estimates.json` format with validation; verify the reader when upgrading it.

Use a comparison of unchanged engine code to check noise before interpreting
small changes. Full artifacts are retained even when the report hides unchanged
rows. Three paired runs are an initial budget, not proof of statistical power.
