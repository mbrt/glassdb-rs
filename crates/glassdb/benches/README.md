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
wrapped in `DelayBackend`, with the `s3_delays()` profile and no persistent cache.
Provider latencies have zero variance; the S3 throttling limits remain enabled.
The profile uses 22 ms for object reads and LIST, and 55 ms for writes and DELETE,
in model time. Both revisions use a 5× model clock, as does the mixed workload:
backend delays, engine waits, expiry, and background schedules advance five times
faster, while CPU work does not. Nominal wall delays are 4.4 ms and 11 ms;
Tokio timer rounding and host scheduling can extend them.
Criterion reports wall time. These results compare revisions under the model;
they do not predict production latency at 1×. Fixed provider delays remove
random latency variance, but do not make task scheduling deterministic.

Local sweeps of 1×, 2×, 5×, 10×, 20× and 40× favored 5× for these short cases.
Higher speeds increased the read-to-write delay ratio; 20× and 40× also brought
reclamation past the default 45-model-second GC horizon into the timed runs.
Except for fresh-client reads, fixtures warm caches before the warmup. A 250 ms
warmup retained the measured PR effects while keeping the eight-case runtime close to the former
undelayed harness. Clock and warmup changes require checking request counts,
background work, measurement variance, and total process time together.

| Case | Condition | Transactions per iteration |
| --- | --- | ---: |
| `warm_read` | One key, 256-byte value, warmed client caches | 1 |
| `warm_read_external` | One key, 1,025-byte value stored in a transaction log, warmed client caches | 1 |
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
Each case requests a 250 ms warmup and 20 flat samples with a two-second
measurement target.
Current-thread Tokio creates a batching opportunity, not an exact batch-size
guarantee. Coordinator submissions and rounds record the combining achieved.

## Backend costs

Costs come from the same fixture and iterations as the Criterion run, including
warmup. Criterion does not expose the warmup/sample boundary to `iter_custom`,
so costs describe the whole run, while the timing estimate uses sampled iterations.
There is no separate short cost pass. Requests, successful read-body bytes,
attempted write-body bytes, and coordinator counters are normalized by the
number of completed transactions. Successful read and attempted write bodies
are counted in the measured backend. Other counters are captured outside each
sample's timer (each read's timer for fresh-client reads).
The engine counts DELETE requests as writes; they add no write-body bytes.

Workload, shutdown, and combined windows are separate. Fresh-client reads
close their client after each measured read; other cases close after the benchmark.
Setup calls are excluded; background work that overlaps a measured window is
included, even if setup started it. Shutdown drains managed work but cancels GC
and split loops, so combined cost is not full lifecycle or reclamation cost.
Body bytes exclude paths, headers, LIST response bodies, and transport overhead.

There is no combined score or automatic performance gate. Timing results
depend on the host. Real-provider costs require separate measurements.
Exact protocol guarantees belong in integration/simulation tests, not timing
assertions. Fixture preparation, transaction completion, and zero backend reads
for warmed inline writes are checked by the benchmark harness. The inline-write
and transaction-log cache checks use separate, undelayed memory backends.
The inline-write check freezes model time to exclude GC deadlines. `make test-all`
runs all benchmark targets in test mode, including these checks.

## Comparison artifacts

The driver copies the candidate's benchmark sources and Cargo benchmark
declarations into the baseline snapshot. It records harness identity, compiler
version, executable hashes, both resolved lockfile hashes, and workload settings. Each
revision keeps its engine dependency graph; identical harnesses do not imply
identical engine dependencies.
New comparison manifests record the diagnostic delay model, warmup, and cost window.
The report rejects diagnostic timing as well as costs when that metadata is
missing or differs. Legacy comparison artifacts remain readable with their
original manifest; they cannot enter a new delayed comparison. Comparisons with
an explicit historical `--candidate` require a driver compatible with that
candidate's harness. This driver expects the delayed harness for new comparisons.
Criterion 0.8.2 is pinned in the benchmark dependencies. The report reads its
private `estimates.json` format with validation; verify the reader when upgrading it.

Use a comparison of unchanged engine code to check noise before interpreting
small changes. Full artifacts are retained even when the report hides unchanged
rows. CI checks each benchmark after 8, 16, and 32 back-to-back process pairs,
with balanced revision order at every checkpoint. Resolved benchmarks stop;
unresolved benchmarks receive more pairs until the final checkpoint at 32.
On Linux, diagnostics use one fixed CPU to limit migration. Mixed measurements
use a fixed pool of up to four CPUs, matching the CI runner's runtime width.
Each process has a two-minute timeout to catch hangs; elapsed time across
completed processes does not stop sampling.
The report uses variation between pairs to
test statistical significance separately from the 2% effect threshold, with
Bonferroni correction across all planned timing metrics and checkpoints,
including unused checks. A process timeout fails the comparison while preserving
verdicts from completed checkpoints. The two-second diagnostic
measurement window, model clock, and background work remain part of the workload.
The mixed workload uses fixed S3 mean latencies, retains throttling, and runs
for ten seconds. Random provider delays would add sampling noise unrelated to
the code change. It reports transaction p50,
p90, and throughput. All four transaction shapes run until all twelve metrics
are resolved or a limit is reached. Its intervals retain one observation per process pair;
pooling transactions as independent samples would hide variation between runs.
