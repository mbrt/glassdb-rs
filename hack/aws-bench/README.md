# Performance benchmark harness

`perfbench` is the single database-level performance runner. Its subcommands
share backend selection, simulated-time scaling, repetitions, cooldown, bounded
draining, and a versioned JSON result envelope:

```text
perfbench mixed
perfbench contention
perfbench inline-pressure
perfbench split-merge-fight
```

Raw backend latency remains in `backendbench`. Criterion owns controlled
timing diagnostics and their separate backend-cost measurement pass.

## Mixed workload

The `mixed` scenario runs four transaction shapes concurrently:

- `rwSingle`: one-key read-modify-write;
- `rwMany`: multi-key read-modify-write;
- `roSingle`: one-key serializable read;
- `roMulti`: multi-key serializable read.

Every open `Database` runs every shape and has a distinct home collection.
For each transaction it chooses its home with the configured affinity;
otherwise it chooses uniformly among all collections. The default
`0,25,50,75,100%` sweep ranges from no instance-specific preference to complete
instance isolation. The separate `lo` and `hi` modes vary the key pool within a
collection, keeping key contention independent from collection affinity.
`--workers-per-shape` and `--databases` also accept comma-separated sweeps.
The Database value is a limit: each cell opens the smaller of that limit and
its worker count, ensuring that every open database instance runs every shape.
Results record the limit, active database-instance count, and worker count
separately.

Each cell gets an isolated database namespace. A throwaway database instance
seeds every collection and observes its completed-split counter. Any change
resets the quiet timer. Fresh measurement database instances open only after
that counter stays unchanged for `--split-quiet`; failure to settle before
`--split-settle-timeout` fails the cell. Setup split count and settlement wall
time are included in each result.

```bash
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend=memory --delays=s3 --delay-scale=0.2 \
  --drain-timeout=90s --output=/tmp/mixed.json mixed \
  --modes=lo,hi --affinities=0,25,50,75,100 \
  --databases=4 --workers-per-shape=8 \
  --duration=2s --max-duration=60s --target-ci=0.1
```

Every shape runs until all shapes reach the requested throughput confidence
interval, or the cell reaches `--max-duration`. Capped shapes are marked
unconverged. Whole-cell results include backend operations and transaction
replays plus coordinator submissions, rounds, CAS retries, members per round, and
direct-path coverage, all derived from the public `Database::stats()` counters.

### Worker and affinity sweep plots

The canonical scale plots use the low-contention mixed workload and the local
S3 model. `--prefix-depth=3` gives each physical collection subtree
(`db/_c/<collection-id>`) an independent simulated S3 request-rate bucket;
database-wide transaction-record shards remain separate prefixes. Build once and
run the two grids:

```bash
cargo build --release -p glassdb-bench-scale --bin perfbench

target/release/perfbench \
  --backend=memory --delays=s3 --delay-scale=0.2 --prefix-depth=3 \
  --runs=3 --drain-timeout=90s \
  --output=hack/aws-bench/out-sweeps/workers.json \
  mixed --modes=lo --affinities=100 --databases=5 \
  --workers-per-shape="1,$(seq -s, 10 10 200)" \
  --duration=2s --max-duration=60s --target-ci=0.1

target/release/perfbench \
  --backend=memory --delays=s3 --delay-scale=0.2 --prefix-depth=3 \
  --runs=3 --drain-timeout=90s \
  --output=hack/aws-bench/out-sweeps/affinity.json \
  mixed --modes=lo --affinities=0,25,50,75,100 \
  --databases=1,3,5,7 --workers-per-shape=20 \
  --duration=2s --max-duration=60s --target-ci=0.1

uv run hack/aws-bench/plot-mixed-sweeps.py
```

The plotter requires three clean, converged runs. Throughput is shown as the
cross-run median line. Latency uses the cross-run median p50 as a line and the
area from p50 through p90 as a band. Affinity figures put all transaction shapes
on one panel per Database-instance count. The four figures are written under
`hack/aws-bench/out-sweeps/`.

## Focused scenarios

`contention` measures five overlapping multi-key RMW workers. `--keys=1`
selects the focused hot-key regression cell; omitting it sweeps one through six
keys and every overlap width.

```bash
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend=memory --delays=s3 --delay-scale=0.2 \
  --output=/tmp/contention.json contention --keys=1 --duration=2s
```

`inline-pressure` fills the aggregate inline budget of a leaf, then repeats
waves of transactions over the keys after it until one wave commits without
locks. It keeps a pinned inline policy and uses the default topology policy. It
reports direct commits, locking, backend operations and bytes, splits, and
merges for the saturation, pressure, and recovery phases. `--settle-timeout`
limits the pressure phase.

```bash
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend=memory --delays=s3 --delay-scale=0.2 \
  --output=/tmp/inline-pressure.json inline-pressure --settle-timeout=5s
```

`split-merge-fight` runs a stable load under which the topology policies of
database instances disagree. One collection has two keys in one leaf. Splitter
instances write one key each, so they lose leaf CASes to each other, and their
policy splits the leaf between the keys. A merger instance scans both keys, so
after the split its scans cross into the right leaf, and its policy merges the
leaves again. The scenario reports the splits and merges in the warmup and in
the measurement, the throughput and protocol counters of each instance, and
the splits and merges of each instance in each wall second of the warmup and
the measurement. The `fixed-1` and
`fixed-2` policies keep one or two leaves, to give the throughput of each
topology without structural changes.

```bash
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend=memory --delays=s3 --delay-scale=0.2 --runs=3 \
  --output=/tmp/split-merge-fight.json split-merge-fight
```

All subcommands support `--backend=memory|fakes3|s3|gcs`. Real S3 and GCS use
the bucket in `$BUCKET` and always run at real time. For `memory` and `fakes3`,
`--delay-scale` selects the inverse process-wide model-time speedup: backend
latency, rate limits, SDK and engine retries, leases, and background cadence
advance coherently, while measurement windows and drain deadlines remain wall
time. The `0.2` default keeps S3-profile sleeps above timer granularity; smaller
scales are useful only as explicitly approximate probes.

## Comparing references

`compare-refs.sh` uses the same comparison driver as CI. It copies the
candidate's benchmark sources into disposable snapshots of both revisions,
then builds each against its own engine. The current source tree is not
changed. Unsupported engine APIs fail the comparison; old workloads are not
silently substituted.

```bash
# main against the current worktree
hack/aws-bench/compare-refs.sh

# compatibility spelling for the same bounded comparison
hack/aws-bench/compare-refs.sh --summary

# explicit references
BASE=main TARGET=my-branch OUT=/tmp/glassdb-comparison \
  hack/aws-bench/compare-refs.sh
```

## Real S3 runner

The AWS harness preserves a private execution environment: an EC2 instance in
a VPC without Internet or NAT, an S3 gateway endpoint, SSM interface endpoints,
an encrypted result bucket, and no inbound access. CloudFormation owns only
that infrastructure and artifact bootstrap. `deploy.sh` owns workload choices
by uploading the binary, `run-perfbench.sh`, and a shell-escaped configuration.

Prerequisites are AWS credentials, AWS CLI v2, the Session Manager plugin for
live logs, and a musl toolchain matching `RUST_TARGET`.

```bash
# Build, provision, and upload the runner.
AWS_REGION=us-east-1 hack/aws-bench/deploy.sh deploy

# Follow bootstrap and benchmark output through SSM.
hack/aws-bench/deploy.sh logs

# Download the newest mixed/contention JSON and bootstrap log.
hack/aws-bench/deploy.sh results

# Empty the result/benchmark bucket and remove all infrastructure.
hack/aws-bench/deploy.sh teardown
```

The default real-S3 run executes the complete mixed affinity grid and the
contention matrix once. Set `RUNS`, `RUN_COOLDOWN`, the `MIX_*` variables, or
`CONTENTION_*` variables to tune it. `RUN_INLINE_PRESSURE=true` adds the focused
inline-pressure scenario. `AUTO_STOP=false` keeps the instance running for
interactive inspection.

The stack creates billable EC2, S3, and interface-endpoint resources. Auto-stop
halts compute after the run, but endpoints and stored objects continue billing
until `deploy.sh teardown` completes.
