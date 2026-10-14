# Performance benchmark harness

`perfbench` is the single database-level performance runner. Its subcommands
share backend selection, simulated-time scaling, repetitions, cooldown, bounded
draining, and a versioned JSON result envelope:

```text
perfbench mixed
perfbench contention
perfbench inline-pressure
perfbench split-merge-fight
perfbench membership-churn
```

Raw backend latency remains in `backendbench`. Criterion owns controlled
timing diagnostics and their separate backend-cost measurement pass.

## Mixed workload

`mixed` runs four transaction shapes at the same time and reports the
throughput and latency of each:

- `rwSingle`: read-modify-write of one key.
- `rwMany`: read-modify-write of `--multi-keys` keys.
- `roSingle`: serializable read of one key.
- `roMulti`: serializable read of `--multi-keys` keys.

Each of these options takes a comma-separated list. Every combination of the
values is one cell:

- `--modes`: `lo` gives each worker its own keys, so that transactions rarely
  conflict. `--shared-pct` percent of the transactions use any key instead.
  `hi` makes all workers share `--hot-keys` keys.
- `--affinities`: the percentage of transactions that a database instance runs
  on its own home collection. The other transactions use a random collection.
- `--databases`: the maximum number of database instances in a cell. A cell
  never opens more instances than it has workers per shape.
- `--workers-per-shape`: the concurrent workers of each shape, spread over the
  instances.

In the `lo` mode, `--num-keys` must give each worker at least `--multi-keys`
keys. The benchmark rejects a smaller pool.

```bash
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend=memory --delays=s3 --delay-scale=0.2 \
  --drain-timeout=90s --output=/tmp/mixed.json mixed \
  --modes=lo,hi --affinities=0,25,50,75,100 \
  --databases=4 --workers-per-shape=8 \
  --duration=2s --max-duration=60s --window-commits=50
```

Run `perfbench mixed --help` for all options and their defaults.

### Setup, warm-up, and measurement

Before the cells run, perfbench writes all keys and waits until splits, merges,
and garbage collection stop for `--quiet-period`. If that takes longer than
`--seed-timeout`, the run fails. With `--backend=memory`, this happens once for
each mode, and each cell gets its own copy of the result. With other backends,
each cell writes its keys again.

Each cell then runs its load without measuring it. The warm-up lasts at least
`--warmup-minimum`, and then until splits and merges stop for `--quiet-period`,
or until `--warmup-timeout`. Under load, the tree often keeps splitting, so a
loaded cell usually measures after the timeout. The cell result records the
splits and merges of the warm-up. `hack/perf/investigations.md` explains why the
splits continue.

The measurement runs for at least `--duration`. perfbench splits it into 20
equal throughput windows. The measurement ends when every shape has committed
`--window-commits` per window on average, or at `--max-duration`. A shape that
reaches `--max-duration` first is marked unconverged. Its windows have fewer
commits, so its throughput distribution is noisier. The commits of the
transactions that finish after the measurement are not in a window. Each shape
reports its window counts, and `relCi` is the throughput 95% confidence interval
from their batch means. The result of each cell also includes backend operations
and coordinator counters.

### Worker and affinity sweep plots

The canonical plots use the `lo` mode and the local S3 model. `--prefix-depth=3`
gives each collection its own simulated S3 request-rate limit. The worker grid
uses 20,000 keys, so that each of the 800 workers at 200 workers per shape gets
25 keys. Build once and run the two grids:

```bash
cargo build --release -p glassdb-bench-scale --bin perfbench

target/release/perfbench \
  --backend=memory --delays=s3 --delay-scale=0.2 --prefix-depth=3 \
  --runs=3 --drain-timeout=90s \
  --output=hack/aws-bench/out-sweeps/workers.json \
  mixed --modes=lo --affinities=100 --databases=5 \
  --workers-per-shape="1,$(seq -s, 10 10 200)" --num-keys=20000 \
  --quiet-period=5s --warmup-timeout=180s \
  --duration=2s --max-duration=60s --window-commits=50

target/release/perfbench \
  --backend=memory --delays=s3 --delay-scale=0.2 --prefix-depth=3 \
  --runs=3 --drain-timeout=90s \
  --output=hack/aws-bench/out-sweeps/affinity.json \
  mixed --modes=lo --affinities=0,25,50,75,100 \
  --databases=1,3,5,7 --workers-per-shape=20 \
  --duration=2s --max-duration=60s --window-commits=50

uv run hack/aws-bench/plot_results.py --canonical
```

The canonical mode needs three clean, converged runs. Throughput shows the p50
of the throughput windows as a line and their p10 to p90 range as a band.
Latency shows the p50 as a line and the p50 to p90 range as a band. Both use the
median over the runs. Each affinity figure has one panel for each database
instance count, with all shapes. The four figures go to `hack/aws-bench/out-sweeps/`.

### Plotting any result file

`plot_results.py` also plots single result files, such as the ones in `out/`
from a real S3 run. It picks the figures from the scenario in each file and
writes them beside the file unless `--out` says otherwise:

```bash
uv run hack/aws-bench/plot_results.py hack/aws-bench/out/mixed.json \
  hack/aws-bench/out/contention.json
```

A `mixed` file must sweep exactly one of affinity or workers; it gets one panel
per mode. A `contention` file shows the 100% overlap cells by contended keys.
Shapes that did not converge are plotted with a warning. Both `plot_results.py`
and `compare.py` read result files through `perfbench_results.py`, which owns the
JSON schema.

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

`membership-churn` changes the key membership of one leaf while other
transactions scan it. Each role runs in its own database instance. A direct
churner creates and deletes keys with single-key transactions, which commit
direct. A locked churner does the same after a scan in the same transaction,
so it takes key locks and the membership lock of the leaf. A scanner scans the
whole collection. Each role can have zero workers, to measure the others
alone. The results vary much from run to run, so compare at least 4 runs.

```bash
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend=memory --delays=s3 --delay-scale=0.2 --runs=4 \
  --output=/tmp/membership-churn.json membership-churn
```

All subcommands support `--backend=memory|fakes3|s3|gcs`. Real S3 and GCS use
the bucket in `$BUCKET` and always run at real time. For `memory` and `fakes3`,
`--delay-scale` selects the inverse process-wide model-time speedup: backend
latency, rate limits, SDK and engine retries, leases, and background cadence
advance coherently, while measurement windows and drain deadlines remain wall
time. The `0.2` default keeps S3-profile sleeps above timer granularity; smaller
scales are useful only as explicitly approximate probes.

## Comparing references

`compare_refs.sh` uses the same comparison driver as CI. It copies the
candidate's benchmark sources into disposable snapshots of both revisions,
then builds each against its own engine. The current source tree is not
changed. Unsupported engine APIs fail the comparison; old workloads are not
silently substituted.

```bash
# main against the current worktree
hack/aws-bench/compare_refs.sh

# compatibility spelling for the same bounded comparison
hack/aws-bench/compare_refs.sh --summary

# explicit references
BASE=main TARGET=my-branch OUT=/tmp/glassdb-comparison \
  hack/aws-bench/compare_refs.sh
```

### Comparing result directories

To compare result files that already exist, such as a real S3 run against a
local model run, call `compare.py` directly. Each directory holds
`mixed.json`, `contention.json`, or `inline-pressure.json`; it compares the
files that both directories have.

```bash
hack/aws-bench/compare.py --a out --label-a s3 --b out-fake --label-b model \
  --out /tmp/cmp-plots --summary-out /tmp/cmp-plots/summary.md
```

It prints tables of the ratio b/a and a verdict for each metric, and draws one
set of overlay figures per scenario: `cmp-mixed-*.png` and
`cmp-contention-latency.png`. Draw the overlays alone with
`plot_results.py --out DIR --series s3=out/mixed.json --series model=out-fake/mixed.json`.

## Real S3 runner

The AWS harness preserves a private execution environment: an EC2 instance in
a VPC without Internet or NAT, an S3 gateway endpoint, SSM interface endpoints,
an encrypted result bucket, and no inbound access. CloudFormation owns only
that infrastructure and artifact bootstrap. `deploy.sh` owns workload choices
by uploading the binary, `run_perfbench.sh`, and a shell-escaped configuration.

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
