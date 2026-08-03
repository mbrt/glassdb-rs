# Performance benchmark harness

`perfbench` is the single database-level performance runner. Its subcommands
share backend selection, simulated-time scaling, repetitions, cooldown, bounded
draining, and a versioned JSON result envelope:

```text
perfbench mixed
perfbench contention
perfbench inline-pressure
```

Raw backend latency remains in `backendbench`; Criterion owns microbenchmarks;
`autoresearch` owns the deterministic backend-operation cost score.

## Mixed workload

The `mixed` scenario runs four transaction shapes concurrently:

- `rwSingle`: one-key read-modify-write;
- `rwMany`: multi-key read-modify-write;
- `roSingle`: one-key serializable read;
- `roMulti`: multi-key serializable read.

Every client `Database` runs every shape and has a distinct home collection.
For each transaction it chooses its home with the configured affinity;
otherwise it chooses uniformly among all collections. The default
`0,25,50,75,100%` sweep ranges from no client-specific preference to complete
client isolation. The separate `lo` and `hi` modes vary the key pool within a
collection, keeping key contention independent from collection affinity.

Each cell gets an isolated database namespace. A throwaway client seeds every
collection and observes its completed-split counter. Any change resets the
quiet timer. Fresh measurement clients open only after that counter stays
unchanged for `--split-quiet`; failure to settle before
`--split-settle-timeout` fails the cell. Setup split count and settlement wall
time are included in each result.

```bash
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend=memory --delays=s3 --delay-scale=0.02 \
  --drain-timeout=90s --output=/tmp/mixed.json mixed \
  --modes=lo,hi --affinities=0,25,50,75,100 \
  --databases=4 --workers-per-shape=8 \
  --duration=2s --max-duration=60s --target-ci=0.1
```

Every shape runs until all shapes reach the requested throughput confidence
interval, or the cell reaches `--max-duration`. Capped shapes are marked
unconverged.

## Focused scenarios

`contention` measures five overlapping multi-key RMW workers. `--keys=1`
selects the focused hot-key regression cell; omitting it sweeps one through six
keys and every overlap width.

```bash
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend=memory --delays=s3 --delay-scale=0.02 \
  --output=/tmp/contention.json contention --keys=1 --duration=2s
```

`inline-pressure` retains the pinned ADR-056 phase sequence and policy. It
reports direct commits, locking, backend operations and bytes, and ordinary and
pressure-specific split outcomes per phase.

```bash
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend=memory --delays=s3 --delay-scale=0.02 \
  --output=/tmp/inline-pressure.json inline-pressure --settle-timeout=5s
```

All subcommands support `--backend=memory|fakes3|s3|gcs`. Real S3 and GCS use
the bucket in `$BUCKET`; simulated backends compensate reported throughput and
latency for `--delay-scale`.

## Comparing references

`compare-refs.sh` builds each reference in its own worktree, alternates paired
focused runs, runs the adaptive mixed sweep, and appends target/base ratios to
`out-refs/summary.md`:

```bash
# main against the current worktree
hack/aws-bench/compare-refs.sh

# smaller windows, no plots
hack/aws-bench/compare-refs.sh --summary

# explicit references
BASE=main TARGET=my-branch LABEL_A=main LABEL_B=candidate \
  hack/aws-bench/compare-refs.sh
```

Current references use `perfbench`. Historical references are run through their
retired `mixbench` and `rtbench` binaries, and `compare.py` retains readers for
their JSON and CSV artifacts. The mixed grid is compared only when both sides
support the same affinity workload; the old `shared/per-shape` grid is not
silently paired with it.

Common knobs include `MIX_MODES`, `MIX_AFFINITIES`, `MIX_DATABASES`,
`MIX_WORKERS`, `MIX_NUM_KEYS`, `MIX_HOT_KEYS`, `MIX_MULTI_KEYS`,
`MIX_SPLIT_QUIET`, `MIX_SPLIT_SETTLE_TIMEOUT`, `MIX_DURATION`,
`MIX_MAX_DURATION`, `MIX_TARGET_CI`, `NUM_RUNS`, `DRAIN_TIMEOUT`,
`CONTENTION_DURATION`, and `COMMAND_TIMEOUT`.

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
