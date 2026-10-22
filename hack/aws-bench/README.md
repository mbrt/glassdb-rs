# Performance benchmark tools

These tools run `perfbench` on real S3, plot its results, and compare result
sets. `perfbench` and its experiments are explained in
[crates/glassdb-bench-scale/README.md](../../crates/glassdb-bench-scale/README.md).

## Plots

### Canonical sweep plots

The canonical plots come from the `workers` and `affinity` experiments, in
`hack/aws-bench/out-sweeps/`. The crate README has the commands. Then:

```bash
uv run hack/aws-bench/plot_results.py --canonical
```

The canonical mode needs three clean, converged runs. Throughput shows the p50
of the throughput windows as a line and their p10 to p90 range as a band.
Latency shows the p50 as a line and the p50 to p90 range as a band. Both use the
median over the runs. Each affinity figure has one panel for each database
instance count, with all shapes. The four figures go to `hack/aws-bench/out-sweeps/`.

### Plotting any result file

`plot_results.py` also plots single result files, such as the ones in `out/`
from a real S3 run. It picks the figures from the experiment in each file and
writes them beside the file unless `--out` says otherwise:

```bash
uv run hack/aws-bench/plot_results.py hack/aws-bench/out/mixed.json \
  hack/aws-bench/out/contention.json
```

A file of a mixed experiment (`mixed`, `ci`, `workers`, or `affinity`) must
sweep exactly one of affinity or workers; it gets one panel for each conflict
level (`lo` and `hi`). A `contention` file shows the 100% overlap cells by
contended keys.
Shapes that did not converge are plotted with a warning. Both `plot_results.py`
and `compare.py` read result files through `perfbench_results.py`, which owns the
JSON schema.

## Comparing references

`compare_refs.sh` uses the same comparison driver as CI. It copies the
candidate's `glassdb-bench-scale` crate into disposable snapshots of both
revisions, then builds each against its own engine. It runs the `ci` and
`diagnostics` experiments. The current source tree is not
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
`mixed.json`, `contention.json`, or `inline-pressure.json`, as perfbench writes
them with `--output`; it compares the files that both directories have.

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

# Download the newest result JSON and bootstrap log.
hack/aws-bench/deploy.sh results

# Empty the result/benchmark bucket and remove all infrastructure.
hack/aws-bench/deploy.sh teardown
```

The default real-S3 run executes the `mixed` and `contention` experiments once,
with the longer real-S3 timeouts of `aws.toml` and 20 s for each contention
cell (`CONTENTION_DURATION`).
Set `EXPERIMENTS` to a comma-separated list of other experiments, `RUNS` and
`RUN_COOLDOWN` for the repetitions, and `PERFBENCH_CONFIG` to the path of a
configuration file that changes the built-in experiments. `AUTO_STOP=false`
keeps the instance running for interactive inspection.

The stack creates billable EC2, S3, and interface-endpoint resources. Auto-stop
halts compute after the run, but endpoints and stored objects continue billing
until `deploy.sh teardown` completes.
