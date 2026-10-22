# Real S3 runner

These scripts run `perfbench` on real S3 in AWS. `perfbench` and its
experiments are explained in
[crates/glassdb-bench-scale/README.md](../../crates/glassdb-bench-scale/README.md),
and the tools that plot and compare the results in
[hack/perf/README.md](../perf/README.md).

| File | Purpose |
| --- | --- |
| `deploy.sh` | Builds perfbench, provisions the stack, uploads the runner, and downloads results. |
| `cloudformation.yaml` | The infrastructure: the instance, its network, and the result bucket. |
| `run_perfbench.sh` | Runs the experiments on the instance. |
| `aws.toml` | The perfbench configuration layer with the real-S3 timeouts. |
| `out/` | The results of the last real-S3 run that the docs refer to. |

## Usage

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

To plot the downloaded results:

```bash
uv run hack/perf/plot_results.py hack/aws-bench/out/mixed.json \
  hack/aws-bench/out/contention.json
```
