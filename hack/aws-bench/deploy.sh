#!/usr/bin/env bash
#
# Deploy, inspect, and tear down the private real-S3 perfbench runner.
#
# Usage:
#   deploy.sh deploy        build perfbench, provision the stack, and upload it
#   deploy.sh logs          stream the bootstrap and benchmark log over SSM
#   deploy.sh results [ts]  download one result set (the newest by default)
#   deploy.sh teardown      empty the bucket and delete the complete stack
#
# `logs` requires the AWS Session Manager plugin. Every command requires AWS
# credentials and AWS CLI v2; AWS_REGION is optional when the CLI has a default.
#   https://docs.aws.amazon.com/systems-manager/latest/userguide/session-manager-working-with-install-plugin.html
#
# `deploy` cross-compiles a statically linked perfbench binary so it can run on
# the bare Amazon Linux 2023 instance. Install the target and a musl C toolchain
# first, for example:
#
#   rustup target add x86_64-unknown-linux-musl
#   sudo apt install musl-tools
#
# Configuration via environment variables (all optional):
#
# Infrastructure and local paths:
#   STACK_NAME       CloudFormation stack name       (default: glassdb-bench)
#   AWS_REGION       AWS region                      (default: AWS CLI config)
#   INSTANCE_TYPE    EC2 instance type               (default: c7i.8xlarge)
#   RUST_TARGET      Rust target triple
#                    (default: x86_64-unknown-linux-musl)
#   AUTO_STOP        stop the instance after the run (default: true)
#   OUT_DIR          downloaded result directory     (default: hack/aws-bench/out)
#   BINARY_S3_KEY    uploaded perfbench key           (default: bin/perfbench)
#   RUNNER_S3_KEY    uploaded runner-script key       (default: bin/run-perfbench)
#   CONFIG_S3_KEY    uploaded configuration key       (default: bin/perfbench.env)
#
# Shared benchmark lifecycle:
#   RUNS             repetitions of each scenario    (default: 1)
#   RUN_COOLDOWN     cooldown between repetitions    (default: 60s)
#   DRAIN_TIMEOUT    worker/shutdown deadline         (default: 90s)
#
# Experiments:
#   EXPERIMENTS      names of the experiments in hack/perf/experiments to run,
#                    comma-separated; each one writes out/<experiment>.json
#                    (default: mixed,contention)
#   PERFBENCH_CONFIG path of a perfbench configuration file that changes all
#                    experiments, after aws.toml (default: none). It cannot
#                    include other files. See
#                    crates/glassdb-bench-scale/README.md.
#   CONTENTION_DURATION measured window of each contention cell (default: 20s)
#
# The provisioned instance polls S3 for the three uploaded artifacts, runs the
# experiments, uploads versioned JSON and its bootstrap log, and optionally
# stops itself. User data
# runs only when the instance is first provisioned; tear down a completed stack
# before starting another run. The bucket and VPC endpoints continue to incur
# charges until `deploy.sh teardown` completes.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

STACK_NAME="${STACK_NAME:-glassdb-bench}"
INSTANCE_TYPE="${INSTANCE_TYPE:-c7i.8xlarge}"
RUST_TARGET="${RUST_TARGET:-x86_64-unknown-linux-musl}"
AUTO_STOP="${AUTO_STOP:-true}"
OUT_DIR="${OUT_DIR:-$SCRIPT_DIR/out}"
BINARY_S3_KEY="${BINARY_S3_KEY:-bin/perfbench}"
RUNNER_S3_KEY="${RUNNER_S3_KEY:-bin/run-perfbench}"
CONFIG_S3_KEY="${CONFIG_S3_KEY:-bin/perfbench.env}"

RUNS="${RUNS:-1}"
RUN_COOLDOWN="${RUN_COOLDOWN:-60s}"
DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-90s}"
EXPERIMENTS="${EXPERIMENTS:-mixed,contention}"
PERFBENCH_CONFIG="${PERFBENCH_CONFIG:-}"
CONTENTION_DURATION="${CONTENTION_DURATION:-20s}"

region_args=()
if [[ -n "${AWS_REGION:-}" ]]; then
  region_args=(--region "$AWS_REGION")
fi

stack_output() {
  aws cloudformation describe-stacks "${region_args[@]}" \
    --stack-name "$STACK_NAME" \
    --query "Stacks[0].Outputs[?OutputKey=='$1'].OutputValue" \
    --output text
}

# Fails unless each name of EXPERIMENTS is one experiment file, so that a wrong
# name fails before the stack starts to cost.
check_experiments() {
  local name names files
  IFS=',' read -r -a names <<<"$EXPERIMENTS"
  for name in "${names[@]}"; do
    files=("$REPO_ROOT"/hack/perf/experiments/*/"$name".toml)
    if [[ ${#files[@]} -ne 1 || ! -f "${files[0]}" ]]; then
      echo "EXPERIMENTS: $name is not one file in hack/perf/experiments" >&2
      return 1
    fi
  done
}

write_config() {
  local path="$1" name config="" files
  # aws.toml is always a layer, so it travels in the environment file too.
  printf 'AWS_CONFIG=%q\n' "$(<"$SCRIPT_DIR/aws.toml")" >"$path"
  for name in RUNS RUN_COOLDOWN DRAIN_TIMEOUT EXPERIMENTS CONTENTION_DURATION; do
    printf '%s=%q\n' "$name" "${!name}" >>"$path"
  done
  if [[ -n "$PERFBENCH_CONFIG" ]]; then
    config="$(<"$PERFBENCH_CONFIG")"
  fi
  printf 'PERFBENCH_CONFIG=%q\n' "$config" >>"$path"
  # The experiment files and the files that they include, with their layout,
  # so that the relative paths of `include` stay valid.
  files="$(tar -C "$REPO_ROOT/hack/perf" -cz configs experiments | base64 -w0)"
  printf 'EXPERIMENT_FILES=%q\n' "$files" >>"$path"
}

deploy() {
  local temp_dir binary config bucket
  temp_dir="$(mktemp -d)"
  trap 'rm -rf "$temp_dir"' EXIT
  binary="$temp_dir/perfbench"
  config="$temp_dir/perfbench.env"
  check_experiments

  echo ">> building static perfbench binary for $RUST_TARGET"
  cargo build --release --manifest-path "$REPO_ROOT/Cargo.toml" \
    --package glassdb-bench-scale --bin perfbench --target "$RUST_TARGET"
  cp "$REPO_ROOT/target/$RUST_TARGET/release/perfbench" "$binary"
  write_config "$config"

  echo ">> deploying stack $STACK_NAME"
  aws cloudformation deploy "${region_args[@]}" \
    --stack-name "$STACK_NAME" \
    --template-file "$SCRIPT_DIR/cloudformation.yaml" \
    --capabilities CAPABILITY_IAM \
    --parameter-overrides \
    "InstanceType=$INSTANCE_TYPE" \
    "AutoStop=$AUTO_STOP" \
    "BinaryS3Key=$BINARY_S3_KEY" \
    "RunnerS3Key=$RUNNER_S3_KEY" \
    "ConfigS3Key=$CONFIG_S3_KEY"

  bucket="$(stack_output BucketName)"
  echo ">> uploading perfbench artifacts to s3://$bucket/bin/"
  aws s3 cp "${region_args[@]}" "$binary" "s3://$bucket/$BINARY_S3_KEY"
  aws s3 cp "${region_args[@]}" "$SCRIPT_DIR/run_perfbench.sh" \
    "s3://$bucket/$RUNNER_S3_KEY"
  aws s3 cp "${region_args[@]}" "$config" "s3://$bucket/$CONFIG_S3_KEY"

  rm -rf "$temp_dir"
  trap - EXIT
  echo ">> runner started; stream it with: $0 logs"
  echo ">> download the completed artifacts with: $0 results"
}

logs() {
  local instance
  instance="$(stack_output InstanceId)"
  aws ssm start-session "${region_args[@]}" \
    --target "$instance" \
    --document-name AWS-StartInteractiveCommand \
    --parameters command="sudo tail -n +1 -F /var/log/perfbench-bootstrap.log"
}

results() {
  local requested="${1:-}" bucket prefix
  bucket="$(stack_output BucketName)"
  prefix="$requested"
  if [[ -z "$prefix" ]]; then
    prefix="$(aws s3 ls "${region_args[@]}" "s3://$bucket/results/" \
      | awk '/ PRE / {print $2}' | sort | tail -n1)"
  fi
  prefix="${prefix%/}"
  if [[ -z "$prefix" ]]; then
    echo "no benchmark results found" >&2
    return 1
  fi
  mkdir -p "$OUT_DIR"
  aws s3 cp "${region_args[@]}" "s3://$bucket/results/$prefix/" \
    "$OUT_DIR/" --recursive
  echo ">> downloaded results to $OUT_DIR"
}

teardown() {
  local bucket
  bucket="$(stack_output BucketName)"
  if [[ -n "$bucket" && "$bucket" != "None" ]]; then
    aws s3 rm "${region_args[@]}" "s3://$bucket" \
      --recursive --only-show-errors || true
  fi
  aws cloudformation delete-stack "${region_args[@]}" --stack-name "$STACK_NAME"
  aws cloudformation wait stack-delete-complete \
    "${region_args[@]}" --stack-name "$STACK_NAME"
}

case "${1:-deploy}" in
  deploy) deploy ;;
  logs) logs ;;
  results) results "${2:-}" ;;
  teardown) teardown ;;
  *)
    echo "usage: $0 {deploy|logs|results [timestamp]|teardown}" >&2
    exit 2
    ;;
esac
