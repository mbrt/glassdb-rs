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
# Mixed-workload affinity sweep:
#   MIX_MODES        contention modes                 (default: lo,hi)
#   MIX_AFFINITIES   home-affinity percentages       (default: 0,25,50,75,100)
#   MIX_DATABASES    clients and home collections    (default: 4)
#   MIX_WORKERS      workers per transaction shape   (default: 8)
#   MIX_NUM_KEYS     keys per low-contention home    (default: 5000)
#   MIX_HOT_KEYS     keys per high-contention home   (default: 8)
#   MIX_MULTI_KEYS   keys per multi-key transaction  (default: 10)
#   MIX_DURATION     minimum measured cell window    (default: 5s)
#   MIX_MAX_DURATION maximum measured cell window    (default: 120s)
#   MIX_TARGET_CI    throughput CI relative half-width (default: 0.1)
#   MIX_SPLIT_QUIET  required stable split interval  (default: 10s)
#   MIX_SPLIT_SETTLE_TIMEOUT setup convergence limit (default: 120s)
#
# Focused scenarios:
#   CONTENTION_KEYS     key counts to sweep           (default: 1,2,3,4,5,6)
#   CONTENTION_DURATION measured window per cell      (default: 20s)
#   RUN_INLINE_PRESSURE run the ADR-056 scenario       (default: false)
#   INLINE_PRESSURE_SETTLE_TIMEOUT split wait limit   (default: 30s)
#
# The provisioned instance polls S3 for the three uploaded artifacts, runs the
# mixed and contention scenarios (plus inline-pressure when enabled), uploads
# versioned JSON and its bootstrap log, and optionally stops itself. User data
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
MIX_MODES="${MIX_MODES:-lo,hi}"
MIX_AFFINITIES="${MIX_AFFINITIES:-0,25,50,75,100}"
MIX_DATABASES="${MIX_DATABASES:-4}"
MIX_WORKERS="${MIX_WORKERS:-8}"
MIX_NUM_KEYS="${MIX_NUM_KEYS:-5000}"
MIX_HOT_KEYS="${MIX_HOT_KEYS:-8}"
MIX_MULTI_KEYS="${MIX_MULTI_KEYS:-10}"
MIX_DURATION="${MIX_DURATION:-5s}"
MIX_MAX_DURATION="${MIX_MAX_DURATION:-120s}"
MIX_TARGET_CI="${MIX_TARGET_CI:-0.1}"
MIX_SPLIT_QUIET="${MIX_SPLIT_QUIET:-10s}"
MIX_SPLIT_SETTLE_TIMEOUT="${MIX_SPLIT_SETTLE_TIMEOUT:-120s}"
CONTENTION_KEYS="${CONTENTION_KEYS:-1,2,3,4,5,6}"
CONTENTION_DURATION="${CONTENTION_DURATION:-20s}"
RUN_INLINE_PRESSURE="${RUN_INLINE_PRESSURE:-false}"
INLINE_PRESSURE_SETTLE_TIMEOUT="${INLINE_PRESSURE_SETTLE_TIMEOUT:-30s}"

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

write_config() {
  local path="$1" name
  : >"$path"
  for name in \
    RUNS RUN_COOLDOWN DRAIN_TIMEOUT MIX_MODES MIX_AFFINITIES MIX_DATABASES \
    MIX_WORKERS MIX_NUM_KEYS MIX_HOT_KEYS MIX_MULTI_KEYS MIX_DURATION \
    MIX_MAX_DURATION MIX_TARGET_CI MIX_SPLIT_QUIET MIX_SPLIT_SETTLE_TIMEOUT \
    CONTENTION_KEYS CONTENTION_DURATION RUN_INLINE_PRESSURE \
    INLINE_PRESSURE_SETTLE_TIMEOUT; do
    printf '%s=%q\n' "$name" "${!name}" >>"$path"
  done
}

deploy() {
  local temp_dir binary config bucket
  temp_dir="$(mktemp -d)"
  trap 'rm -rf "$temp_dir"' EXIT
  binary="$temp_dir/perfbench"
  config="$temp_dir/perfbench.env"

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
  aws s3 cp "${region_args[@]}" "$SCRIPT_DIR/run-perfbench.sh" \
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
