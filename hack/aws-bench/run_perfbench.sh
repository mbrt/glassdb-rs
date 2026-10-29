#!/usr/bin/env bash

# Runs inside the provisioned EC2 instance. deploy.sh uploads a shell-escaped
# configuration beside this file, keeping workload knobs out of CloudFormation.
set -euo pipefail
# The deployment-generated configuration intentionally has no source-tree path.
# shellcheck source=/dev/null
source ./perfbench.env

common=(
  --backend=s3
  --runs="$RUNS"
  --run-cooldown="$RUN_COOLDOWN"
  --drain-timeout="$DRAIN_TIMEOUT"
  --config=aws.toml
)
# The configuration and experiment files are in the environment, because the
# stack fetches only the binary, this script, and its environment file.
printf '%s\n' "$AWS_CONFIG" >aws.toml
if [[ -n "${PERFBENCH_CONFIG:-}" ]]; then
  printf '%s\n' "$PERFBENCH_CONFIG" >perfbench.toml
  common+=(--config=perfbench.toml)
fi
mkdir -p perf
base64 -d <<<"$EXPERIMENT_FILES" | tar -C perf -xz

IFS=',' read -r -a experiments <<<"$EXPERIMENTS"
for experiment in "${experiments[@]}"; do
  files=(perf/experiments/*/"$experiment".toml)
  if [[ ! -f "${files[0]}" ]]; then
    echo "no experiment $experiment in hack/perf/experiments" >&2
    exit 1
  fi
  extra=()
  if [[ "$experiment" == "contention" ]]; then
    extra=(--set="cell.measure.duration=\"$CONTENTION_DURATION\"")
  fi
  ./perfbench "${common[@]}" "${extra[@]}" --experiment="${files[0]}" \
    --output="out/$experiment.json"
done
