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
  # The bootstrap log is the only view of a run of hours, and it is not a
  # terminal, so it has no progress bar. The details of each cell show its
  # progress there.
  --verbose
)
# The configuration and experiment files are in the environment, because the
# stack fetches only the binary, this script, and its environment file.
printf '%s\n' "$AWS_CONFIG" >aws.toml
if [[ -n "${PERFBENCH_CONFIG:-}" ]]; then
  printf '%s\n' "$PERFBENCH_CONFIG" >perfbench.toml
  common+=(--config=perfbench.toml)
fi
mkdir -p files
base64 -d <<<"$EXPERIMENT_FILES" | tar -C files -xz

IFS=',' read -r -a experiments <<<"$EXPERIMENTS"
for experiment in "${experiments[@]}"; do
  # An experiment of real S3 replaces the local one with the same name.
  file="files/aws-bench/experiments/$experiment.toml"
  if [[ ! -f "$file" ]]; then
    file="files/perf/experiments/$experiment.toml"
  fi
  if [[ ! -f "$file" ]]; then
    echo "no experiment $experiment in hack/aws-bench/experiments" \
      "or hack/perf/experiments" >&2
    exit 1
  fi
  extra=()
  if [[ "$experiment" == "contention" ]]; then
    extra=(--set="cell.measure.duration=\"$CONTENTION_DURATION\"")
  fi
  ./perfbench "${common[@]}" "${extra[@]}" --experiment="$file" \
    --output="out/$experiment.json"
done
