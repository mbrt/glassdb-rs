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
# The configuration files are in the environment, because the stack fetches
# only the binary, this script, and its environment file.
printf '%s\n' "$AWS_CONFIG" >aws.toml
if [[ -n "${PERFBENCH_CONFIG:-}" ]]; then
  printf '%s\n' "$PERFBENCH_CONFIG" >perfbench.toml
  common+=(--config=perfbench.toml)
fi

IFS=',' read -r -a experiments <<<"$EXPERIMENTS"
for experiment in "${experiments[@]}"; do
  extra=()
  if [[ "$experiment" == "contention" ]]; then
    extra=(--set="cell.measure.duration=\"$CONTENTION_DURATION\"")
  fi
  ./perfbench "${common[@]}" "${extra[@]}" --experiment="$experiment" \
    --output="out/$experiment.json"
done
