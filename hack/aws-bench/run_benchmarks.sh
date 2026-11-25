#!/usr/bin/env bash

# Runs inside the provisioned EC2 instance. deploy.sh uploads a shell-escaped
# configuration beside this file, keeping workload knobs out of CloudFormation.
set -euo pipefail
# The deployment-generated configuration intentionally has no source-tree path.
# shellcheck source=/dev/null
source ./benchmarks.env

run_perfbench() {
  local common names experiment file extra
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
  # stack fetches only the binaries, this script, and its environment file.
  printf '%s\n' "$AWS_CONFIG" >aws.toml
  if [[ -n "${PERFBENCH_CONFIG:-}" ]]; then
    printf '%s\n' "$PERFBENCH_CONFIG" >perfbench.toml
    common+=(--config=perfbench.toml)
  fi
  mkdir -p files
  base64 -d <<<"$EXPERIMENT_FILES" | tar -C files -xz

  IFS=',' read -r -a names <<<"$EXPERIMENTS"
  for experiment in "${names[@]}"; do
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
}

run_backendbench() {
  ./backendbench --backend=s3 >out/backendbench.md
}

IFS=',' read -r -a names <<<"$BENCHMARKS"
for benchmark in "${names[@]}"; do
  case "$benchmark" in
    perfbench) run_perfbench ;;
    backendbench) run_backendbench ;;
    *)
      echo "no benchmark $benchmark" >&2
      exit 1
      ;;
  esac
done
