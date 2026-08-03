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
)

./perfbench "${common[@]}" --output=out/mixed.json mixed \
  --modes="$MIX_MODES" --affinities="$MIX_AFFINITIES" \
  --databases="$MIX_DATABASES" --workers-per-shape="$MIX_WORKERS" \
  --num-keys="$MIX_NUM_KEYS" --hot-keys="$MIX_HOT_KEYS" \
  --multi-keys="$MIX_MULTI_KEYS" --duration="$MIX_DURATION" \
  --max-duration="$MIX_MAX_DURATION" --target-ci="$MIX_TARGET_CI" \
  --split-quiet="$MIX_SPLIT_QUIET" \
  --split-settle-timeout="$MIX_SPLIT_SETTLE_TIMEOUT"

./perfbench "${common[@]}" --output=out/contention.json contention \
  --keys="$CONTENTION_KEYS" --duration="$CONTENTION_DURATION"

if [[ "$RUN_INLINE_PRESSURE" == "true" ]]; then
  ./perfbench "${common[@]}" --output=out/inline-pressure.json \
    inline-pressure --settle-timeout="$INLINE_PRESSURE_SETTLE_TIMEOUT"
fi
