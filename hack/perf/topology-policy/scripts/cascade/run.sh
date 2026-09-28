#!/usr/bin/env bash
# Live net-passes runs in hi mode with 8 databases, with all windows traced
# from the start: no warmup, and the live policy also runs as a shadow, so the
# trace has its decisions.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-conflicts
P=net-passes:0.05:0.1:0.25:10
run() {
  local name=$1
  shift
  "$B" mixed --delays=s3 --workers-per-shape=8 --modes=hi --databases=8 \
    --policies=$P --shadow-policies=$P --trace-windows \
    --warmup=0s --duration=40s --max-duration=40s --target-ci=0 \
    --split-quiet=5s --split-settle-timeout=300s \
    "$@" --output="$name.json" >"$name.log" 2>&1 &
}
run hi-db8-a0 --affinities=0
run hi-db8-a50 --affinities=50
wait
