#!/usr/bin/env bash
# Traced live runs with the conflict time of the transactions over two adjacent
# leaves on the merge side.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-crossing2
run() {
  local name=$1 p=$2
  shift 2
  "$B" mixed --delays=s3 --workers-per-shape=8 --databases=8 \
    --policies=$p --shadow-policies=$p --trace-windows \
    --warmup=0s --duration=40s --max-duration=40s --target-ci=0 \
    --split-quiet=5s --split-settle-timeout=300s \
    "$@" --output="$name.json" >"$name.log" 2>&1 &
}
run cross2-hi-a0 net-conflicts:0.05:0.1:4:10:1 --modes=hi --affinities=0
run cross2-lo-a0 net-conflicts:0.05:0.1:4:10:1 --modes=lo --affinities=0
wait
