#!/usr/bin/env bash
# The cascade runs again, with the lost CAS fix: the time stops at a loss on
# the same keys, and only members that a transaction waits for count.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-lostfix
run() {
  local name=$1 p=$2
  shift 2
  "$B" mixed --delays=s3 --workers-per-shape=8 --modes=hi --databases=8 \
    --policies=$p --shadow-policies=$p --trace-windows \
    --warmup=0s --duration=40s --max-duration=40s --target-ci=0 \
    --split-quiet=5s --split-settle-timeout=300s \
    "$@" --output="$name.json" >"$name.log" 2>&1 &
}
run fix-passes-a0 net-passes:0.05:0.1:0.25:10 --affinities=0
run fix-conflicts-a0 net-conflicts:0.05:0.1:4:10 --affinities=0
wait
