#!/usr/bin/env bash
# Traced counterfactual runs: the hi mode on a few layouts, and the lo mode.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-harness
COMMON=(--delays=s3 --workers-per-shape=8 --affinities=0
  --policies=fixed --shadow-policies=avoidable,size --trace-windows
  --warmup=5s --duration=5s --max-duration=30s
  --split-quiet=5s --split-settle-timeout=300s)
run() {
  local name=$1
  shift
  "$B" mixed "${COMMON[@]}" "$@" --output="$name.json" >"$name.log" 2>&1 &
}
run tr-hi-ranges-rw-db2 --modes=hi --seed-leaf-entries=default,7 --databases=2 --key-layouts=ranges --shapes=rwSingle
run tr-hi-inter-rw-db2 --modes=hi --seed-leaf-entries=default,7 --databases=2 --key-layouts=interleaved --shapes=rwSingle
run tr-hi-shared-rw-db1 --modes=hi --seed-leaf-entries=default,7 --databases=1 --shapes=rwSingle
run tr-lo-db1 --modes=lo --seed-leaf-entries=default,128 --databases=1
run tr-lo-db8 --modes=lo --seed-leaf-entries=default,128 --databases=8
wait
