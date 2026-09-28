#!/usr/bin/env bash
# Why rwMany and roMulti starve after splits in hi mode with 8 databases:
# fixed trees of 1, 2, 4, and more leaves for each collection, for each shape
# alone and in pairs.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-attempts2
COMMON=(--delays=s3 --workers-per-shape=8 --affinities=0 --modes=hi --databases=8
  --policies=fixed --seed-leaf-entries=default,7,3,2
  --warmup=5s --duration=5s --max-duration=30s
  --split-quiet=5s --split-settle-timeout=300s)
run() {
  local name=$1
  shift
  "$B" mixed "${COMMON[@]}" "$@" --output="$name.json" >"$name.log" 2>&1 &
}
run all --shapes=rwSingle,rwMany,roSingle,roMulti
run rwMany --shapes=rwMany
run roMulti --shapes=roMulti
run rwMany-roSingle --shapes=rwMany,roSingle
wait
run rwMany-rwSingle --shapes=rwSingle,rwMany
run roMulti-roSingle --shapes=roSingle,roMulti
run roMulti-rwSingle --shapes=rwSingle,roMulti
run rwMany-roMulti --shapes=rwMany,roMulti
wait
