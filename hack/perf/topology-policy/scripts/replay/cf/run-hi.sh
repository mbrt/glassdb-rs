#!/usr/bin/env bash
# Counterfactual runs of the hi mode: the unsplit tree and seeded splits under
# the fixed policy, with shadow policies on the windows.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-harness
COMMON=(--delays=s3 --workers-per-shape=8 --affinities=0 --modes=hi
  --policies=fixed --shadow-policies=avoidable,size
  --seed-leaf-entries=default,7,3 --warmup=5s --duration=5s --max-duration=30s
  --split-quiet=5s --split-settle-timeout=300s)
run() {
  local name=$1
  shift
  "$B" mixed "${COMMON[@]}" "$@" --output="$name.json" >"$name.log" 2>&1 &
}
run hi-ranges-rw-db2 --databases=2 --key-layouts=ranges --shapes=rwSingle
run hi-inter-rw-db2 --databases=2 --key-layouts=interleaved --shapes=rwSingle
run hi-shared-rw-db2 --databases=2 --shapes=rwSingle
run hi-shared-rw-db1 --databases=1 --shapes=rwSingle
run hi-shared-all-db1 --databases=1
run hi-shared-all-db4 --databases=4
run hi-ranges-all-db2 --databases=2 --key-layouts=ranges
wait
