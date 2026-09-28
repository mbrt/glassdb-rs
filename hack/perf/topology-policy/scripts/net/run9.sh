#!/usr/bin/env bash
# Ninth live comparison: two more runs of the two net-conflicts variants, with
# the ADR-074 policy, after the lost CAS fix.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-crossing3
for D in 1 2 4 8; do
  "$B" mixed --delays=s3 --databases=$D --workers-per-shape=8 --runs=2 \
    --affinities=0,50,100 --modes=lo,hi \
    --policies=size,avoidable,net-conflicts:0.05:0.1:4:10,net-conflicts:0.05:0.1:4:10:1 \
    --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=r9-mixed-s3-db$D.json >r9-mixed-s3-db$D.log 2>&1 &
done
wait
