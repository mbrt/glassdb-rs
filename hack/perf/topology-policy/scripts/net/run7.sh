#!/usr/bin/env bash
# Seventh live comparison: the lost CAS fix, and the conflict time of the
# transactions over two adjacent leaves on the merge side.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-crossing3
for D in 1 2 4 8; do
  "$B" mixed --delays=s3 --databases=$D --workers-per-shape=8 \
    --affinities=0,50,100 --modes=lo,hi \
    --policies=size,net-conflicts:0.05:0.1:4:10,net-conflicts:0.05:0.1:4:10:1,net-passes:0.05:0.1:0.25:10:1 \
    --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=r7-mixed-s3-db$D.json >r7-mixed-s3-db$D.log 2>&1 &
done
wait
