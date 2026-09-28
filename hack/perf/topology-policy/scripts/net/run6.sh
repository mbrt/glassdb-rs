#!/usr/bin/env bash
# Sixth live comparison: the time that a split adds, estimated from the time of
# the divided commit passes that a conflict ended.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-conflicts
for D in 1 2 4 8; do
  "$B" mixed --delays=s3 --databases=$D --workers-per-shape=8 \
    --affinities=0,50,100 --modes=lo,hi \
    --policies=size,net-passes:0.05:0.1:0.25:10,net-conflicts:0.05:0.1:4:10,net-conflicts:0.05:0.1:8:10 \
    --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=r6-mixed-s3-db$D.json >r6-mixed-s3-db$D.log 2>&1 &
done
wait
