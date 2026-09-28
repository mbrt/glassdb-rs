#!/usr/bin/env bash
# Mixed lo on GCS with two runs: does a smaller divided conflict weight keep
# the splits that help in lo? Weights 0, 2, and 4, all with the crossing
# weight 1.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-crossing3
for D in 1 2 4 8; do
  "$B" mixed --delays=gcs --databases=$D --workers-per-shape=8 --runs=2 \
    --affinities=0,50,100 --modes=lo \
    --policies=size,avoidable,net-conflicts:0.05:0.1:0:10:1,net-conflicts:0.05:0.1:2:10:1,net-conflicts:0.05:0.1:4:10:1 \
    --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=r13-mixed-gcs-db$D.json >r13-mixed-gcs-db$D.log 2>&1 &
done
wait
