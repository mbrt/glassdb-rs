#!/usr/bin/env bash
# Mixed on GCS with two runs: the crossing weights 1 and 0.5 against the size
# causes and the ADR-074 policy.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-crossing3
for D in 1 2 4 8; do
  "$B" mixed --delays=gcs --databases=$D --workers-per-shape=8 --runs=2 \
    --affinities=0,50,100 --modes=lo,hi \
    --policies=size,avoidable,net-conflicts:0.05:0.1:4:10:1,net-conflicts:0.05:0.1:4:10:0.5 \
    --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=r11-mixed-gcs-db$D.json >r11-mixed-gcs-db$D.log 2>&1 &
done
wait
