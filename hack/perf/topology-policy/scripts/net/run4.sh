#!/usr/bin/env bash
# Fourth live comparison: higher weights of the divided transactions at split
# multiple 0.05, in the mixed lo and hi modes on S3.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-net
for D in 1 2 4 8; do
  "$B" mixed --delays=s3 --databases=$D --workers-per-shape=8 \
    --affinities=0,50,100 --modes=lo,hi \
    --policies=size,net:0.05:0.1:0.25:10,net:0.05:0.1:0.5:10,net:0.05:0.1:1:10 \
    --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=r4-mixed-s3-db$D.json >r4-mixed-s3-db$D.log 2>&1 &
done
wait
