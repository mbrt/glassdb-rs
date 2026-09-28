#!/usr/bin/env bash
# Third live comparison: the net time policy with moving averages for merges
# (perfbench-net2) against the one-window merge rule (perfbench-net), in the
# hi mode cells, and in the topology workloads that merge.
set -euo pipefail
cd "$(dirname "$0")"
NEW=/tmp/topo-exp/perfbench-net2
OLD=/tmp/topo-exp/perfbench-net
N05=net:0.05:0.1:0.25:10
N10=net:0.1:0.1:0.25:10
MIXED=(--delays=s3 --workers-per-shape=8 --affinities=0,50,100 --modes=hi
  --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s
  --split-settle-timeout=300s)
for D in 1 2 4 8; do
  "$NEW" mixed --databases=$D "${MIXED[@]}" --policies=size,avoidable,$N05,$N10 \
    --output=r3-new-hi-s3-db$D.json >r3-new-hi-s3-db$D.log 2>&1 &
  "$OLD" mixed --databases=$D "${MIXED[@]}" --policies=size,$N05 \
    --output=r3-old-hi-s3-db$D.json >r3-old-hi-s3-db$D.log 2>&1 &
done
for delays in s3 gcs; do
  for L in 16 128; do
    "$NEW" --delays=$delays --runs=1 --output=r3-topo-$delays-L$L.json topology \
      --workloads=adjacent,scan --workers=8 --databases=1,4 \
      --leaf-sizes=$L --num-keys=1024 --duration=10s --max-duration=30s \
      --split-settle-timeout=600s --split-quiet=5s \
      --policies=fixed,avoidable,$N05 --adapt=20s >r3-topo-$delays-L$L.log 2>&1 &
  done
done
wait
