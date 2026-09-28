#!/usr/bin/env bash
# Second live comparison: the net time policy with split multiples that give
# about the threshold of the offline replay (0.25 x 500 ms), because the live
# typical split time on S3 is about 1.1 s.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-net
N10=net:0.1:0.1:0.25:10
N05=net:0.05:0.1:0.25:10
MIXED=(--workers-per-shape=8 --affinities=0,50,100 --modes=lo,hi
  --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s
  --split-settle-timeout=300s)
for D in 1 2 4 8; do
  "$B" mixed --delays=s3 --databases=$D "${MIXED[@]}" \
    --policies=size,avoidable,$N10,$N05 \
    --output=r2-mixed-s3-db$D.json >r2-mixed-s3-db$D.log 2>&1 &
  "$B" mixed --delays=gcs --databases=$D "${MIXED[@]}" \
    --policies=size,avoidable,net:0.25:0.1:0.25:10,$N10,$N05 \
    --output=r2-mixed-gcs-db$D.json >r2-mixed-gcs-db$D.log 2>&1 &
done
for delays in s3 gcs; do
  for L in 16 128; do
    "$B" --delays=$delays --runs=1 --output=r2-topo-$delays-L$L.json topology \
      --workloads=single,hot,adjacent,random,scan --workers=8 --databases=1,4 \
      --leaf-sizes=$L --num-keys=1024 --duration=10s --max-duration=30s \
      --split-settle-timeout=600s --split-quiet=5s \
      --policies=fixed,avoidable,$N10,$N05 --adapt=20s >r2-topo-$delays-L$L.log 2>&1 &
  done
done
wait
