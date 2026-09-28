#!/usr/bin/env bash
# Validation after the review: the net policy decides merges only for the
# pairs of the window. Mixed on S3 and topology on S3 and GCS, two runs.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-crossing4
P=net-conflicts:0.05:0.1:4:10:1
for D in 1 2 4 8; do
  "$B" mixed --delays=s3 --databases=$D --workers-per-shape=8 --runs=2 \
    --affinities=0,50,100 --modes=lo,hi --policies=size,avoidable,$P \
    --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=r15-mixed-s3-db$D.json >r15-mixed-s3-db$D.log 2>&1 &
done
for delays in s3 gcs; do
  for L in 16 128; do
    "$B" --delays=$delays --runs=2 --output=r15-topo-$delays-L$L.json topology \
      --workloads=single,hot,adjacent,random,scan --workers=8 --databases=1,4 \
      --leaf-sizes=$L --num-keys=1024 --duration=10s --max-duration=30s \
      --split-settle-timeout=600s --split-quiet=5s \
      --policies=fixed,avoidable,$P --adapt=20s >r15-topo-$delays-L$L.log 2>&1 &
  done
done
wait
