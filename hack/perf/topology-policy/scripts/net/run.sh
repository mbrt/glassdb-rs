#!/usr/bin/env bash
# Live comparison of the net time policy with the ADR-074 policies, on the
# matrix of the ADR-074 final runs, plus the perfect split of two databases.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-net
NET=net:0.25:0.1:0.25:10
for D in 1 2 4 8; do
  "$B" mixed --delays=s3 --databases=$D --workers-per-shape=8 \
    --affinities=0,50,100 --modes=lo,hi --policies=size,avoidable,$NET \
    --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=mixed-s3-db$D.json >mixed-s3-db$D.log 2>&1 &
done
for delays in s3 gcs; do
  for L in 16 128; do
    "$B" --delays=$delays --runs=1 --output=topo-$delays-L$L.json topology \
      --workloads=single,hot,adjacent,random,scan --workers=8 --databases=1,4 \
      --leaf-sizes=$L --num-keys=1024 --duration=10s --max-duration=30s \
      --split-settle-timeout=600s --split-quiet=5s \
      --policies=fixed,avoidable,$NET --adapt=20s >topo-$delays-L$L.log 2>&1 &
  done
done
"$B" mixed --delays=s3 --databases=2 --workers-per-shape=8 --affinities=0 \
  --modes=hi --key-layouts=ranges --shapes=rwSingle \
  --policies=fixed,avoidable,$NET --warmup=20s --duration=5s --max-duration=30s \
  --split-quiet=5s --split-settle-timeout=300s \
  --output=ranges-s3-db2.json >ranges-s3-db2.log 2>&1 &
wait
