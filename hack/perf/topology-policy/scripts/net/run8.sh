#!/usr/bin/env bash
# The topology scenario with the conflict time over two adjacent leaves on the
# merge side, against the ADR-074 policy and the same net policy without it.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-crossing3
for delays in s3 gcs; do
  for L in 16 128; do
    "$B" --delays=$delays --runs=1 --output=r8-topo-$delays-L$L.json topology \
      --workloads=single,hot,adjacent,random,scan --workers=8 --databases=1,4 \
      --leaf-sizes=$L --num-keys=1024 --duration=10s --max-duration=30s \
      --split-settle-timeout=600s --split-quiet=5s \
      --policies=fixed,avoidable,net-conflicts:0.05:0.1:4:10,net-conflicts:0.05:0.1:4:10:1 \
      --adapt=20s >r8-topo-$delays-L$L.log 2>&1 &
  done
done
wait
