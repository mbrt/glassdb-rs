#!/usr/bin/env bash
# A/B of the merge rule in the hi cells that change the tree: crossing3 decides
# merges also for pairs of earlier windows, crossing4 only for the pairs of the
# window. Two processes of each binary, four runs each.
set -euo pipefail
cd "$(dirname "$0")"
P=net-conflicts:0.05:0.1:4:10:1
for bin in crossing3 crossing4; do
  for part in a b; do
    "/tmp/topo-exp/perfbench-$bin" mixed --delays=s3 --databases=8 --workers-per-shape=8 --runs=4 \
      --affinities=0,50 --modes=hi --policies=size,$P \
      --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
      --split-settle-timeout=300s --output=r16-$bin-$part.json >r16-$bin-$part.log 2>&1 &
  done
done
wait
