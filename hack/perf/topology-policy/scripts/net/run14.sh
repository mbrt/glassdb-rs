#!/usr/bin/env bash
# Mixed lo on GCS, 4 and 8 databases, with a warmup of 60 s: is the lo loss of
# the net policy only a later split? Also a half-life of 5 s and a split
# multiple of 0.025, which make the net policy split sooner.
set -euo pipefail
cd "$(dirname "$0")"
B=/tmp/topo-exp/perfbench-crossing3
for D in 4 8; do
  "$B" mixed --delays=gcs --databases=$D --workers-per-shape=8 --runs=2 \
    --affinities=0,50,100 --modes=lo \
    --policies=size,avoidable,net-conflicts:0.05:0.1:4:10:1,net-conflicts:0.05:0.1:4:5:1,net-conflicts:0.025:0.1:4:10:1 \
    --warmup=60s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=r14-mixed-gcs-db$D.json >r14-mixed-gcs-db$D.log 2>&1 &
  "$B" mixed --delays=s3 --databases=$D --workers-per-shape=8 --runs=2 \
    --affinities=0,50,100 --modes=hi \
    --policies=size,avoidable,net-conflicts:0.05:0.1:4:10:1,net-conflicts:0.05:0.1:4:5:1,net-conflicts:0.025:0.1:4:10:1 \
    --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
    --split-settle-timeout=300s --output=r14-hi-s3-db$D.json >r14-hi-s3-db$D.log 2>&1 &
done
wait
