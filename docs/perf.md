# Perf tracking

This document tracks changes to the engine that affect performance. The baseline
is the v0.1.0 release, which is the first public release and the best tested
version.

Keep this document sorted by the most recent changes first. Each entry should
include a reference to the commit or ADR that introduced the change.

## v2 MVP

Described in [algo-v2.md](algo-v2.md) and implemented by ADRs (016 - 023).

### compare-refs summary

- base: main (v1)
- target: current worktree (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.22 median=0.26 max=0.46 (geomean=0.28)
- throughput[weak-read]: ratio b/a min=0.22 median=0.26 max=0.46 (geomean=0.28)
- throughput[write]: ratio b/a min=0.22 median=0.26 max=0.46 (geomean=0.28)
- latency-p50[strong-read]: ratio b/a min=0.66 median=0.69 max=1.89 (geomean=0.88)
- latency-p50[weak-read]: ratio b/a min=254.00 median=381.00 max=415.00 (geomean=342.45)
- latency-p50[write]: ratio b/a min=1.27 median=1.48 max=2.53 (geomean=1.63)
- retries: ratio b/a min=0.27 median=0.37 max=0.46 (geomean=0.36)
- backend-ops/tx: ratio b/a min=0.91 median=0.92 max=1.29 (geomean=1.00)

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.21 median=0.23 max=0.51 (geomean=0.27)
- throughput[weak-read]: ratio b/a min=0.21 median=0.23 max=0.51 (geomean=0.27)
- throughput[write]: ratio b/a min=0.21 median=0.23 max=0.51 (geomean=0.27)
- latency-p50[strong-read]: ratio b/a min=0.65 median=1.00 max=1.89 (geomean=1.03)
- latency-p50[weak-read]: ratio b/a min=190.50 median=190.50 max=190.50 (geomean=190.50)
- latency-p50[write]: ratio b/a min=1.27 median=1.47 max=2.44 (geomean=1.61)
- retries: ratio b/a min=0.18 median=0.37 max=0.41 (geomean=0.32)
- backend-ops/tx: ratio b/a min=0.82 median=0.86 max=1.16 (geomean=0.92)

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.20 median=0.30 max=0.38 (geomean=0.29)
- throughput[weak-read]: ratio b/a min=0.20 median=0.30 max=0.38 (geomean=0.29)
- throughput[write]: ratio b/a min=0.20 median=0.30 max=0.38 (geomean=0.29)
- latency-p50[strong-read]: ratio b/a min=0.69 median=0.73 max=2.00 (geomean=0.92)
- latency-p50[weak-read]: ratio b/a min=1.00 median=201.13 max=418.00 (geomean=24.80)
- latency-p50[write]: ratio b/a min=1.52 median=1.56 max=2.48 (geomean=1.74)
- retries: ratio b/a min=0.33 median=0.42 max=0.58 (geomean=0.43)
- backend-ops/tx: ratio b/a min=1.30 median=1.35 max=1.73 (geomean=1.42)

### deadlock

- deadlock-p50: ratio b/a min=0.53 median=0.90 max=4.67 (geomean=1.35)
- deadlock-p90: ratio b/a min=3.10 median=19.96 max=27.59 (geomean=14.58)

### efficiency

- autoresearch-score: v1=402.95 v2=934.42 ratio=2.319
- autoresearch-cost/tx: ratio b/a min=1.81 median=1.84 max=4.53 (geomean=2.32)
- autoresearch-ops/tx: ratio b/a min=0.99 median=2.00 max=4.81 (geomean=1.80)

## Baseline (v0.1.0)

autoresearch-score: 403.57
