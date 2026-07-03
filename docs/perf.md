# Perf tracking

This document tracks changes to the engine that affect performance. The baseline
is the v0.1.0 release, which is the first public release and the best tested
version.

Keep this document sorted by the most recent changes first. Each entry should
include a reference to the commit or ADR that introduced the change.

## ADR-025 - ADR-026

Caching improvements and lock-dedup work.

### compare-refs summary

- base: 8e8011cdf0fd6c388823fd2dc6cd3ce2b0376623 (v1)
- target: 76463a7a583312784d7b0c80252636ec7aa751a2 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=1.13 median=1.26 max=1.98 (geomean=1.37)
- throughput[weak-read]: ratio b/a min=1.13 median=1.26 max=1.98 (geomean=1.37)
- throughput[write]: ratio b/a min=1.13 median=1.26 max=1.98 (geomean=1.37)
- latency-p50[strong-read]: ratio b/a min=0.53 median=0.82 max=0.85 (geomean=0.74)
- latency-p50[weak-read]: ratio b/a min=0.83 median=0.92 max=1.00 (geomean=0.91)
- latency-p50[write]: ratio b/a min=0.48 median=0.66 max=0.67 (geomean=0.61)
- retries: ratio b/a min=1.07 median=1.17 max=1.18 (geomean=1.14)
- backend-ops/tx: ratio b/a min=1.06 median=1.15 max=1.15 (geomean=1.12)

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.94 median=1.31 max=1.85 (geomean=1.32)
- throughput[weak-read]: ratio b/a min=0.94 median=1.31 max=1.85 (geomean=1.32)
- throughput[write]: ratio b/a min=0.94 median=1.31 max=1.85 (geomean=1.32)
- latency-p50[strong-read]: ratio b/a min=0.53 median=0.83 max=1.26 (geomean=0.82)
- latency-p50[weak-read]: ratio b/a min=0.83 median=1.00 max=1.25 (geomean=1.01)
- latency-p50[write]: ratio b/a min=0.48 median=0.65 max=0.88 (geomean=0.65)
- retries: no data
- backend-ops/tx: no data

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=1.15 median=1.26 max=2.29 (geomean=1.43)
- throughput[weak-read]: ratio b/a min=1.15 median=1.26 max=2.29 (geomean=1.43)
- throughput[write]: ratio b/a min=1.15 median=1.26 max=2.29 (geomean=1.43)
- latency-p50[strong-read]: ratio b/a min=0.44 median=0.85 max=0.88 (geomean=0.73)
- latency-p50[weak-read]: ratio b/a min=0.71 median=0.91 max=0.98 (geomean=0.87)
- latency-p50[write]: ratio b/a min=0.48 median=0.68 max=0.72 (geomean=0.63)
- retries: ratio b/a min=1.04 median=1.04 max=1.04 (geomean=1.04)
- backend-ops/tx: ratio b/a min=1.03 median=1.03 max=1.03 (geomean=1.03)

### deadlock

- deadlock-p50: ratio b/a min=0.67 median=4.69 max=11.93 (geomean=3.08)
- deadlock-p90: ratio b/a min=0.29 median=0.34 max=1.83 (geomean=0.46)

### mixbench

- mix-tps[roMulti]: ratio b/a min=2.79 median=5.68 max=9.20 (geomean=5.30)
- mix-tps[roSingle]: ratio b/a min=0.93 median=1.33 max=1.48 (geomean=1.25)
- mix-tps[rwMany]: ratio b/a min=1.42 median=1.83 max=8.54 (geomean=2.53)
- mix-tps[rwSingle]: ratio b/a min=1.14 median=1.55 max=1.99 (geomean=1.52)
- mix-ops/tx[hi/roMulti]: ratio b/a min=0.70 median=0.70 max=0.70 (geomean=0.70)
- mix-ops/tx[hi/roSingle]: ratio b/a min=1.08 median=1.08 max=1.08 (geomean=1.08)
- mix-ops/tx[hi/rwMany]: ratio b/a min=0.76 median=0.76 max=0.76 (geomean=0.76)
- mix-ops/tx[hi/rwSingle]: ratio b/a min=0.87 median=0.87 max=0.87 (geomean=0.87)
- mix-ops/tx[lo/roMulti]: ratio b/a min=1.11 median=1.11 max=1.11 (geomean=1.11)
- mix-ops/tx[lo/roSingle]: ratio b/a min=1.04 median=1.04 max=1.04 (geomean=1.04)
- mix-ops/tx[lo/rwMany]: ratio b/a min=0.99 median=0.99 max=0.99 (geomean=0.99)
- mix-ops/tx[lo/rwSingle]: ratio b/a min=1.02 median=1.02 max=1.02 (geomean=1.02)
- mix-retries/tx[hi]: ratio b/a min=0.48 median=0.78 max=1.30 (geomean=0.78)
- mix-retries/tx[lo]: ratio b/a min=0.88 median=2.04 max=2.54 (geomean=1.72)
- mix-agg-ops/tx[hi]: ratio b/a min=1.84 median=1.84 max=1.84 (geomean=1.84)
- mix-agg-ops/tx[lo]: ratio b/a min=2.15 median=2.15 max=2.15 (geomean=2.15)

### efficiency

- autoresearch-score: v1=1003.84 v2=933.75 ratio=0.930
- autoresearch-cost/tx: ratio b/a min=0.69 median=0.99 max=1.02 (geomean=0.93)
- autoresearch-ops/tx: ratio b/a min=0.70 median=0.99 max=1.02 (geomean=0.93)

## ADR-024

Designed in [ADR-024](adr/024-hold-and-wait-conflict-resolution.md).

### compare-refs summary

- base: 0ed3eda3a60b7efe3395f2ae6573aa05b8e63297 (v1)
- target: 80ee152db2f6860313ffe97d660b9d62ee1c4870 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.96 median=3.37 max=3.59 (geomean=2.50)
- throughput[weak-read]: ratio b/a min=0.96 median=3.37 max=3.59 (geomean=2.50)
- throughput[write]: ratio b/a min=0.96 median=3.37 max=3.59 (geomean=2.50)
- latency-p50[strong-read]: ratio b/a min=0.95 median=1.11 max=1.14 (geomean=1.08)
- latency-p50[weak-read]: ratio b/a min=0.00 median=0.00 max=0.56 (geomean=0.00)
- latency-p50[write]: ratio b/a min=1.17 median=1.39 max=1.41 (geomean=1.34)
- retries: no data
- backend-ops/tx: no data

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.97 median=4.19 max=4.44 (geomean=2.95)
- throughput[weak-read]: ratio b/a min=0.97 median=4.19 max=4.44 (geomean=2.95)
- throughput[write]: ratio b/a min=0.97 median=4.19 max=4.44 (geomean=2.95)
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.11 max=1.41 (geomean=1.15)
- latency-p50[weak-read]: ratio b/a min=0.00 median=0.00 max=0.80 (geomean=0.01)
- latency-p50[write]: ratio b/a min=1.21 median=1.36 max=1.60 (geomean=1.37)
- retries: no data
- backend-ops/tx: no data

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.88 median=1.33 max=1.93 (geomean=1.28)
- throughput[weak-read]: ratio b/a min=0.88 median=1.33 max=1.93 (geomean=1.28)
- throughput[write]: ratio b/a min=0.88 median=1.33 max=1.93 (geomean=1.28)
- latency-p50[strong-read]: ratio b/a min=1.01 median=1.06 max=1.08 (geomean=1.05)
- latency-p50[weak-read]: ratio b/a min=0.94 median=0.97 max=1.14 (geomean=1.00)
- latency-p50[write]: ratio b/a min=1.20 median=1.34 max=1.38 (geomean=1.31)
- retries: ratio b/a min=1.22 median=1.24 max=1.25 (geomean=1.24)
- backend-ops/tx: ratio b/a min=1.15 median=1.17 max=1.17 (geomean=1.17)

### deadlock

- deadlock-p50: ratio b/a min=1.12 median=1.30 max=16.79 (geomean=2.37)
- deadlock-p90: ratio b/a min=1.19 median=1.30 max=2.95 (geomean=1.48)

### mixbench

- mix-tps[roMulti]: ratio b/a min=1.44 median=1.94 max=3.42 (geomean=2.05)
- mix-tps[roSingle]: ratio b/a min=1.13 median=1.94 max=12.37 (geomean=2.60)
- mix-tps[rwMany]: ratio b/a min=0.72 median=0.97 max=1.43 (geomean=0.98)
- mix-tps[rwSingle]: ratio b/a min=0.78 median=1.12 max=2.37 (geomean=1.23)
- mix-ops/tx[hi/roMulti]: ratio b/a min=1.53 median=1.53 max=1.53 (geomean=1.53)
- mix-ops/tx[hi/roSingle]: ratio b/a min=0.59 median=0.59 max=0.59 (geomean=0.59)
- mix-ops/tx[hi/rwMany]: ratio b/a min=2.77 median=2.77 max=2.77 (geomean=2.77)
- mix-ops/tx[hi/rwSingle]: ratio b/a min=0.86 median=0.86 max=0.86 (geomean=0.86)
- mix-ops/tx[lo/roMulti]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00)
- mix-ops/tx[lo/roSingle]: ratio b/a min=0.99 median=0.99 max=0.99 (geomean=0.99)
- mix-ops/tx[lo/rwMany]: ratio b/a min=1.20 median=1.20 max=1.20 (geomean=1.20)
- mix-ops/tx[lo/rwSingle]: ratio b/a min=1.17 median=1.17 max=1.17 (geomean=1.17)
- mix-retries/tx[hi]: ratio b/a min=0.42 median=1.79 max=3.53 (geomean=1.29)
- mix-retries/tx[lo]: ratio b/a min=1.18 median=1.39 max=4.49 (geomean=1.79)
- mix-agg-ops/tx[hi]: ratio b/a min=1.15 median=1.15 max=1.15 (geomean=1.15)
- mix-agg-ops/tx[lo]: ratio b/a min=1.09 median=1.09 max=1.09 (geomean=1.09)

### efficiency

- autoresearch-score: v1=934.42 v2=1003.84 ratio=1.074
- autoresearch-cost/tx: ratio b/a min=1.00 median=1.00 max=1.22 (geomean=1.07)
- autoresearch-ops/tx: ratio b/a min=1.00 median=1.00 max=1.24 (geomean=1.08)

## v2 MVP

Described in [algo-v2.md](algo-v2.md) and implemented by ADRs (016 - 023).

### compare-refs summary

- base: e2171c3c8e2d6b9f7bf27c57b59e802c04f3a1fd (v1)
- target: 0ed3eda3a60b7efe3395f2ae6573aa05b8e63297 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.21 median=0.25 max=0.46 (geomean=0.28)
- throughput[weak-read]: ratio b/a min=0.21 median=0.25 max=0.46 (geomean=0.28)
- throughput[write]: ratio b/a min=0.21 median=0.25 max=0.46 (geomean=0.28)
- latency-p50[strong-read]: ratio b/a min=0.69 median=0.72 max=1.88 (geomean=0.90)
- latency-p50[weak-read]: ratio b/a min=1.12 median=630.88 max=839.30 (geomean=137.82)
- latency-p50[write]: ratio b/a min=1.47 median=1.51 max=2.40 (geomean=1.68)
- retries: no data
- backend-ops/tx: no data

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.21 median=0.24 max=0.50 (geomean=0.27)
- throughput[weak-read]: ratio b/a min=0.21 median=0.24 max=0.50 (geomean=0.27)
- throughput[write]: ratio b/a min=0.21 median=0.24 max=0.50 (geomean=0.27)
- latency-p50[strong-read]: ratio b/a min=0.65 median=1.03 max=1.90 (geomean=1.04)
- latency-p50[weak-read]: ratio b/a min=1.12 median=464.42 max=783.50 (geomean=116.48)
- latency-p50[write]: ratio b/a min=1.27 median=1.49 max=2.41 (geomean=1.61)
- retries: no data
- backend-ops/tx: no data

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.19 median=0.30 max=0.37 (geomean=0.28)
- throughput[weak-read]: ratio b/a min=0.19 median=0.30 max=0.37 (geomean=0.28)
- throughput[write]: ratio b/a min=0.19 median=0.30 max=0.37 (geomean=0.28)
- latency-p50[strong-read]: ratio b/a min=0.70 median=0.74 max=2.11 (geomean=0.95)
- latency-p50[weak-read]: ratio b/a min=1.20 median=283.42 max=661.00 (geomean=32.14)
- latency-p50[write]: ratio b/a min=1.54 median=1.60 max=2.48 (geomean=1.77)
- retries: no data
- backend-ops/tx: no data

### deadlock

- deadlock-p50: ratio b/a min=0.56 median=3.89 max=5.27 (geomean=2.32)
- deadlock-p90: ratio b/a min=0.50 median=20.22 max=24.21 (geomean=10.50)

### mixbench

- mix-tps[roMulti]: ratio b/a min=0.12 median=0.43 max=0.99 (geomean=0.37)
- mix-tps[roSingle]: ratio b/a min=0.48 median=0.80 max=1.46 (geomean=0.82)
- mix-tps[rwMany]: ratio b/a min=0.07 median=0.42 max=1.09 (geomean=0.28)
- mix-tps[rwSingle]: ratio b/a min=0.18 median=0.51 max=0.85 (geomean=0.40)
- mix-ops/tx[hi/roMulti]: ratio b/a min=6.16 median=6.16 max=6.16 (geomean=6.16)
- mix-ops/tx[hi/roSingle]: ratio b/a min=5.83 median=5.83 max=5.83 (geomean=5.83)
- mix-ops/tx[hi/rwMany]: ratio b/a min=4.30 median=4.30 max=4.30 (geomean=4.30)
- mix-ops/tx[hi/rwSingle]: ratio b/a min=3.16 median=3.16 max=3.16 (geomean=3.16)
- mix-ops/tx[lo/roMulti]: ratio b/a min=2.54 median=2.54 max=2.54 (geomean=2.54)
- mix-ops/tx[lo/roSingle]: ratio b/a min=2.32 median=2.32 max=2.32 (geomean=2.32)
- mix-ops/tx[lo/rwMany]: ratio b/a min=2.60 median=2.60 max=2.60 (geomean=2.60)
- mix-ops/tx[lo/rwSingle]: ratio b/a min=3.12 median=3.12 max=3.12 (geomean=3.12)
- mix-retries/tx[hi]: ratio b/a min=0.59 median=1.56 max=2.62 (geomean=1.39)
- mix-retries/tx[lo]: ratio b/a min=0.14 median=0.40 max=0.68 (geomean=0.35)
- mix-agg-ops/tx[hi]: ratio b/a min=1.83 median=1.83 max=1.83 (geomean=1.83)
- mix-agg-ops/tx[lo]: ratio b/a min=2.32 median=2.32 max=2.32 (geomean=2.32)

### efficiency

- autoresearch-score: v1=402.92 v2=934.42 ratio=2.319
- autoresearch-cost/tx: ratio b/a min=1.81 median=1.84 max=4.55 (geomean=2.32)
- autoresearch-ops/tx: ratio b/a min=0.99 median=1.99 max=4.86 (geomean=1.81)

## Baseline (v0.1.0)

autoresearch-score: 403.57
