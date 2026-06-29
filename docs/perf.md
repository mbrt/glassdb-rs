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

### efficiency

- autoresearch-score: v1=403.57 v2=934.42 ratio=2.315
- autoresearch-cost/tx: ratio b/a min=1.81 median=1.84 max=4.55 (geomean=2.32)
- autoresearch-ops/tx: ratio b/a min=0.99 median=1.96 max=4.86 (geomean=1.80)

## Baseline (v0.1.0)

autoresearch-score: 403.57
