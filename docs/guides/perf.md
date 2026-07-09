# Perf tracking

This document tracks changes to the engine that affect performance. The baseline
is the v0.1.0 release, which is the first public release and the best tested
version.

Keep this document sorted by the most recent changes first. Each entry should
include a reference to the commit or ADR that introduced the change.

## ADR-030: Seed shard loads

Reducing the number of strong shard loads and replacing them with caching in
some safe places ([ADR-030](../adr/030-seed-shard-loads.md)).

### compare-refs summary

- base: 736aa6baef008bc725b1cfe49f2d1a974bd47bda (v1)
- target: current worktree (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)
- each line ends in a `=> better/WORSE/~same` verdict read in that
  metric's own direction, so no axis has to be interpreted by hand
- `autoresearch-*` is **deterministic** (single-client backend ops/tx,
  lower is better) — the most trustworthy signal; `mix-*` cells run
  until their throughput 95% CI reaches --target-ci, so a converged
  ratio is significant — `[unconverged]` marks a cell that hit its time
  cap first (read as indicative); `deadlock-*` stay **[noisy]**

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=1.01 median=1.03 max=1.06 (geomean=1.03, n=2) => better
- throughput[weak-read]: ratio b/a min=1.01 median=1.03 max=1.06 (geomean=1.03, n=2) => better
- throughput[write]: ratio b/a min=1.01 median=1.03 max=1.06 (geomean=1.03, n=2) => better
- latency-p50[strong-read]: ratio b/a min=0.97 median=0.98 max=0.99 (geomean=0.98, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.92 median=0.93 max=0.94 (geomean=0.93, n=2) => better
- latency-p50[write]: ratio b/a min=0.96 median=0.97 max=0.97 (geomean=0.97, n=2) => better
- retries: ratio b/a min=0.96 median=1.00 max=1.07 (geomean=1.01, n=5) => ~same
- backend-ops/tx: ratio b/a min=0.96 median=1.00 max=1.06 (geomean=1.01, n=5) => ~same

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.96 median=0.98 max=1.00 (geomean=0.98, n=2) => ~same
- throughput[weak-read]: ratio b/a min=0.96 median=0.98 max=1.00 (geomean=0.98, n=2) => ~same
- throughput[write]: ratio b/a min=0.96 median=0.98 max=1.00 (geomean=0.98, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=0.98 median=1.22 max=1.45 (geomean=1.19, n=2) => WORSE
- latency-p50[weak-read]: ratio b/a min=1.03 median=1.04 max=1.05 (geomean=1.04, n=2) => WORSE
- latency-p50[write]: ratio b/a min=0.98 median=1.00 max=1.03 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=0.95 median=1.00 max=1.01 (geomean=0.98, n=3) => ~same
- backend-ops/tx: ratio b/a min=0.95 median=1.00 max=1.01 (geomean=0.99, n=3) => ~same

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- throughput[weak-read]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- throughput[write]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.01 max=1.02 (geomean=1.01, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=1.00 median=1.15 max=1.31 (geomean=1.14, n=2) => WORSE
- latency-p50[write]: ratio b/a min=0.99 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00, n=7) => ~same
- backend-ops/tx: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=7) => ~same

### deadlock

- deadlock-p50 [noisy]: ratio b/a min=0.96 median=1.00 max=1.03 (geomean=1.00, n=6) => ~same
- deadlock-p90 [noisy]: ratio b/a min=0.97 median=1.02 max=1.04 (geomean=1.01, n=6) => ~same

### mixbench

- mix-tps[roMulti]: ratio b/a min=0.97 median=1.06 max=1.30 (geomean=1.09, n=4) n_min=1106 => better
- mix-tps[roSingle]: ratio b/a min=0.96 median=1.00 max=1.10 (geomean=1.01, n=4) n_min=2064 => ~same
- mix-tps[rwMany] [unconverged]: ratio b/a min=0.91 median=1.09 max=2.31 (geomean=1.25, n=4) n_min=106 => better
- mix-tps[rwSingle]: ratio b/a min=0.30 median=1.04 max=1.88 (geomean=0.88, n=4) n_min=122 => better
- mix-ops/tx[hi/roMulti]: ratio b/a=0.74 (1 point) n_min=15603 => better
- mix-ops/tx[hi/roSingle]: ratio b/a=0.99 (1 point) n_min=32430 => ~same
- mix-ops/tx[hi/rwMany] [unconverged]: ratio b/a=0.63 (1 point) n_min=106 => better
- mix-ops/tx[hi/rwSingle]: ratio b/a=1.13 (1 point) n_min=264 => WORSE
- mix-ops/tx[lo/roMulti]: ratio b/a=1.01 (1 point) n_min=1106 => ~same
- mix-ops/tx[lo/roSingle]: ratio b/a=1.00 (1 point) n_min=2064 => ~same
- mix-ops/tx[lo/rwMany]: ratio b/a=0.99 (1 point) n_min=172 => ~same
- mix-ops/tx[lo/rwSingle]: ratio b/a=0.73 (1 point) n_min=677 => better
- mix-retries/tx[hi] [unconverged]: ratio b/a min=0.38 median=0.67 max=1.49 (geomean=0.71, n=4) => better
- mix-retries/tx[lo]: ratio b/a min=0.36 median=0.95 max=1.25 (geomean=0.80, n=4) => better
- mix-agg-ops/tx[hi]: ratio b/a=1.02 (1 point) => WORSE
- mix-agg-ops/tx[lo]: ratio b/a=0.92 (1 point) => better

### efficiency

- autoresearch-score (cost/tx geomean, lower=better) [deterministic]: v1=960.48 v2=866.82 ratio b/a=0.902 => better
- autoresearch-cost/tx: ratio b/a min=0.67 median=1.00 max=1.00 (geomean=0.90, n=5) => ~same
- autoresearch-ops/tx: ratio b/a min=0.65 median=1.00 max=1.00 (geomean=0.90, n=5) => ~same
- autoresearch-cost/tx[singleRMW]: ratio b/a=0.67 (1 point) => better
- autoresearch-cost/tx[multiRMW10]: ratio b/a=0.89 (1 point) => better
- autoresearch-cost/tx[readRepeat]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchRead10]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchWrite100]: ratio b/a=1.00 (1 point) => ~same

## ADR-029: GC Shard Coordinator

### compare-refs summary

- base: b789c651741d78f7388dcd71038e95ca095c3974 (v1)
- target: 736aa6baef008bc725b1cfe49f2d1a974bd47bda (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)
- each line ends in a `=> better/WORSE/~same` verdict read in that
  metric's own direction, so no axis has to be interpreted by hand
- `autoresearch-*` is **deterministic** (single-client backend ops/tx,
  lower is better) — the most trustworthy signal; `mix-*` cells run
  until their throughput 95% CI reaches --target-ci, so a converged
  ratio is significant — `[unconverged]` marks a cell that hit its time
  cap first (read as indicative); `deadlock-*` stay **[noisy]**

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- throughput[weak-read]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- throughput[write]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.97 median=1.02 max=1.08 (geomean=1.02, n=2) => WORSE
- latency-p50[write]: ratio b/a min=1.01 median=1.01 max=1.02 (geomean=1.01, n=2) => ~same
- retries: ratio b/a min=0.98 median=1.00 max=1.04 (geomean=1.00, n=5) => ~same
- backend-ops/tx: ratio b/a min=0.98 median=1.00 max=1.03 (geomean=1.00, n=5) => ~same

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- throughput[weak-read]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- throughput[write]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=0.98 median=0.98 max=0.99 (geomean=0.98, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.93 median=1.01 max=1.08 (geomean=1.01, n=2) => ~same
- latency-p50[write]: ratio b/a min=0.98 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- retries: ratio b/a min=1.01 median=1.01 max=1.05 (geomean=1.02, n=5) => ~same
- backend-ops/tx: ratio b/a min=1.01 median=1.01 max=1.04 (geomean=1.02, n=5) => ~same

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=1.03 median=1.07 max=1.11 (geomean=1.07, n=2) => better
- throughput[weak-read]: ratio b/a min=1.03 median=1.07 max=1.11 (geomean=1.07, n=2) => better
- throughput[write]: ratio b/a min=1.03 median=1.07 max=1.11 (geomean=1.07, n=2) => better
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.80 median=0.91 max=1.01 (geomean=0.90, n=2) => better
- latency-p50[write]: ratio b/a min=0.99 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- retries: ratio b/a min=0.98 median=1.00 max=1.02 (geomean=1.00, n=11) => ~same
- backend-ops/tx: ratio b/a min=0.99 median=1.00 max=1.02 (geomean=1.00, n=11) => ~same

### deadlock

- deadlock-p50 [noisy]: ratio b/a min=0.98 median=1.01 max=1.02 (geomean=1.00, n=6) => ~same
- deadlock-p90 [noisy]: ratio b/a min=0.99 median=1.02 max=1.05 (geomean=1.02, n=6) => ~same

### mixbench

- mix-tps[roMulti]: ratio b/a min=0.98 median=1.01 max=1.19 (geomean=1.04, n=4) n_min=1260 => ~same
- mix-tps[roSingle]: ratio b/a min=1.00 median=1.03 max=1.16 (geomean=1.05, n=4) n_min=2297 => better
- mix-tps[rwMany] [unconverged]: ratio b/a min=0.65 median=1.04 max=1.16 (geomean=0.95, n=4) n_min=62 => better
- mix-tps[rwSingle]: ratio b/a min=0.94 median=1.02 max=1.14 (geomean=1.03, n=4) n_min=112 => better
- mix-ops/tx[hi/roMulti]: ratio b/a=1.00 (1 point) n_min=26105 => ~same
- mix-ops/tx[hi/roSingle]: ratio b/a=0.98 (1 point) n_min=66525 => ~same
- mix-ops/tx[hi/rwMany] [unconverged]: ratio b/a=1.27 (1 point) n_min=62 => WORSE
- mix-ops/tx[hi/rwSingle]: ratio b/a=1.02 (1 point) n_min=1541 => ~same
- mix-ops/tx[lo/roMulti]: ratio b/a=0.96 (1 point) n_min=1260 => better
- mix-ops/tx[lo/roSingle]: ratio b/a=0.99 (1 point) n_min=2297 => ~same
- mix-ops/tx[lo/rwMany]: ratio b/a=0.98 (1 point) n_min=149 => better
- mix-ops/tx[lo/rwSingle]: ratio b/a=1.00 (1 point) n_min=714 => ~same
- mix-retries/tx[hi] [unconverged]: ratio b/a min=0.85 median=0.97 max=1.32 (geomean=1.01, n=4) => better
- mix-retries/tx[lo]: ratio b/a min=0.76 median=0.82 max=1.07 (geomean=0.86, n=4) => better
- mix-agg-ops/tx[hi]: ratio b/a=0.99 (1 point) => ~same
- mix-agg-ops/tx[lo]: ratio b/a=1.00 (1 point) => ~same

### efficiency

- autoresearch-score (cost/tx geomean, lower=better) [deterministic]: v1=977.99 v2=992.28 ratio b/a=1.015 => ~same
- autoresearch-cost/tx: ratio b/a min=1.00 median=1.00 max=1.07 (geomean=1.01, n=5) => ~same
- autoresearch-ops/tx: ratio b/a min=1.00 median=1.00 max=1.07 (geomean=1.01, n=5) => ~same
- autoresearch-cost/tx[batchRead10]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[readRepeat]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchWrite100]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[singleRMW]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[multiRMW10]: ratio b/a=1.07 (1 point) => WORSE

## Shard Coordinator (ADR-028)

### compare-refs summary

- base: 26365c728b7f1892c0dc1d28c1beea79a82e03e0 (v1)
- target: b789c651741d78f7388dcd71038e95ca095c3974 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)
- each line ends in a `=> better/WORSE/~same` verdict read in that
  metric's own direction, so no axis has to be interpreted by hand
- `autoresearch-*` is **deterministic** (single-client backend ops/tx,
  lower is better) — the most trustworthy signal; `mix-*` cells run
  until their throughput 95% CI reaches --target-ci, so a converged
  ratio is significant — `[unconverged]` marks a cell that hit its time
  cap first (read as indicative); `deadlock-*` stay **[noisy]**

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.99 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- throughput[weak-read]: ratio b/a min=0.99 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- throughput[write]: ratio b/a min=0.99 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.75 median=0.84 max=0.92 (geomean=0.83, n=2) => better
- latency-p50[write]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=4) => ~same
- backend-ops/tx: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=4) => ~same

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- throughput[weak-read]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- throughput[write]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[write]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=0.96 median=0.99 max=1.01 (geomean=0.99, n=4) => ~same
- backend-ops/tx: ratio b/a min=0.96 median=0.99 max=1.01 (geomean=0.99, n=4) => ~same

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.93 median=0.96 max=0.99 (geomean=0.96, n=2) => WORSE
- throughput[weak-read]: ratio b/a min=0.93 median=0.96 max=0.99 (geomean=0.96, n=2) => WORSE
- throughput[write]: ratio b/a min=0.93 median=0.96 max=0.99 (geomean=0.96, n=2) => WORSE
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=1.02 median=1.04 max=1.06 (geomean=1.04, n=2) => WORSE
- latency-p50[write]: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=0.97 median=1.00 max=1.03 (geomean=1.00, n=6) => ~same
- backend-ops/tx: ratio b/a min=0.98 median=1.00 max=1.02 (geomean=1.00, n=6) => ~same

### deadlock

- deadlock-p50 [noisy]: ratio b/a min=0.26 median=0.34 max=16.78 (geomean=0.63, n=6) => better
- deadlock-p90 [noisy]: ratio b/a min=0.24 median=0.26 max=14.16 (geomean=0.51, n=6) => better

### efficiency

- autoresearch-score (cost/tx geomean, lower=better) [deterministic]: v1=875.76 v2=975.78 ratio b/a=1.114 => WORSE
- autoresearch-cost/tx: ratio b/a min=1.00 median=1.00 max=1.69 (geomean=1.11, n=5) => ~same
- autoresearch-ops/tx: ratio b/a min=1.00 median=1.00 max=1.75 (geomean=1.12, n=5) => ~same
- autoresearch-cost/tx[batchWrite100]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchRead10]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[readRepeat]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[multiRMW10]: ratio b/a=1.02 (1 point) => ~same
- autoresearch-cost/tx[singleRMW]: ratio b/a=1.69 (1 point) => WORSE

## Single RW optimization II (ADR-027)

### compare-refs summary

- base: 80724534f0ea9d3b4a1769aea21cdaabe0d9024b (v1)
- target: 26365c728b7f1892c0dc1d28c1beea79a82e03e0 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)
- each line ends in a `=> better/WORSE/~same` verdict read in that
  metric's own direction, so no axis has to be interpreted by hand
- `autoresearch-*` is **deterministic** (single-client backend ops/tx,
  lower is better) — the most trustworthy signal; `mix-*` and
  `deadlock-*` are **[noisy]** (contention-bound, short windows) and
  `[low-sample]` marks a folded cell below the trust floor

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.94 median=0.97 max=1.01 (geomean=0.97, n=2) => WORSE
- throughput[weak-read]: ratio b/a min=0.94 median=0.97 max=1.01 (geomean=0.97, n=2) => WORSE
- throughput[write]: ratio b/a min=0.94 median=0.97 max=1.01 (geomean=0.97, n=2) => WORSE
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.88 median=0.90 max=0.92 (geomean=0.90, n=2) => better
- latency-p50[write]: ratio b/a min=0.99 median=0.99 max=0.99 (geomean=0.99, n=2) => ~same
- retries: ratio b/a min=1.00 median=1.04 max=1.06 (geomean=1.04, n=4) => WORSE
- backend-ops/tx: ratio b/a min=1.00 median=1.04 max=1.05 (geomean=1.03, n=4) => WORSE

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=1.01 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- throughput[weak-read]: ratio b/a min=1.01 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- throughput[write]: ratio b/a min=1.01 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=0.98 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.92 median=0.96 max=1.00 (geomean=0.96, n=2) => better
- latency-p50[write]: ratio b/a min=0.97 median=0.98 max=1.00 (geomean=0.98, n=2) => ~same
- retries: ratio b/a min=1.05 median=1.05 max=1.05 (geomean=1.05, n=3) => WORSE
- backend-ops/tx: ratio b/a min=1.04 median=1.05 max=1.05 (geomean=1.05, n=3) => WORSE

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=1.00 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- throughput[weak-read]: ratio b/a min=1.00 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- throughput[write]: ratio b/a min=1.00 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=0.99 median=1.00 max=1.02 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.96 median=0.99 max=1.02 (geomean=0.99, n=2) => ~same
- latency-p50[write]: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=1.00 median=1.00 max=1.02 (geomean=1.00, n=5) => ~same
- backend-ops/tx: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=5) => ~same

### deadlock

- deadlock-p50 [noisy]: ratio b/a min=0.03 median=0.96 max=1.05 (geomean=0.54, n=6) => better
- deadlock-p90 [noisy]: ratio b/a min=0.03 median=1.02 max=1.05 (geomean=0.56, n=6) => ~same

### mixbench

- mix-tps[roMulti] [noisy]: ratio b/a min=0.59 median=0.84 max=0.96 (geomean=0.79, n=4) n_min=1034 => WORSE
- mix-tps[roSingle] [noisy]: ratio b/a min=0.82 median=0.88 max=0.94 (geomean=0.88, n=4) n_min=2229 => WORSE
- mix-tps[rwMany] [noisy] [low-sample]: ratio b/a min=0.65 median=0.93 max=1.16 (geomean=0.90, n=4) n_min=17 => WORSE
- mix-tps[rwSingle] [noisy] [low-sample]: ratio b/a min=1.23 median=1.61 max=1.97 (geomean=1.58, n=4) n_min=86 => better
- mix-ops/tx[hi/roMulti]: ratio b/a=1.33 (1 point) n_min=1034 => WORSE
- mix-ops/tx[hi/roSingle]: ratio b/a=1.09 (1 point) n_min=3043 => WORSE
- mix-ops/tx[hi/rwMany] [low-sample]: ratio b/a=0.95 (1 point) n_min=17 => better
- mix-ops/tx[hi/rwSingle] [low-sample]: ratio b/a=0.89 (1 point) n_min=86 => better
- mix-ops/tx[lo/roMulti]: ratio b/a=1.01 (1 point) n_min=1217 => ~same
- mix-ops/tx[lo/roSingle]: ratio b/a=1.01 (1 point) n_min=2229 => ~same
- mix-ops/tx[lo/rwMany] [low-sample]: ratio b/a=0.99 (1 point) n_min=129 => ~same
- mix-ops/tx[lo/rwSingle] [low-sample]: ratio b/a=1.37 (1 point) n_min=865 => WORSE
- mix-retries/tx[hi] [noisy]: ratio b/a min=0.83 median=1.04 max=1.73 (geomean=1.11, n=4) => WORSE
- mix-retries/tx[lo] [noisy]: ratio b/a min=0.69 median=0.91 max=1.87 (geomean=1.01, n=4) => better
- mix-agg-ops/tx[hi]: ratio b/a=1.05 (1 point) => WORSE
- mix-agg-ops/tx[lo]: ratio b/a=1.06 (1 point) => WORSE

### efficiency

- autoresearch-score (cost/tx geomean, lower=better) [deterministic]: v1=958.84 v2=873.14 ratio b/a=0.911 => better
- autoresearch-cost/tx: ratio b/a min=0.68 median=1.00 max=1.00 (geomean=0.91, n=5) => ~same
- autoresearch-ops/tx: ratio b/a min=0.66 median=1.00 max=1.00 (geomean=0.91, n=5) => ~same
- autoresearch-cost/tx[singleRMW]: ratio b/a=0.68 (1 point) => better
- autoresearch-cost/tx[multiRMW10]: ratio b/a=0.92 (1 point) => better
- autoresearch-cost/tx[batchWrite100]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchRead10]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[readRepeat]: ratio b/a=1.00 (1 point) => ~same

## Single RW optimization (ADR-020)

The end of implementation of ADR-020, which optimizes single read/write
transactions.

### compare-refs summary

- base: 55b5f7f72ef2919af41faefb4a3681c03349cb15 (v1)
- target: 80724534f0ea9d3b4a1769aea21cdaabe0d9024b (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.91 median=0.98 max=1.02 (geomean=0.97)
- throughput[weak-read]: ratio b/a min=0.91 median=0.98 max=1.02 (geomean=0.97)
- throughput[write]: ratio b/a min=0.91 median=0.98 max=1.02 (geomean=0.97)
- latency-p50[strong-read]: ratio b/a min=0.99 median=1.00 max=1.02 (geomean=1.00)
- latency-p50[weak-read]: ratio b/a min=1.00 median=1.00 max=1.10 (geomean=1.02)
- latency-p50[write]: ratio b/a min=0.99 median=1.01 max=1.02 (geomean=1.01)
- retries: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00)
- backend-ops/tx: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00)

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.99 median=1.00 max=1.03 (geomean=1.00)
- throughput[weak-read]: ratio b/a min=0.99 median=1.00 max=1.03 (geomean=1.00)
- throughput[write]: ratio b/a min=0.99 median=1.00 max=1.03 (geomean=1.00)
- latency-p50[strong-read]: ratio b/a min=0.97 median=0.99 max=1.00 (geomean=0.99)
- latency-p50[weak-read]: ratio b/a min=0.90 median=1.00 max=1.12 (geomean=1.00)
- latency-p50[write]: ratio b/a min=0.99 median=1.00 max=1.00 (geomean=0.99)
- retries: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00)
- backend-ops/tx: ratio b/a min=0.98 median=1.00 max=1.02 (geomean=1.00)

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.92 median=0.97 max=1.01 (geomean=0.97)
- throughput[weak-read]: ratio b/a min=0.92 median=0.97 max=1.01 (geomean=0.97)
- throughput[write]: ratio b/a min=0.92 median=0.97 max=1.01 (geomean=0.97)
- latency-p50[strong-read]: ratio b/a min=0.98 median=1.00 max=1.00 (geomean=1.00)
- latency-p50[weak-read]: ratio b/a min=1.00 median=1.00 max=1.05 (geomean=1.01)
- latency-p50[write]: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00)
- retries: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00)
- backend-ops/tx: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00)

### deadlock

- deadlock-p50: ratio b/a min=0.85 median=0.95 max=1.02 (geomean=0.94)
- deadlock-p90: ratio b/a min=0.89 median=0.98 max=1.02 (geomean=0.97)

### mixbench

- mix-tps[roMulti]: ratio b/a min=0.83 median=1.09 max=1.15 (geomean=1.03)
- mix-tps[roSingle]: ratio b/a min=0.78 median=1.05 max=1.15 (geomean=1.00)
- mix-tps[rwMany]: ratio b/a min=0.53 median=1.16 max=1.19 (geomean=0.96)
- mix-tps[rwSingle]: ratio b/a min=0.41 median=0.80 max=1.36 (geomean=0.77)
- mix-ops/tx[hi/roMulti]: ratio b/a min=1.07 median=1.07 max=1.07 (geomean=1.07)
- mix-ops/tx[hi/roSingle]: ratio b/a min=1.05 median=1.05 max=1.05 (geomean=1.05)
- mix-ops/tx[hi/rwMany]: ratio b/a min=0.98 median=0.98 max=0.98 (geomean=0.98)
- mix-ops/tx[hi/rwSingle]: ratio b/a min=0.98 median=0.98 max=0.98 (geomean=0.98)
- mix-ops/tx[lo/roMulti]: ratio b/a min=0.99 median=0.99 max=0.99 (geomean=0.99)
- mix-ops/tx[lo/roSingle]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00)
- mix-ops/tx[lo/rwMany]: ratio b/a min=0.98 median=0.98 max=0.98 (geomean=0.98)
- mix-ops/tx[lo/rwSingle]: ratio b/a min=0.63 median=0.63 max=0.63 (geomean=0.63)
- mix-retries/tx[hi]: ratio b/a min=0.89 median=1.11 max=1.34 (geomean=1.10)
- mix-retries/tx[lo]: ratio b/a min=0.86 median=1.00 max=1.67 (geomean=1.09)
- mix-agg-ops/tx[hi]: ratio b/a min=0.96 median=0.96 max=0.96 (geomean=0.96)
- mix-agg-ops/tx[lo]: ratio b/a min=0.87 median=0.87 max=0.87 (geomean=0.87)

### efficiency

- autoresearch-score: v1=924.85 v2=942.15 ratio=1.019
- autoresearch-cost/tx: ratio b/a min=0.87 median=1.00 max=1.27 (geomean=1.02)
- autoresearch-ops/tx: ratio b/a min=0.88 median=1.00 max=1.29 (geomean=1.03)

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

Designed in [ADR-024](../adr/024-hold-and-wait-conflict-resolution.md).

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

Described in [object-storage-native.md](../designs/object-storage-native.md) and implemented by
ADRs (016 - 023).

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
