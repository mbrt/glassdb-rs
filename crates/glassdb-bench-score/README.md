# Backend-operation benchmark

This crate provides the `bench-score` executable used by the performance CI
workflow and comparison tools.

Run the benchmark from the repository root:

```bash
make bench-score
cargo run --release -p glassdb-bench-score --bin bench-score -- --json --count 3
```

The executable reports a weighted backend-operation score and allocation, CPU,
and elapsed-time measurements. It uses fixed workloads with one client,
eight-byte values, and an in-memory backend with a fixed 1 ms operation delay.

The score is a narrow diagnostic for backend-operation counts in these
workloads, not a general measure of database performance. Measurements end
before database shutdown, so they exclude background work completed during
shutdown. The fixed delay does not model payload-dependent transfer time. Do not
use the score alone to accept or reject changes, or predict object-storage
latency and throughput in general.

The separate [`glassdb-bench-scale`](../glassdb-bench-scale/) crate provides
concurrency and throughput benchmarks. See
[`hack/aws-bench`](../../hack/aws-bench/README.md) for comparisons and
[`hack/perf`](../../hack/perf/README.md) for CPU profiling.
