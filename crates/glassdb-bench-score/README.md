# Backend-operation benchmark

This crate provides the `bench-score` executable used by the performance CI
workflow and comparison tools.

Run the benchmark from the repository root:

```bash
make bench-score
cargo run --release -p glassdb-bench-score --bin bench-score -- --json --count 3
```

The executable reports a weighted backend-operation score and allocation, CPU,
and elapsed-time measurements. It uses fixed workloads with one client and a 1
ms backend delay. Use the score as a diagnostic.

The separate [`glassdb-bench-scale`](../glassdb-bench-scale/) crate provides
concurrency and throughput benchmarks. See
[`hack/aws-bench`](../../hack/aws-bench/README.md) for comparisons and
[`hack/perf`](../../hack/perf/README.md) for CPU profiling.
