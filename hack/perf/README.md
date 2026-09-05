# Profiling glassdb-rs (hack/perf)

A CPU-profiling recipe. Use it to identify where CPU time goes. The profiler
attaches to a compiled benchmark executable.

Manual experiments that use this or other performance tooling are recorded in
[`investigations.md`](investigations.md).

## Usage

```bash
hack/perf/profile.sh                 # flamegraph of the bench-score harness
TARGET=bench hack/perf/profile.sh    # flamegraph of the transactions bench
make flamegraph                      # same as the default invocation
```

Artifacts are written under `hack/perf/` (and are gitignored): `flamegraph.svg`
(open in a browser) and, when the collapse tool is available,
`flamegraph.folded` (a greppable, text-readable stack collapse).

### Tunables (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `TARGET` | `bench-score` | What to profile: `bench-score` or `bench`. |
| `COUNT` | `50` | Suite repeats for the `bench-score` target; more repeats give more samples and a less noisy profile. |
| `OUT` | `hack/perf` | Output directory for artifacts. |

## Targets

- **`bench-score`** - the single-client scoring harness
  (`glassdb-bench-score`), run against a latency-stabilized in-memory backend.
  Use it to inspect CPU use and allocations. See the caveat below.
- **`bench`** - the `transactions` Criterion microbenchmark (`glassdb`), whose
  `DelayBackend` injects a compressed S3/GCS latency profile. Its profile is
  closer to the real, round-trip-bound cost model. The profiler attaches to the
  compiled benchmark binary.

## The in-memory caveat

The `bench-score` harness injects a fixed 1 ms delay over the in-memory backend
to stabilize deferred work, while real glassdb cost is dominated by much longer
and variable object-storage round-trips (the metric weights each backend op at
~31-70ms). Its flamegraph therefore still over-weights the codec, allocator,
and harness machinery and under-represents the paths that actually dominate in
production. Read a `bench-score` profile as a guide to the CPU/allocation
tie-breakers only; for a production-shaped picture, profile the `bench` target
instead.

## Profiler

[`cargo-flamegraph`](https://github.com/flamegraph-rs/flamegraph) (`cargo
install flamegraph`) renders the SVG via Linux `perf`. Optionally install
[`inferno`](https://github.com/jonhoo/inferno) (`cargo install inferno`) to also
get the greppable `flamegraph.folded`.

Builds use the dedicated `profiling` Cargo profile (release optimizations with
debug symbols retained) so stacks are both fast and readable.

### Linux perf permissions

`perf`-based profiling needs kernel access. If a run fails, relax the limits:

```bash
sudo sysctl kernel.perf_event_paranoid=1
sudo sysctl kernel.kptr_restrict=0   # if stacks show only raw addresses
```
