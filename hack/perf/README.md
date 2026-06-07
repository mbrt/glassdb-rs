# Profiling glassdb-rs (hack/perf)

A small, general-purpose CPU-profiling recipe. It is a **diagnostic aid only**:
it changes no source, is not part of any test or benchmark metric, and is not
part of the autoresearch correctness gate. Use it whenever you want to see where
CPU time goes - guiding the autoresearch CPU/allocation work is one use, but it
is not tied to that loop.

## Usage

```bash
hack/perf/profile.sh                 # flamegraph of the autoresearch harness
TARGET=bench hack/perf/profile.sh    # flamegraph of the transactions bench
make flamegraph                      # same as the default invocation
```

Artifacts are written under `hack/perf/` (and are gitignored): `flamegraph.svg`
(open in a browser) and, when the collapse tool is available,
`flamegraph.folded` (a greppable, text-readable stack collapse).

### Tunables (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `TARGET` | `autoresearch` | What to profile: `autoresearch` or `bench`. |
| `COUNT` | `50` | Suite repeats for the `autoresearch` target; more repeats give more samples and a less noisy profile. |
| `OUT` | `hack/perf` | Output directory for artifacts. |

## Targets

- **`autoresearch`** - the single-client scoring harness
  (`glassdb-bench-score`), run against the in-memory backend. Best for the
  CPU/allocation hot spots that show up as the loop's secondary axes. See the
  caveat below.
- **`bench`** - the `transactions` Criterion microbenchmark (`glassdb`), whose
  `DelayBackend` injects a compressed S3/GCS latency profile. Its profile is
  closer to the real, round-trip-bound cost model. The profiler attaches to the
  compiled benchmark binary, so the frozen benchmark source is never edited.

## The in-memory caveat

The `autoresearch` harness uses the in-memory backend, while real glassdb cost
is dominated by object-storage round-trips (the metric weights each backend op
at ~31-70ms). An in-memory flamegraph therefore over-weights the codec,
allocator, and harness machinery and under-represents the paths that actually
dominate in production. Read an `autoresearch` profile as a guide to the
CPU/allocation tie-breakers only; for a production-shaped picture, profile the
`bench` target instead.

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
