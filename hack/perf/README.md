# Profiling glassdb-rs (hack/perf)

A CPU-profiling recipe. Use it to identify where CPU time goes. The profiler
attaches to the compiled `perfbench` executable.

Manual experiments that use this or other performance tooling are recorded in
[`investigations.md`](investigations.md).

## Usage

```bash
hack/perf/profile.sh
CASE=rmw_inline_1024 hack/perf/profile.sh
make flamegraph
```

Artifacts are written under `hack/perf/` (and are gitignored): `flamegraph.svg`
(open in a browser) and, when the collapse tool is available,
`flamegraph.folded` (folded stacks: one semicolon-separated call stack and its
sample count per line).

### Tunables (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `EXPERIMENT` | `diagnostics` | The perfbench experiment. |
| `CASE` | all cases | One case of the `diagnostics` experiment. |
| `SECONDS_PER_CASE` | `10` | Measured time of each `diagnostics` case. |
| `OUT` | `hack/perf` | Output directory for artifacts. |

## Target

The script profiles one perfbench experiment, by default `diagnostics`: the
single transactions of one worker with warm caches. The seed and the warm-up
appear in the profile; use the transaction stacks to inspect measured work.
`CASE` selects one case of the experiment.

## The in-memory caveat

The experiments use the memory backend with the S3 delay profile and a 5×
model clock. A CPU profile explains local execution costs; it does not predict
object-storage latency or waiting time. Use real-provider measurements for
workload behavior.

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
