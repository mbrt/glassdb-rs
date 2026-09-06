# Profiling glassdb-rs (hack/perf)

A CPU-profiling recipe. Use it to identify where CPU time goes. The profiler
attaches to a compiled benchmark executable.

Manual experiments that use this or other performance tooling are recorded in
[`investigations.md`](investigations.md).

## Usage

```bash
hack/perf/profile.sh
FILTER=diagnostic/rmw_inline_1024 hack/perf/profile.sh
make flamegraph
```

Artifacts are written under `hack/perf/` (and are gitignored): `flamegraph.svg`
(open in a browser) and, when the collapse tool is available,
`flamegraph.folded` (a greppable, text-readable stack collapse).

### Tunables (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `FILTER` | `diagnostic` | Criterion case filter. |
| `SECONDS_PER_CASE` | `10` | Profiling duration per selected case. |
| `OUT` | `hack/perf` | Output directory for artifacts. |

## Target

The script profiles the self-contained `diagnostics` Criterion target.
Preparation and the cost pass appear in the profile; use the transaction
stacks to inspect measured work.
`FILTER` selects cases within this target.

## The in-memory caveat

These diagnostics use memory without provider delay and a 20× engine model
clock. A CPU profile explains local execution costs; it does not predict
object-storage latency or waiting time. Use `perfbench` and real-provider
measurements for workload behavior.

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
