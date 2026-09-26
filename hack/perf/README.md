# Profiling glassdb-rs (hack/perf)

A CPU-profiling recipe. Use it to identify where CPU time goes. The profiler
attaches to a compiled benchmark executable. The
[topology policy report](#topology-policy-report) compares the topology
policies of ADR-074 in perfbench.

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
`flamegraph.folded` (folded stacks: one semicolon-separated call stack and its
sample count per line).

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

## Topology policy report

`plot-topology.py` renders perfbench `topology` and `mixed` results as one HTML
report. The report compares each policy with a baseline of the same file and
run: `fixed` for `topology`, and `size` for `mixed`. The JSON does not record
the delay model, so each file name must contain `-s3-` or `-gcs-`.

```bash
perfbench() { cargo run --release -p glassdb-bench-scale --bin perfbench -- "$@"; }
out=hack/perf/out
mkdir -p $out
for delays in s3 gcs; do
  for leaf in 16 128; do
    perfbench --delays=$delays --runs=3 --output=$out/topo-$delays-L$leaf.json \
      topology --workloads=single,hot,adjacent,random,scan --workers=8 \
      --databases=1,4 --leaf-sizes=$leaf --num-keys=1024 --duration=10s \
      --max-duration=30s --split-settle-timeout=600s --split-quiet=5s \
      --policies=fixed,avoidable --adapt=20s
  done
  for databases in 1 2 4 8; do
    perfbench mixed --delays=$delays --databases=$databases \
      --workers-per-shape=8 --affinities=0,50,100 --modes=lo,hi \
      --policies=size,avoidable --warmup=20s --duration=5s --max-duration=30s \
      --split-quiet=5s --split-settle-timeout=300s \
      --output=$out/mixed-$delays-db$databases.json
  done
done
hack/perf/plot-topology.py $out/*.json --output $out/report.html --image-dir $out/img
```

An input `PATH=POLICY,POLICY` keeps only those policies and the baselines. Run
`hack/perf/test_plot_topology.py` after a change to the script.
