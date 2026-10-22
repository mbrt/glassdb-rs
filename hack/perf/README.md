# Performance tools (hack/perf)

Tools that plot, compare, and profile `perfbench` runs. `perfbench` and its
experiments are explained in
[crates/glassdb-bench-scale/README.md](../../crates/glassdb-bench-scale/README.md),
and the real-S3 runner in [hack/aws-bench/README.md](../aws-bench/README.md).
Manual experiments that use these tools are recorded in
[`investigations.md`](investigations.md).

| File | Purpose |
| --- | --- |
| `plot_results.py` | Plots result files: the canonical sweeps, single files, and overlays. |
| `compare.py` | Compares two directories of result files. |
| `compare_refs.sh` | Compares two revisions with the CI comparison driver. |
| `perfbench_results.py` | Reads the result files; the other tools use it. |
| `profile.sh` | Records a CPU flamegraph of one experiment. |

## Plotting results

### Canonical sweep plots

The canonical plots come from the `workers` and `affinity` experiments, in
`hack/perf/out-sweeps/`. The crate README has the commands. Then:

```bash
uv run hack/perf/plot_results.py --canonical
```

The canonical mode needs three clean, converged runs. Throughput shows the p50
of the throughput windows as a line and their p10 to p90 range as a band.
Latency shows the p50 as a line and the p50 to p90 range as a band. Both use the
median over the runs. Each affinity figure has one panel for each database
instance count, with all shapes. The four figures go to `hack/perf/out-sweeps/`.

### Plotting any result file

`plot_results.py` also plots single result files, such as the ones in
`hack/aws-bench/out/` from a real S3 run. It picks the figures from the experiment in each file and
writes them beside the file unless `--out` says otherwise:

```bash
uv run hack/perf/plot_results.py hack/aws-bench/out/mixed.json \
  hack/aws-bench/out/contention.json
```

A file of a mixed experiment (`mixed`, `ci`, `workers`, or `affinity`) must
sweep exactly one of affinity or workers; it gets one panel for each conflict
level (`lo` and `hi`). A `contention` file shows the 100% overlap cells by
contended keys.
Shapes that did not converge are plotted with a warning. Both `plot_results.py`
and `compare.py` read result files through `perfbench_results.py`, which owns the
JSON schema. `make test-bench` runs their tests.

## Comparing revisions

`compare_refs.sh` uses the same comparison driver as CI. It copies the
candidate's `glassdb-bench-scale` crate into disposable snapshots of both
revisions, then builds each against its own engine. It runs the `ci` and
`diagnostics` experiments. The current source tree is not
changed. Unsupported engine APIs fail the comparison; old workloads are not
silently substituted.

```bash
# main against the current worktree
hack/perf/compare_refs.sh

# compatibility spelling for the same bounded comparison
hack/perf/compare_refs.sh --summary

# explicit references
BASE=main TARGET=my-branch OUT=/tmp/glassdb-comparison \
  hack/perf/compare_refs.sh
```

### Comparing result directories

To compare result files that already exist, such as a real S3 run against a
local model run, call `compare.py` directly. Each directory holds
`mixed.json`, `contention.json`, or `inline-pressure.json`, as perfbench writes
them with `--output`; it compares the files that both directories have.

```bash
hack/perf/compare.py --a hack/aws-bench/out --label-a s3 --b out-model --label-b model \
  --out /tmp/cmp-plots --summary-out /tmp/cmp-plots/summary.md
```

It prints tables of the ratio b/a and a verdict for each metric, and draws one
set of overlay figures per scenario: `cmp-mixed-*.png` and
`cmp-contention-latency.png`. Draw the overlays alone with
`plot_results.py --out DIR --series s3=hack/aws-bench/out/mixed.json --series model=out-model/mixed.json`.

## CPU profiling

A CPU-profiling recipe. Use it to identify where CPU time goes. The profiler
attaches to the compiled `perfbench` executable.

### Usage

```bash
hack/perf/profile.sh
CASE=rmw_inline_1024 hack/perf/profile.sh
make flamegraph
```

Artifacts are written under `hack/perf/` (and are gitignored): `flamegraph.svg`
(open in a browser) and, when the collapse tool is available,
`flamegraph.folded` (folded stacks: one semicolon-separated call stack and its
sample count per line).

#### Tunables (env)

| Var | Default | Meaning |
|-----|---------|---------|
| `EXPERIMENT` | `diagnostics` | The perfbench experiment. |
| `CASE` | all cases | One case of the `diagnostics` experiment. |
| `SECONDS_PER_CASE` | `10` | Measured time of each `diagnostics` case. |
| `OUT` | `hack/perf` | Output directory for artifacts. |

### Target

The script profiles one perfbench experiment, by default `diagnostics`: the
single transactions of one worker with warm caches. The seed and the warm-up
appear in the profile; use the transaction stacks to inspect measured work.
`CASE` selects one case of the experiment.

### The in-memory caveat

The experiments use the memory backend with the S3 delay profile and a 5×
model clock. A CPU profile explains local execution costs; it does not predict
object-storage latency or waiting time. Use real-provider measurements for
workload behavior.

### Profiler

[`cargo-flamegraph`](https://github.com/flamegraph-rs/flamegraph) (`cargo
install flamegraph`) renders the SVG via Linux `perf`. Optionally install
[`inferno`](https://github.com/jonhoo/inferno) (`cargo install inferno`) to also
get the greppable `flamegraph.folded`.

Builds use the dedicated `profiling` Cargo profile (release optimizations with
debug symbols retained) so stacks are both fast and readable.

#### Linux perf permissions

`perf`-based profiling needs kernel access. If a run fails, relax the limits:

```bash
sudo sysctl kernel.perf_event_paranoid=1
sudo sysctl kernel.kptr_restrict=0   # if stacks show only raw addresses
```
