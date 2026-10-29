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
| `experiments/` | The experiment files, each for `--experiment`, in a directory for the time mode that they need. [Experiments](#experiments) lists them. |
| `configs/` | Files that experiment files include. |

## Experiments

The directory of an experiment in `hack/perf/experiments` tells which time mode
it needs:

- `default/`: the experiments of the CI comparison and of `make bench`. They
  run in virtual time, so that two revisions compare with results that repeat.
  Some of them do not give the results of real time, for example
  `rmw_shared_leaf_three_transactions` (see `--time=virtual` in the crate
  README).
- `any-time/`: experiments that show the same behavior in virtual time and in
  real time. Their absolute values can differ, because latencies in virtual
  time have no timer rounding.
- `real-time/`: experiments that need real time. In virtual time, CPU takes no
  model time, and identical workers batch better (see `--time=virtual` in the
  crate README), so they show a different behavior.

Put a new experiment in `any-time/` when it shows the same behavior in virtual
time, so that fast runs can use it.

perfbench does not check the time mode. `make bench-all` runs all experiments in
real time, and `make bench-all TIME=virtual` runs `default/` and `any-time/` in
virtual time.

| Experiment | Directory | What it measures |
| --- | --- | --- |
| The cases of [Default experiments](#default-experiments) | `default` | Single transactions of one worker with warm caches, and their backend costs, and four shapes at the same time on one leaf. Each case has a fixed length of 10 s. |
| `inline-pressure` | `any-time` | How leaves adapt when their aggregate inline budget is full (ADR-056): passes over the next keys until one pass commits without locks. |
| `contention` | `any-time` | Five writers whose transactions share some of their 1 to 6 keys. |
| `split-merge-fight` | `real-time` | A stable load under which the topology policies of three instances disagree, with the `avoidable`, `fixed-1`, and `fixed-2` policies. |
| `membership-churn` | `real-time` | Creates and deletes of keys in one leaf, with direct and locked commits, while another instance scans the leaf. |
| `mixed` | `real-time` | Four shapes (`rw-single`, `rw-many`, `ro-single`, `ro-multi`) at the same time, with low and high conflict, at five collection affinities. |
| `workers` | `real-time` | The worker grid of the canonical plots: 1 to 200 workers per shape, each count after the warm-up of the count before it. |
| `affinity` | `real-time` | The affinity grid of the canonical plots: 5 affinities × 1, 3, 5, and 7 database instances. |
| `load-shift` | `real-time` | A change of the load on one tree: independent keys, then keys that cross leaf boundaries, then independent keys again. Each warm-up shows whether the topology becomes quiet again. |
| `false-sharing` | `real-time` | Four instances that each write one of four adjacent keys of one leaf, while one more instance reads and writes the keys around them (ADR-084), with three topology policies. |
| `fixed-ranges` | `real-time` | Transactions on fixed ranges of 16 and 64 keys, with transactions on single keys (ADR-084). |
| `random-ranges` | `real-time` | Transactions on ranges of 16 and 64 adjacent keys from random starts, on fixed sets of 3 keys, and on single keys (ADR-084). |

The files in `hack/perf/configs` are not experiments:

- `common-workloads.toml`: the pools, shapes, and workloads of the mixed
  workloads (`low-conflict`, `high-conflict`, and `crossing`).
- `default.toml`: the settings and the parts of the default experiments.

### Mixed workloads

In the `low-conflict` workload, each worker selects its keys from its own
stripe of 5,000 keys, so transactions seldom conflict, but they share leaves.
In `high-conflict`, all workers share 8 keys. The stripes of a pool must give
each worker at least the keys of a transaction; the loader rejects a smaller
pool.

### Default experiments

The experiments of `hack/perf/experiments/default` replace the Criterion suite
of `glassdb`. Each one is a case of the CI comparison. Except for the last one,
each case runs one worker on one instance with warm caches. A case warms up for
1250 ms and measures for 10 s of model time. The cases include
`hack/perf/configs/default.toml`, which has their settings and their parts.
Most cases use built-in parts:

| Case | Shape |
| --- | --- |
| `warm_read` | A read of one key with a 256-byte value. |
| `warm_read_external` | A read of a 1,025-byte value, which a transaction record stores. |
| `fresh_client_read` | A read on a new database instance, which has no cache (the scenario kind `fresh-client-read`). The open is not in the time. |
| `rmw_inline_1024` | A read-modify-write of one key with a 1,024-byte value, at the default inline limit. |
| `rmw_external_1025` | The same with a 1,025-byte value, above the limit. |
| `rmw_five_leaves` | One key in each of five collections (`collections = 5`). |
| `read_long_keys_large_collection` | A read in a collection of 1,024 keys of 256 bytes (`key-bytes = 256`), after its splits. |
| `rmw_shared_leaf_three_transactions` | Three workers that update their own key of one leaf. |
| `shapes_shared_leaf` | Four shapes (`rw-single`, `rw-many`, `ro-single`, `ro-multi`) at the same time on one leaf, with `low-conflict` keys. |

The `costs` of each cell are the numbers that the CI comparison checks.

## Plotting results

### Canonical sweep plots

The canonical plots come from the `workers` and `affinity` experiments, in
`hack/perf/out-sweeps/`. They use the local S3 model. `--prefix-depth=3` gives
each collection its own simulated S3 request-rate limit:

```bash
cargo build --release -p glassdb-bench-scale --bin perfbench
common=(--backend=memory --delays=s3 --delay-scale=0.2 --prefix-depth=3
  --runs=3 --drain-timeout=90s)
e=hack/perf/experiments/real-time
target/release/perfbench "${common[@]}" --experiment=$e/workers.toml \
  --output=hack/perf/out-sweeps/workers.json
target/release/perfbench "${common[@]}" --experiment=$e/affinity.toml \
  --output=hack/perf/out-sweeps/affinity.json
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

A mixed experiment (`mixed`, `ci`, `workers`, or `affinity`) must sweep exactly
one of affinity or workers; it gets one panel for each conflict level (`lo` and
`hi`). A `contention` experiment shows the 100% overlap cells by contended keys.
A file with several experiments, such as the result of `make bench-all`, gets
the figures of each experiment that has them, named
`<file>-<experiment>-*.png`. `--canonical` reads the `workers` and `affinity`
experiments, so `--workers` and `--affinity` can be the same file.
Shapes that did not converge are plotted with a warning. Both `plot_results.py`
and `compare.py` read result files through `perfbench_results.py`, which owns the
JSON schema. `make test-bench` runs their tests.

## Comparing revisions

`compare_refs.sh` uses the same comparison driver as CI. It copies the
candidate's `glassdb-bench-scale` crate into disposable snapshots of both
revisions, then builds each against its own engine. Both sides run the
experiments of `hack/perf/experiments/default` of the candidate. The current
source tree is not changed. Unsupported engine APIs fail the comparison; old
workloads are not silently substituted.

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
local model run, call `compare.py` directly. It reads the `mixed`,
`contention`, and `inline-pressure` experiments of each directory, from
`<experiment>.json` or from any other result file of the directory that has the
experiment, such as the result of `make bench-all`. It compares the experiments
that both directories have.

```bash
hack/perf/compare.py --a hack/aws-bench/out --label-a s3 --b out-model --label-b model \
  --out /tmp/cmp-plots --summary-out /tmp/cmp-plots/summary.md
```

It prints tables of the ratio b/a and a verdict for each metric, and draws one
set of overlay figures per experiment: `cmp-mixed-*.png` and
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
| `EXPERIMENT` | `hack/perf/experiments/default` | The perfbench experiment file or directory. |
| `CASE` | all cases | One experiment of `hack/perf/experiments/default`, in place of `EXPERIMENT`. |
| `SECONDS_PER_CASE` | `10` | Measured wall time of each default experiment. |
| `OUT` | `hack/perf` | Output directory for artifacts. |

### Target

The script profiles perfbench experiments, by default the experiments of
`hack/perf/experiments/default`: the single transactions of one worker with
warm caches, and four shapes at the same time on one leaf. The seed and the
warm-up appear in the profile; use the transaction stacks to inspect measured
work. `CASE` selects one of these experiments.

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
