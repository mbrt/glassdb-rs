# perfbench

`perfbench` is one runner for all database-level benchmarks. A new benchmark is
a configuration of built-in parts. A scenario that the built-in parts cannot
express adds one part in Rust, and the configuration uses it by name.

An experiment is a TOML file. The experiments of the repository are in
[hack/perf/experiments](../../hack/perf/experiments), and the files that they
share are in [hack/perf/configs](../../hack/perf/configs). The built-in
defaults are in
[src/bin/perfbench/runner/builtin.toml](src/bin/perfbench/runner/builtin.toml).
Raw backend latency is measured by `backendbench`, in the same crate.

## Layers

1. **Runner** (`runner/`). It reads the configurations, runs the loop of cells,
   the seeds, the warm-ups, and the measurements, and writes the result. It
   knows no scenario.
2. **Parts.** Each part has a name and its own parameters, and the
   configuration selects it by name:

   | Part | What it does | Built-in parts |
   | --- | --- | --- |
   | Kind (`runner/kinds.rs`) | Runs one transaction with the selected keys. | `read-modify-write`, `read`, `write`, `scan`, `create-delete` |
   | Key source (`runner/sources.rs`) | Selects the keys of one transaction. | `stripe`, `pool`, `adjacent`, `index`, `sequence`, `overlap` |
   | End condition (`runner/until.rs`) | Decides when a measurement ends. | `converged`, `duration`, `passes` |
   | Axis generator | Gives the values of a coupled axis. | none |

3. **Scenario parts** (`scenarios/`). A part for one scenario is one Rust type
   in the file of its scenario, registered by name in `scenarios/mod.rs`. Its
   parameters are a serde type, so the loader checks them like the parameters
   of a built-in part. The runner, the loop, and the result do not change. The
   scenario parts are the kind `fresh-client-read`, the end condition
   `no-locked-pass`, and the axis generator `contention-overlaps`.

A scenario never needs its own subcommand.

## Terms

| Term | Meaning |
| --- | --- |
| Configuration | The merged layers of one experiment: the built-in defaults, the `--config` files, the files that the experiment file includes, the experiment file, and `--set`. |
| Parameter | One value in the configuration, named by its path, for example `cell.workers` or `shapes.rw-many.keys`. |
| Seed parameter | A parameter that changes what the seed writes or how the seed database opens. |
| Cell parameter | A parameter that changes only the load, the databases, or the timing of a cell. |
| Part | A named piece of code that the configuration selects: a kind, a key source, an end condition, or an axis generator. |
| Kind | A part that runs one transaction. |
| Key source | A part that selects the keys of a transaction from a pool. |
| Shape | A named kind with its parameters, its pool, and its key source. |
| Group | Database instances that run the same shapes. The result reports each group and each shape apart. |
| Workload | A named set of groups that run at the same time. |
| Experiment | A TOML file with parameter values and axes. Its name is the file name without `.toml`. One invocation runs one or more experiments, one after the other. |
| Axis | An ordered list of values for one or more parameters, with a start rule. |
| Cell | One warm-up and one measurement, with one value of each parameter. |
| Run | One repetition of a whole experiment (`--runs`). |

"Experiment" and "run" are the design-of-experiments terms for a design and its
replicates. An axis whose values set more than one parameter is a factor with
composite levels.

## Command line

```bash
cargo build --release -p glassdb-bench-scale --bin perfbench
e=hack/perf/experiments
target/release/perfbench --experiment $e/real-time/mixed.toml
target/release/perfbench --experiment $e/real-time/split-merge-fight.toml --runs 3
target/release/perfbench --experiment $e/default                # a directory
target/release/perfbench --experiment $e/real-time/*-ranges.toml
target/release/perfbench --config my.toml --experiment probe.toml \
  --set cell.workers=50 --set shapes.rw-many.keys=4
```

- `--experiment PATH` selects an experiment file, or a directory: all its
  `*.toml` files, in name order. It can be given more than once, and the
  experiments run one after the other, into one result. Two experiments with
  the same name are an error. There is no default.
- `--config PATH` loads a file, or the `*.toml` files of a directory, for every
  experiment. It can be given more than once.
- `--set PATH=VALUE` gives one parameter of each experiment a fixed value. It
  can be given more than once, and the last one wins. If an axis sets `PATH`,
  `--set` removes that axis. For a coupled axis, this removes all its paths,
  and they keep the value of the files. `VALUE` is a TOML value, for example
  `--set 'cell.affinity=50'`.
- `--only AXIS=LABEL` keeps only one value of an axis, in each experiment that
  has the axis. It is an error if no experiment has the axis. It can be given
  once for each axis. For example, `--only=phase=pressure` runs one phase of
  `inline-pressure`.

If an experiment fails, the result has the experiments that completed before
it, and perfbench exits with the error. If the first experiment fails, there is
no result.

The options that are the same for all experiments are command-line options:
`--backend`, `--delays`, `--delay-scale`, `--time`, `--prefix-depth`, `--latency-jitter`,
`--enable-throttling`, `--runs`, `--run-cooldown`, `--drain-timeout`,
`--progress`, and `--output`. The result records them, except `--progress`.
`--progress[=auto|always|never]` controls a progress bar on stderr with the
number of the cell and the estimated time left.

All experiments support `--backend=memory|fakes3|s3|gcs`. Real S3 and GCS use
the bucket in `$BUCKET` and always run at real time. For `memory` and `fakes3`,
`--delay-scale` selects the inverse process-wide model-time speedup: backend
latency, rate limits, SDK and engine retries, leases, and background cadence
advance coherently. The parts of an experiment that set what it measures also
use model time: the seed and warm-up waits and timeouts, the measurement
duration and its end conditions, and the timeline. So a run measures the same
model time at each scale. Only the watchdogs (`--drain-timeout` and the limit
of the full GCs) and `--run-cooldown` use wall time, because they protect the
host. The `wallMs` fields of the result are wall time. The `0.2` default keeps
S3-profile sleeps above timer granularity; smaller scales are useful only as
explicitly approximate probes.

`--time=virtual` replaces the compression with virtual time. The process runs
on one thread with a paused Tokio clock, and model time jumps over each wait of
an idle process. The engine and `rt` are the same as in a real-time run. A run takes the time
of its CPU work, not of its model duration. With `--latency-jitter=false`, two
runs of the same revision give nearly the same result: the engine randomizes
the cadence of its background tasks, so a window that holds a pass of the
garbage collector can count one listing more or less, and records with a
timestamp can change their size by a byte with the start time of the process.
This moves the latency of large writes by under 1%. It needs `--backend=memory`
and does not combine with `--delay-scale`: a paused clock also jumps while a
task waits for the operating system, as for the socket of `fakes3`. Latencies
are 10–25% lower than at the 5× scale, because they have no timer rounding, so
a comparison must use one mode on both sides. The
comparison of two revisions in CI uses it, because both sides have the same
effects and the results repeat. Use it also for fast checks and for a first look
at the shape of a sweep. These effects make it unfit for the canonical plots and
for the absolute value of a metric:

- CPU and allocation costs take no model time, so a run shows no saturation of
  the host.
- Identical concurrent workers never drift apart, so their transactions batch
  better than in real time. In the diagnostics case with three transactions on
  one leaf, they always share one coordinator round (0.33 operations per
  transaction, against 0.63 in real time). `--latency-jitter` does not change
  this: the transactions resubmit in the same instant that a round ends, and
  only CPU time separates them in real time.
- The watchdogs use virtual time, so only a wall-time limit on the process
  stops a CPU livelock.

The result records the mode as `timeMode`. Results of different modes are not
comparable.

## Configuration layers

Each experiment has its own configuration. perfbench merges these layers in
this order:

1. the built-in defaults (`[seed]` and `[cell]`);
2. each `--config` file, in the order of the command line;
3. the files that the experiment file includes, in their order;
4. the experiment file;
5. each `--set`.

So an experiment file can change any value, and the change does not apply to
other experiments. The values of an experiment win over the `--config` files.

A file can include other files before its own values:

```toml
include = ["../../configs/default.toml"]
```

The paths are relative to the file. An included file can include other files.
A file that includes itself, directly or through other files, is an error.
`include` works the same in an experiment file and in a `--config` file.

Two files merge in order, and a later value wins, with these rules:

- Tables merge key by key. A later file can change one field of a shape and
  keep its other fields.
- A scalar or a list replaces the earlier value.
- The value of a part selector (`source` of a shape or of a workload,
  `cell.measure.until`) replaces the earlier value as a whole. A later file can
  then change the part, and the parameters of the earlier part do not stay.
- `axes` is a list, so the axes of a later file replace all earlier axes. A
  `--config` file with `axes` gives them to each experiment that has none.
- A key that the schema does not know is an error.

The result records each experiment after `--set`, and the configuration that
it uses, so that a result file is enough to repeat its run.

## Kinds

The built-in kinds:

| Kind | Parameters | Transaction |
| --- | --- | --- |
| `read-modify-write` | `keys`, `value-bytes`, `collections` | Reads `keys` keys and writes each of them. With `collections` > 1, it uses the same keys in that many collections of the pool, from the home collection on. |
| `read` | `keys` | Serializable read of `keys` keys. |
| `write` | `value-bytes` | Writes one key without a read. A single-key write commits direct. |
| `scan` | `span` | Scans `span` keys from the selected key. `span = "all"` scans the whole collection. |
| `create-delete` | `value-bytes`, `locked` | Creates the selected key, and deletes it in the next transaction of the worker. With `locked = true`, the transaction first scans the whole collection, so it cannot commit direct and takes the membership lock. |

`value-bytes` defaults to the `value-bytes` of the pool, `collections` to 1,
`span` to `"all"`, and `locked` to `false`.

## Key sources and pools

```toml
[pools.<name>]
keys = 5000          # integer, or omitted (see below)
seeded = true        # false: the seed creates the collection, but no keys
value-bytes = 128    # size of each value that the seed writes
key-bytes = 8        # minimum size of each key name
```

The keys of a pool have names whose byte order is their index order. A name is
padded to `key-bytes`. Inline or external storage of a value depends on its
size, so the seed writes the values that a scenario needs.

| Key source | Parameters | Keys of one transaction |
| --- | --- | --- |
| `stripe` | `shared-pct` (default 1) | From the worker's own keys: every Nth key of the pool, where N is the workers of all `stripe` and `adjacent` shapes on this pool, over all groups. Each worker has its own offset, so two workers never share own keys. `shared-pct` percent of the transactions select from the whole pool. |
| `pool` | | From all keys of the pool. |
| `adjacent` | | Consecutive keys of the worker's own block of the pool, from a random start. The blocks are distinct for all `stripe` and `adjacent` workers on the pool. |
| `index` | `keys` (list of indices) | Always these keys. |
| `sequence` | `start`, `count`, `order` | One key at a time, in a fixed order over keys `start` to `start + count - 1`. One pass is all of them. `order = "ascending"` or `"alternate-halves"`, which alternates between the lower half and the upper half. |
| `overlap` | `shared` | The first `shared` keys of the pool, which all workers share, then the worker's own block of `keys - shared` keys. |

A shape has `kind`, `pool`, `source`, and the parameters of its kind. A
parameter that the kind or the source does not have is an error. The default
source is `stripe`.

When a pool has no `keys`, its size is what its sources need. Only `index`,
`sequence`, and `overlap` can give a size. A pool that no source can give a size
to must have `keys`.

## Groups and workloads

```toml
[workloads.<name>.groups.<group>]
instances = 1                     # optional, the default is cell.databases
shapes = { rw-single = 8 }        # shape name = its workers, or "cell"
```

- `shapes` maps each shape to the workers of the group. The value `"cell"`
  takes `cell.workers`.
- A group opens `instances` database instances, but at most the largest worker
  count of its shapes, and at least one. The workers of each shape are spread
  as evenly as possible over the instances. Each instance runs all shapes of its
  group.
- Each instance has a home collection. Instance `i` of all groups, counted over
  the groups in the order of the file, has collection `i % cell.collections` of
  each pool.
- A shape with 0 workers runs nothing. A group whose shapes all open their own
  instance for each transaction (the kind `fresh-client-read`) opens none.
- A workload can give a `pool` and a `source` that replace those of all its
  shapes, so that one set of shapes runs on another pool.

## Database parameters

The database options of the seed and of the cells are apart, so that a seed can
make a topology that the cells then keep:

| Path | Default | Meaning |
| --- | --- | --- |
| `<db>.topology` | `"size"` | `"size"` (ADR-031, ADR-056, ADR-073, ADR-084), or `"avoidable"` (ADR-074). |
| `<db>.leaf-max-entries` | engine default | Soft cap on leaf entries. |
| `<db>.leaf-min-entries` | engine default | Underfull threshold of leaves. |
| `<db>.index-min-children` | engine default | Underfull threshold of index nodes. |
| `<db>.inline-max-value-bytes` | engine default | Largest value that a leaf can carry inline. |
| `<db>.inline-max-leaf-bytes` | engine default | Aggregate inline budget of a leaf. |

`<db>` is `seed.database` or `cell.database`. `seed.database.*` are seed
parameters, and `cell.database.*` are cell parameters. The seed waits until its
splits and merges stop, so a seed with `leaf-max-entries = 1` leaves one key in
each leaf.

## End conditions

A warm-up ends when the splits and merges of all database instances in the last
`quiet` window of model time are at most `quiet-rate` in each second of model
time, or at its timeout. Under a constant load, the topology can keep changing
slowly near its stable boundaries, so a warm-up does not wait for no change at
all. A measurement ends on its end condition, or at `max-duration`:

| Path | Default | Meaning |
| --- | --- | --- |
| `cell.warm-up.minimum` | `"5s"` | Minimum model time. |
| `cell.warm-up.quiet` | `"10s"` | The model time window of the quiet check. `"never"` runs until `timeout`. |
| `cell.warm-up.quiet-rate` | 0.2 | The splits and merges of all instances in each second of model time of a `quiet` window, at or below which the warm-up ends. 0 waits for a window without changes. |
| `cell.warm-up.timeout` | `"60s"` | Maximum model time. `"0s"` is no warm-up. |
| `cell.measure.until` | `"converged"` | The end condition. `"converged"`: every shape has `window-commits` commits in each throughput window on average. `"duration"`: exactly `duration`. `"passes"`: each worker makes one pass of its `sequence` source. `"no-locked-pass"`: each such worker makes passes until one pass takes no lock; that pass is not in the measurement. |
| `cell.measure.duration` | `"2s"` | Minimum measurement for `converged`, the measurement for `duration`. |
| `cell.measure.max-duration` | `"60s"` | Maximum model time for all conditions. |
| `cell.measure.window-commits` | 50 | For `converged`. |

`timeout` wins over `minimum`. A warm-up that ends at its timeout without a
quiet window, and a measurement that ends at `max-duration` before its
condition, are marked in the result.

## Parameters

All other parameters, with their scope and the built-in default:

| Path | Scope | Default | Meaning |
| --- | --- | --- | --- |
| `cell.collections` | seed | `"databases"` | Collections of each pool. `"databases"` gives one for each instance of the cell. |
| `seed.quiet-period` | seed | `"10s"` | Model time without splits and merges that ends the seed. `"0s"` ends the seed after its writes, without a wait and without full GCs. |
| `seed.timeout` | seed | `"180s"` | The run fails after this model time. |
| `cell.workload` | cell | none | The workload. Each cell must have one. |
| `cell.databases` | cell | 4 | Instances of a group that does not give `instances`. |
| `cell.workers` | cell | 8 | Workers of a shape whose value is `"cell"`. |
| `cell.affinity` | cell | 100 | Percent of transactions on the home collection. The others select a collection at random. |
| `cell.gc` | cell | `true` | Pause the workers after the warm-up, and run full GCs until the GC backlog is gone. Without it, the measurement follows the warm-up with no pause. |

The scope of a parameter is what it changes, not its path:

- `pools.*`, `seed.*`, and `cell.collections` are seed parameters.
- A cell parameter that decides the size of a pool is also a seed parameter.
  This is a `keys` or a source parameter of a shape on a pool without `keys`.
- With `cell.collections = "databases"`, the parameters that decide the
  instances of the cell are also seed parameters.

The seed writes only the pools of the shapes that the cells of the experiment
use. The loader checks only these shapes, workloads, and pools.

## Experiments

An experiment file has the values of the experiment, and `axes`: the axes, from
the outer loop to the inner loop. With no axes, the experiment has one cell.

```toml
# An experiment of the mixed workloads at five affinities.
include = ["../../configs/common-workloads.toml"]

[cell]
workload = "low-conflict"

[[axes]]
path = "cell.affinity"
values = [0, 25, 50, 75, 100]
```

An axis has one of two forms:

```toml
# One parameter.
[[axes]]
path = "cell.affinity"
values = [0, 50, 100]
start = "seed"

# Coupled parameters. Each value has a label and sets one or more paths.
[[axes]]
name = "policy"
start = "seed"
values = [
  { label = "band" },
  { label = "avoidable", set = { "cell.database.topology" = "avoidable" } },
]
```

An axis can also take its values from an axis generator, a scenario part that
gives the values of a coupled axis:

```toml
[[axes]]
name = "overlap"
generate = { contention-overlaps = { shape = "contention-rmw", max-keys = 6 } }
```

The loader replaces `generate` with the values before `--set` and `--only`, so
the result and `--only` see the generated labels. Use a generator when the
values follow a rule that a plain list repeats many times.

- A path must not be in two axes.
- An axis value wins over the value of the files. A value of the coupled form
  that does not set a path keeps the value of the files.
- A value can repeat, for example `["low-conflict", "crossing", "low-conflict"]`.

The values of `start`:

| `start` | Where the cell starts | Storage |
| --- | --- | --- |
| `seed` (default) | A copy of the seed, with new database instances. | all |
| `previous` | The storage and the database instances at the end of the cell before it. | all |
| `previous-warm-up` | A copy of the storage after the warm-up of the cell before it, with new database instances. | `memory` only |

`previous` keeps the instances, so their caches and the state of their topology
policy carry over. This is necessary for `load-shift` and `inline-pressure`. A
`previous` axis must then not change the groups or the instances of a group,
nor `cell.database.*`, because the open instances cannot change. A change of
the shapes and their workers is allowed.

## Loop

The cells of a run are the nested loops of the axes, in their order:

```text
for run in 1..=runs:
    for each cell in product(axes):          # the first axis is the outer loop
        axis = outermost axis that changed since the cell before it
        if this is the first cell, or axis.start is "seed":
            state = seed(seed parameters of the cell)
        else:
            state = state at the end (or after the warm-up) of the cell before it
        warm up, then measure
```

One rule decides the start of each cell: the outermost axis that changed. When
an outer axis changes, the inner axes start again from their first values.

Seeds are cached. Cells with the same values of all seed parameters use the
same seed: on storage that can fork, each cell gets a copy, and the seed runs
once for each run. On other storage, the seed runs again for each cell that
starts from the seed. So in `mixed`, the 5 affinities of each workload share
one seed, as today.

The checks before the first seed:

- An axis that sets a seed parameter, or that changes the instances or
  `cell.database`, must have `start = "seed"`, or `"previous-warm-up"` for a
  change of the instances.
- An axis with `start = "previous"` or `"previous-warm-up"` must be inside all
  axes with `start = "seed"`.
- Each name that a parameter uses must exist.
- Each shape must have the parameters of its kind and of its source, and no
  others.
- Each source must have enough keys for each worker, at the largest worker
  count of the experiment.

## Result

The result has `schemaVersion` 3. It has `backend`, `timeMode`,
`modelTimeSpeedup`, `latencyJitter`, the command-line `options`, and
`experiments`: one entry for each experiment, in the order that they ran. Each
entry has the `name` of the experiment, its resolved `configuration`, its
`axes`, and its `runs`. Each run has its cells. Each cell has:

- `labels` (the label of each axis value), `parameters` (the paths that the
  axes set, and a summary of the sources: `keys` and `shared` of an `overlap`
  source, `sharedPct` of a `stripe` source), and `start`;
- `workload`, `affinityPct`, `databases`, `collections`, and `failures`, and
  `databaseLimit` and `workersPerShape` when the workload uses
  `cell.databases` and `cell.workers`;
- `seed`: its splits, wall time, and its full GCs;
- `warmup`: wall time, splits, merges, whether it became quiet, the splits
  and merges in each second of model time of its last `quiet` window
  (`changeRate`, null
  with no full window), and its GC activity; `fullGc`: the full GCs after the warm-up;
- `measure`: wall time, whether its end condition was met, its splits and
  merges, and the worker drain time after it;
- `shapes`: for each shape of each group, its commits, whether it converged,
  the throughput windows with their `relCi`, the mean latency with its
  standard error, and the p50 and p90;
- `groups`: the counters of each instance in the measurement, with its
  shutdown, unless the next cell continues on the same instances.
- `aggregateOps` and `aggregateProtocol`: the backend operations, bytes, and
  protocol counters of the cell, for each transaction;
- `costs`: the backend requests, the body bytes, and the coordinator counters
  of each transaction, in three windows: `workload`, the shutdown of the
  instances, and both (`combined`). Body bytes are the bodies of successful
  reads and of attempted writes, without paths, headers, LIST responses, and
  transport;
- `timeline`: the splits and merges of each instance in each second of model
  time of the warm-up and the measurement.

All database instances use the production pending timeout and no clock skew
allowance, because they read one clock. Full GCs after a seed or a warm-up
stop after 600 s of wall time. Every cell runs on one multi-thread runtime.

`hack/perf/perfbench_results.py` reads the result files for the
comparison and plot tools.

## Seeds, warm-ups, and measurements

Before the cells run, the seed writes all keys and waits until splits, merges,
and garbage collection stop for `seed.quiet-period`. If that takes longer than
`seed.timeout`, the run fails. With `--backend=memory`, the cells with the same
seed share one seed, and each cell gets its own copy. With other backends, each
cell writes its keys again.

Each cell then runs its load without measuring it. The warm-up lasts at least
`cell.warm-up.minimum`, and then until the rate of splits and merges in a
`cell.warm-up.quiet` window of model time is at most `cell.warm-up.quiet-rate`,
or until `cell.warm-up.timeout`. Under load, the tree often keeps splitting, so
a loaded cell usually measures after the timeout.
`hack/perf/investigations.md` explains why the splits continue.

The measurement runs for at least `cell.measure.duration`. perfbench splits it
into 20 equal throughput windows. With `until = "converged"`, the measurement
ends when every shape has committed `window-commits` per window on average, or
at `max-duration`. A shape that reaches `max-duration` first is marked
unconverged. Its windows have fewer commits, so its throughput distribution is
noisier. The commits of the transactions that finish after the measurement are
not in a window.

A chain of cells that continue each other shares one seed. The seed writes the
collections of the largest cell of the chain, and each cell uses the first of
them.

## Experiments of the repository

[hack/perf/README.md](../../hack/perf/README.md) lists the experiments of the
repository, the time mode that each one needs, and the commands of the
canonical plots. [hack/aws-bench/README.md](../../hack/aws-bench/README.md)
explains the real-S3 runner.
