# Topology policy experiments (ADR-074)

This directory keeps the scripts, patches, and results of the experiments for
the avoidable time topology policy of
[ADR-074](../../../docs/adr/074-avoidable-time-drives-splits-and-merges.md).
This file is also the hand-over: it gives the status, a summary of the
experiments, and the open problems. The investigation log
([`investigations.md`](../investigations.md), entry "2026-09-26: ADR-074
splits in the mixed hi mode") has the full runs and numbers.

The files are as they ran. The run scripts use perfbench binaries in
`/tmp/topo-exp`, which are not kept. [Reproduce](#reproduce) tells how to
build them again.

## Status

| Item | State |
| --- | --- |
| Branch `topology-policy-pluggable` | Rebuilt on a later `main` with the minimal change for review. Its earlier 27 commits, up to `814d47137`, are in this branch. |
| Branch `topology-policy-experiments` | The 27 commits, one commit with this directory, and a merge that keeps the other branches (see below). |
| ADR-074 | Proposed. |
| Tests at `814d47137` | `make test` and `make test-bench` pass. |
| Engine default | Without a policy, the size causes decide, as before ADR-074. |
| Opt-in | `DatabaseBuilder::topology_policy(AvoidableTimePolicy::new())`. |

The 27 commits up to `814d47137`, by subject:

- Measurement: `62179ec96` (GCS retries throttled requests), `ce4afd9cb`
  (topology counters and the perfbench `topology` scenario), `3042bef99`
  (avoidable time), `078de7e0a`, `a0823c47f`, `8d6474c44`, `0484028fe` (lost
  CAS fix), `c6760e49b` (divided and crossing conflict time), `814d47137`
  (removes the window counts that no policy uses).
- Policy seam and rule: `b959d6d62` (the `TopologyPolicy` trait), `f158a1b07`,
  `9943902ee`, `17db0d830`, `31d55a7a2`, `0ac7e3238`, `5d9a399de`,
  `36e5e6e7f` (the rule of this hand-over).
- Perfbench: `b80f6e006` (memory policy), `d7dda97df` (HTML report),
  `79c89fb87` (counterfactual runs), `616692322` and `93059740b` (net time
  policy), `fa755a58a` (removes it after the rule went into the engine).
- Documents: `c35fd6def`, `b920dbf15`, `7343df1a1`, `597b59eca`, `89552f702`.

Other branches. The last commit of this branch is a merge with the `ours`
strategy: it has their last commits as parents and does not change the files.
So their commits stay reachable without their branch names. To work on one
again, make a branch at its commit, for example
`git branch topology-policy-central f8211c8e1`.

- `topology-policy-central` (`f8211c8e1`): the same measures with one rule in
  the engine and no policy seam. It gave the same decisions and throughput.
  ADR-074 rejects it ("Decide in the engine, without a policy seam").
- `topology-policy-chain` (`a0954b97f`): merges over chains of linked leaves.
  Rejected ("Merge the leaves that transactions cross in a chain").
- `experiment/delay-throttle-waits` (`9cb3c6c8f`): an earlier step, which the
  later commits of this branch replace.
- `topology-signals` (`3042bef99`): an earlier step, which is in the history
  of this branch.

### The rule

`AvoidableTimePolicy` in `crates/glassdb-trans/src/structural/policy.rs`.
Each database instance runs it once in each window of 1 s, on its own
transactions:

```text
decay = 0.5 ^ (elapsed / 10 s)

for each leaf:
  if split_recently or merged_recently: restart its averages
  net   = split-side avoidable time - 4 * divided_conflict_time
  delay = lost CAS + queue wait + slow leaf CAS - 4 * divided_conflict_time
  average = decay * average + (1 - decay) * net       (same for delay)
  pays   = not merged_recently and average > 0.05 * split time
  at_key = pays and delay average > 0.05 * split time
  if at_key and the leaf has a split key:  split at the split key
  elif pays or over a soft cap:            split at the median
  else:                                    next leaf
  drop the averages of the leaf, and hold it against merges

for each pair with a crossing average:
  if a leaf of the pair split_recently or merged_recently: drop the average
for each pair of adjacent leaves in the window:
  crossing = decay * crossing + (1 - decay) * crossing_conflict_time
for each pair of the window, and each pair with a crossing average:
  merge side = pair avoidable time of the window + crossing
  if no leaf of the pair is held (split_recently, or changed in this window)
     and merge side > 0.1 * merge time + split side of both leaves in the window:
    merge the left leaf into its current right sibling
    drop the crossing average of the pair
```

The divided conflict time of a leaf is the time of the commit passes that a
conflict ended, and whose keys a split at the median puts in both halves. The
crossing conflict time of a pair is the time of the commit passes that a
conflict ended, and whose keys are in the two leaves only. The first stops
the splits that make conflicting transactions wait for the locks of more
leaves. The second merges back a split when the first was too small.

### Results of the rule

Perfbench of the engine rule (`results/engine`, script
`scripts/net/run18.sh`), 2 runs, all 12 processes at the same time. `mixed`
compares with the size causes, `topology` with the fixed seeded tree. The
values are geometric means of the shape throughputs (mixed) or of the cells
(topology):

| Scenario | S3 | GCS | Rule of one window (before) |
| --- | ---: | ---: | --- |
| `topology`, 20 cells | `1.17`, `1.16` | `1.23`, `1.28` | about 3% more on S3, the same on GCS |
| `mixed` lo, 12 cells | `1.13`, `1.12` | `1.24`, `1.25` | `1.14`–`1.15` on S3, `1.29` on GCS |
| `mixed` hi, 12 cells | `0.98`, `1.02` | `0.96`, `1.00` | `0.62` on S3, `0.74` on GCS |

`cd results/engine && python3 ../../scripts/net/r18.py` prints these numbers
and the split and merge counts of the worst cells.

## Experiments

The objective was the geometric mean of the shape throughputs, so that no
shape starves. The sum of the throughputs hid the problem: the rule of one
window had `1.22` times the sum of the size causes in hi mode, but `0.62` of
their geometric mean on S3.

| Step | Question | Result | Kept in |
| --- | --- | --- | --- |
| 1 | Which topology is faster, for each backend and workload? | It depends on both. A cost model with a fixed cost for each event failed on GCS. | ADR-074 context |
| 2 | One rule in the engine, or a policy seam? | Same decisions and throughput. The seam keeps the engine default. | branch `topology-policy-central` |
| 3 | Rule of one window: split at `0.25`, merge at `0.1` times the change time | Good in `topology` and lo, starves `rwMany` and `roMulti` in hi. | `0ac7e3238`, `5d9a399de` |
| 4 | Ask a change to pay back the change that it undoes (`MemoryPolicy`) | Fewer changes, same throughput. | `b80f6e006`, perfbench `memory:<split>:<merge>` |
| 5 | Merge over chains of linked leaves | Hi `0.65`–`0.66`, against `0.62`. | branch `topology-policy-chain` |
| 6 | Subtract the locked commit penalty of divided direct commits | The starving shapes seldom use direct commits. No effect. | `patches/split-penalty.patch` |
| 7 | Why do `rwMany` and `roMulti` starve? Fixed trees of 1, 2, and 4 leaves | `rwMany` goes from `22.4` to `5.7` to `2.6`–`3.0` tx/s: its locks over more leaves conflict more. | `scripts/diag` |
| 8 | Offline replay of estimates of the time that a split adds, on traced unsplit runs | The divided conflict time separates the splits that hurt from the ones that help. The divided commits and the divided pass time do not. | `scripts/replay` |
| 9 | Live: why does the policy split more than the replay? | The lost CAS time had two errors. Fixed in `0484028fe`. | `scripts/cascade` |
| 10 | Merge side: crossing conflict time of a pair | Merges back the splits that cost. Hi `0.97`–`1.03` on S3. | `c6760e49b`, `93059740b` |
| 11 | Undo a split only after it costs time (divided weight 0) | Churn. Hi `0.79`–`0.83`. | `scripts/net/run12.sh`, `run13.sh` |
| 12 | Moving averages for all merge causes | Scans do not merge any more. `topology` scan cells `1.02`–`1.07`, against `1.44`–`3.46`. | `patches/merge-averages.patch` |
| 13 | Split sooner: half-life 5 s or split multiple `0.025` | The hi loss comes back: `0.57`–`0.79` in 3 of 4 cells. | `scripts/net/run14.sh` |
| 14 | Merge only the pairs of the current window | A/B with 8 runs: not more than the noise. Kept the pairs of earlier windows. | `patches/window-pairs-only.patch`, `scripts/net/run15.sh` to `run17.sh` |
| 15 | The rule in the engine | Same as the perfbench candidate. | `36e5e6e7f`, `results/engine` |

ADR-074 records each rejected step as an alternative, with the reason.

## Open problems

These are in order of priority.

1. **Split and merge churn between database instances.** In `mixed` hi with 8
   instances, a few runs of the cells with affinities 0 and 50 split and merge
   the same leaves again and again in the measurement. They had `0.65` (S3, an
   earlier run of the same rule) and `0.73` (GCS, `results/engine`) of the size
   causes. One instance splits for its lost CAS time, another merges for its
   crossing conflict time. It is rare. In the A/B runs of step 14 with 8
   databases and affinity 0, the merge rule that the engine has churned in 1 of
   21 runs of `crossing3` and 0 of 8 runs of `crossing5`. The rule of step 14
   (`crossing4`) churned in 2 of 10 runs. To diagnose, trace the cell
   until a churn run occurs, and read it with `scripts/cascade/splits.py`. Here
   `perfbench` is the binary of `814d47137`:

   ```console
   perfbench mixed --delays=gcs --workers-per-shape=8 --databases=8 \
     --affinities=50 --modes=hi --policies=avoidable \
     --shadow-policies=avoidable --trace-windows --warmup=0s --duration=40s \
     --max-duration=40s --target-ci=0 --split-quiet=5s \
     --split-settle-timeout=300s --output=churn.json
   ```

   Candidates that nobody tested yet:
   - Step 4 (`MemoryPolicy`) ran only with the rule of one window. With moving
     averages, a merge that undoes a split could also pay back that split.
   - A longer hold-down for a leaf that a merge for crossing conflict time
     wrote.

2. **Worst `topology` cell.** `single` with leaves of 16 entries and 4
   databases on S3 had 28 tx/s, against 33 to 36 for the fixed tree, with 6 to
   10 splits. The rule of one window had 30 tx/s with more than 100 splits. Each
   transaction writes one key, so a split must not add time. Trace this cell
   to find which cause pays for the splits.

3. **Slower convergence on GCS in lo mode.** The rule waits for a moving
   average, so it splits later than the rule of one window: `1.24` against
   `1.29` with a warmup of 20 s. With a warmup of 60 s, the difference was less
   than the noise. ADR-074 accepts this. Step 13 shows that a faster average
   brings the hi loss back.

4. **ADR-074 is still proposed.** Decide if the open problems 1 and 2 block
   it. The rebuilt `topology-policy-pluggable` is the change to review. Its
   policy seam is internal, and `DatabaseBuilder::leaf_changes` turns on the
   avoidable time rule.

5. **Policy tests outside the engine.** The window types are
   `#[non_exhaustive]` and have no public constructor. So a crate outside the
   engine cannot unit-test a policy with synthetic windows. Perfbench policies
   can only run live.

6. **One read validation path is not reported.** When a transaction body
   returns an error, the engine validates its reads (`validate_reads` in
   `crates/glassdb-trans/src/algo.rs`). A conflict in this validation does not
   report its commit pass to the topology policy. Other commit passes, also the
   ones of read-only transactions, report it. The perfbench shapes do not use
   this path.

## Working notes

- Do not compile or run tests while a benchmark runs. The benchmarks use a
  model clock, and CPU load changes their results.
- Runs of 12 processes at the same time have a noise of about 5% for each
  cell. Most hi cells do not change the tree. So compare the cells that
  changed the tree on their own (`r18.py` and `mixruns.py` do this). For the
  hi cells with 8 databases, use A/B runs of 8 or more at the same time.
- A policy form of perfbench is `engine|fixed|size|avoidable[:<split>:<merge>]|memory:<split>:<merge>`.
  The `net` forms of the scripts in `scripts/net` need the perfbench of
  `597b59eca`, where `avoidable` is still the rule of one window.
- `--shadow-policies` and `--trace-windows` write the windows and the
  decisions of other policies in the JSON, without changing the tree. The
  replay scripts use them.

## Reproduce

Build perfbench at the commit of the binary that a script uses, and copy it
to that path:

```console
git worktree add /tmp/pb <commit>
cd /tmp/pb && git apply <patch, if the table has one>
cargo build --release -p glassdb-bench-scale --bin perfbench
mkdir -p /tmp/topo-exp && cp target/release/perfbench /tmp/topo-exp/perfbench-<name>
```

The `engine` binary, nearest to `36e5e6e7f`, made `results/engine`.
`814d47137` removes only the window counts that no policy uses, so it decides
the same. Use it as
`perfbench-engine` for new runs. The run scripts write their results next to
themselves, so `run18.sh` writes the files of `results/engine` in
`scripts/net`.

The binaries came from working trees, often before the commit. The commit is
the nearest one. Its code can be a bit different.

| Binary | Nearest commit | Scripts |
| --- | --- | --- |
| `pluggable` to `pluggable4` | `b959d6d62`, `f158a1b07`, `9943902ee`, `17db0d830` | none kept (step 2) |
| `central`, `central2`, `central3` | `5d249a23d`, `1e1e57813`, `31d55a7a2` (branch `topology-policy-central`) | none kept (step 2) |
| `pluggable5` to `pluggable8` | `b80f6e006` to `0ac7e3238` | none kept (steps 3 and 4) |
| `penalty1` | `d7dda97df` and `patches/split-penalty.patch` | none kept (step 6) |
| `harness` | `79c89fb87` | `replay/cf` |
| `lostfix`, first build | `078de7e0a` | `replay/cf2` |
| `counts` | `a0823c47f` | `replay/cf3` |
| `net`, `net2` | `616692322` | `net/run.sh` to `run4.sh` |
| `attempts`, `attempts2` | `8d6474c44` | `replay/cf4`, `diag`, `net/run5.sh` |
| `conflicts` | `8d6474c44` and the divided conflict time of `c6760e49b` | `replay/cf5`, `cascade/run.sh`, `net/run6.sh` |
| `lostfix`, second build | `0484028fe` | `cascade/run2.sh` |
| `crossing`, `crossing2` | early versions of the crossing average of `93059740b` | `cascade/run3.sh`, `run4.sh` |
| `crossing3` | `93059740b`, before the review | `cascade/run5.sh`, `net/run7.sh` to `run14.sh`, A/B in `run16.sh` and `run17.sh` |
| `crossing4` | `93059740b` and `patches/window-pairs-only.patch` | `net/run15.sh`, `run16.sh` |
| `crossing5` | `93059740b` | `net/run17.sh` |
| `engine` | `36e5e6e7f` | `net/run18.sh` |

## Contents

```text
patches/
├── split-penalty.patch        # step 6, applies on d7dda97df
├── merge-averages.patch       # step 12, applies on 616692322
└── window-pairs-only.patch    # step 14, applies on 93059740b
scripts/
├── net/                       # live mixed and topology runs of the net rule
│   ├── run.sh … run18.sh      # one script for each round, in order
│   └── r18.py                 # summary of results/engine
├── replay/                    # traced runs and offline replay of split rules
│   ├── cf/                    # counterfactual hi runs; label.py, trace.py
│   ├── cf2/                   # traced runs with transaction counts
│   ├── cf3/                   # counts.py, rule.py, ewma.py: first estimates
│   ├── cf4/                   # ewma2.py: divided commits against pass time
│   └── cf5/                   # ewma3.py, pairs.py: divided conflict time
├── cascade/                   # traced live runs; splits.py prints each split
├── diag/                      # fixed trees of 1 to 4 leaves in hi mode
└── report/                    # tables and geometric means from JSON results
results/
└── engine/                    # r18 JSON and logs of the rule in the engine
```

`hack/perf/plot-topology.py` renders the JSON results as an HTML report. The
file names in `results/engine` have `-s3-` or `-gcs-`, as it needs.
