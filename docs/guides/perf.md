# Perf tracking

This document tracks changes to the engine that affect performance. The baseline
is the v0.1.0 release, which is the first public release and the best tested
version.

Keep this document sorted by the most recent changes first. Each entry should
include a reference to the commit or ADR that introduced the change.

## ADRs 064-065: bounded parallel leaf work and renewed fallback identity

[ADR-064](../adr/064-bounded-parallel-point-leaf-work.md) runs independent
point-access work on distinct leaves with a transaction-local limit of 16.
[ADR-065](../adr/065-renewed-transaction-identity-on-serial-fallback.md) gives a
new transaction identity to an attempt that changes from parallel to sorted
serial lock acquisition.

### Setup

- base: `24c4a979` (accepted ADRs, before implementation); target: `4e8251ef`
- ratio = target / base (throughput >1 good; latency/operations <1 good)
- the paired regression cell uses the in-memory backend, the S3 delay profile,
  `delay-scale=0.2`, `prefix-depth=3`, one `Database`, one worker per shape,
  100% home-collection affinity, 5,000 logical keys, and 10 keys for each
  multi-key transaction
- three interleaved pairs use a three-second minimum window, 10% throughput-CI
  target, 30-second maximum window, three-second split quiet period, and
  90-second drain bound. All cells converge with zero failures
- per-side command: `perfbench --backend=memory --delays=s3 --delay-scale=0.2
  --prefix-depth=3 --runs=1 --drain-timeout=90s mixed --modes=lo
  --affinities=100 --databases=1 --workers-per-shape=1 --multi-keys=10
  --num-keys=5000 --duration=3s --max-duration=30s --target-ci=0.10
  --split-quiet=3s --split-settle-timeout=45s`
- the focused `large_transactions` Criterion benchmark puts one logical key in
  each of 16 or 64 independent collection-root leaves. It uses the S3 delay
  profile, one rate-limit prefix per collection, and 10 samples per timed cell.
  Like all groups in the `transactions` benchmark, it uses `20x` model time.
  The same benchmark-only change was applied to the base
- focused command: `cargo bench -p glassdb --bench transactions --
  large_transactions`

### Mixed-workload regression cell

Cross-run medians are:

| Shape | Metric | Base | Target | Target/base |
| --- | --- | ---: | ---: | ---: |
| `roMulti` | Throughput | `5.09 tx/s` | `14.56 tx/s` | `2.858` |
| `roMulti` | p50 | `191.49 ms` | `63.02 ms` | `0.329` |
| `roMulti` | p90 | `248.00 ms` | `103.26 ms` | `0.416` |
| `rwMany` | Throughput | `4.80 tx/s` | `4.72 tx/s` | `0.984` |
| `rwMany` | p50 | `205.27 ms` | `208.59 ms` | `1.016` |
| `roSingle` | Throughput | `24.53 tx/s` | `26.30 tx/s` | `1.072` |
| `rwSingle` | Throughput | `13.48 tx/s` | `12.59 tx/s` | `0.934` |

The 10-key cell gives a clear multi-key read improvement. It does not establish
a multi-key write change. The single-key throughput moves by less than 7%, and
the branch adds no backend operation to the single-key paths. Aggregate backend
operations per completed transaction increase from `4.24` to `4.62` (`1.089`),
while transaction retries fall from `0.00361` to `0.00229` (`0.635`).

### Large transactions

| Workload | Leaves | Base median | Target median | Target/base |
| --- | ---: | ---: | ---: | ---: |
| Read-only | 16 | `36.29 ms` | `3.27 ms` | `0.090` |
| Read-only | 64 | `143.81 ms` | `10.72 ms` | `0.075` |
| Existing-key RMW | 16 | `14.12 ms` | `16.05 ms` | `1.136` |
| Existing-key RMW | 64 | `14.84 ms` | `28.76 ms` | `1.938` |

The read-only intervals do not overlap: base/target intervals are
`34.28-37.53`/`3.16-3.34 ms` at 16 leaves and
`138.31-148.00`/`10.53-10.86 ms` at 64 leaves. The result is consistent with one
bounded wave at 16 leaves and four bounded waves at 64 leaves, instead of one
serial wait per leaf.

The write result records the cost of the bound. The base lock path submitted
all leaf operations without a limit. The target uses one bounded wave at 16
leaves and four waves at 64 leaves. Its intervals are `15.15-16.47 ms` and
`28.58-29.00 ms`, versus base intervals of `13.40-14.35 ms` and
`14.40-15.14 ms`. Thus the initial limit of 16 gives a large read benefit but
reduces throughput for transactions that write much more than 16 independent
leaves.

### README graphs

The updated worker sweep uses the target, the same S3 model, three runs, 5,000
logical keys per collection, 100% affinity, five `Database` clients, and 1 then
10 through 200 workers per shape. All 63 cells converge with zero failures and
a maximum relative throughput-CI half-width of `0.0996`.

At one worker per shape, `roMulti` reaches `14.72 tx/s` with `61.58 ms` p50.
At 200 workers per shape, throughput is `2,520.09 tx/s` for `roSingle`,
`866.84 tx/s` for `roMulti`, `644.45 tx/s` for `rwSingle`, and `189.89 tx/s`
for `rwMany`. The graph now shows the low-concurrency multi-leaf read gain and
the high-concurrency multi-key write limit.

## ADR-061: atomic logless commits within one leaf

[ADR-061](../adr/061-atomic-logless-single-leaf-commits.md) generalizes direct
commit from one existing-key overwrite to complete point-access transactions
whose reads and writes share one leaf. Creates, overwrites, and tombstone
deletes publish atomically in one CAS when the complete output fits inline.

### Setup

- base: `2b54060f` (ADR-only control, before implementation); target: this
  worktree
- release Criterion benchmark on the in-memory backend and the existing
  model-time GCS/S3 delay profiles; 10 samples per timed cell
- low-contention and disjoint-key same-leaf-contention matrices cover blind
  puts, mixed put/delete, and cross-key RMW at 2, 8, and 32 keys
- separate memory cells approach the per-value and aggregate inline limits at
  each key count
- each operation-cost sample contains 30 completed transactions; the
  uncontended gate requires 30 candidates, 30 direct landings, one object write
  per transaction, and no lock call

### Protocol outcomes

- all 27 low-contention shape/count/backend cells pass the gate: every
  transaction lands directly with exactly one object write and no lock call
- all six near-boundary cells also land `30/30` with one write and no lock call
- every same-leaf-contention transaction remains a direct candidate. The short
  simulated-provider windows land `29–30/30` directly, with at most `0.07` lock
  calls per transaction when clean CAS contention exhausts the bounded direct
  retry and selects the regular fallback; memory cells land `30/30`
- direct counters count transactions rather than keys or physical CAS attempts,
  as required by the ADR

### Latency

Low-contention memory medians are:

| Workload | 2 keys | 8 keys | 32 keys |
| --- | ---: | ---: | ---: |
| Blind put | `11.21 µs` | `26.55 µs` | `101.57 µs` |
| Mixed put/delete | `12.32 µs` | `29.11 µs` | `101.52 µs` |
| Cross-key RMW | `13.44 µs` | `35.69 µs` | `143.44 µs` |

GCS/S3 low-contention medians are `1.10–1.23 ms`, dominated by the one modeled
provider write. At the inline boundaries, memory medians are `11.23`, `27.10`,
and `97.69 µs` near the per-value limit and `12.93`, `30.81`, and `103.33 µs`
near the aggregate limit for 2, 8, and 32 keys respectively.

The pre-existing benchmarks provide the paired regression check:

| Workload | Base median | Target median | Target/base |
| --- | ---: | ---: | ---: |
| Single-key RMW | `9.34 µs` | `9.48 µs` | `1.015` |
| 10-key RMW | `116.77 µs` | `44.19 µs` | `0.378` |

The single-key intervals overlap (`9.19–9.60 µs` base and `9.43–9.52 µs`
target), so the implementation does not establish a regression in the original
fast path. For 10-key RMW, backend work falls from `3.20` writes and `0.63`
reads per transaction to exactly one write and no reads.

## ADR-060: bounded delayed write-back convergence

[ADR-060](../adr/060-bounded-delayed-write-back-convergence.md) moves one
committed write-back retry after a definitive leaf-CAS loss into a bounded,
database-local quiet-period queue.

### Setup

- base: `a4b7c419` (ADR-only control, before implementation); target: uncommitted
  ADR-060 implementation on the same tree
- ratio = target / base (throughput >1 good; latency/ops <1 good)
- three interleaved release pairs, in-memory backend with the S3 delay profile,
  `delay-scale=0.2`, four Databases, 0% affinity, 2,000 keys per collection,
  eight workers per shape, a two-second minimum window, 12% throughput-CI
  target, 40-second maximum window, three-second split quiet period, and
  90-second drain bound
- per-side command: `perfbench --backend memory --delays s3 --delay-scale 0.2
  --runs 1 --drain-timeout 90s mixed --modes lo --affinities 0 --databases 4
  --workers-per-shape 8 --num-keys 2000 --duration 2s --max-duration 40s
  --target-ci 0.12 --split-quiet 3s --split-settle-timeout 45s`
- all six cells report zero failures, every shape converges, setup reaches its
  quiet period after `28–40` completed splits, and shutdown drains successfully;
  backend and coordinator counters include the forced shutdown cleanup

### Results

| Metric | Pair ratios | Median |
| --- | --- | ---: |
| Aggregate throughput | `0.982`, `0.867`, `1.401` | `0.982` |
| Aggregate write-shape throughput | `1.073`, `0.712`, `1.345` | `1.073` |
| `rwSingle` throughput | `1.031`, `0.693`, `1.555` | `1.031` |
| `rwMany` throughput | `1.202`, `0.765`, `0.648` | `0.765` |
| `roSingle` throughput | `0.953`, `0.903`, `1.418` | `0.953` |
| `roMulti` throughput | `1.061`, `0.828`, `1.363` | `1.061` |
| `rwSingle` p50 | `1.354`, `0.730`, `0.748` | `0.748` |
| `rwMany` p50 | `1.107`, `0.761`, `0.772` | `0.772` |
| Backend operations / transaction | `1.084`, `0.901`, `0.769` | `0.901` |
| Coordinator CAS retries / transaction | `1.450`, `0.669`, `0.491` | `0.669` |

The durable implementation retains the prototype's median write-throughput
gain, while aggregate throughput is effectively flat and individual shapes are
mixed. Because counters are sampled after shutdown, the lower median backend
work and coordinator retry rate represent coalescing rather than omitted
cleanup. The earlier temporary prototype's `1.079` aggregate ratio did not
reproduce reliably enough to claim as the implementation result.

## Current tree: baseline reassessment after inline-policy tuning

### Setup

- target: `40bc6b8d`; canonical base: `v0.1.0`; diagnostic base:
  `69663dde` (post-ADR-044); inline-default base: `fbdb99f5`
- ratio = target / base (throughput >1 good; latency/ops/cost <1 good)
- canonical command: `BASE=v0.1.0 LABEL_A=v010 LABEL_B=current
  DELAY_SCALE=0.02 DB_LIST=1,10,20 NUM_KEYS=5000 DURATION=5s NUM_RUNS=3
  DEADLOCK_DURATION=1s COUNT=5 DRAIN_TIMEOUT=90s COMMAND_TIMEOUT=15m
  hack/aws-bench/compare-refs.sh --summary`
- the v0.1.0 binary predates simulated-time reporting. Its compressed
  `rtbench` latency and duration fields are multiplied by `50` before
  comparison; operation counts and the autoresearch score require no
  normalization
- all measured cells report zero transaction failures. Every current-tree
  worker and shutdown drain completes within its bound; v0.1.0 predates the
  separate drain fields

### Canonical v0.1.0 baseline

- balanced rw9010 aggregate-throughput ratios are `0.381–0.444` at one
  Database, `0.010–0.022` at 10 Databases, and `0.017–0.021` at 20 Databases.
  Across all nine paired cells the geomean is `0.049`; Jain-fairness geomean is
  `0.86`
- one-Database worker time remains about five seconds on both refs. At 10 and
  20 Databases the current worker time is `16.1–29.7 s`, versus `5.1–5.8 s`
  on v0.1.0, because the current workers need that long to complete their
  minimum samples after the five-second launch window
- strong-read p50 ratio has geomean `1.11`; write p50 has geomean `5.26` and
  median `4.00`. The weak-read ratio is not comparable because v0.1.0 reports
  a near-zero cached-read p50
- noisy deadlock p50 and p90 geomeans are `2.00` and `1.78`; the three one-key
  p50 ratios are `5.24–5.29`
- backend operations/transaction fall to a `0.47` geomean and retries to
  `0.14`. The deterministic autoresearch score falls from `403.43` to `98.13`
  (`0.243`)
- autoresearch secondary geomeans move to `2.79` allocation bytes/transaction,
  `2.25` allocations/transaction, `1.65` wall ns/transaction, and `1.25` CPU
  ns/transaction. `batchWrite100` alone uses `8.65x` the allocation bytes and
  `5.40x` the CPU time, despite reducing weighted backend cost to `0.011`
- `readRepeat` weighted cost is `1.82`, while physical calls remain at parity
  and CPU time falls to `0.54`; the cost ratio reflects reclassification from a
  metadata read to an object read

### Post-ADR-044 reference

- command: `BASE=69663dde LABEL_A=adr044 LABEL_B=current DELAY_SCALE=0.02
  DB_LIST=1,10 NUM_KEYS=5000 DURATION=5s NUM_RUNS=3
  DEADLOCK_DURATION=1s COUNT=5 RW_MIX="balanced readheavy writeheavy"
  MIX_DURATION=1s MIX_MAX_DURATION=30s MIX_TARGET_CI=0.15 MIX_MODES=hi
  MIX_TOPOLOGIES=shared,per-shape MIX_WORKERS=8 MIX_CLIENTS=4
  MIX_HOT_KEYS=8 MIX_MULTI_KEYS=8 DRAIN_TIMEOUT=90s COMMAND_TIMEOUT=15m
  hack/aws-bench/compare-refs.sh --summary`
- rw9010 aggregate-throughput geomeans are `1.13` balanced, `0.97` read-heavy,
  and `1.04` write-heavy. Backend operations/transaction geomeans are `1.15`,
  `1.14`, and `1.22`, respectively
- deadlock p50 and p90 geomeans are `0.91` and `0.90`. In the one-key cells,
  current p50 is `0.51–0.55` and p90 `0.47–0.51` of the reference, with
  `417–447` completions versus `233–242`
- mixbench throughput geomeans are `1.83` for `roMulti`, `1.10` for
  `roSingle`, `2.35` for `rwMany`, and `1.04` for `rwSingle`. The reference
  `hi/per-shape` `rwMany` cell is unconverged; all other cells converge
- deterministic autoresearch score improves from `122.89` to `97.48`
  (`0.793`). Secondary geomeans move to `0.93` allocation bytes/transaction,
  `0.83` allocations/transaction, `0.67` wall ns/transaction, and `0.61` CPU
  ns/transaction

### 16 KiB inline-default guardrails

- base `fbdb99f5` differs only by its 64 KiB aggregate inline default. The
  focused inline-pressure workload remains explicitly pinned to 64 KiB: S3
  recovery throughput is `1.00`, GCS is `0.97`, backend work is at parity, both
  sides land `64/64` recovery mutations directly, and both complete exactly two
  pressure splits
- deadlock p50 and p90 geomeans are `0.93` and `0.94`; one-key direct outcomes
  are unchanged. Every cell completes with zero failures and a bounded drain
- three additional alternating `hi/per-shape` pairs all converge. Throughput
  geomeans are `1.03` for `roSingle`, `1.00` for `roMulti`, `1.70` for
  `rwSingle`, and `0.87` for `rwMany`; operations/transaction geomeans are
  `0.95`, `0.97`, `0.83`, and `1.00`, respectively
- ten alternating autoresearch pairs put the overall score at a `1.005`
  geomean. `batchWrite100` cost has a `0.990` geomean; its median object-write
  counts are `112.5` with 64 KiB and `113.5` with 16 KiB

## ADR-056: demand-driven inline-pressure splits

[ADR-056](../adr/056-demand-driven-inline-pressure-splits.md) requests a
background median split when aggregate inline capacity prevents an otherwise
eligible direct commit.

### Setup

- base: `e88cb819` (accepted ADR, before implementation); target: `0be65fee`
- ratio = target / base (throughput >1 good; latency/ops/bytes <1 good)
- release `rtbench`, in-memory backend with `s3` and `gcs` delay profiles,
  `delay-scale=0.02`, and three paired, interleaved runs per profile
- per-side command: `rtbench --backend=memory --delays=<s3|gcs>
  --delay-scale=0.02 --test-name=inline-pressure --num-runs=1
  --inline-pressure-settle-timeout=3s`
- the focused scenario starts with 192 existing external 1 KiB values, below
  the ordinary 256-entry split threshold. It lands 64 direct mutations to fill
  the 64 KiB inline budget, uses two distinct mutations to request a root and
  then a non-root split, and measures 64 later mutations interleaved across the
  resulting leaves
- the same benchmark-only harness was applied to both refs. Every run completed
  all 130 mutations and shutdown without an error

### S3 profile

- recovery throughput ratio: min `3.50`, median `3.56`, max `3.65`
- recovery p50 ratio: `0.26–0.27` (median `0.26`); p90 ratio:
  `0.29–0.32` (median `0.31`)
- recovery backend operations/transaction ratio: `0.26–0.29` (median `0.28`);
  write bytes/transaction ratio: `0.20–0.22` (median `0.21`)
- whole-scenario backend operations/transaction ratio: `0.53–0.55`; write
  bytes/transaction ratio: `0.42–0.45`

### GCS profile

- recovery throughput ratio: min `3.46`, median `3.87`, max `3.90`
- recovery p50 ratio: `0.22–0.25` (median `0.23`); p90 ratio:
  `0.51–0.66` (median `0.60`)
- recovery backend operations/transaction ratio: `0.28–0.29` (median `0.28`);
  write bytes/transaction ratio: `0.20–0.22` (median `0.21`)
- whole-scenario backend operations/transaction ratio: `0.53–0.54`; write
  bytes/transaction ratio: `0.42–0.44`

### Protocol outcomes

- both discovering mutations use the locked fallback on both refs
- recovery direct land rate changes from `0/64` to `64/64`; recovery lock calls
  change from `64` to `0`
- whole-scenario direct land rate changes from `64/130` (`0.492`) to `128/130`
  (`0.985`), and lock calls change from `66` to `2`
- every target run records exactly two pressure candidates and two completed
  pressure splits, with zero deferrals and zero discards. The base records no
  splits. Starting from one leaf, the final leaf count is therefore `1` on the
  base and `3` on the target

## ADR-054: reserve inline publication for logless commits

[ADR-054](../adr/054-reserve-inline-publication-for-logless-commits.md) stops
logged write-back and help-forwarding from copying values into leaf entries.
Logless direct commits retain authoritative inline values.

### Setup

- base: `ed590a8c` (accepted ADR, before implementation); target: this worktree
- ratio = target / base (throughput >1 good; latency/ops/cost <1 good)
- command: `BASE=ed590a8c LABEL_A=before LABEL_B=adr054
  DRAIN_TIMEOUT=90s hack/aws-bench/compare-refs.sh --summary`
- summary defaults: 5,000 keys, two paired 3-second rw9010 and deadlock runs,
  three deterministic efficiency repeats, and adaptive mixbench cells with a
  20% relative-CI target and 20-second cap
- every workload completed with zero transaction failures; every mixbench
  shape reached its CI target

### Deterministic efficiency

- autoresearch score: `98.76` to `97.52` (ratio `0.987`)
- `batchWrite100`: cost/transaction `166.34` to `156.54` and
  operations/transaction `2.38` to `2.24` (both `0.941`)
- `multiRMW10`: cost ratio `0.995`, operations ratio `1.000`
- `singleRMW`: cost ratio `1.002`, operations ratio `1.005`
- `batchRead10` and `readRepeat`: cost and operations ratios `1.000`

### Contention workloads

- rw9010 aggregate-throughput geomeans: balanced `0.89`, read-heavy `1.07`,
  write-heavy `1.25`. Per-cell ratios span `0.43–1.24`, `1.01–1.17`, and
  `0.72–2.09`, respectively
- deadlock sweep: p50 and p90 geomeans `0.97`, throughput `1.03`, and
  retries/transaction `0.95`
- mixbench throughput geomeans: `roMulti 1.72`, `roSingle 1.72`,
  `rwMany 0.77`, and `rwSingle 1.19`
- shared-Database aggregate operations/transaction: high contention `0.90`,
  low contention `0.84`
- the low-contention mixbench cells are near parity or better for every shape:
  throughput ratios range from `0.93` to `1.67`
- high-contention/per-shape `rwMany` is the outlier: throughput `1.896` to
  `0.716 tx/s` (`0.377`), p50 ratio `0.987`, p90 `0.988 s` to `13.762 s`
  (`13.93`), object reads/transaction `1.327` to `1.764`, object
  writes/transaction `3.156` to `3.309`, and total operations/transaction
  `4.482` to `5.073`

## ADR-053: replay definitive logless RMW losses

[ADR-053](../adr/053-replay-definitive-logless-rmw-losses.md) replays an
eligible read-modify-write after a certified logless loss instead of publishing
a holder, and removes ADR-027's separate logged single-RW fallback.

### Setup

- base: `b18b4b36` (the accepted ADR, before implementation); target:
  `5c3e5ac6` (the implementation and removal of ADR-027)
- ratio = target / base (throughput >1 good; latency/ops/cost <1 good)
- command: `BASE=b18b4b36 LABEL_A=pre053 LABEL_B=adr053 DELAY_SCALE=0.02
  DB_LIST=1 NUM_KEYS=500 DURATION=1s NUM_RUNS=3 DEADLOCK_DURATION=1s
  COUNT=5 RW_MIX=balanced MIX_DURATION=1s MIX_MAX_DURATION=30s
  MIX_TARGET_CI=0.1 MIX_MODES=lo,hi
  MIX_TOPOLOGIES=shared,per-shape MIX_WORKERS=8 MIX_CLIENTS=4
  MIX_NUM_KEYS=5000 MIX_HOT_KEYS=8 MIX_MULTI_KEYS=8 DRAIN_TIMEOUT=60s
  COMMAND_TIMEOUT=10m hack/aws-bench/compare-refs.sh --summary`
- rw9010 and the one-key workload use three paired, interleaved repetitions.
  Autoresearch uses five internal repeats. The first mixbench sweep uses one
  adaptive pair; the high-contention guardrails were then repeated as three
  interleaved pairs with CI target `0.15`
- every measured cell has zero transaction failures and completes its drain.
  The target's first `lo/per-shape` sweep hit its 30-second cap, so it is not
  used for an acceptance conclusion. All repeated guardrail cells converge

### Focused one-key recovery

The five-writer, one-key workload recovers on every paired run:

- completion throughput rises from `1.65–1.84 tx/s` to `8.65–9.00 tx/s`;
  the paired ratios are `4.90–5.28`
- p50 falls from `2.50–2.82 s` to `0.55–0.57 s` (`0.20–0.22`), and p90
  falls from `3.78–4.24 s` to `0.72–0.76 s` (`0.17–0.20`)
- successful transactions/run rise from `87–97` to `437–455`. Worker drain
  falls from `50.1–56.0 ms` to `10.0–10.9 ms`
- retries/transaction rise from `2.03–2.40` to `3.32–3.50`, as expected when
  certified losses replay the body. Direct land rate rises from `1.0–2.0%` to
  `22.2–23.2%`, and the extra attempts now produce useful progress rather than
  a persistent logged phase

The balanced one-Database rw9010 guard remains flat: aggregate throughput
ratios are `1.00`, `1.01`, and `1.01`; backend operations/transaction ratios
are `1.01`, `0.99`, and `0.99`. Jain fairness is `1.00` by construction with
one Database.

### Guardrails and remaining signal

- uncontended autoresearch `singleRMW` is unchanged at `70.79` weighted
  cost/transaction, with exactly the same reads, writes, lists, and zero
  retries. Wall time moves from `12.48` to `11.60 µs/transaction` (`0.93`);
  this noisy secondary axis does not indicate a regression
- across the three repeated `hi/shared` pairs, aggregate throughput ratios are
  `0.98–1.12` (median `0.99`), while `rwSingle` is `0.97–1.21` (median
  `1.03`). Aggregate backend operations/transaction have a median ratio of
  `1.01`
- `hi/per-shape` aggregate throughput improves by `1.37–1.45`; its
  `rwSingle` throughput improves by `2.02–3.88` (median `2.16`) and
  backend operations/transaction fall to a median ratio of `0.62`

The repeated `lo/shared` cell is a separate warning: aggregate throughput is
`0.78–0.87` of the base and `rwSingle` is `0.52–0.89`. This topology co-locates
direct single-RMW and logged multi-key traffic, so it is not the uncontended
direct-path guardrail. The result is consistent with ADR-053's accepted cost
for falling back to regular locking, but these measurements do not attribute
the cause. Backend-operation ratios are mixed (`0.66–1.38`) rather than showing
a uniform amplification. Carry this signal into inline-admission and
direct-commit-coverage measurement.

## ADR-045–ADR-051: current-state reassessment

This rerun reassesses the cumulative engine after the persistent cache,
transactional collection management, the collection-record/tree split, and
[ADR-051](../adr/051-inline-latest-values.md). It also establishes the first
rw9010 result using aggregate completions over one shared cell clock. Older
entries retain the historical `num_databases * median(per_database_rate)`
estimator and should not be compared directly without reprocessing their CSVs.

### Setup

- base: `69663ddecc23c57ea40d9d0995d1d663797f251b` (post-ADR-044);
  target: `3fccb3ba4f2b5dd154645cc6a42a7ead730d354a` plus the benchmark
  accounting and direct candidate/landed counters in this worktree
- ratio = target / base (throughput >1 good; latency/ops/cost <1 good)
- command: `BASE=69663dde LABEL_A=adr044 LABEL_B=current DELAY_SCALE=0.02
  DB_LIST=1,10,20 NUM_KEYS=5000 DURATION=8s NUM_RUNS=3
  DEADLOCK_DURATION=500ms COUNT=3 RW_MIX="balanced readheavy writeheavy"
  MIX_DURATION=1s MIX_MAX_DURATION=20s MIX_TARGET_CI=0.2 MIX_MODES=hi
  MIX_TOPOLOGIES=shared MIX_WORKERS=8 MIX_CLIENTS=4
  DRAIN_TIMEOUT=90s hack/aws-bench/compare-refs.sh --summary`
- the three rw9010 and deadlock repetitions are paired and interleaved, with
  execution order reversed on the second repetition
- all cells complete with zero transaction failures

### Corrected rw9010 results

- aggregate-throughput geomeans: balanced `0.92`, read-heavy `1.00`,
  write-heavy `1.01`. The corresponding Jain-fairness geomeans are `0.97`,
  `0.96`, and `1.01`, with a median fairness ratio of `1.00` in every mix
- write p50 geomeans are `2.14` in balanced and `1.51` in read-heavy, but
  `0.99` in write-heavy. Strong reads improve slightly in the first two mixes;
  the completion result does not support a broad throughput recovery or
  regression
- backend operations/transaction are `0.97`, `0.88`, and `1.05`; retries per
  transaction are `1.05`, `1.05`, and `1.04`
- current workers overrun the requested 8-second measurement window by
  `12.0–26.9 s` at 10 and 20 Databases. The ADR-044 binary predates the split
  drain field, but total cell wall time is similarly long on both sides
  (`8.0–37.7 s`). Throughput uses the completed transaction count and common
  worker elapsed time; drain is reported separately

### Focused hot-key result

The one-key, five-writer cell is the clear localized regression:

- p50 ratio is `2.02–2.47`; p90 ratio is `1.85–2.79` across the three paired
  runs. The target records 148 successful samples versus 326 in the reference
- the target has 405 direct candidates: 14 land and 391 (`96.5%`) do not. That
  is `2.74` candidates per completed transaction, alongside 257
  logged-transaction retries
- larger fully overlapping transactions are mixed around parity. Across all
  key counts, the noisy p50 and p90 geomeans are `1.18` and `1.19`; the
  one-key direct-path eligibility is what makes the outlier distinct

The two durable counters prove that the direct path does not retain its
uncontended advantage under same-key contention. Reason-specific instrumentation
should remain temporary while the next P1 identifies whether batch exclusion,
leaf-CAS loss, or renewed transaction re-entry dominates.

### Secondary signals

- deterministic efficiency improves from `122.01` to `98.46` (`0.807`).
  Single-RMW cost is `0.35`, multi-RMW `0.82`, and batch-write `1.17`
- the single short `hi/shared` mixbench cell is secondary: `roMulti` throughput
  is `0.62`, `roSingle` `0.90`, `rwMany` `0.94`, and `rwSingle` `0.99`, with
  aggregate backend operations/transaction at `1.06`

## ADR-051: Inline latest values in leaf entries

[ADR-051](../adr/051-inline-latest-values.md) makes a small committed value part
of the leaf entry that names its writer, so a latest read can be served from the
node alone, and an eligible single read-write transaction commits in one
conditional leaf CAS with no lock, no transaction object, and no write-back.

### Setup

- base: `d2878227` (the ADR commit, code-identical to the pre-implementation
  tree); target: this worktree
- ratio = target / base (throughput >1 good; latency/ops/cost <1 good)
- inline budgets at their defaults: 1 KiB per value, 64 KiB aggregate per leaf
- the full `compare-refs.sh` harness was not used; these are the three
  benchmarks named in the plan, run locally on the memory and simulated
  backends: `autoresearch --count 3`, `cargo bench -p glassdb`, and `mixbench
  --duration 3s --max-duration 20s`

### Deterministic efficiency (autoresearch, most trustworthy)

- autoresearch-score (cost/tx geomean, lower=better): `122.31` to `98.17`
  (ratio `0.803`) => better
- cost/tx[singleRMW]: `182.4` to `70.6` (ratio `0.387`) => better
- cost/tx[batchWrite100]: `178.9` to `151.6` (ratio `0.847`) => better
- cost/tx[multiRMW10]: `254.9` to `258.9` (ratio `1.016`) => ~same
- cost/tx[batchRead10] and cost/tx[readRepeat]: `57.4` unchanged => ~same
- ns/tx geomean: `79690` to `61259` (ratio `0.769`); cpu ns/tx `98931` to
  `69309` (ratio `0.700`); allocs/tx `1024.0` to `824.2` => better
- ns/tx[batchWrite100] rises from `6.21 ms` to `7.96 ms` (ratio `1.283`): the
  cost of encoding and CAS-ing leaves that now carry value bytes

### Microbenchmarks (criterion, mean, sample_size 10)

- single_rmw backend writes/op: `2.47` (memory), `2.80` (gcs), `2.90` (s3) all
  to `1.03`; reads/op stays `0.03`. This is the direct measurement of the
  one-CAS commit: three writes become one
- single_rmw: memory `26.4 µs` to `10.9 µs` (`0.41`), gcs `2.33 ms` to
  `1.20 ms` (`0.51`), s3 `2.30 ms` to `1.19 ms` (`0.52`) => better
- multi_read_10: memory `20.2 µs` to `14.2 µs` (`0.70`) => better; gcs and s3
  unchanged within noise
- multi_rmw_10: gcs `0.98`, s3 `0.97`, memory `1.09` => ~same
- write_100 and shared_read are within their own (wide) noise bands: write_100
  memory `16.8 ms` to `22.5 ms` against a base 95% interval of `13.2–20.5 ms`

The read short-circuit does not move the read-only workloads here because their
transaction objects are already served by the decoded cache
([ADR-036](../adr/036-decoded-object-cache-with-bounded-freshness.md)) in steady
state; it converts an already-cheap cached read into no read at all, which shows
up as CPU and allocation savings rather than fewer backend operations. The
saved backend read matters on a cold or evicted cache.

### Contention sweep (mixbench, short cells, indicative)

- zero transaction failures in all four cells
- lo/shared aggregate backend operations/tx: `0.367` to `0.238` (`0.65`);
  retries/tx `0.0109` to `0.0073` => better
- hi/shared aggregate backend operations/tx: `0.618` to `0.567` (`0.92`);
  retries/tx `0.310` to `0.277` => better
- hi/per-shape mix-tps[rwSingle]: `3.00` to `5.82` with p50 `217 ms` to
  `113 ms` => better
- lo/shared mix-tps[rwSingle]: `4.62` to `2.24` => WORSE, while its read shapes
  commit 14–30% more transactions in the same window

The lo/shared write regression is not a protocol regression: that cell is
saturated (p50 above one second for a single-key RMW on both sides), performs
35% fewer backend operations per transaction, and retries less; the read shapes
absorb the freed capacity. Short saturated cells redistribute throughput between
concurrently running shapes, so these cells are indicative only.

### Budgets

The 1 KiB / 64 KiB defaults are the tunable outcome, not a measured optimum.
The trade-off they price is visible above: every leaf CAS carries the inline
bytes of its whole leaf, which costs encode/CAS work on write-heavy multi-key
workloads (`batchWrite100`), and buys the transaction-object read plus, for a
single read-write transaction, two of its three object writes.

## ADR-044: CAS-fenced structural gate

[ADR-044](../adr/044-cas-fenced-structural-gate.md) removes shared structure
readers from ordinary transactions. Stable-leaf work now checks that the
exclusive structural gate is absent in the same node CAS that installs or
publishes data; only a structural mutation closes the gate and quiesces the
whole node.

### compare-refs summary

- command: `BASE=51b1bbc1 LABEL_A=before LABEL_B=cas-gate DIAGNOSTICS=1 DRAIN_TIMEOUT=90s compare-refs.sh --summary`
- base: `51b1bbc15853d270c75966456dab3efbb010cf2b` (before)
- target: current worktree based on `51b1bbc1` (cas-gate)
- ratio = cas-gate / before (throughput >1 good; latency/ops/cost <1 good)
- summary parameters: 5,000 keys, two 3-second rw9010 runs at 1 and 10
  Databases, two 3-second deadlock sweeps, three deterministic efficiency
  repeats, and adaptive mixbench cells capped at 20 seconds
- every rw9010 and mixbench cell completed with zero transaction failures. The
  high-contention per-shape mixbench cell was unconverged on both sides
- the historical baseline needed a 90-second completion grace for mixbench; the
  default 30-second summary grace rejected the first run before the target ran

### Results

- rw9010 throughput geomean: balanced `0.79`, read-heavy `0.94`, write-heavy
  `1.30`. The short sweep is mixed and should not be treated as a proven
  throughput improvement
- rw9010 backend operations/transaction geomean: balanced `0.64`, read-heavy
  `0.80`, write-heavy `0.51`; node operations and transaction-log operations
  both fall in all three mixes
- deterministic efficiency score: `122.18` to `120.39` (`0.985`, effectively
  unchanged). Single-RMW cost falls to `0.932` and multi-RMW cost to `0.967`;
  batch-write cost rises to `1.031`
- deadlock p50 and p90 geomeans are `0.99` and `0.97`, both within the noisy
  workload's unchanged band
- converged mixbench cells are also mixed: low-contention shared topology loses
  7–19% throughput while aggregate backend operations/transaction stay within
  2%; several per-shape write cells perform fewer backend operations but do not
  convert that saving into higher throughput

The deterministic stable-leaf regression tests provide the direct protocol
check: an ordinary mutation neither records a structural lock nor resolves an
unrelated entry holder, and a single-RMW transaction falls back from the fast
path when a gate is present. The benchmark confirms the expected reduction in
backend work without establishing an overall throughput win.

## ADR-031–ADR-043 and transaction-log refactoring

This cumulative comparison covers dynamic range sharding
([ADR-031](../adr/031-dynamic-range-sharding.md)), node-level locking and
coordinated splits ([ADR-032](../adr/032-node-locking-and-coordinated-splits.md)),
the subsequent listing and transaction changes, the decoded object cache
([ADR-036](../adr/036-decoded-object-cache-with-bounded-freshness.md)), and the
causally coordinated backend operations of
[ADR-043](../adr/043-causally-coordinated-backend-operations.md). The target also
includes the transaction-log refactor in `f9625778` (PR #21).

### compare-refs full summary

- command: `BASE=7a1e05b1 LABEL_A=adr030 LABEL_B=head compare-refs.sh`
- base: `7a1e05b1f27737169872b6162f6dc98d8c91fe46` (adr030)
- target: current worktree based on `122e229c` (head)
- ratio = head / adr030 (throughput >1 good; latency/ops/cost <1 good)
- full parameters: 5,000 keys, 15-second rw9010 cells at 1/10/20/40
  Databases, 8-second deadlock cells, five deterministic efficiency repeats,
  and adaptive mixbench cells capped at 60 seconds
- every rw9010 cell completed with zero transaction failures; deadlock and all
  four mixbench cells also completed. The high-contention per-shape mixbench
  cell reached its time cap and is marked `[unconverged]`
- at 40 Databases, the target's nominal 15-second cells took 36.5 seconds
  (balanced), 42.4 seconds (read-heavy), and 49.7 seconds (write-heavy) to
  finish in-flight work, versus 17.8–20.0 seconds for adr030. These successful
  drains are included rather than discarded
- weak-read ratios magnify adr030's 0.04–0.07 ms p50 baseline; the target's
  40-Database weak-read p50 is 45–49 ms

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.03 median=0.05 max=0.62 (geomean=0.08, n=4) => WORSE
- throughput[weak-read]: ratio b/a min=0.03 median=0.05 max=0.62 (geomean=0.08, n=4) => WORSE
- throughput[write]: ratio b/a min=0.03 median=0.05 max=0.62 (geomean=0.08, n=4) => WORSE
- latency-p50[strong-read]: ratio b/a min=0.91 median=0.97 max=1.99 (geomean=1.14, n=4) => better
- latency-p50[weak-read]: ratio b/a min=5.00 median=620.35 max=894.40 (geomean=199.88, n=4) => WORSE
- latency-p50[write]: ratio b/a min=1.03 median=6.90 max=30.69 (geomean=5.06, n=4) => WORSE
- retries: ratio b/a min=0.03 median=0.06 max=1.64 (geomean=0.11, n=4) => better
- backend-ops/tx: ratio b/a min=0.42 median=0.75 max=1.53 (geomean=0.77, n=4) => better

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.03 median=0.07 max=0.61 (geomean=0.10, n=4) => WORSE
- throughput[weak-read]: ratio b/a min=0.03 median=0.07 max=0.61 (geomean=0.10, n=4) => WORSE
- throughput[write]: ratio b/a min=0.03 median=0.07 max=0.61 (geomean=0.10, n=4) => WORSE
- latency-p50[strong-read]: ratio b/a min=0.53 median=0.95 max=1.92 (geomean=0.98, n=4) => better
- latency-p50[weak-read]: ratio b/a min=6.25 median=280.20 max=909.30 (geomean=69.90, n=4) => WORSE
- latency-p50[write]: ratio b/a min=1.06 median=6.19 max=21.40 (geomean=4.47, n=4) => WORSE
- retries: ratio b/a min=0.01 median=0.03 max=1.38 (geomean=0.07, n=4) => better
- backend-ops/tx: ratio b/a min=0.48 median=0.67 max=1.20 (geomean=0.71, n=4) => better

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.02 median=0.04 max=0.58 (geomean=0.06, n=4) => WORSE
- throughput[weak-read]: ratio b/a min=0.02 median=0.04 max=0.58 (geomean=0.06, n=4) => WORSE
- throughput[write]: ratio b/a min=0.02 median=0.04 max=0.58 (geomean=0.06, n=4) => WORSE
- latency-p50[strong-read]: ratio b/a min=0.93 median=1.12 max=2.20 (geomean=1.27, n=4) => WORSE
- latency-p50[weak-read]: ratio b/a min=0.62 median=0.76 max=4.43 (geomean=1.12, n=4) => better
- latency-p50[write]: ratio b/a min=1.35 median=5.51 max=19.67 (geomean=4.38, n=4) => WORSE
- retries: ratio b/a min=0.20 median=0.37 max=1.09 (geomean=0.40, n=4) => better
- backend-ops/tx: ratio b/a min=0.33 median=1.42 max=3.36 (geomean=1.19, n=4) => WORSE

### deadlock

- deadlock-p50 [noisy]: ratio b/a min=0.34 median=0.83 max=0.86 (geomean=0.72, n=6) => better
- deadlock-p90 [noisy]: ratio b/a min=0.45 median=1.14 max=1.26 (geomean=1.00, n=6) => WORSE

### mixbench

- mix-tps[roMulti] [unconverged]: ratio b/a min=0.00 median=0.26 max=0.67 (geomean=0.07, n=4) n_min=96 => WORSE
- mix-tps[roSingle] [unconverged]: ratio b/a min=0.00 median=0.42 max=1.43 (geomean=0.09, n=4) n_min=55 => WORSE
- mix-tps[rwMany] [unconverged]: ratio b/a min=0.09 median=0.41 max=0.66 (geomean=0.30, n=4) n_min=77 => WORSE
- mix-tps[rwSingle]: ratio b/a min=0.07 median=0.67 max=6.95 (geomean=0.52, n=4) n_min=790 => WORSE
- mix-ops/tx[hi/roMulti] [unconverged]: ratio b/a=0.63 (1 point) n_min=96 => better
- mix-ops/tx[hi/roSingle] [unconverged]: ratio b/a=8.26 (1 point) n_min=55 => WORSE
- mix-ops/tx[hi/rwMany] [unconverged]: ratio b/a=0.03 (1 point) n_min=77 => better
- mix-ops/tx[hi/rwSingle]: ratio b/a=0.18 (1 point) n_min=14954 => better
- mix-ops/tx[lo/roMulti]: ratio b/a=0.23 (1 point) n_min=12416 => better
- mix-ops/tx[lo/roSingle]: ratio b/a=0.56 (1 point) n_min=96356 => better
- mix-ops/tx[lo/rwMany]: ratio b/a=1.03 (1 point) n_min=394 => WORSE
- mix-ops/tx[lo/rwSingle]: ratio b/a=3.33 (1 point) n_min=1201 => WORSE
- mix-retries/tx[hi] [unconverged]: ratio b/a min=0.09 median=0.82 max=4.99 (geomean=0.51, n=4) => better
- mix-retries/tx[lo]: ratio b/a min=0.03 median=0.93 max=462.57 (geomean=0.96, n=4) => better
- mix-agg-ops/tx[hi]: ratio b/a=0.12 (1 point) => better
- mix-agg-ops/tx[lo]: ratio b/a=0.04 (1 point) => better

### efficiency

- autoresearch-score (cost/tx geomean, lower=better) [deterministic]: adr030=863.53 head=122.66 ratio b/a=0.142 => better
- autoresearch-cost/tx: ratio b/a min=0.01 median=0.10 max=1.11 (geomean=0.14, n=5) => better
- autoresearch-cost/tx[batchWrite100]: ratio b/a=0.01 => better
- autoresearch-cost/tx[multiRMW10]: ratio b/a=0.08 => better
- autoresearch-cost/tx[batchRead10]: ratio b/a=0.10 => better
- autoresearch-cost/tx[readRepeat]: ratio b/a=0.98 => ~same
- autoresearch-cost/tx[singleRMW]: ratio b/a=1.11 => WORSE

### Attribution

The largest throughput change remains the ADR-031/ADR-032 transition. Comparing
ADR-030 with `118c0224` (PR #15, the first standard-workload-compatible commit
after PR #14) gives throughput geomeans of 0.16 for balanced, 0.18 for
read-heavy, and 0.15 for write-heavy. PR #14 (`996d5078`, ADR-031) cannot run
the standard 5,000-key workload by itself: it fails at the first root split with
`collection root node is not a leaf`. That prevents a clean separation of
ADR-031 from ADR-032, but locates most of the cumulative throughput regression
to their dynamic-sharding and node-locking transition.

The largest efficiency improvement remains PR #20 (`b11eeb39`, ADR-036). In a
direct comparison with its parent, the deterministic efficiency score falls
from 598.98 to 127.13 (ratio 0.212), while backend operations/transaction fall
to 0.45–0.58 across the rw9010 mixes. The same comparison has throughput ratios
of 0.83 for balanced, 0.36 for read-heavy, and 0.58 for write-heavy: fewer
backend operations and better median reads, but long write tails reduce
completed throughput.

The earlier deadlock `NotFound` started with ADR-036's cache and is addressed by
ADR-043's completion-before-invocation ordering. This full target run completes
the deadlock matrix. During the full rw9010 validation, a second false
`NotFound` exposed a transaction-lifecycle invariant violation: missing-object
expiry could re-read a concurrently committed object and pass that final
observation to `force_abort`, allowing `committed → aborted`. Status appearance
now re-enters ordinary resolution, and `force_abort` independently refuses to
overwrite a final observation. The remaining 36–50 second cells are therefore
performance tails rather than failed or corrupted transactions.

## ADR-030: Seed shard loads

Reducing the number of strong shard loads and replacing them with caching in
some safe places ([ADR-030](../adr/030-seed-shard-loads.md)).

### compare-refs summary

- base: 736aa6baef008bc725b1cfe49f2d1a974bd47bda (v1)
- target: current worktree (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)
- each line ends in a `=> better/WORSE/~same` verdict read in that
  metric's own direction, so no axis has to be interpreted by hand
- `autoresearch-*` is **deterministic** (single-client backend ops/tx,
  lower is better) — the most trustworthy signal; `mix-*` cells run
  until their throughput 95% CI reaches --target-ci, so a converged
  ratio is significant — `[unconverged]` marks a cell that hit its time
  cap first (read as indicative); `deadlock-*` stay **[noisy]**

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=1.01 median=1.03 max=1.06 (geomean=1.03, n=2) => better
- throughput[weak-read]: ratio b/a min=1.01 median=1.03 max=1.06 (geomean=1.03, n=2) => better
- throughput[write]: ratio b/a min=1.01 median=1.03 max=1.06 (geomean=1.03, n=2) => better
- latency-p50[strong-read]: ratio b/a min=0.97 median=0.98 max=0.99 (geomean=0.98, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.92 median=0.93 max=0.94 (geomean=0.93, n=2) => better
- latency-p50[write]: ratio b/a min=0.96 median=0.97 max=0.97 (geomean=0.97, n=2) => better
- retries: ratio b/a min=0.96 median=1.00 max=1.07 (geomean=1.01, n=5) => ~same
- backend-ops/tx: ratio b/a min=0.96 median=1.00 max=1.06 (geomean=1.01, n=5) => ~same

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.96 median=0.98 max=1.00 (geomean=0.98, n=2) => ~same
- throughput[weak-read]: ratio b/a min=0.96 median=0.98 max=1.00 (geomean=0.98, n=2) => ~same
- throughput[write]: ratio b/a min=0.96 median=0.98 max=1.00 (geomean=0.98, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=0.98 median=1.22 max=1.45 (geomean=1.19, n=2) => WORSE
- latency-p50[weak-read]: ratio b/a min=1.03 median=1.04 max=1.05 (geomean=1.04, n=2) => WORSE
- latency-p50[write]: ratio b/a min=0.98 median=1.00 max=1.03 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=0.95 median=1.00 max=1.01 (geomean=0.98, n=3) => ~same
- backend-ops/tx: ratio b/a min=0.95 median=1.00 max=1.01 (geomean=0.99, n=3) => ~same

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- throughput[weak-read]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- throughput[write]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.01 max=1.02 (geomean=1.01, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=1.00 median=1.15 max=1.31 (geomean=1.14, n=2) => WORSE
- latency-p50[write]: ratio b/a min=0.99 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00, n=7) => ~same
- backend-ops/tx: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=7) => ~same

### deadlock

- deadlock-p50 [noisy]: ratio b/a min=0.96 median=1.00 max=1.03 (geomean=1.00, n=6) => ~same
- deadlock-p90 [noisy]: ratio b/a min=0.97 median=1.02 max=1.04 (geomean=1.01, n=6) => ~same

### mixbench

- mix-tps[roMulti]: ratio b/a min=0.97 median=1.06 max=1.30 (geomean=1.09, n=4) n_min=1106 => better
- mix-tps[roSingle]: ratio b/a min=0.96 median=1.00 max=1.10 (geomean=1.01, n=4) n_min=2064 => ~same
- mix-tps[rwMany] [unconverged]: ratio b/a min=0.91 median=1.09 max=2.31 (geomean=1.25, n=4) n_min=106 => better
- mix-tps[rwSingle]: ratio b/a min=0.30 median=1.04 max=1.88 (geomean=0.88, n=4) n_min=122 => better
- mix-ops/tx[hi/roMulti]: ratio b/a=0.74 (1 point) n_min=15603 => better
- mix-ops/tx[hi/roSingle]: ratio b/a=0.99 (1 point) n_min=32430 => ~same
- mix-ops/tx[hi/rwMany] [unconverged]: ratio b/a=0.63 (1 point) n_min=106 => better
- mix-ops/tx[hi/rwSingle]: ratio b/a=1.13 (1 point) n_min=264 => WORSE
- mix-ops/tx[lo/roMulti]: ratio b/a=1.01 (1 point) n_min=1106 => ~same
- mix-ops/tx[lo/roSingle]: ratio b/a=1.00 (1 point) n_min=2064 => ~same
- mix-ops/tx[lo/rwMany]: ratio b/a=0.99 (1 point) n_min=172 => ~same
- mix-ops/tx[lo/rwSingle]: ratio b/a=0.73 (1 point) n_min=677 => better
- mix-retries/tx[hi] [unconverged]: ratio b/a min=0.38 median=0.67 max=1.49 (geomean=0.71, n=4) => better
- mix-retries/tx[lo]: ratio b/a min=0.36 median=0.95 max=1.25 (geomean=0.80, n=4) => better
- mix-agg-ops/tx[hi]: ratio b/a=1.02 (1 point) => WORSE
- mix-agg-ops/tx[lo]: ratio b/a=0.92 (1 point) => better

### efficiency

- autoresearch-score (cost/tx geomean, lower=better) [deterministic]: v1=960.48 v2=866.82 ratio b/a=0.902 => better
- autoresearch-cost/tx: ratio b/a min=0.67 median=1.00 max=1.00 (geomean=0.90, n=5) => ~same
- autoresearch-ops/tx: ratio b/a min=0.65 median=1.00 max=1.00 (geomean=0.90, n=5) => ~same
- autoresearch-cost/tx[singleRMW]: ratio b/a=0.67 (1 point) => better
- autoresearch-cost/tx[multiRMW10]: ratio b/a=0.89 (1 point) => better
- autoresearch-cost/tx[readRepeat]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchRead10]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchWrite100]: ratio b/a=1.00 (1 point) => ~same

## ADR-029: GC Shard Coordinator

### compare-refs summary

- base: b789c651741d78f7388dcd71038e95ca095c3974 (v1)
- target: 736aa6baef008bc725b1cfe49f2d1a974bd47bda (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)
- each line ends in a `=> better/WORSE/~same` verdict read in that
  metric's own direction, so no axis has to be interpreted by hand
- `autoresearch-*` is **deterministic** (single-client backend ops/tx,
  lower is better) — the most trustworthy signal; `mix-*` cells run
  until their throughput 95% CI reaches --target-ci, so a converged
  ratio is significant — `[unconverged]` marks a cell that hit its time
  cap first (read as indicative); `deadlock-*` stay **[noisy]**

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- throughput[weak-read]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- throughput[write]: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.97 median=1.02 max=1.08 (geomean=1.02, n=2) => WORSE
- latency-p50[write]: ratio b/a min=1.01 median=1.01 max=1.02 (geomean=1.01, n=2) => ~same
- retries: ratio b/a min=0.98 median=1.00 max=1.04 (geomean=1.00, n=5) => ~same
- backend-ops/tx: ratio b/a min=0.98 median=1.00 max=1.03 (geomean=1.00, n=5) => ~same

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- throughput[weak-read]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- throughput[write]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=0.98 median=0.98 max=0.99 (geomean=0.98, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.93 median=1.01 max=1.08 (geomean=1.01, n=2) => ~same
- latency-p50[write]: ratio b/a min=0.98 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- retries: ratio b/a min=1.01 median=1.01 max=1.05 (geomean=1.02, n=5) => ~same
- backend-ops/tx: ratio b/a min=1.01 median=1.01 max=1.04 (geomean=1.02, n=5) => ~same

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=1.03 median=1.07 max=1.11 (geomean=1.07, n=2) => better
- throughput[weak-read]: ratio b/a min=1.03 median=1.07 max=1.11 (geomean=1.07, n=2) => better
- throughput[write]: ratio b/a min=1.03 median=1.07 max=1.11 (geomean=1.07, n=2) => better
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.80 median=0.91 max=1.01 (geomean=0.90, n=2) => better
- latency-p50[write]: ratio b/a min=0.99 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- retries: ratio b/a min=0.98 median=1.00 max=1.02 (geomean=1.00, n=11) => ~same
- backend-ops/tx: ratio b/a min=0.99 median=1.00 max=1.02 (geomean=1.00, n=11) => ~same

### deadlock

- deadlock-p50 [noisy]: ratio b/a min=0.98 median=1.01 max=1.02 (geomean=1.00, n=6) => ~same
- deadlock-p90 [noisy]: ratio b/a min=0.99 median=1.02 max=1.05 (geomean=1.02, n=6) => ~same

### mixbench

- mix-tps[roMulti]: ratio b/a min=0.98 median=1.01 max=1.19 (geomean=1.04, n=4) n_min=1260 => ~same
- mix-tps[roSingle]: ratio b/a min=1.00 median=1.03 max=1.16 (geomean=1.05, n=4) n_min=2297 => better
- mix-tps[rwMany] [unconverged]: ratio b/a min=0.65 median=1.04 max=1.16 (geomean=0.95, n=4) n_min=62 => better
- mix-tps[rwSingle]: ratio b/a min=0.94 median=1.02 max=1.14 (geomean=1.03, n=4) n_min=112 => better
- mix-ops/tx[hi/roMulti]: ratio b/a=1.00 (1 point) n_min=26105 => ~same
- mix-ops/tx[hi/roSingle]: ratio b/a=0.98 (1 point) n_min=66525 => ~same
- mix-ops/tx[hi/rwMany] [unconverged]: ratio b/a=1.27 (1 point) n_min=62 => WORSE
- mix-ops/tx[hi/rwSingle]: ratio b/a=1.02 (1 point) n_min=1541 => ~same
- mix-ops/tx[lo/roMulti]: ratio b/a=0.96 (1 point) n_min=1260 => better
- mix-ops/tx[lo/roSingle]: ratio b/a=0.99 (1 point) n_min=2297 => ~same
- mix-ops/tx[lo/rwMany]: ratio b/a=0.98 (1 point) n_min=149 => better
- mix-ops/tx[lo/rwSingle]: ratio b/a=1.00 (1 point) n_min=714 => ~same
- mix-retries/tx[hi] [unconverged]: ratio b/a min=0.85 median=0.97 max=1.32 (geomean=1.01, n=4) => better
- mix-retries/tx[lo]: ratio b/a min=0.76 median=0.82 max=1.07 (geomean=0.86, n=4) => better
- mix-agg-ops/tx[hi]: ratio b/a=0.99 (1 point) => ~same
- mix-agg-ops/tx[lo]: ratio b/a=1.00 (1 point) => ~same

### efficiency

- autoresearch-score (cost/tx geomean, lower=better) [deterministic]: v1=977.99 v2=992.28 ratio b/a=1.015 => ~same
- autoresearch-cost/tx: ratio b/a min=1.00 median=1.00 max=1.07 (geomean=1.01, n=5) => ~same
- autoresearch-ops/tx: ratio b/a min=1.00 median=1.00 max=1.07 (geomean=1.01, n=5) => ~same
- autoresearch-cost/tx[batchRead10]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[readRepeat]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchWrite100]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[singleRMW]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[multiRMW10]: ratio b/a=1.07 (1 point) => WORSE

## Shard Coordinator (ADR-028)

### compare-refs summary

- base: 26365c728b7f1892c0dc1d28c1beea79a82e03e0 (v1)
- target: b789c651741d78f7388dcd71038e95ca095c3974 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)
- each line ends in a `=> better/WORSE/~same` verdict read in that
  metric's own direction, so no axis has to be interpreted by hand
- `autoresearch-*` is **deterministic** (single-client backend ops/tx,
  lower is better) — the most trustworthy signal; `mix-*` cells run
  until their throughput 95% CI reaches --target-ci, so a converged
  ratio is significant — `[unconverged]` marks a cell that hit its time
  cap first (read as indicative); `deadlock-*` stay **[noisy]**

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.99 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- throughput[weak-read]: ratio b/a min=0.99 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- throughput[write]: ratio b/a min=0.99 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.75 median=0.84 max=0.92 (geomean=0.83, n=2) => better
- latency-p50[write]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=4) => ~same
- backend-ops/tx: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=4) => ~same

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- throughput[weak-read]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- throughput[write]: ratio b/a min=1.01 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[write]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=0.96 median=0.99 max=1.01 (geomean=0.99, n=4) => ~same
- backend-ops/tx: ratio b/a min=0.96 median=0.99 max=1.01 (geomean=0.99, n=4) => ~same

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.93 median=0.96 max=0.99 (geomean=0.96, n=2) => WORSE
- throughput[weak-read]: ratio b/a min=0.93 median=0.96 max=0.99 (geomean=0.96, n=2) => WORSE
- throughput[write]: ratio b/a min=0.93 median=0.96 max=0.99 (geomean=0.96, n=2) => WORSE
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.01 max=1.01 (geomean=1.01, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=1.02 median=1.04 max=1.06 (geomean=1.04, n=2) => WORSE
- latency-p50[write]: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=0.97 median=1.00 max=1.03 (geomean=1.00, n=6) => ~same
- backend-ops/tx: ratio b/a min=0.98 median=1.00 max=1.02 (geomean=1.00, n=6) => ~same

### deadlock

- deadlock-p50 [noisy]: ratio b/a min=0.26 median=0.34 max=16.78 (geomean=0.63, n=6) => better
- deadlock-p90 [noisy]: ratio b/a min=0.24 median=0.26 max=14.16 (geomean=0.51, n=6) => better

### efficiency

- autoresearch-score (cost/tx geomean, lower=better) [deterministic]: v1=875.76 v2=975.78 ratio b/a=1.114 => WORSE
- autoresearch-cost/tx: ratio b/a min=1.00 median=1.00 max=1.69 (geomean=1.11, n=5) => ~same
- autoresearch-ops/tx: ratio b/a min=1.00 median=1.00 max=1.75 (geomean=1.12, n=5) => ~same
- autoresearch-cost/tx[batchWrite100]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchRead10]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[readRepeat]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[multiRMW10]: ratio b/a=1.02 (1 point) => ~same
- autoresearch-cost/tx[singleRMW]: ratio b/a=1.69 (1 point) => WORSE

## Single RW optimization II (ADR-027)

### compare-refs summary

- base: 80724534f0ea9d3b4a1769aea21cdaabe0d9024b (v1)
- target: 26365c728b7f1892c0dc1d28c1beea79a82e03e0 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)
- each line ends in a `=> better/WORSE/~same` verdict read in that
  metric's own direction, so no axis has to be interpreted by hand
- `autoresearch-*` is **deterministic** (single-client backend ops/tx,
  lower is better) — the most trustworthy signal; `mix-*` and
  `deadlock-*` are **[noisy]** (contention-bound, short windows) and
  `[low-sample]` marks a folded cell below the trust floor

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.94 median=0.97 max=1.01 (geomean=0.97, n=2) => WORSE
- throughput[weak-read]: ratio b/a min=0.94 median=0.97 max=1.01 (geomean=0.97, n=2) => WORSE
- throughput[write]: ratio b/a min=0.94 median=0.97 max=1.01 (geomean=0.97, n=2) => WORSE
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.88 median=0.90 max=0.92 (geomean=0.90, n=2) => better
- latency-p50[write]: ratio b/a min=0.99 median=0.99 max=0.99 (geomean=0.99, n=2) => ~same
- retries: ratio b/a min=1.00 median=1.04 max=1.06 (geomean=1.04, n=4) => WORSE
- backend-ops/tx: ratio b/a min=1.00 median=1.04 max=1.05 (geomean=1.03, n=4) => WORSE

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=1.01 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- throughput[weak-read]: ratio b/a min=1.01 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- throughput[write]: ratio b/a min=1.01 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=0.98 median=0.99 max=1.00 (geomean=0.99, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.92 median=0.96 max=1.00 (geomean=0.96, n=2) => better
- latency-p50[write]: ratio b/a min=0.97 median=0.98 max=1.00 (geomean=0.98, n=2) => ~same
- retries: ratio b/a min=1.05 median=1.05 max=1.05 (geomean=1.05, n=3) => WORSE
- backend-ops/tx: ratio b/a min=1.04 median=1.05 max=1.05 (geomean=1.05, n=3) => WORSE

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=1.00 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- throughput[weak-read]: ratio b/a min=1.00 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- throughput[write]: ratio b/a min=1.00 median=1.02 max=1.03 (geomean=1.02, n=2) => ~same
- latency-p50[strong-read]: ratio b/a min=0.99 median=1.00 max=1.02 (geomean=1.00, n=2) => ~same
- latency-p50[weak-read]: ratio b/a min=0.96 median=0.99 max=1.02 (geomean=0.99, n=2) => ~same
- latency-p50[write]: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=2) => ~same
- retries: ratio b/a min=1.00 median=1.00 max=1.02 (geomean=1.00, n=5) => ~same
- backend-ops/tx: ratio b/a min=1.00 median=1.00 max=1.01 (geomean=1.00, n=5) => ~same

### deadlock

- deadlock-p50 [noisy]: ratio b/a min=0.03 median=0.96 max=1.05 (geomean=0.54, n=6) => better
- deadlock-p90 [noisy]: ratio b/a min=0.03 median=1.02 max=1.05 (geomean=0.56, n=6) => ~same

### mixbench

- mix-tps[roMulti] [noisy]: ratio b/a min=0.59 median=0.84 max=0.96 (geomean=0.79, n=4) n_min=1034 => WORSE
- mix-tps[roSingle] [noisy]: ratio b/a min=0.82 median=0.88 max=0.94 (geomean=0.88, n=4) n_min=2229 => WORSE
- mix-tps[rwMany] [noisy] [low-sample]: ratio b/a min=0.65 median=0.93 max=1.16 (geomean=0.90, n=4) n_min=17 => WORSE
- mix-tps[rwSingle] [noisy] [low-sample]: ratio b/a min=1.23 median=1.61 max=1.97 (geomean=1.58, n=4) n_min=86 => better
- mix-ops/tx[hi/roMulti]: ratio b/a=1.33 (1 point) n_min=1034 => WORSE
- mix-ops/tx[hi/roSingle]: ratio b/a=1.09 (1 point) n_min=3043 => WORSE
- mix-ops/tx[hi/rwMany] [low-sample]: ratio b/a=0.95 (1 point) n_min=17 => better
- mix-ops/tx[hi/rwSingle] [low-sample]: ratio b/a=0.89 (1 point) n_min=86 => better
- mix-ops/tx[lo/roMulti]: ratio b/a=1.01 (1 point) n_min=1217 => ~same
- mix-ops/tx[lo/roSingle]: ratio b/a=1.01 (1 point) n_min=2229 => ~same
- mix-ops/tx[lo/rwMany] [low-sample]: ratio b/a=0.99 (1 point) n_min=129 => ~same
- mix-ops/tx[lo/rwSingle] [low-sample]: ratio b/a=1.37 (1 point) n_min=865 => WORSE
- mix-retries/tx[hi] [noisy]: ratio b/a min=0.83 median=1.04 max=1.73 (geomean=1.11, n=4) => WORSE
- mix-retries/tx[lo] [noisy]: ratio b/a min=0.69 median=0.91 max=1.87 (geomean=1.01, n=4) => better
- mix-agg-ops/tx[hi]: ratio b/a=1.05 (1 point) => WORSE
- mix-agg-ops/tx[lo]: ratio b/a=1.06 (1 point) => WORSE

### efficiency

- autoresearch-score (cost/tx geomean, lower=better) [deterministic]: v1=958.84 v2=873.14 ratio b/a=0.911 => better
- autoresearch-cost/tx: ratio b/a min=0.68 median=1.00 max=1.00 (geomean=0.91, n=5) => ~same
- autoresearch-ops/tx: ratio b/a min=0.66 median=1.00 max=1.00 (geomean=0.91, n=5) => ~same
- autoresearch-cost/tx[singleRMW]: ratio b/a=0.68 (1 point) => better
- autoresearch-cost/tx[multiRMW10]: ratio b/a=0.92 (1 point) => better
- autoresearch-cost/tx[batchWrite100]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[batchRead10]: ratio b/a=1.00 (1 point) => ~same
- autoresearch-cost/tx[readRepeat]: ratio b/a=1.00 (1 point) => ~same

## Single RW optimization (ADR-020)

The end of implementation of ADR-020, which optimizes single read/write
transactions.

### compare-refs summary

- base: 55b5f7f72ef2919af41faefb4a3681c03349cb15 (v1)
- target: 80724534f0ea9d3b4a1769aea21cdaabe0d9024b (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.91 median=0.98 max=1.02 (geomean=0.97)
- throughput[weak-read]: ratio b/a min=0.91 median=0.98 max=1.02 (geomean=0.97)
- throughput[write]: ratio b/a min=0.91 median=0.98 max=1.02 (geomean=0.97)
- latency-p50[strong-read]: ratio b/a min=0.99 median=1.00 max=1.02 (geomean=1.00)
- latency-p50[weak-read]: ratio b/a min=1.00 median=1.00 max=1.10 (geomean=1.02)
- latency-p50[write]: ratio b/a min=0.99 median=1.01 max=1.02 (geomean=1.01)
- retries: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00)
- backend-ops/tx: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00)

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.99 median=1.00 max=1.03 (geomean=1.00)
- throughput[weak-read]: ratio b/a min=0.99 median=1.00 max=1.03 (geomean=1.00)
- throughput[write]: ratio b/a min=0.99 median=1.00 max=1.03 (geomean=1.00)
- latency-p50[strong-read]: ratio b/a min=0.97 median=0.99 max=1.00 (geomean=0.99)
- latency-p50[weak-read]: ratio b/a min=0.90 median=1.00 max=1.12 (geomean=1.00)
- latency-p50[write]: ratio b/a min=0.99 median=1.00 max=1.00 (geomean=0.99)
- retries: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00)
- backend-ops/tx: ratio b/a min=0.98 median=1.00 max=1.02 (geomean=1.00)

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.92 median=0.97 max=1.01 (geomean=0.97)
- throughput[weak-read]: ratio b/a min=0.92 median=0.97 max=1.01 (geomean=0.97)
- throughput[write]: ratio b/a min=0.92 median=0.97 max=1.01 (geomean=0.97)
- latency-p50[strong-read]: ratio b/a min=0.98 median=1.00 max=1.00 (geomean=1.00)
- latency-p50[weak-read]: ratio b/a min=1.00 median=1.00 max=1.05 (geomean=1.01)
- latency-p50[write]: ratio b/a min=0.98 median=1.00 max=1.01 (geomean=1.00)
- retries: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00)
- backend-ops/tx: ratio b/a min=0.99 median=1.00 max=1.01 (geomean=1.00)

### deadlock

- deadlock-p50: ratio b/a min=0.85 median=0.95 max=1.02 (geomean=0.94)
- deadlock-p90: ratio b/a min=0.89 median=0.98 max=1.02 (geomean=0.97)

### mixbench

- mix-tps[roMulti]: ratio b/a min=0.83 median=1.09 max=1.15 (geomean=1.03)
- mix-tps[roSingle]: ratio b/a min=0.78 median=1.05 max=1.15 (geomean=1.00)
- mix-tps[rwMany]: ratio b/a min=0.53 median=1.16 max=1.19 (geomean=0.96)
- mix-tps[rwSingle]: ratio b/a min=0.41 median=0.80 max=1.36 (geomean=0.77)
- mix-ops/tx[hi/roMulti]: ratio b/a min=1.07 median=1.07 max=1.07 (geomean=1.07)
- mix-ops/tx[hi/roSingle]: ratio b/a min=1.05 median=1.05 max=1.05 (geomean=1.05)
- mix-ops/tx[hi/rwMany]: ratio b/a min=0.98 median=0.98 max=0.98 (geomean=0.98)
- mix-ops/tx[hi/rwSingle]: ratio b/a min=0.98 median=0.98 max=0.98 (geomean=0.98)
- mix-ops/tx[lo/roMulti]: ratio b/a min=0.99 median=0.99 max=0.99 (geomean=0.99)
- mix-ops/tx[lo/roSingle]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00)
- mix-ops/tx[lo/rwMany]: ratio b/a min=0.98 median=0.98 max=0.98 (geomean=0.98)
- mix-ops/tx[lo/rwSingle]: ratio b/a min=0.63 median=0.63 max=0.63 (geomean=0.63)
- mix-retries/tx[hi]: ratio b/a min=0.89 median=1.11 max=1.34 (geomean=1.10)
- mix-retries/tx[lo]: ratio b/a min=0.86 median=1.00 max=1.67 (geomean=1.09)
- mix-agg-ops/tx[hi]: ratio b/a min=0.96 median=0.96 max=0.96 (geomean=0.96)
- mix-agg-ops/tx[lo]: ratio b/a min=0.87 median=0.87 max=0.87 (geomean=0.87)

### efficiency

- autoresearch-score: v1=924.85 v2=942.15 ratio=1.019
- autoresearch-cost/tx: ratio b/a min=0.87 median=1.00 max=1.27 (geomean=1.02)
- autoresearch-ops/tx: ratio b/a min=0.88 median=1.00 max=1.29 (geomean=1.03)

## ADR-025 - ADR-026

Caching improvements and lock-dedup work.

### compare-refs summary

- base: 8e8011cdf0fd6c388823fd2dc6cd3ce2b0376623 (v1)
- target: 76463a7a583312784d7b0c80252636ec7aa751a2 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=1.13 median=1.26 max=1.98 (geomean=1.37)
- throughput[weak-read]: ratio b/a min=1.13 median=1.26 max=1.98 (geomean=1.37)
- throughput[write]: ratio b/a min=1.13 median=1.26 max=1.98 (geomean=1.37)
- latency-p50[strong-read]: ratio b/a min=0.53 median=0.82 max=0.85 (geomean=0.74)
- latency-p50[weak-read]: ratio b/a min=0.83 median=0.92 max=1.00 (geomean=0.91)
- latency-p50[write]: ratio b/a min=0.48 median=0.66 max=0.67 (geomean=0.61)
- retries: ratio b/a min=1.07 median=1.17 max=1.18 (geomean=1.14)
- backend-ops/tx: ratio b/a min=1.06 median=1.15 max=1.15 (geomean=1.12)

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.94 median=1.31 max=1.85 (geomean=1.32)
- throughput[weak-read]: ratio b/a min=0.94 median=1.31 max=1.85 (geomean=1.32)
- throughput[write]: ratio b/a min=0.94 median=1.31 max=1.85 (geomean=1.32)
- latency-p50[strong-read]: ratio b/a min=0.53 median=0.83 max=1.26 (geomean=0.82)
- latency-p50[weak-read]: ratio b/a min=0.83 median=1.00 max=1.25 (geomean=1.01)
- latency-p50[write]: ratio b/a min=0.48 median=0.65 max=0.88 (geomean=0.65)
- retries: no data
- backend-ops/tx: no data

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=1.15 median=1.26 max=2.29 (geomean=1.43)
- throughput[weak-read]: ratio b/a min=1.15 median=1.26 max=2.29 (geomean=1.43)
- throughput[write]: ratio b/a min=1.15 median=1.26 max=2.29 (geomean=1.43)
- latency-p50[strong-read]: ratio b/a min=0.44 median=0.85 max=0.88 (geomean=0.73)
- latency-p50[weak-read]: ratio b/a min=0.71 median=0.91 max=0.98 (geomean=0.87)
- latency-p50[write]: ratio b/a min=0.48 median=0.68 max=0.72 (geomean=0.63)
- retries: ratio b/a min=1.04 median=1.04 max=1.04 (geomean=1.04)
- backend-ops/tx: ratio b/a min=1.03 median=1.03 max=1.03 (geomean=1.03)

### deadlock

- deadlock-p50: ratio b/a min=0.67 median=4.69 max=11.93 (geomean=3.08)
- deadlock-p90: ratio b/a min=0.29 median=0.34 max=1.83 (geomean=0.46)

### mixbench

- mix-tps[roMulti]: ratio b/a min=2.79 median=5.68 max=9.20 (geomean=5.30)
- mix-tps[roSingle]: ratio b/a min=0.93 median=1.33 max=1.48 (geomean=1.25)
- mix-tps[rwMany]: ratio b/a min=1.42 median=1.83 max=8.54 (geomean=2.53)
- mix-tps[rwSingle]: ratio b/a min=1.14 median=1.55 max=1.99 (geomean=1.52)
- mix-ops/tx[hi/roMulti]: ratio b/a min=0.70 median=0.70 max=0.70 (geomean=0.70)
- mix-ops/tx[hi/roSingle]: ratio b/a min=1.08 median=1.08 max=1.08 (geomean=1.08)
- mix-ops/tx[hi/rwMany]: ratio b/a min=0.76 median=0.76 max=0.76 (geomean=0.76)
- mix-ops/tx[hi/rwSingle]: ratio b/a min=0.87 median=0.87 max=0.87 (geomean=0.87)
- mix-ops/tx[lo/roMulti]: ratio b/a min=1.11 median=1.11 max=1.11 (geomean=1.11)
- mix-ops/tx[lo/roSingle]: ratio b/a min=1.04 median=1.04 max=1.04 (geomean=1.04)
- mix-ops/tx[lo/rwMany]: ratio b/a min=0.99 median=0.99 max=0.99 (geomean=0.99)
- mix-ops/tx[lo/rwSingle]: ratio b/a min=1.02 median=1.02 max=1.02 (geomean=1.02)
- mix-retries/tx[hi]: ratio b/a min=0.48 median=0.78 max=1.30 (geomean=0.78)
- mix-retries/tx[lo]: ratio b/a min=0.88 median=2.04 max=2.54 (geomean=1.72)
- mix-agg-ops/tx[hi]: ratio b/a min=1.84 median=1.84 max=1.84 (geomean=1.84)
- mix-agg-ops/tx[lo]: ratio b/a min=2.15 median=2.15 max=2.15 (geomean=2.15)

### efficiency

- autoresearch-score: v1=1003.84 v2=933.75 ratio=0.930
- autoresearch-cost/tx: ratio b/a min=0.69 median=0.99 max=1.02 (geomean=0.93)
- autoresearch-ops/tx: ratio b/a min=0.70 median=0.99 max=1.02 (geomean=0.93)

## ADR-024

Designed in [ADR-024](../adr/024-hold-and-wait-conflict-resolution.md).

### compare-refs summary

- base: 0ed3eda3a60b7efe3395f2ae6573aa05b8e63297 (v1)
- target: 80ee152db2f6860313ffe97d660b9d62ee1c4870 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.96 median=3.37 max=3.59 (geomean=2.50)
- throughput[weak-read]: ratio b/a min=0.96 median=3.37 max=3.59 (geomean=2.50)
- throughput[write]: ratio b/a min=0.96 median=3.37 max=3.59 (geomean=2.50)
- latency-p50[strong-read]: ratio b/a min=0.95 median=1.11 max=1.14 (geomean=1.08)
- latency-p50[weak-read]: ratio b/a min=0.00 median=0.00 max=0.56 (geomean=0.00)
- latency-p50[write]: ratio b/a min=1.17 median=1.39 max=1.41 (geomean=1.34)
- retries: no data
- backend-ops/tx: no data

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.97 median=4.19 max=4.44 (geomean=2.95)
- throughput[weak-read]: ratio b/a min=0.97 median=4.19 max=4.44 (geomean=2.95)
- throughput[write]: ratio b/a min=0.97 median=4.19 max=4.44 (geomean=2.95)
- latency-p50[strong-read]: ratio b/a min=1.00 median=1.11 max=1.41 (geomean=1.15)
- latency-p50[weak-read]: ratio b/a min=0.00 median=0.00 max=0.80 (geomean=0.01)
- latency-p50[write]: ratio b/a min=1.21 median=1.36 max=1.60 (geomean=1.37)
- retries: no data
- backend-ops/tx: no data

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.88 median=1.33 max=1.93 (geomean=1.28)
- throughput[weak-read]: ratio b/a min=0.88 median=1.33 max=1.93 (geomean=1.28)
- throughput[write]: ratio b/a min=0.88 median=1.33 max=1.93 (geomean=1.28)
- latency-p50[strong-read]: ratio b/a min=1.01 median=1.06 max=1.08 (geomean=1.05)
- latency-p50[weak-read]: ratio b/a min=0.94 median=0.97 max=1.14 (geomean=1.00)
- latency-p50[write]: ratio b/a min=1.20 median=1.34 max=1.38 (geomean=1.31)
- retries: ratio b/a min=1.22 median=1.24 max=1.25 (geomean=1.24)
- backend-ops/tx: ratio b/a min=1.15 median=1.17 max=1.17 (geomean=1.17)

### deadlock

- deadlock-p50: ratio b/a min=1.12 median=1.30 max=16.79 (geomean=2.37)
- deadlock-p90: ratio b/a min=1.19 median=1.30 max=2.95 (geomean=1.48)

### mixbench

- mix-tps[roMulti]: ratio b/a min=1.44 median=1.94 max=3.42 (geomean=2.05)
- mix-tps[roSingle]: ratio b/a min=1.13 median=1.94 max=12.37 (geomean=2.60)
- mix-tps[rwMany]: ratio b/a min=0.72 median=0.97 max=1.43 (geomean=0.98)
- mix-tps[rwSingle]: ratio b/a min=0.78 median=1.12 max=2.37 (geomean=1.23)
- mix-ops/tx[hi/roMulti]: ratio b/a min=1.53 median=1.53 max=1.53 (geomean=1.53)
- mix-ops/tx[hi/roSingle]: ratio b/a min=0.59 median=0.59 max=0.59 (geomean=0.59)
- mix-ops/tx[hi/rwMany]: ratio b/a min=2.77 median=2.77 max=2.77 (geomean=2.77)
- mix-ops/tx[hi/rwSingle]: ratio b/a min=0.86 median=0.86 max=0.86 (geomean=0.86)
- mix-ops/tx[lo/roMulti]: ratio b/a min=1.00 median=1.00 max=1.00 (geomean=1.00)
- mix-ops/tx[lo/roSingle]: ratio b/a min=0.99 median=0.99 max=0.99 (geomean=0.99)
- mix-ops/tx[lo/rwMany]: ratio b/a min=1.20 median=1.20 max=1.20 (geomean=1.20)
- mix-ops/tx[lo/rwSingle]: ratio b/a min=1.17 median=1.17 max=1.17 (geomean=1.17)
- mix-retries/tx[hi]: ratio b/a min=0.42 median=1.79 max=3.53 (geomean=1.29)
- mix-retries/tx[lo]: ratio b/a min=1.18 median=1.39 max=4.49 (geomean=1.79)
- mix-agg-ops/tx[hi]: ratio b/a min=1.15 median=1.15 max=1.15 (geomean=1.15)
- mix-agg-ops/tx[lo]: ratio b/a min=1.09 median=1.09 max=1.09 (geomean=1.09)

### efficiency

- autoresearch-score: v1=934.42 v2=1003.84 ratio=1.074
- autoresearch-cost/tx: ratio b/a min=1.00 median=1.00 max=1.22 (geomean=1.07)
- autoresearch-ops/tx: ratio b/a min=1.00 median=1.00 max=1.24 (geomean=1.08)

## v2 MVP

Described in [object-storage-native.md](../designs/object-storage-native.md) and implemented by
ADRs (016 - 023).

### compare-refs summary

- base: e2171c3c8e2d6b9f7bf27c57b59e802c04f3a1fd (v1)
- target: 0ed3eda3a60b7efe3395f2ae6573aa05b8e63297 (v2)
- ratio = v2 / v1 (throughput >1 good; latency/ops/cost <1 good)

### rw9010/balanced

- throughput[strong-read]: ratio b/a min=0.21 median=0.25 max=0.46 (geomean=0.28)
- throughput[weak-read]: ratio b/a min=0.21 median=0.25 max=0.46 (geomean=0.28)
- throughput[write]: ratio b/a min=0.21 median=0.25 max=0.46 (geomean=0.28)
- latency-p50[strong-read]: ratio b/a min=0.69 median=0.72 max=1.88 (geomean=0.90)
- latency-p50[weak-read]: ratio b/a min=1.12 median=630.88 max=839.30 (geomean=137.82)
- latency-p50[write]: ratio b/a min=1.47 median=1.51 max=2.40 (geomean=1.68)
- retries: no data
- backend-ops/tx: no data

### rw9010/readheavy

- throughput[strong-read]: ratio b/a min=0.21 median=0.24 max=0.50 (geomean=0.27)
- throughput[weak-read]: ratio b/a min=0.21 median=0.24 max=0.50 (geomean=0.27)
- throughput[write]: ratio b/a min=0.21 median=0.24 max=0.50 (geomean=0.27)
- latency-p50[strong-read]: ratio b/a min=0.65 median=1.03 max=1.90 (geomean=1.04)
- latency-p50[weak-read]: ratio b/a min=1.12 median=464.42 max=783.50 (geomean=116.48)
- latency-p50[write]: ratio b/a min=1.27 median=1.49 max=2.41 (geomean=1.61)
- retries: no data
- backend-ops/tx: no data

### rw9010/writeheavy

- throughput[strong-read]: ratio b/a min=0.19 median=0.30 max=0.37 (geomean=0.28)
- throughput[weak-read]: ratio b/a min=0.19 median=0.30 max=0.37 (geomean=0.28)
- throughput[write]: ratio b/a min=0.19 median=0.30 max=0.37 (geomean=0.28)
- latency-p50[strong-read]: ratio b/a min=0.70 median=0.74 max=2.11 (geomean=0.95)
- latency-p50[weak-read]: ratio b/a min=1.20 median=283.42 max=661.00 (geomean=32.14)
- latency-p50[write]: ratio b/a min=1.54 median=1.60 max=2.48 (geomean=1.77)
- retries: no data
- backend-ops/tx: no data

### deadlock

- deadlock-p50: ratio b/a min=0.56 median=3.89 max=5.27 (geomean=2.32)
- deadlock-p90: ratio b/a min=0.50 median=20.22 max=24.21 (geomean=10.50)

### mixbench

- mix-tps[roMulti]: ratio b/a min=0.12 median=0.43 max=0.99 (geomean=0.37)
- mix-tps[roSingle]: ratio b/a min=0.48 median=0.80 max=1.46 (geomean=0.82)
- mix-tps[rwMany]: ratio b/a min=0.07 median=0.42 max=1.09 (geomean=0.28)
- mix-tps[rwSingle]: ratio b/a min=0.18 median=0.51 max=0.85 (geomean=0.40)
- mix-ops/tx[hi/roMulti]: ratio b/a min=6.16 median=6.16 max=6.16 (geomean=6.16)
- mix-ops/tx[hi/roSingle]: ratio b/a min=5.83 median=5.83 max=5.83 (geomean=5.83)
- mix-ops/tx[hi/rwMany]: ratio b/a min=4.30 median=4.30 max=4.30 (geomean=4.30)
- mix-ops/tx[hi/rwSingle]: ratio b/a min=3.16 median=3.16 max=3.16 (geomean=3.16)
- mix-ops/tx[lo/roMulti]: ratio b/a min=2.54 median=2.54 max=2.54 (geomean=2.54)
- mix-ops/tx[lo/roSingle]: ratio b/a min=2.32 median=2.32 max=2.32 (geomean=2.32)
- mix-ops/tx[lo/rwMany]: ratio b/a min=2.60 median=2.60 max=2.60 (geomean=2.60)
- mix-ops/tx[lo/rwSingle]: ratio b/a min=3.12 median=3.12 max=3.12 (geomean=3.12)
- mix-retries/tx[hi]: ratio b/a min=0.59 median=1.56 max=2.62 (geomean=1.39)
- mix-retries/tx[lo]: ratio b/a min=0.14 median=0.40 max=0.68 (geomean=0.35)
- mix-agg-ops/tx[hi]: ratio b/a min=1.83 median=1.83 max=1.83 (geomean=1.83)
- mix-agg-ops/tx[lo]: ratio b/a min=2.32 median=2.32 max=2.32 (geomean=2.32)

### efficiency

- autoresearch-score: v1=402.92 v2=934.42 ratio=2.319
- autoresearch-cost/tx: ratio b/a min=1.81 median=1.84 max=4.55 (geomean=2.32)
- autoresearch-ops/tx: ratio b/a min=0.99 median=1.99 max=4.86 (geomean=1.81)

## Baseline (v0.1.0)

autoresearch-score: 403.57
