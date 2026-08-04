# Performance investigations

This is the log for manual performance investigations that do not belong to the
autonomous [`autoresearch`](../autoresearch/README.md) loop. Entries may describe
temporary instrumentation, inconclusive variants, and design candidates.

This file is evidence, not a record of accepted behavior:

- [`docs/guides/perf.md`](../../docs/guides/perf.md) records metrics for landed
  performance-affecting changes.
- [`docs/guides/perf-todos.md`](../../docs/guides/perf-todos.md) tracks open
  performance work.
- ADRs record significant decisions once accepted.

## 2026-08-03: Simulated-time calibration and cross-Database attribution

Status: benchmark timing correction implemented; corrected affinity curves and
the production-timescale canonical baseline rerun are complete.

Reference: `14de11e8`, after `mixbench` and `rtbench` were consolidated into
`perfbench`. The investigation asks whether the new affinity workload measures
steady-state cross-Database costs faithfully and which foreground phase causes
the remaining logged-path gap.

### Timing calibration

`Bench` converts compressed wall time back into a simulated production-time
domain, but the original `0.02` profile compressed only backend delays and rate
limits. The engine retained its production 200 ms to 5 s coordination retry
schedule. A single 200 ms retry was consequently reported as 10 seconds.

Temporary CLI overrides applied the same compression to `RetryConfig`. In the
spread, 0%-affinity endpoint this changed `rwSingle` from `1.77` to `14.51`
transactions/s and `rwMany` from `0.79` to `6.29`; their p90 fell from
`11.9/23.8 s` to `0.88/1.92 s`. Aggregate throughput moved only from `65.85` to
`74.62` transactions/s because the concurrently running reads dominate the
total and faster writes consume some of the same backend capacity. CAS retries
rose from `0.021` to `0.696` per transaction: the unscaled sleep was suppressing
work, not resolving contention efficiently. The hot 0%-affinity aggregate rose
from about `17.2` to `28.4` transactions/s.

Scaling retry timing alone was insufficient. S3-profile reads average 22 ms;
at `0.02` their requested sleep is about 0.44 ms, below the practical Tokio
timer granularity. Three-run hot-mode calibration sweeps used proportional
retry intervals and no real S3:

| Delay scale | Retry initial / max | 0% affinity tx/s | 100% affinity tx/s |
| ---: | ---: | ---: | ---: |
| `0.02` | `4 / 100 ms` | `28.4` | `69.4` |
| `0.05` | `10 / 250 ms` | `43.5` | `110.3` |
| `0.10` | `20 / 500 ms` | `49.6` | `136.4` |
| `0.20` | `40 / 1000 ms` | `65.1` | `149.6` |
| `0.50` | `100 / 2500 ms` | `51.6` | `169.9` |
| `1.00` | `200 / 5000 ms` | `71.4` | `169.6` |

All cells completed without failures. These are calibration runs with different
fixed windows, not reference-comparison results. The isolated 100%-affinity
case converges monotonically and `0.5` matches uncompressed throughput. The
0%-affinity case has a much noisier long tail; its `0.5` point is not monotonic.
At `0.2`, however, both aggregate rates are within `9–12%` of the uncompressed
medians, total operations/transaction are within `4–7%`, and individual backend
sleeps are several milliseconds. It is the smallest practical scale supported
by this calibration. `0.5` or uncompressed runs remain the confirmation tier
for a contentious decision.

The benchmark now defaults to `0.2` for simulated backends. ADR-058 replaced
the temporary per-builder correction with one immutable process-wide
model-time speedup. Nominal backend latency and rate limits, SDK and engine
retries, protocol-liveness timing, deadlock budgets, and background cadence now
advance together. Reported latency and throughput use that model time;
measurement windows, settlement quiet periods, cooldowns, and drain deadlines
remain real wall time. A historical reference comparison whose older binary
lacks process-wide model time must use `delay-scale=1`; the harness rejects an
accelerated comparison with mismatched timing models.

The first corrected sweep also showed why the settlement window cannot remain
at two seconds. Identical 5,000-key spread cells reported 108 and 56 completed
splits: a long in-flight split left the counter unchanged long enough for a
false success. Three calibration cells with a ten-second quiet interval each
settled at 116 completed splits after `34.4–36.2 s`. The default is now ten
seconds for local and real-provider runs; the signal remains only the completed
counter, with no topology-specific expected count.

### Foreground attribution

Temporary counters, removed after the experiment, bracketed holder waiting and
the shard coordinator's submission, load, resolution, store, and backoff
phases. The existing role-aware backend wrapper separated node and
transaction-log traffic. Four Databases ran every shape with eight workers per
shape; setup completed its split cascade before measurement.

With corrected `0.02` retry timing, the backend attribution was:

| Mode / affinity | Aggregate tx/s | Node ops/tx | Transaction-log reads/tx |
| --- | ---: | ---: | ---: |
| spread / 0% | `74.6` | `5.58` | `0.310` |
| spread / 100% | `128.5` | `3.57` | `0.039` |
| hot / 0% | `28.4` | `1.77` | `0.458` |
| hot / 100% | `69.4` | `0.57` | `0.049` |

The uncompressed hot calibration confirms the phase shape. At 0% affinity,
coordinator submission wait is `302 ms/transaction`, versus `118 ms` at 100%;
load/store add `6.4/34.3 ms`, versus `2.7/15.0 ms`. Resolution itself is only
`5.4` versus `3.5 ms`. Explicit holder waiting moves in the opposite direction:
`20.3 ms/transaction` at 0%, versus `35.6 ms` at 100%.

At 100% affinity, all traffic for one collection passes through one Database's
cache and shard coordinator. It can fold local submissions into fewer CAS
rounds and already knows the status of its own transactions. At 0%, the same
logical collection load is distributed across independent coordinators. They
cannot merge across processes, issue competing node CASes, reload losers, and
read foreign transaction logs. The dominant cost is therefore cross-client
node arbitration and lost local batching; foreign status resolution is a
secondary cost. It is not a holder-polling delay.

The benchmark now retains the already-public coordinator and direct-path
counters in each mixed cell's `aggregateProtocol` object. A three-run hot sweep
shows why complete affinity is qualitatively different:

| Affinity | Aggregate tx/s | Backend ops/tx | Coordinator rounds/tx | Members/round | CAS retries/tx | Direct land rate |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `0%` | `62.0` | `2.08` | `0.497` | `1.74` | `0.111` | `35.3%` |
| `25%` | `59.5` | `2.11` | `0.494` | `1.82` | `0.117` | `33.8%` |
| `50%` | `53.1` | `1.95` | `0.457` | `1.98` | `0.108` | `28.7%` |
| `75%` | `57.0` | `2.07` | `0.532` | `2.09` | `0.124` | `29.8%` |
| `100%` | `161.6` | `0.81` | `0.274` | `3.27` | `0` | `17.4%` |

The 100% endpoint wins despite landing a smaller fraction of direct candidates.
One coordinator folds almost twice as many members per round, needs about half
as many rounds per transaction, and never loses a leaf CAS to another
Database. At every partial-affinity point, foreign writers preserve the CAS
retry rate; extra local traffic increases fold width gradually but cannot
produce the endpoint's single-owner behavior.

The spread endpoint rerun has noisier absolute throughput (`181.6` versus
`339.3` transactions/s, compared with `227.2/340.5` in the full curve), but
isolates the other mechanism. Members/round barely moves from `1.06` to `1.13`,
while CAS retries fall from `0.414` per transaction to zero and backend work
falls from `5.14` to `4.21` operations/transaction. With keys distributed
across many leaves there is little local folding opportunity; independent
Databases instead collide on the same multi-key leaf even when their logical
keys differ. v0.1.0's one-object-per-key representation did not have this
cross-key CAS domain, although it paid much more backend work elsewhere.

### Earlier-split screen

A temporary benchmark-only override lowered the ordinary 256-entry leaf cap,
with every setup and measurement Database configured identically. It was
removed after the screen. One 0%-affinity spread cell at each lower cap gives:

| Leaf cap | Setup splits | Settle wall time | Aggregate tx/s | Backend ops/tx | CAS retries/tx | rwSingle tx/s | rwMany tx/s |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| `256` | `116–128` | `33.4–38.4 s` | `178.6–226.5` | `4.89–5.18` | `0.403–0.438` | `20.1–28.3` | `10.3–13.5` |
| `128` | `236` | `59.6 s` | `198.7` | `6.06` | `0.479` | `27.2` | `14.0` |
| `64` | `508` | `115.4 s` | `271.8` | `3.30` | `0.084` | `25.2` | `4.59` |

The 128-entry cap approximately doubles structural work without moving any
steady-state signal outside the default run-to-run range; it does not lower CAS
retries. At 64 entries, CAS contention and single-read latency improve, but the
multi-key write rate loses more than half and setup performs roughly four times
as many splits. The cell then runs longer for `rwMany` to reach its sample
target, so fast reads make the aggregate throughput and operations/transaction
look better; those aggregate values do not represent an unchanged completed
transaction mix.

A 100%-affinity 64-entry control has zero CAS retries and improves `rwSingle`
from `95–98` to `113` transactions/s and `rwMany` from `34–35` to `37`, but
reduces `roMulti` from `44–45` to `33.6`. This confirms a genuine parallelism
versus routing/fan-out trade-off, not a universally better tree shape. Lowering
the global/default threshold is therefore rejected. A future split response
would need to be demand-driven by sustained cross-client CAS contention,
bounded above a leaf-size floor, and evaluated separately for single- and
multi-key shapes.

### Rejected retry shortcuts

A spread-mode sweep shortened the initial retry from 16 ms down to zero in the
compressed domain. Zero backoff improved `rwSingle` from `14.5` to `18.1`
transactions/s and `rwMany` from `6.29` to `7.42`, but raised node operations
from `5.58` to `6.33` per transaction; aggregate throughput improved only
`3.4%`. An immediate first retry followed by normal backoff produced similar
spread throughput, but reduced hot `rwMany` throughput by about `9%` and raised
its p90 from `7.0` to `10.4 s`. Restricting the shortcut to locally singleton
rounds still reduced hot `rwMany` by about `10%` and left p90 at `8.6 s`:
singleton local membership does not imply low distributed contention.

Proportionally shortening the five-second suspected-deadlock timeout at
`delay-scale=0.5` changed aggregate throughput by only about `3%` and did not
repair the cross-client gap. No production retry or deadlock-timing change is
supported by these experiments.

### Corrected affinity curves

The first decision-grade sweep uses the corrected `0.2` S3 profile, automatic
`5x` retry-time scaling, three runs, all five affinities, both contention modes,
four Databases, and eight workers per shape. Every one of the 30 cells and all
four shapes converges to a 10% throughput-CI target with zero failures.

Spread setup takes `34.7–39.5 s` and completes `110–126` splits; hot setup
completes no splits and waits the full ten-second quiet window. Split-count
variation is caused by background splits racing the sequential seed batches.
It does not consistently explain throughput: all three 75%-affinity spread
cells complete 124 splits with nearly identical aggregate rates, while the
100%-affinity cells remain within 3% despite completing 116, 126, and 116
splits.

Three-run medians are:

| Mode / affinity | Aggregate tx/s | rwSingle | rwMany | roSingle | roMulti | Backend ops/tx |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| hot / 0% | `62.5` | `5.28` | `3.80` | `33.6` | `19.7` | `2.04` |
| hot / 25% | `57.0` | `4.79` | `3.64` | `31.5` | `17.1` | `2.00` |
| hot / 50% | `50.1` | `4.72` | `4.04` | `28.1` | `14.5` | `2.10` |
| hot / 75% | `53.9` | `4.15` | `3.83` | `28.3` | `15.6` | `1.88` |
| hot / 100% | `159.2` | `7.87` | `5.21` | `102.4` | `43.7` | `0.82` |
| spread / 0% | `227.2` | `30.0` | `11.2` | `157.4` | `26.9` | `4.85` |
| spread / 25% | `224.4` | `29.4` | `11.8` | `158.8` | `24.8` | `4.65` |
| spread / 50% | `187.8` | `26.7` | `11.3` | `128.5` | `21.3` | `4.72` |
| spread / 75% | `263.1` | `45.8` | `17.6` | `169.0` | `27.9` | `4.61` |
| spread / 100% | `340.5` | `97.0` | `35.3` | `166.2` | `44.9` | `4.28` |

The curve is not linear. Hot 25–75% affinity is no better than uniform access;
only complete isolation removes foreign status reads and cross-coordinator CAS
competition. Rare foreign operations retain most of the distributed
coordination cost without providing enough local traffic for the foreign
Database to batch effectively. Spread traffic begins benefiting at 75%, but
the 0% and 50% cells are noisy and the decisive change is again 100%.

At 100%, hot aggregate throughput is `2.55x` the 0% median and backend work
falls to `0.40x`. Spread throughput is `1.50x` and backend work `0.88x`.
Transaction-body retry rates do not explain the cliff: hot medians remain
`0.24–0.30` retries/transaction across the curve. The extra work is below that
counter, in shard-coordinator rounds, CAS misses, reloads, and transaction-log
resolution identified by the phase probe.

### Production-timescale v0.1.0 comparison

A final three-run comparison used `delay-scale=1`, five deterministic
efficiency samples, and three-second contention cells:

```console
BASE=v0.1.0 LABEL_A=v010 LABEL_B=current DELAY_SCALE=1 NUM_RUNS=3 \
  CONTENTION_DURATION=3s COUNT=5 DRAIN_TIMEOUT=90s \
  hack/aws-bench/compare-refs.sh --summary
```

The current and v0.1.0 binaries therefore use their unmodified production
retry intervals and backend-delay ratios. Contention p50 has a `2.16`
geomean ratio, `2.01` median, and `0.92–5.19` range; p90 has a `1.77`
geomean, `2.11` median, and `0.41–4.39` range. These closely reproduce the
previous compressed comparison's `2.00/1.78` p50/p90 geomeans. Simulated-time
distortion did not create the focused latency regression.

The one-key cell needs a narrower interpretation, however. v0.1.0 completes
`54–58` transactions in each nominal three-second run and current completes
`56–57`; current throughput is `17.4–17.8` transactions/s. Current p50 is
`259–266 ms`, versus `51–56 ms` on v0.1.0, because each committed transaction
incurs a median of about `3.18` replay retries and `4.18` direct candidates.
Five contending workers keep the serialized key busy, so the extra replay
latency does not reduce its aggregate completion rate. The remaining one-key
issue is latency and redundant foreground work, not lost system throughput.

The deterministic efficiency score improves from `403.04` to `97.48` cost per
transaction, a `0.242` ratio. `batchRead10`, `batchWrite100`, and `multiRMW10`
cost ratios are `0.183`, `0.011`, and `0.228`; `singleRMW` is at parity
(`0.982`). `readRepeat` is the only weighted-cost increase at `1.811`, but it
does not add a physical call: the same request moved from v0.1.0's metadata
class to the current object-read class and receives the harness's larger
weight. The aggregate operation-count geomean is `0.18`, confirming that
current does much less backend work than v0.1.0 in these deterministic cases.

This historical comparison cannot run `perfbench mixed`: v0.1.0 predates its
workload schema, split-settlement guard, and result envelope. Porting only the
driver would still leave the old engine without an equivalent completed-split
signal. The retired rw9010 result must therefore not be used as a baseline for
the affinity curve; it combined a different collection layout with unsettled
splitting. The corrected curve is currently an absolute characterization of
the current engine, not a cross-version throughput ratio.

### Current conclusion

The original `0.02` absolute throughput and tail numbers are not
decision-grade. They amplify engine retries by 50 and quantize backend sleeps.
The corrected affinity effect is nevertheless real: it survives the
uncompressed control and is explained by per-client batching/cache boundaries.
Partial affinity does not gradually recover the cost; complete collection
ownership is qualitatively different.

The production-timescale baseline also separates two signals that were
previously conflated. Current one-key throughput is already at v0.1.0 parity
despite its roughly five-times-higher p50, while deterministic backend work is
substantially lower. The next investigation should therefore target the
current engine's cross-client shard-CAS rounds, not the `readRepeat`
classification or the retired rw9010 throughput number.

The coordinator counters establish that cross-client leaf false sharing is
the remaining spread-path opportunity, but the threshold screen rejects a
global tree-shape change. The next design decision is whether repeated CAS
misses should provide a bounded, demand-driven split hint, analogous to
ADR-056's inline-pressure hint. Before implementation it needs an explicit
contention signal, hysteresis, a minimum leaf size, and a policy for multi-key
transactions; otherwise sustained true hot-key contention can irreversibly
split every unrelated entry away while making multi-leaf transactions worse.
Any candidate must show per-shape throughput and tail benefit on the corrected
affinity workload, not only a read-dominated aggregate improvement.

## 2026-07-29: Inline admission and structural amplification

Status: logged-publication simplification implemented by
[ADR-054](../../docs/adr/054-reserve-inline-publication-for-logless-commits.md);
inline-pressure splitting implemented and validated by
[ADR-056](../../docs/adr/056-demand-driven-inline-pressure-splits.md); budget
tuning completed by the [inline-policy sweep](#inline-policy-sweep) below.

Reference: `5c3e5ac6`, after ADR-053. The goal is to isolate ADR-051's
provisional inline budgets from the later contention fix.

### Initial sweep

A temporary role-counting backend wrapper, removed after the experiment,
distinguishes node, transaction-log, and structural-log operations and bytes.
Three runs sweep no inlining, the then-current 1 KiB / 64 KiB policy, 4 KiB and
16 KiB aggregate budgets, and selected per-value and encoded-object limits.
The workloads cover serial and dense-leaf RMW, batch write, cold and warm read,
cache pressure, and foreground versus background split work under the S3, GCS,
and memory profiles. Every run completes without transaction failures.

#### Direct-path benefit

- On a serial S3-profile RMW, the then-current policy reduces median latency
  from `614` to `177 ms/transaction` for 128 B values and from `627` to
  `192 ms/transaction` for 1 KiB values. A cold inline read uses one node
  operation instead of a node plus transaction-log read; a warm read gains
  little once the transaction object is cached.
- The GCS-profile latency gain is smaller but the operation saving remains:
  median latency falls from `1367` to `1017 ms/transaction` at 128 B and from
  `1176` to `1052 ms/transaction` at 1 KiB.
- Raising the per-value limit makes a serial 4 KiB S3-profile RMW land directly
  and reduces median latency from `525` to `183 ms/transaction`. This does not
  establish that dense leaves should carry 4 KiB inline values, where every CAS
  would rewrite them.

#### Dense-leaf mixed regime

- A logged 128-key batch transaction performs the same number of backend
  operations with or without inlining. At 1 KiB, however, median node-write
  volume rises from `11.26` to `118.24 KiB/transaction`: write-back adds a
  cached copy of values already durable in the transaction object.
- After that batch, the then-current policy lands only `50%` of 1 KiB single-key
  RMWs directly. Median latency remains roughly flat (`457` versus
  `451 ms/transaction`) and operations fall only from `2.48` to `2.38`, while
  node-write volume rises from `4.84` to `95.54 KiB/transaction`.
- Smaller aggregate budgets are worse, not safer. The 4 KiB and 16 KiB policies
  land `3.1%` and `12.5%` of 1 KiB direct candidates, respectively, but increase
  median S3-profile RMW latency to `667` and `626 ms/transaction` and operations
  to `3.63` and `3.42`.
- With 900 keys split across five leaves, the then-current policy lands exactly
  `320/900` 1 KiB candidates: 64 values per leaf, the 64 KiB budget's capacity.
  Compared with no inlining, median mutation work rises from `2.55` to
  `3.45` operations/transaction, node-write volume from `7.89` to
  `108.74 KiB/transaction`, and memory-profile latency by `33%`. At 128 B the
  same leaves have enough budget for every value; direct land rate is `100%`
  and latency improves by `30%`, although node-write bytes still rise by
  `3.9x`.

The aggregate budget is sticky and first-come. Logged write-backs consume it
even though their transaction objects remain authoritative; later direct
commits that require inline storage fall back to locking. Existing inline
payloads must be preserved because a logless writer may have no transaction
object, so every later leaf CAS continues to rewrite those bytes. This partial
coverage can therefore cost more than either full direct coverage or no
inlining.

#### Split attribution

Across three waves of 900 inserts, both policies produce exactly five split
candidates, four completed splits, zero deferrals, and 13 structural-log writes.
Inlining increases foreground node-write bytes by `4.1x` at 128 B and `6.0x` at
1 KiB, and background split node-write bytes by `4.6x` and `5.7x`,
respectively. Inline leaf growth alone therefore does not explain the earlier
`3.8–8.1x` increase in structural-log operation counts, although it clearly
amplifies the bytes moved by each structural operation.

### Write-back suppression and split proxy

A second three-run experiment uses 128 interleaved, existing 1 KiB keys on one
leaf. One logged transaction updates the 64 even keys, then two single-key RMW
passes update the 64 odd and 64 even keys. It compares current behavior with a
temporary variant that suppresses only new logged write-back inlining, while
preserving existing inline states and direct publication. A 64-entry leaf split
threshold is an upper-bound proxy for reacting to inline pressure; it is not a
proposed global default.

- Suppressing write-back inlining reduces the logged batch's node-write volume
  from `73.8` to `9.6 KiB` while retaining the same transaction-log write and
  two node writes. A cold scan adds one `65.7 KiB` transaction-log read, because
  every key names the same logged transaction; median scan latency is
  effectively unchanged in both profiles.
- With the normal 256-entry split threshold, current behavior lands `0/64`
  direct commits on the odd keys and `64/64` on the already-inline even keys.
  Suppressing write-back inlining reverses the result: `64/64`, then `0/64`.
  Total direct capacity does not change; the policy only chooses which keys
  receive it.
- The 64-entry split proxy performs one split and three structural-log writes,
  after which both passes land `64/64` direct commits. Combined median RMW
  latency falls from `570` to `265 ms` in the S3 profile and from `2211` to
  `1435 ms` in GCS. It also makes the logged batch span two leaves: node writes
  rise from two to four and median batch latency rises from `7.2` to `9.8 ms`
  in S3 and from `6.9` to `11.4 ms` in GCS.

As secondary guardrails, five deterministic autoresearch runs move the median
score from `99.72` to `97.36` with write-back inlining suppressed; the
`batchWrite100` median falls from `8.32` to `5.94 ms/transaction`, although the
overall score ranges overlap. Three short paired 128 B `lo/shared` mixbench
runs remain at parity: aggregate-throughput median ratio `0.99`, total backend
operations/transaction ratio `0.99`, and zero failures.

The implemented ADR-054 comparison against `ed590a8c` confirms the deterministic
write benefit: `batchWrite100` cost and operations/transaction both fall to
`0.941`, with the overall score at `0.987`. Its broader adaptive mixbench sweep
also exposes a workload not covered by the original `lo/shared` guardrail. In
`hi/per-shape`, `rwMany` throughput falls to `0.377` and p90 rises from
`0.988 s` to `13.762 s`, while object reads/transaction rise from `1.327` to
`1.764`. The same cell's read-only shapes become over `6x` faster and the other
mixed cells are mostly flat or better, so this is not a uniform slowdown.
The result is consistent with smaller leaf transfers helping cached reads while
cross-client logged-value resolution adds transaction-object lookups and a long
tail to contended multi-key mutations. That attribution needs a repeated,
phase-level run before changing policy.

### Multi-RMW tail follow-up

Status: closed as non-reproducible; the measured representation trade-off
supports ADR-054 and does not justify a tail-specific engine change.

The follow-up first isolates ADR-054 by comparing its accepted parent
`f618e738` with implementation `7bc6fb01`. Three alternating S3-profile
`hi/per-shape` pairs use eight workers per shape, four client Databases per
shape, eight hot keys, a 10% throughput-CI target, and a 30-second cap. Every
shape converges with zero failures.

- `rwMany` throughput ratios are `1.459`, `1.062`, and `1.473`.
- p50 ratios are `0.998`, `0.995`, and `1.014`; p90 ratios are `0.988`,
  `1.009`, and `0.926`.
- Object-read ratios are `0.923`, `1.021`, and `0.968`; object-write ratios are
  `0.967`, `0.997`, and `0.981`.

The original broad comparison (`ed590a8c` to `7bc6fb01`) is also repeated with
the same settings. Its throughput ratios are `0.961`, `1.593`, and `1.390`,
while p90 ratios are `0.984`, `1.022`, and `1.012`. Neither comparison
reproduces the prior `0.377x` throughput and `13.9x` p90 result. That result was
a one-run outlier, not evidence that ADR-054 regressed multi-RMW.

Temporary instrumentation, applied identically to the isolated refs, wraps
each benchmark Database with role- and byte-aware backend counters and brackets
the measured foreground separately from shutdown. It also records decoded L1
hits and misses. The wrapper perturbs scheduling, so its timing is not used;
the operation and byte deltas are stable and all runs still converge without
failures.

For `rwMany` in the cross-client `per-shape` topology:

- transaction-log body reads rise by only `11.3–14.8 B/transaction`; physical
  transaction-log calls are too variable to distinguish because unchanged
  conditional reads transfer no body;
- L1 misses rise by `0.12–0.39/transaction`;
- node reads fall by `57–81 B/transaction`; and
- node writes fall by `2.14–2.15 KiB/transaction`.

The shared-Database topology makes the added transaction-log body transfer
almost disappear (`0.071–0.079 B/transaction` across the whole mixed cell),
confirming that the decoded cache absorbs repeated resolution when clients
share it. Cross-client caches cannot share that entry, but their extra body
transfer remains much smaller than the leaf bytes no longer rewritten.
Shutdown attribution is zero for every per-shape run and negligible in the
shared runs, so this is a foreground representation trade-off rather than
deferred cleanup.

### Demand-driven split validation

A focused scenario in the existing `rtbench` binary fills one 192-entry leaf's
64 KiB aggregate budget with 64 direct 1 KiB mutations. Two distinct external
keys then encounter aggregate rejection. The first requests the root split; the
second, after rerouting, requests a split of the still-saturated child. A final
64-mutation wave alternates between the two leaves with newly available
capacity. The fixture stays below ADR-031's ordinary entry and encoded-byte
thresholds, so pressure is the only possible split cause.

The harness is applied identically to `e88cb819` and `0be65fee`; the historical
copy changes only its statistics-field adapter because ADR-056's pressure
counters do not exist there. Three release-mode pairs per profile are
interleaved by side.

- Every target run processes two pressure candidates, completes two splits,
  and records no deferral or discard. The tree grows from one leaf to three.
  The base remains at one.
- Both discovering mutations still use the locked fallback. The later wave
  changes from `0/64` to `64/64` direct landings and from 64 lock calls to zero.
- S3-profile recovery throughput improves by `3.50–3.65x`, with p50 at
  `0.26–0.27x`, p90 at `0.29–0.32x`, operations/transaction at `0.26–0.29x`,
  and write bytes/transaction at `0.20–0.22x`.
- GCS-profile recovery throughput improves by `3.46–3.90x`, with p50 at
  `0.22–0.25x`, p90 at `0.51–0.66x`, operations/transaction at `0.28–0.29x`,
  and write bytes/transaction at `0.20–0.22x`.
- Including both discovering fallbacks and structural work, total
  operations/transaction fall to `0.53–0.55x` in S3 and `0.53–0.54x` in GCS;
  total write bytes/transaction fall to `0.42–0.45x` and `0.42–0.44x`.

Access order is material under the GCS profile. An initial version grouped 32
recovery mutations on one new leaf before moving to the next. It still doubled
throughput, but target p90 rose to `1.61–1.64x` because the model enforces GCS's
one-write-per-second limit per object and its retry backoff can overshoot the
next token. Alternating the same fixed keys across the two leaves measures the
parallel capacity created by the split and changes p90 to `0.51–0.66x`. This
does not promise relief for a workload that remains concentrated on one leaf;
tree widening helps only when demand spans the new ranges.

### Inline-policy sweep

Status: complete. Among the tested policies, the recommended default is a
1 KiB per-value limit and a 16 KiB aggregate leaf limit. The temporary harness
was removed after recording the results.

The sweep used reference `9fce478d`. It ran three 32-cell
matrices in forward/reverse/forward order over the same policies, values, and
S3/GCS delay profiles. Every cell measured 24 serial RMWs, 128 interleaved
dense RMWs, cold and warm reads after a fresh 256 KiB-cache reopen, and one
128-key logged batch. Fresh strong reads verified the serial and dense markers
after bounded shutdown. All 864 phase rows reported zero failures, every
bounded shutdown completed, and all fresh verification passed. Eligible
partial-admission cells included a separate three-second settle phase so the
splitter's real-time sweep cadence was measured outside foreground throughput.

The dense-wave medians below are relative to `InlinePolicy::none()`. Node bytes
include foreground, settle, and shutdown node mutations, amortized over the 128
logical mutations. Split counts are pressure splits over the same interval.

| Value / aggregate policy | Direct land, S3 / GCS | S3 rate / p50 / p90 | GCS rate / p50 / p90 | Node KiB/tx, S3 / GCS | Splits, S3 / GCS |
| --- | ---: | ---: | ---: | ---: | ---: |
| 8 B / any inline budget | 100% / 100% | `2.54–2.59x` / `0.37–0.38x` / `0.41x` | `1.12x` / `0.36–0.37x` / `2.36x` | `5.0` / `5.0` | `0` / `0` |
| 128 B / 4 KiB | 25% / 72% | `0.87x` / `1.33x` / `1.25x` | `1.18x` / `0.42x` / `1.28x` | `13.5` / `7.8` | `1` / `3` |
| 128 B / 16 or 64 KiB | 100% / 100% | `2.43–2.48x` / `0.38–0.39x` / `0.42–0.43x` | `1.15–1.16x` / `0.37x` / `2.35x` | `12.8` / `12.8` | `0` / `0` |
| 1 KiB / 4 KiB | 6% / 16% | `0.54x` / `1.47x` / `1.33x` | `0.80x` / `1.01x` / `1.37x` | `16.1` / `11.8` | `3` / `8` |
| 1 KiB / 16 KiB | 13% / 50% | `0.65x` / `1.44x` / `1.32x` | `0.99x` / `0.87x` / `1.29x` | `35.4` / `26.0` | `1` / `6` |
| 1 KiB / 64 KiB | 50% / 50% | `1.09x` / `0.82x` / `1.27x` | `0.75x` / `1.24x` / `1.48x` | `81.4` / `86.7` | `1` / `1` |

The GCS p90 inversion is expected from this model: an uninterrupted sequence
of direct root CASes hits the modeled one-write-per-second same-object limit,
and its retry backoff overshoots the next token. Inlining still removes work and
improves p50, but a single hot leaf has worse modeled tail latency. Splitting
can spread disjoint keys over objects; one hot key cannot benefit. Node-CAS p50
was otherwise stable at roughly `104–109 ms` in S3. The delay model charges no
extra latency for larger payloads, so these results measure operation count and
rate limiting, not the real transfer cost of a 64 KiB CAS.

The cost boundary is sharper than foreground latency alone:

- Full admission at 8 B removes one transaction-log mutation per dense RMW,
  reduces backend operations to `0.36x`, and leaves node bytes effectively
  unchanged. This is an unambiguous win in both profiles.
- Full admission at 128 B also removes one transaction-log mutation and reduces
  operations to `0.35–0.37x`, but node write bytes rise from roughly `5–6` to
  `12.8 KiB/tx`. The 16 KiB and 64 KiB policies are equivalent in this cell;
  4 KiB falls onto the partial-admission cliff.
- At 1 KiB, the 64 KiB policy saves only `0.48` S3 and `0.44` GCS
  transaction-log operations per mutation in the first dense wave. Total write
  bytes rise by `12.5x` and `14.6x`, respectively. Smaller budgets cap each
  leaf but pay more fallbacks and permanent splits; none dominates both
  profiles. The 4 KiB aggregate candidate is not worth carrying forward.
- Over-budget 4 KiB values admit nothing and behave like no inlining, within
  run noise. This confirms that merely enabling an inline policy has no
  material fallback tax.

Reads repay some of the retained inline bytes. At 8 B and fully admitted 128 B,
cold reads fall from `2.01` to `1.01` backend operations per key. At 1 KiB, the
64 KiB policy's median half-coverage lowers cold operations to `1.52`; after a
warm pass, transaction-log reads fall to `0.03–0.04` per key versus `0.27` with
no inlining. External 4 KiB values exceed the 256 KiB cache working set and
still need about `0.98` transaction-log reads per warm key.

Logged batches publish no new inline values, but acquiring and clearing leaves
must preserve existing ones. With 128 B values, fully admitted policies write
`27.6 KiB` of node data versus `11.3 KiB` with no inlining. With 1 KiB values,
the 64 KiB policy writes `75.5 KiB` of node data and `207.3 KiB` total versus
`11.3 KiB` and `143.0 KiB`; narrower policies use smaller leaves but can touch
many more of them after pressure splitting. Batch latency was not consistently
different across the three samples, so bytes and node-CAS count are the useful
signals here.

The best default among the tested policies is therefore:

- `max_value_bytes = 1 KiB`
- `max_leaf_bytes = 16 KiB`

This is a robust default, not a universal optimum. The 4 KiB cap reaches partial
admission too early. The 64 KiB cap behaves identically to 16 KiB for the clear
8 B and 128 B wins, and both retain the one-CAS path for a hot 1 KiB key. Its
extra capacity helps transient S3 admission in the dense 1 KiB cell, but costs
`2.3–3.3x` as many node-write bytes as 16 KiB there and performs worse in the
GCS profile. Because the delay model does not charge for payload size, that
comparison is biased in favor of 64 KiB. A 16 KiB default keeps the proven
small-value and hot-key benefits while placing a four-times lower cap on
aggregate inline payload.

The trade-off is earlier, durable splitting and more leaves for dense 1 KiB
sets. Workloads that have measured request count as more important than leaf
bytes can still opt into 64 KiB through `DatabaseBuilder`. Lowering the default
is correctness-safe: existing inline values are grandfathered and the policy is
not persisted. Clients of one database should nevertheless deploy the same
configuration, because pressure splits durably change its topology.

### Post-selection guardrails

The default-only comparison uses `fbdb99f5` (1 KiB / 64 KiB) as the base and
`40bc6b8d` (1 KiB / 16 KiB) as the target. All cells complete with zero
transaction failures and bounded drains.

- The permanent inline-pressure scenario is explicitly pinned to 64 KiB, so
  its protocol outcomes are identical: `64/64` recovery mutations land
  directly and both sides complete exactly two pressure splits. Recovery
  throughput is at parity in S3 and `0.97` in GCS.
- Three alternating high-contention/per-shape mixbench pairs all converge. The
  read-shape throughput geomeans are `1.03` (`roSingle`) and `1.00`
  (`roMulti`), with backend-operations geomeans of `0.95` and `0.97`. The eight
  128 B hot values fit under both aggregate limits, so the absence of a stable
  delta is expected.
- Ten alternating autoresearch pairs put the total score at a `1.005` geomean.
  `batchWrite100` cost is `0.990`; its median object-write count is `112.5`
  under 64 KiB and `113.5` under 16 KiB. The suite's 8 B values and logged
  batches do not exercise the aggregate-cap difference.
- The broad rw9010 throughput geomeans are `0.92` balanced, `1.04` read-heavy,
  and `1.04` write-heavy; backend-operation geomeans are `1.08`, `1.02`, and
  `1.10`. Their wide paired ranges, together with the deterministic and focused
  results, do not establish a default-induced regression.

The guardrails therefore retain the 16 KiB default. Its intentional cost
remains the earlier pressure splitting measured by the sweep for dense 1 KiB
values; workloads that prefer fewer objects can select 64 KiB explicitly.

### Current conclusion

ADR-054 removes logged write-back amplification, and ADR-056 supplies the
focused inline-capacity fix without globally lowering split thresholds. The
multi-RMW follow-up finds no durable tail regression and shows that the
cross-client transaction-object transfer is small beside the saved leaf bytes.
The focused split result proves that pressure is observed, rerouted, converted
into capacity, and repaid by later mutations. It did not establish that the
then-current 1 KiB/64 KiB budgets were optimal, nor quantify permanent widening
under a broad workload.

The inline-policy sweep changed the default from 1 KiB / 64 KiB to 1 KiB /
16 KiB. The post-selection guardrails find no repeatable regression in the
existing below-cap workloads. Real-provider and repeated-wave measurements
remain useful for later tuning, but are not a blocker: 16 KiB is the safer
cross-profile default, while 64 KiB remains an explicit workload-specific
option.
