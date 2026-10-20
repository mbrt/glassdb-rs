# Performance investigations

This is the log for manual performance investigations. Entries may describe
temporary instrumentation, inconclusive variants, and design candidates.

This file is evidence, not a record of accepted behavior:

- [`docs/guides/perf.md`](../../docs/guides/perf.md) records metrics for landed
  performance-affecting changes.
- ADRs record significant decisions once accepted.

## 2026-10-20: a split saves queue wait, and costs divided rounds

Status: the queue wait that the avoidable time rule (ADR-074) counts is the
wait that a split removes. But the rule does not count the cost of a split: a
round that has members in both halves becomes two leaf CASes. Without the
throttle, the saved wait for each added write is 0.6 to 1.1 leaf CAS times.
With the throttle, it rises to 3.5 to 5.7 leaf CAS times, while the splits
lower the throughput. So a price of one leaf CAS time for each write does not
see the throttle.

These runs had one write limit for all collections. With one limit for each
collection, the same cell does not reach the limit, and behaves like the runs
without the throttle. With 5 database instances on one collection, the limit
comes back. The signals of the engine told the limit apart from a topology
that does not pay only with small margins, and none located the partition at
its limit. So ADR-082 rejects detection of the write limit. Splits from old rule requests continue after the
load stops (see the last section).

The first runs used the cell of the 2026-10-13 entry at `14aa6e295`, with the
throttle on and off. Each variant ran twice. Temporary counters measured
the leaf coordinator, the splits, and the writes of the memory backend, and
were removed after the runs.

### Counters

Every 10 s of wall time, over the 5 database instances:

- the coordinator rounds whose leaf CAS landed, their members, and the time of
  that CAS;
- the landed rounds with members on both sides of the median of the leaf
  (divided rounds). After a split, each of these rounds needs two CASes. A
  round with only members whose own keys are on both sides did not count;
- all queue wait, and the queue wait that the rule counts;
- in the runs without the throttle only, for each counted queue wait, the
  part that a split would remove: the time from the end of the last round with
  keys in the half of the member to the start of the round of the member, at
  most the wait (shadow wait). The end of a round came from one map for all
  database instances;
- the splits and their time, and the backend writes with their throttle wait.

### Results

All 4 warm-ups did 7,318 to 7,702 splits and 0 merges in 300 s, and did not
become quiet. Times are model time. The rates are for 10 s of wall time. One
run of each variant:

| Throttle | Time | Rounds | Members/round | Leaf CAS | Throttle wait/write | Counted wait | Divided rounds | Splits |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| on | 100 s | 172,651 | 2.42 | 194 ms | 96 ms | 2,102 s | 10,616 | 432 |
| on | 190 s | 173,801 | 1.48 | 412 ms | 291 ms | 2,404 s | 2,909 | 189 |
| on | 370 s | 174,589 | 1.26 | 671 ms | 517 ms | 472 s | 199 | 48 |
| off | 100 s | 305,544 | 2.18 | 72 ms | 0 | 680 s | 14,429 | 830 |
| off | 190 s | 628,084 | 1.41 | 71 ms | 0 | 148 s | 1,991 | 151 |
| off | 370 s | 730,707 | 1.28 | 72 ms | 0 | 103 s | 1,374 | 49 |

The second runs had the same rounds, members, leaf CAS time, and splits
within about 10%. The counted wait and the divided rounds varied more, for
example by 38% and 58% at 190 s with the throttle.

- Without the throttle, the shadow wait was equal to the counted wait in all
  windows. A member that the rule counts waits for a round of the other half,
  while its own half has no round. So a split removes the time that the rule
  counts. This is near to true by construction of the counter, and it does not
  include the time after a split, for example in the throttle.
- With the throttle, the landed rounds stayed at about 17,400 in each second
  of wall time for the whole warm-up. The backend write rate limits the rounds,
  and each split that lowers the members of a round lowers the throughput. The
  queue wait that the splits removed came back as throttle wait.
- Without the throttle, queue wait for each member fell from about 19 ms to
  about 3 ms, of a mean transaction time of 65 to 273 ms for each shape.
  Backend writes went from about 36,000 to about 78,000 in each second of wall
  time. The 2026-10-13 entry measured that the throughput did not change.
- Of all queue wait, the rule counts 4.6 to 18% with the throttle, and 2 to
  5.3% without it. The other part is wait between
  members in the same half, which no split removes.

The saved wait for each added write is the counted wait over the divided
rounds, in multiples of the leaf CAS time. The leaves are one root leaf plus
the splits of the seed and the warm-up, of run 1 and run 2:

| Throttle | 80 s | 100 s | 190 s | 370 s |
| --- | ---: | ---: | ---: | ---: |
| on | 0.77 to 0.85 | 1.02 to 1.04 | 1.74 to 2.01 | 3.53 to 5.69 |
| on, leaves | 1,215 to 1,370 | 2,270 to 2,320 | 4,880 to 4,980 | 6,980 to 7,050 |
| off | 0.67 to 0.69 | 0.65 to 0.73 | 0.92 to 1.04 | 1.03 |
| off, leaves | 1,120 to 1,410 | 2,810 to 2,990 | 5,600 to 5,980 | 7,290 to 7,520 |

- With the throttle, the ratio rises as the leaves get smaller. A price of one
  leaf CAS time for each write stops no split after about 100 s, and a split
  then pays more than the one before it.
- The counted wait grows with the time of a round, and so with the leaf CAS
  time. A price that is a multiple of the leaf CAS time grows in the same way.
  So the throttle does not change the decision of such a price, but the splits
  lowered the throughput by 36%.
- The ratios are of all leaves. Each leaf can have a different ratio.
- The divided rounds do not include rounds with a member whose own keys are on
  both sides of the median. Late in the throttled runs, these were 2 to 48% of
  the divided rounds, so the ratios are too high by up to this part.

### Results of the cell

| Throttle | Writes/tx | Members/round | `rwSingle` | `rwMany` | `roSingle` | `roMulti` |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| on | 1.21 to 1.24 | 1.24 to 1.25 | 271 to 286 | 206 to 207 | 1,939 to 1,964 | 837 to 886 |
| off | 2.03 to 2.04 | 1.28 to 1.29 | 1,454 to 1,500 | 729 to 750 | 2,979 to 3,068 | 1,336 to 1,418 |

The unit is tx/s.

### Cause

The rule counts the time that a split removes, but not the time that it adds,
except for the divided conflict time. The merge side counts the time that a
merge removes, but not the queue wait that it adds, and not the writes that it
saves. Under this load, the warm-ups made no merges. The keys of one
transaction are in different leaves, and no shape scans, so the merge side
likely has no time. The probe did not measure the merge side.

### One write limit for each collection

The s3 delay profile limited the writes of each prefix of 2 path segments, so
all collections of a database shared one limit of 3,500 writes in each second
(`db/_c`). S3 documents no partition boundary, and it partitions a prefix
further under a sustained load. The profile now limits each prefix of 3 path
segments, so each collection has its own limit (`db/_c/<collection>`), and
transaction records have 64 limits (`db/_t/<symbol>`).

With this limit, one run of the same cell had no throttle wait. The mean leaf
CAS time of a window under load stayed at 71 to 82 ms, the backend writes went from about 31,000 to about
80,000 in each second of wall time, and the warm-up did 7,459 splits and 0
merges. The cell had 1,564 tx/s of `rwSingle` and 777 tx/s of `rwMany`. Each
collection had about 3,200 writes in each second of model time, just below its
limit.

### Five database instances on one collection

The new `mixed` option `--collections` sets how many collections the database
instances share. One run with `--collections 1`, and the same other options,
with the limit for each collection:

| Time | Rounds | Members | Members/round | Leaf CAS | Throttle wait/write | Splits |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 50 s | 65,066 | 120,432 | 1.85 | 67 ms | 0 | 58 |
| 110 s | 128,400 | 190,692 | 1.49 | 65 ms | 0 | 99 |
| 150 s | 151,645 | 194,187 | 1.28 | 76 ms | 11 ms | 180 |
| 230 s | 158,902 | 205,515 | 1.29 | 147 ms | 69 ms | 191 |
| 310 s | 162,536 | 201,298 | 1.24 | 206 ms | 128 ms | 138 |

The rates are for 10 s of wall time, and the leaf CAS time is the mean of a
window. The seed made 113 splits, and the warm-up 5,167 splits and 0 merges.
The collection has all 20,000 keys of the pool, but each of the 5 own
collections of the other runs also had all of them. So this cell has 5 times
the load on each key, not only on each limit.

- Before about 150 s, the splits raised the round members, a measure of the
  throughput, by 61 to 71%.
- After about 150 s, the rounds stayed at about 16,000 in each second of wall
  time, which is about 3,200 in each second of model time. The members stayed
  the same, while the rule made about 2,700 more splits by 320 s. The mean leaf
  CAS time rose from 76 ms to 206 ms, of which 128 ms was throttle wait.

Two signals of the limit, from the same windows:

| Time | Throttle wait / leaf CAS | 1 − 62 ms / leaf CAS |
| ---: | ---: | ---: |
| 110 s | 0 | 0.05 |
| 150 s | 0.14 | 0.19 |
| 190 s | 0.41 | 0.49 |
| 310 s | 0.62 | 0.70 |

62 ms is about the lowest mean leaf CAS time of a window in the runs: 58.6 ms
in a window with 104 rounds, and 63.8 ms in the windows with more than 20,000
rounds. Without a throttle, the mean leaf CAS time under load was 69 to 82 ms,
so the second signal was 0.10 to 0.24. This difference comes from the load,
not only from jitter. The probe measured means, not medians. The engine cannot
see the throttle wait, because the memory backend waits inside the backend, and
a provider adapter waits in its retries.

### Medians and the landed rounds

A second probe recorded each landed leaf CAS time with its collection and leaf,
and printed the quantiles of each collection for each window. It ran two more
times on the cell with 5 database instances on one collection (c1a and c1b),
and one time with own collections (own), on the commit before ADR-082. The
values are for 10 s windows late in the warm-up, from 200 s to 320 s:

| Run | Splits | Rounds in 10 s | Members/round | Leaf CAS p50 | Throttle wait/write |
| --- | ---: | ---: | --- | ---: | ---: |
| c1a | 6,619 | 153,000 to 168,000 | 1.21 to 1.18 | 59 to 64 ms | 0 to 5 ms |
| c1b | 4,615 | 152,000 to 163,000 | 1.36 to 1.22 | 104 to 132 ms | 37 to 63 ms |
| own | 6,763 | 472,000 to 575,000 | 1.48 to 1.37 | 75 to 82 ms | 0 |

The rounds of `own` are of 5 collections, and the others of one. One window of
c1a, at 260 s, had a p50 of 86 ms and 22 ms of throttle wait.

- The same cell had a p50 of 59 to 64 ms in one run, and of 104 to 132 ms in
  the other. In both runs, the collection was at about 3,200 writes in each
  second of model time, and the splits added writes. c1a had little throttle
  wait, and c1b had 37 to 63 ms.
- With own collections, the p50 was 75 to 82 ms with no throttle wait. With a
  baseline of 59 ms, its saturation was about 0.23, against 0.05 for c1a and
  0.53 for c1b. So a threshold on the leaf CAS time has a small margin, and the
  baseline changes with the load.
- From 200 s to 320 s, the landed rounds changed by −0.2% (c1a) and +5.6%
  (c1b) at the limit, and by +0.3% in both runs with one limit for all
  collections. Without a limit, they rose by 10 to 18% (own, both runs without
  the throttle, and the run of depth 3 with own collections). But in windows
  of 30 s, the runs without a limit also had rises of less than 1%. So the
  check on the landed rounds needs some minutes of history, and its margin is
  small.
- After about 70 s, the busiest leaf had at most 1.6% of the leaf CASes of its
  collection. No run had a hot leaf.
- The throttle wait told the runs apart best: 0 without a limit, 0 to 22 ms in
  c1a, and 37 to 63 ms in c1b. The engine cannot see it.

### Splits continue after the load stops

From about 330 s, the cell paused its workers for its full garbage
collections. The restructurer still made 500, 551, 540, 174, 110, and 141
splits in the next 10 s windows, with almost no transactions: about 2,000
splits. The probe counted about 4,000 splits in the warm-up, against the 5,167
of the result, so it may not count all splits. The rule asks for more splits
than the restructurer can make, and the candidate queue keeps the requests.
The cause was not confirmed.

## 2026-10-13: concurrent splits, and a tree that still does not settle

Status: the restructurer now splits up to 16 nodes of one sweep at the same
time (`df17a7576`), and reconciles the parents of one batch together
(`85d7d03d8`). The seed settles in 25 to 29 s instead of 44 to 47 s. The
warm-up at 200 workers per shape still does not become quiet in 300 s. The
split time grows because of the simulated write throttle, but the warm-up does
not become quiet without the throttle either. Smaller leaves share fewer rounds
of the leaf coordinator, so under the throttle each split adds write load that
the rule does not see. Whether the rule should count it is an ADR-074
question.

This settles the open question of the 2026-10-12 entry: one split at a time no
longer limits the convergence of the tree.

All runs used one cell of the `mixed` scenario: the memory backend, the s3
delay profile, 5 instances, 100% affinity, 20,000 keys, lo mode, and 200
workers per shape:

```console
perfbench --backend memory --delays s3 mixed --modes lo --affinities 100 \
  --databases 5 --workers-per-shape 200 --num-keys 20000 \
  --quiet-period 10s --warmup-timeout 300s --duration 10s --max-duration 20s
```

Each variant ran once or twice, and a range gives the runs. Run-to-run
variance is large: the same commit became quiet after 119 s in one run and
after 280 s in another. Temporary counters
measured the restructurer and the backend, and were removed after the runs.

### Splits one at a time

Before `df17a7576`, a sweep of the restructurer made one split at a time. A
split took about 1.3 s of model time from start to end. Model time runs 5
times faster than wall time, so each instance made about 4 splits per second
of wall time. The rule asked for many more (see the 2026-10-12 entry).

### Concurrent splits and the collection record

A first variant ran the full lifecycle of up to 16 splits at the same time.
Each split joined and left the collection record with its own CAS, so the
splits lost most of these CASes to each other:

| Variant | Splits in a 90 s warm-up | Mean time of one split | Lost record CASes in 10 s |
| --- | ---: | ---: | ---: |
| One at a time | 1,336 | about 1.3 s | about 2 |
| 16 at a time, a record CAS for each split | 893 | about 35 s | about 2,700 |
| 16 at a time, a record CAS for each round | 1,353, quiet after 37 s | not measured | about 10 |

The times are model time. The warm-ups in this table used a quiet period of
3 s, which ends a warm-up at a pause in the splits. The later runs used 10 s.

`df17a7576` keeps one topology participant for each split (ADR-049), but the
participants of one collection in a batch join and leave the record with one
CAS.

### Parent reconciliation

After `df17a7576`, the sibling splits of one batch lost the gate of their
parent to each other. In the seed, about 9,100 parent gates were lost, and
about 470 splits ended in recovery. `85d7d03d8` makes the node writes of a
batch first, and then reconciles the routes of one parent with one gate and
one CAS. The reconciliations of the sweep, of the deferred queue, and of
structural recovery also take turns for each collection in one instance. Then
the seed lost no parent gate.

In the warm-up, about 8,400 parent gates were still lost in 300 s, and about
1,200 changes ended in recovery. 67% of the lost gates were in structural
recovery. Each instance lists the structural intents of the whole database, so
the recovery of one instance can reconcile a parent that the sweep of another
instance reconciles. This cause was not measured.

### Seed and warm-up

| Commit | Seed settled after | Warm-up splits in 300 s | Warm-up quiet |
| --- | ---: | ---: | --- |
| `df17a7576` | 44 to 47 s | 2,911 to 5,508 | after 119 to 280 s |
| `85d7d03d8` without the turns | 25 s | 7,524 to 7,732 | no |
| `85d7d03d8` | 28 to 29 s | 7,565 to 7,578 | no |

`85d7d03d8` lands more splits in the same time, so its tree is larger when
the warm-up ends: about 8,100 leaves against about 3,500 to 6,000. Over the
warm-up, the rule asked for fewer splits in each window: in one run from about
6,600 to about 90 in 30 s.

### The split time grows with the write throttle

The typical split time is the time that a split holds the gate of its source.
It grew from about 180 ms in the seed to about 1,400 ms after 300 s. Of that
time, only the create of the new node and the CAS of the source grew, each
from about 60 ms to about 650 ms. The CAS of the structural intent stayed at
about 63 ms. The writes to all node paths grew the same way, from 59 ms to
665 ms. Almost all of the growth was wait in the write throttle of the memory
backend. The backend write rate stayed at about 17,500 to 20,000 writes per
second of wall time, and the garbage collection backlog stayed at 28,000 to 30,000
candidates.

With `--enable-throttling=false`, the write latency stayed at 73 to 83 ms, and
the typical split time at 90 to 175 ms. The warm-up still did 8,198 splits in
300 s and did not become quiet.

A higher split time raises the split threshold of the rule (0.05 of the
typical split time). So the throttle slows the convergence, but it does not
cause the splits.

### Write load of smaller leaves

A temporary sampler summed `Database::stats()` of the 5 instances every 10 s
of wall time in the warm-up of `85d7d03d8`. Leaves are 567 plus the splits of
the warm-up. A transaction is any committed transaction, also a read-only one.
The rates are for each second of wall time, and each row is the mean of one
30 s bucket, with the leaves at its end.

| Throttle | Leaves | Tx/s | Writes/tx | Rounds/tx | Members/round |
| --- | ---: | ---: | ---: | ---: | ---: |
| on | 2,768 | 27,801 | 0.725 | 0.635 | 2.75 |
| on | 5,941 | 21,108 | 0.905 | 0.842 | 1.48 |
| on | 8,270 | 17,852 | 1.048 | 0.986 | 1.24 |
| off | 3,537 | 35,218 | 0.832 | 0.739 | 2.45 |
| off | 6,273 | 36,364 | 1.637 | 1.536 | 1.44 |
| off | 8,848 | 36,720 | 1.883 | 1.780 | 1.28 |

As the leaves get smaller, fewer transactions share one round of the leaf
coordinator, so each transaction needs more backend writes. With the
throttle, the backend did about 18,700 to 20,100 writes per second for the
whole warm-up. From the first to the last row, writes for each transaction
rose by 45%, and transactions per second fell by 36%. Without the throttle, the writes per second rose from
about 29,000 to about 69,000, and transactions per second did not fall.

So under a write budget, the splits that the rule asks for to remove queue
wait in the leaf coordinator add write load to every transaction. The rule
does not see the time that this load adds in the throttle. Without the
throttle, rounds for each transaction rose above 1. Sharing of rounds alone
cannot explain that, and its cause was not measured.

The rule (ADR-074) could count the write load of a split against it, or the
`mixed` scenario could stop waiting for a quiet tree under this load. Neither
was tried.

## 2026-10-12: leaves split for minutes under the mixed lo load

Status: the cause is the measured queue wait, which the avoidable time policy
(ADR-074) removes as designed. The tree converges, but each database instance
does one split at a time, so the convergence takes more than 10 minutes. A
conditioning load before the cells did not help, because of the garbage
collection work that it leaves. The `mixed` scenario now forks one seed for
each cell, and records the splits and merges of each warm-up. Faster
convergence and garbage collection under load are engine questions.

### Symptom

A warm-up under load that waited for splits to stop for 3 seconds did not end.
At 100 workers per shape (5 instances, 100% affinity, 20,000 keys, stripes,
1% shared), the instances did about 20 splits per second for 240 seconds and
no merges. The 5 collections went from about 590 leaves after the seed to about
5,400. At 200 workers, the warm-up did 1,924 splits in 120 seconds.

### Measurement

Temporary counters in the leaf coordinator, the rule, and the splitter counted
each split cause, each split reason, and the leaf entries after each split.
Single runs at 100 workers per shape:

- All splits under load were `Demand` splits at a measured split key. Soft cap,
  capacity, and inline pressure splits did not occur.
- The split cause was queue wait for other keys: 40 to 80 seconds of wait in
  each second, over all instances. Lost CAS time was zero, slow leaf CAS time
  was about 0.2 seconds in each second, and the divided conflict time that
  counts against a split was near zero.
- A leaf that the rule split had about 180 ms of delay in its window. The split
  threshold is 0.05 of the typical split time of about 190 ms, so about 9.5 ms.
- The rule asked for 1,200 to 2,800 splits in each 10 seconds, but about 210
  landed. Each instance does one split at a time, so the split rate is the
  split capacity of about 5 splits per second for each instance.

| Time under load | Splits/s | Entries of a new leaf | Queue wait (s/s) |
| --- | ---: | ---: | ---: |
| 10 s | 23 | about 400 | 77 |
| 2 to 3 min | 20 | about 9 | 30 to 40 |
| 6 to 9 min | 3 to 6 | 8 to 10 | 3 to 4 |

At 50 workers per shape, the rate went from 25 splits per second to 4 to 5
after 6 minutes, with about 3 seconds of queue wait in each second.

### Cause

The queue wait of a leaf grows with the square of its round submissions. A
split halves the submissions of each half. A 10-key transaction in a 20,000-key
pool touches about 10 leaves before and after a split, so a split adds almost
no divided conflict time. So the rule splits until each leaf has about one
round at a time, which under this load is a leaf of 5 to 20 keys. This is the
designed behavior of ADR-074. The splits are worth their cost: after a 120
second warm-up at 100 workers, `rwMany` had 449 tx/s and a p50 of 208 ms. The
sweep before this entry, which measured the tree of the seed, had about
240 tx/s and about 290 ms.

So each cell of the earlier sweeps measured a tree that was still splitting,
at a different point of its convergence.

### Variants

- A lower leaf soft cap during the seed and warm-up did not help. The seed then
  splits at about 11 to 12 splits per second. With a cap of 16 it did 6,743
  splits in 532 seconds and did not finish. The cap moves the convergence time
  from the warm-up into the seed, and gives a tree that depends on the cap.
- A warm-up per worker count costs 6 to 10 minutes per cell, so more than six
  hours for the canonical worker sweep.

### Conditioning the tree before the cells

A variant ran the cell with the most workers unmeasured on the seed, and gave
each cell a fork of the result. Single runs at 5 instances and 20,000 keys:

| Variant | `rwMany` at 200 workers | Busy workers per shape | Writes/tx | Warm-up |
| --- | ---: | ---: | ---: | --- |
| Fork of the seed, no conditioning | 598 tx/s | 197 | 0.94 | not quiet in 120 s |
| 600 s conditioning, garbage collection not waited for | 308 tx/s | 126 | 2.01 | 701 splits, not quiet |
| 120 s conditioning, garbage collection waited for | 632 tx/s | 196 | 1.59 | 2,126 splits, 0 merges, not quiet |

Busy workers come from Little's law. Without the wait, every fork carried the
garbage collection backlog of the load, and each cell did that work while it
measured. With the wait, the result is the same as without conditioning, and
the tree still splits at about the split capacity. At 50 and 100 workers, the
warm-ups on a tree conditioned at 200 workers did 1,680 and 1,918 splits in
120 seconds, and no merges.

### Garbage collection after a load

Temporary instrumentation counted the transaction records in the memory
backend every 10 seconds after a 120 second conditioning load at 200 workers per
shape. Four extra database instances with a GC parallelism of 64 helped the
5 clients.

| Time after the load | Records | Records deleted in each second |
| --- | ---: | ---: |
| 16 s | 327,834 | |
| 16 to 450 s | 328k to 244k | about 190 |
| 450 to 520 s | 244k to 172k | about 1,000 |
| 520 to 600 s | 172k to 31k | about 1,800 |
| 638 s | quiet | |

- The load created about 2,800 records in each second. The GC hint queues
  dropped 1.4 million candidates during the load, so GC found most records by
  LIST scans.
- GC made 2.8 progress events for each deleted record.
- GC LIST requests went from about 0.7 to about 4 in each second when deletions
  became fast. This agrees with the adaptive scan cadence, but the cause is not
  confirmed.
- After a 600 second load, GC did not become quiet within 30 minutes.

The `mixed` scenario therefore waits for garbage collection only after the
seed. There it waits until no candidate is left, because a hinted candidate is
first checked after the lease and the clock skew allowance, which is longer
than a short quiet period.

## 2026-10-12: multi-key writes stop scaling in the mixed lo mode

Status: the cause is key overlap in the benchmark, not a coordinator or backend
limit. The `lo` mode now gives each worker its own stripe of keys, except for
`--shared-pct` of transactions.

### Symptom

In the worker sweep (5 instances, 100% affinity, 5,000 keys, 10 keys per
multi-key transaction), `rwMany` went from 82 tx/s at 30 workers per shape to
about 160 tx/s from 120 workers, and stayed there. Body replays per transaction
went from 0.023 to 0.080. The process used about 1.1 of 22 cores. The backend
did about 3,500 operations per second over separate prefixes. Coordinator CAS
retries were about 0.0003 per transaction, and members per round increased with
load. Each instance did 15 to 19 splits and no merges during measurement.

### Cause

All workers drew keys from the whole pool. At 150 workers per shape, about 150
to 200 keys of each collection were locked at a time, about 3 to 4% of the pool.
A 10-key transaction then touches a locked key with a probability of about
1 - 0.96^10 = 33%. A younger writer waits and keeps its other locks (ADR-024),
so it blocks more transactions. A multi-key read of a write-locked key fails
optimistic validation and takes read locks, which also block writers.

### Variants

Single runs at 150 workers per shape, the S3 model, `delay-scale=0.2`, and
`prefix-depth=3`:

| Variant | `rwMany` | `roMulti` | `roMulti` p90 | Replays/tx |
| --- | ---: | ---: | ---: | ---: |
| Whole pool (before) | 167 tx/s | 718 tx/s | 371 ms | 0.072 |
| Stripes for writers only | 206 tx/s | 696 tx/s | 418 ms | 0.046 |
| Stripes for writers only, no shared draws | 207 tx/s | 705 tx/s | 412 ms | 0.044 |
| Stripes in each database instance, no shared draws, 5,000 keys | 321 tx/s | 1,198 tx/s | 142 ms | 0.002 |
| Stripes for all workers, 1% shared, 20,000 keys | 358 tx/s | 1,142 tx/s | 170 ms | 0.001 |

The row with stripes in each database instance used temporary instrumentation.
Each instance divided the pool among its own 120 workers, so stripes had about
41 keys, and workers of different instances used the same stripes. With 100%
affinity, each instance used only its home collection, so no two workers drew
from the same stripe of one collection.

Stripes for writers only remove writer conflicts, but readers of written keys
still block writers. With stripes for all workers, `rwMany` scales from
123 tx/s at 30 workers to 410 tx/s at 200 workers (20,000 keys). Its throughput
per worker still decreases. The next entry shows that this decrease is queue
wait in leaves that are still splitting, and not a cost of shared rounds.

At 200 workers per shape, a cell has 800 workers. With 5,000 keys, a stripe
then has 6 keys, fewer than one multi-key transaction. The benchmark rejects
such a configuration, and the canonical worker grid uses 20,000 keys.

A pool of 50,000 keys without stripes gave 227 tx/s at 150 workers. That pool
needs about 1,370 splits and 2 minutes of setup for each cell, and the topology
changes with the pool size. Stripes keep the conflict rate independent of the
pool size.

## 2026-10-05: database instances that split and merge the same leaf

Status: reproduced by the perfbench `split-merge-fight` scenario. Paid rates
(ADR-076) slow the fight from about 25 to about 3 splits and merges in 60 s.
The merge side measures the time that a merge removes, but most of the cost
of two leaves was in other parts of the scan. Two of them are fixed: the
validation fallback reads no index node, and scans read no transaction
record of an overwrite (ADR-077).

ADR-074 records that each database instance decides on its own transactions,
so one instance can split a leaf while another merges it. In the `mixed` hi
mode with 8 instances, only some runs showed this. The `split-merge-fight`
scenario shows it in each run.

### Load

One collection has the keys `left` and `right` in one leaf. One splitter
instance writes `left` and one writes `right`, each with 4 workers. A merger
instance scans both keys with 8 workers. The splitters lose leaf CASes to each
other, and their policy splits the leaf between the keys. Each leaf then has
one key, so it cannot split again. After the split, each scan crosses into the
right leaf, and the policy of the merger merges the leaves. All instances use
the avoidable time policy. `fixed-1` and `fixed-2` use the size causes with no
underfull threshold, and keep one leaf or one leaf for each key.

```console
perfbench --delays=s3 --runs=3 split-merge-fight --policies=<policy>
```

The defaults are a warmup of 10 s and a measurement of 60 s, in wall time.
Model time runs 5 times faster. The 4 processes ran at the same time, two of
them with `avoidable`.

### Results

Splits and merges are of all instances in the measurement. The throughputs
are in transactions per model second, for each run:

| Policy | Splits / merges | Left splitter | Right splitter | Merger |
| --- | --- | ---: | ---: | ---: |
| `avoidable` | 3/3, 6/7, 11/11, 3/4, 5/5, 4/5 | `5.7`–`7.1` | `2.9`–`7.1` | `84`–`112` |
| `fixed-1` | 0/0, 0/0, 0/0 | `3.3`–`5.4` | `2.5`–`4.7` | `186`–`189` |
| `fixed-2` | 0/0, 0/0, 0/0 | `8.0` | `7.9`–`8.0` | `67`–`68` |

All splits came from the splitters, and all merges from the merger. The load
does not change, but a split and a merge occur about every 5 to 20 s of wall
time. A merge comes 5 to 15 s after a split, and a split comes 1 to 2 s after
a merge. Thus the merge side sets the rate. With 1 merger worker, the scans crossed too
seldom: the leaf split once in the warmup and stayed split. With 30 s
measurements, a run had 1 to 6 splits, so the default is 60 s.

The splitter workers of one instance write the same key, so most of their
commits are locked commits (about 0.9 lock calls for each commit), with a
median latency of about 510 ms in all policies.

### Loads that did not show the fight in each run

A first version of the scenario had more keys in each half, and merger loads
other than the scan. The runs used 1 to 8 splitter workers, 1 to 32 merger
workers, 1 to 32 keys in each half, and the S3 and GCS delays. No load had 3
or more splits and merges in each run of a 30 s measurement:

- With more than one key and more than one writer in each half, the
  splitters also split their own half. A lost CAS or a queue wait counts when
  the keys of the two rounds are on different sides of the median. This is
  also true for two keys of one half. Up to 27 splits made leaves that were
  too small for the merge side. Scans then crossed many pairs. The cross-leaf
  transactions were over more than two leaves, and the crossing conflict
  time does not count them.
- With one writer in each half, the splitter that won the CASes had no time,
  and the other one often had almost no commits. A cycle took 10 s or more.
- When one instance wrote both keys, it made no split. Its writes of both
  keys were in one coordinator round, so no member waited for a round of
  the other key.
- A merger that reads one key of each half, that writes one key of each half,
  or that reads and writes all keys, conflicts with the splitters. It
  committed 0 to 2 transactions in each second under the avoidable time
  policy, and it also got split-side time from its own leaf CASes.
- On GCS, the splitters had 0.2 to 2 transactions in each second, and the
  merger merged less often than on S3.

### Other findings

- The size causes ignored the underfull thresholds of the
  `NodeSizePolicy` of a database instance. `DatabaseMetadata::node_size_policy`
  kept only the local soft caps, and used the default underfull thresholds.
  So a `fixed-2` tree with leaf entry minimum 0 merged in the warmup. The
  change that adds this scenario fixes it.
- One load can stop two of its instances. In this load, two instances write
  8 keys of each half, with one worker each. A third instance writes one key
  of each half in one transaction, with two workers. One splitter and the
  merger then have no commit and no lock call for the full measurement, more
  than 130 model seconds. The other splitter commits. This occurred in 1 to 2 of
  3 runs, with the avoidable time policy and with a `fixed-2` tree that the
  bug above merged. With a fixed tree of one or two leaves, only the
  locked commits of the merger starved (0 to 0.9 transactions in each
  second), and no instance stopped. The cause is not known.

### Avoidable time of the two sides

A fix can let the instances compare the time that a split removes with the
time that a merge removes. Temporary instrumentation printed the windows of
the rule, its requests, and the merge checks. Four runs had 19 periods with one
leaf and 22 periods with two leaves. The rates are in seconds of avoidable time
for each model second:

| State | Measured side | Window before the request | Whole state |
| --- | --- | ---: | ---: |
| One leaf | Split side, both splitters | `1.9` | `1.3` |
| Two leaves | Merge side, merger | `0.26` | `0.14` |

The real costs are almost equal. With one leaf, the writers lose about 4
worker seconds in each second (8 against 16 transactions in each second). With
two leaves, the merger loses about 5 (68 against 187 scans in each second, 8
workers). So the measured merge side was about 35 times too small. Also:

- The rule decides a split on a moving average that starts at zero at each
  change. At the request, it was about 10 times below the raw rate.
- The rule decides a merge on one window. At the request, this window was
  about 2.3 times the steady rate.
- While a change waited to land, the split-side rate fell to about 0.2,
  because the writers waited for the change.
- In 9% of the merge checks, a live lock holder deferred the merge. Most of
  the time from a request to the landing was in the change attempts.

### Scan crossings in a validation that reads the leaves again

In 96% of the scans over two leaves, a write changed the first leaf before the
validation. The validation then read the range again in `scan_coverage`, and
this read of the second leaf took about 27 ms. Only the check of the second
leaf in the other 20% of the scans counted as a scan crossing, about 2.7 ms for
each scan. `scan_coverage` now reports the time of each next leaf that it reads.
This counts about 28 ms of the 75 ms that the second leaf adds to each scan.

Most of the other time is in the body of the scan, which resolves the lock
records of the writers. With two leaves, the writers commit about twice as
often, so this time is larger. A merge removes it, but it is not the time of a
crossing.

### Typical times of splits and merges

The typical time of a change scales the thresholds of the rule. It was the
time of the attempt that landed, and most of this time was the wait for the
structural gate: 73% of the merge time and 90% of the split time. The gate
round lost leaf CASes to the writers, with a backoff of up to 5 s, and each
deferred gate ended at a live lock holder. In one run, the first merge took
13.3 s, and its work took about 0.8 s. The merge threshold became 1.33 s for
each second, no merge landed after that, and the typical time did not change
again. The tree stayed split for more than 300 model seconds.

The typical time is now the time that a leaf change holds its gate, until its
node writes land. Only in this time do the transactions of the leaf wait for
the change. The first sample still replaces the default, and the typical time
changes only when a change lands. So one slow change can still raise a
threshold for a long time, but a wait for writers no longer causes this.

`mixed` on S3 with 1 and 8 databases, affinities 0 and 100, 8 workers for each
shape, and 2 runs, against the binary before both fixes. The values are the
geometric means of the shape throughputs:

| Mode | Geometric mean | Cells |
| --- | ---: | --- |
| lo | `0.99` | `0.96` to `1.07` |
| hi | `1.03` | `0.85` to `1.18` |

The lowest hi value is the cell with 8 databases and affinity 0, which is
noisy (see the 2026-09-26 entry). `split-merge-fight` still had 5 to 11
splits and merges in each run.

### Avoidable time after the two fixes

The same instrumentation, 4 runs of about 350 model seconds each. Measured and
real rates are in seconds for each model second. The real rates come from the
throughputs in the two states:

| Run | Split side, one leaf | Real | Merge side, two leaves | Real |
| --- | ---: | ---: | ---: | ---: |
| 1 | `0.86` | `4.17` | `1.09` | `3.90` |
| 2 | `0.54` | `3.97` | `1.14` | `4.02` |
| 3 | `1.85` | `4.07` | `1.07` | `3.86` |
| 4 | `2.26` | `4.18` | `1.09` | `3.74` |

The merge side is now `0.28` of the real cost in each run. The split side is
`0.14` to `0.54` of it. In the one-leaf state, 73% to 94% of the windows of a
splitter had no split-side time. In one run, the right splitter had no commit
for 75 s, and so it measured no time. The typical times stayed at 0.29 s to
0.50 s for splits and 0.5 s to 1.05 s for merges. A queued split request can
also land after a merge: one request landed 16 s after the rule made it, and
the leaf stayed merged for only about 2 s.

### One writer instance takes the leaf from the other

With one fixed leaf, the two writer instances took turns: for 10 to 70 model
seconds, one instance committed about 8 transactions in each second, and the
other none. In 4 of 4 runs of 40 s, each instance had 7 to 24 wall seconds
with no commit.

The instance that has the leaf commits with two leaf CASes for each
transaction, so it changes the leaf about every 55 ms. Its rounds follow one
another, and each new round starts its backoff again at 200 ms. A round of the
other instance needs 70 to 150 ms from its read to its CAS, so it loses
almost each CAS. Its backoff grows by 1.5 times after each loss, up to 5 s,
and all workers of the instance wait in that round. In one round of 68 s, 23
of 24 CASes lost, and 96.6% of the time was backoff. With the backoff of
these rounds capped at 200 ms, no instance had a second with no commit, and
the total commits did not change. A cap of 1 s did not remove the gaps.

A sweep of the first delay and the cap of these rounds, in one binary with
temporary overrides, and the default twice for the noise. Zero seconds are
the wall seconds with no commit, summed over the two writers and 3 runs of
40 s:

| First delay / cap | `fixed-1` zero seconds | `avoidable` zero seconds |
| --- | ---: | ---: |
| 200 ms / 5 s (default) | `72`, `75` | `44`, `49` |
| 20 ms / 300 ms | `0` | `0` |
| 50 ms / 300 ms | `3` | `4` |
| 100 ms / 300 ms | `3` | `4` |
| 200 ms / 300 ms | `7` | `5` |
| 20 ms / 500 ms | `2` | `1` |
| 50 to 200 ms / 500 ms | `12` to `17` | `3` to `5` |

In `mixed` on S3 (1 and 8 databases, affinities 0 and 100, lo and hi, 2
runs), the default against itself had `0.96` overall and `0.84` in the cell
with 8 databases, affinity 0, and hi mode. A first delay of 20 to 100 ms with
a cap of 300 ms had `1.04` to `1.07` overall, and no cell below `0.96`. In the
cell with 8 databases, affinity 0, and lo mode, these variants had `1.12` to
`1.26`, and the p90 of `rwSingle` fell from 458 ms to 260 to 330 ms. The CAS
retries for each transaction in the cells with 8 databases and affinity 0
were about two times more (`0.43` to `0.72` to `0.86` in hi mode), and the
backend operations for each transaction did not change more than the noise.
In `contention`, a cap of 300 ms had `0.99` to `1.00`. The leaf CAS rounds now
use a first delay of 20 ms and a cap of 300 ms after a lost CAS, a precondition
miss, or a validation retry. After an in-doubt CAS, which can come from a
backend that throttles or fails, they keep the retry schedule of the database
instance. So the 50 attempts of a round that keeps losing now take about 10 s
of delays instead of about 3.5 minutes, and the round reports a conflict
sooner. The sweep ran only with the S3 delays. On GCS, a writer that keeps
losing sends about 4 CASes in each second to an object that accepts one write
in each second. Under the avoidable time
policy, the writers then measure their lost CAS time, and the leaf split and
merged 15 to 20 times in each run, against 5 to 8.

The short schedule is now `RetryTiming::contention`, which a database instance
can configure. The CAS loops on collection records and the contended releases
of locks also use it. On S3, `mixed` and `contention` had `1.01` and `1.00` of
the version before this change, in 2 runs. These benchmarks seldom contend on
collection records, and no benchmark measured these loops under contention. On
GCS, a CAS on a collection record that keeps losing now retries about 3 times
in each second.
Waits for backends and for other transactions use `RetryTiming::wait`, with
the earlier 200 ms to 5 s.

The starved instance also measured little split-side time. A round's keys
included each key that its CAS changed. The CAS also removes the finished
holders and the vestigial entries of the other key, so a peer write of that
key was a loss on the same keys, and its time was not split time. In the
round of 68 s, 13 of 23 losses were of this kind. The keys of a round are now
the keys that its members read and publish.

### Avoidable time after the retry fix

The same instrumentation on `c458496e8`, 4 runs of 650 model seconds, and runs
of each fixed tree. A long run where the rules measured but made no change
gave the steady rates. Rates are in seconds for each model second:

| Side | Measured | Real, from the fixed trees | Measured / real |
| --- | ---: | ---: | ---: |
| Split side, one leaf, both splitters | `3.0` to `3.6` | `3.7` | `0.82` to `0.95` |
| Split side, one leaf, one splitter | `1.3` to `1.8` | | |
| Merge side, two leaves | `1.0` to `1.1` | `5.0` | `0.20` to `0.22` |

Both sides were stable across runs. The two topologies cost almost the same,
but one leaf lost 1.25 worker seconds less in each second. The leaf split and
merged about 12 times in 60 model seconds.

An offline replay of the windows tested option A: a change records in its
leaves the rate that paid for it, and the opposite change needs more than a
margin `m` times that rate, which halves every `H` seconds. With the mean rate
of the state before the change landed, the sum over the instances, `H` of 60 s,
and `m` of 1.5, the replay made about 0.9 changes in 60 model seconds. But it
kept two leaves for 0.94 of the time, because the merge side measured about 4
times too little. With the merge side multiplied by 3 or more, it kept one
leaf. With the rate of one instance instead of the sum, it also kept two
leaves.

### Paid rates

ADR-076 implements option A with the mean rate since the last change, the
rate of one instance, a margin of 1.5, and a half-life of 60 s. In
`split-merge-fight` on S3, 3 runs of 60 s against `43a7187f1`:

| Version | Splits / merges | Writers (tx/s) | Merger (scans/s) |
| --- | --- | ---: | ---: |
| `43a7187f1` | 29/29, 24/24, 22/22 | `12.0` to `12.9` | `80` to `88` |
| Paid rates | 2/2, 4/3, 3/2 | `13.0` to `15.1` | `71` to `83` |

The leaves stayed split for most of the time, as the offline replay
predicted. `mixed` on S3 (1 and 8 databases, affinities 0 and 100, 2 runs) had
`1.00` in lo mode and `0.98` in hi mode, against `43a7187f1`.

### The time of a merger scan over two leaves

Temporary instrumentation, with global atomic sums of the time in each part
of the transactions that scan, divided the time of each merger scan with
`fixed-1` and `fixed-2`. There were
no replays and no retries. Times are in model ms for each committed scan:

| Part | `fixed-1` | `fixed-2` | Difference |
| --- | ---: | ---: | ---: |
| Total | `45.0` | `118.7` | `+73.7` |
| Body: lock-holder resolves | `9.4` | `41.1` | `+31.7` |
| Validation: check of the first leaf | `35.5` | `48.8` | `+13.3` |
| Validation fallback: route to the first leaf | `0.0` | `14.0` | `+14.0` |
| Validation: reads of the second leaf (scan crossing time) | `0` | `14.6` | `+14.6` |

- The scan crossing time is all of the time that comes from the read of the
  second leaf.
- A resolve that reads the transaction record of a holder takes about 27 ms.
  With `fixed-1`, 18% of the resolves did this, and with `fixed-2`, 80%. The
  body of the scan uses `Requirement::ANY`. The next section has the cause.
  It is not a crossing, so it is not merge-side time.
- The physical check of the first leaf failed in 38% of the scans with
  `fixed-1`, and in 79% with `fixed-2`. The logical fallback then validated
  the scan.
- The fallback routed to its first leaf at the validation barrier, so it read
  the root index node from the backend in each validation. A merge does not
  remove this read, because the root stays an index node (ADR-073).

The fallback now routes through the cached index nodes and checks only the
leaves, as point routes do (ADR-031). In `split-merge-fight` on S3, 2 runs
against `83dcc16af`, in scans for each model second:

| Policy | `83dcc16af` | Cached index nodes |
| --- | ---: | ---: |
| `fixed-1` | `165`–`178` | `164`–`176` |
| `fixed-2` | `68` (p50 118 ms) | `91` (p50 87 ms) |
| `avoidable` | `75`–`76` | `100`–`103` |

The writers did not change. `mixed` on S3 (1 and 8 databases, affinities 0
and 100, 2 runs) had `1.01` in lo mode and `1.01` in hi mode. Its lowest cell
was 8 databases, affinity 0, and hi mode, with `0.95`.

### Reads of the transaction records of new lock holders

Temporary instrumentation sorted each lock-holder resolve of the scan bodies
by its path, in runs of 30 s:

| | `fixed-1` | `fixed-2` |
| --- | ---: | ---: |
| Writer commits | `2538` | `4677` |
| Backend reads of transaction records, for each writer commit | `0.96` | `0.85` |
| Slow resolves (2 ms or more), for each writer commit | `7.3` | `6.6` |
| Slow resolves that joined a read of another worker | `87%` | `86%` |

Each commit of a writer makes a new holder. No worker of the merger instance
has seen it, so the first resolve must read its record, and the cache cannot
help. All 8 workers come to the new lock at about the same time, and they wait
for one shared read of about 27 ms. The holder was never pending at this read:
its record was missing, because its lock was in the leaf before the record, or
it was committed. There were no reads again at a stricter requirement, no
wounds, and no evictions.

An experiment did not read a record that was not in the cache. The merger then
had 277 instead of 177 scans each second with `fixed-1`, and 108 instead of 68
with `fixed-2`. The time of the bodies fell to about 0.1 ms. The validation
time and the writers did not change, because validation already resolves the
holders again after its barrier. The experiment depends on the cache of an
instance, and it is not correct: a create or a direct commit in the same leaf
could end the membership lock of a committed delete before its write-back.
The deleted key then stayed in the leaf with its earlier value, as after an
overwrite (test `a_scan_omits_a_committed_delete_whose_write_back_is_held`).

ADR-077 removes the read for the holders of key locks that hold no membership
lock, and makes each membership writer leave with its key locks. On S3, 2 runs
against `5c2660468`, in scans for each model second:

| Policy | Merger before | Merger after | Writers before | Writers after |
| --- | ---: | ---: | ---: | ---: |
| `fixed-1` | `177`–`179` | `281`–`283` | `8.4`–`8.5` | `8.4`–`8.6` |
| `fixed-2` | `91` (p50 87 ms) | `139`–`140` (p50 55 ms) | `15.9`–`16.1` | `15.7`–`16.1` |
| `avoidable` | `102`–`105` | `177`–`199` | `14.8`–`15.1` | `12.3`–`13.6` |

With `avoidable`, the writers committed less because the tree kept one leaf
longer, not because of a cost on the writers. A prototype study rebuilt the
topology from the timeline of each run: the tree had two leaves for 82% to 95%
of the time before, and for 23% to 67% after. The writers committed about 8.4
transactions each second with one leaf and 15.9 with two, in both builds. The
faster scans measured a merge-side rate of about 2.0 instead of 1.06 seconds
each second, so the paid rates of ADR-076 held one leaf longer. A base build
with 16 merger workers had the same shift, and a build with ADR-077 and 4
merger workers had none. So the cause is the speed of the scans. Which
topology is cheaper for this load is not measured again.

`mixed` on S3 (1 and 8 databases, affinities 0 and 100, 2 runs) had `1.00` in
lo mode and `1.02` in hi mode. It has no range scans, so the changed paths do
not run in it. Its cell with 8 databases, affinity 0, and hi mode has two
modes in both builds. In about 1 of 4 runs, `rwMany` almost stops and
`roSingle` gets faster. In 20 runs of each build, no shape was significantly
different (Mann-Whitney, |z| below 1.2).

The perfbench `membership-churn` scenario measures the cost on creates and
deletes, 12 runs before and 8 after:

| Role | Before (tx/s) | After (tx/s) | t |
| --- | ---: | ---: | ---: |
| Direct churner | `7.4` | `8.0` | `+0.6` |
| Locked churner | `0.40` | `0.37` | `-1.1` |
| Scanner | `14.3` | `14.8` | `+0.3` |

No difference is above the noise. In a prototype, the release of the key
locks of a membership writer by another transaction occurred in about 2% of
the direct commits, in a leaf CAS that it already made. With 4 locked churn
workers, 1 of 12 runs after ADR-077 failed, because a transaction did not end
in the drain timeout of 30 s of wall time. The 12 runs before had no failure,
and one failure cannot show a difference. The cause is not known. The default
is now 2 locked workers.

## 2026-09-26: ADR-074 splits in the mixed hi mode

Status: in the engine as `TopologyPolicy::AvoidableTime`, the default policy
since ADR-075. The runs below used the branch `topology-policy-experiments`. There,
the rule is the public `AvoidableTimePolicy`, and perfbench has the `topology`
scenario and the `--policies` option. The commits below are on that branch, and
its `hack/perf/topology-policy/README.md` tells how to reproduce the runs.
There, `AvoidableTimePolicy` uses the rule of
`net-conflicts:0.05:0.1:4:10:1`, and the perfbench policy
`avoidable` measures it. Perfbench has no `net` forms any more. The commands below that use them
need the perfbench of commit `597b59eca`, where `avoidable` is the rule of one
window. The windows now have only the divided and crossing conflict times of
the commit passes. The commits, latencies, rounds, and the other pass times
are in the windows up to commit `89552f702`. The loss comes from transactions that conflict on the
same keys and that a split puts in more leaves. The first estimates of the
time that a split adds did not show this cost. After a fix of the lost CAS
time, `net-conflicts:0.05:0.1:4:10:1` subtracts 4 times the divided conflict
time from the split side, and adds the crossing conflict time of two adjacent
leaves to the merge side. Against the size causes, the engine rule had `0.98`
and `1.02` in hi mode on S3 and `0.96` and `1.00` on GCS, where the rule of one
window had `0.62` and `0.74`. In lo mode it had `1.12` to `1.13` on S3 and
`1.24` to `1.25` on GCS, against `1.14` to `1.15` and `1.29`. On GCS the lo difference was less than the noise with a warmup of
60 s. In the `topology` scenario, it had about 3% less than `avoidable` on S3,
and the same on GCS. It needs both terms.

The perfbench `NetTimePolicy` subtracts an estimate of the time that a split
adds from the split-side time of a leaf, and decides on a moving average
with a half-life of 10 s (`net:<split>:<merge>:<weight>:<half-life>`). The
estimate is a weight times one of these:

- `net`: the divided commits, times the mean latency of the commits of the
  leaf.
- `net-passes`: the time of the divided commit passes (`LeafWindow::divided_time`),
  also of the passes that did not commit.

### Live comparison

Mixed scenario on S3, 1 to 8 databases, affinities 0, 50, and 100, one run.
The values are the geometric means of the shape throughputs against `size`,
over 12 cells for each mode.

```console
perfbench mixed --delays=s3 --databases=$D --workers-per-shape=8 \
  --affinities=0,50,100 --modes=lo,hi \
  --policies=size,net:0.05:0.1:0.25:10,net-passes:0.05:0.1:0.25:10,net-passes:0.05:0.1:0.5:10 \
  --warmup=20s --duration=5s --max-duration=30s --split-quiet=5s \
  --split-settle-timeout=300s
```

| Policy | lo | hi |
| --- | ---: | ---: |
| `net:0.05:0.1:0.25:10` | `1.117` | `0.869` |
| `net-passes:0.05:0.1:0.25:10` | `1.128` | `0.922` |
| `net-passes:0.05:0.1:0.5:10` | `1.065` | `0.907` |

In hi mode with 8 databases, `size` had `15.1` and `17.1` tx/s at affinities 0
and 50, and `net-passes` with weight 0.25 had `8.3` and `11.2`. It made 36 to
47 splits and 13 to 20 merges. At affinity 0, `roSingle` went from `36` to
`79` tx/s, `rwMany` from `8.3` to `1.4`, and `roMulti` from `23` to `6.6`.
The mean latency of `rwMany` went from `0.9 s` to `5–12 s`. Body replays and
lost CAS time for each transaction decreased.

### Shapes alone and in pairs on fixed trees

Hi mode has 8 keys in each collection, and `rwMany` and `roMulti` use all of
them. Fixed trees with 1, 2, and 4 leaves for each collection (seed leaf
entries `default`, `7`, `3`, and `2`; `3` and `2` give the same tree), hi mode,
8 databases, affinity 0, one run:

```console
perfbench mixed --delays=s3 --workers-per-shape=8 --affinities=0 --modes=hi \
  --databases=8 --policies=fixed --seed-leaf-entries=default,7,3,2 \
  --warmup=5s --duration=5s --max-duration=30s --split-quiet=5s \
  --split-settle-timeout=300s --shapes=<shapes>
```

| Shapes: measure | 1 leaf | 2 leaves | 4 leaves |
| --- | ---: | ---: | ---: |
| `rwMany`: tx/s | `22.4` | `5.7` | `2.6–3.0` |
| `rwMany`: coordinator rounds per transaction | `2.0` | `7.1` | `15–17` |
| `rwMany`: object reads per transaction | `1.0` | `32` | `84–100` |
| `roMulti`: tx/s | `286` | `243` | `212` |
| `rwMany`, `roMulti`: `rwMany` tx/s | `11.7` | `3.7` | `1.7–1.9` |
| `rwMany`, `rwSingle`: `rwMany` / `rwSingle` tx/s | `19.8` / `18.5` | `4.4` / `11.6` | `1.9` / `11.6–12.6` |
| `rwMany`, `roSingle`: `rwMany` / `roSingle` tx/s | `10.9` / `38.6` | `4.3` / `51.1` | `2.9–3.2` / `64–70` |
| `roMulti`, `rwSingle`: `roMulti` tx/s | `33.5` | `29.3` | `16.5–20.4` |
| `roMulti`, `roSingle`: `roSingle` tx/s | `273` | `274` | `275` |
| All: `rwMany` / `roMulti` / `roSingle` tx/s | `7.2` / `25.7` / `38.6` | `2.5` / `12.1` / `66.8` | `0.8–1.3` / `6.6–6.9` / `88–98` |

All `rwMany` transactions write the same 8 keys. In one leaf, one leaf CAS
locks all of them. Over more leaves, a transaction locks the leaves in
parallel, keeps the locks that it got while it waits for the others, and
renews its identity for serial acquisition after 3 conflicts or 5 s. Alone,
this makes `rwMany` 4 to 8 times slower. `roMulti` alone is only 15 to 25%
slower, but with writers its reads over more leaves meet more conflicts.
`roSingle` is fast without writers, and it becomes faster as `rwMany` commits
less. Thus, a part of its gain after a split is not a gain of the split.

### Why the policy does not see the cost

- Before a split, the estimate is a weight times the divided time. The cost
  after a split was 4 to 8 times the latency of `rwMany`, but less than 0.3
  times the latency of `roMulti` alone. The cost depends on the conflicts of
  the divided transactions on the same keys, and one weight cannot fit both.
- After a split, the only merge-side cause for transactions over more leaves
  is the adjacent miss. It counts direct commit candidates whose keys are in
  exactly two adjacent leaves. Locked commits over 3 or more leaves and read
  validation over more leaves add no merge-side time.

### Conflict time of divided transactions

`LeafWindow::divided_conflict_time` is the part of `divided_time` in commit
passes that a conflict ended without a commit. `net-conflicts` uses it as the
estimate. The offline replay uses the unsplit traces of 8 cells. The label is
the geometric mean of the tree that the setup split once, against the unsplit
tree. A cell is `leaves that split / leaves`, with the split multiple at 57 ms
(`0.05` of the live split time) and a half-life of 10 s:

```console
perfbench mixed --delays=s3 --workers-per-shape=8 --affinities=0 \
  --policies=fixed --shadow-policies=avoidable,size --trace-windows \
  --warmup=5s --duration=5s --max-duration=30s --split-quiet=5s \
  --split-settle-timeout=300s --modes=<mode> --databases=<n> \
  --seed-leaf-entries=default,<7 for hi, 128 for lo> [--key-layouts=ranges] [--shapes=rwSingle]
```

| Cell | Label | `net` 0.25 | `net-passes` 0.25 | `net-passes` 0.5 | `net-conflicts` 1 | 2 | 4 | 8 |
| --- | ---: | --- | --- | --- | --- | --- | --- | --- |
| hi, ranges, `rwSingle`, 2 databases | `2.14` | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 | 4/4 |
| hi, shared, `rwSingle`, 1 database | `1.04` | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 | 1/1 |
| hi, shared, all shapes, 1 database | `0.72` | 0/1 | 0/1 | 0/1 | 0/1 | 0/1 | 0/1 | 0/1 |
| hi, shared, all shapes, 4 databases | `0.81` | 3/16 | 4/16 | 3/16 | 3/16 | 2/16 | 2/16 | 1/16 |
| hi, shared, all shapes, 8 databases | `0.79` | 17/64 | 16/64 | 11/64 | 6/64 | 5/64 | 2/64 | 2/64 |
| hi, ranges, all shapes, 2 databases | `0.89` | 1/4 | 1/4 | 1/4 | 0/4 | 0/4 | 0/4 | 0/4 |
| lo, 1 database | `1.14` | 25/26 | 23/26 | 9/26 | 26/26 | 24/26 | 22/26 | 17/26 |
| lo, 8 databases | `0.99` | 53/1952 | 53/1952 | 51/1952 | 54/1952 | 54/1952 | 54/1952 | 54/1952 |

In lo mode, the divided transactions seldom conflict, so the conflict time
keeps the splits that help.

The live comparison with the same command as above, and the policies
`size,net-passes:0.05:0.1:0.25:10,net-conflicts:0.05:0.1:4:10,net-conflicts:0.05:0.1:8:10`:

| Policy | lo | hi | hi, 8 databases, affinity 0 (tx/s) |
| --- | ---: | ---: | ---: |
| `size` | `1.000` | `1.000` | `14.9` |
| `net-passes:0.05:0.1:0.25:10` | `1.127` | `0.943` | `8.2` |
| `net-conflicts:0.05:0.1:4:10` | `1.100` | `0.969` | `7.7` |
| `net-conflicts:0.05:0.1:8:10` | `1.113` | `0.927` | `8.5` |

The offline replay did not predict the live loss. The live runs split more
than the replay, for the reasons in the next section.

### Split cascade in a live run

A traced live run of `net-passes:0.05:0.1:0.25:10` in hi mode with 8
databases (no warmup, 40 s, the live policy also as a shadow, so that the
trace has its decisions) made 35 splits and 14 merges from 65 split and 19
merge requests. The trace shows:

- One instance can lose the CAS of a hot leaf for many seconds, while the
  other instances commit. In one leaf, an instance had no commits for about
  17 model seconds, and its lost CAS time grew to `11.3 s` in each window of
  `1 s`, with 4 workers for each instance.
- The policy of that instance asked for a split in almost each window. The
  split also needs the leaf CAS, so it landed only after about 15 model
  seconds. A request at the old split key does not divide the left half, so
  the engine drops it. A request at the median divides the left half.
- The halves kept this lost CAS time and split again.

The lost CAS time had two errors. After the first lost CAS on other keys, the
round counted all its time until a CAS landed, also the time of later lost
CASes on its own keys, which no split removes. And it counted all members of
the round, also write-backs, releases, and structural gates, which no
transaction waits for. With one worker for each shape, only `rwSingle` has
rounds whose keys a split separates, so 11 members were mostly pending
write-backs.

The fix stops the time at a lost CAS on the keys of the round, and counts only
the members that a transaction waits for (`MemberPolicy::delays_transaction`).
With the fix, the same traced run of `net-conflicts:0.05:0.1:4:10` made 23
splits from 29 requests, and no leaf had more than about 4 s of lost CAS time
in 10 windows. But the policies still split leaves whose instance saw almost
no divided transaction:

- With 8 databases and 8 workers for each shape, each instance has one worker
  for each shape. The `rwMany` worker of an instance uses a given collection
  about once in 4 s, but its `rwSingle` worker loses CAS all the time. The
  estimate of the time that a split adds has few samples in each instance.
- An instance reports lost CAS time while the round retries, but the time of a
  commit pass only when the pass ends. In the first windows, and after each
  change of a leaf, the split side comes first.
- After some collections split, `rwMany` spends most of its time in them, and
  uses the other collections less. The estimate in those leaves decreases, so
  more of them split.

### Conflict time over two adjacent leaves

An estimate before a split cannot see its cost well. After a split, the cost
is in the commit passes of the transactions over the two new leaves.
`PairWindow::crossing_time` is the time of the commit passes whose point reads
are in exactly two adjacent leaves, and `crossing_conflict_time` is the part
that a conflict ended without a commit.

The existing traces show the size of this cost. On the tree that the setup
split once, the halves had these times, in ms for each collection in each
window. The divided conflict time counts a transaction over both halves in
each half:

| Cell | Split side of the halves | Divided conflict time of the halves | Merge side |
| --- | ---: | ---: | ---: |
| hi, shared, all shapes, 8 databases | `133` | `3428` | `34` |
| hi, shared, all shapes, 4 databases | `175` | `6955` | `60` |
| lo, 1 database | `8160` | `655` | `0` |
| lo, 8 databases | `1449` | `65` | `0` |

With a crossing weight `c` (`net-conflicts:<split>:<merge>:<weight>:<half-life>:<c>`),
the `NetTimePolicy` adds `c` times the crossing conflict time to the merge side
of a pair, and a moving average of the merge side less the split side of both
leaves decides a merge, with the same half-life as the split side.

A traced live run as above, with `net-conflicts:0.05:0.1:4:10:1`:

| Mode, 8 databases, affinity 0 | Splits | Merges | `rwSingle` | `rwMany` | `roSingle` | `roMulti` | Geometric mean |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hi, `net-passes:0.05:0.1:0.25:10`, before the lost CAS fix | 35 | 14 | `6.5` | `1.7` | `87.2` | `8.3` | `9.4` |
| hi, `net-conflicts:0.05:0.1:4:10` | 23 | 14 | `6.7` | `2.5` | `85.7` | `11.9` | `11.4` |
| hi, `net-conflicts:0.05:0.1:4:10:1` | 7 | 7 | `7.8` | `6.7` | `52.5` | `21.0` | `15.5` |
| lo, `net-conflicts:0.05:0.1:4:10:1` | 155 | 0 | `36.2` | `11.4` | `254.3` | `107.9` | `58.1` |

In hi mode, the policy merged back each split. In lo mode, it merged none.

A first version averaged the whole merge side, less the split side of both
leaves. ADR-074 records that a moving average of scan crossings stays below
the time of the windows that have them, so that the leaves that scans cross do
not merge. The policy now averages only the crossing conflict time, and keeps
the other merge-side time and the split side of the leaves of one window, as
`AvoidableTimePolicy` does. Without a crossing weight, the merge rule is the
one of `AvoidableTimePolicy`. The policy also drops the average of a pair when
it asks for its merge. Before this, it asked about 70 times in each second for
merges of leaves that a merge had already removed, and 109 times in all after
it. The traced hi run had a geometric mean of `16.0`.

The live comparison with the lost CAS fix, with the same command as above,
and the policies
`size,net-conflicts:0.05:0.1:4:10,net-conflicts:0.05:0.1:4:10:1,net-passes:0.05:0.1:0.25:10:1`.
In hi mode with 8 databases and affinity 100, `net-conflicts:0.05:0.1:4:10:1`
had `62.8` tx/s against `89.7` for `size`, with no split and no merge. This is
noise of the parallel runs, so the hi column is without this cell:

| Policy | lo | hi | hi, 8 databases, affinity 0 (tx/s) |
| --- | ---: | ---: | ---: |
| `size` | `1.000` | `1.000` | `15.4` |
| `net-conflicts:0.05:0.1:4:10` | `1.135` | `1.010` | `13.3` |
| `net-conflicts:0.05:0.1:4:10:1` | `1.114` | `0.993` | `14.6` |
| `net-passes:0.05:0.1:0.25:10:1` | `1.124` | `0.983` | `10.6` |

The lost CAS fix alone took `net-conflicts:0.05:0.1:4:10` from `0.969` to
`1.010` in hi mode. In the hi cell with 8 databases and affinity 0, it made 9
splits and 7 merges in the measurement, and the crossing weight made none.

Two more runs with the same command, the policies
`size,avoidable,net-conflicts:0.05:0.1:4:10,net-conflicts:0.05:0.1:4:10:1`,
and `--runs=2`. The values are for each run, with all 12 cells:

| Policy | lo | hi | hi, 8 databases, affinity 0 (tx/s) |
| --- | ---: | ---: | ---: |
| `size` | `1.000`, `1.000` | `1.000`, `1.000` | `15.5`, `15.1` |
| `avoidable` | `1.126`, `1.139` | `0.650`, `0.616` | `6.5`, `5.7` |
| `net-conflicts:0.05:0.1:4:10` | `1.135`, `1.102` | `0.989`, `0.921` | `14.1`, `9.4` |
| `net-conflicts:0.05:0.1:4:10:1` | `1.122`, `1.110` | `1.010`, `0.966` | `14.9`, `14.8` |

In hi mode, the two `net-conflicts` policies changed the tree only in the
cells with 8 databases and affinities 0 and 50. In the other 20 cells of the
two runs, the tree was the same as with `size`, and the ratios had a geometric
mean of `0.985` and `0.989` with a root mean square deviation of 5%. This is
the noise of the measurement. In the 4 cells that changed the tree, the
geometric mean was `0.818` without the crossing weight and `0.984` with it.
Without the crossing weight, one run made 16 splits and 13 merges in the
measurement of the cell with affinity 0. With it, the policy merged back all
splits in the warmup in the second run.

Is the estimate before a split still necessary when the crossing conflict
time undoes a split? The same command with `--runs=2`, and divided conflict
weights 0, 2, and 4, all with the crossing weight 1:

| Policy | lo | hi | hi cells that changed the tree |
| --- | ---: | ---: | ---: |
| `avoidable` | `1.118`, `1.169` | `0.649`, `0.643` | |
| `net-conflicts:0.05:0.1:0:10:1` | `1.125`, `1.161` | `0.826`, `0.794` | 22 of 24 |
| `net-conflicts:0.05:0.1:2:10:1` | `1.109`, `1.136` | `1.007`, `0.992` | 6 of 24 |
| `net-conflicts:0.05:0.1:4:10:1` | `1.114`, `1.121` | `1.026`, `0.995` | 4 of 24 |

Without the estimate, the leaves of hi mode split and merged again and again,
and the transactions over them starved in each split. So the policy needs
both. No lo cell of any policy had a merge, so in lo the crossing weight
changes no decision. The lo loss against `avoidable` is in the cells with 2
databases and affinities 0 and 50. There, over 4 runs, weight 4 made 46 to 90
splits in the warmup, against 102 to 122 for `avoidable`, and had `0.849` and
`0.967` of its throughput.

Mixed on GCS, the same command with `--delays=gcs`, `--runs=2`, and the
crossing weights 1 and 0.5:

| Policy | lo | hi |
| --- | ---: | ---: |
| `avoidable` | `1.301`, `1.291` | `0.733`, `0.743` |
| `net-conflicts:0.05:0.1:4:10:1` | `1.281`, `1.206` | `1.017`, `1.009` |
| `net-conflicts:0.05:0.1:4:10:0.5` | `1.260`, `1.244` | `0.997`, `1.017` |

In hi mode on GCS, `size` had only 0.9 to 1.6 tx/s in the cells with 4 and 8
databases and affinities 0 and 50, so these cells are noisy. In lo mode, the
net policies made fewer splits than `avoidable` with 4 and 8 databases, for
example 78 against 147 to 174 with 4 databases and affinity 50, and had
`0.873` of its throughput there. With 2 databases and affinity 0, they had
`0.926`.

Mixed lo on GCS with `--runs=2`, and divided conflict weights 0, 2, and 4,
all with the crossing weight 1:

| Policy | lo |
| --- | ---: |
| `avoidable` | `1.307`, `1.271` |
| `net-conflicts:0.05:0.1:0:10:1` | `1.273`, `1.209` |
| `net-conflicts:0.05:0.1:2:10:1` | `1.273`, `1.222` |
| `net-conflicts:0.05:0.1:4:10:1` | `1.269`, `1.229` |

Without the estimate, the net policy also had less than `avoidable`, so the
estimate does not cause the GCS lo loss. The loss is in the cells with 4 and 8
databases, where the net policies made fewer splits. All policies still split
in the measurement: `avoidable` made 50 to 115 splits in the 5 s with 4 and 8
databases, after 41 to 190 in the warmup. So the tree did not settle in the
warmup of 20 s, and a policy that waits for a moving average splits later.

A half-life of 5 s or a split multiple of 0.025 makes the net policy split
sooner. Hi mode on S3 with 4 and 8 databases, `--runs=2`, and the same
command as above:

| Policy | hi, 4 and 8 databases | hi cells that changed the tree |
| --- | ---: | ---: |
| `avoidable` | `0.606`, `0.630` | `0.561` (10 of 12) |
| `net-conflicts:0.05:0.1:4:10:1` | `1.002`, `0.975` | `0.957` (5 of 12) |
| `net-conflicts:0.05:0.1:4:5:1` | `0.880`, `0.985` | `0.875` (6 of 12) |
| `net-conflicts:0.025:0.1:4:10:1` | `0.939`, `0.910` | `0.857` (6 of 12) |

With 8 databases, the two faster variants made 7 to 29 splits and 2 to 14
merges in the measurement, and had `0.57` to `0.79` of `size` in 3 of the 4
cells with affinities 0 and 50. So a faster split rule brings the hi loss
back.

Lo mode on GCS with 4 and 8 databases, `--runs=2`, and a warmup of 60 s
instead of 20 s:

| Policy | Warmup 20 s | Warmup 60 s |
| --- | ---: | ---: |
| `avoidable` | `1.220`, `1.165` | `1.294`, `1.336` |
| `net-conflicts:0.05:0.1:4:10:1` | `1.147`, `1.109` | `1.348`, `1.211` |
| `net-conflicts:0.05:0.1:4:5:1` | | `1.390`, `1.223` |
| `net-conflicts:0.025:0.1:4:10:1` | | `1.327`, `1.333` |

With the longer warmup, the difference between `avoidable` and the net policy
is less than the difference between the two runs. All policies still split in
the measurement, 15 to 107 times in 5 s. So the net policy splits later in lo
mode, but the tree that it gets to is about as fast. The faster variants
reduce the delay, but they bring the hi loss back, so the policy keeps a
half-life of 10 s and a split multiple of 0.05.

A review found that the merge rule also decided for the pairs of earlier
windows. Another instance can change the leaves of such a pair, so the policy
can ask for a merge of leaves that are no longer adjacent. The policy now
decides only for the pairs of the window, and keeps crossing averages only
with a crossing weight. The same commands as above, `--runs=2`, with
`net-conflicts:0.05:0.1:4:10:1` after this change:

| Scenario | `avoidable` | `net-conflicts:0.05:0.1:4:10:1` |
| --- | ---: | ---: |
| Mixed on S3, lo | `1.146`, `1.148` | `1.131`, `1.119` |
| Mixed on S3, hi | `0.627`, `0.603` | `0.981`, `0.944` |
| `topology` on S3 | `1.173`, `1.207` | `1.141`, `1.172` |
| `topology` on GCS | `1.324`, `1.266` | `1.248`, `1.299` |

In hi mode, the 18 cells where the tree stayed the same as with `size` had
`0.974`, and the 6 cells that changed the tree had `0.929`. The cell with 8
databases and affinity 0 had `1.08` and `0.68`. In the second run it made 7
splits and 5 merges in the measurement. Before the change, this cell had
`0.93` to `1.02` in 5 runs.

An A/B of the two merge rules in hi mode on S3 with 8 databases, affinities 0
and 50, and 8 runs of each rule, run at the same time:

| Merge rule | Affinity 0 | Affinity 50 | Both |
| --- | ---: | ---: | ---: |
| Also for the pairs of earlier windows | `0.983` | `1.008` | `0.996` |
| Only for the pairs of the window | `0.933` | `1.001` | `0.966` |

With the second rule, one run at affinity 0 had `0.66`, with 12 splits and 6
merges in the warmup, and 7 splits and 8 merges in the measurement. The first
rule had no run below `0.86`. The crossing conflict time counts only the
transactions over exactly two leaves. When another instance splits a leaf of
a pair, these transactions are over three leaves, and no pair of the window
has their time. With the first rule, the average of the old pair still asks
for the merge of its left leaf, and the engine merges it with its right
sibling of now. If the right leaf split, this is the same merge. If the left
leaf split, this merge takes away one of the three leaves. If the left leaf
is gone, the engine skips the merge. So the policy decides again also for the
pairs of earlier windows, and keeps crossing averages only with a crossing
weight.

A second A/B, of the binary before the review and the binary with this last
rule, which decide the same, also 8 runs of each:

| Binary | Affinity 0 | Affinity 50 | Both |
| --- | ---: | ---: | ---: |
| Before the review | `0.916` | `1.057` | `0.984` |
| After the review | `0.979` | `0.982` | `0.980` |

This time the binary before the review had one run at `0.65`, with 13 splits
and 8 merges in the measurement. So each rule has rare runs in which the
leaves split and merge again and again in the measurement: over all runs with
8 databases and affinity 0, 1 of 21 for the first rule before the review, 2 of
10 for the second rule, and 0 of 8 after the review. The difference between
the two rules is not more than the noise, and the policy keeps the first rule
for the reason above. Each instance decides on its own transactions, so one
instance can split a leaf for its lost CAS time while another merges it for
its crossing conflict time. This is the remaining risk of the policy in hi
mode.

The `topology` scenario with the lost CAS fix, one run, against the fixed
seeded tree:

```console
perfbench --delays=<s3|gcs> --runs=1 topology \
  --workloads=single,hot,adjacent,random,scan --workers=8 --databases=1,4 \
  --leaf-sizes=<16|128> --num-keys=1024 --duration=10s --max-duration=30s \
  --split-settle-timeout=600s --split-quiet=5s \
  --policies=fixed,avoidable,net-conflicts:0.05:0.1:4:10,net-conflicts:0.05:0.1:4:10:1 \
  --adapt=20s
```

| Policy | S3 | S3 worst cell | GCS | GCS worst cell |
| --- | ---: | ---: | ---: | ---: |
| `avoidable` | `1.190` | `0.90` | `1.257` | `0.83` |
| `net-conflicts:0.05:0.1:4:10` | `1.160` | `0.82` | `1.237` | `0.81` |
| `net-conflicts:0.05:0.1:4:10:1` | `1.150` | `0.80` | `1.242` | `0.66` |

An earlier run before the fix had `1.204` and `1.263` for `avoidable`, so the
fix did not change it here. The worst cell of the crossing weight was
`adjacent` on GCS with leaves of 16 entries and 4 databases: it made 14 merges
in the measurement, against 4 without the weight, and had `5.5` tx/s against
`7.0`. In one leaf, the transactions of this workload also conflict, so only a
part of the crossing conflict time is time that a merge removes.

A repeat with `--runs=2` and the crossing weight 0.5 added. The values are for
each run:

| Policy | S3 | GCS |
| --- | ---: | ---: |
| `avoidable` | `1.185`, `1.180` | `1.242`, `1.251` |
| `net-conflicts:0.05:0.1:4:10` | `1.166`, `1.155` | `1.262`, `1.235` |
| `net-conflicts:0.05:0.1:4:10:1` | `1.165`, `1.178` | `1.247`, `1.171` |
| `net-conflicts:0.05:0.1:4:10:0.5` | `1.179`, `1.144` | `1.266`, `1.198` |

In the `adjacent` cell on GCS with leaves of 16 entries and 4 databases, the
crossing weight 1 had `1.07` and `1.09`, so the `0.66` of the first run was
noise. The lowest value of the crossing weight 1, `1.171` on GCS, came from
the `hot` cells with leaves of 128 entries (`0.73` and `0.98`, against `1.04`
and `1.75` without the weight). The `hot` transactions write one key, so no
pair has crossing time, and the policy decides as without the weight. The
crossing weight did not change the `topology` results more than the noise of
the runs. The net policies had about 2% less than `avoidable` on S3, and the
same on GCS.

### Engine rule

`AvoidableTimePolicy` with the rule of `net-conflicts:0.05:0.1:4:10:1`, as the
perfbench policy `avoidable`, against `size` in `mixed` and `fixed` in
`topology`. The same commands as above with `--runs=2` and
`--policies=size,avoidable` or `--policies=fixed,avoidable`, on S3 and GCS,
with all 12 processes at the same time. The values are for each run:

| Scenario | lo or all cells | hi | Worst cell |
| --- | ---: | ---: | ---: |
| Mixed on S3 | `1.126`, `1.117` | `0.983`, `1.018` | |
| Mixed on GCS | `1.240`, `1.245` | `0.958`, `0.995` | |
| `topology` on S3 | `1.173`, `1.162` | | `0.80`, `0.84` |
| `topology` on GCS | `1.233`, `1.278` | | `0.75`, `0.86` |

These agree with the earlier runs of `net-conflicts:0.05:0.1:4:10:1`. In hi
mode on GCS with 8 databases and affinity 50, the first run had `0.73`, with 6
splits and 9 merges in the measurement. This is the remaining risk above. On
S3, no hi cell had more than one split and one merge in the measurement.

In lo mode, the gain with 1, 2, 4, and 8 databases was `1.25`, `1.19`,
`1.05`, and `1.01` on S3, and `1.42`, `1.39`, `1.20`, and `1.00` on GCS. On
GCS, it was `1.20` with affinity 0 and `1.37` with affinity 100. On S3, it was
`1.11` and `1.14`.

The worst `topology` cell on S3 was `single` with leaves of 16 entries and 4
databases: 28 tx/s against 33 to 36 for the fixed tree, with 6 to 10 splits.
The net policies had the same in the earlier runs. The rule of one window had
30 tx/s there, with more than 100 splits.

## 2026-08-21: root-leaf structural-gate coordinator rationale

Status: implemented through the typed coordinator interface. A deterministic
regression proves one root read and one CAS. No retained benchmark
compares contention or latency between the two acquisition paths.

### Finding

A tree root `_r` starts as a leaf and becomes an index after its first
split
([ADR-050](../../docs/adr/050-separate-collection-record-and-tree-root.md)). As a
leaf, it is the CAS unit for its keys and has the same coordination semantics as
a non-root leaf.

[`ShardCoordinator::coordinate`](../../crates/glassdb-trans/src/shard_coord.rs)
accepts a typed
[`ShardOperation`](../../crates/glassdb-trans/src/shard_coord.rs).
[`StructuralGateOperation`](../../crates/glassdb-trans/src/node_locking.rs) uses
this interface for root and non-root leaf paths. Before this change,
[`StructuralNodeAccess::acquire_structural_gate`](../../crates/glassdb-trans/src/structural/nodes.rs)
selected the path from `Option<&NodeToken>`:

- a non-root leaf uses the coordinator;
- a non-root index uses the direct path; and
- the root always uses the direct path, even when it is a leaf.

Thus, the root-leaf exception was based on the address, not the node shape. Its
direct CAS could race data mutations that used the same `Database` coordinator.

The change affects only root-leaf gate acquisition. Index acquisition and
writes after the gate is held stay outside it. This includes a root rewrite and
[ADR-062](../../docs/adr/062-splitter-driven-tombstone-reclamation.md)
compaction, which can finish without a split.

### Rationale and evidence

Commit `c4216276` introduced the exception with the claim that roots and indexes
carry no data-mutation traffic. This claim did not apply to a root leaf, which
was already key-bearing. The commit has no benchmark for the exception.
ADR-050 later made the mismatch explicit by giving `_r` the same node semantics
as other B-link nodes. The structural-gate operation already supported root
paths; commit `c13aab70` only gave its coordinator call a typed result.

The retained performance evidence does not justify the bypass:

- [ADR-044](../../docs/adr/044-cas-fenced-structural-gate.md) reduced structural
  work on ordinary mutations. It did not compare gate-acquisition paths.
- [ADR-056](../../docs/adr/056-demand-driven-inline-pressure-splits.md) ran one
  direct root split and one coordinated non-root split. It did not compare their
  acquisition cost.
- [ADR-061](../../docs/adr/061-atomic-logless-single-leaf-commits.md) sends
  direct commits through the coordinator. Thus, “direct commit” does
  not mean coordinator bypass.
- The 2026-08-12 coordinator measurements show that competing CAS owners can be
  costly. They did not measure gate acquisition on `_r`.

A coordinated root leaf reuses the root load that classifies the node. The
regression records one coordinator submission and round, one physical root
read, and one CAS.

### Remaining performance guardrail

Compare only the two root-leaf acquisition paths under one shared `Database`
and under independent `Database` instances. Race each path with ordinary
root-leaf mutations. Cover a real root split and ADR-062 compaction that avoids
a split.

Measure root reads, writes, bytes, precondition misses, coordinator retries,
and latency. Verify values, deleted-key absence, final topology, holder cleanup,
and bounded shutdown.

Until this comparison exists, coordinated root-leaf acquisition is an
architecture correction, not a performance improvement claim.

## 2026-08-18: ADR-061 acceptance matrix

Status: complete; the direct path meets its uncontended one-CAS cost gate and
retains bounded fallback under same-leaf contention.

The 2/8/32-key blind-put, mixed put/delete, and cross-key RMW matrix landed all
30 low-contention operations directly on memory and the modeled GCS/S3
profiles. Every cell used exactly one object write per transaction and no lock.
Near both inline boundaries, all six memory cells had the same result. The
paired 10-key RMW median fell from `116.77 µs` to `44.19 µs`, while the
single-key guardrail moved from `9.34 µs` to `9.48 µs` with overlapping
intervals.

Disjoint transactions from separate databases still contend on the shared leaf
CAS. Every measured operation entered the direct path; short provider-profile
windows landed `29–30/30` directly and used at most `0.07` lock calls per
transaction after bounded clean-CAS retry exhaustion. This is expected fallback,
not an eligibility failure, so the durable benchmark asserts exact one-CAS
behavior only in uncontended cells and records contention coverage separately.

## 2026-08-15: ADR-060 implementation removed

Status: closed; the delayed write-back scheduler was removed and definitive
write-back losses again use the ordinary convergent coordinator retry.

ADR-060's mechanism worked as designed: it moved a losing committed write-back
out of the contention burst, coalesced later retries, converged before graceful
shutdown completed, and avoided the permanent cold-read debt of abandoning
write-back. The durable implementation nevertheless did not reproduce the
prototype's stable aggregate-throughput improvement. Across three interleaved
pairs, aggregate throughput ratios were `0.982`, `0.867`, and `1.401` (median
`0.982`). Write-shape throughput had a positive `1.073` median but ranged from
`0.712` to `1.345`, while `rwMany` had a `0.765` median. Median backend
operations and coordinator retries fell to `0.901` and `0.669`, respectively,
but their individual pairs were also inconsistent. The implementation therefore
demonstrated mechanical coalescing, not a reliable product-level performance
gain.

Retaining that narrow optimization required a database-local scheduler, a
one-time retry ownership transfer, capacity and shutdown protocols, public
quiet/max-age timing controls, and per-member definitive-loss attribution in
the shard coordinator. Those boundaries were necessary to make delayed work
safe; removing only pieces of them would reintroduce dropped-work, repeated
deferral, in-doubt failure, or shutdown races. The ordinary retry path already
provides correctness, physical convergence, and no unbounded fresh-read debt,
so the measured benefit did not justify maintaining the additional protocol.

The implementation and its architecture documentation were cleanly restored to
their pre-ADR-060 state. The earlier rejection of permanently abandoning a
cleanly losing write-back still stands: it caused repeated transaction-record
reads for fresh database instances and prevented reclamation. Pre-existing
per-member in-doubt attribution and the safety improvements from the
review-blind-spots work remain unchanged.

## 2026-08-12: fixed-topology coordinator retry attribution

Status: investigation complete; no engine change retained. The coordinator's
cross-Database cost is retry sleep after leaf-CAS loss, not its resolver evaluation,
local owner queue, or synthetic backend rate limiter. Permanently deferring a
committed background write-back after its first definitive CAS loss improves
the mixed window but is rejected: the resulting holders create unbounded cold
scan work until later mutation or structural quiescing cleans them. A
one-second, per-leaf quiet retry retains most of the gain while retiring the
holders and is ready for a focused design decision.

Reference: `d7635058`. The benchmark seeded one tree per run and mode, reused
that settled topology for each paired cell, alternated pair order, and opened
fresh `Database` instances so coordinators and caches remained independent.
Temporary probes split coordinator time into owner queueing, node load,
resolver evaluation, mutation, and retry sleep; attributed failed rounds by resolver
kind; and measured actual waits inside the synthetic provider limiters. The
probes, fixed-topology harness, and experimental policies were removed after
the runs.

The decomposition used the two affinity endpoints:

```console
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend memory --delays s3 --delay-scale 0.2 --runs 3 \
  --drain-timeout 90s --output /tmp/p1-coordinator-fixed.json mixed \
  --modes lo,hi --affinities 0,100 --databases 4 \
  --workers-per-shape 8 --duration 2s --max-duration 60s \
  --target-ci 0.1 --split-quiet 10s --split-settle-timeout 60s
```

All 12 cells converged with zero failures and bounded drain. The following are
aggregate worker times over each cell, not wall-clock transaction latency;
ranges cover the three runs.

| Mode / affinity | Failed CAS rounds | Load | Resolver evaluation | Persist | Retry sleep | Retry share of worker time |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| spread / 0% | `2,558–2,842` | `161–167 s` | `9.5–9.8 s` | `668–680 s` | `616–676 s` | `42–44%` |
| spread / 100% | `0` | `102–112 s` | `6.1–6.7 s` | `473–524 s` | `0` | `0%` |
| hot / 0% | `701–916` | `48–50 s` | `31–41 s` | `213–235 s` | `384–424 s` | `54–57%` |
| hot / 100% | `0` | `36–38 s` | `35–40 s` | `184–192 s` | `0` | `0%` |

Mean local queueing in spread mode remains roughly `9–15 ms` per member at
both endpoints. Hot acquisition queueing is higher at 0% affinity (`55–67 ms`
versus about `43 ms`), but is still much smaller than the service and retry
gap. Actual limiter waits total only tens of milliseconds per complete cell,
never more than roughly `75 ms` across thousands of calls. They are negligible
next to hundreds of model seconds of configured provider latency and engine
retry sleep. A fixed tree therefore preserves the gap and rules out topology
variance, process-local queueing, and rate limiting as its primary cause.

A separate one-run attribution counted resolver members present in a failed
CAS round. Multiple members can share one failed round, so these counts do not
equal the round totals.

| Mode / affinity | Direct | Foreground acquire | Background write-back | Dominant failed work |
| --- | ---: | ---: | ---: | ---: |
| spread / 0% | `508 / 1,368` | `579 / 4,539` | `2,049 / 5,747` | write-back (`65%` of failed members) |
| hot / 0% | `115 / 385` | `1,301 / 3,622` | `64 / 1,103` | acquire (`88%` of failed members) |

Each cell is `failed members / staged members`. The distinction matters:
foreground acquisition must eventually establish ownership, while committed
write-back is only eager publication of an already-authoritative transaction
log.

### Evaluated policies

A process-local post-miss batching window does not solve distributed
contention. A `200 ms` window shifted capacity toward reads and hurt write
shapes without reliably reducing misses. A `10/25/50 ms` sweep gave an initial
signal at `50 ms`, but an alternating confirmation did not reproduce a
coherent write gain. Such a window delays an independent database instance's
next CAS; it does not remove any required backend operation and can move
capacity between shapes. It is rejected together with broad retry-backoff
changes.

The focused throughput variant stopped only a committed write-back after its
first clean `PreconditionMiss`. It did not abandon an unavailable or in-doubt
mutation, change foreground acquisition, or suppress uncontended write-back.
The committed transaction record remains authoritative, so the state remains
correct even though the physical holder is left behind. Three alternating
fixed-topology pairs used:

```console
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend memory --delays s3 --delay-scale 0.2 --runs 3 \
  --drain-timeout 90s --output /tmp/p1-drop-writeback.json mixed \
  --modes lo,hi --affinities 0 --databases 4 \
  --workers-per-shape 8 --duration 2s --max-duration 60s \
  --target-ci 0.1 --split-quiet 10s --split-settle-timeout 60s
```

All 12 cells converged with zero failures and bounded drain. Ratios below are
three-run medians for variant over control.

| Mode | Aggregate throughput | `rwSingle` | `rwMany` | `rwSingle` p50 | `rwMany` p50 | Coordinator retries / tx | Backend ops / tx |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| spread / 0% | `1.108` | `1.151` | `1.191` | `0.891` | `0.964` | `0.849` | `0.997` |
| hot / 0% | `1.040` | `1.018` | `1.034` | `0.852` | `0.778` | `0.882` | `0.980` |

In spread mode, failed write-back membership fell from `1,775–1,918` to
`1,185–1,362`, total failed CAS rounds from `2,775–2,801` to `2,159–2,363`,
and coordinator worker time from roughly `1,519 s` to `1,223 s` in the first
pair. Backend operations per transaction remain nearly flat because cleanup is
deferred rather than erased. Hot mode has little failed write-back work, so its
smaller and noisier response is expected. Per-Database completion remains
reasonably balanced in spread mode; hot mode is uneven under both policies,
with no clear additional capture by the variant.

### Follow-up correctness and read-debt validation

Validation reference: `1eaff5dd`. A temporary deterministic integration test
used two independent `Database` instances and a hooked S3-delay memory backend.
It parked a locked transaction's first post-commit write-back CAS, let the
other instance land a disjoint direct commit on the same leaf, then released
the stale CAS. The clean miss stopped with exactly the original write-back CAS
and the winning direct CAS; no second cleanup CAS was issued. The committed
put and delete remained readable with the expected values. A later same-key
mutation removed that key's holder without removing a disjoint holder from the
same transaction.

An `Unavailable` injected before the write-back CAS landed still produced two
write-back attempts and a fully published leaf. This confirms that restricting
deferral to a clean precondition does not consume the existing in-doubt
reconciliation path. All read-only validation waves completed without errors
or body replays, and issued no writes.

The read cost depends on shape and cache lifetime:

| Read after cleanup loss | Normal write-back | Permanently deferred | Repeated warm access |
| --- | ---: | ---: | ---: |
| Point read of external put, per fresh Database | `1` transaction-record read; p50 `62–71 ms` | `1`; p50 `62–66 ms` | `0` object reads; p50 `19–23 ms` for both |
| Point read of delete, per fresh Database | `0`; p50 `44–48 ms` | `1`; p50 `63–72 ms` | `0`; p50 `20–23 ms` for both |
| Full scan with 8 deferred writers, per fresh Database | `0`; p50 `43–49 ms` | `8`; p50 `214–237 ms` | `0`; p50 `18–25 ms` for both |

The scan experiment left two put keys from each of eight distinct committed
transactions on one leaf and opened 12 fresh Databases. The normal tree issued
zero transaction-record reads; the deferred tree issued exactly `96` (`8` per
database instance). Reopening another 12-instance wave issued the same `96`
again, with p50 `220–239 ms`. Terminal-status caching removes the cost for
repeated reads in one Database, but a new database instance or later cache
eviction pays once per distinct holder. Deferring only puts or holders without a
membership lock avoids the delete-specific point-read penalty, but not this
linear scan amplification.

Read resolution only help-forwards a committed holder logically. It does not
rewrite the leaf. GC also does not finish key write-back: a committed
`locked_by` reference makes the transaction record live, so the current
reverse-check keeps it. Same-key mutation or structural quiescing can remove a
holder; absent either, it may persist indefinitely. The earlier assumption
that ordinary access or GC would converge it was wrong.

### Delayed retry validation

A second temporary variant ended the original write-back coordinator episode
after its first *definitive* CAS loss, then placed the transaction and leaf in a
process-local, per-leaf debounce queue. A one-second model-time quiet interval
reset whenever another write-back on that leaf lost. At expiry, all queued
transactions for the leaf re-entered concurrently through the ordinary
write-back path, so the coordinator could merge them and the retry retained its
existing convergence and in-doubt behavior. Each write-back could be deferred
only once. Shutdown first closed the queue, made later submissions retry inline,
and woke existing quiet workers before draining background operations; it never
waited through the quiet interval.

Three alternating control/variant pairs used the S3 profile, four independent
Databases, 0% affinity, 2,000 keys per collection, a 12% throughput-CI target,
and a three-second split-quiet interval. Every cell converged, reported zero
failures, and drained within its bound. Ratios are variant/control; the median
is the middle of the three paired ratios.

| Metric | Pair ratios | Median |
| --- | --- | ---: |
| Aggregate throughput | `1.079`, `1.117`, `1.036` | `1.079` |
| Aggregate write-shape throughput | `1.074`, `1.140`, `1.008` | `1.074` |
| `rwSingle` throughput | `1.056`, `1.266`, `1.009` | `1.056` |
| `rwMany` throughput | `1.124`, `0.846`, `1.005` | `1.005` |
| Backend operations / transaction | `1.014`, `0.893`, `1.017` | `1.014` |
| Coordinator CAS retries / transaction | `1.064`, `0.901`, `1.212` | `1.064` |

Unlike permanent deferral, the delayed variant pays all cleanup before the
post-shutdown counters are sampled. The small increase in median backend work
and coordinator retries is therefore honest deferred work, not hidden debt;
the timed throughput gain comes from moving that work out of the contention
burst. The per-shape result is mixed, but the total write rate improves in all
three pairs and the read shapes also benefit from less concurrent leaf-CAS
pressure.

A separate fixed three-second contention cell exercised the queue directly.
The four measured Databases enqueued `47` transactions into `35` leaf batches;
the largest batch held four transactions. After one quiet second, 12 newly
opened Databases issued 48 full-collection scans over 24,000 returned keys.
Both immediate and delayed policies made exactly `240` node reads and zero
transaction-record reads. Delayed p50 was `29.6–32.7 ms`, versus `29.7 ms` for
the immediate control. The read debt found under permanent deferral is absent.

Two deterministic write-back tests covered the state transitions. A clean
precondition loss returned one delayed request; its later normal retry
published the writer and removed its holder. An injected `Unavailable` before
the write reached storage did not enter the delay queue: the same coordinator
episode issued a second CAS, published the writer, and removed the holder.
Finally, a one-second contention cell with a two-second drain bound had two
pending leaf batches when shutdown began. Both skipped their remaining quiet
period and the run drained successfully.

The one-second interval is a viable initial policy, not an optimized constant.
The durable shape should remain internal to the transaction algorithm: a
per-Database, per-leaf debounce queue owns delayed scheduling; the key locker
returns an opaque deferred result and performs the later normal write-back; and
engine shutdown closes and flushes the queue before closing background task
admission. A queue entry groups intents by transaction id, and different ids
are submitted together so the existing coordinator performs the actual CAS
coalescing. This preserves independent Database instances and introduces no
cross-instance coordination.

### Conclusion

Stopping forever after the first clean loss remains rejected. The bounded,
coalesced delayed retry satisfies the missing convergence, fresh-read,
in-doubt, and shutdown constraints while retaining a median `7.9%` aggregate
throughput gain in the focused spread comparison. It should proceed through a
focused ADR before durable implementation because queue ownership and shutdown
admission are architectural correctness boundaries, even though the behavior
is narrow. GC-driven key publication is unnecessary for this bottleneck. There
remains no evidence for weakening foreground acquisition, direct commit,
release, or other resolver kinds.

## 2026-08-12: transaction-shape coordinator and cleanup attribution

Status: closed without an engine change. Ordinary data-transaction cleanup is
already backgrounded, its remaining foreground work is negligible, and neither
cross-Database transaction-record read coalescing nor unbounded cross-leaf
write-back parallelism moves throughput reliably. The remaining affinity gap is
inside independent coordinators competing on the same leaf CAS, outside their
resolver evaluations.

Reference: `11d934ac`. Temporary process-wide counters attributed coordinator
submission, resolver evaluation, transaction-status resolution, write-back, retry
release, collection-directory write-back, and collection finalization to the
four mixed-workload shapes. A corrected overlap counter covered remote status
reads across all `Database` instances. All probes and experimental switches
were removed: their atomics and global maps perturb scheduling and are not a
production metrics interface.

The primary run used the same corrected synthetic S3 profile and endpoints as
the preceding routing investigation:

```console
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend memory --delays s3 --delay-scale 0.2 --runs 3 \
  --drain-timeout 90s --output /tmp/p1-phase-global.json mixed \
  --modes lo,hi --affinities 0,100 --databases 4 \
  --workers-per-shape 8 --duration 2s --max-duration 60s \
  --target-ci 0.1 --split-quiet 10s --split-settle-timeout 60s
```

All 12 cells completed with zero failures, bounded shutdown, and every shape at
the 10% throughput-CI target. Spread setup completed `104–124` splits and hot
setup completed none. Instrumented throughput is deliberately not a new
baseline.

### Shape and phase attribution

The table contains three-run medians in model milliseconds. Submission and
resolver evaluation times are means within a cell and are not additive
transaction latency: workers, coordinator members, and background cleanup
overlap. `rwMany` write-back is the duration of one background pass; it submits
once per touched leaf. Remote status rates use all four shapes as the
transaction denominator.

| Mode / affinity | `rwSingle` direct submit / resolver evaluation | `rwMany` acquire submit / resolver evaluation | `rwMany` background write-back | Foreground post-commit | Remote status calls / body misses per tx | Same-TID remote overlap |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| hot / 0% | `801 / 1.36 ms` | `434 / 7.92 ms` | `270 ms` | `<0.015 ms` | `1.163 / 0.423` | `42.4%` |
| hot / 100% | `124 / 0.018 ms` | `139 / 8.49 ms` | `176 ms` | `<0.015 ms` | `0.0126 / 0.0004` | negligible volume |
| spread / 0% | `313 / 0.075 ms` | `150 / 0.123 ms` | `2,085 ms` | `<0.015 ms` | `0.234 / 0.215` | `7.8%` |
| spread / 100% | `77.6 / 0.026 ms` | `104 / 0.021 ms` | `678 ms` | `<0.015 ms` | `0.068 / 0.050` | `27.0%` |

The ordinary data path has no collection changes, so collection-directory
write-back and `finish_committed` perform only local empty-work bookkeeping.
Their combined measured cost is under `0.015 ms`. Key write-back is already a
shutdown-drained `Background::spawn_waited` task; "move write-back off the
foreground path" is therefore not an available optimization.

Resolver work also does not explain submission time. At the hot endpoints the
`rwMany` resolver evaluation remains roughly `8 ms` while submission changes by
`3.1x`; direct-commit resolver evaluation work is almost zero while submission
changes by `6.5x`. The missing time is after local admission and outside policy
resolution: independent per-Database owners race the same object CAS, reload,
and back off. This agrees with the earlier per-leaf miss probe and zero CAS
retries at 100% affinity.

### Rejected candidates

Three temporary variants bounded the two proposed fixes. Every run converged
without failures or shutdown timeout.

- Suppressing measured key write-back was not a viable simplification. It
  removes cleanup CAS work but leaves committed holders for readers and later
  acquires to resolve. The three-run aggregate median moved only `+3%` at the
  spread 0% endpoint and regressed about `9%` at hot 0%; read shapes lost the
  benefit of prompt publication. This is a trade between writes and subsequent
  reads, not eliminated work.
- Running one transaction's independent write-back leaves concurrently cut the
  spread `rwMany` cleanup pass to `0.26–0.31x`, backend work to `0.91–0.97x`,
  and transaction-status body misses to `0.93–0.98x`. In three interleaved
  spread/0% comparisons, however, aggregate throughput ratios were `1.36`,
  `0.81`, and `0.92`. The winning pair had the same 116-leaf setup; the losing
  pairs also exposed the known settled-tree-count variance. Unbounded cleanup
  parallelism creates a larger CAS burst and has no stable throughput result,
  so the temporary change is rejected.
- A temporary backend-response singleflight coalesced only concurrent `_t`
  reads with the same operation and conditional revision. Each `Database` still
  installed the reply in its own cache and timeline, avoiding a fake shared
  observation. Hot/0% has real opportunity—`38–40%` of remote status reads in
  the interleaved controls overlapped the same transaction—but aggregate
  throughput ratios were `1.06`, `0.90`, and `1.01` (median `+0.8%`), while
  `rwMany` ratios were `0.98`, `0.97`, and `0.92`. Physical reads per
  transaction were mixed as the workload state changed. Besides not producing
  a stable gain, process-local coalescing would optimize colocated benchmark
  database instances rather than the distributed case the affinity sweep
  represents.

No status singleflight, cleanup suppression, unbounded write-back concurrency,
or backoff change is justified. A follow-up should hold the settled tree fixed
between paired variants and split the coordinator's time outside resolver
evaluation among local owner queueing, backend mutation/rate limiting, and retry
sleep. Any proposed protocol fix should reduce required leaf-CAS work for
independent database instances, rather than rely on sharing process-local state.

## 2026-08-11: ADR-050 resolved-handle routing

Status: closed without an engine change. Resolved data access does not repeat
collection lookup, and neither duplicate tree descent nor the pre-commit lock
interval explains the cross-Database affinity gap.

Reference: `3f70a759`. Temporary process-wide counters timed point routing,
lock grouping, validation fallback, coordinator submission/load, and the
locked-validation-to-log-commit interval. The role-aware backend wrapper and
decoded-cache counters were bracketed before shutdown. All probes were removed
after the experiment because their atomics perturb scheduling and their
process-wide shape is not a useful production interface.

The primary run used the calibrated synthetic S3 profile:

```console
cargo run --release -p glassdb-bench-scale --bin perfbench -- \
  --backend memory --delays s3 --delay-scale 0.2 --runs 3 \
  --drain-timeout 90s --output /tmp/adr050-routing-probe.json mixed \
  --modes lo,hi --affinities 0,100 --databases 4 \
  --workers-per-shape 8 --duration 2s --max-duration 60s \
  --target-ci 0.1 --split-quiet 10s --split-settle-timeout 60s
```

All 12 cells completed with zero failures, bounded drain, and every shape at
the 10% throughput-CI target. Spread setup completed `107–124` splits and hot
setup completed none. Measurement performed no structural-log reads or writes;
one `_s/` recovery list per newly opened Database can race the measurement
bracket and is not a split.

### Physical operation traces

Temporary `RecordingBackend` tests exercised cold and warm read-only
single/multi transactions, direct single-RMW, and locked multi-RMW on both a
single-root collection and a split tree.

- A cold small-tree read loads `_r`, its external transaction value, and then
  conditionally validates `_r`. The warm equivalent performs only the expected
  terminal `_r` validation.
- A cold split-tree read enters at `_r`, loads the owning `_n` leaf and external
  value, and validates that leaf. Once `_r` and the leaf are cached, the warm
  read does not physically re-descend from `_r`.
- A multi-read loads only an as-yet-uncached second leaf and validates each
  touched leaf. A direct RMW issues the owning-leaf CAS. A locked multi-RMW
  issues one validation/CAS sequence per touched leaf and creates one
  transaction record.
- No resolved operation reads `_i`. The existing permanent regression test
  continues to enforce that contract.

Two of the three long `lo/0%` cells each recorded one zero-byte `_i` read among
thousands of transactions. A focused path probe reproduced it only after a
long cell: it targets an absent collection ID. Static call-chain
review distinguishes it from data routing (`TreeRouter` has no
`CollectionStore`): this is delayed background lifecycle/GC reclamation of an
unreachable prepared collection, not logical-path revalidation. Its cost is
negligible and belongs to background lifecycle cleanup, not foreground routing.

### Phase attribution

The following are three-run medians. Throughput is included only to show that
the instrumented run reproduced the affinity effect; probe atomics make it
unsuitable as a new baseline. Times are model milliseconds per completed phase,
not additive transaction latency because phases overlap across workers.

| Mode / affinity | Aggregate tx/s (range) | Route cache hit | Lock grouping | Coordinator submission | Coordinator load hit | Locks to validation | Validation to log commit |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| hot / 0% | `61.1` (`60.2–63.9`) | `98.0%` | `0.58 ms` | `448 ms` | `86.7%` | `0.11 ms` | `60.3 ms` |
| hot / 100% | `153.4` (`153.0–154.7`) | `99.9%` | `0.06 ms` | `134 ms` | `100%` | `0.14 ms` | `61.1 ms` |
| spread / 0% | `194.5` (`175.8–226.0`) | `97.1%` | `0.75 ms` | `194 ms` | `68.7%` | `6.85 ms` | `61.4 ms` |
| spread / 100% | `342.8` (`336.9–348.7`) | `99.3%` | `0.17 ms` | `89.3 ms` | `100%` | `1.74 ms` | `59.8 ms` |

Lock grouping remains below one millisecond in every cell. Point-route cache
hit rate is at least 97%; moreover, mean point-route time in spread mode moves
slightly in the wrong direction for the hypothesis (`1.96 ms` at 0% versus
`2.39 ms` at 100%). The much larger route-call and L1-read volume at 0% is
downstream re-resolution driven by cross-instance protocol work, not two
physical loads inside one ADR-050 descent.

The post-lock interval also rejects shortening lock lifetime as this fix.
Validation-to-transaction-record commit is essentially invariant at `60–61 ms`;
locks-to-validation is tiny except for a still-secondary `6.85 ms` spread/0%
mean. Coordinator submission remains the differentiator: `3.3x` slower at the
hot endpoint and `2.2x` slower at the spread endpoint, while coordinator load
latency itself moves only from about `12–15 ms` to `12–14 ms`. The missing time
therefore remains CAS ownership, member scheduling, backoff, and any resolution
performed within those rounds.

No redundant physical node read was found, so this investigation produces no
durable routing optimization. The next locked-path investigation should split
coordinator submission and post-commit foreground work by transaction shape;
aggregate role counts alone cannot choose between foreign-status deduplication
and deferred cleanup because adaptive mixed cells complete different shape
mixtures at different affinities.

## 2026-08-03: Simulated-time calibration and cross-Database attribution

Status: benchmark timing correction implemented; corrected affinity curves and
the production-timescale canonical baseline rerun are complete.

Reference: `14de11e8`, after `mixbench` and `rtbench` were consolidated into
`perfbench`. The investigation asks whether the new affinity workload measures
steady-state cross-Database costs faithfully and which foreground phase causes
the remaining locked-path gap.

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
transaction-record traffic. Four Databases ran every shape with eight workers per
shape; setup completed its split cascade before measurement.

With corrected `0.02` retry timing, the backend attribution was:

| Mode / affinity | Aggregate tx/s | Node ops/tx | Transaction-record reads/tx |
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
cache and shard coordinator. It can batch local submissions into fewer CAS
rounds and already knows the status of its own transactions. At 0%, the same
logical collection load is distributed across independent coordinators. They
cannot merge across processes, issue competing node CASes, reload losers, and
read foreign transaction records. The dominant cost is therefore cross-instance
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
One coordinator batches almost twice as many members per round, needs about half
as many rounds per transaction, and never loses a leaf CAS to another
Database. At every partial-affinity point, foreign writers preserve the CAS
retry rate; extra local traffic increases members per round gradually but cannot
produce the endpoint's single-owner behavior.

The spread endpoint rerun has noisier absolute throughput (`181.6` versus
`339.3` transactions/s, compared with `227.2/340.5` in the full curve), but
isolates the other mechanism. Members/round barely moves from `1.06` to `1.13`,
while CAS retries fall from `0.414` per transaction to zero and backend work
falls from `5.14` to `4.21` operations/transaction. With keys distributed
across many leaves there is little local batching opportunity; independent
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
would need to be demand-driven by sustained cross-instance CAS contention,
bounded above a leaf-size floor, and evaluated separately for single- and
multi-key shapes.

### Per-leaf CAS-miss concentration

A final temporary benchmark probe at `7b87b91a` tested whether that
demand-driven response could target a small set of unusually contentious
leaves. A `HookBackend` counted conditional node-write attempts and
precondition failures by object path only during the measured interval. For
each path it also found the maximum failures in a sliding 30-second model-time
window. The probe was removed after the experiment; these results are not
absolute throughput measurements because the callback itself adds work.

Three S3-profile spread runs used `delay-scale=0.2`, four Databases, eight
workers per shape, and the normal 256-entry leaf cap. The `100%`-affinity cells
are controls for false positives:

| Run / affinity | Active paths | Paths with misses | Node misses / coordinator retries | Busiest-quarter miss share | Paths with at least 16 misses / captured misses |
| --- | ---: | ---: | ---: | ---: | ---: |
| `1 / 0%` | `114` | `114` | `2894 / 2885` | `38.4%` | `71.1% / 83.5%` |
| `2 / 0%` | `110` | `110` | `2690 / 2681` | `37.4%` | `83.6% / 91.7%` |
| `3 / 0%` | `120` | `120` | `2852 / 2842` | `37.4%` | `81.7% / 90.4%` |
| `1 / 100%` | `120` | `0` | `0 / 0` | `0%` | `0% / 0%` |
| `2 / 100%` | `125` | `0` | `0 / 0` | `0%` | `0% / 0%` |
| `3 / 100%` | `124` | `0` | `0 / 0` | `0%` | `0% / 0%` |

All shapes converged, every cell had zero failures and zero measured splits,
and setup reached the ten-second quiet period. The small excess of observed
node failures over coordinator retries is consistent with failed direct-path
CASes, which use the same node objects but are outside the coordinator counter.
At thresholds of four and eight failures per 30 seconds, effectively every
0%-affinity path qualifies and captures effectively every miss.

The signal is broad, not concentrated. The busiest quarter of paths is the
best possible subset of that size, yet it captures only `37–38%` of misses.
Therefore no threshold can select at most `25%` of paths while capturing at
least `50%` of the observed contention. Splitting enough leaves to address the
signal would approximate the rejected global-cap change, including its extra
structural work and multi-key fan-out cost. A CAS-miss-driven split hint is
rejected for this workload; the remaining cross-Database gap is an ownership
and coordination problem rather than a localized leaf-capacity problem.

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
repair the cross-instance gap. No production retry or deadlock-timing change is
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
Transaction-body replay rates do not explain the cliff: hot medians remain
`0.24–0.30` replays/transaction across the curve. The extra work is below that
counter, in shard-coordinator rounds, CAS misses, reloads, and transaction-record
resolution identified by the phase probe.

### Production-timescale v0.1.0 comparison

A final three-run comparison used `delay-scale=1`, five deterministic
efficiency samples, and three-second contention cells:

```console
BASE=v0.1.0 LABEL_A=v010 LABEL_B=current DELAY_SCALE=1 NUM_RUNS=3 \
  CONTENTION_DURATION=3s COUNT=5 DRAIN_TIMEOUT=90s \
  hack/aws-bench/compare_refs.sh --summary
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
incurs a median of about `3.18` replays and `4.18` direct candidates.
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
uncompressed control and is explained by per-instance batching/cache boundaries.
Partial affinity does not gradually recover the cost; complete collection
ownership is qualitatively different.

The production-timescale baseline also separates two signals that were
previously conflated. Current one-key throughput is already at v0.1.0 parity
despite its roughly five-times-higher p50, while deterministic backend work is
substantially lower. The next investigation should therefore target the
current engine's cross-instance shard-CAS rounds, not the `readRepeat`
classification or the retired rw9010 throughput number.

The coordinator counters establish cross-instance leaf false sharing, but
neither a global nor a demand-driven structural change is supported. The
global-cap screen trades lower contention for much more structural work and
worse multi-key fan-out. The per-leaf probe finds no small hot subset: misses
cover every active leaf, and the busiest quarter captures only `37–38%` of them.
Addressing this gap requires reducing cross-Database ownership and coordination
cost rather than interpreting widespread CAS misses as local split pressure.

## 2026-07-29: Inline admission and structural amplification

Status: locked-publication simplification implemented by
[ADR-054](../../docs/adr/054-reserve-inline-publication-for-logless-commits.md);
inline-pressure splitting implemented and validated by
[ADR-056](../../docs/adr/056-demand-driven-inline-pressure-splits.md); budget
tuning completed by the [inline-policy sweep](#inline-policy-sweep) below.

Reference: `5c3e5ac6`, after ADR-053. The goal is to isolate ADR-051's
provisional inline budgets from the later contention fix.

### Initial sweep

A temporary role-counting backend wrapper, removed after the experiment,
distinguishes node, transaction-record, and structural-log operations and bytes.
Three runs sweep no inlining, the then-current 1 KiB / 64 KiB policy, 4 KiB and
16 KiB aggregate budgets, and selected per-value and encoded-object limits.
The workloads cover serial and dense-leaf RMW, batch write, cold and warm read,
cache pressure, and foreground versus background split work under the S3, GCS,
and memory profiles. Every run completes without transaction failures.

#### Direct-path benefit

- On a serial S3-profile RMW, the then-current policy reduces median latency
  from `614` to `177 ms/transaction` for 128 B values and from `627` to
  `192 ms/transaction` for 1 KiB values. A cold inline read uses one node
  operation instead of a node plus transaction-record read; a warm read gains
  little once the transaction record is cached.
- The GCS-profile latency gain is smaller but the operation saving remains:
  median latency falls from `1367` to `1017 ms/transaction` at 128 B and from
  `1176` to `1052 ms/transaction` at 1 KiB.
- Raising the per-value limit makes a serial 4 KiB S3-profile RMW land directly
  and reduces median latency from `525` to `183 ms/transaction`. This does not
  establish that dense leaves should carry 4 KiB inline values, where every CAS
  would rewrite them.

#### Dense-leaf mixed regime

- A locked 128-key batch transaction performs the same number of backend
  operations with or without inlining. At 1 KiB, however, median node-write
  volume rises from `11.26` to `118.24 KiB/transaction`: write-back adds a
  cached copy of values already durable in the transaction record.
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

The aggregate budget is sticky and first-come. Locked write-backs consume it
even though their transaction records remain authoritative; later direct
commits that require inline storage fall back to locking. Existing inline
payloads must be preserved because a direct writer may have no transaction
record, so every later leaf CAS continues to rewrite those bytes. This partial
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
leaf. One locked transaction updates the 64 even keys, then two single-key RMW
passes update the 64 odd and 64 even keys. It compares current behavior with a
temporary variant that suppresses only new locked write-back inlining, while
preserving existing inline states and direct publication. A 64-entry leaf split
threshold is an upper-bound proxy for reacting to inline pressure; it is not a
proposed global default.

- Suppressing write-back inlining reduces the locked batch's node-write volume
  from `73.8` to `9.6 KiB` while retaining the same transaction-record write and
  two node writes. A cold scan adds one `65.7 KiB` transaction-record read, because
  every key names the same locked transaction; median scan latency is
  effectively unchanged in both profiles.
- With the normal 256-entry split threshold, current behavior lands `0/64`
  direct commits on the odd keys and `64/64` on the already-inline even keys.
  Suppressing write-back inlining reverses the result: `64/64`, then `0/64`.
  Total direct capacity does not change; the policy only chooses which keys
  receive it.
- The 64-entry split proxy performs one split and three structural-log writes,
  after which both passes land `64/64` direct commits. Combined median RMW
  latency falls from `570` to `265 ms` in the S3 profile and from `2211` to
  `1435 ms` in GCS. It also makes the locked batch span two leaves: node writes
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
cross-instance external-value resolution adds transaction-record lookups and a
long tail to contended multi-key mutations. That attribution needs a repeated,
phase-level run before changing policy.

### Multi-RMW tail follow-up

Status: closed as non-reproducible; the measured representation trade-off
supports ADR-054 and does not justify a tail-specific engine change.

The follow-up first isolates ADR-054 by comparing its accepted parent
`f618e738` with implementation `7bc6fb01`. Three alternating S3-profile
`hi/per-shape` pairs use eight workers per shape, four `Database` instances per
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

For `rwMany` in the cross-instance `per-shape` topology:

- transaction-record body reads rise by only `11.3–14.8 B/transaction`; physical
  transaction-record calls are too variable to distinguish because unchanged
  conditional reads transfer no body;
- L1 misses rise by `0.12–0.39/transaction`;
- node reads fall by `57–81 B/transaction`; and
- node writes fall by `2.14–2.15 KiB/transaction`.

The shared-Database topology makes the added transaction-record body transfer
almost disappear (`0.071–0.079 B/transaction` across the whole mixed cell),
confirming that the decoded cache absorbs repeated resolution when workers
share it. Cross-instance caches cannot share that entry, but their
extra body transfer remains much smaller than the leaf bytes no longer
rewritten. Shutdown attribution is zero for every per-shape run and negligible
in the shared runs, so this is a foreground representation trade-off rather than
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
128-key locked batch. Fresh strong reads verified the serial and dense markers
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

- Full admission at 8 B removes one transaction-record mutation per dense RMW,
  reduces backend operations to `0.36x`, and leaves node bytes effectively
  unchanged. This is an unambiguous win in both profiles.
- Full admission at 128 B also removes one transaction-record mutation and reduces
  operations to `0.35–0.37x`, but node write bytes rise from roughly `5–6` to
  `12.8 KiB/tx`. The 16 KiB and 64 KiB policies are equivalent in this cell;
  4 KiB falls onto the partial-admission cliff.
- At 1 KiB, the 64 KiB policy saves only `0.48` S3 and `0.44` GCS
  transaction-record operations per mutation in the first dense wave. Total write
  bytes rise by `12.5x` and `14.6x`, respectively. Smaller budgets cap each
  leaf but pay more fallbacks and permanent splits; none dominates both
  profiles. The 4 KiB aggregate candidate is not worth carrying forward.
- Over-budget 4 KiB values admit nothing and behave like no inlining, within
  run noise. This confirms that merely enabling an inline policy has no
  material fallback tax.

Reads repay some of the retained inline bytes. At 8 B and fully admitted 128 B,
cold reads fall from `2.01` to `1.01` backend operations per key. At 1 KiB, the
64 KiB policy's median half-coverage lowers cold operations to `1.52`; after a
warm pass, transaction-record reads fall to `0.03–0.04` per key versus `0.27` with
no inlining. External 4 KiB values exceed the 256 KiB cache working set and
still need about `0.98` transaction-record reads per warm key.

Locked batches publish no new inline values, but acquiring and clearing leaves
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
not persisted. Database instances of one database should nevertheless deploy the
same configuration, because pressure splits durably change its topology.

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
  under 64 KiB and `113.5` under 16 KiB. The suite's 8 B values and locked
  batches do not exercise the aggregate-cap difference.
- The broad rw9010 throughput geomeans are `0.92` balanced, `1.04` read-heavy,
  and `1.04` write-heavy; backend-operation geomeans are `1.08`, `1.02`, and
  `1.10`. Their wide paired ranges, together with the deterministic and focused
  results, do not establish a default-induced regression.

The guardrails therefore retain the 16 KiB default. Its intentional cost
remains the earlier pressure splitting measured by the sweep for dense 1 KiB
values; workloads that prefer fewer objects can select 64 KiB explicitly.

### Current conclusion

ADR-054 removes locked write-back amplification, and ADR-056 supplies the
focused inline-capacity fix without globally lowering split thresholds. The
multi-RMW follow-up finds no durable tail regression and shows that the
cross-instance transaction-record transfer is small beside the saved leaf bytes.
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
