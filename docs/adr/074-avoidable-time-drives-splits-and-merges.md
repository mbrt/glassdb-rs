# ADR-074: Avoidable time drives splits and merges

## Status

Proposed.

Refines [ADR-073](073-merge-nodes-into-right-sibling.md)'s maintenance policy.
With the avoidable time policy, leaves get demand causes for splits and merges,
and the underfull threshold of leaves becomes one live entry. The candidate
queue, the merge vetoes, and the split and merge protocols do not change.

Refines [ADR-056](056-demand-driven-inline-pressure-splits.md). With the
avoidable time policy, an aggregate inline rejection adds avoidable time to its
leaf. It does not request a split alone.

Keeps the soft caps of [ADR-031](031-dynamic-range-sharding.md) and the
capacity hints of [ADR-072](072-persisted-database-settings.md) as split
causes.

Keeps the position of [ADR-064](064-bounded-parallel-point-leaf-work.md) that
the backend and its provider own throttling. The `Backend` trait does not report
throttling.

## Context

The split causes of ADR-031 and ADR-056 count entries and bytes, or react to
one inline rejection. The merge cause of ADR-073 counts live entries. None of
them measures what the topology costs transactions.

The perfbench `topology` scenario measures fixed topologies under workloads with
different key locality, on the S3 and GCS delay models. Each tree has leaf
entry limits of 16 or 128. Measurements with 8 workers:

| Workload                    | Faster on S3        | Faster on GCS       | Counter with the largest difference, per transaction |
| --------------------------- | ------------------- | ------------------- | ---------------------------------------------------- |
| One key, 1 database         | neither             | 16, by 3 times      | GCS queue wait: 185 ms at 128, 14 ms at 16          |
| One key, 4 databases        | 16, by 1.7 times    | 16, by 3 to 5 times | lost CAS on other keys: 0.5 at 128, 0.2 at 16       |
| One writer, 64 hot keys     | neither             | 16, by 4 times      | GCS leaf CAS time: 1065 ms at 128, 285 ms at 16     |
| 8 adjacent keys, 1 database | 128, by 1.6 times   | 16, by 1.4 times    | S3 direct commits: 0.93 at 128, 0.35 at 16; GCS queue wait: 270 ms at 128, 80 ms at 16 |
| Scan of 64 keys             | 128, by 4 times     | 128, by 3 to 4 times | backend operations: 1.5 at 128, 6 to 8 at 16       |

Other results:

- On both backends, a leaf of 200 entries with 1 KiB inline values (about
  200 KiB) had the same throughput as a leaf with 64-byte values. Its leaf CAS
  time without throttle wait was also the same. These cells ran without the
  model time speedup. The delay models have no per-byte cost, so this shows
  only that CPU cost is small below the soft cap.
- When a direct commit cannot publish its values inline, it uses a locked
  commit. With leaves of 16 entries, a single-key transaction then takes about
  90 ms more on S3 and about 200 ms more on GCS.
- For one writer on GCS, the leaf CAS time that the engine measured was about
  76 ms more than the throttle wait of the delay model. This is the time of one
  leaf CAS without throttling.
- An earlier cost model counted lost CAS, cross-leaf misses, and scan crossings,
  with one fixed cost per event. It selected the faster topology in 15 of 16
  cells. It failed on GCS, where a lost CAS costs more than on S3.

So the faster topology depends on the backend and on the workload. Counts of
entries and bytes cannot select it. Measured times of backend operations can.

## Decision

### Measure avoidable time

Avoidable time is the time of backend operations that a transaction spends
because of the current topology, and that one split or one merge removes. Each
database instance measures it for its own transactions. It measures split causes
for each leaf, and merge causes for each pair of adjacent leaves with the same
parent.

Split causes of a leaf:

- **Lost CAS on other keys.** A leaf CAS of a coordinator round fails, because a
  CAS for other keys landed first. The time is from when the round sent the
  failed CAS to when it sent the CAS that lands or that loses to a change of
  its own keys, or to the end of the round if no CAS lands. At each leaf CAS,
  it counts the members of the round that a transaction waits for, and not
  write-backs, releases, or structural gates. The round measures it at each
  retry, so that a window has the time of a round that still loses. A split
  can put the keys in different leaves.
- **Queue wait for other keys.** A round member waits for an earlier
  coordinator round of the same leaf, and that round has none of the keys of
  the member. A split can put the keys in different leaves.
- **Slow leaf CAS.** The time of a leaf CAS above 4 times the lowest recent
  leaf CAS time of the database instance. This includes the time that the
  backend adapter retries throttled requests, for example for the GCS limit on
  writes to one object. The reference is the lowest time, and not a quantile,
  because when all writes go to one throttled leaf, all recent CASes are slow.
- **Inline pressure.** An aggregate inline rejection of ADR-056. Its time is the
  locked commit penalty: the time of the locked commit minus the typical time
  of a direct commit, as the database instance measures it.

A lost CAS, a queue wait, and a slow leaf CAS count only when a split at the
median key of the leaf puts the keys of the member in one half and the other
keys in the other half. For a lost CAS, the other keys are the keys that the
landed CAS changed, and the median is of the leaf that the round expected. For
a queue wait and a slow leaf CAS, the other keys are the keys of the previous
round of the leaf, and the median is of the leaf of that round. A slow leaf CAS
counts once for each member that the split separates. The median of the last
such cause is the split key of the leaf. A split for these causes is at the
split key, because the leaf can change after each measurement. A split for
inline pressure is at the median of the current leaf.

Merge causes of two adjacent leaves:

- **Adjacent cross-leaf miss.** A direct commit candidate has its keys in these
  two leaves only, so it uses a locked commit. Its time is the locked commit
  penalty.
- **Scan crossing.** A scan continues from the left leaf into the right leaf.
  Its time is the time of the extra leaf read in the scan, and of the check of
  that leaf when the scan is validated.

These are not causes, because no single split or merge removes them:

- A lost CAS on the same keys. A split cannot separate them.
- A queue wait for an earlier round that has one or more keys of the member.
  A split cannot separate them.
- A lost CAS, a queue wait, or a slow leaf CAS whose keys a split at the median
  key does not separate.
- A cross-leaf miss whose keys are in more than two leaves.
- S3 throttling of a prefix. All nodes of a collection share one prefix.

### Select the policy for each database instance

A database option selects the topology policy of a database instance. The
size causes stay the default. The avoidable time policy is opt-in, and a later
change can make it the default. The option is a non-exhaustive enum, so that a
later policy can add a variant without a break.

### Decide in a topology rule

A topology rule decides the splits and merges of leaves for a policy. It is an
internal trait of the engine. Once in each window of one second, the engine
gives the rule the measurements of the window: the avoidable time and the split
key of each active leaf, the avoidable time of each pair of adjacent leaves,
and the typical times of a split and of a merge. The rule returns the changes
that it wants: a split at the median, a split at a key, or a merge into the
right sibling. The trait and the window are internal, so that a new
measurement is not a break of the public API.

For each leaf, the window also has the time of the commit passes that a
conflict ended without a commit, and whose keys a split at the median puts in
both halves (the divided conflict time). For each pair of adjacent leaves, it
has the time of the commit passes that a conflict ended, and whose keys are in
the two leaves only (the crossing conflict time). The time of a commit pass
includes the body run before it. A transaction that starves seldom commits, so
only the time of its commit passes shows it in each window.

The engine checks each change again against current state. It skips a split
of a leaf with less than two entries, a split at a key that leaves one half
empty, a merge of the root, and a merge that the ADR-073 merge rules do not
allow, except the underfull threshold. So a rule cannot make the tree
unsafe. The engine still decides the hard cap splits, the soft cap splits, the
merges of leaves with no live entries, and the index nodes. With the size
causes, the rules of ADR-031, ADR-056, and ADR-073 decide leaves, as before
this ADR.

The avoidable time rule decides splits on moving averages with a half-life
of 10 s. The net split-side time of a leaf in a window is its split-side
avoidable time, less 4 times its divided conflict time. After a split, the
divided transactions that conflict wait for the locks of more leaves, and
they conflict more. A leaf becomes a split candidate when the moving average
of its net split-side time is more than 0.05 times the typical time of one
split. A split is at the split key when the lost CAS, queue wait, and slow
leaf CAS time alone pays for it, and at the median when inline pressure is
also necessary. A leaf over a soft cap splits at the median without avoidable
time.

Two adjacent leaves become a merge candidate when their merge-side time is
more than 0.1 times the typical time of one merge, plus the split-side
avoidable time of both leaves in the window. The merge-side time is the
merge-side avoidable time of the window, plus the moving average of the
crossing conflict time of the pair. The crossing conflict time shows the cost
of a split after the split, when the estimate before the split was too small.
The average also decides for pairs of earlier windows. When another instance
splits a leaf of the pair, the transactions are over three leaves, and a
merge of the left leaf with its current right sibling takes one of them away.
The other merge causes are of one window, because scans cross leaves only in
some windows, and a moving average stays below the time of these windows.

The multiples are less than one, because a change keeps its effect after the
windows that paid for it, and each database instance sees only its own part
of the time. Multiples of one did not merge leaves of 16 entries under scans
of 64 keys, which a merge made 3 times faster. The split multiple is lower,
because a moving average stays below the time of the windows that have the
most time. The weights, the half-life, and the multiples come from the
perfbench `mixed` and `topology` scenarios. The investigation log
([hack/perf/investigations.md](../../hack/perf/investigations.md)) has the
runs.

The database instance measures the typical time of a split and of a merge. A
default applies until it measured one. The window is local to each database
instance, like the soft thresholds (ADR-072).

A leaf that a split wrote in this window or the last one does not merge, and a
leaf that a merge wrote in this window or the last one does not split for
avoidable time. This is the hold-down window. With the ADR-073 merge vetoes, it
stops leaves from splitting and merging again and again in each window. The
moving averages of a leaf and of its pairs start again at each change of the
leaf.

### Keep the size causes

A capacity hint (ADR-072) still forces a split when a change does not fit in the
hard cap. The soft caps on entries and encoded bytes stay as split causes and as
merge vetoes. Below the
soft caps, the measurements show no time cost, so the soft caps limit the size
of one leaf CAS and cost no throughput. On a backend with a per-byte cost, a
large leaf shows as slow leaf CAS.

### Merge leaves with no live entries

A leaf with few live entries costs time only when transactions or scans cross
its boundaries. The merge causes measure this time. But no transaction may use
a leaf with no live entries again, so it gets no time. With a queue-like load,
these leaves would stay, and the tree would grow without a limit. So a
non-root leaf with no live entries is still an underfull merge candidate of
ADR-073, with a threshold of one live entry. Index nodes keep the underfull
cause, because transactions do not write them.

### Measure in the engine

The leaf coordinator measures the time of each leaf CAS. Backend adapters retry
throttled requests, so this time includes throttling. The leaf coordinator also
measures the queue wait of each round member, because only it knows the keys of
the member and of the earlier round. The GCS adapter must retry throttled
requests with the same retry budget as the S3 adapter. This is a separate
change.

## Consequences

- The topology follows the measured cost. One policy suits S3 and GCS, without
  thresholds for each backend. In the perfbench `topology` scenario, seeded
  with leaves of 16 or 128 entries, the avoidable time policy had 1.16 to 1.17
  times the throughput of the fixed seeded tree on S3, and 1.23 to 1.28 times
  on GCS. This is the geometric mean over 20 cells, in each of 2 runs. The
  worst cell was 0.80 to 0.84 of the fixed tree on S3, and 0.75 to 0.86 on GCS.
- In the perfbench `mixed` scenario with 5000 keys for each collection (lo
  mode), the policy had 1.12 to 1.13 times the throughput of the size causes
  on S3, and 1.24 to 1.25 times on GCS. This is the geometric mean of the four
  transaction shapes, over 1 to 8 database instances and 0 to 100% collection
  affinity, in each of 2 runs. On S3, the gain was about the same when the
  instances shared all collections as when each instance used only its own
  collection. On GCS, it was 1.20 when they shared all collections and 1.37
  when each used its own. The gain was smaller with more instances, because
  the same number of workers then had less contention on each collection.
- In the `mixed` hi mode, each collection has 8 hot keys, and two shapes read
  all 8. One of them also writes all 8, so it conflicts with every other
  write. A split puts these transactions in more leaves, where they conflict
  more. The divided conflict time stops most of these splits, and the crossing
  conflict time merges back the others. The policy had 0.98 to 1.02 of the
  size causes on S3, and 0.96 to 1.00 on GCS.
- Each database instance decides on its own transactions, so one instance can
  split a leaf for its lost CAS time while another merges it for its crossing
  conflict time. In the `mixed` hi mode with 8 instances, a few runs of a cell
  split and merged the same leaves again and again in the measurement, and had
  0.65 to 0.73 of the size causes.
- A leaf splits only after its avoidable time continues for some windows. A
  short burst of contention does not split it. After a change of the load,
  the policy splits later than a rule of one window.
- Each database instance sees about one part in N of the time of a leaf that N
  instances share. So with more instances, a change needs more time in total.
  With 4 instances, leaves of 16 entries under scans merged only with a merge
  multiple of 0.1.
- After a split, the time that the split removed is not measured again. A merge
  for adjacent misses can then undo the split, and the leaf can split again.
  With 8 adjacent keys in each transaction and leaves of 128 entries, the
  policy made about 20 splits and 20 merges before the measurement and 5 to 12
  of each during it, and had 0.90 to 1.02 of the throughput of the fixed tree
  on S3.
- A tree does not change under a load that its topology does not slow down.
  After deletes, cold leaves with few live entries stay until transactions or
  scans cross them. Fewer merges also leave fewer drained nodes.
- The first inline rejections of a leaf use the locked commit. The leaf splits
  only after enough rejections to pay for the split.
- After the hold-down window, the opposite cause can correct a wrong decision.
  If the opposite change does not pay back, the wrong decision stays, but it
  costs less time in each window than that change.
- Each database instance sees only its own transactions. Instances with
  different loads can make different decisions. A slow leaf CAS is visible to
  all writers of a leaf. A lost CAS is visible only to the instance that loses
  it: when one instance wins each CAS of a hot leaf, the other instance has the
  lost CAS time, and the winner has none.
- The measurements are volatile, like ADR-056 requests. A restart loses them.
- With the avoidable time policy, each database instance keeps time sums for
  its active leaves and pairs, and drops them for inactive leaves. The rule
  also keeps its moving averages for these leaves and pairs, until they decay
  to zero.
- With the size causes, the engine does not measure avoidable time, so that
  their workloads do not pay for it.
- Cold tombstones in leaves with live entries stay until a split or a merge of
  their leaf.
- The perfbench results come from a version of the rule behind a public trait,
  with the same decisions. The investigation log has the commands, and the
  experiments branch has the version that they need.

## Alternatives considered

### Keep counts of entries and bytes, and tune thresholds for each backend

For the same workload, S3 and GCS can prefer opposite topologies. Fixed
thresholds suit only one backend and one load.

### Count events with a fixed cost for each kind

This needs no timing, but the cost of one event depends on the backend. The
earlier cost model failed where a lost CAS on GCS cost more than its fixed cost.

### Let the backend report throttling

This adds a method or a result field to the `Backend` trait, which every adapter
must implement. The leaf CAS time already includes the throttle wait. Prefix
throttling is shared by all nodes of a collection, so a report of it does not
help to select a node.

### Split on each inline rejection

This is the ADR-056 rule. One rejection costs one locked commit penalty, while a
split costs several backend writes and a later merge to undo. With merges, a
split that does not pay back can alternate with a merge for adjacent misses.
One unit of time for both causes lets them compete.

### Keep the underfull threshold of leaves

A merge of leaves with live entries that no transaction or scan crosses costs
structural writes and a drained node, and saves no time.

### Compact leaves with only tombstones in place

The ADR-062 split steps can compact a leaf that holds only cold tombstones,
and keep its node. But the empty node stays, so a queue-like load still adds
leaves without a limit. The compaction also needs a new candidate cause.

### Share measurements between database instances

This needs shared state, durable or over the network. The instances that share
a hot leaf already see its slow leaf CAS, and the instance that loses a CAS
sees the lost CAS. One instance can split the leaf for all.

### Decide in the restructurer, without a rule seam

One rule in the restructurer, with its two multiples as database options,
decides as soon as a measurement pays. It gave the same decisions and the same
throughput as the same rule behind a seam in the `topology` scenario. But its
multiples become database options, and each other rule changes the
restructurer. With the seam, a rule is one type that gets a window and returns
changes.

### Make the rule seam public

A public trait lets applications write their own rules, and the experiments
used one. But the window becomes a public API, and each new measurement is
then a break. A later variant of the policy enum can expose custom rules when
the window is stable.

### Decide splits on the time of one window

A leaf can split when its split-side time in one window is more than 0.25
times the typical time of one split, with no estimate of the time that a split
adds. In the `mixed` hi mode, each collection has 8 hot keys, and two shapes
read all 8. One of them also writes all 8, so it conflicts with every other
write. After a split, the reads and locks of these transactions are in more
leaves, and the two shapes starved. The sum of the throughputs of the shapes
was 1.22 times that of the size causes on S3 and GCS, but their geometric mean
was 0.62 on S3 and 0.74 on GCS. In lo mode, this rule had about 2% more than
the moving averages on S3 and 3% more on GCS, and about 3% more in the
`topology` scenario on S3, because it splits sooner. On GCS, a warmup of 60 s
instead of 20 s removed the lo difference.

### Remember what paid for a change

A rule can keep the time that paid for each change, and ask a change to also
pay back the opposite change that it undoes. With the rule of one window, this
made fewer changes with 8 adjacent keys, but the throughput was the same within
the noise of the runs, in the `topology` and `mixed` scenarios.

### Subtract the time that a split adds to direct commits

A direct commit that lands, and whose keys a split at the median of its leaf
puts in both halves, needs a locked commit after the split. The rule can
subtract the locked commit penalty of these commits from the split-side
avoidable time. But the shapes that starve in the `mixed` hi mode seldom land
in a direct commit, before or after a split. With one database instance on S3,
this time was 0.04 ms for each transaction, against 1.43 ms of split-side
avoidable time. With four instances, it was 1.21 ms against 1.51 ms. The hot
leaf split in both.

### Estimate the time that a split adds from all divided transactions

The estimate can be the divided commits times their mean latency, or the time
of all divided commit passes. These estimates do not separate the divided
transactions that conflict from the ones that do not. In the `mixed` hi mode
on S3, before the fix of the lost CAS time, they had 0.87 to 0.92 of the size
causes, against 0.97 for the divided conflict time.

### Undo a split only after it costs time

The crossing conflict time alone, with no estimate before a split, can undo a
split that costs more than it saves. In the `mixed` hi mode on S3, the leaves
then split and merged again and again, and the transactions over them starved
after each split. The geometric mean of the shapes was 0.79 to 0.83 of the
size causes.

### Merge the leaves that transactions cross in a chain

A cross-leaf miss over a chain of linked leaves can charge each pair of the
chain, and each commit pass of a locked commit can count, also a pass that a
wound ends. This undoes a split after it costs time, instead of preventing it.
With the rule of one window, in the `mixed` hi mode on S3, the geometric mean
of the shapes was 0.65 to 0.66 of the size causes, against 0.62 without it.

### Decide all merge causes on moving averages

A rule can keep a moving average of all merge-side time of each pair, and
merge a pair when this average is more than the merge threshold plus the
averages of both leaves. In the `mixed` hi mode on S3, this made 11 merges
instead of 42, with 107 splits in both cases, and the geometric mean of the
shapes did not change. A pair has scan crossings only in some windows, so its
average stays below the time of the windows that have them. In 3 of the 4
`topology` cells with scans over leaves of 16 entries, it made 0 to 4 merges,
against 22 to 68 when the scan crossings of each window decide alone. It had 1.02 to 1.07 times
the throughput of the fixed tree, against 1.44 to 3.46.

### Split sooner

A half-life of 5 s or a split multiple of 0.025 makes a leaf split sooner.
In the `mixed` hi mode on S3 with 8 databases, the leaves then split and
merged more often, and 3 of the 4 cells with affinities 0 and 50 had 0.57 to
0.79 of the size causes.
