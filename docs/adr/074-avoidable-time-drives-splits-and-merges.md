# ADR-074: Avoidable time drives splits and merges

## Status

Proposed.

Refines [ADR-073](073-merge-nodes-into-right-sibling.md)'s maintenance policy.
Leaves get demand causes for splits and merges, and lose the underfull cause.
The candidate queue, the merge vetoes, and the split and merge protocols do not
change.

Refines [ADR-056](056-demand-driven-inline-pressure-splits.md). An aggregate
inline rejection adds avoidable time to its leaf. It does not request a split
alone.

Refines [ADR-062](062-splitter-driven-tombstone-reclamation.md). A leaf that
holds only cold tombstones is compacted in place, instead of merged.

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
  CAS for other keys landed first. The time is from the failed CAS to the CAS
  that lands. A split can put the keys in different leaves.
- **Queue wait for other keys.** A round member waits for an earlier
  coordinator round of the same leaf, and that round has none of the keys of
  the member. A transaction counts only its longest wait, because it waits for
  its leaves in parallel. A split can put the keys in different leaves.
- **Slow leaf CAS.** The time of a leaf CAS above the typical leaf CAS time of the
  database instance. This includes the time that the backend adapter retries
  throttled requests, for example for the GCS limit on writes to one object.
- **Inline pressure.** An aggregate inline rejection of ADR-056. Its time is the
  locked commit penalty: the typical time of a locked commit minus the typical
  time of a direct commit, as the database instance measures them.

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
  Its time is the time of the extra leaf read.

These are not causes, because no single split or merge removes them:

- A lost CAS on the same keys. A split cannot separate them.
- A queue wait for an earlier round that has one or more keys of the member.
  A split cannot separate them.
- A lost CAS, a queue wait, or a slow leaf CAS whose keys a split at the median
  key does not separate.
- A cross-leaf miss whose keys are in more than two leaves.
- S3 throttling of a prefix. All nodes of a collection share one prefix.

### Decide in a topology policy

A topology policy decides the splits and merges of leaves. It is a public
trait. Once in each window of one second, the engine gives the policy the
measurements of the window: the avoidable time, the split key, and the size of
each active leaf, the avoidable time of each pair of adjacent leaves, and the
typical times of a split and of a merge. The policy returns the changes that it
wants: a split at the median, a split at a key, or a merge into the right
sibling.

The engine checks each change again against current state. It skips a split
of a leaf with less than two entries, a split at a key that leaves one half
empty, a merge of the root, and a merge that the ADR-073 merge rules do not
allow, except the underfull threshold. So a policy cannot make the tree
unsafe. The engine still decides the hard cap splits and the index nodes.
Without a policy, the size causes of ADR-031, ADR-056, and ADR-073 decide
leaves, as before this ADR.

The avoidable time policy uses one rule. A leaf becomes a split candidate when
its split-side avoidable time in the window is more than 0.25 times the
typical time of one split. Two adjacent leaves become a merge candidate when
their merge-side avoidable time in the window is more than 0.1 times the
typical time of one merge, plus the split-side avoidable time of both leaves.
A leaf over a soft cap also splits, at the median.

The multiples are less than one, because a change keeps its effect after the
window that paid for it, and each database instance sees only its own part of
the time. Multiples of one did not merge leaves of 16 entries under scans of 64
keys, which a merge made 3 times faster.

The database instance measures the typical time of a split and of a merge. A
default applies until it measured one. The window is local to each database
instance, like the soft thresholds (ADR-072).

A leaf that a split wrote in this window or the last one does not merge, and a
leaf that a merge wrote in this window or the last one does not split for
avoidable time. This is the hold-down window. With the ADR-073 merge vetoes, it
stops leaves from splitting and merging again and again in each window.

### Keep the size causes

A capacity hint (ADR-072) still forces a split when a change does not fit in the
hard cap. The soft caps on entries and encoded bytes stay as split causes and as
merge vetoes. Below the
soft caps, the measurements show no time cost, so the soft caps limit the size
of one leaf CAS and cost no throughput. On a backend with a per-byte cost, a
large leaf shows as slow leaf CAS.

### Remove the underfull cause of leaves

A leaf with few live entries costs time only when transactions or scans cross
its boundaries. The merge causes measure this time. Index nodes keep the
underfull cause, because transactions do not write them.

A leaf that holds only cold tombstones becomes a compaction candidate. The
worker uses the ADR-062 split steps: it gates and compacts the leaf, finds no
split need, and stores the compacted leaf.

### Measure in the engine

The leaf coordinator measures the time of each leaf CAS. Backend adapters retry
throttled requests, so this time includes throttling. The leaf coordinator also
measures the queue wait of each round member, because only it knows the keys of
the member and of the earlier round. The GCS adapter retries
throttled requests with the same retry budget as the S3 adapter.

## Consequences

- The topology follows the measured cost. One policy suits S3 and GCS, without
  thresholds for each backend. In the perfbench `topology` scenario, seeded
  with leaves of 16 or 128 entries, the avoidable time policy had 1.18 to 1.23
  times the throughput of the fixed seeded tree on S3, and 1.30 to 1.36 times
  on GCS. This is the geometric mean over 20 cells, in each of 4 runs. The
  worst cell was 0.85 to 0.92 of the fixed tree on S3, and 0.79 to 0.85 on GCS.
- In the perfbench `mixed` scenario with 5000 keys for each collection, the
  policy had 1.16 times the throughput of the size causes on S3, and 1.27
  times on GCS. This is the geometric mean of the four transaction shapes, over
  1 to 8 database instances and 0 to 100% collection affinity. The gain was
  about the same when the instances shared all collections as when each
  instance used only its own collection. It was smaller with more instances,
  because the same number of workers then had less contention on each
  collection.
- The split rule does not count the time that a split adds to the direct
  commit candidates with keys in both halves. A cross-leaf miss over more than
  two leaves is not a merge cause either. So a hot leaf can split under
  transactions that write all of its keys. In the `mixed` hi mode, each
  collection has 8 hot keys, and one shape writes all 8. The leaf split, the
  multi-key transactions used locked commits and starved, and the geometric
  mean of the shapes was 0.62 of the size causes on S3 and 0.75 on GCS.
- Each database instance sees about one part in N of the time of a leaf that N
  instances share. So with more instances, a change needs more time in total.
  With 4 instances, leaves of 16 entries under scans merged only with a merge
  multiple of 0.1.
- After a split, the time that the split removed is not measured again. A merge
  for adjacent misses can then undo the split, and the next window can split
  the leaf again. With 8 adjacent keys in each transaction and leaves of 128
  entries, the policy made about 20 splits and 20 merges before the
  measurement and about 10 of each during it, and had 0.92 of the throughput of
  the fixed tree on S3.
- A tree does not change under a load that its topology does not slow down.
  After deletes, cold leaves with few entries stay until transactions or scans
  cross them. Fewer merges also leave fewer drained nodes.
- The first inline rejections of a leaf use the locked commit. The leaf splits
  only after enough rejections to pay for the split.
- After the hold-down window, the opposite cause can correct a wrong decision.
  If the opposite change does not pay back, the wrong decision stays, but it
  costs less time in each window than that change.
- Each database instance sees only its own transactions. Instances with
  different loads can make different decisions. Lost CAS and slow leaf CAS are
  visible to all writers of a leaf, so the instances that share a hot leaf agree
  on its split causes.
- The measurements are volatile, like ADR-056 requests. A restart loses them.
- Each database instance keeps time sums for its active leaves and pairs, and
  drops them for inactive leaves.
- Cold tombstones in leaves that are not all tombstones stay until a split,
  a merge, or a compaction in place of their leaf.

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

### Keep the underfull cause of leaves

A merge of leaves that no transaction or scan crosses costs structural writes
and a drained node, and saves no time.

### Share measurements between database instances

This needs shared state, durable or over the network. The instances that share
a hot leaf already see its lost CAS and slow leaf CAS.

### Decide in the engine, without a policy seam

One rule in the restructurer, with its two multiples as database options, has a
smaller public API and decides as soon as a measurement pays. It gave the same
decisions and the same throughput as the avoidable time policy in the
`topology` scenario. But each other rule needs a change of the engine, and the
engine must change its default. With the seam, a rule is a type outside the
engine, and the default of the engine does not change.

### Remember what paid for a change

A policy can keep the time that paid for each change, and ask a change to also
pay back the opposite change that it undoes. This made fewer changes with 8
adjacent keys, but the throughput was the same as the avoidable time policy
within the noise of the runs, in the `topology` and `mixed` scenarios.

### Multiples of 0.1 for splits and merges

On GCS, this had 1.36 times the throughput of the fixed tree, against 1.32 for
the default. On S3 it had the same geometric mean, but its worst cell was 0.84
of the fixed tree, against 0.92, because it split and merged more often. S3 has
priority when the two backends do not agree.
