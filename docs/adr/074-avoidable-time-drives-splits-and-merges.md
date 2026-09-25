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
- **Queue wait.** A round member waits for an earlier coordinator round of the
  same leaf. A transaction counts only its longest wait, because it waits for
  its leaves in parallel.
- **Slow leaf CAS.** The time of a leaf CAS above the typical leaf CAS time of the
  database instance. This includes the time that the backend adapter retries
  throttled requests, for example for the GCS limit on writes to one object.
- **Inline pressure.** An aggregate inline rejection of ADR-056. Its time is the
  locked commit penalty: the typical time of a locked commit minus the typical
  time of a direct commit, as the database instance measures them.

Merge causes of two adjacent leaves:

- **Adjacent cross-leaf miss.** A direct commit candidate has its keys in these
  two leaves only, so it uses a locked commit. Its time is the locked commit
  penalty.
- **Scan crossing.** A scan continues from the left leaf into the right leaf.
  Its time is the time of the extra leaf read.

These are not causes, because no single split or merge removes them:

- A lost CAS on the same keys. A split cannot separate them.
- A cross-leaf miss whose keys are in more than two leaves.
- S3 throttling of a prefix. All nodes of a collection share one prefix.

### Decide with one rule

A leaf becomes a split candidate when its split-side avoidable time in the last
window is more than the typical time of one split.

Two adjacent leaves become a merge candidate when their merge-side avoidable
time in the last window is more than the typical time of one merge, plus the
split-side avoidable time of both leaves.

The database instance measures the typical time of a split and of a merge. A
default applies until it measured one. The window is local to each database
instance, like the soft thresholds (ADR-072).

A node that took part in a structural change does not become a candidate of the
other kind during the next window, the hold-down window. With the ADR-073 merge
vetoes, this stops nodes from splitting and merging again and again.

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
throttled requests, so this time includes throttling. The GCS adapter retries
throttled requests with the same retry budget as the S3 adapter.

## Consequences

- The topology follows the measured cost. One policy suits S3 and GCS, without
  thresholds for each backend.
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
