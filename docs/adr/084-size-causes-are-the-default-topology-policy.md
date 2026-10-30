# ADR-084: The size causes are the default topology policy

## Status

Accepted — implemented.

Refined by [ADR-085](085-lost-leaf-cas-separates-the-keys-of-database-instances.md).

Supersedes [ADR-075](075-avoidable-time-is-the-default-topology-policy.md).
The rule of the avoidable time policy of
[ADR-074](074-avoidable-time-drives-splits-and-merges.md) does not change, but
the new default soft cap of leaf entries applies to it.

Refines [ADR-073](073-merge-nodes-into-right-sibling.md). With the size
causes, write spans move soft cap splits and cause merges.

## Context

With the avoidable time policy, the queue wait of one database instance pays
for most splits. Under sustained load, the rule keeps splitting and the tree
does not settle. The leaves get so small that a coordinator round has few
members, and the transactions write more.

The investigations in
[`hack/perf/investigations.md`](../../hack/perf/investigations.md) found why:

- A split does remove queue wait. But this is a small part of all the queue
  wait under load, and each split also divides the rounds of its leaf, which
  loses batching.
- Neither a price for each backend write, nor an estimate of the rounds that a
  split adds, told the useful splits from the others.
- The size causes keep leaves of moderate size. They did as well as the
  avoidable time policy on every measured load, and better on the loads with
  sustained contention. They also settle.
- Smaller leaves gain on some loads. They lose on loads where several database
  instances share a collection.

The size causes leave one gap. A leaf boundary can divide keys that
transactions write together, for example after a split at the median during a
bulk load. Nothing moves such a boundary. Larger leaves hide the gap, but they
slow the transactions on single keys.

## Decision

The size causes are the default topology policy. The defaults of the soft cap
of leaf entries and of the underfull threshold of leaves are smaller, for
moderate leaves. The byte soft cap, the hard caps, and the thresholds of index
nodes do not change. The values are in `NodeSizePolicy`.

The avoidable time policy stays a policy that a database instance can select.

### Keep the keys that transactions commit together in one leaf

Each committed entry of a leaf names the transaction that wrote it. With the
size causes, the entries of one recent writer of two or more of them, in a
leaf or in two adjacent leaves, are a write span. The decisions read the write
spans from the leaves, so every database instance sees the same ones, and none
keeps state.

Only recent writers count. A leaf knows only the last writer of each key, so
one transaction that wrote many keys at once, such as a bulk load, would
otherwise hold them together until other transactions write each of them. The
leaves of the whole range would then grow. Keys that transactions keep writing
together get a new writer at each commit, so their write spans stay recent.
The recency threshold is a constant in the code. A later change can make it a
setting.

- **Split.** A soft cap split of a leaf that is not the root is at the median
  of the leaf, unless the median divides a write span. Then the split is at
  the nearest key near the middle of the leaf that divides none, or it waits.
  A wait has a bound: a leaf that grows well past the soft cap splits at the
  key that divides the fewest write spans. A leaf with tombstones to reclaim
  does not wait either (ADR-062). Root leaves, capacity splits, and inline
  pressure splits do not change.
- **Merge.** A commit whose written keys are in two adjacent leaves asks for
  their merge. The leaves merge when all of these conditions are true:
  - The write spans over their boundary are a large share of the write spans
    of the two leaves.
  - The merged leaf is within a bounded multiple of the soft cap, and within
    the byte and inline budgets. The other merge rules of ADR-073 allow the
    merge.
  - When the merged leaf is over the soft cap, a split key near its middle
    divides no write span.

The first and the last condition each stop a change that does not pay. Without
the first, transactions on random keys merge leaves, because a few of them
cross each boundary, and the next split divides the other transactions again.
Without the last, a merged leaf whose write spans cover its middle would split
through them again when it grows.

## Consequences

- A database instance that does not select a policy takes no measurements of
  avoidable time.
- The soft thresholds are local to each database instance. An instance that
  opens an existing database splits the large leaves that it writes. Leaves
  that it only reads keep their size.
- Database instances with different thresholds can change a leaf again and
  again, for example during an upgrade. To avoid this, the instances can
  select the previous thresholds until all of them run the new version.
- An instance that selects the avoidable time policy now also uses the smaller
  soft cap. The measurements of ADR-074 to ADR-076 used larger leaves.
- Keys that several database instances write in one leaf stay in one leaf, and
  lose leaf CASes to each other. The size causes do not separate them.
- Transactions on fixed sets of keys gain, because their keys end up in one
  leaf. Transactions on single keys keep their throughput. The investigation
  entry of 2026-10-28 has the measurements.
- A range of keys wider than the leaves that the soft cap allows stays
  divided.
- A write span covers all keys between its first and its last key. A
  transaction that writes a few keys far apart keeps the keys between them in
  one leaf too.
- A leaf knows only the last writer of each key. A later write of a key by
  another transaction shortens or removes the write span.
- The start of the writer is the start of its transaction. A transaction that
  ran for a long time, or that renewed its identity, counts as older. The
  clocks of database instances differ, so the recency check accepts a small
  skew.
- After a bulk write, the soft cap splits of its leaves wait while its writer
  is recent. The leaves can grow up to the bound of the wait. A waiting split
  comes back at the next write of its leaf, so a leaf that nobody writes again
  keeps its size. A benchmark that seeds in large transactions sees a burst of
  splits after the seed.
- A merge needs a commit of a database instance over the boundary, and the
  commit must have read its written keys with point reads, so that it observed
  their leaves. The entries of that commit often do not name it yet, so in
  practice the same keys must be written together more than once.
- While a split waits, each write of the leaf asks for the split again, and the
  restructurer reads the cached leaf each time.
- Scans, and the GCS delay model, were not measured.

## Alternatives considered

### Keep the avoidable time policy as the default

Under sustained load, it splits leaves below the size that the size causes
keep, writes more, and does not settle. A price for writes, a correction of the
queue wait, and an estimate of the rounds that a split adds did not fix this
without the loss of useful splits.

### A different soft cap

Smaller leaves gained throughput on loads without sharing. They lost it on
loads where several database instances share a collection, and wrote more.
Larger leaves divide fewer sets of keys that transactions write together, but
they slow the transactions on single keys. The write spans keep these sets
together at a moderate soft cap.

### Lost CAS splits below the underfull threshold

The rule of ADR-074 with only lost CAS as a split cause, a split key between
the keys that conflict, and a merge veto, separated the hot keys of the load
with several database instances on adjacent keys. But lost CAS that is spread
over a shared collection also paid for splits below the threshold. These
splits cost throughput and writes, and no measured rate of lost CAS told the
two loads apart.

### Prices for divided transactions

A merge or a split could pay with the time of the transactions that a boundary
divides, as the avoidable time rule does. The write spans need no price. The
share of the write spans over a boundary, and a split key that divides no
write span, told the measured loads apart.

### Keep the write spans of commits in each database instance

Each database instance could record the write spans of its own commits. It
would then see only its own spans, and another instance could split a merged
leaf through them. It would also need bounded state for every collection. The
entries of the leaves show the write spans of all instances without state.

### A soft cap in bytes only

The leaves of the measured loads had small values, so their entries and their
bytes grew together. The byte soft cap stays as the second cap. A band in bytes
only needs an underfull threshold in bytes, which the size causes do not have.
