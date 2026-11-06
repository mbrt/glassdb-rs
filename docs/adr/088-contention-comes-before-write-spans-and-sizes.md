# ADR-088: Contention comes before write spans and leaf sizes

## Status

Accepted.

Refines [ADR-085](085-lost-leaf-cas-separates-the-keys-of-database-instances.md)
and [ADR-084](084-size-causes-are-the-default-topology-policy.md). It replaces
the merge condition of ADR-085, and the rule of ADR-085 that a contention split
divides no write span. It also applies the merge condition to the merges for
write spans of ADR-084.

## Context

The writers policy has three causes, and each has its own goal:

- **Contention** (ADR-085): split a leaf to separate a contended key from the
  keys of other database instances, so that the database instances lose fewer
  leaf CASes.
- **Write spans** (ADR-084): keep the keys that a transaction commits together
  in one leaf, so that the commit is a direct commit.
- **Size** (ADR-073, ADR-084): merge underfull leaves.

The splits and the merges do not order these causes in the same way:

- A contention split never divides a write span. So the split puts write spans
  first.
- A merge for a write span does not read the writers of the leaves. So it puts
  write spans first too.
- An underfull merge waits only when this database instance has no local writer
  in one of the two leaves. It assumes that one database instance writes each
  leaf.

A leaf can have recent local and recent foreign writers together. For example,
database instance A writes the keys 0 and 4 to 7, and three other database
instances each write one of the keys 1 to 3. A contention split makes the leaves
`[0] | [1..7]`. A sees a local writer in each leaf, so its next write of key 0
merges them. The merge puts key 0 back with the keys 1 to 3, while the other
database instances still write them. The size cause then undoes the contention
cause. The integration test `the_keys_of_database_instances_split_apart` failed
in about one run of three because of this. `false-sharing` did not show it,
because its hot database instances write only their own keys.

## Decision

The causes have one order, from first to last: contention, write spans, size. A
decision for a cause never undoes a boundary that a cause before it keeps.

### Contention splits can divide write spans

A contention split chooses its split key as in ADR-085, but a write span does
not stop it. Of the edges of the run of the contended key, the first one that
divides no write span goes first. When each edge divides a write span, the split
occurs at the first edge.

### Merges keep the writers of database instances apart

A database instance merges two leaves only when one of these is true:

- One of the leaves has no live value of a recent writer.
- All the recent writers of live values in the two leaves are local.

The rule applies to underfull merges and to merges for write spans. The other
merge rules of ADR-073 and ADR-084 do not change.

So a merge never puts a live value of a recent foreign writer and a live value
of a recent local writer in one leaf. When the foreign writers of a leaf stop
being recent, the database instance that still writes it sees only local
writers, and it merges the leaf.

The lost leaf CASes that pay for a contention split stop after the split. The
merge rule therefore reads the writers of the leaves, which stay after the
split, and not the lost leaf CASes.

## Consequences

- Leaves do not merge while several database instances write them, so these
  leaves do not split and merge again while the load stays the same.
- A transaction that writes keys on both sides of a contended key can need a
  locked commit in place of a direct commit, while the contention continues.
- Under a load where several database instances write the same keys, an
  underfull leaf can stay underfull until all but one of its writers stop being
  recent.
- The merge veto reads only the writers of the leaves. It cannot tell a boundary
  that a contention split made from a boundary that a size split made. A write
  span over a boundary of a size split also waits, when another database
  instance recently wrote a key of one of the two leaves, even if the database
  instances lose no leaf CASes there. In `fixed-ranges` with 5 database
  instances, this cost a few percent of the throughput, and more writes for
  each transaction. While the leaves are apart, nothing shows the lost leaf
  CASes that a merge would cause, so this decision accepts the cost.
- In `false-sharing-spans`, a write span crosses four contended keys. The hot
  writers and the other writers of the leaf commit several times as many
  transactions as when write spans come first. The writer of the span needs
  locked commits, and it commits fewer transactions on average.
- The other measured loads did not change by more than the noise of the runs.

The entry "2026-11-06: contention comes before write spans and leaf sizes" in
[`hack/perf/investigations.md`](../../hack/perf/investigations.md) has the
measurements.
- Tombstones still count as no recent writer, so the leaves of deleted keys
  merge as before.
- The soft cap split still divides no write span. It is a size cause.

## Alternatives considered

### Mark the boundaries of contention splits

A contention split could mark its boundary in the leaf, and the merge veto could
apply only to marked boundaries. The write spans over the boundaries of size
splits would then merge, as before. This is not the boundary lease of ADR-085:
the writers of the leaves keep the mark, so it needs no renewal from lost leaf
CASes. But it needs a new field in the stored leaves, and a write span over a
marked boundary still waits. It was not tried.

### Write spans before contention

The contention split and the merge for a write span already put write spans
first, so only the underfull merge would change. But lost leaf CASes delay every
writer of the leaf for as long as the database instances write it, and a divided
write span costs a locked commit only for the transactions that write both
sides. A transaction that spans a contended key keeps the key with the keys of
other database instances. This order was measured (see the investigation
entry). In `false-sharing-spans`, the contended keys did not split apart, and
the throughput of all shapes was lower than with contention first. In
`fixed-ranges` with 5 database instances, it was several percent higher.

### Weigh the causes by their cost

One measure could compare the time of lost leaf CASes with the time that direct
commits save. The avoidable time policy of ADR-074 was such a measure, and
ADR-086 removed it because it did not settle.

### Veto only at the boundary of the two leaves

The merge could wait only when the last entry of the left leaf and the first
entry of the right leaf have a local and a foreign writer. This vetoes fewer
merges. But a foreign key that is not at the boundary joins the local keys of
the other leaf.

### Veto from the contended keys of the database instance

The merge could wait while a leaf holds a contended key of the database
instance. In the example, A is not the instance that loses leaf CASes, so it
merges. ADR-085 also rejects vetoes from current lost leaf CASes, because the
split removes them.
