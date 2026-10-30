# ADR-085: Lost leaf CASes separate the keys of database instances

## Status

Accepted — implemented.

Refines [ADR-084](084-size-causes-are-the-default-topology-policy.md). The size
causes stay the default topology policy, and this decision adds a split cause
and a merge condition to them.

## Context

With the size causes, keys that several database instances write in one leaf
stay in one leaf. The rounds of different database instances cannot share a
leaf CAS, so the instances lose leaf CASes to each other. In the `false-sharing`
experiment, four database instances each write one of four adjacent keys. Each
writer commits about 2 transactions per second with the size causes, and about
4.3 with one key in each leaf.

The rounds of one database instance share a leaf CAS. Moderate leaves keep
this batching, which is why the size causes are the default. So a split pays
only when it separates keys of different database instances. A leaf CAS that
one database instance loses to another is the direct evidence of that.

An earlier attempt used lost leaf CASes as a split cause of the avoidable time
rule, below the underfull threshold (see the alternatives of ADR-084). It
separated the hot keys. But it also split leaves under random load, and the
underfull threshold merged them again, so the tree did not settle. Two gaps
caused this:

- No rate of lost time for each leaf told false sharing from random load.
- A split removes the lost leaf CASes that paid for it. After the split, no
  database instance has evidence for the boundary, and the underfull merge
  undoes it.

The entry "2026-10-30: lost leaf CASes separate the keys of database
instances" in
[`hack/perf/investigations.md`](../../hack/perf/investigations.md) has the
measurements of this decision.

## Decision

With the size causes, a database instance splits a leaf to separate a key on
which it loses leaf CASes to other database instances. It also merges a leaf
only when it wrote recently live values of both leaves of the merge, or when
one of them has no live value of a recent writer.

### Find the keys on which a database instance loses leaf CASes

The leaf coordinator records a lost leaf CAS with every topology policy. A
rejected leaf CAS counts when the CAS that landed changed only keys that the
round does not access. The coordinator compares the two leaf states at the
next load of the round, so a leaf CAS that lands costs nothing more.

Each lost leaf CAS gives each key that the round accesses an equal share of one
loss. A database instance keeps a decayed sum of the shares of each key. A key
whose sum is above a threshold is contended for that database instance.

The shares, and not a count for each key, tell false sharing from random load.
A round of a busy database instance batches many keys, so a count gives each of
them the full loss. In the false sharing load, a round of a hot writer has one
key, and its share is the full loss.

### Know the writers of this database instance

The monitor of each database instance keeps a bounded set of the transaction
identities of its recent owner operations. Each owner operation adds its
identity before it can publish a leaf write, so that the entries of a local
writer never show a foreign one. Read-only identities take room in the set too,
but no leaf entry names them as its writer. A writer of a leaf entry is local
when its identity is in the set, and foreign when it is not. As with write spans, only
recent writers count. Only the writers of live values count too: a contended
key has a live value, because database instances keep writing it, and a
tombstone holds no key apart. The set must hold the identities of the recency
window, because an older local writer would count as foreign.

The set keeps no durable state, and transaction identities do not change.

### Split a contended key away from foreign keys

For a contended key, the database instance asks for a split of its leaf. The
split key is at the edge of the run of keys around the contended key that
have no live value of a recent foreign writer. The contended key is in the run
whoever wrote it last: a database instance that loses every leaf CAS on the key
has not written it yet. The first split is on
the side with more keys outside the run. The split key divides no write span,
or the database instance does not ask for the split.

A run can become its own leaf after two splits. This leaf can have fewer live
entries than the underfull threshold. Only a database instance that loses leaf
CASes asks for this split. A database instance that writes a leaf alone, or
that reads it without locks, never loses one. A database instance that only
reads the key with locks can ask for it, but the split occurs only when other
database instances wrote the keys around the key.

### Merge only leaves that this database instance writes

An underfull merge does not occur when either of its two leaves has live
values of recent writers, and none of these writers is local. The other merge rules of ADR-073 and
ADR-084 do not change.

The database instance that checks a merge decides only for the leaves that it
writes. A database instance that writes neither leaf leaves the merge to their
writers. When the writers of the two leaves are different database instances,
each of them sees a foreign leaf, and the leaves stay apart. When one database
instance writes both leaves, it sees two local leaves, and the size causes
merge them. The leaves of keys that database instances delete merge as before,
because their tombstones count as no recent writer.

A database instance asks for an underfull merge when its write of a leaf
lands, as before. When the writers of a leaf stop, their writers stop being
recent after the recency window. The next write of the leaf then asks for the
merge again, and the size causes merge the leaf. A leaf that nobody writes
again keeps its size, as a waiting split of ADR-084 does.

The check reads the two leaves when the merge runs, not when the database
instance asked for it. A database instance can ask for the merge of a leaf at a
path while the leaf holds its keys, and a split of another database instance
can move those keys away before the merge runs.

## Consequences

- In the `false-sharing` experiment, each hot writer commits about 8
  transactions per second, which is about four times the size causes alone and
  about twice one key in each leaf. The keys next to the hot keys stay in
  moderate leaves, so their rounds keep their batching. The investigation entry
  has the measurements of `false-sharing`, `load-shift`, and `workers`.
- The leaves of contended keys can be below the underfull threshold. The
  transactions of the other keys can then write more leaves, so writes for each
  transaction rise. In `false-sharing`, they rose from 0.32 to 0.56.
- A key that only one database instance writes never splits away for this
  cause, even when it is hot. Its rounds keep their batching.
- Each database instance decides from the same leaf entries, but it judges
  their writers with its own set. Two database instances can therefore see the
  same leaf differently. A database instance that is not a writer of a leaf
  never merges it. A wrong decision only keeps two leaves apart until their
  writers stop being recent, and the next write of a leaf after that.
- Lost leaf CASes are measured in each database instance. A database instance
  that loses leaf CASes at a lower rate than the threshold does not split.
- Each write of an underfull leaf asks again for its merge. While the leaves of
  a merge stay apart, the restructurer reads the cached leaves again for each of
  these requests. This costs CPU, and no backend operation.
- A merge can wait for longer than before, because a database instance must be
  a writer of both leaves. A leaf whose live values only other database
  instances wrote waits for a write of one of them, or for its writers to stop
  being recent and a later write of the leaf.
- The recency of a writer is the start of its transaction, as for write spans.
  The clocks of database instances differ, so this decision has the same skew
  limits as ADR-084.
- Scans, the GCS delay model, and loads with more than five database instances
  were not measured.

## Alternatives considered

### The database instance in the transaction identity

Two bytes of the random prefix of each transaction identity could name the
database instance that made it. Every database instance would then judge the
writers of a leaf in the same way. The prototype measured the same results as
this decision. But it changes the format of transaction identities, two
database instances can have the same tag, and the random prefix that spreads
the transaction records over backend partitions gets shorter. The set of local
writers gives an exact answer for the decisions of each database instance,
without these costs.

### A merge condition on the target leaf only

A merge could wait only when its target leaf has recent foreign writers and no
local writer. This works when the database instance that checks a merge has
just written the source leaf. A merge request can be stale, so in the
prototype, database instances merged hot leaves that they did not write. In
two runs of three, the slowest hot writer fell to 3.7 and to 5.0 transactions
per second.

### A lease on the boundary

The split could write a time into the leaf until which no merge removes the
boundary. Every database instance would see the lease. But it needs a new field
in the leaf, and its renewal needs the evidence that the split removes. Without
a renewal, the leaves merge and split again in each lease.

### A merge veto from the current lost leaf CASes

A database instance could stop a merge while it loses leaf CASes on the leaf.
After the split, it loses none, so the veto ends and the leaves merge again.
This is the cycle of the earlier attempt.

### A count of lost leaf CASes for each key

A count gave the keys of random load values within a factor of two of the hot
keys, because a round of a busy database instance has many keys.

### Hot keys without lost leaf CASes

A split for each key that a database instance writes often would also separate
the keys of one database instance, whose rounds batch. It loses the batching
that moderate leaves keep.
