# ADR-110: A starving leaf writer asks other instances to yield

## Status

Accepted — implemented.

Adds the `_y` object to the objects of a collection. The lost leaf CASes of
[ADR-085](085-lost-leaf-cas-separates-the-keys-of-database-instances.md) do
not change.

## Context

A coordinator round starts its first CAS from the leaf state that the round
before it installed. When a leaf is busy, the next CAS of the database
instance goes out right after the last one lands. A writer on another database
instance must read the leaf before it can send its CAS, so it starts each race
last and can lose almost every one. In the `foreign-writers` experiment, such
a writer committed nothing for most of a run.

Only the writer that starves knows that it starves. The instance that wins
the races also loses leaf CASes to the other instance, about as many as in
ordinary false sharing, so it cannot tell a starving writer from an active
one. A gap after each lost leaf CAS ends the starvation, but the ordinary
loads with false sharing pay for it.

The contention split of ADR-085 does not help here. Both instances write
random keys of the same leaves, so the lost leaf CASes spread over many keys,
and no key becomes contended. When a hot subset of keys shares a leaf with the
keys of another instance, the split already separates them, and that writer
does not starve.

The entry "2026-11-26: a database instance starves a foreign writer of its
leaves" in [`hack/perf/investigations.md`](../../hack/perf/investigations.md)
has the measurements of this decision.

## Decision

A round whose leaf CAS keeps failing, on a leaf where the CASes of its
instance seldom land, writes a yield request of its database instance for the
collection. An instance that keeps a leaf of the collection busy reads the
requests. While a request of another instance lasts, its rounds of a leaf
leave the requested gap between them.

### A round whose leaf CAS keeps failing asks for a gap

When a key change of another writer rejects many leaf CASes in a row of one
round, the instance writes its yield request for the collection of the leaf.
The coordinator knows the cause of a rejection at the next load of the round,
as for a lost leaf CAS (ADR-085). A change of the node itself, such as a
split, shows no other writer, so it starts the count again.

A long streak alone also happens in a fair race between a few instances on the
same leaves. So the instance also keeps the outcomes of its recent leaf CASes
on each leaf, landed or rejected by another writer, and writes the request
only when a small share of them landed. A leaf without recorded outcomes
counts as starved, because a writer of random keys seldom comes back to the
same leaf. The round writes the request during its retry backoff, so that the
write does not delay its next CAS. The request holds:

- A random token of the database instance, chosen when the leaf coordinator
  starts.
- An expiry a few seconds after the write.
- A gap that is a little longer than the last leaf load and CAS of the round,
  within fixed limits.

A later such round renews the request, at most once in a renewal interval. An
instance never withdraws its request. The request expires.

### The requests of a collection are one object

The requests of a collection are in the object `_y` under the collection
prefix, sorted by instance, with at most one request for each instance. A
writer replaces its own request and drops the expired ones in the same
conditional write. A drop of the collection deletes the object with the
nodes, before the root. Any instance can create the object at any time, so the
drop reads it after the current barrier, not from the cache.

### An instance reads the requests only while it keeps a leaf busy

Two rounds of a leaf run back to back when the second one starts right after
the first one ends. When the rounds of a leaf have run back to back for a poll
interval, the instance reads the requests of the collection, at most once in
each interval. The read runs at the same time as a round, because it decides
only the later rounds.

An instance whose rounds of a leaf do not run back to back reads no request.
Its rounds already leave gaps for other writers.

### Rounds leave the gap while a request of another instance lasts

While a request of another instance has not expired, a round of a leaf of the
collection waits before its batching window until the gap has passed since
the last round of the leaf ended. The instance ignores its own request. It
yields until the read request expires, and never longer than the lifetime of
a request after the read.

## Limits

- Correctness does not depend on yield requests. A request changes only when
  rounds start.
- Clock skew changes only how long an instance yields, within the lifetime of
  a request.
- A drop that runs at the same time as a starving writer can leave an `_y`
  object behind, as a late create of a node can.
- Expired requests stay in the object until a later write or the drop removes
  them.

## Consequences

- A writer on another instance starves on a busy leaf much less, and the
  instance of the load commits more than before, because its rounds carry
  larger batches. The starvation can come back for a while after a yield
  ends: the writer landed CASes during the yield, so it requests again only
  after later rejections push those landings out of its recent outcomes.
- An instance reads the requests of each collection whose leaf it keeps busy,
  also when no other instance exists. The reads are a small share of the
  backend operations of a busy instance, and latency and commits do not change.
- A request that a round writes without starvation slows the rounds of the
  whole collection on the other instances while it lasts. In the measured
  loads with low contention, with contention that a split resolves, and with
  no contention, few rounds reach the threshold. Latency and commits stay
  within the noise of each experiment, except beside hot writers of other
  instances. There, a writer of a busy leaf lands few of its CASes and writes
  requests, and the hot writers get a little slower while they yield.
- A fair race between a few instances on the same hot keys writes few
  requests, because each instance lands its share of the leaf CASes.
- When two instances starve each other, both write requests and both yield.
- When two instances write the same hot keys, a writer whose CASes seldom
  land writes requests, and the other instance yields. A transaction conflict
  on the same keys looks the same to a round as a starvation, so no threshold
  separates the two cases. A yielding instance holds its locks longer, and a
  light writer on the other instance waits for them, so that writer can get
  slower and commit less often.

## Alternatives considered

### A gap after each lost leaf CAS

After any lost leaf CAS, the instance leaves a gap between its rounds of each
leaf for a short time, and each round loads its leaf fresh. A fresh load that
finds a change of another writer counts as a lost CAS, so that the contention
split still sees the other instances. This ends the starvation. But the
winning instance loses about as many leaf CASes beside an active writer of
another instance as beside a starving one, so ordinary false sharing turns
the gap on too, and the ordinary loads pay for it, most on a busy leaf beside
writers of other instances.

Narrower rules on the same side did not separate the two cases either:

- A gap only on the leaf that lost a CAS misses the starving writer. Its
  rounds go to other leaves, and the losses that it causes are rare.
- A gap only for rounds that start right after the last round of their leaf
  still slows a busy leaf beside writers of other instances.
- A fresh load without a gap does not end the starvation.

So the signal must come from the writer that starves. The section "The full
candidate" of the investigation entry has the measurements.

### Faster retries of the starving writer

A shorter backoff does not help. The writer must read the leaf before its
CAS, and the other instance sends its next CAS right after the last one lands.

### A request for each leaf

A request that names one leaf leaves the other leaves of the transaction, and
its later rounds on the same leaf, to their own detection, so a transaction
with keys in several leaves still starves.

### A read of the requests after each lost leaf CAS

The instance that wins the races loses a leaf CAS only when the starving
writer lands one, which is rare. It almost never read the requests, and the
writer still starved.

### One object for each database instance

Each instance writes its own object under a database prefix. The readers must
list the prefix and read each object, the instances need an identity, and the
objects of an instance that stops stay behind. One object for each collection
needs one read, and the drop removes it.

### The requests in the collection record or in a node

The collection record holds directory locks and topology participants, and the
split that serves ordinary contention writes it. A request there would race
with that split and follow the rules of the record. A node is the object that
the starving writer cannot write.

### A streak of rejections alone

A request after the streak alone, without the share of landed CASes, also
comes from a fair race between several instances on the leaves of a few hot
keys. Many instances wrote requests with no starving writer, all instances
yielded to each other, and the whole load got slower.

### No yield for an instance that requests itself

An instance with its own request would not yield. This removes the cost of
the fair race, but when two instances starve each other, both request, so
neither yields and both starve again.

### A share only over a full history

A request only after a full history of outcomes on the leaf misses a writer
of random keys. It comes back to the same leaf too seldom to fill the history.

### A higher threshold

A higher threshold gives fewer requests when two instances write the same hot
keys. But two instances that both carry a load then have too few long streaks
to keep their requests alive, and they starve each other again. Both cases
give the same signal, so the threshold only moves the cost between them.

### A wait only on rounds that start right after the last one

A yielding instance waits only on rounds that would otherwise run back to
back. The light writer on the same hot keys still got slower.

### Smaller leaves

Smaller leaves give fewer rounds to each leaf, but they give up the batching
that moderate leaves give (ADR-084). No key becomes contended, so a contention
split does not start, and a split of the starved leaf needs a CAS on that
leaf.
