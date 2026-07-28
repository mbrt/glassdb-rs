# ADR-040: Snapshot history retention

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md).

On acceptance, this supersedes ADR-022's current-reference-only liveness rule
for committed values and its deletion of outcome evidence needed to fence
delayed artifacts. Pending-lock reclamation remains, extended by
[ADR-038](038-hlc-snapshot-cuts.md)'s commit-age bound.

Because [ADR-039](039-timestamp-versioned-key-history.md) requires certified
history from every writer, a writer ID again implies durable outcome evidence.
[ADR-051](051-inline-latest-values.md)'s logless IDs, which have no such
evidence, do not occur in this format. Inline current bytes still do, and they
are part of the leaf rather than a reclaimable object, so they neither act as a
GC root nor satisfy a retention obligation.

## Context

Reader pins would require ephemeral clients to refresh leases and would let a
crashed reader retain storage indefinitely. Commit age alone is not a valid GC
criterion: an ancient value can remain current until it is superseded just after
a new snapshot starts, and that snapshot still needs the value for its full
lifetime.

Keeping only current writer references also loses the historical floor version
needed at the oldest permitted cut.

## Decision

Snapshot readers create no durable pin, lease, or heartbeat. Derive a fixed
minimum history window from the persisted policy:

```text
maximum staleness + maximum read lifetime + safety guard
```

The guard covers the rate divergence between a reader's and GC's elapsed-time
clocks over the window, final history certification, GC cadence, and operation
margin. Cut selection no longer depends on client clocks under ADR-052, so this
is a rate allowance over one bounded interval rather than an absolute-time
budget. Policy validation rejects a retention setting below the derived minimum.

Measure a version's retention from when it is superseded, not when it originally
committed. For the oldest cut that may still be read, retain:

- every version newer than that cut; and
- the first certified version at or before it, the floor version.

Do not trust a writer's recorded time to prove supersession age. GC may start
the full retention delay from its own observation; after recovery, a helper that
cannot conservatively prove elapsed time waits again. This intentionally permits
excess retention rather than early reclamation.

Count history and catalog predecessor references as GC roots. Retain transaction
certification metadata while any data or catalog history entry needs it. Reclaim
independent per-key values when their own history no longer needs them. A
tombstoned leaf's key-directory entry and history head remain roots while any
admissible or live cut may observe a present floor version; prune them only after
all such cuts observe absence.

ADR-035's paginated, sharded transaction-log walk remains the completeness
mechanism for bulky transaction and preparation cleanup. Snapshot
history additionally uses history indexes as GC roots and candidate sources;
those records do not change the backend's opaque-cursor listing contract.

Treat a durable preparation manifest as a GC root for every named payload and
physical collection root until terminal commit or abort. Reclaiming prepared
objects requires a durable abort fence; absence from committed history alone is
not enough while preparation remains pending. Only never-reused object
identities may use backend deletion.

Retain or monotonically compact transaction outcome fences without losing the
proof that a transaction was committed or aborted. Bulky transaction state may
be reclaimed; every path still treats the compact fence as authoritative.
Missing promised history is corruption, never logical absence.

Provide a persisted operational state machine that rejects new snapshot binds
without affecting strict read-write traffic or its timestamp and certificate
protocol. Existing snapshots keep their original deadlines. After their maximum
remaining lifetime drains, GC may reduce history to latest-state roots. Without
reader pins, this means waiting the maximum lifetime, the safety guard, and the
policy's control-staleness bound from the durable bind-disable fence, and
retaining history whenever elapsed time cannot be proved conservatively. That
last term is what lets a bind validate operational state from a bounded-staleness
observation instead of a strongly consistent read.

The operational state does not remove snapshot capability or the mandatory
writer format. Its transitions and recovery are ownerless, idempotent, and
helpable; uncertain drain or rebuild progress rejects new snapshot binds and
retains history.

Re-enable binds only after durably entering `rebuilding`, closing and resolving
the latest-only GC reclamation generation—or fencing every authorized delete
against delayed execution—and then establishing and verifying a current-state
baseline. Writers always emit certified history even while snapshot binds are
disabled. After the GC fence, pre-baseline writes are in the baseline and every
later supersession is retained. Verify the baseline's data and catalog roots
before publishing the new history floor; cuts older than it are never admitted.
GC may retain excess data during failure or pressure but never reclaims required
history early.

## Consequences

- Snapshot read availability does not depend on tracking live clients.
- Storage use is bounded by policy and write volume rather than reader crashes,
  but the worst-case retained volume can still be large.
- Disabling new binds is an operational pressure valve, not permission to
  invalidate existing transactions or immediate permission to reclaim history.
- Re-enabling after compaction requires a baseline-building transition, not a
  Boolean flip.
- GC must become history-aware and preserve floor versions, tombstones, catalog
  state, shared commit certificates, and compact permanent outcome fences.

## Alternatives considered

### Reader pins, leases, or heartbeats

Registering live readers would let GC reclaim as soon as the last reader
finished, retaining far less than a worst-case window and removing the clock
dependency from retention. It requires every ephemeral client to refresh a
durable registration for the whole life of a read, and a client that crashes
mid-read retains storage until its registration expires — which reintroduces the
same time-based reasoning, now on the critical path of an unrelated client's
storage. The fixed policy window bounds retention without tracking anyone.

### Reclaim by commit age

Deleting versions older than the window is the obvious rule and is wrong. A
value that stayed current for years and is superseded one second after a
snapshot begins must survive that snapshot's full lifetime, even though it
committed long before the window. Retention is measured from supersession for
this reason, and the floor version exists because the oldest readable cut needs
the version that precedes it however old that is.

### Trust a writer-recorded supersession time

Reading the supersession time from the record itself would let GC reclaim
promptly after recovery instead of restarting a conservative wait. It makes
another client's recorded time authoritative over whether data still needed by a
live reader may be deleted. GC uses its own observation, which can over-retain
but cannot reclaim early.

### Never reclaim, or reclaim to latest state only

Retaining everything removes the entire problem and makes storage grow without
bound. Retaining only current state makes snapshots impossible. The operational
state machine exists so an operator can move between those two regimes
deliberately, with the drain and rebuild that safety requires, rather than as a
Boolean that invalidates live transactions.

### Order binds against operational disable with a strongly consistent read

Reading the control record strongly at every bind would make the ordering exact
and let the drain wait start immediately at the disable fence. It puts a single
hot object on the path of every snapshot execution, which is the choke point
this design removes elsewhere. Extending the drain wait by the control-staleness
bound buys the same safety for one small cacheable read.
