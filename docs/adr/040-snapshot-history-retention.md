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
committed. Retain, for each key, the newest certified version at or before each
admissible grid point in the readable range. Stating the rule per grid point
rather than per interval keeps it free of boundary cases, and it subsumes the
floor version, which is simply the newest version at or before the oldest cut
that may still be read.

Every other version is invisible to every reader that could exist, because
[ADR-038](038-hlc-snapshot-cuts.md) admits cuts only at grid points. Retaining
them buys nothing: a key written a hundred times per second against a five-second
grid produces five hundred versions per slot of which one is observable.

Reclaim those intermediate versions as soon as their slot closes, with no
retention wait. A live reader is bound to a grid point like any other cut, so it
cannot observe them either, and the commit-age bound is what makes closure
durable enough to rely on. This is deliberately a different class of reclamation
from the window above: it removes nothing any cut can observe, so it neither
waits for nor advances the history floor. Hot-key garbage therefore survives for
seconds rather than for the full window.

Retained versions per key are consequently bounded by

```text
(maximum staleness + maximum read lifetime) / cut grid period
```

regardless of write rate, which makes the grid period the control over hot-key
retention: a coarser grid retains proportionally less at the cost of staleness.

Do not trust a writer's recorded time to prove supersession age. GC may start
the full retention delay from its own observation; after recovery, a helper that
cannot conservatively prove elapsed time waits again. This intentionally permits
excess retention rather than early reclamation.

### Make a retention violation detectable

The guard above is an assumption about clock rates, and an assumption that is
only asserted fails silently. If it is violated, GC reclaims history a live
reader still needs, and for a pruned deleted key that reader observes a
legitimately absent key. Absence is a valid answer at some cuts, so nothing
distinguishes reclaimed history from history that never existed, and the
retention rule above degrades into a wrong answer rather than an error.

Publish a durable history floor: the oldest cut GC still guarantees to serve
completely, quantized to the cut grid and advancing monotonically. GC must
durably advance the floor **before** performing any reclamation that the new
floor authorizes; reclaiming first and publishing after leaves exactly the
window this exists to close. A bind requires its cut at or above the floor, and
a reader whose cut falls below it fails with a distinct error instead of
returning data.

The floor lives in the same bounded-staleness metadata a bind already validates
for operational state, so one cached read covers both and yields a server-time
observation from its own response. A reader may therefore validate against a
floor observation no older than the control-staleness bound, and GC waits that
bound after publishing before it reclaims, exactly as the bind-disable fence
already does. Readers re-validate on the observation refresh they already
perform, which turns a violation that develops mid-execution into an error
before any torn result is returned.

This is the same value the rebuild transition below publishes; ordinary GC and
rebuild are two writers advancing one monotone floor.

Clocks now sit in liveness and retention rather than in correctness. GC
advancing the floor too eagerly is detectable by every reader; advancing it too
slowly only over-retains. The floor does not defend against GC reclaiming above
its own published floor, which is a protocol violation rather than a clock one
and remains corruption.

Count history and catalog predecessor references as GC roots. Retain transaction
certification metadata while any data or catalog history entry needs it. Reclaim
independent per-key values when their own history no longer needs them. A
deleted key's residue and the history head it names remain roots while any
admissible or live cut may observe a present floor version; prune them only
after all such cuts observe absence. ADR-039 moves that residue out of the leaf
once the deletion's slot closes, so GC roots it in the side structure covering
that key range and reclaims the structure as a whole once its newest entry
leaves the window.

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

Missing promised history is corruption, never logical absence. History is
promised when some admissible cut can observe it, not merely when something
references it. A coalesced version is not promised, so a predecessor pointer
into one is an expected dangling reference rather than corruption. Nothing
depends on the chain being dense, because lookup goes through the timestamp
index rather than a predecessor walk.

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
- Retained history is bounded by the number of admissible cuts rather than by
  write volume, so a hot key no longer scales retention with its write rate.
  Choosing the grid period now trades staleness against storage.
- Coalescing gives GC a second reclamation class with different rules: prompt,
  triggered by slot closure, and independent of the floor. Conflating it with
  window-based reclamation would either forfeit the win or reclaim early.
- A clock-rate violation between a reader and GC surfaces as a clean error
  rather than as a missing version or a key that appears never to have existed.
- GC gains an ordering obligation it did not have: the floor must be durable
  before the deletions it authorizes begin, and a crash between the two is safe
  only in that direction.
- The floor is one small monotone object, but no writer touches it, GC writes it
  at its own cadence rather than per transaction, and readers may use a stale
  copy, so it is not a coordination point on any transaction path.
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

### Retain every version in the window

Keeping every version newer than the oldest readable cut is the obvious rule and
needs no notion of grid points in GC at all. It retains a multiple of what any
reader can observe, and the multiple is the key's write rate times the grid
period, so the cost is unbounded in exactly the workload that can least afford
it. Coalescing coincidentally makes retention predictable, which is worth more
than the simplicity.

### Coalesce on the retention window rather than on slot closure

Deferring coalescing until a version leaves the window would keep one uniform
reclamation trigger. Intra-slot versions are unobservable from the moment their
slot closes, so this retains known garbage for the whole window, which for a hot
key is most of the storage the format uses.

### Rely on the retention window alone, with no published floor

Sizing the window conservatively and trusting it is what this ADR did before,
and it needs no extra object or ordering obligation. It makes a clock-rate
assumption load-bearing for correctness with no way to notice when it breaks,
and the specific failure — a pruned deleted key reading as absent — is
indistinguishable from a correct answer. The floor costs one small cacheable
object to convert that into an error.

### Detect reclamation at the point of use instead

A reader could treat a dangling history pointer as proof of early reclamation,
which needs no floor and no bind check. It catches only the cases that leave a
dangling reference; the dangerous case prunes the directory entry too, leaving
nothing to dangle. Detection has to be based on the cut, not on what survives.

### A per-key or per-collection floor

Finer floors would let a heavily reclaimed region reject reads without
penalizing the rest of the database. A reader's cut spans everything it may
touch, so it would have to validate a floor per key it reads, and the error
would then depend on access order rather than on the cut. One database-wide
floor keeps the check at bind where the cut is chosen.

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
