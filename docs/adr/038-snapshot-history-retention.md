# ADR-038: Snapshot history retention

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md).

On acceptance, this supersedes ADR-022's current-reference-only liveness rule
for committed values and its deletion of outcome evidence needed to fence
delayed epoch-protocol artifacts. Pending-lock reclamation remains, extended by
ADR-036's final-phase grace for admitted writers.

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
maximum begin staleness + maximum read lifetime + safety guard
```

The guard covers the policy's maximum cross-client duration-clock uncertainty,
final history certification, GC cadence, and operation margin. Policy validation
rejects a retention setting below the derived minimum.

Measure a version's retention from when it is superseded, not when it originally
committed. For the oldest cut that may still be read, retain:

- every version newer than that cut; and
- the first certified version at or before it, the floor version.

Do not trust an unbounded writer timestamp to prove supersession age. GC may
start the full retention delay from its own monotonic observation; after
recovery, a helper that cannot conservatively prove elapsed time waits again.
This intentionally permits excess retention rather than early reclamation.

Reader and GC duration clocks must advance through process and machine
suspension and remain within the policy's bounded rate uncertainty over the
retention horizon. A database cannot enable pin-free snapshots when that clock
contract is unavailable.

Count history and catalog predecessor references as GC roots. Retain transaction
certification metadata while any data or catalog history entry needs it. Reclaim
independent per-key values when their own history no longer needs them.

Treat a durable preparation manifest as a GC root for every named payload and
physical collection root until terminal commit or abort. Reclaiming prepared
objects requires a durable abort fence; absence from committed history alone is
not enough while preparation remains pending. A reusable fixed root is reclaimed
only to an incarnation-bearing CAS tombstone and is never unconditionally
deleted; only never-reused object identities may use backend deletion.

Retain or monotonically compact epoch/lane outcome fences without losing the
proof that a transaction was committed or aborted and that a lane is closed.
Bulky transaction state may be reclaimed; every path still treats the compact
fence as authoritative. Missing promised history is corruption, never logical
absence.

Provide a persisted operational state machine that rejects new snapshot
admission without affecting strict read-write traffic or its epoch/certificate
protocol. Existing snapshots keep their original deadlines. After their maximum
remaining lifetime drains, GC may reduce history to latest-state roots. Without
reader pins, this means waiting the full maximum lifetime plus the safety guard
from the durable admission-disable fence and retaining history whenever elapsed
time cannot be proved conservatively.

Re-enable admission only after durably entering `rebuilding`, closing and
resolving the latest-only GC reclamation generation—or fencing every authorized
delete against delayed execution—and then fencing and sealing a current-state
baseline. Writers always emit certified history even while snapshot admission
is disabled. After the GC fence, pre-baseline writes are in the baseline and
every later supersession is retained. Verify the baseline's data and catalog
roots before publishing the new history floor; cuts older than it are never
admitted. GC may retain excess data during failure or pressure but never
reclaims required history early.

## Consequences

- Snapshot read availability does not depend on tracking live clients.
- Storage use is bounded by policy and write volume rather than reader crashes,
  but the worst-case retained volume can still be large.
- Disabling new snapshots is an operational pressure valve, not permission to
  invalidate existing transactions.
- Re-enabling after compaction requires a baseline-building transition, not a
  Boolean flip.
- GC must become history-aware and preserve floor versions, tombstones, catalog
  state, shared commit certificates, and compact permanent outcome fences.
