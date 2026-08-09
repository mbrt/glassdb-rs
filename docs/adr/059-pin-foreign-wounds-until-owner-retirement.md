# ADR-059: Pin foreign transaction wounds until owner retirement

## Status

Proposed

This refines the lazy pending-object protocol in
[ADR-021](021-wound-wait-leases-shard.md),
[ADR-022](022-garbage-collection-mark-sweep.md), and
[ADR-024](024-hold-and-wait-conflict-resolution.md). If accepted, it supersedes
their use of a finite aborted-object lifetime as the anti-resurrection fence.
It does not change [ADR-057](057-bounded-in-doubt-commit-recovery.md)'s handling
of a final commit whose outcome may already have landed.

## Context

ADR-024 permits a transaction to publish its identity as a lock holder before
its pending transaction object exists. The background refresher later creates
the object with create-if-absent semantics. A foreign wound creates an aborted
object at the same path, and ADR-022 retains that tombstone long enough for an
ordinarily delayed owner to observe it.

No finite lifetime covers an unbounded suspension or partition. An owner can
publish a holder, stop before creating its pending object, and resume only after
a peer has wounded it and GC has deleted the abort tombstone. The path is absent
again, so a new `pending` or `committed` create can resurrect an identity whose
holders peers already reclaimed.

Preparing every pending object before the first holder closes the gap, but adds
one serial backend operation and latency wave to every locked transaction. The
optimistic alternative is to make the exceptional foreign wound, rather than
the normal transaction, carry the durable fence.

## Decision

### Distinguish a pinned foreign wound from an acknowledged abort

Add a transaction status named `Wounded`. An aborter that cannot prove local
retirement of the transaction identity writes `Wounded`:

- a missing transaction path is changed to `Wounded` with create-if-absent;
- an observed `pending` object is changed to `Wounded` with CAS; and
- the wound must be durable before any holder is released or reused.

`Wounded` is terminal for transaction semantics. Readers, lock resolvers, and
helpers treat it as an abort; a lease refresh or commit cannot replace it.
Unlike `Aborted`, however, it is pinned. General GC may clear stale holders and
reclaim resources whose cleanup remains durably described, but it may not
delete `Wounded` or make it reclaimable merely because a clock or keep-alive has
advanced.

The distinction is based on proof, not process identity. A waiter, GC worker,
or even a task in the same process that lacks the owning transaction's local
retirement proof writes `Wounded`.

### Let the live owner acknowledge retirement durably

When the owning transaction observes `Wounded`, it first retires that identity
in its local transaction state. Before acknowledging the wound, it must know
that no unresolved transaction-object create or commit can later land and that
any recovery-owned physical effects still have a durable cleanup owner. A
dropped or in-doubt mutation that cannot satisfy this condition leaves the
object pinned.

The owner then conditionally changes `Wounded` to `Aborted`, retaining any
recovery manifest still needed by GC. This CAS is the durable acknowledgement
that the old identity cannot be resurrected. After it lands, any GC instance
may apply the ordinary aborted-object cleanup and retention rules. Enqueuing the
record on the owner's local GC accelerates that work but is not part of the
safety proof.

An owner-initiated abort may write `Aborted` directly when it establishes the
same retirement and cleanup proof. A newly opened Database is not the owner of
transactions abandoned by an earlier incarnation and cannot acknowledge them
merely because it uses the same database.

Transactions that can create recovery-owned physical resources must persist
their complete recovery manifest before those effects can become durable. A
minimal `Wounded` object created for a previously missing ordinary transaction
cannot retroactively describe unknown resources.

## Consequences

- The uncontended lazy transaction path gains no pending-object operation or
  latency wave before its first holder.
- Foreign wounds gain a distinct terminal transition. Healthy owners normally
  acknowledge it promptly, after which existing finite GC applies.
- A permanently dead owner, or one with an unresolved mutation, leaves a small
  `Wounded` object indefinitely. Repeated failures can therefore grow retained
  object count and GC listing work without bound. This is the explicit cost of
  moving the fence off the normal path.
- Stale holders can be released while the marker remains, so a pinned wound
  does not have to keep user keys blocked.
- `Wounded` and `Aborted` make the lifecycle proof visible in durable state:
  terminal-but-unacknowledged is distinct from terminal-and-GC-eligible.

## Future optimizations

The owner could conditionally delete `Wounded` directly after proving that no
mutation can land and no cleanup obligation remains. That would save the later
background delete, but not an owner-side operation or latency wave, and stale
holders would again resolve through the slower missing-object grace path. The
initial protocol keeps the explicit `Wounded → Aborted` handoff; direct deletion
is a cleanup-only optimization.

Finite reclamation without owner acknowledgement would require a stronger time
contract. One possible design would associate every exact transaction-object
version with a provider-assigned application time and make each refresh or
commit carry the previous valid refresh time. Every observer would interpret a
mutation whose application time exceeds that chained lease as aborted, and an
expired chain could never be restarted under the same identity. Only then could
GC delete an unacknowledged wound knowing that a later create self-invalidates.

The current backend exposes only object contents and an opaque version, and the
current transaction timestamp is client-authored. A future time-based design
therefore needs a separate ADR proving provider timestamp availability,
cross-object rate and skew bounds, cache propagation, and fail-closed behavior.
A fresh timestamp field or a local pre-write deadline by itself is not a fence.

## Alternatives considered

- **Prepare `pending` before every holder.** This gives a simple retained-CAS
  fence and finite cleanup, but adds a serial operation and wave to the normal
  locked path.
- **Keep ordinary abort tombstones for a longer finite interval.** Any finite
  interval can be exceeded by suspension or partition.
- **Retain every foreign wound forever without acknowledgement.** This is safe
  but misses the common cleanup opportunity when the owner returns and
  definitively retires the identity.
- **Authorize deletion from a local GC enqueue.** A queue entry is not durable,
  does not settle ambiguous writes, and cannot transfer cleanup ownership after
  the process stops.
