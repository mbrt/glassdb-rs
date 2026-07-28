# ADR-052: Backend server-time observation

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md), and a prerequisite for
[ADR-038](038-hlc-snapshot-cuts.md).

Extends [ADR-023](023-slimmed-backend-trait.md) with one additional observation
on successful operations. Its operation set, opaque content versions, and
version-conditional read are unchanged.

## Context

A snapshot cut is a point in a time domain that every client must agree on.
The obvious source is each client's own wall clock, which makes the safety of
every cut depend on the absolute skew between arbitrary machines. The failure
mode is the worst kind: a reader whose clock runs ahead of a writer's by more
than the staleness margin can miss a write at one key and see it at another,
returning a torn cut with no error.

Nothing in the current format can detect that. Backend versions are opaque and
order one object only. ADR-036's `LogicalTime` is explicitly process-local and
meaningful only within one open `Database`, so two clients cannot compare
theirs.

Every client already contacts one shared party on every single operation, and
that party keeps a clock. S3 and GCS both report a server time on every
response.

## Decision

Every successful backend operation reports an observation of the backend's
clock alongside its result. The observation is an absolute time comparable
across all clients of one database, and it must be generated at or after the
operation was applied, so that a mutation's observation never precedes the
moment that mutation became durable.

A backend declares the granularity of its reported time. A backend that has no
server time declares that instead; a database opened on such a backend has no
snapshot capability unless the deployment explicitly configures it to trust
client clocks, which reinstates client skew as a safety input.

A database maintains one monotone maximum over the observations it has seen.
That maximum is the only clock permitted to source commit timestamps and cut
selection. Local clocks are restricted to measuring elapsed time, where only
rate matters, and may never be used to extrapolate a time domain forward.

This observation is not a version, not an ADR-036 freshness watermark, and not
a lease. By itself it orders nothing and grants nothing. ADR-036's local
validation watermarks remain separate and remain process-local.

Because clients now see one shared clock on every response, comparing it
against the local clock is free. A client whose local clock has drifted beyond
the policy's allowance in either direction marks itself unhealthy. It may still
commit, because its timestamps come from the backend rather than from itself.

## Consequences

- The safety of a snapshot cut no longer depends on skew between client clocks.
  It depends on skew within the backend's fleet plus the granularity of its
  reported time, which the snapshot policy's margin must absorb.
- Every backend implementation must supply the observation or declare it
  absent. In-process and simulated backends must model it, including injectable
  skew, so that the margin can be exercised deterministically.
- Clients gain a drift detector in both directions at no cost, replacing the
  external coarse elapsed-time signal that a client-clock design would need.
- Cloud providers run dedicated time infrastructure but publish no contractual
  bound on fleet skew, so this remains an environmental assumption. It is a
  substantially narrower one than arbitrary client clocks, and the margin is
  sized to absorb it with room to spare.
- A backend outage removes new observations. Cuts become progressively staler
  rather than unsafe, because a cut derived from an older observation is still
  correct.

## Alternatives considered

### Client wall clocks

Requires no backend change at all and is what comparable distributed databases
do. It makes absolute skew between arbitrary client machines a safety input,
with a silently torn cut as the failure mode, and the margin must then be sized
for the worst clock any operator might run.

### A clock object inside the database

Clients could CAS a shared object carrying the latest observed time. It is a
single-object choke point on a path we are otherwise removing, and it
fundamentally cannot work: an object written by clients can only ever measure
divergence between them, never establish absolute time. If every client's clock
is wrong in the same direction, the object agrees with all of them.

### An external time service

A dedicated NTP or Roughtime dependency would give an authoritative reference.
It adds an operational dependency and a new failure mode to a library whose
whole premise is that object storage is the only infrastructure required, to
obtain something the object store already reports for free.

### Order derived from backend versions

Content versions are already returned on every operation. ADR-023 makes them
opaque by design, and they order exactly one object, so they cannot place two
writes to different keys in a common time domain.

### A registry of writers publishing their clocks

Readers could take the minimum clock published by live registered writers,
which is safe with no absolute-clock assumption at all. One alive-but-slow
client then freezes snapshot freshness for the whole database, distinguishing
slow from crashed needs a clock anyway, and joining requires a CAS-ordered
shared object.
