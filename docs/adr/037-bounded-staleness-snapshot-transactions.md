# ADR-037: Bounded-staleness snapshot transactions

## Status

Proposed.

Umbrella ADR for the living
[snapshot-reads design](../designs/snapshot-reads.md).

## Context

The existing read-only transaction path obtains strict serializability by
optimistically reading current values, validating them at the end, and replaying
the user closure after a conflict. It cannot provide a long, stable database
view without repeated work, and independent stale reads can mix unrelated
points in time.

Analytics, backup-style traversal, and consistent pagination need one database
cut that remains usable without locks or validation. Object storage makes a
freshly synchronized cut expensive, so bounded staleness and a bounded lifetime
are acceptable.

## Decision

Add an explicit read-only `Database::read_tx` API. Its closure receives one
`ReadTransaction` facade with point reads, ADR-033's forward keys-only
`KeyScan`/`KeyPage` operations, collection and subcollection enumeration, and
cross-collection operations. Values for scanned keys are ordinary tracked point
reads at the same cut.

Each execution binds one cut timestamp before the closure runs and keeps it for
the execution's lifetime. Reads take no data locks and perform no commit
validation. The implementation may retry idempotent operations against the same
fixed cut and remaining deadline. Writes are not available through the facade.
Caller-selected historical cuts and portable snapshot-bearing continuation
tokens are not supported; `KeyScan::after` is only an exclusive raw-key bound
and carries no cut or deadline.

Binding selects a cut from a server-time observation under
[ADR-038](038-hlc-snapshot-cuts.md), so it performs no coordination and cannot
fail for lack of a frontier. A caller may request a fresher cut than the one
the client currently holds evidence for, which costs at most one small backend
operation and fails only the way any backend operation fails. There is no
acquisition timeout, no freshness certificate, no fallback mode, and therefore
no `FreshSnapshotUnavailable`.

A database whose backend reports no server time has no snapshot capability, as
ADR-052 describes. That is a property of the open database rather than of a
call, so it is reported when the database is opened and by any `read_tx` on it,
not as a per-call acquisition failure.

Because binding cannot fail and execution cannot be invalidated, the closure
runs exactly once. The API accepts `FnOnce`. Bodies must still tolerate
cancellation at the fixed deadline.

One read-execution deadline starts immediately before the closure is invoked and
never resets. Crossing it cancels the closure, discards any operation or page
result, and returns `ReadTransactionExpired`; no operation completing after
expiry is observable. The deadline is measured with a monotonic clock that
advances through suspension. Under ADR-052 local clocks no longer contribute to
cut selection, so an error here costs staleness or a spurious expiry rather than
consistency.

A bind validates the database's operational state from an observation no older
than the policy's control-staleness bound. ADR-040's drain wait is extended by
that bound, so ordering a bind against an operational disable requires no
strongly consistent read at bind time.

Store one immutable `SnapshotPolicy` in database metadata. It defines maximum
staleness, the cut-grid period, maximum lifetime, the fleet-skew,
reported-granularity, and apply-anchoring allowances whose sum is the cut
margin, the commit-age bound, the control-staleness bound, the
elapsed-time rate uncertainty between a reader and GC, and the minimum derived
retention guarantee. Per-call requests may be stricter but never exceed the
database policy. Online policy reconfiguration is deferred. Every database in
this format has this policy and snapshot capability.

Existing `Database::tx` retains its strict, retryable behavior even when a
particular execution produces no writes.

## Consequences

- A snapshot execution is internally consistent and cannot be invalidated by
  later writers or by the age of its cut.
- The closure runs once, so the API is `FnOnce` and callers reason about a
  single execution rather than about replay.
- The absence of an acquisition step removes the failure mode, the timeout, the
  fallback implementation, and the latency cliff that a fallback would impose
  on exactly the long-running reads this feature targets.
- The fixed deadline bounds storage retention and prevents abandoned readers
  from pinning history indefinitely.
- ADR-033 remains authoritative for scan bounds, ordering, page shape, and
  strict validation. Calls inside one snapshot execution additionally share its
  fixed cut; separate `Collection::scan_keys` transactions do not.
- Cuts, historical data, pin-free retention, and a versioned catalog require the
  separate decisions in ADR-038 through ADR-041, and ADR-038 requires ADR-052.
- Acceptance requires the living design's
  [performance gate](../designs/snapshot-reads.md#performance-acceptance-gate).
  The logged commit paths are unchanged, so that gate mostly covers the cost of
  writing and retaining history. The exception is the small single-key
  overwrite, which loses [ADR-051](051-inline-latest-values.md)'s one-CAS commit
  to mandatory history and is measured as its own cell.

## Alternatives considered

How a cut is defined is a separate question, decided in
[ADR-038](038-hlc-snapshot-cuts.md) and compared in
[Cut definition](../designs/snapshot-reads.md#cut-definition). The alternatives
below concern the public contract.

### No snapshot API

Callers could reconcile repeated strict reads themselves. Nothing they can build
on top gives a stable view: every conflict re-reads, and independent stale reads
mix unrelated points in time. This is the problem statement, not an option.

### Caller-selected historical cuts

Explicit time travel would subsume bounded staleness, but it turns the retention
obligation from a policy-bounded window into an open-ended one and makes the
contract about arbitrary history rather than a recent consistent view. Deferred
rather than rejected: the versioned format admits it later.

### Reader-held pins or leases instead of a bounded lifetime

Pins would let a reader hold a cut for as long as it likes. Ephemeral clients
would have to heartbeat, and a crashed reader would retain storage until its
lease expired anyway. The fixed lifetime achieves the same bound without
tracking live clients; see ADR-040.

### Portable snapshot-bearing continuation tokens

Exporting a cut across processes or clients would make pagination resumable
outside one execution. It requires carrying a retention obligation across
lifetime and trust boundaries with no holder to attribute it to. Deferred.

### A strict read-only fallback behind the same facade

An earlier revision fell back to a strict OCC execution of this facade when
acquisition failed. Acquisition can no longer fail, so the fallback has nothing
to handle. It was also a poor fit for the target workload: a long scan under
strict OCC accumulates a read set proportional to the database, would rarely
validate against concurrent writers, and would replay until its deadline.

### A strict-only database format or creation-time opt-out

Two formats would let applications that never read snapshots skip the write-path
cost entirely, at the price of doubling the protocol surface, the recovery
matrix, and the test matrix.

The mandatory cost is no longer only writing history. A small single-key
overwrite also loses ADR-051's one-CAS commit, and that is the most common write
in an OLTP workload, so the case for an opt-out is stronger than it was when
this alternative was first rejected. It is still rejected: two formats would
double the surface permanently to avoid a cost that a certified single-CAS path
could remove for both, and committing to the fork now would remove the incentive
to find that path. If the performance gate's inline-overwrite cell fails and no
such path is found, this should be reconsidered rather than treated as settled.
