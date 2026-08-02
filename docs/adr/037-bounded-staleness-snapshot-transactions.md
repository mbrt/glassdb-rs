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

Because binding cannot fail for lack of a cut and no concurrent writer can
invalidate an execution, the closure runs at most once and is never replayed.
The API accepts `FnOnce`. Bodies must still tolerate cancellation at the fixed
deadline, and at the retention check below.

One read-execution deadline starts immediately before the closure is invoked and
never resets. Crossing it cancels the closure, discards any operation or page
result, and returns `ReadTransactionExpired`; no operation completing after
expiry is observable. The deadline is measured with a monotonic clock that
advances through suspension. Under ADR-052 local clocks no longer contribute to
cut selection, so an error here costs staleness or a spurious expiry rather than
consistency.

A bind validates the database's operational state and the history floor
[ADR-040](040-snapshot-history-retention.md) publishes from one observation no
older than the policy's control-staleness bound. ADR-040's drain wait and its pre-reclamation wait are
both extended by that bound, so neither ordering requires a strongly consistent
read at bind time.

A cut below the floor returns `SnapshotTooOld`. Under healthy operation the
freshest admissible cut sits far above the floor, so a bind fails this way only
when the database retains no usable history, such as during a rebuild. The same
check repeats whenever a running execution refreshes its server-time
observation, and crossing it cancels the closure and discards results exactly as
the deadline does.

Its purpose is to make a reader-versus-GC clock violation surface as an error
rather than as a missing version, and in that role it should never fire. It has
a second role that can fire in a perfectly healthy database: ADR-040 lets an
operator advance the floor deliberately to reclaim storage, which abandons the
lifetime promised to executions already running. A caller cannot tell the causes
apart and has no reason to, since retrying with a fresher cut answers all of
them. None replays, so `FnOnce` is unaffected.

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
- Callers gain one error they cannot prevent by construction. `SnapshotTooOld`
  reports that the database can no longer serve the cut, and retrying with a
  fresher one is the correct response. A long execution can therefore be ended
  by an operator relieving storage pressure, so the lifetime is a promise the
  database keeps rather than a guarantee it cannot break.
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

That reasoning covers arbitrary history and not the narrower case of naming a
cut still inside the retained window, which asks for nothing GC is not already
keeping. The narrower case is deferred only because it shares an entry point
with the broader one, and it carries a condition the broader one does not: a
supplied cut cannot be taken on trust, because one that is too fresh reinstates
the invisible-write hazard the margin exists to prevent. A client would have to
establish admissibility against its own observation rather than accept the
value, in addition to the floor check every bind already performs.

### Reader-held pins or leases instead of a bounded lifetime

Pins would let a reader hold a cut for as long as it likes. Ephemeral clients
would have to heartbeat, and a crashed reader would retain storage until its
lease expired anyway. The fixed lifetime achieves the same bound without
tracking live clients; see ADR-040.

### Portable snapshot-bearing continuation tokens

Exporting a cut across processes or clients would make pagination resumable
outside one execution, and would let several workers scan disjoint ranges of one
cut at once. For a system whose lifetime default exists to serve cold
object-store scans and analytics, that is the shape those workloads actually
want, and under ADR-038 a cut is only a timestamp that ADR-052 already makes
comparable between clients, so sharing one needs no coordination.

An earlier revision deferred this because it meant carrying a retention
obligation across lifetime and trust boundaries with no holder to attribute it
to. ADR-040's history floor removed that difficulty: retention is a policy
window that GC honors whether or not anyone is reading, so there is no
obligation to attribute, and a cut that has aged out of the window is refused
with `SnapshotTooOld` rather than served incorrectly.

Still deferred, but for what remains rather than for that. A resumed cut lives
no longer than a lifetime, since ADR-040 derives the retention window from it,
so this buys parallelism and restartability and not duration. And an exported
cut must be validated for admissibility by whoever receives it, never trusted as
supplied. Those are the terms on which it should be reconsidered.

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
