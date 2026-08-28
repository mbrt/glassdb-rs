# ADR-067: Reject backend-versioned snapshot ingestion

## Status

Accepted.

This record replaces a discarded snapshot-read design sketch. It follows
[ADR-066](066-reject-timestamp-versioned-snapshot-history.md), which rejected
the timestamp-versioned history that this sketch tried to repair.

## Context

The goal stayed the same: one read-only execution that keeps a fixed database
state for minutes, over point reads, key scans, and collections. The previous
proposal failed because it wrote history in the commit path. This sketch moved
that work out of the commit path with two mechanisms.

The first mechanism was storage-native versioning. Every supported bucket keeps
each overwritten or deleted object, through S3 Versioning or Cloud Storage
Object Versioning. The retained versions are then a complete journal of every
commit: successive leaf versions contain the direct commits, and the transaction
objects contain the logged ones. A background compiler reads that journal
through new version-listing, version-read, and version-delete backend
operations, and builds immutable checkpoint trees. A snapshot read binds one
published checkpoint and never touches live state.

The second mechanism was time. After a successful commit response, the writer
sampled a bounded-time source, chose a completion timestamp, and put an ordering
attestation in a background queue. The writer then waited locally until that
timestamp was certainly in the past, and only then reported success. This
"commit-wait" was there to order two transactions that touch different keys and
therefore share no lock.

The commit path itself kept its accepted shape: one leaf CAS for a direct
commit, the locked protocol for a logged one, and no history object.

## Decision

Do not implement backend-versioned snapshot ingestion.

The reasons are:

- **Commit-wait adds latency to every commit, for a guarantee most callers do
  not need.** The wait lasts as long as the uncertainty of the time source. Its
  cost is real, it is paid by databases that never read a snapshot, and it
  cannot be removed while the design claims real-time order.
- **No portable time source has the needed contract.** The wait proves an
  ordering only with a documented error bound, such as the one AWS ClockBound
  reports on some Linux instances. GlassDB must run against plain object storage
  from any host, so this makes the feature conditional on the deployment.
- **Bucket configuration becomes part of the database format.** Snapshot
  correctness then depends on a bucket setting and on lifecycle rules that
  GlassDB does not control. A wrong rule deletes history that a reader still
  needs.
- **A hot key keeps one complete object copy for each write.** Versioning stores
  full objects, not deltas, and AWS documents degraded service for objects with
  very many versions. The compiler must therefore always keep up with the write
  rate, which makes a background component correctness-relevant.
- **Crash recovery was unsolved.** A commit whose queued attestation is lost has
  no safe position in the order, so it stops the snapshot frontier. The sketch
  answered this with a full new baseline that it did not specify.

## Consequences

- The backend trait keeps its small operation set. It needs no version listing,
  no version read, and no version delete, and GlassDB stays free of
  provider-specific version behavior.
- No deployment must configure object versioning, and no deployment must supply
  a bounded-time service.
- Commit latency contains backend round trips only. No local wait belongs to a
  successful commit.

## Alternatives considered

- **A bounded undo buffer inside the leaf.** It keeps the previous value without
  provider versioning, but a hot key fills it while the background worker is
  unavailable. The writer must then block, spill, or lose history.
- **Asynchronous history without retained versions.** A second overwrite
  destroys the earlier value before the compiler observes it.
- **A larger margin over measured provider clock agreement.** This lowers the
  probability of a wrong order. It does not create a guarantee.
- **Drop commit-wait and publish dependency-consistent cuts instead.** This
  removes the time source and the added latency, and it is the direction that
  [ADR-068](068-reject-demand-activated-dependency-checkpoints.md) explored and
  rejected for other reasons.
