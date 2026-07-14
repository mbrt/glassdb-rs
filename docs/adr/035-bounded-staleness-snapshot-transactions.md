# ADR-035: Bounded-staleness snapshot transactions

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
cut that remains usable without locks or validation. Object storage makes that
cut expensive to create on every read, so bounded staleness and a bounded
lifetime are acceptable.

## Decision

Add an explicit snapshot-preferred, read-only `Database::read_tx` API. Its
closure receives one `ReadTransaction` facade with point, range, pagination,
collection/subcollection, and cross-collection operations in either execution
mode.

Each snapshot execution binds the freshest sealed database epoch satisfying the
call's freshness request before invoking the closure. Reads take no data locks,
perform no commit validation, and may internally retry only idempotent
operations against the same fixed epoch and remaining deadline. Writes are not
available through the facade. Caller-selected historical epochs and portable
continuation tokens are not supported.

Snapshot acquisition has its own bounded timeout. By default, failure to acquire
an admissible epoch falls back before execution to a strict read-only OCC
implementation of the same facade. Ranges and pages in that mode remain in one
attempt and contribute to its accumulated predicate read set. A conflict
replays the complete closure; it never continues a cursor in a different
attempt. This strict implementation is a prerequisite for shipping transparent
fallback for every advertised operation.

The API accepts `FnMut` and requires side-effect-free transaction bodies in both
modes; exact-once closure execution is a goal of the snapshot implementation,
not a public guarantee. Bodies must also tolerate cancellation at the fixed
deadline. A per-call option may require snapshot execution
instead; it returns `FreshSnapshotUnavailable` without invoking the closure when
acquisition fails. The mode is selected before the first invocation, and a
started snapshot execution never falls back or changes epoch.

One read-execution deadline starts before the final snapshot control read that
validates the enabled-state generation and binds the epoch. This orders even a
cached-cut bind against operational disable with its lifetime already ticking.
A strict fallback starts its deadline immediately before the first closure
invocation. The same deadline covers every retry and never resets. Crossing it
cancels the closure attempt, discards an operation or page result, and returns
`ReadTransactionExpired`; no operation completing after expiry is observable.
The deadline source must advance through suspension and satisfy the bounded
duration uncertainty included in the retention policy.

Store one immutable `SnapshotPolicy` in database metadata. It defines maximum
staleness, acquisition timeout, maximum lifetime, duration-clock uncertainty,
epoch cadence, writer grace, and the minimum derived retention guarantee.
Per-call requests may be stricter but never exceed the database policy. Online
policy reconfiguration is deferred.

Existing `Database::tx` retains its strict, retryable behavior even when a
particular execution produces no writes.

## Consequences

- A successful snapshot attempt is internally consistent and cannot be
  invalidated by later writers or epoch age.
- The default API favors availability through a stronger strict fallback while
  making possible closure replay explicit in its type and documentation. Both
  modes expose the same operation surface and fixed overall deadline.
- Callers needing predictable snapshot execution can reject fallback.
- The fixed deadline bounds storage retention and prevents abandoned readers
  from pinning history indefinitely.
- Sealed epochs, historical data, pin-free retention, and a versioned catalog
  require the separate decisions in ADR-036 through ADR-039.
