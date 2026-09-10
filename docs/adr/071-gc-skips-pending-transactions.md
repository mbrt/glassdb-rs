# ADR-071: GC skips pending transactions

## Status

Accepted — implemented.

Refines the GC policy in [ADR-022](022-garbage-collection-mark-sweep.md) and
[ADR-059](059-pin-foreign-wounds-until-owner-retirement.md). Scheduling remains
as defined in [ADR-070](070-demand-driven-garbage-collection.md).

## Context

Wounding abandoned pending transactions during GC adds requests even when no
read or write needs their resources. Retaining these objects costs less than
actively resolving them. Pending transaction expiry and wounding already belong
to Monitor.

## Decision

GC reads the transaction log first and applies these rules:

- Missing or `Pending`: skip without changing the log or its effects.
- `Wounded`: remove recorded aborted effects, but keep the wound marker.
- `Aborted`: after the safety horizon, remove recorded aborted effects and
  conditionally delete the log when cleanup is complete.
- Committed: after the safety horizon, check recorded references and conditionally
  delete the log only when none remain and cleanup is complete. Entry locks
  awaiting write-back retain the log.

Finding a GC candidate does not initiate pending expiry or wounding. Monitor
keeps that responsibility when reads or lock operations need to resolve a
transaction. Existing lock cleanup can still ask Monitor to resolve another
transaction that blocks cleanup; this decision does not change that protocol.
GC filters candidates by durable status and the safety horizon under one
`Requirement`, then captures a fresh `Requirement` for reference checks.
Final-log retention still protects ambiguous commit recovery under
[ADR-057](057-bounded-in-doubt-commit-recovery.md).

## Consequences

Abandoned pending logs and their effects can remain indefinitely if no operation
needs them. Pending status causes no expiry-based retry and retained objects
alone are not GC backlog. Later hints or GC scans, including hints received
during a check, can schedule another check. Retained objects can thus still add
scan requests. We accept this storage and scan cost to avoid active resolution
requests. Wound markers remain pinned until owner retirement.

## Alternatives considered

- Ask Monitor to resolve each pending GC candidate. This keeps expiry policy in
  one place, but still spends requests on transactions no workload needs.
- Decide pending expiry in GC. This also duplicates Monitor policy.
