# ADR-071: GC skips pending and wounded transactions

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

A `Wounded` record can remain pinned indefinitely. Each GC scan that finds it can
repeat checks of every recorded lock and collection effect. A previous GC
pass cannot prove completion: an operation already in flight at the wound can
publish an effect later. This creates recurring request cost with no finite end.

## Decision

GC reads the transaction record first and applies these rules:

- Missing, `Pending`, or `Wounded`: skip without inspecting or changing its
  recorded resources. GC never removes a pinned wound.
- `Aborted`: after the safety horizon, remove recorded aborted effects and
  conditionally delete the record when release is complete.
- Committed: after the safety horizon, check recorded references and conditionally
  delete the record only when none remain and write-back is complete. Key locks
  awaiting write-back retain the record.

Finding a GC candidate does not initiate pending expiry or wounding. Monitor
keeps that responsibility when reads or lock operations need to resolve a
transaction. Existing lock release can still ask Monitor to resolve another
transaction that blocks release; this decision does not change that protocol.
The owner can acknowledge retirement by changing `Wounded` to `Aborted`, after
which ordinary GC applies.

GC filters candidates by durable status and the safety horizon under one
`Requirement`, then captures a fresh `Requirement` for GC checks.
Final-record retention still protects in-doubt commit recovery under
[ADR-057](057-bounded-in-doubt-commit-recovery.md).

## Consequences

Pending and wounded records and their unused resources can remain indefinitely.
We accept their storage cost to avoid active resolution and repeated resource
checks. These statuses cause no GC retry or positive scan-demand signal;
retained objects alone are not GC backlog.

Later hints or GC scans, including hints received during a check, can schedule
another record read. This can detect owner acknowledgement, so the decision does
not eliminate all request cost for retained objects. Pinned wounds remain
until owner retirement.

## Alternatives considered

- Ask Monitor to resolve each pending GC candidate. This keeps expiry policy in
  one place, but still spends requests on transactions no workload needs.
- Decide pending expiry in GC. This also duplicates Monitor policy.
- Repeat wounded-resource reclamation on every encounter. This spends requests
  indefinitely.
- Mark a wound's reclamation complete after one pass. Late effects invalidate
  that claim until the owner has proved retirement.
