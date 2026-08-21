# ADR-063: Retire transaction resources while propagating body panics

## Status

Accepted — implemented.

This refines [ADR-012](012-tokio-util-cancellation.md),
[ADR-024](024-hold-and-wait-conflict-resolution.md), and
[ADR-059](059-pin-foreign-wounds-until-owner-retirement.md).

## Context

Transaction bodies run before their reads are validated. A normal returned
outcome can therefore be discarded and replayed when its snapshot is stale.
A panic has different language semantics: catching it for validation or replay
would delay unwinding, complicate payload preservation, and risk executing an
abnormal body twice.

A panic can also occur on a locked replay whose engine identity, locks, or
prepared collection objects were retained from an earlier execution. Letting
unwinding drop those resources without a recovery owner can leave local state
inconsistent and peers blocked until lease recovery.

## Decision

Body panics propagate immediately with their original payload. They are not
read-validated or retried, including when the body observed a stale snapshot.
Snapshot transparency applies only to normal returned values and errors.

Cancellation and unwinding share one armed retirement guard for each active
engine attempt. The guard is disarmed only after owner finalization succeeds.
Otherwise its drop synchronously hands the identity to the engine, clears
process-local lock ownership, and admits waited recovery that either settles
the owner abort or leaves a durable recovery fence. Physical locks and prepared
objects are not eagerly reclaimed; helpers and garbage collection recover them
from durable protocol state. Cleanup failures are diagnostic and never replace
the panic.

The guarantee covers framework-owned attempt resources. Detached work and
external side effects started by a body remain the caller's responsibility.
Repository-owned conditions derived from transaction reads continue to return
errors so they remain eligible for validation and replay. With `panic=abort`,
ordinary crash recovery applies because no unwind guard runs.

## Consequences

- A stale-snapshot panic can escape and its body is never replayed.
- Uncommitted database changes remain unpublished, while retained locks and
  prepared objects keep a durable recovery owner.
- Graceful shutdown drains admitted retirement work and may wait indefinitely;
  cancelling shutdown is safe and a later call resumes the drain.
- Panic safety and snapshot transparency are separate API guarantees.
