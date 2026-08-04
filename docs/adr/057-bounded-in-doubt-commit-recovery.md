# ADR-057: Bound in-doubt commit recovery by the reclamation horizon

## Status

Accepted — implemented.

This refines [ADR-009](009-in-doubt-conditional-writes.md)'s treatment of
logged commits and leaves [ADR-022](022-garbage-collection-mark-sweep.md)'s
reclamation policy unchanged.

## Context

ADR-009 assumed that the owner could always resolve an ambiguous final-log
write by reading the transaction record back. That is not guaranteed: once a
final record is old and unreferenced, GC may reclaim it while the owner is still
retrying after a lost acknowledgement.

An absent record cannot distinguish a reclaimed commit from a reclaimed abort.
Re-creating either outcome could contradict the durable decision and the lock
state other transactions have since observed.

## Decision

When a final-log write may have landed, the owner immediately reads the durable
status instead of re-issuing the write. Recovery is bounded from the start of
that write attempt to the pending timeout. A possibly-landed write stamps its
record no earlier than the attempt began, while GC also waits the clock-skew
allowance, so this is a conservative bound before reclamation.

A `pending` read proves that attempt did not land because final records are
immutable. The uncertainty is resolved, and a subsequent write receives a new
recovery budget. If the status cannot be established within the budget, the
operation returns `Error::InDoubt`.

Absence is never used as a CAS expectation or repaired by re-creating the
record:

- After a definite conflict, the owner's write did not land. Only a peer's
  abort can have won, so the result is `AlreadyFinalized`.
- After a write that may have landed, the missing decision is irreducibly
  uncertain, so the result is `Error::InDoubt`.
- Pre-commit maintenance only writes `pending`; observing absence therefore
  means the transaction is already final and must not be resurrected.

## Consequences

- The logged commit path can surface `Error::InDoubt` after a sustained failure
  to confirm a possibly-landed write.
- Ordinary lost acknowledgements still recover transparently when the final
  status can be read.
- Recovery of one unresolved attempt is bounded by the pending timeout.
- GC retains its existing horizon and reference checks.

## Alternatives considered

- Retaining final records longer only moves the race unless retention is
  unbounded.
- Re-creating an absent record can choose the wrong terminal outcome.
- Treating absence as an abort can report failure for a durable commit.
