# ADR-080: Commit recovery does not depend on GC retention

## Status

Accepted — implemented.

Refines the classification of a missing record in
[ADR-057](057-bounded-in-doubt-commit-recovery.md). The rule of
[ADR-059](059-pin-foreign-wounds-until-owner-retirement.md) does not change.

## Context

ADR-057 lets the owner read back an in-doubt commit write until the pending
timeout expires. GC keeps a final record for the safety horizon, which is
longer. Two owner decisions depended on that difference, so an owner that paused
for longer than the safety horizon could decide wrongly:

- **Abort reported for a commit.** A `Pending` read after an in-doubt
  `set_if(Committed)` proves only that the write had not landed yet. If it lands
  later, the retry is rejected. GC can then delete the committed record before
  the owner reads it again. ADR-057 maps a missing record after a clean
  rejection to `AlreadyFinalized`, so the caller gets an abort for a transaction
  that committed.
- **Commit after a wound.** An in-doubt create of a committed record can land
  after a peer wounds the empty path. If the owner acknowledges the wound as
  `Aborted`, GC can delete that record, and the delayed create then lands on an
  empty path. Peers and GC already treat the identity as aborted and released
  its locks, but the identity now reads as committed.

A process or a machine can pause for any length of time, so the safety horizon
cannot be a correctness argument.

## Decision

- During a commit, a missing record is always in doubt, also after a clean
  rejection. GC deletes only `Committed` and `Aborted` records
  ([ADR-071](071-gc-skips-pending-and-wounded-transactions.md)). Peers write
  `Wounded`. An acknowledgement as `Aborted` needs proof that no owner operation
  is active, and a commit runs inside one. So the deleted record can only be the
  owner's own commit from an earlier attempt.
- A create of the committed record that ended in doubt is treated like
  unresolved owner work, unless a later read finds `Committed`. The owner then never changes a
  wound of that identity to `Aborted`, so GC never deletes the record that
  fences the delayed create.

A conditional commit from a `Pending` revision does not need the second rule. A
wound changes the revision, so the delayed write cannot land after it.

The read-back deadline of ADR-057 stays. It now only makes the in-doubt outcome
less frequent.

## Consequences

- A commit can return `Error::InDoubt` in one more case: an owner pause longer
  than the safety horizon, after a clean rejection. Before, that case returned a
  wrong abort.
- A wounded identity whose committed-record create was in doubt keeps a
  `Wounded` record forever. ADR-059 already accepts this cost for unresolved
  owner work.
- A create of a `Pending` record that is in doubt can still land after GC
  deletes an acknowledged abort. The result is an expired `Pending` record that
  a peer later wounds and that nobody acknowledges. It costs storage, but
  nothing commits with it, because the owner already retired that identity and
  does not use it again.

## Alternatives considered

- **Keep the read-back deadline as the guarantee.** No finite deadline covers
  an unbounded pause.
