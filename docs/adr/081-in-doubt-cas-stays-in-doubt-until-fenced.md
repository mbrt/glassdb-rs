# ADR-081: An in-doubt leaf CAS stays in doubt until a later CAS fences it

## Status

Accepted — implemented.

Refines the non-landing proof of
[ADR-061](061-atomic-logless-single-leaf-commits.md) and the in-doubt
recovery of the leaf coordinator in
[ADR-028](028-shard-mutation-coordinator.md).
[ADR-080](080-commit-recovery-does-not-depend-on-gc-retention.md) fixes the
same mistake for transaction records.

## Context

A backend can apply a request after it reported `Unavailable` to the caller.
For example, a server can still run a request after a client timeout. So an
in-doubt CAS can land after the caller read the object back.

ADR-061 lets a direct commit prove that its in-doubt CAS did not land when every
written target still names its recorded predecessor. That proves only that the
CAS had not landed when the read ran. If the leaf is still in the state that the
CAS expected, the CAS can land later. If the direct commit then reports a loss,
the body replays or a locked commit runs, and the late CAS applies the same
transaction a second time.

The deterministic fuzzer found this after its transport could deliver a
mutation after the caller received `Unavailable`.

## Decision

The coordinator keeps, for each member of an in-doubt CAS, the leaf state that
the CAS expected. A later load tells the member policy whether the leaf
*fences* that CAS, that is, whether the leaf changed in a way that cannot be
undone:

- the membership generation increased, which never goes back
  ([ADR-073](073-merge-nodes-into-right-sibling.md)), or
- the current writer of an entry changed. A writer is a transaction identity,
  and it never returns to an entry.

A different revision alone does not fence the CAS. On a backend whose
revisions depend on content, such as S3, a lock that is taken and then released
restores the exact bytes and the revision. The in-doubt CAS can then land, as
[ADR-043](043-causally-coordinated-backend-operations.md) allows.

- While the leaf does not fence the CAS, the CAS can still land. A member that
  stages again keeps its in-doubt state until a CAS lands. That CAS changes the
  current writer of its outputs, so if it lands, the in-doubt CAS can never
  land. A member that does not stage reports `InDoubt`.
- When the leaf fences the CAS, the ADR-061 proof with unchanged predecessors
  applies.

If the retry budget ends before a CAS lands, the outcome is `InDoubt`.

## Consequences

- An in-doubt direct commit commits transparently in the common case: a retry
  from the unchanged leaf lands.
- A direct commit that cannot stage again on a leaf that does not fence its
  in-doubt CAS now returns `Error::InDoubt` where it replayed the body or used
  a locked commit before. Examples: a new holder blocks it, or a split
  reroutes it.
- On every backend, the proof uses only facts that cannot be undone, so it is
  also correct when revisions repeat.

## Alternatives considered

- **Re-issue the exact same CAS.** A new stage from the same expected state
  also fences the in-doubt CAS. It can also carry other members of the round,
  and it does not need a separate retry path.
- **Fence on any revision change.** This is correct only when revisions never
  repeat. It is false on S3.
- **Add a leaf generation that every CAS increments.** This makes any change
  a fence on every backend, but it changes the leaf format and adds bytes to
  every leaf.
- **Report `InDoubt` at once.** This is safe, but it loses the transparent
  recovery that ADR-009 and ADR-061 give for the common case.
