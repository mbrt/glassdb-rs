# ADR-042: Conditional-only backend mutations

## Status

Accepted.

This refines [ADR-023](023-slimmed-backend-trait.md)'s backend surface and
preserves [ADR-009](009-in-doubt-conditional-writes.md)'s outcome contract.

## Context

`Backend` exposes unconditional write and delete operations alongside create
and content-CAS. Unconditional write is unused by the engine and is dangerous
under delay or cancellation: an old request can overwrite a state created by a
later protocol step. Unconditional delete has the same problem when a path is
deleted and subsequently reused.

GlassDB already retains opaque object revisions for validation and conditional
writes. S3 and GCS also support deleting the current object only when its
[ETag](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-deletes.html)
or
[generation](https://docs.cloud.google.com/storage/docs/request-preconditions)
matches a supplied revision. Maintenance code normally reads the transaction
log, structural record, or node it intends to reclaim before deleting it, but
currently discards that observation at the delete boundary.

## Decision

Make every backend mutation conditional:

```text
write_if_not_exists(path, value)
write_if(path, value, expected_revision)
delete_if(path, expected_revision)
```

Remove unconditional `write` and `delete` from `Backend` and from the cached
store. Do not expose delete-if-anything-exists: deletion always names the exact
semantic state the caller intends to remove. An opaque revision is a content
validator, not a unique mutation identifier; rewriting equivalent contents may
be indistinguishable, consistent with ADR-036's state-based cache semantics.

A caller must retain a present observation until deletion or read the object
again. Transaction-log GC, structural recovery, and orphan-node cleanup carry
that observation to `delete_if`; an extra read is acceptable on a cleanup path
that otherwise lacks one.

Deletion outcomes update knowledge as follows:

- success proves absence after the operation's invocation, advances the
  expected observation's evidence to that invocation, and installs absence;
- `NotFound` is successful convergence on definitive absence and also makes the
  expected observation obsolete;
- a clean precondition failure makes the expected observation obsolete but
  does not identify the current state; and
- an ambiguous outcome is `Unavailable` and leaves the path uncertain under
  ADR-009.

For every mutation, `Unavailable` means the request may have applied. Any other
backend error exposed after dispatch must mean the backend knows the mutation
did not apply; an implementation that cannot make that distinction returns
`Unavailable`.

Database metadata continues to use create-if-absent. Backend conformance tests
and benchmarks seed state through create and CAS rather than an unconditional
overwrite.

## Discarded options

### Retain unconditional mutations for selected callers

Path-uniqueness conventions do not make a delayed mutation safe: they are easy
to weaken as layouts evolve, and they cannot distinguish an old request from a
new semantic state at the same path. Keeping the methods would also leave a
dangerous escape hatch below the cache protocol. Tests and benchmarks do not
justify a production primitive the engine does not need.

### Delete whichever state currently exists

An existence-only condition such as S3 `If-Match: *` prevents deleting absence
but can still remove a state the caller never observed. Exact revision matching
is required even when current cleanup paths use immutable or nominally unique
objects.

### Emulate conditional delete with read then delete

A separate read and unconditional delete has a time-of-check/time-of-use race:
the object can change between the two calls. Both supported providers expose an
atomic revision condition, so emulation would weaken the backend contract for
no compatibility benefit.

## Consequences

- Delayed writes and deletes cannot overwrite or remove a different semantic
  state.
- Backend implementations map every engine mutation to a native conditional
  object-store operation; no read-then-mutate emulation is permitted.
- Cleanup APIs must propagate revisions and may perform an additional read.
- A conflict may invalidate old knowledge without revealing the winner, so a
  caller that needs the current state performs an explicit read.
- Removing two trait methods is a breaking change for external backend
  implementations and for middleware that mirrors `Backend`.
- Conditional deletion can still be in doubt after a lost acknowledgement;
  ADR-009 remains authoritative for recovery.
