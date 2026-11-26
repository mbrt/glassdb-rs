# ADR-105: A node state never repeats

## Status

Accepted — implemented. Extended to collection records by
[ADR-106](106-a-collection-record-state-never-repeats.md).

Refines [ADR-081](081-in-doubt-cas-stays-in-doubt-until-fenced.md): any change
of a leaf fences an in-doubt CAS, and the coordinator fences one itself before
it plans. Refines [ADR-095](095-the-record-lists-every-lock-of-the-identity.md):
it removes the two causes of stray holders that ADR-095 kept. Refines
[ADR-079](079-gc-safety-does-not-depend-on-time.md): its proof no longer needs
stray holders.

## Context

A backend can apply a conditional mutation after it reported `Unavailable`, as
long as its precondition holds when it applies
([ADR-043](043-causally-coordinated-backend-operations.md)). On the memory
backend and on S3, a revision depends only on the content. So when a node comes
back to earlier content, it also comes back to the earlier revision, and a late
CAS that expected that revision can land. A lock that is taken and released does
this.

The protocol works around this in several places:

- An in-doubt leaf CAS is fenced only by a change that cannot be undone: a
  higher membership generation, or a new current writer (ADR-081). While the
  leaf can come back, the CAS stays in doubt, so a direct commit that cannot
  stage again reports `Error::InDoubt`.
- A gate installation increases the membership generation, so that its removal
  never restores the earlier encoding
  ([ADR-073](073-merge-nodes-into-right-sibling.md)).
- Two causes of stray holders stay (ADR-095). A late lock CAS of a committed
  identity lands after the write-back, when the leaf came back to the state
  that the CAS expected. A late lock CAS of an aborted identity lands on a leaf
  that nothing changed after the CAS was sent. Each stray holder costs a grace
  wait, and its wound is blind
  ([ADR-104](104-a-blind-wound-does-not-prove-that-the-identity-never-committed.md)).

ADR-081 rejected a counter that every CAS increases, because it changes the leaf
format and adds bytes to every leaf. These workarounds cost more than those
bytes.

## Decision

### Each node change increases its node sequence

A node stores its **node sequence**: the number of changes of the node since its
creation. A create stores 0. Each replace stores the node sequence of the
expected state plus one. The node store sets it, so a caller cannot forget it or
set it wrong.

So a state of a node never comes back while the node exists, and on every
backend a CAS can land only on the exact state that it expected. Each node path
is created once, because node IDs and collection IDs are never reused.

### Any change fences an in-doubt CAS

A loaded leaf fences an in-doubt CAS when its node sequence differs from the one
that the CAS expected. A lost CAS fences it in the same way.

### The coordinator fences an in-doubt CAS before it plans

When a round loads a leaf in the state that an in-doubt CAS of the round
expected, that CAS has not landed, but it can still land. The round then writes
the loaded leaf again, which changes only the node sequence. Then it loads the
leaf again. A lost fence also fences the CAS. An in-doubt fence leaves the
decision to the next load.

So a round plans only on a leaf that fences each of its in-doubt CASes. A member
can still find that its CAS landed earlier, but its CAS cannot land after the
load. A member that does not stage again, for example because it waits for a
holder, gets a definitive outcome.

### A lock CAS that stays in doubt keeps the owner operation unresolved

A round can still end with a CAS in doubt: when its retry budget ends, or when a
backend error stops it. An acquisition in that state reports that its lock CAS
can still land. The owner then keeps the owner operation unresolved, as for a
dropped acquisition, so the identity is pinned as wounded
([ADR-059](059-pin-foreign-wounds-until-owner-retirement.md)). GC never deletes
the record of a pinned identity, so the late CAS cannot leave a stray holder.

## Limits

A node that is deleted and then created again late repeats its first state. This
happens only to an object that nothing reads: a split sibling that recovery
deleted, or a node of a dropped collection.

The node sequence does not apply to transaction records. A wound on an empty
record path cannot tell a deleted record from one that never existed. ADR-104
covers that case.

## Consequences

- Stray holders cannot exist. A lock CAS of a committed identity cannot land
  after its write-back, and a lock CAS of an aborted identity cannot land after
  the owner retired. The simulation checks that no leaf keeps a holder whose
  record GC deleted, also with transport failures and slow mutations. A lock
  whose owner crashed before its first record write is not a stray holder: it
  has no record to delete, and a later writer wounds it (ADR-024).
- A direct commit whose CAS was in doubt and that cannot stage again reports a
  definitive outcome, instead of `Error::InDoubt`, in more cases.
- Recovery from an in-doubt CAS on an unchanged leaf costs one more CAS, one
  more load, and one attempt of the retry budget of the round.
- A node is up to 11 bytes larger. The field costs nothing at creation, 2 bytes
  up to 127 changes, and 3 bytes up to 16383 changes.
- The membership generation keeps its meaning. Merges still compare it, so a
  write that does not change the key membership does not stop a merge.

## Alternatives considered

- **Rely on backend revisions that never repeat.** GCS generations never repeat,
  but the memory backend and S3 depend on content.
- **Reuse the membership generation.** Merges compare it, so an increase for
  every write would make merges fail on any write.
- **Let GC fence the late lock CASes.** GC does not know which lock CASes were
  in doubt. The record would have to keep the leaf and the node sequence of each
  one.
- **Keep the in-doubt CAS until a later stage lands (ADR-081).** A member that
  waits or conflicts stages nothing, so its CAS can stay landable without a
  limit.
