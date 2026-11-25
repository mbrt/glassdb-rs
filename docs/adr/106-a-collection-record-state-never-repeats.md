# ADR-106: A collection record state never repeats

## Status

Accepted — implemented.

Extends [ADR-105](105-a-node-state-never-repeats.md) from nodes to collection
records. Refines [ADR-059](059-pin-foreign-wounds-until-owner-retirement.md):
an in-doubt CAS on a collection record also keeps the owner operation
unresolved.

## Context

ADR-105 made each state of a node unique, so a late lock CAS cannot land on a
node after its holder was released. Collection records did not get the same
change. A collection record holds three kinds of holder: the directory lock,
the topology participants, and the topology freeze.

On the memory backend and on S3, a revision depends only on the content. Many
changes of a collection record bring it back to earlier content:

- A directory reader takes and releases the directory lock. A read lock
  changes no binding, so the directory generation does not change.
- An aborted creator takes and releases the directory write lock.
- A structural change joins and leaves the topology participants.

The simulation shows both causes of stray holders that ADR-105 removed for
nodes. Its end-of-run check, extended to collection records, finds stray
directory holders and stray topology participants under transport faults:

1. A lock CAS or a join CAS is in doubt. The identity finishes, GC removes its
   holders and deletes its transaction record. Other identities then take and
   release the record, which comes back to the state that the late CAS
   expected. The late CAS lands.
2. A read-lock CAS is in doubt. The commit pass gets the error, and it
   completes its owner operation. That states that no write of the identity
   can land after retirement, but the CAS can. GC deletes the transaction
   record, nothing changes the collection record, and the late CAS lands.

A stray holder on a collection record has the costs that ADR-095 lists for a
leaf. Its resolution finds no transaction record, so it waits, and its wound is
blind (ADR-104). A writer that knows the holder as committed loads the record
again without end.

The directory generation cannot do this job. Directory validation compares it
(ADR-018), so an increase on each lock change would fail each validation that
overlaps a directory reader.

## Decision

### Each change of a collection record increases its record sequence

A collection record stores its **record sequence**: the number of changes of
the record since its creation. A create stores 0. Each replace stores the
record sequence of the expected state plus one. The collection store sets it,
so a caller cannot forget it or set it wrong.

So a state of a collection record never comes back while the record exists,
and a CAS can land only on the exact state that it expected. Collection IDs are
never reused, so each record path is created once.

The directory generation keeps its meaning. It changes only when a binding
changes.

### The collection store fences an in-doubt CAS before it decides again

When a CAS in a convergence of a collection record is in doubt, the store loads
the record again at a fresh barrier:

- If the record sequence differs from the expected one, the CAS can never land.
  The store decides again on the loaded state, as after a lost CAS.
- If the record sequence is the expected one, the CAS did not land yet, but it
  can still land. The store writes the loaded state again, which changes only
  the record sequence. A lost fence also fences the CAS. Then the store loads
  again and decides again.

Each decision on a collection record is idempotent on the state that it loads,
for example "already held", so a CAS that landed before the fence needs nothing
more.

### A CAS that stays in doubt keeps the owner operation unresolved

A convergence can still end with a CAS in doubt, for example when the fence
CAS meets a backend error too. The store then reports that the CAS can still
land, separately from other errors. The owner of the identity keeps its owner
operation unresolved, so a later abort pins the identity as wounded (ADR-059).
GC never deletes the record of a pinned identity, so the late CAS cannot leave
a stray holder.

Only a CAS that adds a holder needs this: a directory lock, a topology freeze,
and a topology join. A late CAS that removes a holder, for example a release,
a write-back, or a leave, can land only on the exact state that it expected,
and there it only removes the holder.

- A commit pass whose directory lock or topology freeze stays in doubt ends
  with an in-doubt error, as before. It does not complete its owner operation,
  as for a key lock CAS in doubt (ADR-105).
- A topology participant whose join stays in doubt is aborted as pinned in its
  finalization, instead of a commit.

## Limits

A collection record that is deleted and created again late repeats its first
state. This happens only to the record of a dropped collection, which nothing
reads.

## Consequences

- Stray holders cannot exist on collection records. The simulation checks
  collection records and leaves after a full GC, also with transport failures
  and slow mutations.
- A collection record is up to 11 bytes larger. The field costs nothing at
  creation.
- Recovery from an in-doubt CAS on an unchanged collection record costs one
  more CAS and one more load.
- A commit pass or a structural change whose collection record CAS stays in
  doubt pins its identity. GC keeps that record until the owner retires.

## Alternatives considered

- **Reuse the directory generation.** Directory validation compares it, so an
  increase on each lock change would fail validations.
- **Only add the record sequence.** It stops a late CAS after an ABA, but not a
  late CAS on a record that nothing changed (cause 2).
- **Fence until the CAS is definitive, and never pin.** During an outage the
  fence CAS is in doubt too, so a commit or a structural change would not end.
- **Keep the sequence in the generic cached store.** The cached store would
  then know a field of one codec. The node store and the collection store each
  own their own sequence.
