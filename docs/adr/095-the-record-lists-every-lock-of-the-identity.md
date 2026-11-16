# ADR-095: The record lists every lock of the identity

## Status

Accepted — implemented.

Refines [ADR-079](079-gc-safety-does-not-depend-on-time.md). It removes three
causes of stray holders, and it keeps the GC safety rule of that ADR. Refines
[ADR-059](059-pin-foreign-wounds-until-owner-retirement.md): a `Wounded` record
holds no recovery manifest.

## Context

A stray holder is a lock of T in a leaf after GC deleted the record of T, on a
key whose value T does not hold (ADR-079). Each stray holder costs the next
transaction on its key one missing-record grace. It also leaves one `Wounded`
record that nobody acknowledges. GC never deletes that record, so these records
grow without a limit.

The owner keeps one lock list in memory for each identity. Commit writes the
list to the committed record. The refresher writes it to the pending record. A
stray holder is a lock that is not in the list when GC reads the record. There
are five causes:

1. **A body replay drops locks.** Each commit pass replaces the list with the
   locks of that pass. A key that an earlier pass of the same identity locked,
   and the last pass did not, is not in the list. Write-back removes the other
   locks of T only in the leaves of the last pass.
2. **A failed acquisition records nothing.** A pass that stops with a conflict,
   a full leaf, or a wound keeps the locks that landed. The owner updates the
   list only after a pass acquires all its locks.
3. **An abort record copies an old list.** A wound, and later the `Aborted`
   record of the owner, copy the list of the stored record. The refresher
   writes the pending record only from time to time, so its list can be older
   than the list of the owner.
4. **A late lock CAS of an aborted T.** An in-doubt lock CAS counts as a
   conflict. The owner operation completes, and the owner can write `Aborted`.
   The CAS can land after GC deleted the record.
5. **A late lock CAS of a committed T.** The CAS lands after the write-back,
   when the leaf came back to the revision that the CAS expects.

GC can fence a late lock CAS only with a change that cannot be undone
([ADR-081](081-in-doubt-cas-stays-in-doubt-until-fenced.md)). A lock CAS does
not change the writer of an entry. A release by GC does not change it either.
So GC cannot fence causes 4 and 5.

## Decision

### One lock set for each identity

The owner keeps one lock set for the identity, for all commit passes. It adds
a lock to the set before it sends the CAS that takes the lock. It never removes
a lock from the set while the identity lives. A key that the set already holds
adds nothing. A stronger lock type on that key replaces the weaker one.

Each record that the owner writes copies this set: the pending record, the
committed record, and the `Aborted` record. This removes causes 1 and 2.

The set has no size limit. It grows only when a body replay locks keys that no
earlier pass locked. The limits of each execution bound one pass, and most
transactions do not replay.

The set holds keys, not leaves. GC routes each key to its current leaf, so the
set stays correct after splits and merges.

### The owner writes `Aborted` from its own set

When the owner writes `Aborted`, it takes the recovery manifest from its own
memory, not from the stored record. The Monitor tracks the owned record until
the owner closes it, also when the owner acknowledges a wound in the
background. This removes cause 3.

### A `Wounded` record holds no recovery manifest

A party that writes `Wounded` writes only the identity and the status. GC never
reads a `Wounded` record past its status (ADR-071). A reader needs only the
status to drop the holder. This makes the record minimal in size. Before this
decision, only the `Aborted` record of the owner used the copied manifest, and
that record now comes from the owner.

### GC releases every lock of a committed record that holds no value

Before GC deletes a committed record, it releases the locks in the set on keys
that T did not write. It does this through the coordinator, as for membership
locks (ADR-029). A held lock on a key that T wrote still keeps the record until
write-back or help-forward removes it, as before.

### Late lock CASes stay a known rare case

This decision does not fence causes 4 and 5. Both need a lock CAS that lands
after the retention period, which starts at the last update of the record. The
retention period makes them rare. They cost the same as before: one grace wait
and one `Wounded` record that nobody acknowledges.

## Consequences

- The causes that come from the normal protocol are gone. Only a lock CAS that
  lands after the retention period can leave a stray holder.
- A committed or aborted record can list more keys than its last pass locked.
  GC then releases more locks, but only for identities that replayed.
- GC writes to leaves for the locks that a committed T took on keys that it did
  not write, when write-back did not remove them. This work is off the commit
  path.
- A wound writes a smaller record, and its content does not depend on the age of
  the pending record.
- After this decision, nothing reads the lock list of the pending record. The
  pending record keeps the list for later work: a party that wounds an identity
  can release the locks in the list after the wound is durable.
- A simulation check can now require that, after the workload stops and a full
  GC runs, no leaf has a holder without a record. The check must allow the
  holders of a lock CAS that landed late.

## Alternatives considered

- **Release the dropped locks when the body replays.** The owner would release
  each lock that the new pass does not take again. This frees contended keys
  sooner, but it adds writes during a retry. GC does the same work later and
  off the commit path.
- **Record leaves instead of keys.** A merge moves all entries of a leaf to its
  right sibling, so a recorded leaf can stop holding the entries. Keys route to
  the current leaf.
- **Limit the size of the set.** When the set grows past a limit, the owner
  could renew the identity, so the old record lists the locks and GC releases
  them. The set grows only for a body whose keys change on each replay. The
  number of replays has no limit now either.
- **Keep the aborted record until the leaf fences the CAS.** On a leaf that gets
  no writes, the fence can take forever, and GC checks the record again each
  time it finds it. ADR-071 rejects this repeated cost.
- **Pin the identity when a lock CAS is in doubt.** The owner would treat an
  in-doubt lock CAS as unresolved, as it does for an abandoned acquisition. A
  late CAS then leaves a holder of a `Wounded` record, which costs no grace
  wait. But the owner pins a record each time a CAS is in doubt and the owner
  gives up, not only when the CAS lands late.
- **Let GC release the locks of `Wounded` records.** A late effect can land
  until the owner retires, so GC would release the locks again each time it
  finds the record (ADR-071). A holder of a `Wounded` record costs no grace
  wait, because a reader drops it at once.
