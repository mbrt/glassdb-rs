# ADR-090: Directory holds last for the identity

## Status

Accepted.

Refines [ADR-024](024-hold-and-wait-conflict-resolution.md),
[ADR-047](047-transactional-collection-management.md), and
[ADR-065](065-renewed-transaction-identity-on-serial-fallback.md). It needs
[ADR-089](089-directory-loads-do-not-wait-for-readers.md).

## Context

A locked commit pass takes the directory locks of the transaction, then its key
locks, and then validates its reads. If validation fails, the body replays.
ADR-024 keeps the key holds for the replay, so the replay cannot lose the same
race again. The directory holds did not stay: the pass released them, the body
replayed under the same identity, and the next pass took the directory locks
again.

### A released hold looks like the hold of the committed pass

A peer database instance can cache a collection record while the first pass
holds its directory lock. Between the two passes, another transaction can change
the directory, for example drop the child that the first pass saw. The second
pass validates against the new bindings and commits. The cached record of the
peer then shows the hold of the first pass together with the bindings from
before the drop.

Committed directory write-back assumes that a record that lists the committed
identity as a holder shows the bindings that its committed pass validated. A
peer that resolves the holder as committed helps its write-back from the cached
record. The committed create finds the old binding, and write-back returns
"committed collection create conflicts with a newer binding" to the client. A
deep PCT sweep of
`shared_collection_history_holds_under_contention_and_failures` found it at seed
12298.

A key hold ends only when its identity has a final status: after a wound, after
the serial fallback of ADR-065, or in GC. A cached key hold of an ended identity
resolves as aborted, so the problem does not occur for keys.

### The replay took directory locks after key locks

Directory locks have no deadlock timeout and no serial fallback, and a wait for
a directory write holder does not wound it. Only the order of acquisition
prevents a wait cycle. A replay kept its key holds and then took its directory
locks again, so it could wait for a directory while it held keys. With equal
priorities and a peer in serial acquisition, which has no timeout, that wait
can be part of a cycle.

## Decision

### Keep directory holds for the replay

A live identity never releases a directory hold. It keeps its directory holds,
like its key holds, for a body replay after a failed validation. The holds end
at write-back after the commit, or after the identity has a final status.

The transaction owner records the directory locks that its identity holds. A
pass that needs only these locks does not take them again.

### Renew the identity for a new directory lock

The order of acquisition for one identity is all directory locks, in address
order, before all key locks. A replay holds key locks, so it must not wait for a
directory lock. If a pass of an identity that was engaged in an earlier pass
needs a directory lock that the identity does not hold, or a write lock where it
holds a read lock, the owner ends the identity and continues under a renewed
identity. This is the transition of ADR-065. A transaction with collection
changes replays its body, because its prepared resources belong to the old
identity. A renewed identity holds no lock, so it takes all its locks in order.

### Body loads do not wait

A replaying body can hold directory and key locks, so a body load must not wait
for a directory holder. A directory load of a transaction body does not wait
for a pending holder or a pending topology freeze, in the same way as a key read
does not wait for a pending key writer. It resolves the holders and the freeze
that have a final status. A pending writer has not changed the record yet, so
the body sees the bindings from before the change. A pending drop has not
removed the collection yet. Validation under the held locks checks the reads,
and it waits for a pending freeze.

The load also ignores the holds of the identity of the body. The record does
not contain the pending changes of the identity, and the body reads them from
its own overlay.

## Correctness

- **A cached hold belongs to the committed pass.** Each directory hold of an
  identity is one continuous interval. While the identity holds the lock, only
  the identity can change the bindings, and it changes them only in the
  write-back CAS that also removes the hold. So each record that lists the
  identity has the bindings that its committed pass validated, and the binding
  checks of write-back are correct also for cached records.
- **No wait cycle.** An identity waits for a directory lock holder only in its
  first locking pass, before it holds a key lock, and only for directories at
  higher addresses than the ones it holds. Body loads do not wait for directory
  holders. Validation runs under the held directory locks, so the only foreign
  holders that it can find have a final status. So a transaction that holds a
  key lock never waits for a directory lock holder, and a wait chain through
  directory locks follows the address order and ends at a transaction that
  waits for no directory lock. Key waits keep the argument of ADR-024. Waits of
  validation for a topology freeze, and of key reads for a drop intent, do not
  change, and follow ADR-047.
- **Renewal makes no peer wait for a live identity.** The old identity has a
  final status before the renewed identity takes a lock, so a peer resolves its
  holds as aborted.

## Consequences

- The write-back error from a cached hold of an earlier pass cannot occur.
- A replay that needs the same directories takes no directory lock again, so it
  saves a release CAS and a lock CAS for each directory.
- A replay keeps write holds on its parents for the whole replay. The lock
  requests of other transactions on those directories wait for it. Their body
  loads do not wait: they see the bindings from before the change. Collection
  changes are rare, so this is acceptable.
- A replay that needs a new directory lock renews its identity. It loses its key
  holds and writes an abort record. Renewals count toward the serial fallback
  threshold of ADR-020.
- `docs/guides/perf.md` has the measured cost.

## Alternatives considered

### Read the record again before a conflict error

Before write-back reports a binding conflict, it could read the record again
under a currentness barrier taken after the commit. This removes the error, but
a cached hold still does not prove that it belongs to the committed pass, and
each decision that uses a cached directory hold needs the same check.

### Renew the identity after each failed validation with directory locks

Release the directory holds, but end the identity first, so that a cached hold
of an earlier pass resolves as aborted. The renewal also releases the key holds,
which removes the progress argument of ADR-024. Two transactions that read the
same key then fail validation in turn and renew again and again. In the
simulation, this multiplied the backend operations, and a perfbench cell that
opens a collection in each body did not complete.

### Release only write holds with a renewal

A pass can release a read hold, and a later pass of the same identity can take
a write hold on the same directory. The cached read hold then shows bindings
that the committed pass did not validate, so this has the same failure.
