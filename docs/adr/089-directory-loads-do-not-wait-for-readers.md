# ADR-089: Directory loads do not wait for readers

## Status

Accepted.

Refines [ADR-047](047-transactional-collection-management.md). It changes
which directory holders a load of a collection record resolves.

## Context

A transaction body that opens or lists a collection loads the collection record
of the parent. Its locked commit then holds a read lock on that directory, and
its validation loads the record again.

Each of these loads waited for the final status of every directory holder of
another transaction, also for holders of a read lock. A pending reader cannot
change the child bindings or the directory generation, so the wait did not
protect any result. But it made a directory read lock work like an exclusive
lock for every other transaction that opened a child of the same parent.

The `open-in-body` perfbench experiment opens the collection in each
read-modify-write body. The open made the throughput fall by orders of
magnitude, and with five database instances the workers did not complete the
cells. `docs/guides/perf.md` has the results.

The wait also made wound-wait for directory lock requests unreachable. A lock
request loads the record first, and the load had already waited for all holders
of other transactions, so the request never found one.

## Decision

A load of a collection record does not resolve the holders of a directory read
lock. It returns the record with its pending readers. Write holders and the
topology freeze are resolved as before.

A write lock request that finds readers uses wound-wait: an older requester
wounds a younger reader, and a younger requester waits. When a reader has a
final status, the requester removes it: it removes an aborted reader, and it
completes the write-back of a committed reader.

This follows the principle that collection changes can be slow, but ordinary
operations, such as reads of collections, must be fast.

## Consequences

- Readers of one directory do not wait for each other. On five database
  instances, the cells of `open-in-body` complete.
- A reader no longer removes the stale holds of readers with a final status.
  Write lock requests and GC remove them. A stale read hold does not delay a
  load.
- New readers can join while a writer waits. Priority comes from the start
  time, so a waiting writer becomes older than each new reader, and wounds it.
- A read lock is still a CAS on the collection record of the parent, and its
  release is another CAS. All transactions that open children of one parent
  contend on that record. This is not changed here.

## Alternatives considered

### Keep the wait for readers

The wait gives no correctness property, and it serializes all transactions
that open children of the same parent. This option was not chosen.

### Resolve readers that already have a final status

A load could read the status of each reader without waiting, and remove the
readers with a final status. This keeps the records clean, but it adds a status
read for each reader to every load, which is the frequent operation. The
infrequent write lock request does this work instead.
