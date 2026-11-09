# ADR-092: Key accesses cover the binding of their collection

## Status

Accepted.

Refines [ADR-047](047-transactional-collection-management.md),
[ADR-089](089-directory-loads-do-not-wait-for-readers.md), and
[ADR-090](090-directory-holds-last-for-the-identity.md). It changes which
directory reads of a commit take a directory lock, and it lets some
transactions with directory reads use the direct commit.

## Context

A transaction body that opens a collection by name reads the binding in the
collection record of the parent. ADR-047 treats each directory read like a key
read under strict two-phase locking. The locked commit takes a directory read
lock on the parent, and validation loads the collection record again. Any
directory read also makes the transaction use the locked commit, never the
direct commit.

The read lock is a CAS on the collection record of the parent, and its release
is a second CAS. All transactions that open children of one parent contend on
that one record. In one database instance, `CachedStore` runs one backend call
for each path at a time, so the CASes of all workers on the record run in
sequence. Most of them fail, because their workers built them from the same
cached observation, and each failure waits for a backoff.

The `open-in-body` perfbench experiment
(`hack/perf/experiments/open-in-body.toml`) opens the collection in each
read-modify-write body. Its throughput is orders of magnitude lower than the
throughput of the same cells with the open outside the transaction, and most of
its writes are failed CASes on the record of the root collection. A test change
with no directory read lock removes most of the difference. The rest is the
locked commit: with the open outside the transaction, the transaction has no
directory read and uses the direct commit. `docs/guides/perf.md` has the
results.

A transaction that opens the collection outside its body is correct with no
directory lock. Its collection handle is bound to the collection ID. If a drop
of the collection commits, the next access through a node of the collection
finds the drop intent, and the access reports the handle as stale.

The same fence protects a binding that the body reads. A binding `name → X`
can change only through a drop of X, because there is no rename and
collection IDs are never used again. Before a drop of X commits, it installs a
drop intent on every node of X. Every lock acquisition on a node first
resolves the drop intent of the node. A committed intent reports
`StaleCollection`, and a pending intent takes part in wound-wait. So while a
transaction holds a lock on a leaf of X, a drop of X cannot commit. A direct commit does not continue
on a leaf with a live drop intent, and its CAS fails if a drop intent was
installed after its read of the leaf. So a direct commit on a leaf of X that
lands comes before the drop.

## Decision

### A key access covers the binding of its collection

A directory read that finds a binding to collection X is a covered binding
read in a commit pass when one of these conditions is true:

- The pass holds a key lock on a leaf of X at its commit point.
- The pass is a direct commit, and its leaf is a leaf of X.
- Another covered binding read of the pass is in the collection record of X.

The third condition covers nested paths. For `a/b`, the pass reads `a → A` in
the root collection and `b → B` in the record of A. A drop of A needs A to have
no children. The transaction that drops A reads the list of bindings of A, and
that read takes a read lock. So it cannot drop A while `b → B` exists, and the
leaves of B keep `b → B`.

The commit pass decides coverage from the point accesses of its body
execution, because each point access locks a leaf of its collection, or is part
of the CAS of a direct commit on that leaf. Range scans do not count, because a
scan of an empty range can hold no leaf. Each body replay can have different
directory reads and key accesses, so each commit pass decides again.

The locked validation of the error outcome of a read-only transaction also
holds the key locks of its point accesses until the outcome is decided. It
uses the same rule.

### A covered binding read takes no directory lock

A covered binding read takes no directory lock, and validation does not load
the collection record for it. A pass whose directory reads are all covered can
use the direct commit, with the other conditions of
[ADR-061](061-atomic-logless-single-leaf-commits.md).

### A stale binding replays the body

A body loads the collection record from the cache, so it can read a binding of
a collection that a committed drop removed. The access to a leaf of that
collection then reports `StaleCollection`. The commit loads the directory
reads again at a new validation barrier. If a directory read changed, the body
replays, so the replay opens the current binding. This is the existing
behavior for `StaleCollection` during a commit.

### Other directory reads do not change

These directory reads keep the directory read lock and the directory
validation:

- A read that finds no binding, also in a create-if-absent. No leaf of a
  missing collection can block a create.
- A read of the list of bindings.
- A read that finds a binding to X when the pass has no key access to X.

Optimistic validation does not change. It holds no lock, so the argument above
does not apply to it.

## Correctness

- **One instant for all reads.** A transaction is serializable when all reads
  of its final body execution are true at its commit point. Key locks keep the
  key reads true until after the commit point. A covered binding read stays
  true for the same interval, because a drop of X cannot commit while the pass
  holds a lock on a leaf of X. A direct commit validates and commits in one
  leaf CAS, and that CAS fails if a drop intent is on the leaf.
- **Control flow that depends on reads.** A body can open X or Y depending on
  a key read. The commit pass decides coverage from the reads that the body
  made. A binding that the body did not read is not in the access set and does
  not need protection. If the key read is invalidated, the body replays and
  makes new reads.
- **No new wait cycle.** A covered binding read adds no wait for a directory
  lock holder. A wait for a drop intent is a wait at the node, and wound-wait
  resolves it in the same way as for a collection handle that a transaction
  opened outside its body. A drop takes all its directory locks before it
  installs a drop intent, so it holds no drop intent while it waits for a
  directory lock. The order of acquisition of ADR-090 does not change.
- **Removed nodes.** After a drop commits, GC can delete the nodes of the
  collection. An access to a missing node reports `StaleCollection`, not an
  empty collection (ADR-047), so a stale binding replays the body. A direct
  commit can find its leaf missing after an in-doubt CAS on it. The drop can
  have removed a CAS that landed, so the missing leaf does not resolve the CAS,
  and the commit stays in doubt
  ([ADR-081](081-in-doubt-cas-stays-in-doubt-until-fenced.md)).

## Consequences

- A transaction that opens a collection in its body and accesses its keys
  costs the same backend operations as a transaction that opens the
  collection outside its body. The `open-in-body` cells should reach the
  throughput of the same cells without the open.
- A drop can commit while transactions that read its binding run, if they
  have not locked a leaf of the collection yet. They then find the drop intent
  and replay their body. Collection changes can be slow, so this is acceptable.
- A covered binding read does not wait for a pending writer of the parent
  directory, for example a create of a sibling.
- Reads that find no binding, reads of the list of bindings, and bindings
  with no key access still contend on the collection record of the parent.
- The commit must decide coverage from exactly the leaves that it locks or on
  which it does a CAS. An error in that set is a serializability error, so the
  simulation tests must cover drop races against direct commits, locked
  commits, blind writes, range scans, and removed nodes.

## Alternatives considered

### Combine directory CASes in rounds

A coordinator for each collection record, like `LeafCoordinator` for leaves,
can add or remove the read holds of many transactions with one CAS. This keeps
the protocol, but each transaction still waits for two extra rounds, the
locked commit stays mandatory, and the coordinators of different database
instances still contend on the record. The best possible result is near the
second row of the table.

### Validate all directory reads with no lock

Validation can load the collection record under the held key locks and not
take a directory read lock. This also covers reads that find no binding and
reads of the list of bindings. But validation must then wait for a pending
writer of the directory while it holds key locks, which breaks the order of
acquisition of ADR-090. It also does not let the transaction use the direct
commit.

### Reject a CAS on a known old revision in the cache

`CachedStore` can reject a CAS locally when its cache already holds a newer
revision of the path. This removes the failed backend calls in one database
instance, but two CASes for each transaction on one record still limit all
instances together to about one transaction for each two write latencies. It
can still help the reads that keep the lock, as a separate change.
