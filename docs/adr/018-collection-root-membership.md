# ADR-018: Collection root and membership coordination

## Status

Accepted — implemented (`glassdb-storage::CollectionRoot` + the membership
protocol in the transaction engine)

## Context

[ADR-016](016-object-storage-native-layout.md) makes the **shard** objects
([ADR-017](017-shard-object.md)) the home of per-key lock and version state: a
key *exists* iff its shard entry has a `current_writer` and is not `deleted`.
Reading or writing an existing key touches only its shard, with no central
coordination — that is the whole point.

But the *set* of keys is not a per-key fact, and neither is the set of
subcollections. Two operations need a consistent view of membership:

- **Listing** keys (or subcollections) must return a serializable snapshot. With
  the directory spread across `C` shards, a naïve scan that reads shard 0 … `C-1`
  can miss or double-count a key that a concurrent transaction creates or deletes
  mid-scan — a phantom.
- **Create / delete** must be ordered against concurrent listers (and each
  other), so that a committed list reflects exactly the creates/deletes
  serialized before it.

Today (v1) the collection-info object (`{prefix}/_i`) is just a one-byte
existence marker, and `Collection::keys` / `Collection::collections`
(`crates/glassdb/src/collection.rs`) enumerate by a raw, non-transactional
backend `list` over the `_k/` and `_c/` prefixes. v2 has no per-key value objects
to list, so that approach is gone, and we want listing to be properly
serializable rather than best-effort.

ADR-016 already fixed the direction: the **collection root** records existence,
the shard count, and the subcollection list, and is the *membership-coordination
point* — create/delete lock it, listing validates against its version. This ADR
makes that concrete: the root's **format**, where the **key directory** lives,
and the exact **coordination contract** for create / delete / list.

Out of scope, deferred to their ADRs: the commit / CAS *sequencing* that makes a
multi-object membership change atomic (ADR-020), lease and wound-wait *mechanics*
(ADR-021, reframing [ADR-002](002-wound-wait-locking.md)), and reclamation
(ADR-022). This ADR defines *what* state the root holds and *which* locks /
versions a membership operation takes and why.

## Decision

### Collection root object

The root keeps the existing path `{prefix}/_i` (the `_i` marker and
`paths::collection_info` are reused), but its body changes from the marker bytes
to a protobuf `CollectionRoot`, encoded with the existing toolchain like the
shard and tx-log:

```proto
message CollectionRoot {
    // The shard count C this collection was created with. Part of the on-disk
    // format; validated against the compile-time SHARD_COUNT on open.
    uint32 shard_count = 1;
    // Child collection names (raw bytes), the subcollection directory.
    repeated bytes subcollections = 2;
    // Membership lock serializing create/delete (write) against listing (read).
    Lock.LockType membership_lock = 3;
    repeated bytes membership_locked_by = 4;
}
```

- **Existence**: a collection exists iff its root exists. `create` is
  `write_if_not_exists` of a root with `shard_count = C`, an empty subcollection
  list, and no membership lock; a lost race surfaces as `Precondition` and is
  treated as success, as today.
- **`shard_count`** is recorded so a client built with a different compile-time
  `C` fails fast on open (a format-version mismatch) instead of silently using
  the wrong key→shard mapping. With a single fixed `C` this is a guard for future
  migrations and a contract for tooling/GC, not a runtime option.
- The root is **small** and rewritten only by membership operations, so making it
  the CAS unit for membership is cheap.

### The key directory lives in the shards, not the root

Confirming ADR-016's working assumption: each shard is the directory for the keys
that hash to it (the entries with `current_writer`/`deleted`), and the root holds
**no** per-key state. The asymmetry with subcollections (which *are* listed in
the root) is deliberate:

- Keys can be numerous (the 50k-key benchmark); a central key list would make the
  root huge and rewritten on every create/delete. Sharding keeps a key
  create/delete touching exactly one shard plus a root version bump.
- Subcollections are expected to be few, so an explicit list in the root costs
  little and buys *consistent enumeration without a scan* and a single membership
  token shared with key listing. (Unbounded subcollection fan-out is an accepted
  MVP limitation; sharding the subcollection directory is a later option.)

Listing keys therefore enumerates the **existing** shard objects via a backend
`list` over the `_s/` prefix (lazily-created shards that were never written simply
don't exist and read as empty), conditionally GETs each, and unions their live
entries. Cost is `O(non-empty shards)`: cheap for small collections, ~`C` GETs
for a full one — inherent to listing every key.

**Considered and deferred — a shard-occupancy set in the root.** The root could
carry a small set/bitmap of which shards are non-empty (≤ `C` bits), letting a
lister skip the `list` and GET only populated shards. Rejected for the MVP
because the occupancy is *derivable* from the shards, so storing it makes a
denormalized second source of truth that must stay in sync across create, delete,
**and** GC. The failure mode is asymmetric: a stale "empty" bit makes listing
**skip a populated shard** — a lost key in a supposedly serializable list (a
correctness bug) — while it only saves the *discovery* `list`, not the dominant
per-shard GETs, and `list` is a primitive the slimmed `Backend` keeps anyway. It
also couples the background reclaimer to the foreground membership lock. If
revisited, the safe framing is to treat the set as a **hint** validated by the
root version (every membership change already bumps the root, §below), never as
authoritative state, so a stale bit can never drop a key.

### Membership changes and the root invariant

A **membership change** is any write that toggles a key's existence — a *create*
(absent/tombstoned → exists, marked by the `create` lock of ADR-017) or a
*delete* (exists → tombstoned) — plus subcollection add/remove. A plain overwrite
of an existing key is **not** a membership change and never touches the root; this
keeps the read/write hot path free of the root, as ADR-016 requires.

The load-bearing invariant:

> **Every membership change writes the root** (by taking and releasing its
> membership lock). Therefore the root's object **version** (generation / ETag) is
> a complete summary of all membership changes to the collection.

This is what makes a cross-shard scan validatable with a *single* comparison
(below).

### Create / delete: pessimistic on the root

A create or delete transaction acquires the root's **membership write lock**
(`membership_lock = write`, its txid in `membership_locked_by`) in addition to the
key's shard lock (the `create` lock for a create, the write lock for a delete,
per ADR-017). Holding the root write lock for the operation's duration:

- prevents phantoms — it conflicts with listers that have escalated to a read
  lock, and with other membership writers;
- gives membership changes the same wound-wait fairness and liveness as the rest
  of S2PL, rather than a raw CAS-retry race (concurrent creates already serialize
  on the root object's CAS regardless, since they all must bump its version; the
  lock just makes that serialization fair and starvation-bounded).

All membership changes in a collection — key *and* subcollection — share this one
lock. That is coarse (unrelated creates serialize), and the accepted MVP trade:
membership churn is far rarer than reads/writes of existing keys, which the root
never sees. Splitting into separate key- vs subcollection-membership tokens, or
sub-sharding the membership lock, is a later optimization.

Acquiring two locks (root + shard) is multi-object locking; deadlock is prevented
by wound-wait and, in the rare equal-priority case, the serial sorted-by-path
fallback ([ADR-002](002-wound-wait-locking.md)) — the root and shard paths are
deterministic and sortable. The exact acquisition sequence and its atomic commit
are ADR-020.

### Listing: optimistic on the root version, read-lock fallback

Listing keys (or subcollections) is **optimistic** and lock-free in the common
case:

1. Read the root; record its version `V0` (and, for subcollections, its list).
2. Scan: for keys, `list` + GET the existing shards and union live entries; for
   subcollections, take the names from the root.
3. Re-read the root version `V1`. If `V1 == V0`, the snapshot is consistent —
   object generations are monotonic and every membership change bumps the root,
   so equal endpoints prove no create/delete/subcollection-change committed during
   the scan. Return the result.
4. If `V1 != V0`, a membership change raced the scan; retry from (1) a bounded
   number of times.

Value writes to existing keys change shards but not membership and never touch the
root, so they never invalidate a listing — the read-set validation collapses from
"every shard version" to the **single root version**, which summarizes the whole
membership read set (the analog of the cycle observer validating a read set with
one comparison).

Under sustained membership churn the optimistic retry can starve. After the retry
budget is exhausted, the lister **escalates to a root read lock**
(`membership_lock = read`; read locks are shared, so concurrent listers coexist
while membership *writers* wait or are wounded), scans once against the now-stable
membership, and releases. This guarantees progress and is the pessimistic
baseline the optimism shortcuts.

### Subcollections

The root's `subcollections` list is the authoritative directory of child
collections; enumeration is the listing protocol above (OCC on the root version,
read-lock fallback), so it is serializable without a `_c/` prefix scan.

- **Create** a subcollection: add its name to the parent root's list (under the
  parent's membership write lock) and `write_if_not_exists` the child's own root,
  committed together so the name and the child root appear atomically (sequencing
  in ADR-020). `Collection::collection(name)` keeps deriving the child prefix via
  `paths::from_collection`.
- **Delete** a subcollection: remove the name under the parent membership write
  lock. Tearing down the child's shards / transaction objects is reclamation,
  deferred to ADR-022; v1 has no collection-delete API, so this only fixes the
  membership contract, not a full recursive teardown.

## Consequences

- Listing becomes properly serializable: a single root-version comparison
  validates an entire cross-shard scan, because every existence change is funneled
  through the root. Read-mostly listing stays lock-free; only churn forces the
  read lock.
- The hot path is untouched: reads and overwrites of existing keys never read or
  write the root. Only create/delete and listing do, exactly the operations
  ADR-016 said may serialize on membership.
- Membership coordination is coarse — one lock per collection covers all key and
  subcollection create/delete — trading concurrency for a simple, obviously
  correct MVP, with finer-grained tokens available later.
- The root gains a real format (`CollectionRoot` protobuf) and a recorded
  `shard_count`, pinning the key→shard mapping per collection and giving a fail-
  fast guard against a future `C` change. A new golden vector is needed, and the
  v1 one-byte marker is dropped.
- `Collection::keys` changes from a `_k/` prefix scan to a shard `list` + GET +
  union with root-version validation; `Collection::collections` reads the root
  list instead of scanning `_c/`. Listing a large collection costs ~`C` shard
  GETs — the inherent price of enumerating every key, and a motivation to keep the
  optimistic path cache-friendly.
- Unbounded subcollection fan-out and full collection teardown remain MVP
  limitations (deferred to a later directory-sharding step and ADR-022).
- The cross-object membership operation (root + shard, or parent root + child
  root) needs the atomic commit protocol of ADR-020; this ADR is correct only
  paired with it.
