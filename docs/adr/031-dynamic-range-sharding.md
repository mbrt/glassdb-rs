# ADR-031: Dynamic range-partitioned sharding (B-link tree)

## Status

Proposed

Supersedes the fixed-hash key→shard mapping and fixed shard count of
[ADR-016](016-object-storage-native-layout.md) (the "fixed `C` shards" clause),
[ADR-017](017-shard-object.md) (FNV-1a `key & (C-1)` mapping, the fixed `_s/<i>`
addressing, the fixed `SHARD_COUNT`), and [ADR-018](018-collection-root-membership.md)
(the recorded fixed `shard_count` and the single coarse membership lock). The
shard _entry_ model (lock type, `locked_by`, `current_writer`, tombstone) from
ADR-017, the transaction object (ADR-019), the commit/write-back protocol
(ADR-020), wound-wait/leases (ADR-021), GC (ADR-022), and the shard-mutation
coordinator (ADR-028/029) all carry over unchanged.

[ADR-046](046-incarnation-addressed-collections.md) proposes replacing this
ADR's physical-root existence marker and name-only subcollection directory with
direct parent mappings and incarnation-unique physical prefixes. [ADR-047]
proposes transactional management and all-node deletion fencing on top, using
ADR-044's structural gate for per-node quiescence. The B-link topology and key
hot-path decisions here are unchanged.

[ADR-047]: 047-transactional-collection-management.md

## Context

Coordination state lives in a fixed set of `C = 1024` **hash** shard objects per
collection (ADR-016/017): `shard_index(key) = fnv1a(key) & (C-1)`, with `C`
baked into the on-disk format. This has four limitations, all called out as
deferred in [`object-storage-native.md`](../designs/object-storage-native.md):

1. **Capacity ceiling.** A collection is capped at `C × keys-per-shard`
   (~256k keys at the soft cap). Growing past that has no answer.
2. **Over-provisioning small collections.** A ten-key collection still spreads
   across the hash space; listing it costs a `list` plus a GET per non-empty
   shard, and the mapping cannot adapt to actual size.
3. **No sorted listing.** Hashing deliberately destroys key order, so key
   enumeration is unordered and range/prefix scans are impossible — a hard
   requirement we now want to support.
4. **Coarse membership.** A single collection-root lock serializes _all_
   create/delete across the whole collection (ADR-018).

Requirement (3) is the decisive one: efficient **sorted** and **range** listing
is incompatible with hashing. It forces an **order-preserving** partitioning,
which in turn wants a growable, range-addressed directory rather than a fixed
hash space — which also resolves (1), (2), and (4).

The invariants any scheme must keep (unchanged from ADR-016): stateless,
ephemeral, uncoordinated clients; **content CAS on a single object** as the only
primitive; **reads/overwrites of an existing key must not touch a central
object** (the hot-path invariant); a **deterministic, DST-replayable** mapping;
and compatibility with wound-wait, leases, GC, and the shard-mutation
coordinator, which all address shards by identity.

## Decision

Replace hash sharding with an **order-preserving, range-partitioned
coordination directory**: a **B-link tree** ([Lehman & Yao]) of objects per
collection, mutated only by content CAS.

### Object model

- **Root object** (`_i`, a well-known fixed path). _Is_ the B-link tree's current
  **root node** — a leaf while the collection is small, an index node once it
  grows — and also carries the collection metadata (existence marker +
  subcollection directory, as in ADR-018). There is **no separate anchor**: the
  root keeps a fixed address, so the tree grows in height by **splitting the root
  in place** (below) rather than moving it, and every descent simply starts at
  `_i` — no pointer indirection. The trade-off is that `_i`'s version bumps on
  any root-level structural change _and_ on membership change, coupling the two;
  this is accepted deliberately to keep the object model minimal (see
  Consequences).
- **Index node** (interior, including the root at height ≥ 2). An ordered list of
  separator keys → child-node pointers, plus a **high-key** and a
  **right-sibling** pointer. Maps a key range to the child that owns it.
- **Leaf shard** (including the root at height 1). Owns a contiguous key range and
  holds the ADR-017 per-key entries (lock type, `locked_by`, `current_writer`,
  `deleted`) sorted by key — the lock table, MVCC index, and key directory for its
  range. Carries the same **high-key** and **right-sibling** pointer. It is the
  CAS unit for its keys.

Keys are ordered **lexicographically over the raw key bytes**, consistent with
the order-preserving path encoding (`glassdb-data::base64`).

**Addressing.** Non-root nodes are a new object kind named by an **opaque,
randomly generated identity token** under a new path-type marker, replacing
ADR-017's computed `_s/<i>`: the index is dynamic, so addressing is by
_descending the tree_, never by formula. Names are **random, not monotonic**, on
purpose — object stores partition by key prefix, so monotonically increasing
names would pile new nodes onto one partition and accidentally hot-key the
backend; a random token spreads them. The root is the exception — it keeps the
fixed `_i` path so every descent has a well-known entry point.

**Encoding.** Node bodies get **golden-anchored protobuf encodings**, as
ADR-017/018 did: an _index_ node (separator keys, child names, high-key,
right-sibling) and a _leaf_ that extends the ADR-017 shard-entry list with a
high-key and right-sibling. The `_i` body carries the current root node plus the
collection metadata, superseding ADR-018's fixed-`shard_count` `CollectionRoot`.

### Mapping and the read hot path

A key's leaf is found by descending from the root object `_i` through index
nodes. Every node **self-describes** the range it covers (its high-key), so the
descent is **cached and self-correcting**:

- Clients cache interior nodes, including the root `_i` (revalidated by version
  like any coordination object, ADR-023). A hit descends from the cache with no
  central read.
- If a lookup reaches a node whose high-key shows the key belongs further right —
  a split moved it after the cache was taken — the client **follows the
  right-sibling link** (B-link's defining property) or re-descends from a
  refreshed `_i`. The rare stale case self-heals.

This is the range analogue of the ADR-018 root-version trick and the ADR-030
`AllowStale` seed: the hot path reads only the leaf; higher levels are cached and
change only on splits/merges.

### Split (grow), background and lock-free

The **background** maintenance task (like GC, ADR-022) splits a leaf over its
soft cap (bytes and/or key count). A split is the classic B-link half-split,
driven through the **shard-mutation coordinator** (ADR-028) and recovered like
any in-doubt CAS (ADR-009):

1. **Create the right sibling** (`write_if_not_exists`) holding the upper half of
   the entries — _including their live lock and MVCC state_ — with the parent's
   old high-key and right-link.
2. **Shrink the source in one CAS**: drop the upper half, set the source's
   high-key to the split key, and point its right-sibling at the new node. **This
   single CAS is the linearization point** — before it, only the source is
   authoritative; after it, source and sibling own disjoint ranges. Exactly one
   object is authoritative for any key at every instant.
3. **Insert the separator into the parent** as an independent, recoverable step
   (may recurse upward). Until it lands, lookups still reach the moved keys via
   the right-link, so correctness never depends on the parent insert completing
   promptly.

A crash between (1) and (2) leaves an **unreferenced** sibling that GC reclaims;
(2) and (3) are each idempotent under reload. Because lookups tolerate an
in-progress split via right-links, splitting needs **no quiescence** and does not
block concurrent locking on the source leaf — this is why B-link is chosen over
locked top-down descent.

**Root split (height growth), in place.** The root has no parent and must not
move (every descent starts at `_i`). So when `_i` overflows, height grows by an
_in-place_ root split: create two new child nodes holding the two halves of
`_i`'s contents (`write_if_not_exists`), then **one CAS rewrites `_i`** from its
old contents into a two-entry root pointing at the children. That CAS is the
height-growth linearization point; `_i` transitions leaf→index (height 1→2) or
stays an index node (deeper) but keeps its address. A crash after the children
are created but before the rewrite leaves them unreferenced for GC.

The split point is the **median entry** of the node (balanced split); a
load-aware split point is a future refinement.

### Merge (shrink): deferred, format-reserved

Merging underfull siblings is **not implemented now**, but the node format
reserves what merge needs (sibling links and range bounds) so it can be added
without a format migration.

### Range-granular membership and phantom prevention

The single coarse membership lock (ADR-018) is replaced by **per-leaf**
coordination:

- **Create/delete** locks only the target key's entry in its leaf (the `create`
  / write lock of ADR-017). Creates and deletes in different ranges no longer
  serialize.
- **Range/sorted listing** descends to the range start, then scans leaves
  left-to-right following right-links, **OCC-validating each covered leaf's
  version**; a split during the scan is absorbed by following the right-link. A
  membership change bumps its leaf's version, so equal endpoints prove no
  create/delete raced within a leaf — the per-leaf analogue of ADR-018's
  root-version validation. Under contention the scan escalates to **per-leaf read
  locks** over the covered range. Range boundaries are protected the same way
  (validate/lock the boundary leaf), preventing boundary phantoms.

### Wound-wait, leases, GC

Mechanisms are unchanged (ADR-021/022), now at leaf/index granularity. GC and
crash recovery **re-resolve** a key's shard through the _current_ topology; a
back-reference to a node that a split/merge has removed triggers a topology
refresh rather than a lost reference. Index and leaf nodes holding locks are live
references; orphaned split siblings (a crash before step 2) are unreferenced and
reclaimable.

## Consequences

- **Sorted and range listing become native and efficient** — an ordered leaf
  scan — which hashing could never provide. This is the primary win.
- **Unbounded growth** with each object staying small: the tree adds levels as it
  grows, so no single object (`_i`, index, or leaf) scales with collection size.
  Small collections start as a single leaf (the root `_i` itself) and split only
  as needed, removing the over-provisioning of fixed `C`.
- **Finer membership concurrency**: create/delete serialize only within a leaf's
  range, not the whole collection.
- **The hot path is preserved**: reads/overwrites of an existing key still touch
  only the leaf; interior nodes including the root `_i` are cached and
  self-correcting via B-link right-links, so they stay off the hot path as the
  collection root did in ADR-018.
- **Fewer objects, at the cost of coupling.** The collection metadata (existence,
  subcollection directory) lives _in_ the B-link root at `_i` rather than in a
  separate anchor that points at a movable root. This drops an object kind, the
  root pointer to keep valid, and the height-change indirection — every descent
  just starts at the fixed `_i`. The accepted cost: `_i`'s version now bumps on
  root-level structural churn (a root-level separator insert or a height-growth
  split) as well as on membership change, so it is no longer a pure
  membership-OCC token. Root-level rewrites are rare with wide fan-out, and the
  simpler object model was judged worth the coupling.
- **New costs.** The directory now grows with shard count (accepted; hierarchy
  keeps each object bounded). A cold key access may traverse several index levels
  before it is cached (amortized to ~one leaf read). There are more object kinds
  and a **multi-level, multi-step split protocol** with more CAS sites and
  recovery cases than a fixed hash mapping — more surface to test (DST oracles
  and golden vectors regenerate; the split half-step and right-link traversal
  need dedicated deterministic tests).
- **Range hotspots.** Order preservation reintroduces the classic B-tree
  monotonic-insert hotspot: an append-only key pattern hammers the rightmost
  leaf, and splitting distributes _many_ keys but not a single hot tail. Noted as
  a known limitation (a load-aware split point or key-space salting is a future
  option); single-hot-key relief remains out of scope.
- **Greenfield.** No migration: the fixed-hash format is replaced wholesale, as
  ADR-016 did for the tag-based layout. `SHARD_COUNT`, `shard_index`, and the
  `_s/<i>` addressing of ADR-017 are superseded.
- **Deferred.** Merge/rebalance and underflow policy; load-aware split points;
  compaction (ADR-022's open item) is orthogonal and still deferred.

[Lehman & Yao]: Philip L. Lehman and S. Bing Yao, "Efficient Locking for
Concurrent Operations on B-Trees" (ACM TODS, 1981).
