# Dynamic range sharding (design overview)

## Status

**Proposed.** This is the design overview and decision index for replacing the
fixed compile-time hash sharding of collection metadata with a **dynamic,
order-preserving, range-partitioned** coordination directory. The umbrella
decision is [ADR-031](../adr/031-dynamic-range-sharding.md). This document is the
living companion to that proposed ADR; it captures the shape, the rationale,
the invariants, and the open questions.

It builds on the object-storage-native layout of
[`object-storage-native.md`](object-storage-native.md): for this redesign, the
shard *entry* model, transaction objects, commit/write-back, wound-wait/leases,
GC, and the shard-mutation coordinator all carry over. Only the **key→shard
mapping** and the **directory structure** change. Proposed snapshot ADRs 038–040
would later revise value placement, retention, and catalog authority without
replacing the B-link topology.

## Why

Fixed hash sharding (`C = 1024`, `shard_index(key) = fnv1a(key) & (C-1)`,
ADR-016/017) has four limitations:

1. **Capacity** — a collection is capped at `C × keys-per-shard` (~256k keys).
2. **Small-collection waste** — a tiny collection still spreads across the hash
   space and cannot adapt to its actual size.
3. **No sorted listing** — hashing destroys key order, so ordered enumeration and
   range/prefix scans are impossible.
4. **Coarse membership** — one collection-root lock serializes all create/delete.

Efficient **sorted/range listing** (2 above elevated to a requirement) is
fundamentally incompatible with hashing and forces an **order-preserving**
partition, which — made growable — resolves all four at once.

## Design at a glance

A **B-link tree** of objects per collection (Lehman–Yao), mutated only by content
CAS. Leaves are the shards; interior nodes are the range index.

- **Objects**
  - _Root object_ (`_i`, fixed well-known path): **is** the tree's current root
    node — a leaf while the collection is small, an index node once it grows —
    and under ADR-031 also carries collection metadata (existence +
    subcollection list). Proposed
    [ADR-041](../adr/041-epoch-versioned-collection-catalog.md) moves logical
    authority for that metadata to a versioned catalog while retaining `_i` as
    the routing root.
    There is no separate anchor: the root keeps a fixed address, so height grows
    by **splitting the root in place** and every descent starts at `_i`.
  - _Index node_ (interior, incl. the root at height ≥ 2): ordered separator keys
    → child pointers, a **high-key**, and a **right-sibling** link. Maps a key
    range to its child.
  - _Leaf shard_ (incl. the root at height 1): owns a contiguous key range; holds
    the ADR-017 per-key entries (lock type · `locked_by` · `current_writer` ·
    tombstone) sorted by key; also a **high-key** and **right-sibling** link. The
    CAS unit for its keys.
  - _Transaction object_ (`_t/<ss>/<txid>`): unchanged by range sharding (status +
    values + lease); proposed ADR-039 later splits values into per-key payloads.
  - _Structural record_ (`{db}/_s/<record-id>`): a short-lived split
    write-ahead note, recovered independently from transaction logs
    ([ADR-034](../adr/034-separate-structural-log-namespace.md)).
- **Ordering** — lexicographic over raw key bytes, matching the order-preserving
  path encoding.
- **Mapping** — descend from the root object `_i`; each node self-describes its
  range (high-key), so descent is **cached and self-correcting**.
- **Reads/writes (hot path)** — descend via cached interior nodes (incl. `_i`) to
  the leaf, read the leaf. If a node's high-key shows the key moved (a split raced
  the cache), **follow the right-sibling link** or re-descend from a refreshed
  `_i`. No central read on a cache hit.
- **Split (grow)** — background; a root split grows height **in place** at `_i`.
  A split takes the node **structure write lock** for priority and mutual
  exclusion (wound-wait) but keeps ADR-031's **shrink-CAS linearization** and
  right-link tolerance — it is coordinated, not atomically committed across its
  objects ([ADR-032](../adr/032-node-locking-and-coordinated-splits.md)). It holds
  **one node's structure-W at a time**, releasing it right after the shrink CAS;
  the parent separator is a separately-locked follow-on, so there is no lock chain
  and no cross-level deadlock. Non-root index splits recurse upward the same way.
- **Merge (shrink)** — deferred; the node format reserves sibling links and range
  bounds so it is addable without a format migration.
- **Node locks** — two read/write locks per node beside the per-key entry locks:
  a **structure** lock (data mutations _and_ escalated scans hold read;
  split/merge holds write) and a **membership** lock (escalated scans hold read;
  create/delete hold write; overwrites/reads hold neither)
  ([ADR-032](../adr/032-node-locking-and-coordinated-splits.md)). Uncontended
  pure reads take neither, so the hot path stays lock-free; an invalidated
  read-only scan escalates on retry. Because an escalated scan holds
  structure-read, it conflicts with a split's structure-write, so no scanner lock
  is transferred across a split.
- **Membership** — per-leaf: create/delete take the node **membership write lock**
  (serializing membership change within a leaf, mirroring v1's collection-level
  lock at finer granularity) — this is also what distinguishes a delete from an
  overwrite; overwrites take no membership lock and stay invisible to scans. Range
  listing OCC-validates covered leaves by **status-aware resolution** of the live
  key set (help-forwarding committed holders, so value writes and pure splits do
  not disturb it and a committed-but-unpublished create is still seen); a per-leaf
  **membership version** (bumped by membership-write activity only, not scanner-R
  churn) is the fast-path token (equality is sound only when no observed
  membership intent has since committed). A transaction with **any scan and any
  write** must predicate-lock every scanned range and revalidate membership under
  the lock before commit. A read-only scan starts on the OCC fast path and follows
  right-links across concurrent splits; after an invalidation, retries take the
  same predicate read locks to bound starvation
  ([ADR-032](../adr/032-node-locking-and-coordinated-splits.md),
  [ADR-033](../adr/033-transactional-key-iteration.md)).
- **Wound-wait / leases / GC** — unchanged mechanisms at leaf/index granularity;
  GC and recovery re-resolve a key's shard through the *current* topology, and a
  crash-orphaned split sibling is reclaimed from a **structural log entry**
  recording the split's created node tokens — recovery keeps or deletes them by
  proving **tree-reachability** (right-link chain, or root-split index entries),
  with ambiguity resolved by retry, not deletion
  ([ADR-032](../adr/032-node-locking-and-coordinated-splits.md),
  [ADR-034](../adr/034-separate-structural-log-namespace.md)). Transaction and
  structural records use separate `_t/<ss>` and `_s` namespaces and recovery
  loops.

## Tree shape

### Minimal case — a single key (or a small collection)

The whole collection is a **single object**: `_i` is the root *and* the only
leaf. There are no index nodes and no siblings. Reading or writing the key is a
single GET/CAS on `_i`.

```mermaid
graph TD
    Root["_i — root (leaf)<br/>range (-∞, +∞) · high-key +∞<br/>right-sibling → nil<br/>entries: user:42 → {lock, writer, …}<br/>+ ADR-031 collection metadata"]

    classDef root fill:#e8ecff,stroke:#5566aa,color:#111
    class Root root
```

As keys are added, `_i` eventually crosses its soft cap. Because the root cannot
move, the **first** split grows height *in place*: two new leaves take the two
halves, and `_i` is rewritten from a leaf into a two-entry index root pointing at
them (height 1 → 2):

```mermaid
graph TD
    Root["_i — root (index) · high-key +∞<br/>(-∞, m) → L0<br/>[m, +∞) → L1<br/>+ ADR-031 collection metadata"]
    Root --> L0["Leaf L0<br/>(-∞, m) · hi m<br/>apple, cat"]
    Root --> L1["Leaf L1<br/>[m, +∞) · hi +∞<br/>mango, pear"]

    classDef root fill:#e8ecff,stroke:#5566aa,color:#111
    classDef leaf fill:#fff6d5,stroke:#aa8833,color:#111
    class Root root
    class L0,L1 leaf
```

The leaves are chained left-to-right by their right-sibling pointers:

```mermaid
graph LR
    L0["Leaf L0<br/>(-∞, m)"] -->|right| L1["Leaf L1<br/>[m, +∞)"] -->|right| Nil(("nil"))

    classDef leaf fill:#fff6d5,stroke:#aa8833,color:#111
    classDef nil fill:#f2f2f2,stroke:#999,color:#555
    class L0,L1 leaf
    class Nil nil
```

### Many keys — a multi-level tree

Splits propagate upward, so the tree gains levels as the collection grows (here
height 3). Only leaves hold key entries; interior nodes map ranges to children.

```mermaid
graph TD
    Root["_i — root (index) · hi +∞<br/>(-∞, m) → I1<br/>[m, +∞) → I2<br/>+ ADR-031 collection metadata"]

    Root --> I1["Index I1 · hi m<br/>(-∞, f) → L0<br/>[f, m) → L1"]
    Root --> I2["Index I2 · hi +∞<br/>[m, t) → L2<br/>[t, +∞) → L3"]

    I1 --> L0["Leaf L0<br/>(-∞, f)<br/>apple, cat"]
    I1 --> L1["Leaf L1<br/>[f, m)<br/>fig, kiwi"]
    I2 --> L2["Leaf L2<br/>[m, t)<br/>mango, pear"]
    I2 --> L3["Leaf L3<br/>[t, +∞)<br/>tiger, zebra"]

    classDef root fill:#e8ecff,stroke:#5566aa,color:#111
    classDef index fill:#e6f6e6,stroke:#4c9a4c,color:#111
    classDef leaf fill:#fff6d5,stroke:#aa8833,color:#111
    class Root root
    class I1,I2 index
    class L0,L1,L2,L3 leaf
```

Each level is a left-to-right linked list via the right-sibling pointers — the
index level and the leaf level shown separately (this is what a sorted/range scan
walks, and what a stale cached lookup follows to self-correct after a split):

```mermaid
graph LR
    I1["Index I1"] -->|right| I2["Index I2"] -->|right| NilI(("nil"))
    L0["Leaf L0"] -->|right| L1["Leaf L1"] -->|right| L2["Leaf L2"] -->|right| L3["Leaf L3"] -->|right| NilL(("nil"))

    classDef index fill:#e6f6e6,stroke:#4c9a4c,color:#111
    classDef leaf fill:#fff6d5,stroke:#aa8833,color:#111
    classDef nil fill:#f2f2f2,stroke:#999,color:#555
    class I1,I2 index
    class L0,L1,L2,L3 leaf
    class NilI,NilL nil
```

Notes:

- **Descent** for `"kiwi"`: root `_i` (`"kiwi" < "m"` → I1) → I1 (`"f" ≤ "kiwi" <
  "m"` → L1) → read L1. On a cache hit only L1 is fetched.
- **Right-links cross parent boundaries** (e.g. I1 → I2): a lookup that lands too
  far left after a concurrent split follows the right-link without going back up
  to the root — the B-link property that keeps the hot path off central nodes.
- **Sorted listing** is the leaf chain `L0 → L1 → L2 → L3`; a range scan enters at
  the range's start leaf and stops when a leaf's low bound passes the range end.
- Only leaves hold key entries (lock/MVCC state); index nodes hold separator keys
  → child pointers. The root is the `_i` object itself — once the tree has grown
  it holds separators (plus ADR-031's collection metadata), not keys. Proposed
  ADR-041 moves the metadata's logical authority out of this routing object.

## Constituent ADRs

This redesign spans multiple decisions; each significant one is a frozen ADR,
tracked here. The detailed object model, encoding, split protocol (leaf and
in-place root split), membership rules, invariants, and trade-offs live in the
ADR — this overview and the diagrams above are the map into it.

- **[ADR-031](../adr/031-dynamic-range-sharding.md) — Dynamic range-partitioned
  sharding (B-link tree).** *Proposed.* The umbrella decision: B-link object
  model and encoding, cached self-correcting descent, the leaf split and in-place
  root split, per-range membership and phantom prevention, and what it
  supersedes/reuses. Its per-leaf read-lock escalation is superseded by ADR-032;
  its lock-free split is refined (structure lock + priority) by ADR-032.
- **[ADR-032](../adr/032-node-locking-and-coordinated-splits.md) — Node-level
  locking and coordinated splits.** *Accepted — implemented.* The node
  structure/membership
  read/write lock taxonomy (create/delete take the membership write lock),
  splits coordinated by the structure write lock and wound-wait priority (keeping
  ADR-031's shrink-CAS linearization), the membership version as OCC fast-path
  token, structural-log orphan recovery, and the conditional progress guarantee
  with a hard object-size cap. Supersedes ADR-031's read-lock escalation and
  refines its split coordination.
- **[ADR-033](../adr/033-transactional-key-iteration.md) — Transactional key
  iteration.** *Accepted — implemented.* The range/scan API (half-open bounds,
  keys-only, paging) and its OCC/lock isolation built on the ADR-032 taxonomy.
- **[ADR-034](../adr/034-separate-structural-log-namespace.md) — Separate
  structural-log namespace.** *Accepted — implemented.* Keeps short-lived,
  low-cardinality split recovery records under database-wide `_s`, independent
  from transaction `_t` schema, GC, and scheduling.

Planned follow-on ADRs, as the open questions below resolve: merge/rebalance,
split-point policy, and node fan-out/sizing.

## Open questions / future work

- **Merge & rebalance.** Underflow threshold, sibling-merge protocol, and its
  in-doubt recovery (format is reserved for it).
- **Split-point policy.** Median-key vs load-aware; salting to mitigate the
  monotonic-insert hotspot.
- **Hard object-size cap tuning.** The implemented defaults cap a node at 1 MiB
  and reserve 64 KiB for transient lock metadata. Content growth stops at the
  remaining 960 KiB; an individually unsplittable key and, under ADR-031, an
  overflowing subcollection directory are permanent invalid-input errors.
  Proposed ADR-041 removes the latter from `_i`. Future tuning may still adjust
  those configurable defaults and their foreground-latency trade-off.
- **Directory caching.** Invalidation strategy and memory budget for cached
  index nodes (reuse of the ADR-023 object cache; interaction with ADR-030
  `AllowStale` seeding).
- **Cached-descent fast paths.** How ADR-027's single read-write path and the
  existing strict read-only fast path recover when their cached descent is
  stale.
- **Snapshot and fast paths.** The proposed
  [snapshot-read design](snapshot-reads.md) routes historical logical versions
  through the latest B-link topology. Its correctness baseline replaces
  ADR-027's parallel first-intent path. A specialized replacement is optional
  and would require its own proof.
- **Subcollection directory.** ADR-031 keeps it in `_i`; proposed ADR-041 moves
  logical authority to an epoch-versioned catalog. If that proposal is not
  accepted, unbounded growth and root-rewrite coupling remain open here.
- **Node fan-out / sizing.** Interior fan-out and leaf soft cap vs tree height,
  CAS cost, and cache footprint — to be pinned by a benchmark plan.

## Relationship to existing ADRs

In short: this **supersedes** the mapping/topology of ADR-016/017/018 (fixed `C`
shards, FNV-1a `_s/<i>` mapping, recorded `shard_count`, coarse membership lock)
and **reuses unchanged** everything else in the object-storage-native stack — the
shard *entry* model (ADR-017), transaction object (ADR-019), commit/write-back
(ADR-020), wound-wait/leases (ADR-021), GC (ADR-022), the slimmed backend trait
(ADR-023), and the shard-mutation coordinator (ADR-028/029), which the split
plugs into as another coordinator-driven mutation. Proposed snapshot ADRs
038–040 would subsequently replace value placement, historical liveness, and
root-local catalog authority while retaining the B-link topology. See the
[ADR-031 status](../adr/031-dynamic-range-sharding.md#status) for the exact
clauses.
