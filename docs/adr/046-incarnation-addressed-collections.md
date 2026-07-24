# ADR-046: Incarnation-addressed collection format

## Status

Proposed.

Constituent decision of the
[dynamic-range-sharding design](../designs/dynamic-range-sharding.md).

On acceptance, this supersedes the clauses in ADR-016, ADR-018, and ADR-031
that derive physical collection addresses from logical names, make physical
`_i` presence authoritative for existence, and store a name-only child list.
The B-link tree, node-locking, and key-membership decisions of ADR-031/032 are
unchanged.

[ADR-047](047-transactional-collection-management.md) builds transactional
collection management on this identity and resolution model. This ADR is
deliberately implementable without ADR-047.

Physical creation and reclamation follow
[ADR-042](042-conditional-only-backend-mutations.md)'s conditional-only backend
boundary and [ADR-043](043-causally-coordinated-backend-operations.md)'s
same-path coordination and fresh-identity rules.

[ADR-041](041-epoch-versioned-collection-catalog.md) is a snapshot-read
proposal based on the superseded collection model. It must be revised to
version the ID-based directories introduced here before acceptance; snapshot
semantics are otherwise outside this ADR.

## Context

GlassDB currently uses a collection's full logical name path as its physical
address. A `Collection` can therefore be constructed without I/O, and a missing
root can be interpreted as an empty collection. This conflates a name with the
thing currently bound to it: a future drop and recreation could make an old
handle silently address the replacement, while a rename would require moving
the entire physical tree.

The hierarchy also needs a scalable resolution model. A database-wide list or
full-path catalog would become a central source of contention and correctness
risk. A parent has on the order of one hundred direct children and paths have on
the order of ten components, so a cold lookup may scale with depth. Once a
collection is open, point operations must not revalidate that path.

## Decision

### Incarnation identity and physical layout

Every collection has an opaque, client-generated `CollectionId` that is unique
within the database and never reused. Exact width and encoding are format
details. Its physical B-link tree lives under:

```text
<db>/_c/<collection-id>/_i
<db>/_c/<collection-id>/_n/<node-id>
```

The database owns a reserved, well-known ID outside the generated-ID domain.
`Database::root_collection()` returns that collection synchronously. It is a
regular key-bearing `Collection` and the parent of top-level collections, but
it is permanent and cannot be dropped. Database format metadata remains
separate. Opening an initialized database validates the reserved root; a
missing root is corruption and is never silently recreated as empty.
Initializing that permanent path is idempotent and satisfies ADR-043's
permanent-path create-if-absent exception.

Each collection's `_i` remains its B-link root and also holds a bounded,
canonically ordered directory from raw child name to `CollectionId`. The exact
ID in the parent entry is authoritative for name resolution. There is no
database-wide collection list, full-path index, or reverse catalog. Each
non-root ID is published in exactly one parent/name entry; collection aliases
and hard links are not supported.

Names remain non-empty, bounded raw byte strings. GlassDB does not normalize
them; a future SQL layer owns identifier rules.

### Paths resolve to bound handles

A `CollectionPath` is an unresolved sequence of names, not a data handle.
Opening it walks direct-child directories from `root_collection()` and costs
`O(depth)` when cold. Opening is therefore fallible and asynchronous.

A `Collection` is bound to one incarnation. Except for the permanent database
root, it carries the collection ID plus its direct parent-and-name binding. Data
routing uses only the ID; the direct binding lets later lifecycle operations
compare the exact parent entry without a full-path or reverse-catalog lookup.
The root needs no parent binding because it cannot be dropped. A handle never
automatically rebinds to a different ID at the same logical name.

Immediate-child listing reads the bounded directory in the parent's `_i`; it
does not walk the parent's data tree or reopen each child. It returns entries
containing both the raw name and an incarnation-bound `Collection`, in name
order. A list can become stale after it returns, but its handles remain bound to
the IDs that were listed.

Normal reads and writes require a resolved `Collection`. Missing roots are no
longer empty trees and writes never create them implicitly. This removes the
current synchronous path-constructor APIs in favor of `root_collection()` and
fallible open/create operations.

### Non-transactional implementation boundary

This ADR changes identity, storage layout, and resolution only. It does not yet
make collection management part of a database transaction.

A standalone create allocates a fresh ID, creates its empty physical root with
create-if-absent while it is undiscoverable, and then publishes `name → ID` with
an exact-revision conditional rewrite of the parent. The fresh root satisfies
ADR-043's fresh-identity exception because its existence alone cannot make it
live. Parent publication is the standalone operation's visibility point, so a
published mapping never intentionally names a missing root. Losing the race for
the name reports `AlreadyExists` for strict create or returns the winner for
create-if-absent.

Interruption before publication can leave an unreachable ID prefix for
asynchronous reclamation; an abandoned create may land late but still cannot
publish itself. Reclamation observes the exact object revision before using
ADR-042's conditional delete, and may conservatively leave an ambiguous orphan.
Creation is not atomic with data writes, sibling collection changes, or creation
at another hierarchy level. Open, existence checks, and immediate-child listing
likewise return one current directory view, not a multi-operation transactional
snapshot. ADR-047 replaces these limitations without changing IDs, physical
paths, or path resolution.

Collection deletion is not introduced by this ADR.

## Consequences

- The format and handle API can be implemented independently of transactional
  lifecycle work. ADR-047 can subsequently address stable IDs directly instead
  of also replacing path-derived storage.
- Cold full-path open costs one bounded-directory lookup per component. Once
  open, the ID routes point operations directly with no ancestor validation.
- Collection-directory mutation and root-level B-link activity share the `_i`
  CAS domain. This is accepted for bounded fan-out and DDL-like collection
  churn; ordinary operations in non-root leaves remain unaffected.
- Interrupted standalone creation may leak an undiscoverable empty prefix until
  reclamation, but cannot expose a parent mapping to an uncreated root.
- Stable physical IDs make a future rename or move possible without relocating
  data, and prevent [ADR-045](045-optional-persistent-encoded-body-l2-cache.md)
  entries from aliasing a replacement incarnation. Rename, move,
  metadata/options, deletion, transactional management, and snapshot behavior
  remain out of scope.
- The public collection API and physical format both change incompatibly. The
  current format is changed in place; existing development databases are
  recreated and no migration or compatibility path is provided.

This resembles [FoundationDB's Directory Layer] mapping logical paths to opaque
physical prefixes, while deliberately stopping short of its transactional
directory operations until ADR-047.

[FoundationDB's Directory Layer]: https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/directory/Directory.html
