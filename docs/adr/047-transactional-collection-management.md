# ADR-047: Transactional collection management

## Status

Proposed.

Constituent decision of the
[dynamic-range-sharding design](../designs/dynamic-range-sharding.md). Depends
on [ADR-046](046-incarnation-addressed-collections.md) for collection identity,
physical layout, and path resolution.

On acceptance, this supersedes standalone collection creation and
non-transactional child listing in ADR-018 and completes its deferred collection
teardown. It adds transactional coordination to ADR-046 without changing its
IDs or physical addressing.

Backend mutations retain the conditional boundary of
[ADR-042](042-conditional-only-backend-mutations.md) and causal coordination of
[ADR-043](043-causally-coordinated-backend-operations.md). Collection drop uses
[ADR-044](044-cas-fenced-structural-gate.md) to quiesce each node before
fencing it.

[ADR-041](041-epoch-versioned-collection-catalog.md) must be revised to version
these transactional ID-based directories before acceptance. Snapshot semantics
are otherwise outside this ADR.

## Context

ADR-046 separates logical names from physical collection identity, but its
independently implementable first stage still publishes a prepared root into
one parent outside a database transaction. Creation cannot commit atomically
with initial data, nested collection changes, or a future schema catalog, and
there is no collection deletion operation.

GlassDB needs collection creation, lookup, listing, and deletion to compose
atomically with data writes and with each other. This completes the
transactional surface and provides the storage primitive a future SQL layer
needs to transact schema-catalog changes together with their physical
collections.

Collection churn is expected to resemble OLTP schema churn: rare compared with
data access. Strong, potentially expensive coordination is acceptable on
collection changes, but an ordinary point operation on an open collection must
not add a lifecycle-root or full-path check.

## Decision

### Ordinary transactions and interface

Collection management uses the existing `Database` transaction, not a second
public transaction kind. The transactional surface provides:

- strict create;
- create-if-absent, returning the bound handle and whether this transaction
  created it;
- open and existence checks by parent/name or unresolved path;
- immediate-child listing; and
- non-recursive drop by bound handle.

Standalone operations become convenience wrappers around ordinary
transactions. The parent-oriented shape uses ADR-046's regular root collection:

```rust
let root = db.root_collection();
let users = tx.create_collection(&root, b"users").await?;
let active = tx.create_collection(&users, b"active").await?;
```

A transactional list is serializable and reflects the transaction's own
creates and drops. A standalone list can become stale after it returns, like
any query result, but a returned handle remains safe: it either accesses the
listed incarnation or reports `StaleCollection`.

Collection operations have read-your-writes semantics. In particular:

- a created collection is immediately openable and empty, and may receive data
  writes in the same transaction;
- a parent created earlier in the transaction may receive children;
- children may be explicitly dropped before their parent in one transaction;
- reads may precede a drop, but staging data writes and then dropping the same
  collection makes the transaction invalid; and
- drop followed by recreation of the same parent/name in one transaction is
  rejected initially.

Strict create reports `AlreadyExists`. Create-if-absent returns the existing
incarnation with `created = false`. Opening a missing name reports `NotFound`;
using an obsolete bound handle reports `StaleCollection`; dropping a collection
with children reports `NotEmpty`. Invalid names, cross-database handles, and an
attempt to drop the database root report `InvalidInput`.

### Transactional creation and directories

Creation prepares a fresh physical root while it is undiscoverable. The root,
the parent directory entry, any other collection changes, and initial data all
share the ordinary transaction's outcome. A committed directory entry therefore
never names a missing or differently bound root; an aborted prepared prefix is
reclaimed asynchronously.

Each parent directory established by ADR-046 is one bounded transactional
domain in its `_i`. Creates and drops for one parent serialize there. Opening one
name validates that entry, while listing validates the directory and includes
the transaction's own mutations. Ordinary data operations do not perform a
directory lookup.

### Drop and stale-handle fencing

Drop compares the handle's direct parent entry with its exact `CollectionId`.
It succeeds only when that incarnation is still bound there and has no child
collections in the transaction's logical view. User data need not be empty.
There is no cascade or recursive drop.

Drop first installs and retains a collection-wide topology-freeze intent in
`_i`. Every split and future merge joins that lifecycle coordination before
creating a node and remains joined through publication or recovery. The freeze
excludes new participants, completes or recovers existing ones, and thereby
prevents a new node from being published. Only structural and lifecycle
operations pay this root-coordination cost; ordinary point operations do not.

With topology frozen, drop enumerates the incarnation-unique prefix. For every
root, index, and leaf node, including extant temporarily unreachable structural
nodes, it acquires ADR-044's structural gate, reconciles existing holders, and
installs a collection-delete intent with a conditional node rewrite. The
structural gate can then be released; the intent itself prevents subsequent
stable rewrites or structural-gate acquisition. Drop holds at most one
structural gate at a time.

ADR-043 permits an abandoned fresh-object creation to land late. Such an
unreachable structural object might appear after enumeration, but the topology
freeze prevents it from ever being published into the tree. It is reclaimed as
an orphan and cannot be reached by a collection handle.

The intent is part of each node's existing coordination state. It conflicts
with node mutations and key locks and is observed by strict reads during
validation, so an ordinary point operation needs no additional root or path
check:

- a pending intent participates in normal wound-wait resolution;
- an aborted intent may be helped away; and
- a committed intent is a durable deletion fence and yields
  `StaleCollection`.

Only after every reachable or publishable node is fenced may the ordinary
transaction commit. That one outcome makes both the parent-entry removal and
every delete intent effective. A racing old transaction therefore either
serializes before the drop or conflicts; it cannot commit a write to the dropped
incarnation afterward. Missing physical nodes after reclamation also mean
`StaleCollection`, never an empty collection.

Preparation is recoverable and may temporarily block already-fenced ranges if
the client stops partway through. Aborting the transaction cancels the whole
drop, and later operations may help clear its intents. Once committed, logical
deletion is immediate. Physical reclamation later reads each remaining object
and uses ADR-042's exact-revision conditional delete; a conflict causes
re-evaluation rather than deletion of an unobserved state. Now-unreachable value
reclamation remains asynchronous under the existing GC protocol.

`read_stale` is the deliberate exception to current-liveness validation. It may
return pre-drop data within its requested staleness bound, even after drop has
committed. If it observes the committed fence it fails, and it never aliases a
replacement because collection IDs are not reused.

### Relationship to other database models

- [FoundationDB's Directory Layer] similarly performs directory operations in
  ordinary transactions. FoundationDB warns that clients holding an
  already-open directory may still write after removal; GlassDB's all-node
  fence deliberately provides the stronger stale-handle guarantee needed here.
- [bbolt] creates and deletes buckets in ordinary transactions. Its bucket
  handles are transaction-scoped; GlassDB handles may outlive a transaction and
  are therefore incarnation-bound instead.
- [PostgreSQL system catalogs] are regular tables, and DDL such as `DROP TABLE`
  takes [strong table locks]. GlassDB follows that composable model: a future SQL
  layer can update its catalog records and collections in one transaction, with
  rare drop work paying the strong fencing cost. Schema interpretation remains
  above GlassDB.
- A separate implicit-commit DDL transaction, as used by [MySQL atomic DDL], is
  rejected because it could not atomically compose collection changes with
  application data or a future SQL catalog.

## Consequences

- Collection lifecycle changes gain the same atomicity, retries, and
  read-your-writes behavior as data changes. Creation and seed data can commit
  together, and schema layers need no second transaction protocol.
- ADR-046 remains a useful implementation milestone: this ADR adds coordination
  to its settled `name → ID` hierarchy rather than combining a format rewrite
  with transactional lifecycle work.
- Drop is intentionally DDL-like: its pre-commit latency and lock footprint are
  `O(number of collection nodes)`, and abandoned preparation can cause temporary
  blocking. This moves work to rare destructive operations and keeps normal
  point operations free of a lifecycle-root read.
- Logical deletion precedes physical reclamation, so dropped data may continue
  consuming storage until background cleanup completes.
- Rename, move, collection metadata/options, recursive drop, and snapshot-read
  behavior remain out of scope.

[FoundationDB's Directory Layer]: https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/directory/Directory.html
[bbolt]: https://pkg.go.dev/go.etcd.io/bbolt
[PostgreSQL system catalogs]: https://www.postgresql.org/docs/current/catalogs.html
[strong table locks]: https://www.postgresql.org/docs/current/explicit-locking.html
[MySQL atomic DDL]: https://dev.mysql.com/doc/refman/8.4/en/atomic-ddl.html
