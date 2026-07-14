# ADR-035: Paginated listing and sharded transaction logs

## Status

Accepted — implemented.

Refines the transaction-object path of
[ADR-019](019-unified-transaction-object.md), the candidate discovery of
[ADR-022](022-garbage-collection-mark-sweep.md), and the `list` method of
[ADR-023](023-slimmed-backend-trait.md). Their transaction lifecycle, value
safety, horizon, and reclamation decisions are unchanged.

[ADR-034](034-separate-structural-log-namespace.md) remains authoritative for
structural records: they stay in the independent `_s` namespace and recovery
loop and are not transaction-log records or GC candidates.

## Context

`Backend::list` returns every immediate child of a directory in one `Vec`. The
S3 and GCS backends consume every provider page before returning, and GC then
sorts the complete flat `_t/` result merely to select a small local page. A
database with many transaction objects therefore pays unbounded transfer,
latency, and memory on every nominally paged GC cycle.

A lexicographic `start_after` cursor is not portable: S3 directory buckets
support continuation tokens but neither `StartAfter` nor ordered listing.
Provider-issued opaque cursors are the common primitive across S3 and GCS.

An opaque cursor cannot be constructed at a random position. Always restarting
a large flat `_t/` traversal from the provider's beginning would bias recovery
toward the same records after process restarts. Production transaction IDs
already begin with eight random bytes, so their existing encoding provides
portable, independently traversable partitions.

Structural recovery does not share this scaling problem. ADR-034 places its
short-lived, low-cardinality records under database-wide `_s` and gives them an
independent recovery cadence.

Dynamic range sharding traverses collection nodes through the B-link topology,
not by listing shard or node directories. The current engine consumers of
`Backend::list` are therefore transaction GC and structural recovery, and both
need object paths rather than immediate-child prefixes.

## Decision

### `Backend::list` returns one recursive page

Replace the all-at-once directory operation with a paginated prefix operation:

```text
list(prefix, cursor, limit) -> ListPage { objects, next }
```

- `prefix` is empty or ends in `/`. It selects object keys recursively; results
  contain actual object paths, never synthesized immediate-child prefixes.
- `cursor` is an opaque token previously returned by the same backend for the
  same prefix. Callers cannot inspect or construct one.
- `limit` is positive. A page contains at most `limit` objects, but may contain
  fewer or even none. Only `next = None` means the traversal is complete.
- A successful call returns one provider page and does not drain subsequent
  pages internally. Normal idempotent request retries are unchanged.
- Result order is unspecified and a traversal is not a snapshot. Concurrent
  creates and deletes may cause omissions or duplicates; a later complete
  traversal revisits the prefix.
- A provider-rejected cursor is reported distinctly as `InvalidCursor`. The
  caller can discard it and restart that traversal instead of retrying it
  forever.

S3 maps `cursor` to `ContinuationToken` and `limit` to `MaxKeys`; GCS maps them
to `pageToken` and `maxResults`. Both omit the delimiter. This subset also
supports S3 directory buckets, whose listing prefixes must end in `/`.

### Transaction logs use 4,096 deterministic shards

Transaction records move from `{db}/_t/{encoded-txid}` to:

```text
{db}/_t/{ss}/{encoded-txid}
```

`encoded-txid` is the existing order-preserving base64 encoding of the raw
transaction ID, and `ss` is its first two characters. The 64-character alphabet
yields 4,096 shards selected by the first 12 random bits of a production
transaction ID. Keeping the full encoding as the filename makes shard
derivation reversible.

Structural records remain at `{db}/_s/{record-id}` as decided by ADR-034. They
are neither placed in `_t` nor sharded by this ADR.

This changes the unreleased v2 layout in place. There is no flat-to-sharded
migration or compatibility fallback.

### GC makes shuffled passes over the transaction shards

GC knows the finite set of 4,096 shard prefixes. At the start of a pass it
shuffles them, then traverses each shard using the opaque cursor returned for
that prefix. One cycle skips empty pages and completed shards until it obtains
one non-empty page or exhausts a bounded list-request budget. It processes at
most that page of listed transaction candidates.

The shuffled order, current shard, and cursor are disposable in-memory state.
Completing all shards starts a newly shuffled pass; a process restart also
starts a new shuffle. An invalid cursor restarts only its current shard.
Shuffling uses the deterministic entropy seam so simulation replay remains
stable.

The page size and per-cycle request budget bound useful work and the fixed cost
of skipping sparse shards. Backend transport retries remain governed by the
backend retry policy. Write-back hints remain the primary source of timely GC
candidates; the sharded traversal remains the completeness mechanism.

Once listed, a transaction follows ADR-022's existing policy, including the
ADR-032 implementation refinements: GC re-resolves recorded keys through the
current B-link topology and routes recorded node-lock cleanup and other
reclamation mutations through the coordinator. This ADR changes discovery, not
the liveness proof.

ADR-034's `_s` recovery loop consumes the paginated backend contract on its own
schedule. It may drain its short-lived, low-cardinality prefix and does not
share GC's shard order, cursor, or request budget.

## Consequences

- Listing transfer and memory are bounded per backend call, and `_t` discovery
  work is bounded per GC cycle.
- The contract is portable to S3 directory buckets. The cost is losing
  lexicographic positioning, stable result order, and immediate-child directory
  entries; current engine callers require none of them.
- Four thousand ninety-six shards impose a fixed sparse-database request tax.
  Shuffling prevents fixed restart bias, while the per-cycle request budget
  prevents that tax from becoming a request burst.
- In-memory traversal state is deliberately disposable. Restart may repeat work
  but requires no durable GC cursor.
- `Backend` implementations and middleware must adopt page/cursor forwarding
  and the new `InvalidCursor` error. This is a breaking trait change.
- The sharded `_t` path is a format change for development databases. Because
  v2 has not shipped, they are recreated rather than migrated.
- A traversal may miss a concurrent object until a later pass or see an object
  more than once. ADR-022's authoritative reverse check makes duplicate or
  delayed candidates safe.
