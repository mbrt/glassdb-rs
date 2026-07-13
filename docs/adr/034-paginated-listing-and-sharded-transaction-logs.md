# ADR-034: Paginated listing and sharded transaction logs

## Status

Proposed.

Refines the transaction-object path of
[ADR-019](019-unified-transaction-object.md), the candidate discovery and live
reference set of [ADR-022](022-garbage-collection-mark-sweep.md), the `list`
method of [ADR-023](023-slimmed-backend-trait.md), and the structural-record
recovery of [ADR-032](032-node-locking-and-coordinated-splits.md). Their value
safety, horizon, transaction lifecycle, and structural-reachability decisions
are unchanged.

## Context

`Backend::list` returns every immediate child of a directory in one `Vec`. The
S3 and GCS implementations consume all provider pages before returning, and GC
then sorts the complete `_t/` result merely to select a small local page. A
database with many transaction objects therefore pays unbounded transfer,
latency, and memory on every nominally paged GC cycle.

A lexicographic `start_after` cursor would fix the immediate problem, but it is
not portable: S3 directory buckets support continuation tokens but neither
`StartAfter` nor ordered listing. Provider-issued opaque cursors are the common
primitive across S3 and GCS.

[ADR-032](032-node-locking-and-coordinated-splits.md) also changes the role of
the `_t/` scan. Structural split records share the transaction-log schema, and
scanning them is the primary way a fresh process discovers recovery work. The
old split-active registry and blind node listing are replaced by proof from the
structural record: reachable created nodes are rolled forward; provably
unreachable ones are deleted; ambiguity is retried. The scan must therefore
bound each cycle while eventually covering both ordinary and structural logs.

An opaque cursor cannot be invented at a random position. Always restarting a
large flat `_t/` traversal from the provider's beginning would bias recovery
toward the same records. Transaction IDs already begin with high-entropy bytes,
so their existing encoding can provide portable, independently traversable
shards.

## Decision

### `Backend::list` returns one recursive page

Replace the all-at-once directory operation with a paginated prefix operation:

```text
list(prefix, cursor, limit) -> ListPage { objects, next }
```

- `prefix` is empty or ends in `/`. It selects all object keys recursively; the
  result contains actual object paths, never synthesized immediate-child
  prefixes.
- `cursor` is an opaque token previously returned by the same backend for the
  same prefix. Callers cannot inspect or construct one.
- A page contains at most the positive `limit`, but may contain fewer objects or
  even be empty. `next`, not page length, is authoritative: `next = None` alone
  means the traversal is complete.
- A successful call returns one provider page and does not drain later
  continuation pages internally. Normal idempotent request retries are
  unaffected.
- Result order is unspecified and the traversal is not a snapshot. Concurrent
  creates and deletes may make one traversal omit or repeat an object; an object
  that persists is found by a later complete traversal.
- A cursor rejected by its provider is reported distinctly as `InvalidCursor`,
  allowing a caller to discard it and restart that traversal rather than retry
  it forever.

S3 maps `cursor` to `ContinuationToken`; GCS maps it to `pageToken`. This subset
also supports S3 directory buckets, whose listing prefixes must end in `/`.

### Transaction logs use 4,096 deterministic shards

Both ordinary transaction records and ADR-032 structural records live at:

```text
{db}/_t/{ss}/{encoded-txid}
```

`encoded-txid` is the existing order-preserving base64 encoding and `ss` is its
first two characters. Its 64-character alphabet yields 4,096 uniformly
distributed shards. Keeping the full encoding as the filename makes the shard
derivation reversible and identical for every transaction-log kind.

This changes the unreleased v2 layout in place. There is no flat-to-sharded
migration and no compatibility fallback.

### One shuffled maintenance pass covers every shard

The maintenance scanner knows the finite set of 4,096 shard prefixes. At the
start of a pass it shuffles them, then traverses each shard with its opaque
cursor. A cycle skips empty pages and completed shards until it obtains one
non-empty page or exhausts its list-request budget. It processes at most that
page's configured number of listed records.

The shard order, current shard, and cursor remain in memory. Completing all
shards starts a newly shuffled pass; process restart also starts a new shuffle.
An invalid cursor restarts only its current shard. Randomness uses the existing
deterministic entropy seam so simulation replays remain stable.

The scan interval, page size, and maximum logical list requests per cycle are
configurable. The request budget bounds the fixed cost of skipping sparse
shards; transport retries remain governed by the backend's retry policy.

Each listed record is dispatched by kind before reclamation:

- An ordinary transaction follows ADR-022: apply the safety horizon, reverse
  check every recorded reference, force-abort a dead pending transaction before
  release, and retain aborted tombstones through their post-abort horizon.
- A structural record follows ADR-032, never ordinary transaction status as its
  authority. Its created tokens remain live roots until structural reachability
  proves whether to roll forward or delete them; ambiguity defers recovery.

ADR-032 also expands ordinary transaction liveness to node structure- and
membership-lock holders in addition to `current_writer` and per-key holders.
GC re-resolves recorded keys through the current B-link topology, and every
reclamation mutation takes the required structure lock and flows through the
coordinator as required by ADR-029/032.

The ADR-032 structural log replaces the split-active registry and the blind
`_n/` orphan sweep. Those all-at-once listing consumers are removed rather than
rebuilt on a `list_all` helper.

### Right-link traversal hints the performance-sensitive repair

A normal traversal that must follow a right-link submits a deduplicated,
best-effort structural-repair hint. Background recovery verifies the current
topology and idempotently publishes a missing parent separator. Following a
right-link can also result from a stale cached parent after publication, so a
hint may legitimately resolve to a no-op; ambiguity is deferred rather than
guessed.

The hint repairs only the missing separator. It need not identify the structural
record and adds no reverse pointer to the node format. The paginated `_t/` scan
remains responsible for proving orphanhood and finalizing or deleting the
structural record. Dropping a hint affects performance, not correctness or
eventual recovery.

## Consequences

- Listing transfer and memory are bounded per call, and `_t/` discovery work is
  bounded per maintenance cycle. Backends no longer materialize an entire
  logical directory to return one GC page.
- The contract is portable to S3 directory buckets. The cost is losing
  lexicographic positioning, stable order, and immediate-child directory
  entries, none of which the engine needs after ADR-032 removes the registry and
  blind node sweep.
- Structural recovery has no fixed latency bound: discovering a rare record is
  proportional to a complete `_t/` pass and depends on the configured scan
  throughput. Right-link hints accelerate the case that affects foreground
  traversal; delayed scan recovery otherwise retains safe extra nodes, records,
  or links under ADR-032's invariants.
- Four thousand ninety-six shards impose a fixed sparse-database tax. Shuffling
  prevents a fixed restart bias, while the per-cycle request budget prevents
  that tax from becoming a request burst. Write-back and structural hints remain
  opportunistic accelerators; the scan is the completeness mechanism.
- In-memory traversal state is deliberately disposable. Restart may repeat work,
  but requires no durable GC cursor and begins from a different shard order.
- `Backend` implementers and middleware must adopt page/cursor forwarding and
  the new `InvalidCursor` error. This is a breaking trait change.
- The sharded `_t` path is a format change for development databases. Because v2
  has not shipped, they are recreated rather than migrated.
- A traversal may miss a concurrent object until the next pass or see an object
  more than once. ADR-022's authoritative reverse check and ADR-032's
  proof-before-delete rule make duplicate or delayed candidates safe.
