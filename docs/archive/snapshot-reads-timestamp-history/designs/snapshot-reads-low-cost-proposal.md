# Low-cost always-on snapshot proposal

> **Archived — superseded design sketch.** This intermediate response to the
> timestamp-design review was replaced by the
> [dependency-checkpoint design](../../../designs/snapshot-reads.md).

## Status and constraints

This is a design sketch responding to the review of
[`snapshot-reads.md`](snapshot-reads.md) and to the requirement that snapshot
capability remain permanently enabled without materially changing regular
transaction latency or throughput.

The target constraints are:

- every database always supports snapshots;
- ADR-051's eligible direct commit remains one leaf CAS;
- regular logged transactions retain ADR-020's operation and storage-wave
  shape, ADR-053 remains the only fallback policy, and ADR-054 logged
  publication remains `External`;
- no manifest, history payload, history index, or epoch-admission write is added
  before commit;
- history extraction, indexing, checkpoint publication, and compaction are
  background work;
- existing foreground objects may carry optional dependency summaries, while
  ordering attestations are emitted asynchronously and the backend may retain
  substantially more bytes; and
- a cache-complete snapshot execution performs zero backend operations after
  bind, regardless of its duration within the configured lifetime.

## Two feasibility boundaries

Two facts constrain every design satisfying those goals.

First, background work cannot reconstruct bytes destroyed by a successful
overwrite. A one-CAS direct commit can preserve its predecessor without a
second foreground operation only if either the rewritten leaf carries a durable
undo history or the storage backend retains the overwritten object version. A
bounded in-leaf undo buffer is not sufficient: a hot key can fill it while the
background worker is unavailable, after which a writer must synchronously spill,
block, or discard promised history. This proposal therefore uses storage-native
object version retention.

Second, strict real-time order between disjoint transactions cannot be inferred
from per-object versions. It requires either a foreground coordination point or
a time source with a guaranteed uncertainty interval and commit-wait. Empirical
S3/GCS clock agreement is not a correctness mechanism. This proposal uses a
background-synchronized bounded-time source and a short local commit-wait after
the existing commit point. The wait adds no backend operation, but it is
logically part of foreground completion and cannot be removed while retaining
strict real-time order.

If even that normally-hidden wait is unacceptable, the contract must explicitly
weaken snapshot cuts from strict-serializable prefixes to dependency-consistent
prefixes. There is no object-store-only construction that simultaneously gives
strict real-time cuts, no per-write coordination, no guaranteed time source,
and no commit-wait.

## Storage-native raw version journal

Require every supported backend to retain every overwritten or deleted GlassDB
object version until GlassDB explicitly releases it. This is part of the one
database format, not an opt-in mode.

- S3 buckets use S3 Versioning. S3 creates a unique version ID for every
  overwrite, retains the previous complete object, and returns the version ID
  on the mutation. Each retained version is billed as a complete object; it is
  not a delta. See
  [How S3 Versioning works](https://docs.aws.amazon.com/AmazonS3/latest/userguide/versioning-workflows.html).
- Cloud Storage buckets use Object Versioning. Replaced objects remain
  addressable by generation, and versioned listing and generation-specific reads
  expose them to the compiler. See
  [Object Versioning](https://docs.cloud.google.com/storage/docs/object-versioning)
  and
  [using versioned objects](https://docs.cloud.google.com/storage/docs/using-versioned-objects).
- The in-memory and simulated backends model the same append-only raw-version
  behavior, including delayed listing, pagination, lost replies, and explicit
  version reclamation.

No bucket lifecycle rule may remove a raw GlassDB version before the compiler
has durably acknowledged it and every published checkpoint that references it
has left the retention window. Database open verifies the backend capability
and retention configuration. A dedicated bucket is preferable because both S3
and Cloud Storage configure version retention at bucket scope.

Extend the backend abstraction with background-oriented operations equivalent
to:

```text
list_versions(prefix, cursor, limit)
read_version(path, immutable_version)
delete_version(path, immutable_version)
```

The current foreground `Version` remains opaque. On S3 its internal form must
carry both the ETag needed for current-object CAS and the version ID needed for
an immutable read; on Cloud Storage the generation already serves both roles.
These additions do not change the foreground operation count.

Native object versions form a lossless raw journal:

- successive leaf versions contain every ADR-051 direct commit, including
  several disjoint direct commits batched into one coordinator CAS;
- logged transaction objects contain their authoritative values and terminal
  outcome under ADR-020;
- leaf versions expose logged write-back, tombstones, splits, and lock state;
  and
- catalog and root object versions preserve structural and collection changes.

Raw versions are temporary ingestion material. They are compacted into
snapshot-specific structures as soon as possible so a hot leaf does not retain
an unbounded number of full-object versions. This is operationally important:
AWS documents possible `503` degradation for objects with millions of retained
versions and recommends avoiding that state.

## Foreground transaction protocol

### Direct commits

Keep ADR-051 and ADR-053's leaf mutation unchanged:

1. The existing CAS atomically validates the predecessor and publishes
   `Inline { writer, value }`.
2. After the successful response proves the CAS applied, sample the local
   bounded-time source and choose a completion timestamp at the interval's upper
   bound.
3. Enqueue a small ordering attestation containing the writer ID, immutable
   backend version ID, completion timestamp, and body digest. A background lane
   batches and durably publishes attestations.
4. Wait locally until the bounded-time source proves that timestamp is in the
   past, then report success without waiting for durable attestation publication.

There is still one backend operation, no transaction object, no lock, no
write-back, no timestamp field in the leaf, and no history write. The previous
leaf body remains durable because the backend versions the object. Sampling
after the successful response removes the need for a stamp-to-apply bound: a
commit's ordering timestamp is always later than its actual CAS.

Several disjoint direct commits combined in one coordinator CAS share its
immutable backend version and completion timestamp while retaining their
individual writer IDs. They become visible in one checkpoint batch, which is
consistent with their one physical linearization point.

### Logged commits

Use the regular locked ADR-020 protocol; ADR-027 remains removed by ADR-053.
After acquiring every lock and revalidating all dependencies:

1. perform the existing terminal transaction-object CAS with its authoritative
   value map;
2. after the successful response proves the commit point applied, sample the
   bounded-time source and enqueue an ordering attestation keyed by transaction
   ID; and
3. perform the same local commit-wait before reporting success, while ordinary
   write-back and attestation publication proceed asynchronously. Write-back
   publishes `External { writer }` and releases locks under ADR-054.

Pending holders carry no snapshot timestamp. A transaction that encounters one
already waits for its terminal outcome under ADR-020. Snapshot checkpoints are
published only after every transaction assigned to them is terminal, attested,
and ingested, so snapshot reads never have to resolve a pending holder at or
below their cut.

There is no precommit per-key payload, manifest, history certificate, timestamp,
or epoch admission. The existing terminal object remains the atomic and durable
authority until the background compiler has copied its values into independently
reclaimable snapshot state.

The compiler reconstructs serialization dependencies from the retained versions
of lock, leaf, root, and catalog objects. A compact dependency summary may be
added to the existing terminal object if benchmarks show that transferring more
foreground bytes is cheaper than replaying those raw coordination versions. It
is an optimization, not a new correctness source.

### Bounded time and commit-wait

Define a `BoundedTime` capability that returns an interval known to contain the
shared reference time and supports the predicate "this timestamp is definitely
in the past." Its uncertainty bound must be a documented service contract, not
a measured allowance. The synchronizer runs in the background and local reads
of its current interval perform no network operation.

Commit-wait is required before the success response. If transaction T returns
before U begins, true time is greater than T's chosen upper bound before U can
reach its own commit point, so U receives a greater completion timestamp even
when the transactions touch disjoint keys.

Transactions that overlap may receive completion timestamps in either order.
The compiler reconstructs lock and predecessor edges and computes each
transaction's effective order as the maximum of its completion timestamp and
its predecessors' effective order plus one logical tick. Every checkpoint is a
downward-closed prefix of that order.

Because the timestamp is sampled after the existing terminal CAS, the residual
wait is approximately the time source's current uncertainty rather than being
hidden by the CAS round trip. The target is sub-millisecond, with no backend
operation and no shared foreground object. The acceptance gate must measure and
budget that latency explicitly.

AWS documents an error-bound interface through
[ClockBound](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/compare-timestamps-with-clockbound.html)
on supported Linux EC2 instances. Google documents Compute Engine time
synchronization designed for sub-millisecond or one-millisecond accuracy, but
that wording is not by itself the hard interval contract required here. A
portable deployment may therefore need a separately specified bounded-time
service. S3 `Date` and Cloud Storage object timestamps remain unsuitable because
their API documentation gives no fleet-wide uncertainty bound.

Loss of the bounded-time guarantee must fail closed for commits or explicitly
mark their raw versions unordered and stop publishing new snapshots; it cannot
silently substitute an empirical margin. Letting ordinary writes continue in
the latter mode requires a recovery protocol that establishes a new exact
checkpoint before snapshot publication resumes.

### Asynchronous attestation and crash recovery

An attestation is not required for the transaction's durability or latest-value
visibility. It is required only before the compiler advances a snapshot frontier
past that commit. Attestations are immutable and idempotently keyed, and a
per-client background lane may batch many of them in one object operation.

The compiler enumerates raw object versions as the completeness source. A
terminal direct or logged commit with no matching attestation blocks frontier
advancement; it is never silently omitted. This covers a process crash after the
commit response and before its queued attestation becomes durable. Ordinary
transactions continue using their normal protocol while the frontier is
stalled.

Recovery has two safe choices:

- recover the exact timestamp from a local durable outbox if one exists; or
- establish a new full strict checkpoint that includes the unattested commit and
  every later dependency, then resume incremental compilation above that
  baseline.

Inventing a timestamp from discovery time is not safe: a disjoint transaction
that completed later might already carry an earlier durable attestation. The
baseline recovery must therefore be designed and tested before asynchronous
attestation can be accepted.

### Zero-wait semantic alternative

If regular transactions may not incur even the bounded local commit-wait, omit
completion timestamps and let asynchronous attestations identify only the raw
commit artifacts. The compiler reconstructs the lock/predecessor graph and may
publish any finite downward-closed set it has completely ingested.

That still provides an atomic, dependency-consistent stable view: it never
fractures a multi-key transaction or includes a transaction without its logical
predecessors. It is not a prefix of the strict real-time order. A checkpoint may
include disjoint U while omitting T even when T returned before U began, and its
freshness cannot be stated as a hard wall-time bound. The API and ADR would need
to say this directly rather than call the result a strict-serializable bounded-
staleness cut.

This alternative removes all snapshot-induced foreground latency and the
bounded-time dependency. It is compatible with strong regular transactions only
because `read_tx` explicitly selects the weaker read contract; it is not a fix
for the original strict snapshot contract.

## Background history compiler

The compiler converts raw object versions into immutable, cut-addressed snapshot
trees. It is a cooperative, ownerless state machine; a crashed worker can be
replaced without discarding completed work.

For each cut-grid slot it:

1. chooses a target cut strictly before a bounded-time observation taken before
   its completeness traversal starts, so any commit that reaches its terminal
   point during the traversal receives a later completion timestamp;
2. enumerates ordering attestations, committed logged transaction objects, and
   versioned leaf, catalog, root, and lock objects relevant to the candidate
   prefix;
3. refuses to advance if the traversal finds any terminal raw commit without a
   durable attestation, because an unattested commit has no safe position
   relative to the candidate;
4. reconstructs direct commits by comparing consecutive immutable leaf versions
   and reconstructs logged commits from their terminal objects;
5. reconstructs serialization edges, computes effective order, and verifies
   outcomes, digests, and structural invariants;
6. applies the downward-closed transaction prefix to the previous immutable
   snapshot root, copying only changed paths and sharing unchanged pages;
7. publishes one small checkpoint certificate naming the new data and catalog
   roots only after every referenced object is durable; and
8. advances a monotone `history_ready` frontier.

Provider change notifications and a cached revision map may accelerate step 2,
but they are not completeness proofs unless the provider contract says they are.
The correctness path performs a complete versioned traversal for every frontier
advance or uses another reviewed mechanism that proves no closed-slot version
was omitted. ADR-055-style current-revision listings can identify leaves needing
version expansion, while periodic full reconciliation closes notification gaps.

The checkpoint tree replaces per-key foreground history. It is built by one
logical background publisher, so the objection to copy-on-write trees in the
current ADR-039 no longer applies: regular writers do not publish snapshot roots
and never contend on them. Fixed policy retention replaces reader pins. Each
checkpoint is an exact materialized database state across data and catalog,
while structural sharing bounds copies to changed paths.

The compiler may coalesce every intermediate overwrite of a key inside a slot,
retaining only the slot-final value in the published checkpoint. The raw native
versions make that coalescing recoverable even after the current leaf has moved
on.

The snapshot cut is:

```text
min(latest time-admissible grid point, history_ready)
```

If the compiler falls behind, the frontier stops and snapshot acquisition
becomes staler or returns `FreshSnapshotUnavailable`. Ordinary transactions do
not switch protocols, synchronously help history, or lose the direct path. Raw
versions accumulate until the compiler recovers. Queue stationarity and raw
version count are therefore correctness-relevant operational requirements, not
optional tuning metrics.

This deliberately places overload on snapshot freshness and storage rather than
on foreground latency. No finite design can additionally guarantee bounded
snapshot freshness under arbitrary background failure and unbounded write load.

## Snapshot reads and cache behavior

`read_tx` binds one published checkpoint certificate, not a timestamp against
the mutable live tree. A point lookup, scan, or catalog lookup traverses the
immutable checkpoint roots and pages named by that certificate. It never
resolves live holders, revalidates current leaves, or mixes current topology with
historical contents.

At bind, capture:

- a checkpoint certificate satisfying the requested freshness;
- bounded-age operational-state and history-floor evidence; and
- the fixed execution deadline.

The retention protocol honors that evidence for the maximum execution lifetime.
After bind there is no periodic refresh. Before a cache miss, refresh expired
control evidence and reject a checkpoint below the floor before reading possibly
reclaimed data. A fully cached execution performs no backend operation even when
it runs longer than the control-staleness interval.

An emergency floor advance may therefore stop a reader only when it next needs
backend data. A reader holding every required immutable object may still finish
correctly from cache. Requiring every running reader to observe revocation would
be incompatible with the zero-I/O path.

## GC and retention

There are two reclamation layers:

1. **Raw ingestion versions.** Retain every provider object version and logged
   transaction object until a durable compiler acknowledgement proves it is
   represented in a published checkpoint or is an uncommitted orphan fenced
   against later publication.
2. **Published checkpoints.** Retain checkpoint certificates, roots, shared
   pages, value objects, and catalog state for maximum staleness plus maximum
   execution lifetime plus the safety and control-evidence guards.

GC publishes the history floor before deleting anything it authorizes. Deletes
name exact noncurrent version IDs or immutable checkpoint paths and cannot
delete the live mutable object accidentally. A lifecycle policy based only on
age or "number of newer versions" is insufficient because compiler lag and live
snapshot lifetime, not provider age alone, determine liveness.

## Performance acceptance gate

The foreground gate is deliberately stricter than the current proposal:

- an eligible ADR-051 transaction performs exactly one conditional leaf write;
- every regular transaction shape performs exactly the accepted ADR-020 backend
  operations and synchronous storage waves;
- ADR-051 direct eligibility and ADR-053 replay/fallback rates remain within
  measurement noise of baseline;
- no transaction synchronously writes or helps a history/compiler object;
- added request bytes are bounded and reported separately for direct leaf state,
  lock state, terminal objects, and write-back;
- residual commit-wait time is measured at p50/p95/p99 and must fit the explicit
  foreground latency budget for every supported bounded-time source; and
- foreground latency and throughput are measured with the compiler and raw
  version GC running at stationary offered load, not with background work
  disabled.

The background gate reports:

- raw versions created, listed, read, compacted, and deleted per logical write;
- full-object temporary bytes caused by native versioning;
- checkpoint copy-on-write bytes per changed key and per cut;
- `history_ready` lag and `FreshSnapshotUnavailable` rate;
- version count for a continuously hot leaf;
- provider listing and notification reconciliation cost; and
- queue stability through a complete retention window and after a simulated
  compiler outage.

The warm-cache acceptance cell preloads the checkpoint certificate, control
evidence, roots, pages, and values, runs longer than the control refresh
interval, and requires exactly zero backend operations.

## How this resolves the review findings

| Review finding | Resolution |
|---|---|
| Non-users pay foreground history costs | Capability stays on, but regular transactions retain their accepted operation/wave shape; the cost is provider-retained bytes and background compilation |
| Periodic I/O breaks cache-complete reads | Bind evidence once and refresh it only before a backend miss |
| Disjoint commits can violate real time | Post-commit bounded-time attestation plus commit-wait orders success responses without a global commit object |
| S3/GCS fleet skew is not guaranteed | S3/GCS response clocks leave the proof; a separately qualified `BoundedTime` capability is required |
| ADR-027 and logged inline publication are obsolete | ADR-051 direct and ADR-020 regular locking remain; ADR-053 selects the fallback and ADR-054 publishes logged values as `External` |
| Precommit history work was omitted | There is no precommit history work; asynchronous attestations and the compiler must complete before `history_ready` advances |

## Rejected alternatives under these constraints

- **Creation-time opt-out.** It violates the requirement that every database
  carry snapshot capability.
- **Per-commit epoch admission.** It adds a foreground operation and serialized
  storage wave, even when implemented with lanes.
- **Precommit manifests and per-key payloads.** They make history durable, but
  directly violate the critical-path constraint.
- **A bounded in-leaf undo queue.** It is cheap only while the compiler keeps up;
  an outage eventually forces synchronous spill, write blocking, or history
  loss.
- **Asynchronous history without native object versions.** A second overwrite
  can destroy a direct commit before the compiler observes it.
- **Empirical backend time plus a larger margin.** It lowers probability without
  creating a correctness guarantee.
- **Commit-wait without bounded time.** Waiting on an unbounded or untrusted
  clock proves no ordering property.
- **Removing commit-wait while retaining strict real-time wording.** A later
  disjoint transaction can receive an earlier timestamp. The honest alternative
  is to weaken the snapshot contract.

## Decisions required before adoption

1. Qualify a portable bounded-time source, or explicitly limit supported
   deployments to environments with one.
2. Specify asynchronous-attestation recovery after a crash loses the queued
   timestamp, including how a new exact baseline is established without
   exposing a partial checkpoint.
3. Verify S3 and Cloud Storage versioned-listing semantics are sufficient for a
   complete closed-slot traversal and extend ADR-023 accordingly.
4. Benchmark native versioning on hot leaves, including S3's documented
   high-version-count degradation.
5. Prototype the incremental immutable checkpoint tree and measure its
   copy-on-write and GC amplification.
