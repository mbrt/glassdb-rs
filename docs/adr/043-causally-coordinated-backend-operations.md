# ADR-043: Causally coordinated backend operations

## Status

Accepted.

This depends on
[ADR-042](042-conditional-only-backend-mutations.md), partially supersedes
[ADR-036](036-decoded-object-cache-with-bounded-freshness.md)'s operation-order
and publication rules, and refines ADR-009's treatment of cancellation after a
mutation is dispatched.

## Context

ADR-036 stamps a backend result with the local time at which its operation
started. That is a sound freshness lower bound: the returned state was current
at some point after invocation. It is not enough to order concurrent results.

The failure is sharp for absence. A read can be invoked after a create was
invoked, observe absence before the create linearizes, and return after the
create succeeds. Ordering those publications only by invocation lets the
absence replace the created object and leak a false `NotFound`. Response order
does not fix this: overlapping object-store operations are not ordered by the
order in which their acknowledgements arrive.

Strongly consistent object stores provide the useful real-time edge instead:
an operation invoked after a definitive earlier operation completes observes
that operation or a later state. GlassDB needs to represent that edge and align
backend invocation, cache publication, cancellation, and API completion around
it.

## Decision

### Local operation order

`Backend` requires linearizable single-object reads and conditional mutations,
including read-after-definitive-completion consistency. Eventually consistent
implementations are not supported.

Replace `LogicalTime` with two deliberately distinct concepts:

- `SequencePoint` is a strictly ordered event on one open `Database`. It is not
  persisted, exchanged, or shared by separately opened database instances.
- `MonotonicInstant` is elapsed local time and is not causal evidence.

Every backend call has an internal span with explicit completion certainty:

```text
OperationSpan { invoked: SequencePoint, completion: Completion }
Completion = Definitive(SequencePoint) | InDoubt(SequencePoint)
```

`invoked` is allocated immediately before dispatch. The completion point is
allocated after the result has been reconciled with local knowledge and before
the operation's future becomes ready. For definitive outcomes:

```text
point(A.completion) < B.invoked  =>  A happens before B
```

Overlapping spans are incomparable. Invocation order, completion order, and
acknowledgement order alone do not order them. An in-doubt operation has a local
completion point but establishes no definitive backend happens-before edge.
One span covers the complete backend method, including provider retry attempts;
ADR-009 determines when an ambiguous retry sequence must remain in doubt.

Completion certainty and cache knowledge are separate:

```text
CacheKnowledge   = Present | Absent | Uncertain
```

A definitive read whose changed body cannot be decoded, for example, has a
definitive backend outcome but leaves cache knowledge uncertain.

### Serialize point operations per path

One database-local coordinator serializes actual point-object backend calls per
physical path. Calls on different paths remain concurrent. The initial design
uses an exclusive lane for reads and mutations alike:

```text
check cache
-> acquire path lane
-> check cache again
-> allocate invoked
-> call backend
-> reconcile knowledge
-> allocate completed
-> release path lane
-> return
```

Reconciliation happens before completion while the lane is still held. No
operation holds two path lanes, and cache reconciliation invokes no
higher-level protocol, avoiding lock-order cycles.

An `Any` cache hit does not acquire the lane and may return an older state while
a mutation is active. A read requiring newer evidence waits, rechecks after it
acquires the lane, and avoids a backend call if the preceding operation already
satisfied it.

Concurrent same-path backend reads are a permitted later optimization internal
to the coordinator. They may share or merge identical states; incompatible
overlapping results make the discoverable cache state uncertain. This
optimization is enabled only after a qualitative benchmark shows that exclusive
reads materially regress slow-read or hot-object workloads.

### Evidence and publication

An observation retains `observed_after`, set to the definitive backend
operation's `invoked` point. Completion is a causal boundary, not a freshness
watermark, and is not exposed by `Observation`.

Successful create, CAS, and conditional delete return an exact observation of
the state they installed. Their successful precondition also advances the
retained expected observation to the operation's invocation point. Same-state
validation merges evidence. A later serialized operation may replace earlier
discoverable knowledge; a result that proves the cached state obsolete but
cannot supply a usable replacement invalidates it.

`Uncertain` initially needs no LRU-resident tombstone: exclusive point
operations eliminate delayed local publishers, so removing the usable cache
entry is sufficient. Retained observations preserve their historical evidence
but are not discoverable by new cache reads. A future concurrent-read
optimization may add provenance-bearing tombstones internally.

ADR-036's `read_stale` remains an approximate cache policy only. It may use a
`MonotonicInstant` to derive an explicitly approximate `SequencePoint` cutoff,
but duration-to-sequence conversion is confined to that API. Causal validation,
mutation receipts, and recovery never perform time arithmetic. Snapshot reads
are expected to supersede `read_stale` for real bounded-staleness guarantees.

### Cancellation and lifecycle

Waiting for a path lane is cancellable. At backend invocation, ownership of a
mutation transfers to the database: dropping the caller stops waiting but does
not cancel the dispatched mutation. The owned worker runs through definitive
publication or uncertainty and is included in graceful-shutdown draining.
Reads remain cancellable because they cannot change backend state.

Any panic, task failure, or lost result after mutation dispatch and before
definitive publication is treated like `InDoubt`: invalidate usable knowledge,
notify the waiter of uncertainty when possible, and release the lane. Encoding
or validation failures before dispatch do not invalidate cache state.

Graceful shutdown rejects new public asynchronous operations, drains already
admitted public operations and protocol/background producers, then closes and
drains the backend-operation coordinator. A caller may independently bound
shutdown with a timeout. A slow dispatched backend mutation can block its path
and graceful drain; per-operation timeouts are a follow-up operational policy,
not part of this decision.

Database metadata initialization uses the same spans and conditional
primitives, but occurs before an open database owns background work. Cancelling
`Database::open` is treated as crashing an opener; create-if-absent metadata is
safe under that exception.

### Complete backend boundary

Production engine code performs every backend operation through this boundary.
Database metadata and cached point objects receive operation spans; point
objects additionally use path lanes and cache reconciliation. Backend tests and
standalone backend benchmarks may call a raw backend directly.

Listing is not a point operation and takes no path lane. Each page has its own
`OperationSpan`. S3 documents a continuation token only as an opaque way to
[continue a later request](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html),
not as a snapshot token. A paginated traversal therefore makes no cross-page
consistency claim. Protocols needing a consistent traversal retain their
membership/version validation or recovery logic.

### Verification

Deterministic state-machine and gated tests cover disjoint and overlapping
operations, the false-absence race, path parallelism, cache hits during
mutation, both cancellation boundaries, every certainty/knowledge combination,
conditional deletion, retained observations, and shutdown draining.
Transaction simulation remains the end-to-end check under reordered responses,
lost acknowledgements, outages, and client crashes. Hot-object, slow-read, and
ordinary multi-path benchmarks gate any relaxation of the exclusive read lane.

## Discarded options

### Order publications by invocation or completion alone

Invocation is a freshness lower bound, not a linearization point. A later
invoked read may linearize before an earlier invoked create and return absence,
which is the false-`NotFound` failure that motivated this ADR. Completion order
also cannot order overlapping operations: object stores do not promise that
acknowledgements arrive in linearization order.

Stamping a result with completion as `observed_after` is stronger than either
ordering mistake. Another client may replace the state after this operation
linearizes but before its response arrives, so completion would claim freshness
the result never established.

### Track spans without serializing same-path calls

Retaining all same-path concurrency would preserve more theoretical parallelism,
but it changes cache publication from an ordered state transition into a
persistent partial-order protocol. Different states returned by overlapping
operations are incomparable, so the cache would need to:

- retain active-operation provenance outside the evictable LRU;
- preserve an uncertainty tombstone until every overlapping publisher has
  completed or become in doubt;
- make operation registration, eviction, invalidation, and publication atomic
  with respect to delayed results;
- distinguish same-state evidence merging from conflicting present/absent
  observations and revision/content ABA; and
- encode separate deductions for reads, creates, CAS success and conflict,
  conditional delete, decoding failure, and cancellation.

Richer operation-specific proofs can resolve some schedules, but external
writers mean they cannot eliminate the incomparable cases. In-doubt mutations
are worse: their remote interval may remain open after local completion, so a
later publisher cannot close uncertainty generically.

Per-path serialization collapses definitive local operations into a chain.
After a lane is released there is no delayed local publisher, so the current
entry or its invalidation is sufficient; no long-lived provenance registry is
needed. It preserves concurrency across paths, where GlassDB obtains most of
its parallelism, while existing read coalescing and shard mutation coordination
already reduce useful same-path overlap. Shared reads remain a measured,
internal follow-up rather than making the initial correctness state machine
substantially larger.

### Serialize mutations but not reads

The motivating race is between a read and a mutation, so mutation-only
serialization leaves the false-absence schedule intact. Allowing concurrent
reads immediately has a smaller but real version of the same problem: an
external writer can make two overlapping reads return different states.

### Serialize every object globally

A database-wide lane would also establish an order, but would couple unrelated
objects and discard object storage's useful parallelism. Ordering is required
only among local operations on the same physical path; `SequencePoint` remains
global so transaction barriers can still cover multiple paths.

### Store precise elapsed-time evidence for `read_stale`

Keeping both a `SequencePoint` and `MonotonicInstant` in every observation would
give `read_stale` a more exact age test, but would add a second evidence and
requirement algebra solely for an approximate API expected to be superseded by
snapshot reads. The chosen approximate conversion is quarantined from causal
validation instead.

### Expose completion through `Observation`

Callers need the invocation evidence established by an exact state. Completion
is consumed by the coordinator to order later operations; once publication has
happened and the future is ready, retaining it in every observation adds no
proof a caller currently needs.

## Consequences

- Local cache publication follows the same real-time order guaranteed by the
  backend instead of inventing an order for overlapping operations.
- The false-absence class is removed without weakening `Any` cache reuse or
  serializing operations on different objects.
- Same-path backend latency can cause head-of-line blocking. Existing read
  coalescing and higher-level shard mutation coordination reduce the common
  cost; safe shared reads remain available as a measured optimization.
- Dispatched mutations may outlive their callers, so the database owns and
  drains them.
- Separate `Database` openings have independent causality domains. A cache hit
  in one instance does not inherit completion in another.
- `SequencePoint` remains exact for causality while `read_stale` is explicitly
  approximate and isolated from correctness protocols.
- Lists remain strongly observed per request but are not multi-page snapshots.
