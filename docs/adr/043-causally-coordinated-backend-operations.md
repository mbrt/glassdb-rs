# ADR-043: Causally coordinated backend operations

## Status

Accepted — implemented.

This depends on
[ADR-042](042-conditional-only-backend-mutations.md), partially supersedes
[ADR-036](036-decoded-object-cache-with-bounded-freshness.md)'s operation-order
and publication rules, refines ADR-009's treatment of cancellation after a
mutation is dispatched, and defines ADR-032's exception for structural objects
created after their operation was abandoned.

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

Replace `LogicalTime` with `SequencePoint`, a strictly ordered event on one open
`Database`. It is not persisted, exchanged, or shared by separately opened
database instances. Its allocator remains internally coupled to a monotonic
elapsed-time floor so `read_stale` can derive an approximate cutoff; numeric
distance between sequence points otherwise has no meaning, and no
`MonotonicInstant` becomes part of the cache interface.

For reasoning, every causally coordinated backend call has a conceptual interval
from invocation to local completion. An invoked call is pending until it settles
definitively or in doubt. These are specification labels, not an `Operation`
struct or a `Completion` enum.

The invocation point is allocated immediately before dispatch. Local completion
occurs after the result has been reconciled with local knowledge and before the
operation's future becomes ready. A pending or in-doubt operation establishes no
definitive backend edge. A definitive completion before another operation's
invocation establishes the backend's real-time edge. Invocation order alone
cannot distinguish disjoint operations from overlapping ones; the per-path lane
below supplies that distinction.

The interval is specification notation, not a persisted `OperationSpan`, and
no completion sequence point is stored. One interval covers the complete
backend method, including provider retry attempts; ADR-009 determines when an
ambiguous retry sequence must remain in doubt. For an abandoned call, local
completion means that GlassDB stopped driving the future, not that the remote
request completed.

Cache entries remain `Present` or `Absent`. Removing an entry represents no
usable current knowledge, including a definitive changed read whose body cannot
be decoded. This is ADR-036's `Missing` condition, renamed in this ADR to
"uncertain" to avoid confusing it with observed absence; it is not a third
stored entry variant.

### Required types and ownership

The implementation introduces these named types:

- `SequencePoint`, replacing `LogicalTime`;
- a private `PathCoordinator`, owned and shared by `CachedStore`, for per-path
  admission and compatible-read joining;
- a private `PathPermit`, representing ownership of one path lane;
- a private `ReadAdmission` enum, distinguishing joining the existing read
  flight from leading it with a `PathPermit`;
- a private `ExpectedState` enum, distinguishing expected absence from an
  expected present revision; and
- a private `MutationGuard`, holding the expected state and path permit so a
  dropped invoked mutation invalidates knowledge before releasing the lane.

`PathCoordinator` is an internal helper, not another storage layer. It does not
own the cache or drive backend futures: `CachedStore` performs the cache recheck,
backend call, and reconciliation while holding its permit. The existing read
flight and its shared outcome remain the coalescing mechanism and move under
this coordinator.

No `Operation`, `OperationSpan`, `Completion`, `CacheKnowledge`, `Uncertain`, or
monotonic-instant type is introduced. `Requirement`, the present/absent cache
entry state, mutation results, and observation checks retain their existing
roles.

### Serialize point operations per path

The `PathCoordinator` serializes point-object backend calls per physical path
while this `Database` owns them. Calls on different paths remain concurrent. The
initial design uses an exclusive lane for reads and mutations alike. The
required ordering is:

```text
check cache
-> acquire path lane
-> check cache again
-> allocate invoked
-> call backend
-> definitive: reconcile knowledge, release path lane, return
   in-doubt: remove usable knowledge, release path lane, return `Unavailable`
   abandoned: remove usable knowledge, release path lane, no return
```

Reconciliation happens before definitive completion while the lane is still
held. In-doubt completion and abandonment make knowledge uncertain before
releasing the lane; abandonment is the same state transition without an error
being returned to the cancelled caller. No operation holds two path lanes, and
cache reconciliation invokes no higher-level protocol, avoiding lock-order
cycles.

An `Any` cache hit does not acquire the lane and may return an older state while
a mutation is active. Compatible read callers may also coalesce onto one lane
owner and share its single backend operation and invocation point. A caller
requiring newer evidence cannot join an operation invoked before its bound: it
queues, rechecks after acquiring the lane, and avoids another backend call when
the preceding result already satisfies it.

Multiple concurrent same-path backend reads are a permitted later optimization
internal to the coordinator; coalesced callers are not concurrent backend
operations. Distinct overlapping reads may merge identical states, while
incompatible results make discoverable cache knowledge uncertain. This
optimization is enabled only after a qualitative benchmark shows that exclusive
backend calls materially regress slow-read or hot-object workloads.

### Evidence and publication

An observation retains ADR-036's `current_after`, set to the definitive backend
operation's `invoked` point. Completion is a causal boundary, not a freshness
watermark, and is not exposed by `Observation`.

Successful create, CAS, and conditional delete return an exact observation of
the state they installed. Their successful precondition also advances the
retained expected observation to the operation's invocation point. Same-state
validation merges evidence. A later serialized operation may replace earlier
discoverable knowledge; a result that proves the cached state obsolete but
cannot supply a usable replacement invalidates it.

Uncertain knowledge initially needs no LRU-resident tombstone: exclusive point
operations eliminate delayed local publishers, so removing the usable cache
entry is sufficient. An abandoned mutation may still apply remotely, but it can
never publish a delayed result locally and is treated like a writer from another
database instance. Retained observations preserve their historical evidence but
are not discoverable by new cache reads. A future concurrent-read optimization
may add provenance-bearing tombstones internally.

Any later definitive read or successful mutation may install usable knowledge
again. A clean mutation precondition failure only proves the expected
observation obsolete; because it does not identify the current state, knowledge
remains uncertain until another operation does.

Every conditional mutation must remain semantically safe if it executes
arbitrarily later when its original predicate is true again. CAS and conditional
delete deliberately use ADR-042's state-based revision semantics, including
revision ABA. Create-if-absent is restricted to either a permanent path whose
initial state is idempotent and is never deleted, or a fresh identity path whose
mere existence cannot publish newer user-visible state. An identity object
becomes live only through a separately revision-fenced reference or through
idempotent recovery of that identity.

Transaction objects use fresh transaction IDs and
[ADR-022](022-garbage-collection-mark-sweep.md)'s reference-checked GC;
structural nodes and records use fresh random tokens and
[ADR-032](032-node-locking-and-coordinated-splits.md)'s version-fenced
reachability. A late object therefore belongs to its original lifecycle rather
than replacing a newer one. In particular, a structural node created after its
write-ahead record was resolved cannot become reachable without the
already-fenced tree update. It may remain as an unreachable object, because no
node-directory orphan sweep is required by this decision. The resulting
object-store space leak is accepted for now; it is not permission for late
creation to affect the live tree or transaction state.

ADR-036's `read_stale` remains an approximate cache policy only. It may use a
monotonic elapsed-time sample to derive an explicitly approximate
`SequencePoint` cutoff, but duration-to-sequence conversion is confined to that
API. Causal validation, mutation receipts, and recovery never perform time
arithmetic. Snapshot reads are expected to supersede `read_stale` for real
bounded-staleness guarantees.

### Cancellation and lifecycle

Waiting for a path lane is cancellable. Cancellation before backend invocation
ends the operation without changing cache knowledge and releases the lane if it
was acquired. Cancellation after a mutation is invoked abandons the operation:
while still holding the lane, GlassDB removes usable cache knowledge, then drops
the backend future and releases the lane. It does not move the future to a
background worker and never publishes a result from that call.

Dropping the future cannot prove that the provider cancelled the request. The
mutation may still apply remotely, so abandonment is treated exactly like
`InDoubt` and like an in-flight mutation from a crashed or different `Database`
instance. It establishes no local completion edge, and later local operations
may overlap it remotely. Their conditional operations and freshness
requirements, rather than the path lane, reconcile any effect that becomes
observable. Reads remain cancellable without invalidation because they cannot
change backend state.

Any panic, task failure, or lost result after mutation invocation and before
definitive publication follows the same abandonment transition. Encoding or
validation failures before invocation do not invalidate cache state.

The coordinator admits operations, cancels queued work before dispatch, and
serializes live calls. It owns no continuation task or drain registry for an
abandoned mutation.

Graceful shutdown rejects new public asynchronous operations and drains
still-live admitted operations and protocol/background producers. It does not
wait for abandoned mutations and cannot promise that none will apply after
shutdown returns; this is the same boundary as a crashed database instance. A
caller may independently bound shutdown with a timeout.

Database metadata initialization precedes creation of the database-local
timeline and does not allocate sequence points. Metadata is read only during
startup, publishes no cache knowledge, and uses conditional primitives to make
concurrent initialization safe. Cancelling `Database::open` abandons an invoked
create-if-absent and is treated as crashing an opener; metadata creation is
idempotent under that exception.

### Complete backend boundary

Production engine point-object operations use the coordinated boundary. Cached
point objects use invocation points, path lanes, and cache reconciliation.
Database metadata initialization is the startup exception described above and
uses the raw conditional backend primitives. Backend tests and standalone
backend benchmarks may also call a raw backend directly.

Listing is not a point operation and takes no path lane. Each page has its own
conceptual operation interval and invocation point. S3 documents a continuation
token only as an opaque way to
[continue a later request](https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html),
not as a snapshot token. A paginated traversal therefore makes no cross-page
consistency claim. Protocols needing a consistent traversal retain their
membership/version validation or recovery logic.

### Verification

Deterministic state-machine and gated tests cover disjoint and overlapping
operations, the false-absence race, path parallelism, cache hits during
mutation, both cancellation boundaries, definitive and in-doubt outcomes,
present, absent, and uncertain knowledge, conditional deletion, retained
observations, shutdown with abandoned mutations, and absence/revision ABA.
One backend test separates request dispatch from remote application, abandons
the local mutation, completes shutdown, then applies the request and verifies
that subsequent read/CAS or recovery safely reconciles it.
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

Stamping a result with completion as `current_after` is stronger than either
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
needed. An abandoned mutation can remain active remotely, but has left the
instance's causal domain and is indistinguishable from an external writer. The
lane preserves concurrency across paths, where GlassDB obtains most of its
parallelism, while existing read coalescing and shard mutation coordination
already reduce useful same-path overlap. Multiple concurrent backend reads
remain a measured, internal follow-up rather than making the initial correctness
state machine substantially larger.

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

### Store or expose completion sequence points

The lane and coordinator lifecycle establish that a definitive operation
completed before the next same-path invocation. A numeric completion point has
no consumer in the serialized design, and an in-doubt completion cannot create
a backend edge. Callers need only the invocation evidence established by an
exact state. Completion points would become necessary if serialization were
removed and overlapping intervals had to be compared explicitly.

### Continue cancelled mutations in the background

Transferring an invoked mutation to a database-owned worker would preserve its
place in the local path order and often recover a definitive result. That edge
is not observable by the caller that abandoned the operation, however, and
GlassDB must already tolerate the same mutation arriving from another database
instance or a crashed client. Continuing it would require a task registry,
ownership handoff, panic containment, and shutdown draining, while a slow call
would retain the path lane after its caller no longer needs it. Invalidating
knowledge and applying the existing external-writer model gives the required
correctness with a smaller lifecycle.

## Consequences

- Local cache publication follows the same real-time order guaranteed by the
  backend instead of inventing an order for overlapping operations.
- The false-absence class is removed among definitively completed operations in
  one database instance, without weakening `Any` cache reuse or serializing
  operations on different objects. An abandoned mutation can still make a
  cached result stale as an external writer because it has no successful local
  completion and creates no causal promise.
- Same-path backend latency can cause head-of-line blocking. Existing read
  coalescing and higher-level shard mutation coordination reduce the common
  cost; multiple concurrent backend reads remain available as a measured
  optimization.
- A dispatched mutation may outlive its caller remotely. The database abandons
  it, invalidates local knowledge, and neither owns nor drains it.
- Graceful shutdown does not guarantee that an abandoned provider request will
  never apply afterward.
- A late create at a fresh identity can leave an unreachable object after its
  recovery record has been removed. This is an accepted space leak until a
  node-directory orphan sweep is justified and implemented.
- Separate `Database` openings have independent causality domains. A cache hit
  in one instance does not inherit completion in another.
- `SequencePoint` remains exact for causality while `read_stale` is explicitly
  approximate and isolated from correctness protocols.
- Lists remain strongly observed per request but are not multi-page snapshots.
