# Always-on dependency-consistent snapshot reads

## Status

**Proposed design — not an ADR or an implementation commitment.** This is a
self-contained exploration of an always-on snapshot format whose normal
regular-transaction path remains unchanged. It deliberately has no constituent
ADRs. If implementation is scheduled, significant decisions should be extracted
only as they become actionable and ready for acceptance.

The earlier timestamp-versioned design and its seven unaccepted ADRs were
discarded after review. They remain available in the
[timestamp-history archive](../archive/snapshot-reads-timestamp-history/README.md).

## Goal and scope

`Database::read_tx` should give one read-only execution a fixed logical database
state across point reads, key scans, collection enumeration, and
cross-collection access. The execution may last minutes, takes no data locks,
and does not validate against concurrent regular writes.

Snapshot support is part of every database created in this unreleased format.
It is not a capability that can be disabled to recover regular-transaction
performance. Consequently the design must preserve the accepted regular commit
paths in normal operation:

- an ADR-051-eligible overwrite still commits with one authoritative leaf CAS;
- logged transactions still use ADR-020 locking and commit, ADR-053 fallback,
  and ADR-054 `External` publication;
- no normal commit waits for a snapshot payload, history record, global epoch,
  clock observation, or checkpoint-head mutation; and
- snapshot work may consume background operations, transferred bytes, memory,
  and retained storage, subject to explicit budgets.

The design supports the existing forward keys-only scan and collection APIs. It
does not add writable snapshots, arbitrary historical time travel, portable
bound-snapshot handles, reverse scans, or online migration from an older storage
format. Every new database begins with snapshot capability and an empty genesis
checkpoint.

S3 or Cloud Storage object versioning is not required. Backend wall clocks,
client clock synchronization, and an assumed fleet-skew bound are absent from
the correctness argument.

## Requirements and priorities

When requirements compete, preserve them in this order:

1. regular foreground writes;
2. already-bound snapshots;
3. checkpoint freshness and logical progress.

The resulting requirements are:

- Snapshot capability is always on, but the steady-state regular commit path
  gains no backend operation or storage wave.
- Checkpoints may discretize history. A successful commit need not have its own
  queryable historical version, and overwritten intermediate values may be
  coalesced.
- Publication and logical progress have a configurable target `B`, expected to
  be measured in seconds. `B` is best effort rather than a correctness
  deadline.
- Work stays in the background until `B - safety_gap`. After that point,
  foreground writes may perform bounded assistance.
- A tokenless read prefers an older certified checkpoint to unavailability.
- A causal read never binds before its requested dependency.
- A normally admitted snapshot has a database-configured maximum lifetime
  `L_max`, expected to be measured in minutes.
- Once bound normally, a snapshot is not revoked before its deadline. An
  explicitly tolerated over-age fallback has weaker physical-availability
  guarantees, described below.
- A cache-complete bind and execution perform zero backend operations, with no
  periodic control refresh after binding.

## Terminology

| Term | Meaning |
|---|---|
| **Session** | One open `Database` incarnation. `Database` clones share the same session and `DbInner`; a separately opened database is a different session. |
| **Timeline event** | A locally ordered operation interval with invocation and definitive-completion points, representing a committed regular transaction, an imported checkpoint, or a fence join. |
| **Session delta** | An immutable, background-published contiguous range of Timeline events with enough effects and dependencies for checkpoint compilation. |
| **Dependency** | A data, conflict, collection, session-order, or imported-fence edge that must be represented before its dependent event. |
| **Checkpoint cut** | A dependency-closed set of events represented by one materialized immutable database root. |
| **Checkpoint publication** | A durable checkpoint record that names a root, its parent, and its covered frontiers. A publication may reuse its parent's root. |
| **Certified checkpoint** | A published checkpoint whose immutable objects and dependency closure have been verified. Only certified checkpoints are bindable. |
| **Fence** | A serialized lower bound naming one database, one session incarnation, and a Timeline frontier. |
| **Reconciliation** | A best-effort rebuild of a later complete baseline when asynchronous event evidence may have been lost. |

## Consistency contract

### Regular transactions remain strict serializable

This proposal does not weaken current transactions. Their validation, locks,
commit points, write-back, conflict handling, and real-time contract remain the
ones already implemented. Snapshot metadata is not part of their normal durable
commit condition.

### A snapshot is a dependency-closed transactional projection

Treat each successful transaction and each imported causal observation as an
event. An event records or derives edges to:

- every value version it read;
- the predecessor version it replaced, including for a blind overwrite;
- collection-directory, membership, and scan evidence it observed;
- every local event that completed before the operation began; and
- any checkpoint or fence explicitly imported into the session.

A checkpoint may represent an event only when it also represents every event
reachable through those edges, or when a parent checkpoint has already
compacted that dependency. All writes, deletes, and collection changes from one
transaction enter a checkpoint atomically.

This is weaker than a prefix of the global strict-serializable real-time order.
Two transactions on separate sessions that neither conflict nor observe one
another have no dependency edge. A checkpoint may include either one without
the other even when one completed first in wall-clock time. It may not include
a transaction while omitting a value, predicate, session event, or imported
fence on which that transaction depends.

No clock participates in this rule. Transaction identifiers may continue to
carry wound-wait priority timestamps, but those timestamps do not order
snapshots.

### Checkpoint lineage is monotonic

Every published checkpoint descends from the previous certified checkpoint.
Its logical event set is equal to or a superset of its parent's set. Per-session
frontiers only advance; they never move backwards.

This gives two useful properties:

- once a fence is covered, every later checkpoint covers it; and
- tokenless snapshots do not move backwards in logical history, although a
  publication may temporarily reuse exactly the same state.

Reconciliation must therefore create a logically later baseline. It may not
discard an inconvenient dependency or roll the checkpoint lineage back.

### Discretization may remove intermediate values

Representing an event does not require retaining its exact post-commit state.
For example:

```text
C0: x = 0
T:  x = 1
U:  x = 2, after T
C1: x = 2
```

`C1` may cover both `T` and `U` without storing a queryable `x = 1` version. A
fence after `T` may bind `C1`: it means "not before T", not "return precisely
T's value".

Coalescing remains transaction-aware. If `T` wrote both `x` and `y` and only
`x` was overwritten, a later checkpoint must still represent `T`'s effect on
`y`. A compiler may discard an event payload only after proving that the
materialized root represents or supersedes its complete atomic effect.

### Scans and collections use the same cut

The checkpoint root contains logical key membership and the incarnation-based
collection catalog as well as values. Point reads, range pages, collection
existence, and subcollection enumeration therefore resolve against one root.
Mutable live-tree splits are not snapshot events and cannot introduce phantoms
into an immutable checkpoint scan.

## API contract

The names below are provisional; the semantics are the design decision.

### Unconstrained and same-session reads

`Database::read_tx` binds the newest locally eligible certified checkpoint. It
does not automatically require the checkpoint to include regular transactions
previously completed through the same `Database`. This explicitly permits a
non-monotonic session read in exchange for the ability to serve an older
checkpoint during compiler or reconciliation delay.

An opt-in same-session form, written here as `read_tx_after_current`, captures
the shared Database Timeline and requires a checkpoint at or beyond that local
frontier. It does not expose a token to the caller.

Binding completes before the `FnOnce` closure is invoked. A causal bind wakes
the background exporter and compiler and waits for up to `B`. If no satisfying
checkpoint becomes available, it returns a retryable
`CausalCheckpointNotReady` error without invoking the closure. It never silently
falls back to an earlier cut.

Every `read_tx` that returns successfully appends an import of its bound
checkpoint to the local Timeline. This is a join for future operations; it does
not retroactively claim that an unconstrained read observed earlier local
writes. Regular transactions started after the import therefore inherit both
the prior local context and the checkpoint observation without carrying a
token. Transactions already in flight remain concurrent.

### Serializable database-frontier fences

A caller may capture the current Database frontier after a relevant regular
transaction and serialize it for another process. Capture is local and performs
no backend operation. Because it names the Database frontier rather than one
transaction, it may conservatively include unrelated concurrent completions.

A fence contains at least:

- a format version;
- the permanent database identity;
- the origin session incarnation; and
- its monotonic Timeline frontier.

`read_tx_after` accepts a matching fence and waits for a checkpoint that covers
it. Successful consumption imports the checkpoint into the receiving Database
Timeline, so later local operations inherit the dependency automatically.

Fence capture is not a durability barrier. A fence may become unsatisfiable if
its origin dies before exporting the named event range. A clean shutdown tries
to flush it; an already-bound snapshot remains valid regardless of later origin
failure.

### Fence fan-in

A fixed-size merge is anchored in a receiving Database:

```text
db.merge_fences([a, b, ...]) -> Fence
```

The operation appends one local join event whose dependencies are the input
fences and immediately returns a fence for that event. It performs no backend
operation. Repeated joins form a background event DAG rather than embedding an
ever-growing frontier in the serialized token. Merging imports the dependencies
into that Database, so later operations inherit them just as they do after a
successful causal read.

The implementation must bound the number and encoded bytes of inputs to one
join. Larger fan-in can form a tree of bounded joins.

### Fence trust boundary

Fences are versioned and structurally decoded, but they are not authenticated
and their claimed frontiers are not checked for existence before use. A forged
frontier cannot make an unsafe checkpoint valid: binding still requires actual
durable coverage. It can, however, force waits and work, consume retained state,
or poison the receiving session's snapshot causal cone after a merge or import.

Applications must not accept fences from untrusted sources without their own
authentication and resource controls. This is a documented denial-of-service
boundary, not a data-consistency boundary.

### Read lifetime

The database configures `L_max`. A call may request a shorter lifetime but not a
longer one. The deadline starts immediately before invoking the closure.

At the deadline, `read_tx` cancels the execution and returns `SnapshotExpired`.
It never rebinds or reruns the closure at a newer checkpoint. This is important
for a `FnOnce` body and for application code that may perform work outside the
database while reading.

## Capturing transaction events without changing commit

### Local event capture

Immediately before an operation begins, the engine allocates an invocation
point and captures the local events that had completed before it. After a
definitive successful commit, and before making the transaction future ready,
it allocates a completion point and enqueues an in-memory description of the
operation interval, transaction effects, and dependencies. A read-only regular
transaction may advance only dependency state.

The session-order edge is completion-before-invocation, matching ADR-043's
local causal rule. Overlapping operations have no session edge merely because
one happens to return first. A serialized session frontier may conservatively
cover all definitive completions through one point, but that coverage does not
replace the data and conflict edges that determine how concurrent transaction
effects are materialized.

This enqueue may copy compact metadata and retain references to values already
owned by the attempt. It performs no backend operation. In particular:

- an ADR-051 direct commit remains one leaf CAS and creates no transaction
  object;
- an ineligible direct attempt follows ADR-053 into replay or the regular
  ADR-020 locked protocol;
- logged commit remains the existing transaction-object CAS; and
- logged write-back continues to publish `External` under ADR-054 rather than
  copying snapshot bytes inline.

The snapshot event is not part of the regular transaction's durability proof.
If it is lost, the regular commit remains valid and visible to latest-state
transactions; only later snapshot progress may be delayed.

### Bounded buffering and coalescing

Each session has a bounded in-memory delta buffer. The exporter first coalesces
overwritten effects while retaining transaction boundaries, unresolved
dependencies, and the final logical state needed by the next checkpoint.

If the buffer still fills before escalation, a regular writer does not block.
The session discards unresolved snapshot progress, marks itself as requiring
reconciliation, and continues serving regular traffic. The current certified
checkpoint remains available. A crash before that marker is exported is still
detected as an orphan of the open session.

### Durable session deltas

A background exporter packs a contiguous range of local events into immutable
session-delta objects and advances a small per-session manifest only after the
range is durable. It never publishes a later local frontier with an unexplained
gap. Values may be copied into the delta, packed into immutable objects, or
referenced through another durable object, but a delta must remain independently
resolvable until a checkpoint has compacted it.

The exact packing and the compact representation of scan and catalog
dependencies require prototyping. They do not change the foreground rule: no
session-delta write is a normal commit prerequisite.

## Materialized checkpoint storage

### Genesis and immutable roots

Database creation publishes an empty genesis checkpoint before the database is
opened for user operations. Every later checkpoint names its certified parent
and one immutable logical root.

The root belongs to a persistent, structurally shared tree optimized for
snapshot point reads and ordered scans. It is separate from the mutable live
coordination tree. Applying a session delta rewrites only changed leaves and
their paths; unchanged nodes remain shared with the parent. Repeated writes to
one key before a publication normally produce one changed snapshot leaf rather
than one stored version per commit.

Objects are immutable by GlassDB path and format. Provider-native object
versioning is neither enabled nor consulted.

### Cooperative compilation

Any open Database may compile durable session deltas. Work claims are advisory,
not correctness leases. Another instance may duplicate or steal work after
locally observing that a claim has stopped making progress. Immutable outputs
are idempotent, and a conditional mutation of the checkpoint head selects the
winning publication.

The compiler selects a dependency-closed set extending the current head,
applies complete transaction effects to a candidate immutable tree, verifies
the candidate, then publishes it. Losing a head race discards or reuses the
candidate objects; it never overwrites the winner.

No instance is the exclusive compiler. Redundant background reads and writes
are an accepted cost of avoiding a leader whose failure stops the database.

### Dependency-local progress

The checkpoint manifest records or compactly summarizes per-session coverage.
A session whose next event or dependency is unavailable remains at its previous
frontier. Unrelated sessions continue advancing. Events that depend on the
stalled suffix remain excluded with it.

This means different keys in one checkpoint can have very different real-time
ages. The state remains one dependency-closed transactionally consistent cut.
Per-session frontiers are internal; callers see one checkpoint identity.

Completed and fully compacted session incarnations may be removed from the
active frontier map. Fences are therefore not promised to remain independently
resolvable after their origin is gone unless a checkpoint has already covered
them.

## Publication and progress target

### Two `B` targets

`B` governs two best-effort clocks:

1. **Availability target** — publish a bindable checkpoint record within `B`.
   The publication may point to the same immutable root and frontier as its
   parent after a successful no-op compiler pass.
2. **Progress target** — incorporate or safely supersede eligible discoverable
   work within `B` of becoming available to the compiler.

Work is eligible when its contiguous session range and dependency closure are
durably discoverable. An unreported crash tail is not yet eligible. An event
blocked on a missing dependency is reported as causal-cone lag rather than
allowing one session to stop unrelated progress.

A quiescent database has no logical-progress debt, but it may still republish
the current root to maintain availability evidence. Publication age says when a
compiler last selected a safe cut; it does not bound the real-time age of every
value in that cut.

### Safety-gap escalation

Before `B - safety_gap`, all checkpoint work is background work and is throttled
in favor of regular traffic. At the safety gap, the system may establish a
finite compilation or reconciliation cut and make foreground transactions help
preserve it.

For a live-state cut, a writer that would overwrite an uncopied leaf first
writes one immutable pre-cut backup for that leaf. Backups for all leaves
touched by one transaction run in parallel, adding at most one latency wave and
one object write per touched, not-yet-preserved leaf. A durable per-cut marker
prevents later writers from copying the same leaf again. Once the cut is
established, post-cut writes do not enlarge its remaining work.

Writer assistance continues at that bounded cost until the cut completes. The
system does not introduce a later global write barrier merely because `B` was
missed. Background workers continue when no foreground writers are available.

Routine event-derived checkpoints do not require all sessions to acknowledge a
global epoch. Cooperative epoch acknowledgement is reserved for a full
live-state reconciliation. A nonresponsive instance can delay that rebase, but
not publication from the previous root or unrelated event-derived progress.

### Conditions that may miss `B`

`B` is not a correctness timeout. Expected reasons for a miss include backend
unavailability, snapshot-storage pressure, an orphan under reconciliation, an
unsatisfied causal dependency, a stalled session exporter, insufficient
background capacity, or a Database process that stops running. Such misses are
visible in metrics. They never permit a partial or dependency-open checkpoint.

## Session lifecycle and reconciliation

### Open-session records and keep-alives

Before accepting regular transactions, a Database incarnation creates a durable
open-session record. Its background exporter updates that record as a
keep-alive and publishes its durable frontier. Export progress itself may serve
as a keep-alive.

Keep-alives are a failure-detection hint, not a lease that makes writes valid.
No backend timestamp decides expiry. A compiler suspects an orphan only after
it has observed the same keep-alive generation unchanged for a grace interval
measured on its own monotonic clock. A compiler restart restarts that interval.
Detection is therefore best effort and may be delayed; false suspicion may do
extra work but must remain safe.

### Graceful shutdown

The existing `Database::shutdown()` remains the only graceful lifecycle API. It
first rejects and drains public operations. Snapshot shutdown then flushes the
contiguous local event range and publishes a terminal session marker before the
engine closes storage.

The terminal marker, rather than immediate deletion, proves that no later event
can appear from that incarnation. If the flush or marker cannot complete under
the existing shutdown retry policy, the marker remains open and later
compilers treat it as a possible orphan. Dropping the last Database without
`shutdown()` already aborts background work and naturally follows the same
orphan path.

### Orphan handling

A crash can occur after a regular commit becomes authoritative but before its
event is exported. Because a direct commit leaves no transaction object and no
durable changed-leaf index, an orphan record cannot identify every possibly
lost key. Reconciliation must be able to rebuild a complete current-state
baseline rather than pretending the asynchronous stream was complete.

While reconciliation runs:

- tokenless reads continue binding the last certified checkpoint;
- the compiler may republish that root after a current pass;
- unrelated durable session deltas may continue advancing;
- the orphan's unresolved causal cone remains behind; and
- already-bound snapshots are untouched.

Reconciliation state is intentionally not returned by `read_tx`. Operators see
it through metrics. A causal read whose fence lies in the unresolved cone waits
and may return the retryable not-ready error.

### Full-state reconciliation

A full rebase announces a reconciliation epoch through the open-session
records. Participating instances install the epoch in their local commit paths
and acknowledge it. New instances observe an active epoch before accepting
writes. Background copying and the bounded first-overwrite rule then produce a
stable logical current-state root without relying on storage-provider versions.

The rebase may complete only when its materialized state is transactionally
consistent and logically at or after the previous checkpoint. If an instance
does not acknowledge, the rebase remains incomplete; older checkpoints and
unrelated delta compilation continue. Best-effort orphan detection may later
remove that uncertainty.

## Binding, caching, and retention

### Local publication evidence

Successful checkpoint observation or publication is stamped onto the open
Database Timeline and cached with the checkpoint manifest. A warm Database may
reuse that evidence locally. A newly opened or long-idle Database performs the
necessary background control pass before it can claim recent publication
evidence.

Publication-age policy is a target, not a hard admission condition. Under
normal operation, a checkpoint observed within the target age receives the
full lifetime guarantee below. During reconciliation, the compiler may publish
a new record naming an older root. During a control-path outage, a warm client
may bind its older cached root even after the age target.

### Zero-I/O cache-complete execution

Binding captures one checkpoint identity, immutable root, local publication
evidence, and deadline. It performs no backend operation when all of those are
already cached. After binding:

- the cut never changes;
- no server time, head, floor, keep-alive, or retention record is refreshed
  merely because time passes;
- immutable cached nodes and values remain valid for that cut; and
- a fully cached point-read or scan execution can finish with zero backend
  operations even when it lasts longer than a control-publication interval.

A cache miss loads only immutable objects belonging to the bound root. It never
falls through to the mutable latest-state tree.

### Guaranteed retention window

There are no per-reader backend pins. A root that may be bound from normal
cached publication evidence stays reachable for at least:

```text
publication-age allowance + L_max + conservative GC guard
```

after it stops being current. The current checkpoint head is never reclaimed.
Every immutable node reachable from any retained root remains live, including
nodes structurally shared with newer roots.

GC uses publication/retirement state and locally measured grace intervals, not
comparable client or provider clocks. On uncertain or restarted GC state it
retains data longer. The exact mark/sweep or generational compaction mechanism
is an implementation decision, but it may never reclaim reachable data early.

### Over-age fallback

A warm isolated client cannot know whether another client has superseded its
cached head. Allowing such a client to bind indefinitely while promising a new
full `L_max` would require retaining every historical root forever.

Therefore an over-age bind is internally consistent but its remaining physical
availability is best effort. Cached objects may continue serving it. If a
required immutable object has been reclaimed, the read returns a retryable
`SnapshotUnavailable`; a missing snapshot object is never interpreted as an
absent user key. The execution still never mixes roots.

### Snapshot storage pressure

Snapshot storage has a configured budget and must yield before threatening
regular writes. When the budget is reached, the compiler:

- stops producing logically newer roots;
- discards unreferenced candidate objects and compacts what it safely can;
- keeps the last certified root live; and
- may continue publishing records that name that root.

Tokenless reads remain available on the older state. Causal reads beyond it may
time out. Logical progress may remain suspended indefinitely until GC or
compaction frees space. This deliberately sacrifices freshness before writes or
already-guaranteed snapshot lifetimes.

## Correctness argument

The implementation must turn the following claims into executable invariants.

### Transaction atomicity

A compiler applies one transaction's complete logical write and collection
change set or none of it. Delta packing and overwrite coalescing cannot split a
multi-key transaction across checkpoints.

### Dependency closure

Every applied event's data, conflict, session, catalog, and imported-fence
dependencies are either in the same candidate cut or summarized by its parent.
Unknown dependencies stall that causal cone. They are never guessed from wall
time.

### Monotonic publication

A checkpoint head CAS accepts only a certified child of the head version the
compiler read. A racing candidate is retried against the winner. No successful
publication removes covered history.

### Crash omission is safe

An asynchronously lost transaction event may cause snapshots to omit that
transaction. It cannot produce a torn checkpoint:

- later events from the same session cannot be published past a gap;
- a transaction from another session that observed the lost writer carries an
  unresolved dependency and is excluded; and
- a transaction may certify subsumption only with enough evidence to cover the
  lost transaction's full effect; otherwise reconciliation is required.

This is why occasional missing snapshot progress is acceptable while partial
transaction visibility is not.

### Reconciliation is forward-only

A reconciled current-state root is published only after cooperative copying and
write preservation establish one transactionally consistent state at or after
the previous checkpoint. It becomes a child of that checkpoint, not a second
history branch.

### Immutable reads need no refresh

Once a normally retained immutable root is bound, later regular writes cannot
alter its objects. Its retention window was established before binding, so
periodic backend time or floor checks add no correctness evidence. Over-age
fallback weakens only physical availability, never the fixed-cut semantics.

## Cost model and acceptance gate

### Steady-state foreground shape

| Regular operation | Existing durable path | Snapshot addition before return |
|---|---|---|
| ADR-051 direct overwrite | One authoritative leaf CAS | Local Timeline event and bounded in-memory enqueue |
| Logged read-write transaction | Existing ADR-020 locks and commit CAS; ADR-054 `External` write-back | Local Timeline event and bounded in-memory enqueue |
| Regular read-only transaction | Existing optimistic validation/retry path | Local dependency-frontier advancement |

No session-delta object, checkpoint node, checkpoint-head mutation, keep-alive,
or reconciliation copy belongs to the normal transaction's storage-wave count.
Background work may still affect shared backend throughput and is measured
rather than hidden.

### Initial performance gates

Before any implementation decision is accepted, compare against the current
regular engine under identical backend latency, concurrency, values, and
contention:

- normal foreground backend operations and latency-wave shape are unchanged in
  every regular transaction cell;
- normal p95 and p99 foreground latency are at most `1.25x` baseline;
- saturated foreground throughput with snapshot background work active is at
  least `0.85x` baseline;
- the ADR-051 eligible overwrite still takes exactly one foreground backend
  operation;
- a cache-complete snapshot bind and execution take exactly zero backend
  operations; and
- escalation adds at most one parallel preservation wave and one object write
  per touched, not-yet-preserved leaf.

The matrix covers direct overwrites, logged one-key and multi-key writes,
cross-leaf and cross-collection transactions, scans, collection churn, hot
keys, sparse traffic, saturated traffic, and databases that never call
`read_tx`. Queue stability and reconciliation frequency are validity
conditions, not footnotes.

### Background budgets

The old per-transaction total-operation ratio is not meaningful for sparse
traffic: one commit plus one batched control publication already looks like a
`2x` ratio even though the commit stayed one CAS. Background work is instead
budgeted separately as:

- requests per active Database per time interval;
- bytes read and written per logical changed byte;
- concurrency and bandwidth while foreground work is queued;
- retained snapshot bytes relative to live logical bytes;
- event-buffer and unfinished-checkpoint memory; and
- duplicate work caused by cooperative compilation.

Exact limits must be declared before benchmarks run. Normal background work is
throttled to preserve the throughput gate. Escalation, orphan reconciliation,
storage-pressure stalls, and backend outages are reported in separate cells
because they intentionally have different costs.

## Configuration and observability

The semantic configuration is intentionally small:

| Policy | Contract |
|---|---|
| Checkpoint target `B` | Database-level best-effort publication and eligible-progress target, measured in seconds. |
| Publication-age target | Age at which a cached publication is considered normal rather than over-age fallback. It is not a data-recency guarantee. |
| Maximum read lifetime `L_max` | Database-level guaranteed lifetime ceiling, measured in minutes. Calls may request less. |
| Snapshot storage budget | Point at which freshness yields and the current root is retained. |

The safety gap, heartbeat cadence, orphan-observation grace, delta-buffer size,
checkpoint object sizing, and background concurrency may become tuning policy,
but their exact public configuration surface is deferred until measurements
show which controls operators actually need.

At minimum expose metrics for:

- last successful checkpoint publication and logical frontier advance;
- publication age and per-session or causal-cone lag;
- eligible events and bytes awaiting export or compilation;
- delta-buffer coalescing, drops, and reconciliation requests;
- open, terminal, suspected-orphan, and unacknowledged sessions;
- background requests, bytes, duplicate work, and throttling;
- writer-assistance transactions, leaves, operations, and latency;
- retained roots, reachable bytes, GC debt, and storage-pressure stalls;
- normal and over-age binds;
- causal-bind waits and timeouts; and
- snapshot expiry and missing-object failures.

Reconciliation and held-back progress are metrics, not a degraded flag returned
by tokenless `read_tx`.

## Rejected alternatives

### Timestamped per-version history

The archived design assigned HLC timestamps to commits and retained certified
per-key versions. It removed the one-CAS direct path, added synchronous history
work to logged commits, failed to prove real-time order for disjoint commits,
and relied on a backend fleet-skew bound that S3 and Cloud Storage do not
provide. It also required periodic control I/O during cache-complete reads.

Dependency checkpoints weaken only the historical real-time contract and keep
current regular transactions strict serializable.

### Provider object versioning

Native versions retain mutable live objects but do not supply the transaction
atomicity, dependency closure, session ordering, materialized catalog, or
portable performance contract required here. High version counts also place
provider-specific behavior on the hot path. GlassDB-owned immutable checkpoint
objects are explicit and backend-neutral.

### Synchronous event durability on every commit

Writing a history or event object before returning would close the crash gap,
but it adds at least one operation and often one latency wave to the direct
path. That violates the primary requirement. This design accepts missing
snapshot progress and reconciles it instead.

### Delta chains at read time

Publishing only a checkpoint base plus an unbounded sequence of deltas lowers
compiler write amplification but makes point reads and especially scans merge
multiple histories. It also complicates cache completeness and retention.
Materializing one structurally shared root spends background bytes to keep reads
predictable.

### A global acknowledgement barrier for every checkpoint

Waiting for every open Database would let one unhealthy instance stop all
publication. Routine compilation instead advances independent session
frontiers. Global acknowledgement is reserved for rare reconciliation and does
not make older snapshots unavailable.

### Making snapshots unavailable during reconciliation

Withholding all binds would make a recovery detail user-visible and invert the
priority between availability and freshness. Tokenless reads use the older
certified root; only an explicit unsatisfied fence must wait.

### Purely concatenated composite fences

A pure `Fence::merge` must embed the union of its inputs and grows with every
independent origin. A Database-anchored join stores fan-in in the event graph and
returns one fixed-size fence. The trade-off is intentional import into that
Database's session.

## Relationship to existing ADRs

This proposed design changes no accepted ADR today and supersedes none until an
implementation decision is accepted.

- [ADR-020](../adr/020-commit-write-back-protocol.md) remains the logged commit
  protocol.
- [ADR-051](../adr/051-inline-latest-values.md) remains the one-CAS inline direct
  path.
- [ADR-053](../adr/053-replay-definitive-logless-rmw-losses.md) remains the
  direct-attempt replay and locked-fallback rule.
- [ADR-054](../adr/054-reserve-inline-publication-for-logless-commits.md) keeps
  logged write-back `External`.
- [ADR-033](../adr/033-transactional-key-iteration.md)'s scan API shape is reused
  against a separate immutable checkpoint tree.
- [ADR-043](../adr/043-causally-coordinated-backend-operations.md) currently
  defines `Timeline` as local, non-persisted cache/backend coordination.
  Snapshot session events would be a new semantic layer using the same local
  ordering primitive; a future ADR must explicitly define and justify that
  extension rather than silently rewriting ADR-043.
- [ADR-045](../adr/045-optional-persistent-encoded-body-l2-cache.md) permits
  cache-local sequence evidence to cross an open through one owned L2. Snapshot
  fences instead cross sessions explicitly and cannot be smuggled through cache
  evidence.
- ADR-022 reclamation will eventually need an explicit extension for retained
  checkpoint roots and session-delta inputs. No such extension is accepted by
  this document.

## ADR and implementation staging

Leaving a list of proposed ADRs for an implementation that may happen much
later creates false architectural commitments and frozen numbering without
executable evidence. This design therefore remains the sole active document for
the exploration phase.

Before implementation begins, prototypes and benchmarks should resolve:

- the event and session-delta encoding, including compact scan and catalog
  dependencies;
- how logged transaction objects and direct inline values feed background
  deltas without racing current GC;
- the structurally shared checkpoint-tree and reachability-GC formats;
- cooperative compilation, candidate cleanup, and head-race recovery;
- reconciliation epoch acknowledgement and first-overwrite preservation;
- exact fence encoding, size limits, and API names;
- default values for `B`, `L_max`, the safety gap, and resource budgets; and
- the complete normal, escalation, crash, and storage-pressure benchmark
  matrix.

Only then should the project create the minimum ADR set needed to record
significant decisions being implemented. Likely boundaries are the public
consistency/API contract, persisted event/checkpoint format, and recovery and
retention protocol, but even those should remain sections here until the code
and measurements prove that they are the right boundaries.
