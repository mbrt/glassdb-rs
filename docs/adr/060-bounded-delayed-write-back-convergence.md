# ADR-060: Bounded delayed write-back convergence

## Status

Accepted

This refines [ADR-020](020-commit-write-back-protocol.md)'s asynchronous,
idempotent write-back and [ADR-028](028-shard-mutation-coordinator.md)'s retry
policy. It does not change the commit point, the authority of committed
transaction objects, or the coordinator's mutation semantics.

## Context

Committed write-back publishes a transaction as the current writer and releases
its holders. When independent database instances mutate one leaf, a write-back
CAS that definitively loses immediately rejoins the coordinator's retry loop.
The retry lands eventually, but it does so in the same contention burst that
caused the loss. Coordinator backoff then accounts for much of the workload's
latency.

Stopping after that loss moves work off the hot path, but does not provide
physical convergence. The holder remains authoritative, so reads stay correct
by resolving the committed transaction object. Fresh clients and cache
evictions must nevertheless reload that object, and a full-leaf scan pays once
for every distinct unresolved transaction. GC cannot remove an object while a
holder still references it. Permanent deferral therefore turns a transient
contention optimization into unbounded read amplification and retention.

A focused experiment instead delayed one retry until contention became quiet.
It improved median aggregate throughput by `7.9%`, coalesced `47` transaction
intents into `35` leaf batches, and left fresh scans with no transaction-object
reads after convergence. Definitive and ambiguous backend failures retained
their existing reconciliation behavior, and graceful shutdown forced pending
work without waiting through the delay. See the
[performance investigation](../../hack/perf/investigations.md#delayed-retry-validation).

The delay must remain only a scheduling optimization. Durable holders and
transaction objects continue to carry correctness across cancellation, process
failure, and independently configured clients.

## Decision

### Defer only a definitive leaf-CAS loss

A committed leaf write-back may leave its current coordinator retry episode
only after all of the following are known:

- its conditional mutation definitively did not land;
- a fresh observation shows that its write-back is still required on the same
  current leaf; and
- a local delayed-retry scheduler accepts ownership of that work.

This is the clean precondition-loss case. An unavailable or in-doubt mutation,
a structural move, a non-leaf mutation, or any other ambiguous outcome follows
the ordinary reconciliation path without delay. A write-back whose effect is
already present is complete.

Each write-back may transfer ownership at most once. When delayed work runs, it
re-enters the ordinary convergent write-back protocol and cannot be deferred
again. A second clean loss therefore retries normally rather than alternating
between contention and delay indefinitely.

The queued leaf identity is only a coalescing hint. Before mutating, delayed
work routes through the current topology and revalidates its targets. A split or
other structural change is handled immediately by the ordinary protocol; stale
queue identity cannot authorize a mutation of an obsolete leaf.

### Use a bounded database-local quiet period

Each `Database` has an internal scheduler owned by the transaction algorithm.
It groups delayed intents by leaf hint and coalesces duplicate transaction
identities. The cache and shard coordinator do not know about timers, queue
capacity, or shutdown policy; drained work reaches the coordinator as ordinary
write-back.

A leaf group becomes eligible at:

`min(last activity + quiet period, first enqueue + maximum age)`

New or duplicate activity resets the quiet deadline but not the maximum-age
deadline. An arrival after a group has begun draining starts a new group.
Maximum age prevents a continuously busy leaf from starving convergence, while
the quiet period lets a finite contention burst coalesce. Either duration being
zero disables delayed handoff.

The two durations are part of the public `ProtocolTiming` profile and use
[ADR-058](058-process-wide-model-time.md)'s model time. Their defaults remain a
tunable implementation choice; the measured interval is evidence for an
initial value, not a fixed architectural constant. Different database instances
may use different values safely because timing changes physical convergence,
not transaction semantics.

Capacity is bounded across the whole `Database` and counts distinct queued
intents; duplicates consume no additional capacity. Its limit remains a private
implementation policy. Reaching the limit requests an early drain of the oldest
group. If the new intent cannot immediately transfer ownership, its write-back
continues normally. Enqueueing never waits for capacity and work is never
dropped between owners. A separate per-leaf bound is not required: a hot leaf
may consume local optimization capacity, but overflow affects only whether
other work converges inline.

Groups have no transaction ordering requirement. Transactions spanning several
leaves may converge independently on each leaf, as ADR-020 already permits.
Draining groups are dispatched independently so one slow backend operation does
not prevent other groups' deadlines from firing; the existing coordinator owns
their batching and mutation order.

### Preserve durable authority and lifecycle boundaries

Accepting an intent transfers local ownership independently of cancellation of
the submitting future. Until delayed write-back lands, the committed transaction
object and its leaf holder remain the only durable authority. The scheduler is
not persisted, is not a GC root, and does not participate in read resolution.
Reads tolerate the temporary holder lookup and do not force an early drain.

Graceful shutdown closes the scheduler before closing background work
admission. Closing it makes racing submissions continue inline, forces every
accepted group without waiting for its quiet deadline, and waits for the
dispatched write-backs to converge. The maximum age bounds only scheduling
delay; it does not bound a subsequent backend operation or ordinary recovery.
Delayed work gains no separate error channel or retry semantics. As with
[ADR-043](043-causally-coordinated-backend-operations.md), a caller may
independently place a timeout around asynchronous shutdown.

Dropping or crashing a `Database` may discard its volatile schedule. Durable
holders keep reads correct and transaction objects live, and later ordinary
activity may converge them. No startup scan, persistent retry queue, or
cross-database coordination is introduced. A crash can consequently leave
physical cleanup and its read cost pending until such activity occurs; this is
the accepted crash boundary rather than a successful graceful shutdown.

### Keep observability diagnostic

Trace events distinguish drains caused by quiet, maximum age, capacity, and
shutdown. Queue counters are not added to public statistics, and the focused
fresh-scan instrumentation is not retained as a benchmark interface.

Deterministic validation must cover quiet-period reset and maximum-age forcing,
full and closed queue fallback, in-doubt mutation recovery, cancellation after
ownership transfer, shutdown races, topology movement while queued, and reads
from a fresh database while convergence is pending. Performance validation must
include post-shutdown backend work so the optimization cannot hide cleanup debt.

## Consequences

- Losing committed write-backs leave the contention burst and may coalesce into
  fewer later coordinator rounds without weakening logical visibility.
- Eligibility for another convergence attempt remains bounded even on a
  continuously busy leaf. A delayed attempt cannot defer itself again.
- Reads during the delay may load a transaction object, and transaction objects
  remain live longer. The additional delay introduced by this policy is bounded
  during normal operation and is accepted in exchange for lower write
  contention.
- Graceful shutdown may take longer because it waits for real convergence, but
  it skips all remaining debounce delay. A backend operation can still make
  shutdown wait indefinitely unless its caller applies a timeout.
- A database-wide bound prevents unbounded memory use without adding per-leaf
  fairness machinery. Under pressure, work falls back to immediate convergence.
- Independently configured database instances remain correct but may make
  different performance choices and contend with each other's delayed batches.
- The transaction algorithm gains an internal lifecycle component. The cache,
  durable formats, GC reachability rules, and shard coordinator interfaces do
  not gain scheduling policy.
- `ProtocolTiming` gains performance-only timing controls. Unlike lease timing,
  inconsistent values cannot cause incorrect reclamation or visibility.

## Alternatives considered

### Retry immediately after every clean loss

This is the existing convergent behavior, but it keeps cleanup in the same CAS
storm that caused the failure. Measurements identify that retry episode as a
material throughput bottleneck.

### Stop write-back permanently after a clean loss

Logical reads remain correct through the committed holder, but fresh scans pay
transaction-object reads indefinitely and GC must retain the referenced log.
Warm caches conceal rather than resolve that debt.

### Use an unbounded quiet-period debounce

This coalesces a burst with minimal work, but a continuously active leaf may
never become quiet. Adding maximum age preserves coalescing without making
convergence depend on workload quiescence.

### Delay from first enqueue without a quiet period

A fixed deadline bounds convergence more simply, but does not move a retry past
activity arriving near that deadline. A resettable quiet deadline better groups
one contention episode; maximum age supplies the missing bound.

### Flush delayed work from reads

Read-triggered publication could reduce cold lookup cost, but adds writes and
latency to a read path whose correctness already follows from durable holders.
It also duplicates convergence policy across the reader and scheduler.

### Sleep inside the resolver or coordinator

Holding a resolver episode open through the delay ties scheduling to a shared
mutation lane and lets one slow leaf obstruct unrelated work. A separate
transaction-level scheduler releases that episode and later submits an ordinary
write-back.

### Coordinate queues across database instances

Cross-instance batching could coalesce more work, but requires a new distributed
coordination protocol for an optimization whose durable state is already safe.
Database-local scheduling preserves independent clients and failure domains.

### Persist the queue or reconstruct it at startup

The holder and committed transaction object already encode everything required
for correct reads and eventual activity-driven convergence. A second durable
work log or a startup tree scan would add write and recovery cost to optimize an
exceptional contention path.

### Let GC publish unresolved holders

GC-driven publication broadens reclamation into a shard-mutation protocol and
still requires contention and shutdown policy. The bounded retry solves the
measured problem without coupling ordinary GC to value publication.
