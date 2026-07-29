# Bounded-staleness snapshot reads

## Status

**Proposed.** This design adds long-lived, internally consistent, read-only
transactions over a fixed historical database cut. The umbrella API decision is
[ADR-037](../adr/037-bounded-staleness-snapshot-transactions.md); cut
definition, historical data, retention, and the collection catalog are split
into the focused ADRs indexed below.

A cut is a commit timestamp taken from the backend's own clock, so acquiring
one performs no coordination and no object is written by every commit or read
by every snapshot. [Cut definition](#cut-definition) records that decision and
the two coordinated alternatives it replaced.

Snapshot capability is part of the one database format in this proposal. There
is no creation-time or operational mode that lets read-write transactions avoid
the certification and history protocol. The commit critical path itself is
unchanged, so the mandatory cost is writing and retaining history rather than
commit latency. The [performance acceptance gate](#performance-acceptance-gate)
must be completed before these ADRs can be accepted.

This document is the living companion to those proposed decisions. In
particular, the numeric defaults and optional implementation optimizations may
evolve while the proposal is reviewed.

## Goal & scope

`Database::read_tx` is a read-only API. It gives the execution one global cut
and keeps that cut unchanged for the execution's lifetime. Reads at that cut
are:

- internally consistent across keys, ranges, collections, and subcollections;
- read-only, lock-free, and free of commit-time validation;
- allowed to be boundedly stale;
- valid for a bounded but analytics-friendly lifetime; and
- independent of later writers and of reclamation decisions.

The API supports point reads, ADR-033's forward keys-only range scans and
materialized pages, collection and subcollection enumeration, and
cross-collection reads. Callers obtain values for a scanned page through point
reads in the same transaction and therefore at the same cut. Read-write
transactions keep their existing strict-serializable semantics.

Explicit historical time travel, collection deletion, portable snapshot-bearing
continuation tokens, snapshot migration between clients, and online policy
reconfiguration are out of scope. The storage format is greenfield; existing
databases need not be upgraded or backfilled.

### Terms

| Term | Meaning |
|---|---|
| **Server-time observation** | A reading of the backend's own clock reported alongside a successful operation, comparable across all clients of one database (ADR-052). |
| **Commit timestamp** | The position a committed transaction occupies in the cut order, assigned from server-time observations once every lock is held. |
| **Cut** | The complete logical database state at one timestamp: a downward-closed prefix of the strict-serializable order, not a copied database. |
| **Cut grid / slot** | The fixed-period partition of the timestamp domain that every client computes identically; admissible cuts are grid points, and a slot is the interval between adjacent ones. |
| **Anchoring** | Whether a backend's reported time is known to be at or after the operation applied (apply-anchored) or only to fall inside the request (message-anchored). Declared per backend by ADR-052. |
| **Margin** | How far a cut trails the reader's server-time observation. It sums three allowances: skew within the backend's fleet, the granularity of its reported time, and, for a message-anchored backend, the request timeout. |
| **Commit-age bound** | The maximum age a transaction's commit timestamp may reach before it must abort or be durably aborted by a peer. |
| **History head / floor version** | A leaf entry's pointer into one key's retained history / the first certified version at or before the oldest still-readable cut. |

## User contract

### Execution

`Database::read_tx` binds one cut before invoking the closure and keeps it for
the execution's lifetime. The execution acquires no data locks, validates
nothing at the end, and never advances to another cut. Binding performs no
coordination and cannot fail for lack of a frontier, so the closure runs exactly
once and the API accepts `FnOnce`. The body must still be safe to cancel at the
deadline. The storage layer may retry idempotent reads against the same cut and
deadline without reinvoking the closure.

There is no fallback mode, no acquisition timeout, and no per-call
`require_snapshot` option, because there is no acquisition step that can fail.
A caller may request a cut fresher than the client currently holds evidence
for, which costs at most one small backend operation and fails only the way any
backend operation fails.

Snapshot capability is a property of the open database rather than of a call. A
backend that reports no server time cannot support cuts, as ADR-052 describes;
that is reported when the database is opened and by any `read_tx` on it.

Existing `Database::tx` remains strict and retryable even when its collected
write set is empty. The selected semantics come from the API, not from
inspecting the closure after it runs.

### Freshness and lifetime

Freshness measures how far the cut trails the present, and it is entirely a
function of how recently the client observed the backend's clock. A cut is
never invalidated by age: an older observation yields a staler cut, never an
inconsistent one.

Binding is three local steps and at most one backend operation:

1. take the client's most recent server-time observation, refreshing it with a
   small backend operation if the caller's staleness request needs a newer one;
2. subtract the policy margin, which covers skew within the backend's fleet and
   the granularity of its reported time, then snap down to the nearest cut
   point; and
3. start the fixed `started_at + lifetime` deadline and invoke the closure.

An active client already holds a fresh observation from its ordinary traffic,
so binding usually performs no backend operation at all, and an idle client
pays one small request. There is no fence, no certificate, no control-record
read, and no retry loop, so there is nothing for a begin timeout to bound.

The cut may never be extrapolated forward from the local clock. The local clock
decides *when* to refresh an observation; it never contributes to the cut
itself. Extrapolating would readmit local clock rate into the safety argument
that [Cut definition](#cut-definition) sets out.

A bind also validates the database's operational state against an observation
no older than the policy's control-staleness bound. ADR-040's drain wait is
extended by that bound, so ordering a bind against an operational disable needs
no strongly consistent read.

A snapshot execution's lifetime starts immediately before the closure is
invoked and ends at `started_at + lifetime`. The age of the cut affects nothing
but staleness: a cut taken at the edge of the staleness bound still receives the
full configured lifetime.

Local clocks retain one job: measuring elapsed time for the deadline and for
deciding when to refresh an observation. That clock must be monotonic and
advance through process and machine suspension. This is a BOOTTIME-class
contract; a generic monotonic API is insufficient unless its platform
implementation is qualified to include suspension. Wall-clock adjustment cannot
extend a deadline.

Because cut selection no longer depends on local clocks, a bad one costs
staleness or a premature expiry rather than consistency. The remaining
sensitivity is between a reader measuring its lifetime and GC measuring its
retention wait, which is a rate divergence over one bounded interval and is
covered by the policy's guard.

Drift is detected for free. Every backend response carries a server-time
observation, so each client continuously compares its own clock against the
backend's in both directions and marks itself unhealthy beyond the policy
allowance. An unhealthy client may still commit, because its commit timestamps
come from the backend rather than from itself; it stops binding cuts, and as a
GC worker it retains history and performs no time-authorized reclamation. A
fully cache-served execution issues no requests of its own, so it must refresh
a server-time observation at a bounded interval or expire.

The implementation races the whole closure future against the deadline, checks
before and after every storage await, and checks again when the closure returns.
Results completing after the deadline are discarded and return
`ReadTransactionExpired`; a range page fails atomically rather than returning a
partial page. Resuming pagination does not change or reset the deadline.

### Proposed policy defaults

`SnapshotPolicy` is immutable database metadata written at database creation.
Every client reads the persisted policy; a conflicting local configuration is
an open error rather than a new opinion about retention. Per-call options may
request a shorter lifetime or a stricter freshness bound, never a larger one.
Every database in this format has this policy and emits snapshot history; there
is no strict-only capability or creation-time opt-out.

| Setting | Proposed default | Purpose |
|---|---:|---|
| Cut grid period | 5 seconds | Spacing of admissible cuts; also the retention-coalescing and change-log unit |
| Fleet-skew allowance | 1 second | Skew between servers within the backend's fleet |
| Reported-granularity allowance | 1 second | Truncation in the backend's reported time; one second for an HTTP `Date` |
| Apply-anchoring allowance | backend request timeout | Bound on the gap between stamp and apply for a message-anchored backend; zero when apply-anchored |
| Cut margin | sum of the three allowances | The safety term subtracted from the observation |
| Maximum snapshot staleness | 30 seconds | Total distance a cut may trail the present |
| Maximum read lifetime | 1 hour | Supports cold object-store scans and analytics |
| Commit-age bound | 30 seconds | Age at which a still-pending transaction's timestamp forces abort |
| Control-staleness bound | 60 seconds | Oldest operational-state observation a bind may use; added to the drain wait |
| Reader-versus-GC elapsed-rate allowance | 5 seconds | Rate divergence over one retention interval |
| Minimum history retention | 65 minutes | Derived safety floor; see ADR-040 |

Maximum staleness decomposes into the age of the observation a bind uses, the
margin, and the grid period. Only the margin is a safety term, and it is a sum
of three separately sized allowances rather than one number, because they come
from unrelated sources and differ per backend.

Fleet skew is the smallest of the three: providers keep server clocks within
milliseconds, so a second is already three orders of magnitude of headroom.
Granularity is fixed by the format of the reported time. The apply-anchoring
allowance dominates on a message-anchored backend and is zero on an
apply-anchored one, so the margin is a property of the deployment's backend
rather than a universal constant. Against S3 with a three-second request
timeout the margin is five seconds; against Cloud Storage it is two.

The rest of the staleness budget is a freshness preference, and a caller may ask
for less at the cost of refreshing its observation more often. Under healthy
operation a cut should trail by roughly the margin plus the grid period.

With a one-hour lifetime, the 65-minute retention floor leaves a 4.5-minute
guard beyond maximum staleness plus lifetime for the reader-versus-GC rate
allowance, the control-staleness bound, history certification, GC cadence, and
operation margin.

A persisted operational state may stop new snapshot binds. Strict transactions
continue to assign timestamps and emit durable certification, and existing
snapshots retain their full lifetimes. Only after the maximum outstanding
lifetime drains may GC reduce history to latest-state roots. There are still no
reader pins: GC waits the maximum lifetime, its safety guard, and the
control-staleness bound from the durable disable fence, and retains history if
it cannot prove that interval elapsed.

That last term is what removes the strongly consistent read from every bind. A
bind validates operational state from an observation no older than the bound,
so a bind that has not yet seen `draining` is covered by the extra wait rather
than by exact ordering against the disable CAS.

Re-enabling is a fenced transition, not a Boolean flip. First durably enter
`rebuilding`, close the latest-only reclamation generation, and resolve every
delete it authorized—or fence it against delayed execution—before establishing
the baseline fence. Every writer still emits certified history while binds are
disabled; the mode changes what GC may retain, not the write format. Once the
old reclamation generation is fenced, pre-fence writes are included in the
baseline and every post-fence supersession is retained under the new generation.
Only after verifying that baseline and publishing the new history floor may
binds resume, and never at a cut older than that floor. The operational states
are `enabled -> draining -> disabled -> rebuilding -> enabled`.

Every operational transition and recovery step is ownerless, idempotent, and
helpable after the initiating client disappears. `draining` and `rebuilding`
both reject new snapshot binds and retain history when progress is uncertain.
Disable is therefore delayed pressure relief, not an emergency delete switch:
with the proposed defaults, existing reads may keep the full history obligation
for roughly an hour plus the guard. Rebuilding may require a database-wide
baseline scan while writers continue; implementations must expose its progress,
restart state, and required temporary storage headroom.

### Errors and observability

- `SnapshotUnsupported`: the backend reports no server time, or the operational
  state currently rejects binds. This is a property of the database or its
  operational state, not a transient acquisition failure.
- `ReadTransactionExpired`: the execution crossed its fixed deadline. At or
  after the deadline this error wins over a simultaneous backend result.
- Missing, cyclic, non-monotonic, or uncertified history inside the promised
  window is a corruption/invariant error, never `NotFound`.
- Backend unavailability makes a cut staler, never unsafe. A bind that needs a
  fresher observation than the client holds surfaces the underlying backend
  error rather than inventing a freshness claim.
- An unhealthy local clock refuses to bind. Losing clock health during an
  execution conservatively returns `ReadTransactionExpired` and discards the
  result, because the deadline can no longer be trusted.

Statistics should distinguish cut staleness at bind, observation refreshes per
bind, holders resolved during snapshot reads, clock-drift rejections, expiry,
commit-age aborts, history certification backlog, rebuild progress, historical
objects traversed, and the fraction of snapshot reads served without a backend
operation. These are operational outcomes, not changes to user-visible
consistency.

## Cut definition

**Decision: hybrid-logical-clock commit timestamps sourced from the backend's
clock, read on a locally derived cut grid.** An earlier revision of this design
built cuts from a global sealed epoch. That model was rejected before
acceptance; the comparison is recorded here because it is the decision the rest
of the design hangs on.

### What a cut has to be

A cut must be a downward-closed prefix of the existing strict-serializable
order: whenever the cut contains `U` and there is a serialization edge
`T -> U`, it must also contain `T`. Internal consistency across keys, ranges,
collections, and subcollections is a corollary of that single property rather
than a separate requirement. What distinguishes the candidate mechanisms is how
they establish it, and what each one costs transactions that never read a
snapshot.

### Rejected: a global sealed epoch

Assign every committed read-write transaction to one database-wide epoch,
admitted durably before its terminal certificate, and seal an epoch once every
admission in it is resolved. Locks and intents precede admission, so every
serialization edge implies `epoch(T) <= epoch(U)` and a sealed epoch is
downward-closed by construction.

This is correct and needs no clock to define the cut, which is its real
attraction. It was rejected because the cut boundary is a database-wide object:

- **A choke point on both paths.** Every commit writes the admission structure
  and every uncached bind CAS-fences a single generation object and strongly
  reads a single control record. Cloud object stores document per-object update
  rates around one per second; the workload profile in this document already
  projects ten fences per second against one object.
- **Unrelated transactions share a fate.** A frontier that advances contiguously
  cannot pass one stalled admission, so snapshot freshness for the whole
  database is a function of its single worst transaction. Recovering liveness
  requires force-aborting healthy writers on a grace timer, which is read-only
  work aborting read-write work that it does not conflict with.
- **A mandatory round trip.** Admission sits between durable payloads and the
  terminal certificate, adding a serialized wave to every commit and making
  ADR-027's parallel first-intent path ineligible, for a feature most
  transactions never use.

### Rejected: scope-limited epochs

Keep the epoch machinery but maintain one frontier per collection, fencing only
the collections a reader touches. This restores independence between unrelated
collections and prices acquisition by the reader's actual scope.

It was rejected as a partial mitigation rather than a solution. A hot collection
remains its own choke point, cross-collection writers pay a fence per
participant, the commit path still carries a round trip, ADR-027 is still
ineligible, and the cost is still paid by databases that never read a snapshot.
It is strictly better than a global epoch and strictly worse than timestamps on
every principle this design is trying to hold.

### Chosen: hybrid-logical-clock timestamps

Each read-write transaction assigns itself a commit timestamp taken from the
backend's clock. A cut is a timestamp, and a reader selects one from a
server-time observation it already holds, with no dedicated coordination step.

**Assignment.** Timestamps come from the backend's clock rather than from
client clocks. Every client already contacts one shared party on every
operation, and both S3 and Cloud Storage report a server time on every
response, so that clock is available at no cost. Once every lock is held, a
transaction sets its commit timestamp to the maximum of the server time
reported by its own lock-install responses and every timestamp it observed on
the versions and holder records it touched, plus one. The value is recorded in
holder records as a lower bound while the transaction runs and frozen into its
commit certificate. Assigning it costs no round trip: the reading rides on a
response the protocol already waits for.

A timestamp does not have to land at or after the moment its intents became
durable. Per-key monotonicity and edge propagation both come from the locks and
the maximum rule, and a timestamp that is slightly early only makes the
commit-age bound fire sooner. Readers absorb the difference in their margin
instead, which is why ADR-052 has a backend declare whether its reported time is
apply-anchored rather than requiring that it be.

**Propagation.** Every serialization edge in this system passes through a lock,
which is what makes the maximum rule sufficient:

- *Write-write and write-read.* `U` must acquire a lock `T` holds, so it either
  waits for `T`'s outcome or wounds it. If it waits, it observes `T`'s
  certificate and its timestamp and is pushed above it. If it wounds, `T`
  aborts and there is no edge.
- *Read-write anti-dependencies.* ADR-020's validate-and-lock takes shared
  `locked_by` read locks over the read set, so a later writer of that key
  observes the earlier transaction's holder record and is pushed above it.

Timestamps therefore only have to propagate across genuine lock conflicts,
which always resolve to wait-for-outcome or wound. Versions of one key are
strictly increasing, because every writer of a key holds its write lock and
takes the maximum with the current version.

**Read timestamp and the margin.** A reader derives its cut from an actual
response it received, never from its own clock. Let `D` be the server time on a
response received before it starts reading, and let `E` be the margin, the sum
of three allowances:

- skew within the backend's fleet;
- the granularity of its reported time; and
- for a message-anchored backend, how far a stamp may precede its apply.

Any write whose intent installs after that response was generated was stamped
at a fleet clock reading of at least `D - E`, so its commit timestamp is at
least `D - E`. Choosing

```text
T_read < D - E
```

therefore makes every such write invisible to the cut. Writes whose intents
installed earlier are visible as holders on the keys the reader touches and are
resolved there. No client clock appears anywhere in that argument.

The third allowance is the only one that looks unbounded, and it is not. A
stamp and its apply both fall inside a single request, so they differ by at most
that request's duration, and a response the client actually received arrived
within the client's own request timeout. The term is therefore bounded by a
value the deployment already configures, with no provider guarantee involved. On
an apply-anchored backend it is zero.

In practice a reader takes the greatest cut point at or below `D - margin`,
where the policy's staleness margin is at least `E`. Beyond absorbing `E` the
margin also lets in-flight transactions settle, so that readers rarely have to
resolve holders, and gives the grid room; that part is a policy preference
rather than a safety requirement.

The local clock may decide *when* to take a fresh sample, but it never
contributes to `T_read`. Extrapolating a cut forward from the last observation
would put local clock rate back into the safety argument.

Safety and freshness therefore separate cleanly: an old observation yields a
stale cut, never a wrong one, and freshness costs at most one small request,
which an active client already has for free. This is why maximum staleness
drops from 90 seconds, a figure that existed only to cover arbitrary client
clocks, to a few seconds sized to `E`. The retention floor ADR-040 derives from
staleness shrinks correspondingly; that cascade is not yet applied.

**Using cached observations.** A cached leaf observation may serve a cut only
if its own watermark is at or after `T_read + E`. Otherwise a write could have
landed on that leaf below the cut after the observation was taken, and the
reader would resolve that key at a different effective time from the rest of
its cut. This is the entry-point freshness rule the value cache needs, and it
is what makes a per-slot change log valuable: the log proves the absence of
writes to a leaf over an interval without re-reading the leaf. A fully
cache-served execution samples no server time of its own, so it must refresh a
server-time observation at a bounded interval or expire.

**The cut grid.** Admissible read timestamps are quantized to a fixed grid
derived from the policy, `origin + floor((t - origin) / period) * period`, with
a proposed 5-second period. Every client computes the same grid locally with no
coordination. Effective staleness is at most `margin + period`.

The grid is what makes discrete cuts available again without a global sequence.
Only the last version of a key within a slot is observable at any cut point, so
retention can coalesce a slot to one version per key, and per-slot change logs
become the natural unit for validating cached state.

**Resolving pending holders.** A reader that encounters a holder whose
timestamp lower bound is at or below its cut must resolve that holder's outcome
rather than skip it; a lower bound above the cut proves the writer is invisible.
This is not new machinery or new interference: ADR-020's "resolving the
effective current writer" already makes every strict read do exactly this
through `resolve_holders`. A holder old enough to matter here is also past its
lease, which ADR-021 and ADR-024 already reclaim.

**Commit-age bound.** A transaction must not commit with a timestamp older than
a bounded commit age, and any peer may CAS a pending transaction past that
bound to aborted using the durable fence ADR-022 already defines. The bound
covers only the window from lock completion to the commit CAS, not the user
body, so a generous value well inside the margin costs healthy writers nothing.

This bound is not needed for cut correctness, which readers get by resolving
holders. It exists so that a slot can be declared closed, which is what
retention coalescing and per-slot change logs require. The trigger is the
transaction's own age rather than an unrelated global event, so unlike the
sealed-epoch grace it cannot abort a writer because someone else is slow.

**Clock roles and health.** Local clocks retain exactly one job: measuring
elapsed time for deadlines and for deciding when to resample. That needs
monotonicity and bounded rate through suspension, which is the existing
BOOTTIME-class requirement, and an error there costs staleness rather than
correctness. Every response additionally offers a free comparison between the
local clock and the backend's, so a client that has drifted in either direction
detects it with no external reference and marks itself unhealthy. A client with
a bad clock can still commit, because its timestamps come from the backend and
not from itself. The allowance on that comparison has to exceed the excursion of
a leap smear, because both providers smear a leap second over 24 hours while the
client's own clock may step instead, putting the two half a second apart for a
day through no fault of either.

A backend that reports no server time cannot support this argument. The default
is to fail closed and refuse snapshot execution. A deployment may instead
declare that it trusts client clocks, which requires the staleness margin to
exceed twice the maximum absolute client skew and reinstates skew as a safety
input. That is a documented mode, not the baseline.

**Obtaining server time.** One monotone cell per `Database` holding the maximum
server time seen on any response is sufficient for both roles, so no
per-request attribution is needed. A writer reads the cell after its lock
installs complete; a larger value is always safe because it only delays
visibility. A reader may use any genuine past observation, because writes
installing after it are excluded by the margin and writes installing before it
are visible as holders on the keys it touches.

The two backends differ in what they can report, which is what ADR-052's
anchoring declaration exists to express. Cloud Storage returns the object
resource on the write itself, including a server-assigned modification time, so
it is apply-anchored and pays no third allowance. S3 returns an `ETag` and no
modification time on `PutObject`, so reading one back would cost an extra round
trip per mutation; it uses the `Date` response header instead and is
message-anchored. The AWS SDK exposes response headers through a client-level
interceptor, the same mechanism the S3 client already uses for its own `Expires`
handling.

Either reading counts only when it provably came from the origin. A cached or
proxied response carries an unrelated clock, so a backend must discard a
reported time it cannot attribute, rather than fold it into the cell. The
simulated and in-process backends must model server time with injectable fleet
skew so the margin can be exercised deterministically.

### Assumptions about backend time

Neither provider documents the accuracy of the time it reports, and neither
publishes a bound on skew within its own fleet. This section records what the
assumption actually rests on, so that it is not mistaken for a guarantee.

The strongest artifact is an AWS statement that they hold a SOC control keeping
clocks under a millisecond, which is externally audited but describes AWS
infrastructure generally rather than the S3 API. Amazon Time Sync documents a
typical error bound under 100µs over NTP and under 40µs with a hardware clock,
and Google states that all its services, including all APIs, run on one smeared
time base from their atomic clocks. Both providers reject requests signed more
than about fifteen minutes from their own time, which shows each treats its own
clock as authoritative, though the tolerance is far too loose to be a fleet
bound. AWS ships `correctClockSkew` in its own SDKs, deriving the client offset
from precisely the response header used here.

Against a one-second allowance, evidence of millisecond-scale agreement leaves
three orders of magnitude of headroom. It remains an environmental assumption of
the same kind as trusting that the backend implements conditional writes
correctly, and a strictly weaker one than either the client-clock alternative or
what comparable systems assume: YugabyteDB ships with half a second of assumed
skew across customer-operated machines.

Leap seconds do not enter the argument. Both providers smear one over 24 hours,
drifting up to half a second from UTC, but the design compares backend times
only against each other and never against UTC, so a smear cancels. It reaches
only the local-clock drift detector, whose allowance is sized for it above.

### Costs accepted

- **Cut safety rests on the backend's clock.** A sealed epoch's boundary cannot
  be corrupted by any clock at all. This boundary depends on `E`, and no
  provider documents any part of it, as
  [the assumptions above](#assumptions-about-backend-time) set out. It is a far
  narrower assumption than arbitrary client clocks on arbitrary machines, the
  margin is sized to absorb it with room to spare, and drift is detected on
  every response. A durable reader-validated
  history floor is planned as the detection backstop for reader-versus-GC
  disagreement so that a violation surfaces as an error rather than a wrong
  answer; that decision is not yet written.
- **The backend trait grows.** Every response must carry a server-time
  observation, an additive amendment to ADR-023 that touches every backend
  implementation. Backends that cannot supply one lose snapshot capability
  unless the deployment opts into trusting client clocks.
- **Freshness is asserted rather than proven.** The discarded fence certificate
  could prove that a cut omitted nothing older than a stated age. A timestamp
  cut asserts it from the reader's own clock, which is what comparable systems
  do, but it is a real loss.
- **Exactness is lost.** An epoch cut is an exact set of transactions fixed by
  CAS ordering. Precise incremental change capture between two consistent points
  would be cleaner on epochs; it is deferred rather than solved here.

### What the decision eliminates

Because binding performs no backend operation beyond holding a recent
observation, the whole acquisition apparatus the epoch model needed is absent:
no admission generation, no snapshot control record, no admission lanes or
their registration, no cooperative sealing, no `latest_sealed` frontier, no
freshness certificates, no begin timeout, no `FreshSnapshotUnavailable`, and no
strict read-only OCC fallback. Binding cannot fail, so the closure runs exactly
once and `read_tx` takes `FnOnce`.

ADR-020's commit sequence and ADR-027's parallel single read-write path also
survive intact, which is what narrows the
[performance gate](#performance-acceptance-gate) to history rather than commit
latency. ADR-051's logless one-CAS commit does not survive, but that is
mandatory history rather than cut selection, so no choice about cuts would have
saved it; see [Mandatory cost](#mandatory-cost).

## Design at a glance

### Write path

Snapshot support adds two things to the commit sequence: a commit timestamp,
carried in records the protocol already writes, and per-key history
certification after the commit point. Nothing else about ADR-020 or ADR-027
changes.

The sequence below is therefore the only write path. ADR-051's logless direct
commit has no place in it, because a single leaf CAS produces neither an
immutable payload nor a certificate, so an inline-eligible overwrite takes this
path like any other write. Its inline representation survives: step 7 may leave
the committed bytes in the leaf for strict latest reads.

The full sequence, counting execution of the user body:

1. execute the user body without coordination;
2. install every point, absence/membership, range, and catalog intent, while
   proving structural gates absent for ordinary node rewrites;
3. revalidate and capture actual predecessors while holding those locks;
4. assign the commit timestamp from the server time reported by those installs
   and every timestamp observed while executing;
5. durably prepare an authoritative manifest, then write and verify every
   named immutable payload or physical root, recording an immutable
   initialization witness for each mutable root;
6. publish a terminal commit certificate naming that manifest and timestamp; and
7. certify per-key history and release locks asynchronously.

Step 4 adds no backend operation. The server time is a header on responses that
step 2 already waited for, and the timestamp travels in holder records step 2
already writes.

The cut order follows from the locks rather than from anything published
globally. Because a timestamp is assigned only after every lock is held, any
transaction that depends on this writer must observe its holder record or wait
for its outcome, and is pushed above its timestamp.

This covers predicates, not only point values. Every writing transaction must
lock and revalidate every point, absence/membership, range, and catalog
predicate on which its writes may depend, and must prove structural gates
absent for ordinary node rewrites. An optimization that drops one of those
edges breaks the cut.

ADR-033 and ADR-044 supply the concrete range rule: any transaction containing
both a scan and a write takes membership-read locks on every leaf covered
through each scan's effective frontier, then revalidates while holding them.
If a limited page's frontier moves outward, it retains the locks, extends
the covered range, and repeats to a fixpoint before assigning its timestamp.

The preparation manifest is a GC root from before its named objects are created
until terminal commit or abort. The terminal CAS is allowed only after all
immutable payloads, physical roots, and root initialization witnesses are known
durable.
Helpers reverify immutable payload digests. A root is mutable after visibility,
so its immutable witness proves the initial body while its current body is
checked only for the same stable incarnation binding. Thus observing a committed
certificate still implies that every value and prepared routing root exists,
preserving the durability invariant of the current latest-value protocol.

### Closing a slot

A transaction still pending when its timestamp reaches the commit-age bound must
abort, and any peer may durably abort it through ADR-022's existing fence. Its
commit CAS and that fence race; whichever lands first is final, and a
transaction whose certificate already landed can never be aborted. A peer
without conservative evidence of the age waits a full bound from its own
observation.

Once no transaction can still commit into a grid slot, that slot is closed. Slot
closure is what lets retention coalesce a slot to one version per key and lets a
per-slot change log be treated as complete. It is not part of the
cut-correctness argument, which readers obtain by resolving holders, and its
trigger is a transaction's own age, so no writer is ever aborted because an
unrelated transaction is slow.

Compact transaction outcome fences remain authoritative after bulky transaction
objects are reclaimed, and every lock/install, commit, resolver, wound,
recovery, and GC path validates them. A delayed artifact may become an
unreachable orphan, but can never regain a committed outcome.

### Historical data

Current transaction blobs and a linear `prev_writer` walk are unsuitable for an
hour-long history window: one key could pin unrelated values from a multi-key
transaction, and a hot key could require walking hundreds of thousands of
versions.

The greenfield format separates:

- small transaction commit/certification metadata, which supplies one atomic
  outcome, commit timestamp, and authoritative manifest digest for all writes;
- independently reclaimable immutable per-key values; and
- per-key immutable history chunks with a sparse timestamp index.

Every write, including full commits, records the actual effective predecessor
observed while its install lock is held. The leaf entry names the current history
head, that version's commit timestamp, and optionally ADR-051's inline current
bytes. Those three together let the entry answer any cut at or above the current
version without dereferencing anything; they never replace the immutable history
payload or certificate, which a lower cut still resolves through. Indexed history
lookup finds the newest certified version at or before the cut without work
linear in the number of retained overwrites.

A tombstone is a normal version. Following the same chain therefore handles
create, delete, and recreate without treating an absent current key as proof that
it was absent historically. All writes from one transaction share the same
commit certificate and timestamp, preserving cross-key and cross-collection
atomicity. A committed certificate with a missing or mismatched manifest payload
is corruption, never a partial transaction.

A leaf key-directory entry with a retained history head is not vestigial. After
a delete, retain that entry and its history-head pointer while any admissible or
still-live snapshot cut may resolve the key to a present version, including a
floor version that may have committed long before the retention window began.
Only after GC proves every such cut observes absence may it prune the directory
entry, tombstone, and obsolete history. Point lookup and forward `KeyScan`
traversal depend on this enumeration invariant.

The value cache is keyed by `(logical path, writer)`. ADR-051's inline leaf state
serves strict reads and, per ADR-039, any cut at or above its recorded commit
timestamp; a historical value can never populate or poison that current state.

A cached leaf observation may serve a cut only if its own server-time watermark
is at or after the cut plus the margin, as [Cut definition](#cut-definition)
requires. Values, history chunks, manifests, and certificates are immutable and
cache without further conditions; the entry point is the only part of a
snapshot read that needs a freshness rule.

### Catalog

Collection existence and parent-child membership are versioned by commit
timestamp on top of ADR-047's transactional `name → CollectionId` directories,
which remain the authoritative current-state lookup structure. Collection
creation first writes and verifies a physical B-link root bound to a fresh
stable incarnation ID and an immutable initialization witness under its durable
preparation manifest. The manifest keeps the root live until the transaction
commits or is durably aborted. The transaction then atomically makes that
incarnation visible in its existence record and its parent's membership record.

Collection identity is incarnation-addressed under ADR-046, so no path is ever
reused and no name-derived root tombstone is needed. Incarnation-unique child
paths may be deleted because their IDs are never reused, while historical
catalog records retain a dropped ID through the snapshot horizon so a recreated
logical name cannot alias an older incarnation. Catalog visibility can never
name an absent or differently bound root. Collection deletion is not currently
public.

This makes collection existence, subcollection enumeration, and data reads share
one cut. Physical B-link roots remain routing objects rather than the logical
source of historical collection existence.

### Point reads and transactional key scans

`ReadTransaction::scan_keys` reuses ADR-033's `KeyScan` and `KeyPage` contract:
forward, keys-only scans over raw bytes; half-open `range`, plus `prefix` and
`all`; an exclusive `after` bound; and an optional `limit` on one materialized,
sorted page. `KeyPage::next_after` returns the last key only when a positive
limit filled, without promising that another key exists. Reverse scans and
stateful cursors remain out of scope. Callers needing values issue ordinary
point reads for the returned keys before the transaction ends.

Every point read and `scan_keys` call resolves logical state at the
transaction's one fixed cut. Scans enter the latest physical B-link topology at
the lower bound and follow the forward right-sibling chain. Copy-before-shrink
and current-topology revalidation absorb concurrent splits; history resolution
supplies membership and values at the bound cut. Snapshot scans register no
predicate read set, acquire no data locks, and perform no commit validation. A
collection missing at that cut returns `NotFound`.

A read that encounters a holder whose timestamp lower bound is at or below the
cut resolves that holder's outcome, exactly as a strict read already does; a
lower bound above the cut proves the writer invisible. This is the only point at
which a snapshot read can wait on a writer, it is per key, and a holder old
enough to reach a cut is already past its lease.

With no such holder, the entry's current version is the newest committed one,
and ADR-039 has the entry record its commit timestamp. If that timestamp is at
or below the cut, the current version is by definition the newest version at the
cut, so ADR-051's inline bytes answer the read immediately and a tombstone
answers absence. Only a current version above the cut sends the reader to
history. Because a cold key's current version lies below almost every admissible
cut, this is the common case rather than a special one: a snapshot scan over a
leaf of cold inline keys resolves keys and values from that single leaf, which
is what lets a snapshot execution run from cache. The reader needs no extra
freshness rule for it, since the cached leaf already had to satisfy the cut-plus-
margin watermark to be used at all.

Pagination is repeated `scan_keys` calls inside the same `read_tx` closure,
passing a page's `next_after()` key back through `KeyScan::after`. The resume key
is an ordinary exclusive bound, not an opaque or process-local cursor. Every
such call shares the fixed cut and deadline. A separate `read_tx` call may bind
a later cut, just as separate `Collection::scan_keys` calls remain separate
strict transactions under ADR-033.

ADR-033's scan-plus-write locking rules continue to apply unchanged to ordinary
read-write transactions.

History pointers and any routing needed by a live snapshot cannot be removed.
Future merge or collection teardown must retain forwarding topology through the
maximum lifetime. Expiry discards an in-flight materialized page rather than
returning a partial result.

### Retention and GC

Snapshot reads create no pins or heartbeats. GC instead retains the worst-case
window implied by the persisted policy. For the oldest possibly readable cut it
keeps every newer version plus the first version at or before that cut (the floor
version). A transaction certificate remains while any data or catalog history
references it.

Retention is measured from supersession, not original commit. A value that was
current for years and is replaced immediately after a snapshot begins must still
remain readable for that snapshot's full lifetime.

GC does not trust a writer's recorded time to establish supersession age. It may
wait the full retention interval from its own observation of the supersession;
after a crash or ownership change, inability to prove elapsed time restarts that
conservative wait. This can over-retain but cannot reclaim early.

Only a rate divergence between a reader measuring its lifetime and GC measuring
this wait can put the two out of step, and the policy guard covers it. Cut
selection itself no longer depends on either clock.

ADR-035's paginated, shuffled walk over deterministic `_t/<ss>/` shards remains
the completeness mechanism for transaction and preparation cleanup. Snapshot
history adds compact outcome fences and history indexes as GC roots and
candidate sources; it does not replace the backend's opaque provider cursor
contract. Those backend cursors are unrelated to `KeyScan::after`. GC may retain
excess history during an outage, but it never deletes promised history early.
During the operational `disabled` state it retains latest-state roots and
compact outcome fences; rebuilding a new history floor is required before binds
resume.

## Mandatory cost

Every database in this format pays for snapshot capability whether or not it
ever calls `read_tx`, so it matters exactly what that cost is. There are two
parts, and only the second was avoidable.

The logged commit paths keep their latency. ADR-020's protocol and ADR-027's
parallel single read-write commit both remain in force, because a commit
timestamp needs no object of its own and no ordering against anything global.
Dropping epochs is what buys this: there is no admission step for an install to
race, so the edge-ordering problem that would have forced every writer onto one
intention-first protocol does not arise.

ADR-051's logless direct commit does not survive, and that is the real cost.
That path commits an eligible small overwrite with a single leaf CAS, writing no
transaction object and no external record at all. Mandatory history needs an
immutable payload and a certificate for every version, and one leaf CAS cannot
produce them, so an inline-eligible overwrite falls back to ADR-027's logged
parallel path. Small single-key overwrites are exactly the workload ADR-051 was
built for, so the gate below must measure them against the inline baseline
rather than against the logged one.

What ADR-051 does keep is its representation, and it gets more useful here than
it was. Inline current bytes remain authoritative for strict latest reads, and
because ADR-039 records the current version's commit timestamp in the entry,
they also answer any cut at or above it. Only the one-CAS commit is lost.

On top of that sits ADR-039's history: an extra immutable payload per written
key, predecessor capture, asynchronous certification, and the bytes all of that
retains. Inline values are paid for twice, since the leaf copy and the history
payload both persist for the retention window, which is a reason to re-tune
ADR-051's budgets rather than inherit them.

One cost moves the other way. ADR-051's logless CAS introduced an in-doubt
outcome of its own, because an attempt cancelled after dispatch may or may not
have committed with no record to consult. Removing that path removes that case,
so the in-doubt surface returns to what the logged protocol already had.

Restoring a certified one-CAS commit is a research goal rather than a
prerequisite. Any such path needs its own ADR and must preserve the durability
and abort-fencing proofs while still emitting history.

## Performance acceptance gate

Snapshot capability cannot be opted out, including by applications that never
call `read_tx`. Consequently ADR-037 through ADR-041 and ADR-052 remain
**Proposed** until a reviewed benchmark report shows reasonable cost for the
mandatory format across the primary workloads below. An operationally `disabled`
state is not an escape hatch: it changes retention, not write format.

Because the logged commit paths are unchanged, this gate is narrower than it
would have been under a coordinated cut. It measures what ADR-039's history
costs: extra immutable payloads, predecessor capture, asynchronous
certification, and retained bytes. Commit-path latency is still measured, to
confirm that the timestamp genuinely rides along rather than adding a wave.

One cell is not narrow, and it is the one to watch. An inline-eligible small
overwrite loses ADR-051's single-CAS commit and falls back to the logged path,
so its regression is structural rather than incremental. It must be measured as
its own predeclared cell against the inline baseline; a favorable aggregate over
larger values must not be allowed to absorb it.

The benchmark plan compares the proposed format with the current
ADR-020/027/051 latest-value format under the same backend latency, concurrency,
logical work, value sizes, and fault profile. It is explicitly outcome-based:
storage-wave count and use of a specialized fast path are not pass criteria.

For every primary workload cell below, the initial reasonableness budget is p95
and p99 latency at most `1.25x` baseline and statistically converged throughput
at least `0.85x` baseline. A favorable aggregate cannot hide a failing primary
cell. A cell is one predeclared tuple of operation, strict/snapshot mode, key or
result count, applicable value-size bucket, contention level, client state, and
scan shape where applicable. Each tuple is evaluated separately; the benchmark
report fixes the finite matrix before collecting comparison results.

Proposed strict executions are compared with the current strict API. A proposed
snapshot cell is compared with the current strict read-only execution of the
same logical operation. In particular, scan baselines use the accepted
`Transaction::scan_keys` implementation with the identical `KeyScan`; no
benchmark-only iterator or reverse-scan control is introduced. The same `1.25x`
latency and `0.85x` throughput budgets apply. These ratios may be revised only
while the design is **Proposed**, before running the acceptance comparison, with
an explicit rationale and review.

The four primary workload families are:

- **single-key operations:** strict and snapshot point reads, blind overwrites,
  read-modify-write, create, and delete;
- **multi-key read-only:** fixed-size point batches and cross-collection reads in
  both strict and snapshot mode;
- **multi-key read-write:** fixed-size disjoint-key, same-leaf, and
  cross-collection transactions; and
- **scans:** a bounded forward `KeyScan::range` page, a multi-page walk using
  `next_after`/`after` inside one transaction, and a scan followed by point reads
  of the returned values, each in strict and snapshot mode; plus strict
  scan-then-write. The finite matrix includes small and large results, mid-leaf
  and cross-leaf bounds, stable membership and create/delete churn, and reports
  both transactions and logical keys/bytes per second. `prefix` and `all` are
  conformance cases over the same primitive rather than separate performance
  families.

Use representative fan-outs and values from 1 KiB through 1 MiB where the
operation actually reads or writes values, including hot keys and concurrent
writers. Keys-only scan cells vary result bytes rather than unrelated value
payload size. Add baseline scan benchmark wrappers around the accepted
`KeyScan` API; the rebase introduced the implementation and conformance tests,
not benchmark coverage. A throughput sample is valid only after history
certification and write-back queues reach a stationary bound at the offered
load; queue stability is measurement validity, not a separate performance
budget.

Binding no longer has an acquisition mechanism to benchmark, so the former
fence-rate and cold-burst cells are gone. What remains worth measuring at the
project's existing 500-client scale profile is that binding stays free in
practice: report the fraction of binds served from an observation the client
already held, and the latency of the idle-client case that must refresh one.

Run the matrix when no snapshot is ever requested as well as with concurrent
snapshot reads, and separate warm clients from idle ones. Repeat under healthy
operation, object-store tail latency, CAS contention, lost replies, and
history-certification backlog.

Report foreground p50/p95/p99 latency and storage waves, scale-out throughput,
backend reads/writes/CAS retries per committed transaction, bytes written and
retained, asynchronous backlog, commit-age abort rate, and estimated object
operation and storage cost. Metrics other than the latency, throughput, and
stationary-queue validity check diagnose the result but do not mandate a
particular implementation.

If any primary workload cannot meet the predeclared budgets without invalidating
the cut, durability, or fencing arguments, reject this snapshot design. Do not
add a strict-only database format or make snapshot correctness conditional on an
opt-out.

## Comparison

### bbolt / BoltDB

bbolt permits many read-only transactions alongside one writer, and each
transaction sees the database as it existed when it began. Its copy-on-write
pages and single-writer meta-page publication make that cut cheap, but a
long-running reader prevents page reclamation and can block remapping. GlassDB
instead has many distributed writers, retains a bounded history window without
per-reader pins, and expires the read rather than holding storage indefinitely.
See the official [bbolt transaction documentation](https://pkg.go.dev/go.etcd.io/bbolt#hdr-Transactions).

### FoundationDB

FoundationDB gives a transaction one read version, obtained from a proxy that
every transaction must contact. That centralized sequencer is what GlassDB has
no equivalent of, and building one out of object-store CAS is the model this
design rejected. FoundationDB normally retains only a short multi-version
window—its documentation describes reads older than roughly five seconds as
potentially `transaction_too_old`. Its term "snapshot read" also has a narrower
meaning inside a read-write transaction: the read omits conflict ranges rather
than creating the long-lived read-only facility designed here. See the official
[read/write path](https://apple.github.io/foundationdb/read-write-path.html) and
[ReadTransaction API](https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/ReadTransaction.html).

GlassDB trades substantially more retained object history for hour-scale,
serverless snapshots and keeps strict read-write transactions as a separate
mode.

### Hybrid-logical-clock databases

CockroachDB, YugabyteDB, and MongoDB all define read timestamps from a hybrid
logical clock rather than a sequencer, which is the family this design joins.
They synchronize node clocks with an external time service and size an
uncertainty interval against it. GlassDB has no nodes to synchronize, so it
takes the physical component from the one party every client already contacts
on every operation, and pays for that with staleness rather than with a
restart-on-uncertainty rule that a long read-only execution could not use.

## Validation

The protocol needs deterministic tests at its externally visible and recovery
boundaries. At minimum, the test plan must cover:

- timestamp monotonicity per key and across serialization edges, including a
  writer whose local clock is far behind, a delayed install racing a later
  reader, and wound versus wait resolution of a conflicting holder;
- injected fleet skew at and beyond the margin, proving that a cut stays intact
  within the margin and that the failure outside it is reproducible rather than
  incidental;
- a message-anchored backend that stamps its reported time before the write
  applies, at and beyond the request timeout, proving that the apply-anchoring
  allowance is what covers the gap and that an apply-anchored backend needs
  none of it;
- a reported time arriving from something other than the origin, proving the
  backend discards it rather than admitting a foreign clock;
- local clock and backend diverging by a leap smear's excursion, proving the
  drift detector tolerates it;
- a client that never refreshes its observation, proving its cuts grow staler
  and never become inconsistent, plus the cache-served execution that must
  refresh or expire;
- backends that report no server time, and the documented client-clock mode,
  each failing closed or degrading exactly as specified;
- partial manifests, commit versus commit-age abort, lost acknowledgements,
  root tombstone/recreate versus delayed reclamation, and delayed artifacts
  arriving after a slot closes;
- a reader encountering a pending holder at, just below, and just above its
  cut, proving it resolves exactly the first two and waits on no other writer;
- shared conformance tests for ADR-033's half-open `range`, `prefix`, `all`,
  exclusive `after`, `limit`, `next_after`, sorted materialized pages, zero
  limit, invalid bounds, and a collection missing at the selected cut;
- point, forward `KeyScan`, pagination, split, and catalog reads checked against
  an oracle reconstructed from the transactions committed at or before each cut;
- a multi-page walk bound at cut `T` while create, delete, overwrite, and split
  operations occur between pages, proving the final keys have no gaps or
  duplicates and all point-read values match `T`; a separate `read_tx` is
  explicitly allowed to bind a later cut;
- an inline-eligible small overwrite, proving it takes the logged path and
  emits history, that its inline bytes still serve strict latest reads, and that
  those bytes are never mistaken for a historical version at any cut;
- a snapshot read of a key whose current version sits at, just below, and just
  above the cut, proving the first two are answered from the leaf with no
  history object read and the third is not, including the tombstone case;
- create/delete/recreate history, committed holders awaiting write-back,
  malformed predecessor chains, and exact GC floor-version boundaries; after a
  delete and pruning at each boundary, point lookup plus forward `KeyScan`
  predicates must agree on whether the historical key exists;
- expiry around every storage await and while the user closure future is
  pending, including simulated process suspension, with late results discarded
  and page failure remaining atomic;
- local-clock drift in both directions against the backend's reported time,
  proving detection, that an unhealthy client stops binding but keeps
  committing, and that an unhealthy GC worker retains history;
- bind versus disable across the control-staleness bound, plus delayed GC
  operations across disable/drain/rebuild at exact retention boundaries, with
  crash/restart after every ownerless transition and rebuild step.

The existing deterministic-simulation tape replay, PCT schedules, cycle and
membership workloads, fault injection, and byte-identical operation replay are
the basis, extended with a modeled backend clock. A new cut oracle must verify
the exact logical state at every grid point; serializability-only ring checks do
not prove cut selection or freshness.

## Constituent ADRs

- **[ADR-037](../adr/037-bounded-staleness-snapshot-transactions.md) —
  Bounded-staleness snapshot transactions.** *Proposed.* Defines the public
  read-only contract, the fixed cut and deadline, and the persisted policy.
- **[ADR-038](../adr/038-hlc-snapshot-cuts.md) — Hybrid-logical-clock snapshot
  cuts.** *Proposed.* Defines timestamp assignment, propagation across locks,
  cut selection, the grid, and the commit-age bound.
- **[ADR-039](../adr/039-timestamp-versioned-key-history.md) —
  Timestamp-versioned key history.** *Proposed.* Defines independently
  reclaimable values and indexed per-key history.
- **[ADR-040](../adr/040-snapshot-history-retention.md) — Snapshot history
  retention.** *Proposed.* Defines pin-free retention, floor versions,
  supersession-based GC, and the bind-disable switch.
- **[ADR-041](../adr/041-timestamp-versioned-collection-catalog.md) —
  Timestamp-versioned collection catalog.** *Proposed.* Makes collection
  existence and parent-child membership part of the same cut as data.
- **[ADR-052](../adr/052-backend-server-time-observation.md) — Backend
  server-time observation.** *Proposed.* Supplies the comparable clock ADR-038
  requires, and is a prerequisite for it.

## Open questions / future work

- Complete the mandatory performance gate before accepting any constituent ADR.
  Reject the design if any of the four primary workload families misses its
  predeclared budget.
- Verify how each supported backend reports server time and how the in-process
  and simulated backends model it, including injectable fleet skew.
- Qualify the supported platform clock matrix for the BOOTTIME-class elapsed
  contract, including suspension tests and fail-closed behavior. This is now a
  deadline concern rather than a consistency one.
- Choose the margin from measured provider fleet skew rather than from the
  conservative default proposed here.
- Choose history-chunk and sparse-index sizing from hot-key and range-scan
  benchmarks while preserving a bounded lookup.
- Investigate restoring a certified single-CAS commit for inline-eligible
  overwrites, recovering what ADR-051 loses here. This is the largest identified
  regression and the reason ADR-037 keeps rejecting a second format, so its
  feasibility should be settled before the format is considered final even
  though it is not a prerequisite for acceptance. Two obstacles define the
  search, and they are of different difficulty.

  The first looks harder than it is. A one-CAS commit writes only the leaf, and
  mandatory history wants an immutable payload plus a certificate. But the
  certificate exists to give a *multi-key* write set one atomic outcome, and an
  eligible transaction touches exactly one key, so the CAS is already its
  outcome. What remains is retaining the superseded version, and for a value
  small enough to inline, that could stay in the leaf as a short in-node version
  chain that spills to an external chunk only when it outgrows a budget. The
  trade is leaf size, CAS bandwidth, and split rate against object count, which
  is the same trade ADR-051's budgets already make.

  The second is the real obstacle. A logged writer stamps from its own
  lock-install responses, so the gap between stamp and apply is bounded by one
  request, which is what the apply-anchoring allowance covers. A direct commit
  installs nothing, so it must stamp from an observation it already holds, and
  nothing bounds that observation's age except the client's local clock. Too old
  an observation lets a write install after a reader's observation while
  carrying a timestamp below the reader's cut, which is precisely the invisible
  write the margin exists to prevent. Restricting the path to a healthy client
  holding a recent observation, and adding the permitted age as a fourth margin
  allowance, would close it — at the cost of readmitting local clock rate into
  eligibility, though not into cut selection itself. Whether that is an
  acceptable weakening is the question to answer first, because it decides
  whether the rest is worth designing.
- Add safe online `SnapshotPolicy` enlargement/shrinkage if operational demand
  justifies its transition protocol.
- Define collection drop and physical topology reclamation using the reserved
  incarnation identity and forwarding lifetime.

## Relationship to other designs / ADRs

This design extends the object-storage-native transaction protocol and the
dynamic range-sharding B-link topology. On acceptance:

- ADR-052 extends ADR-023's backend trait with a server-time observation and
  leaves its operation set, opaque versions, and conditional read unchanged.
- ADR-038 adds a commit timestamp to ADR-020's existing sequence without adding
  an operation to it. ADR-027's parallel single read-write commit is unaffected.
- ADR-039 supersedes ADR-019's unified value placement and ADR-051's logless
  direct-commit guarantee, adds retained per-key history, and extends ADR-051's
  inline current values from a strict-read optimization to a cut-read one.
- ADR-028's same-key round reservation exists only to protect a direct commit's
  in-doubt recovery evidence. With no direct commits in this format it becomes
  vestigial; the coordinator invariant it sits on is unaffected.
- ADR-040 supersedes ADR-022's current-reference-only liveness for committed
  values and its cleanup of outcome evidence needed as a fence, while retaining
  its pending-lock recovery machinery and ADR-035's paginated, sharded discovery
  of transaction and preparation garbage.
- ADR-041 versions ADR-046 and ADR-047's ID-based collection directories, and
  supersedes ADR-016, ADR-018, and ADR-031 where they make the physical `_i`
  root authoritative for collection existence and parent-child membership.
- ADR-031/032/044's copy-before-shrink topology and structural gate remain the
  physical routing proof; history retention adds the no-premature-teardown
  constraint.
- ADR-036's local validation watermarks remain process-local and separate from
  ADR-052's cross-client observation. A cached leaf may serve a cut only under
  the rule in [Cut definition](#cut-definition).
- ADR-037 extends rather than supersedes ADR-033: `ReadTransaction` uses the same
  forward `KeyScan`/`KeyPage` surface. Calls inside one snapshot execution share
  a fixed cut, and separate `Collection::scan_keys` calls retain ADR-033's
  current behavior.
- ADR-035's opaque backend-list cursor is independent of key-based
  `KeyScan::after`; neither carries a cut between `read_tx` calls.

On acceptance, ADR-039 supersedes ADR-051's logless direct-commit path, because
a single leaf CAS cannot emit history. ADR-027's logged parallel path is
unaffected and absorbs that traffic. A future certified one-CAS ADR may restore
the optimization without changing snapshot semantics.
