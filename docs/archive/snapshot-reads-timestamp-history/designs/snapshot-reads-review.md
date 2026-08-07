# Review of snapshot reads proposal

> **Archived review.** These findings explain why the accompanying timestamp
> design was discarded. The active replacement is the
> [dependency-checkpoint design](../../../designs/snapshot-reads.md).

The proposal imposes substantial snapshot costs on non-users and does not
guarantee the requested zero-I/O cache path. Its timestamp proof also misses
real-time ordering and relies on an unguaranteed clock bound, while its
write-path and performance analysis conflict with both its own sequence and
accepted ADRs.

## Findings

### P1: Make snapshot history opt-in

Location: [`snapshot-reads.md`, lines 16–18](snapshot-reads.md#L16-L18)

For applications that never call `read_tx`, this still removes the logless
write path, emits and certifies per-key history, and runs retention GC on every
write. That contradicts the pay-for-use requirement mandated by
[`AGENTS.md`, line 5](../../../../AGENTS.md#L5) and
[`docs/principles.md`, lines 15–16](../../../principles.md#L15-L16), as well as the
warm single-value budget in
[`docs/principles.md`, lines 10–11](../../../principles.md#L10-L11). Make snapshot
capability creation-time opt-in, or activate the history format on demand
through the existing disabled/rebuilding machinery.

### P1: Remove periodic I/O from cache-complete reads

Location: [`snapshot-reads.md`, lines 156–157](snapshot-reads.md#L156-L157)

For a cache-complete execution lasting beyond the refresh interval, this
mandates a backend request solely to refresh server time even when all data and
initially validated control evidence remain cached, so an hour-long transaction
cannot be entirely cache-served. This also defeats the optimistic cache-first
rule in [`AGENTS.md`, line 5](../../../../AGENTS.md#L5) and
[`docs/principles.md`, lines 6–8](../../../principles.md#L6-L8). Establish sufficient
control evidence at bind and recheck the floor only before a backend miss, or
define another cache-held validity mechanism, then require zero operations in
the warm-cache acceptance cell.

### P1: Preserve real-time order for disjoint commits

Location: [`snapshot-reads.md`, lines 416–419](snapshot-reads.md#L416-L419)

When disjoint transaction T completes before U starts, no lock propagates T's
timestamp to U. Even with fleet skew inside `E`, T's lock can hit an ahead
server and U's later lock a behind server, yielding `ts(U) < ts(T)`. If they
straddle a grid point, a snapshot includes U while omitting T, which is not a
prefix of the existing strict-serializable order. Add real-time ordering such
as commit-wait or a fence, or weaken the contract; the current claim violates
the strong-consistency requirement in [`AGENTS.md`, line 5](../../../../AGENTS.md#L5)
and [`docs/principles.md`, line 5](../../../principles.md#L5).

### P1: Require a guaranteed backend clock bound

Location: [`snapshot-reads.md`, lines 565–567](snapshot-reads.md#L565-L567)

If actual S3/GCS fleet skew exceeds the configured allowance—which these lines
admit neither provider bounds—a post-observation write can be timestamped below
`D - E`, invalidating the cut proof and silently producing a torn result.
Comparing responses with an untrusted local clock cannot establish a
cross-server bound. Require a documented guarantee or a coordination fallback
rather than empirical headroom, as required by the correctness-over-speed rule
in [`AGENTS.md`, line 5](../../../../AGENTS.md#L5) and
[`docs/principles.md`, line 14](../../../principles.md#L14).

### P1: Rebase the write path on ADR-053 and ADR-054

Location: [`snapshot-reads.md`, lines 644–651](snapshot-reads.md#L644-L651)

With direct commits disabled here,
[ADR-053](../../../adr/053-replay-definitive-logless-rmw-losses.md#L13-L17) says the
fallback is regular ADR-020 locking because ADR-027 no longer exists, while
[ADR-054](../../../adr/054-reserve-inline-publication-for-logless-commits.md#L50-L59)
requires logged write-back to publish `External`, not inline bytes. Thus both
the unchanged ADR-027 path and step 7's inline publication describe unavailable
behavior, invalidating the fallback-latency and leaf-only cache assumptions.

### P1: Count precommit history writes in commit latency

Location: [`snapshot-reads.md`, lines 1004–1006](snapshot-reads.md#L1004-L1006)

For every logged write, step 5 must durably prepare a manifest and write and
verify one or more payloads before the terminal certificate, operations absent
from the current ADR-020 commit path. The logged path therefore cannot keep its
latency, and multi-key transactions add per-key backend operations on the
critical path. The acceptance gate must count these synchronous operations and
waves rather than treating history as only asynchronous or retained-byte
overhead.

## Resolution proposals

> **Constraint update:** The creation-time opt-in recommendation below is
> superseded by the requirement that snapshot capability remain always on while
> regular transaction operation count and storage-wave shape remain unchanged.
> The revised proposal is
> [Low-cost always-on snapshot proposal](snapshot-reads-low-cost-proposal.md).

The earlier proposals below are retained as review history but are superseded:
their creation-time capability split does not satisfy the updated always-on
constraint. The linked low-cost proposal replaces foreground history emission
with storage-native raw versions and asynchronous checkpoint compilation.

| Finding | Proposed resolution |
|---|---|
| Mandatory cost for non-users | Persist an immutable, creation-time snapshot capability; default to strict-only |
| Periodic I/O in cache-complete reads | Bind retention/control evidence once and revalidate only before a backend miss |
| Disjoint real-time order | Use sealed admission epochs, or a guaranteed-time source plus commit-wait |
| Unguaranteed S3/GCS clock bound | Remove provider wall clocks from the safety proof; do not size correctness margins empirically |
| Superseded write path | Use ADR-020 regular locking, ADR-053 fallback rules, and ADR-054 `External` publication |
| Uncounted precommit work | Make manifest, payload, verification, and admission work explicit acceptance-gate inputs |

### Superseded proposal A: Opt-in cooperative sealed epochs

This is the recommended design for the currently supported object-store
backends. It restores the cooperative sealed-epoch construction that preceded
the HLC proposal, while correcting its two largest problems: mandatory cost and
its reliance on the now-superseded ADR-027 path.

#### Make the capability immutable and creation-time opt-in

Persist one of two database capabilities:

- `StrictOnly`, the default, uses the accepted ADR-020/051/053/054 format and
  protocols unchanged. It emits no snapshot history, performs no epoch
  admission, runs no snapshot-history GC, retains ADR-051's eligible logless
  direct commit, and does not require backend time.
- `Snapshots(SnapshotPolicy)` uses the history, catalog, retention, and cut
  protocols needed by `read_tx`. Opening a database with a conflicting local
  capability is an error.

The existing `enabled -> draining -> disabled -> rebuilding -> enabled` state
machine remains an operational state machine *inside* a snapshot-capable
database. `disabled` is not an opt-out: writers continue producing history so
the database can be re-enabled without losing the window.

Online conversion from `StrictOnly` should remain out of scope initially. A
safe conversion would need to fence or drain writers that do not yet consult
snapshot metadata, switch all post-fence writers to the history format, build
and verify a complete data and catalog baseline, publish a new floor, and only
then enable binds. Reusing the existing `rebuilding` name does not by itself
provide that writer fence. Creation-time selection avoids putting a capability
check on every strict-only commit.

#### Define cuts with admitted and sealed epochs

Every transaction in a snapshot-capable database follows this ordering:

1. Execute the user body.
2. Acquire and revalidate all ADR-020 point, predicate, range, catalog, and
   structural locks.
3. Durably prepare the authoritative manifest, immutable per-key payloads, and
   any root witnesses, and recover or verify uncertain writes.
4. Append the manifest identity and digest to an open database epoch.
5. Publish the terminal commit certificate.
6. Publish every history head and current `External` value and release its lock
   atomically per affected shard. This remains asynchronous to the committing
   caller, but is helpable.

Admission uses sparse per-client lanes so commits do not all CAS one object.
Closing an epoch first fences new lanes, then CAS-closes its registered lanes.
The next epoch may open immediately. Sealers resolve every frozen admission to
a durable commit or abort and help committed admissions become discoverable
from every write before advancing the contiguous `latest_sealed` frontier.
All transitions are ownerless, idempotent, and recoverable after a client
crash.

Locks precede epoch admission. Therefore a transaction that follows a
serialization dependency can enter only the same or a later epoch. A
transaction that completes before another begins has already entered an epoch;
the later transaction cannot enter an earlier one. Since readers bind only
whole sealed epochs, no cut can include the later transaction while omitting
the earlier one. This supplies both dependency closure and real-time order
without a clock assumption.

An uncached acquisition fences the current admission generation and helps seal
the frozen suffix. It either obtains a sufficiently fresh sealed frontier
before its begin timeout or returns `FreshSnapshotUnavailable` before invoking
the closure. A recent acquisition certificate may be shared by clones and
reused from cache within the freshness budget. Fencing an empty generation
validates an old sealed state without creating heartbeat epochs.

This changes the trade-off honestly: snapshot-enabled writers pay admission
and history costs, and a stalled writer can delay snapshot freshness. Strict
read-write traffic can continue in the next epoch while an older one is being
resolved. Databases that do not request snapshot capability pay none of these
costs.

#### Make cache-complete execution perform zero backend operations

At bind, capture all evidence needed for the execution:

- a sealed-cut acquisition certificate fresh enough for the requested
  staleness;
- a bounded-age control record containing the snapshot operational generation
  and history floor; and
- the fixed cut and execution deadline.

GC and disable transitions must honor the maximum execution lifetime plus the
control-staleness and safety guards from that evidence. Once bound, an
execution does not refresh time, the cut certificate, operational state, or the
floor merely because time passes. Immutable cached objects remain valid, and a
mutable entry point remains usable when its cached validation evidence covers
the bound cut.

Before an operation that would miss the cache, the reader checks whether its
control evidence remains valid through the operation's maximum duration. If
not, it refreshes the control record and rejects a cut below the floor before
reading possibly reclaimed history. One refreshed record may cover a batch of
misses while its validity remains. This preserves prompt operator floor
advances without periodic traffic:

- an execution that still has every required object cached may complete with a
  correct result;
- an execution that needs reclaimed backend data observes `SnapshotTooOld`
  before the miss and discards the whole result; and
- GC waits long enough after publishing a floor that any backend read admitted
  by older-but-still-valid evidence finishes before reclamation begins.

The performance gate should include an exact acceptance cell in which bind
evidence, entry-point validation, values, history, and certificates are already
cached. That execution must perform zero backend operations even when it lasts
longer than the control refresh interval. Warm scans that still need an
ADR-055 listing to validate old leaves are not cache-complete and should remain
separate cells.

#### Rebase publication on the accepted write protocols

Snapshot-capable writes have only the regular locked ADR-020 path. ADR-053
removed ADR-027, so the proposal must not use ADR-027 as the fallback for an
ineligible direct commit. `StrictOnly` databases retain ADR-051 direct commits;
snapshot-capable databases send writes to regular locking because every
version needs durable history and certification.

ADR-054 also means a newly published logged value is `External`. The immutable
per-key payload and shared certificate are its durable authority; write-back
must not copy the value into a new `Inline` leaf state. A leaf may retain
grandfathered inline state only under ADR-054's existing preservation rule.
Consequently the cache analysis must assume a cached external payload in
addition to a cached leaf for logged current values. It must not claim that a
new logged value is leaf-only.

History-head publication and lock release should be one per-shard atomic
transition. Releasing a lock before making the committed version discoverable
would create a window in which neither holder resolution nor history lookup
can find the value.

#### Account for the actual critical path

The design and benchmark report should state normal, retry, and lost-reply
operation counts for each stage. At minimum, the snapshot-capable path has the
following incremental work:

| Stage | Critical path | Cost to account for |
|---|---:|---|
| Preparation manifest | Yes | At least one durable write unless proved to be folded into an existing ADR-020 record |
| Immutable history payloads | Yes | One write per affected key or explicitly designed packing unit; parallelism reduces waves, not operations |
| Payload verification/recovery | Yes | Returned integrity evidence in the normal case and read-back operations for uncertain outcomes; count unconditional verification if required |
| Epoch admission | Yes | One logical admission and its physical lane-CAS share; at least one ordering wave after preparation |
| Terminal certificate | Yes | The commit CAS and recovery reads for an uncertain result |
| History-head publication and unlock | No for caller latency, yes for stability | Per-shard CAS work plus any immutable index/chunk writes; the queue must reach a stationary bound |
| Sealing | No for an ordinary writer, yes for acquisition freshness | Lane closure, outcome resolution, helping, and frontier publication |

The acceptance gate should make synchronous storage waves a pass criterion,
not merely a diagnostic. It should cover one-key, many-key/same-shard,
cross-shard, and cross-collection transactions, because per-key operation count
and parallel wave count scale differently. A benchmark may credit an operation
fusion only after the concrete encoding and recovery proof demonstrate it.

The strict-only acceptance cell is exact: snapshot support adds zero operations
and does not change direct-commit eligibility. Snapshot-enabled cells compare
their measured cost with a predeclared opt-in budget; they must not claim that
ADR-020 latency is unchanged.

#### ADR changes implied by Proposal A

- ADR-037 makes snapshot capability creation-time opt-in and restores an
  acquisition timeout/error before the `FnOnce` closure begins.
- ADR-038 returns to cooperative sealed epochs and records the HLC design as
  rejected because its safety input is not guaranteed.
- ADR-039 indexes history by epoch, uses regular ADR-020 locking, and publishes
  logged current values as `External` under ADR-054.
- ADR-040 validates control evidence at bind and before backend misses, not on a
  periodic timer, and specifies the corresponding GC grace.
- ADR-052 is no longer a prerequisite for correctness. Backend time may remain
  useful for metrics, but cannot define cuts on S3 or GCS without a hard bound.
- ADR-055 remains useful for scans whose cached leaves require batched
  revalidation; it is not part of the zero-I/O cache-complete case.

### Superseded proposal B: Guaranteed-time HLC with commit-wait

HLC cuts can remain an alternative only for a backend that exposes a contractual
time-uncertainty bound. The capability must provide a lower and upper bound for
the relevant backend time, including fleet skew, reported granularity, and
message-versus-apply anchoring. A measured allowance or local drift comparison
does not qualify.

For such a backend:

1. Assign the commit timestamp after acquiring all locks, as the proposal does.
2. Publish the terminal commit certificate.
3. Before reporting success, perform commit-wait until a guaranteed lower bound
   on current backend time is greater than the transaction's timestamp.
4. Select read cuts only below a guaranteed lower-bound observation.

The wait makes a later transaction's timestamp greater than every transaction
that completed before it began, including transactions on disjoint keys. The
guaranteed interval makes the post-observation-write exclusion proof sound.
The wait and any operation used to obtain the lower bound are part of commit
latency and the performance gate.

As checked on 2026-08-03, the
[S3 documentation](https://docs.aws.amazon.com/AmazonS3/latest/developerguide/RESTCommonRequestHeaders.html)
defines the `Date` header's format and signing use, and the
[Cloud Storage documentation](https://docs.cloud.google.com/storage/docs/metadata#timestamps)
defines object timestamps, but neither gives those API timestamps the required
fleet-wide uncertainty bound. AWS separately documents
[ClockBound](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/compare-timestamps-with-clockbound.html),
including a hardware-clock error bound on supported Linux EC2 instances. That
could be an explicitly configured external time provider for Proposal B; it is
not an S3 response-time guarantee and is not available to arbitrary clients.

For S3 and Cloud Storage themselves, opening a snapshot-capable database must
use Proposal A or fail with `SnapshotUnsupported`; silently substituting an
empirical `E` is not a fallback. Proposal B still requires the same
creation-time opt-in, miss-only control revalidation, ADR-020/053/054 rebase,
and precommit history accounting described above.

### Partial changes that are not sufficient

- Increasing the empirical fleet-skew allowance only makes the silent failure
  less likely; it does not establish a correctness bound.
- Commit-wait without a guaranteed lower-bound clock cannot prove real-time
  order.
- Weakening only the real-time wording does not repair the post-observation
  write that can be stamped below a cut and produce a torn multi-key result.
- Periodically refreshing server time detects neither arbitrary cross-server
  skew nor all floor races, and it destroys the cache-complete path.
- Restoring ADR-027 or logged inline publication would conflict with accepted
  ADR-053 and ADR-054 rather than repair the proposal.
