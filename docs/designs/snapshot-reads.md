# Bounded-staleness snapshot reads

## Status

**Proposed.** This design adds long-lived, internally consistent, read-only
transactions over a fixed historical database cut. The umbrella API decision is
[ADR-035](../adr/035-bounded-staleness-snapshot-transactions.md); sealed cuts,
historical data, retention, and the collection catalog are split into the
focused ADRs indexed below.

Snapshot capability is part of the one database format in this proposal. There
is no creation-time or operational mode that lets read-write transactions avoid
the epoch, manifest, certification, and history protocol. If that mandatory
work imposes an unreasonable burden on strict transactions, the proposal is
rejected rather than split into snapshot-capable and strict-only formats. The
[performance acceptance gate](#performance-acceptance-gate) must be completed
before these ADRs can be accepted.

This document is the living companion to those proposed decisions. In
particular, the numeric defaults and optional implementation optimizations may
evolve while the proposal is reviewed.

## Goal & scope

`Database::read_tx` is a snapshot-preferred read-only API. When snapshot
acquisition succeeds, it gives the execution one global cut and keeps that cut
unchanged for the execution's lifetime. Reads at that cut are:

- internally consistent across keys, ranges, collections, and subcollections;
- read-only, lock-free, and free of commit-time validation;
- allowed to be boundedly stale;
- valid for a bounded but analytics-friendly lifetime; and
- independent of later epoch closure or reclamation decisions.

The API supports point reads, ordered key and value ranges, pagination,
collection and subcollection enumeration, and cross-collection reads. Read-write
transactions keep their existing strict-serializable semantics.

Explicit historical time travel, collection deletion, portable continuation
tokens, snapshot migration between clients, and online policy reconfiguration
are out of scope. The storage format is greenfield; existing databases need not
be upgraded or backfilled.

### Terms

| Term | Meaning |
|---|---|
| **Admission generation** | The currently appendable global writer generation. Fencing it orders every earlier admission before the fence and sends later admissions to the next generation. An empty fenced generation proves freshness without publishing an empty epoch. |
| **Epoch** | A non-empty, monotonically ordered generation of read-write transaction admissions. |
| **Lane** | One of zero or more client-owned CAS append structures registered inside an epoch. A lane batches metadata operations, never transaction semantics or outcomes. |
| **Admission** | A durable transaction entry naming its preparation manifest. It is a promise to reach committed-and-discoverable or aborted-and-fenced, not a commit. |
| **Sealed cut** | The complete logical database state through one sealed epoch: a downward-closed prefix of the strict-serializable order, not a copied database. |
| **Freshness certificate** | Same-client evidence that every commit omitted by a candidate cut is newer than a retained local duration-clock sample. |
| **Snapshot control record** | Strongly read database metadata containing the operational-state generation and contiguous `latest_sealed` frontier used to linearize a bind. |
| **History head / floor version** | A leaf entry's pointer into one key's retained history / the first certified version at or before the oldest still-readable cut. |

## User contract

### Execution

`Database::read_tx` first tries to bind the freshest admissible sealed epoch.
The selected epoch is fixed before invoking the closure. A snapshot execution
does not acquire data locks, validate at the end, or advance to another epoch.
It has no conflict-driven reason to replay the closure, so a single invocation
is an implementation goal. The public API does not promise exact-once closure
execution: it accepts `FnMut`, and every invocation must be free of external
side effects and safe to cancel at the deadline. The storage layer may retry
idempotent reads against the same epoch and deadline without reinvoking the
closure.

By default, failure to obtain a sufficiently fresh epoch before the begin
timeout falls back to a strict read-only OCC implementation of the same
`ReadTransaction` facade. The fallback is current rather than stale, but it may
replay the complete closure during validation conflicts. Point reads and every
range page contribute to one attempt's accumulated read and predicate set; a
retry starts a new attempt and invalidates its old cursors. Shipping transparent
fallback therefore requires strict implementations of the complete operation
surface, rather than delegating pagination to independent transactions.

A per-call `require_snapshot` option disables this fallback. In that mode an
admission failure returns `FreshSnapshotUnavailable` before the closure runs.
Once a snapshot attempt has started, expiry, corruption, or an unavailable read
never switches it to a strict transaction.

Both modes are internally consistent and share one fixed overall execution
deadline. Only snapshot mode promises a fixed, possibly stale cut without data
locks or commit validation; `require_snapshot` is the option that guarantees
fully unlocked execution.

Existing `Database::tx` remains strict and retryable even when its collected
write set is empty. The selected semantics come from the API, not from inspecting
the closure after it runs.

### Freshness and lifetime

Freshness measures the age of the earliest committed transaction that the cut
may omit, not the wall-clock age of the epoch object. A sealed epoch remains
current indefinitely while there are no omitted writes, so an idle database
creates no empty sealed epochs or heartbeat writes.

Snapshot begin proves that bound with a local freshness certificate, not an
untrusted cross-client timestamp. It samples its suspension-aware duration clock
immediately before issuing the admission-generation fence CAS, then freezes the
registered lanes and seals every pre-fence admission or proves the frozen suffix
empty.
Admissions after the fence use the next generation and cannot commit before
being admitted, so any omitted commit is newer than that pre-request sample.
The sample is retained while an in-doubt CAS is resolved; it may be replaced
only after proving that CAS did not land and before issuing a new attempt.

After resolving the frozen suffix, begin is ready to bind the resulting frontier
or a newer sealed one, never a cached older cut. A recent certificate may be
reused by the same client until its remaining freshness budget expires. In both
cases, immediately before the final strongly consistent read of the snapshot
control record, it starts the fixed execution deadline. That read validates the
operational-state generation and `latest_sealed`, selects the newest cut at or
beyond the certified frontier, and linearizes binding. The freshness budget is
rechecked when the read returns. Another client establishes its own proof. An
idle begin may rotate an empty admission generation to obtain this proof, but
does not publish an empty epoch.

Concretely, begin is one bounded loop:

1. establish the original begin deadline, which no retry resets;
2. obtain a certificate by sampling immediately before the fence CAS, retaining
   that sample through in-doubt recovery, and resolving the pre-fence suffix—or
   reuse an unexpired certificate owned by the same logical client;
3. reject the candidate if no positive freshness budget remains after the
   policy's total duration-clock uncertainty;
4. record a prospective `started_at`, then strongly read the snapshot control
   record and validate its operational-state generation;
5. select the newest `latest_sealed` cut no older than the certified frontier;
6. sample again after the control read; if the certificate's budget has expired,
   discard the prospective bind without invoking the closure and restart at step
   2 only if the original begin deadline permits; and
7. otherwise bind the cut and the fixed `started_at + lifetime` deadline. If no
   further acquisition attempt fits, use strict fallback or return
   `FreshSnapshotUnavailable` according to the call option.

Only the successful final control-read attempt supplies the snapshot execution
start. Retrying acquisition may replace a failed prospective start, but it never
extends the original begin timeout.

The acquisition deadline is capped by both the begin timeout and the requested
staleness budget after deducting the policy's maximum duration-clock
uncertainty. If the pre-fence suffix cannot be resolved in that time, or the
request leaves no provable positive budget, begin falls back or fails; it never
infers freshness merely from an old epoch or a racy observation of open lanes.

A snapshot execution's lifetime begins immediately before its final control
read and ends at `started_at + lifetime`. The epoch is bound by that read, just
before the closure is invoked. A strict fallback starts its lifetime immediately
before its first closure invocation. Epoch age affects admission only: a cut
accepted just inside the freshness bound still receives the full configured
lifetime from the start of binding; the epoch's prior age does not reduce it.
Strict fallback retries share the same start and never reset it.

With the proposed one-hour maximum, a strict fallback may replay the complete
closure for as long as that fixed deadline permits. This proposal deliberately
adds no separate retry-count cap: a count would make availability depend on
contention rather than the requested time budget. Callers that cannot tolerate
that replay window can request a shorter lifetime or set `require_snapshot`.
Fallback retry count and time spent retrying are reported separately.

The duration clock must be monotonic, advance through process and machine
suspension, and stay within the policy's bounded duration uncertainty over the
full retention horizon. This is a BOOTTIME-class contract; a generic monotonic
API is insufficient unless its platform implementation is qualified to include
suspension. Wall-clock adjustment cannot extend a deadline.

Clock capability and health fail closed by role. A client that cannot prove the
duration contract may still execute strict reads and writes, but snapshot begin
falls back or fails before invoking the closure. Losing that proof during an
active snapshot conservatively expires and discards its result. A GC worker with
an unhealthy or unsupported duration clock retains history and performs no
time-authorized reclamation.

Each reader and GC process must also maintain a conservative runtime health
check against an independent coarse elapsed-time signal. It checks before
snapshot admission or time-authorized deletion and whenever control returns
after a wait or possible suspension. When both elapsed deltas are usable, a
disagreement larger than that role's allocated uncertainty marks the duration
clock unhealthy; an inconclusive comparison fails closed. The default allocation
is 15 seconds of reader under-count and 15 seconds of GC over-count, whose sum is
the policy's 30-second end-to-end maximum. The comparison anchor is retained for
the full certificate, active-read, or GC retention interval and is not reset by
a successful intermediate check. This check can reject a clock but cannot make
an unqualified platform safe, and a timestamp written by a different client
cannot prove the local clock's rate. Arbitrary platform or hypervisor violations
shared by both signals remain an environmental fault assumption.

The implementation races the whole closure future against the deadline, checks
before and after every storage await, and checks again when the closure returns.
Results completing after the deadline are discarded and return
`ReadTransactionExpired`; a range page fails atomically rather than returning a
partial page. Resuming pagination and strict retry neither change nor reset the
deadline.

### Proposed policy defaults

`SnapshotPolicy` is immutable database metadata written at database creation.
Every client reads the persisted policy; a conflicting local configuration is
an open error rather than a new opinion about retention. Per-call options may
request a shorter lifetime or a stricter freshness bound, never a larger one.
Every database in this format has this policy and emits snapshot history; there
is no strict-only capability or creation-time opt-out.

| Setting | Proposed default | Purpose |
|---|---:|---|
| Activity-driven epoch target | 5 seconds | Normal cut cadence while writes are active |
| Maximum snapshot staleness | 90 seconds | Hard omission-age budget for sealing and acquisition |
| Snapshot begin timeout | 30 seconds | Time allowed to help produce an admissible cut |
| Maximum read lifetime | 1 hour | Supports cold object-store scans and analytics |
| Maximum duration-clock uncertainty | 30 seconds | Total worst-case reader-under-count versus GC-over-count across the full horizon; clocks include suspension |
| Final-phase writer grace | 15 seconds | Time before a stalled admitted writer is resolved or aborted |
| Minimum history retention | 70 minutes | Derived safety floor; see ADR-038 |

The 90-second value is a hard admission boundary, not normal lag: under healthy
operation snapshots should usually trail by no more than the roughly five-second
epoch target. A caller may choose a smaller bound and accept more sealing work or
more strict fallbacks. With a one-hour lifetime, the 70-minute retention floor
leaves an 8.5-minute guard beyond maximum staleness plus lifetime for history
duration-clock uncertainty, history certification, GC cadence, and operation
margin. The clock term is one end-to-end relative bound between the slowest
supported reader clock and fastest supported GC clock, not a separate allowance
that each side may consume independently.

A persisted operational state may stop new snapshot admission. Strict
transactions continue to use epochs and durable certification, and existing
snapshots retain their full lifetimes. Only after the maximum outstanding
lifetime drains may GC reduce history to latest-state roots. There are still no
reader pins: GC waits the full maximum lifetime plus its safety guard from the
durable admission-disable fence, and retains history if it cannot prove that
interval elapsed.

Every bind, including one using a cached freshness certificate, starts its
deadline before strongly validating the operational-state generation. A bind
linearized before the disable CAS is therefore already aging when drain begins;
a bind ordered after it observes `draining` and cannot start a snapshot.

Re-enabling is a fenced transition, not a Boolean flip. First durably enter
`rebuilding`, close the latest-only reclamation generation, and resolve every
delete it authorized—or fence it against delayed execution—before establishing
the baseline fence. Every writer still emits certified history while snapshots
are disabled; the mode changes what GC may retain, not the write format. Once
the old reclamation generation is fenced, pre-fence writes are included in the
baseline and every post-fence supersession is retained under the new generation.
Only after verifying and sealing that floor may the control record admit
snapshots at or after it. The operational states are
`enabled -> draining -> disabled -> rebuilding -> enabled`.

Every operational transition and recovery step is ownerless, idempotent, and
helpable after the initiating client disappears. `draining` and `rebuilding`
both reject new snapshot binds and retain history when progress is uncertain.
Disable is therefore delayed pressure relief, not an emergency delete switch:
with the proposed defaults, existing reads may keep the full history obligation
for roughly an hour plus the guard. Rebuilding may require a database-wide
baseline scan while writers continue; implementations must expose its progress,
restart state, and required temporary storage headroom.

### Errors and observability

- `FreshSnapshotUnavailable`: no admissible cut before the begin timeout and
  strict fallback was disabled.
- `ReadTransactionExpired`: the execution crossed its fixed deadline. At or
  after the deadline this error wins over a simultaneous backend result.
- Missing, cyclic, non-monotonic, or uncertified history inside the promised
  window is a corruption/invariant error, never `NotFound`.
- Backend unavailability cannot be turned into a freshness promise; snapshot
  begin fails closed or uses the explicit strict fallback.
- An unavailable or unhealthy duration clock follows the same pre-execution
  fallback rule; loss of clock proof after binding conservatively returns
  `ReadTransactionExpired` and discards the result.

Statistics should distinguish snapshot selection, strict fallback, helped
sealing, freshness-certificate retries, fence CAS conflicts, clock-health
rejection, expiry, forced live-writer aborts, strict fallback retries, history
certification backlog, sealed-frontier lag, rebuild progress, and historical
objects traversed. These are operational outcomes, not changes to user-visible
consistency.

## Design at a glance

### Global epochs

Every committed read-write transaction belongs to one monotonically increasing,
database-wide epoch. A sealed epoch is a downward-closed prefix of the existing
strict-serializable transaction order. `latest_sealed` advances contiguously;
snapshot begin chooses the newest sealed epoch satisfying its freshness request.

Writers use the correctness-first sequence:

1. execute the user body without coordination;
2. install every point, absence/membership, range, catalog, and structure intent;
3. revalidate and capture actual predecessors while holding those locks;
4. durably prepare an authoritative manifest, then write and verify every
   named immutable payload or physical root, recording an immutable
   initialization witness for each mutable root;
5. durably admit the manifest identity and digest into an open epoch;
6. publish a terminal commit certificate that names that manifest; and
7. certify per-key history and release locks asynchronously.

This overview counts execution of the user body. ADR-036's six protocol steps
begin with lock acquisition after that body has produced its candidate read and
write sets.

Admission happens only after the serialization dependencies are fixed. Thus any
transaction that depends on this writer must observe its intent or wait for its
outcome before entering a later epoch. Every serialization edge `T -> U`
therefore implies `epoch(T) <= epoch(U)`.

This proof includes predicates, not only point values. Every epoch-bearing
transaction must lock and revalidate every point, absence/membership, range,
catalog, and structure predicate on which its writes may depend before
admission. Any optimization that admits an epoch-bearing transaction without
preserving those predicate edges invalidates the sealed-cut proof.

The preparation manifest is a GC root from before its named objects are created
until terminal commit or abort. The terminal CAS is allowed only after all
immutable payloads, physical roots, and root initialization witnesses are known
durable.
Helpers and sealers reverify immutable payload digests. A root is mutable after
visibility, so its immutable witness proves the initial body while its current
body is checked only for the same stable incarnation binding. Thus observing a
committed certificate still implies that every value and prepared routing root
exists, preserving the invariant that the current unified transaction object
provides. The commit certificate and epoch admission may later be co-issued
behind a small two-part candidate certificate because all intents and payloads
are already visible. The baseline proof does not rely on that latency
optimization.

### Cooperative sealing and admission lanes

There is no coordinator process. Epoch sealing is an ownerless, idempotent CAS
state machine helped by active writers and snapshot begin:

```text
open E -> closing E / frozen lane set -> resolving E -> sealed E
```

A logical client (`Database` instance, including its clones) registers a sparse
admission lane in the open epoch root and opportunistically batches its
independent transaction admissions into lane CAS operations. The batching is
strictly client-local: independently opened clients are never combined, even
when they share a process. It is physical group commit only; each entry retains
its own transaction identity and outcome. High-throughput clients may use
several lanes; idle databases and inactive clients create none.

Snapshot execution does not mutate coordination state, but acquisition can: an
uncached begin CAS-fences the shared admission generation and every successful
bind strongly reads the snapshot control record. One `Database` clone family
shares certificate state and singleflights concurrent acquisition; independent
clients establish their own proofs. Expected uncached begin QPS, fence-CAS
retries, and control-record read concentration are part of the performance gate.

Closing the epoch root freezes its registered lane set. CAS-closing each lane
then total-orders every append against closure. A new epoch may accept writers
after the prior epoch's admissions are frozen; it need not wait for the prior
epoch to seal. This keeps read-write availability independent of a stalled
snapshot frontier.

The final-phase writer grace begins when its lane is closed. After the proposed
15 seconds, a sealer may race a durable pending-to-aborted CAS even when the
writer still refreshes its ordinary lease. Commit wins if its terminal CAS lands
first; otherwise the abort fence wins permanently and the writer retries in a
new transaction. Time chooses when to force progress, while the CAS and durable
fence—not elapsed time—prove safety. A helper that cannot conservatively prove
the close age waits a full grace interval from its own observation.

The grace starts at lane close, not admission. Payload and root preparation has
already completed before admission. A transaction that is still pending after
the grace may therefore lose the terminal CAS race even when its owner is
healthy and retry as a new transaction. A transaction whose commit certificate
already landed cannot be force-aborted; sealers instead help its history become
discoverable. The default grace is an availability trade-off chosen from
admission-to-terminal and object-store tail-latency measurements, not a safety
timeout.

The sealer resolves every frozen admission to one of two durable states:

- committed and discoverable from every written data or catalog entry; or
- aborted and fenced so an arbitrarily delayed artifact cannot resurrect it.

Only then may the epoch seal. Competing sealers and clients that crash after any
step converge through the same CAS transitions. Correctness never depends on a
client pause, request-duration, or clock-skew timeout. Compact epoch/lane outcome
fences remain authoritative after bulky transaction objects are reclaimed, and
every admission, lock/install, commit, resolver, wound, recovery, and GC path
validates them. A delayed artifact may become an unreachable orphan, but can
never regain a committed outcome or enter a closed lane.

If sealing falls behind the freshness bound, strict read-write traffic
continues. New snapshot reads use their configured strict fallback or return
`FreshSnapshotUnavailable`.

### Historical data

Current transaction blobs and a linear `prev_writer` walk are unsuitable for an
hour-long history window: one key could pin unrelated values from a multi-key
transaction, and a hot key could require walking hundreds of thousands of
versions.

The greenfield format separates:

- small transaction commit/certification metadata, which supplies one atomic
  outcome, epoch, and authoritative manifest digest for all writes;
- independently reclaimable immutable per-key values; and
- per-key immutable history chunks with a sparse epoch index.

Every write, including full commits, records the actual effective predecessor
observed while its install lock is held. The leaf entry names the current history
head. Indexed history lookup finds the newest certified version at or before the
snapshot epoch without work linear in the number of retained overwrites.

A tombstone is a normal version. Following the same chain therefore handles
create, delete, and recreate without treating an absent current key as proof that
it was absent historically. All writes from one transaction share the same
commit certificate and epoch, preserving cross-key and cross-collection
atomicity. A committed certificate with a missing or mismatched manifest payload
is corruption, never a partial transaction.

A leaf key-directory entry with a retained history head is not vestigial. After
a delete, retain that entry and its history-head pointer while any admissible or
still-live snapshot cut may resolve the key to a present version, including a
floor version that may have committed long before the retention window began.
Only after GC proves every such cut observes absence may it prune the directory
entry, tombstone, and obsolete history. Point lookup and both scan directions
depend on this enumeration invariant.

The value cache is keyed by `(logical path, writer)`. A separate latest-value
alias may accelerate strict reads, but a historical value can never populate or
poison that alias.

### Catalog

Collection existence and parent-child membership move from the mutable
root-local subcollection set to an epoch-versioned system catalog. Collection
creation first writes and verifies a physical B-link root bound to a fresh stable
incarnation ID and an immutable initialization witness under its durable
preparation manifest. The manifest keeps the root live until the transaction
commits or is durably aborted. The transaction then atomically makes that
incarnation visible in its existence record and its parent's membership record.

The reusable fixed `_i` path is never passed to unconditional backend deletion.
Abort CAS-compacts it to a small permanent tombstone containing the incarnation
and fence; a later creation may replace only the exact observed tombstone by
CAS. Therefore a delayed old reclamation cannot erase a newer incarnation.
Incarnation-unique child paths may be deleted because they are never reused.
Catalog visibility can never name an absent or differently bound root.
Collection deletion is not currently public.

This makes collection existence, subcollection enumeration, and data reads share
one global cut. Physical B-link roots remain routing objects rather than the
logical source of historical collection existence.

### Point, range, and paginated reads

Historical reads in both directions route through the latest physical B-link
topology and resolve logical versions at the selected epoch. Copy-before-shrink
splits guarantee that a scan sees either the pre-split source or the post-split
sibling chain. Terminal leaves are revalidated with `Latest`; interior nodes may
use the existing self-correcting cache.

History pointers and any routing needed by a live snapshot cannot be removed.
Future merge or collection teardown must retain forwarding topology through the
maximum lifetime.

A page cursor is process-local and belongs to one `ReadTransaction` attempt. It
binds:

- epoch, original start, and deadline;
- collection incarnation and original bounds/direction; and
- an exclusive resume key.

Continuation uses an exclusive bound: `key > resume_key` when moving forward
and `key < resume_key` in reverse. It never manufactures a successor key,
selects a new epoch, or extends the deadline. Tokens are not serialized,
authenticated, transferred, or resumed after process restart. In strict
fallback, all pages remain in one OCC attempt and their union is validated; a
replay constructs new cursors.

### Retention and GC

Snapshot reads create no pins or heartbeats. GC instead retains the worst-case
window implied by the persisted policy. For the oldest possibly readable cut it
keeps every newer version plus the first version at or before that cut (the floor
version). A transaction certificate remains while any data or catalog history
references it.

Retention is measured from supersession, not original commit. A value that was
current for years and is replaced immediately after a snapshot begins must still
remain readable for that snapshot's full lifetime.

GC does not trust an unbounded client timestamp to establish supersession age.
It may wait the full retention interval from its own suspension-aware
duration-clock observation of the supersession; after a crash or ownership
change, inability to prove elapsed time restarts that conservative wait. A
bounded persisted clock may shorten the delay only with its configured
uncertainty deducted. This can over-retain but cannot reclaim early.

Epoch and lane records provide an ordered GC candidate stream; the current small
paged walk of transaction objects is not sufficient at the target write rate.
GC may retain excess history during an outage, but it never deletes promised
history early. During the operational `disabled` state it retains latest-state
roots and compact epoch fences; rebuilding a new history floor is required
before snapshot admission resumes.

## Mandatory write path and optional single read-write optimization

ADR-027's exact one-wave fast path deliberately publishes its transaction object
and its first write intent in parallel. That is the opposite of the baseline's
intent-before-admission ordering. Running epoch admission beside those two writes
is not sufficient: a later-epoch transaction can read the old value while the
older install is delayed, creating a serialization edge that crosses epochs in
the wrong direction.

The current fast path therefore falls back to the intention-first protocol under
this design. Epoch admission remains part of the write protocol even while the
operational switch rejects new snapshots. The proposal does not require the same
storage-wave shape as ADR-027 or any particular replacement fast path. It is
acceptable only if the resulting user-visible latency and throughput pass the
workload gate below.

An epoch-aware single-write optimization remains possible future work. Any such
optimization needs its own ADR and must preserve the epoch-edge, durability, and
abort-fencing proofs. It is not a prerequisite for accepting snapshot reads.

## Performance acceptance gate

Snapshot capability cannot be opted out, including by applications that never
call `read_tx`. Consequently ADR-035 through ADR-039 remain **Proposed** until a
reviewed benchmark report shows reasonable latency and throughput for the
mandatory format/protocol across the primary workloads below. An operationally
`disabled` snapshot state is not an escape hatch: it changes retention and
admission, not write format or commit work.

The benchmark plan compares the proposed format with the current ADR-020/027
format under the same backend latency, concurrency, logical work, value sizes,
and fault profile. It is explicitly outcome-based: storage-wave count, lane
layout, and use of a specialized fast path are not pass criteria.

For every primary workload cell below, the initial reasonableness budget is p95
and p99 latency at most `1.25x` baseline and statistically converged throughput
at least `0.85x` baseline. A favorable aggregate cannot hide a failing primary
cell. A cell is one predeclared tuple of operation, strict/snapshot mode, key or
result count, value-size bucket, contention level, client state, and—where
applicable—scan direction. Each tuple is evaluated separately; the benchmark
report fixes the finite matrix before collecting comparison results.

Proposed strict executions are compared with the current strict API. A proposed
snapshot cell is compared with the current strict read-only execution of the
same logical operation when one exists. For a new operation with no current
equivalent—most notably bounded or reverse scans—the baseline is a frozen
benchmark-only control that traverses the same B-link data and fetches the same
logical result without epoch, history, or transaction work. That control is not
a product mode or snapshot opt-out. The same `1.25x` latency and `0.85x`
throughput budgets apply. These ratios may be revised only while the design is
**Proposed**, before running the acceptance comparison, with an explicit
rationale and review.

The four primary workload families are:

- **single-key operations:** strict and snapshot point reads, blind overwrites,
  read-modify-write, create, and delete;
- **multi-key read-only:** fixed-size point batches and cross-collection reads in
  both strict and snapshot mode;
- **multi-key read-write:** fixed-size disjoint-key, same-leaf, and
  cross-collection transactions; and
- **scans:** forward and reverse range reads and pagination in strict and
  snapshot mode, plus scan-then-write, using fixed result sizes and reporting
  both transactions and logical keys/bytes per second.

Use representative fan-outs and values from 1 KiB through 1 MiB, including hot
keys and concurrent writers. A throughput sample is valid only after history
certification and write-back queues reach a stationary bound at the offered
load; queue stability is measurement validity, not a separate performance
budget.

Within the read-only families, include acquisition cells matching the project's
existing 500-client scale profile: 500 independent active clients renewing
certificates uniformly every 50 seconds (ten uncached fences per second, leaving
margin inside the 90-second staleness bound after the 30-second uncertainty
deduction), plus a cold burst of 100 independent clients. For the cold case,
which has no current equivalent, the predeclared absolute targets are at least
ten uncached begins per second in steady state and at least 99% of the burst
binding within the 30-second begin timeout. Clone-family cached binds are
separate cells. These are read-workload latency and throughput targets, not a
required acquisition mechanism.

Run the matrix when no snapshot is ever requested as well as with concurrent
snapshot reads. Separate warm registered lanes, newly opened epochs, cold
clients, clone-family begins, and independently opened clients. Repeat under
healthy operation, object-store tail latency, CAS contention, lost replies, and
history-certification backlog.

Report foreground p50/p95/p99 latency and storage waves, scale-out throughput,
backend reads/writes/CAS retries per committed transaction, bytes written and
retained, asynchronous backlog, forced abort/retry rate, and estimated object
operation and storage cost. Metrics other than the latency, throughput, and
stationary-queue validity check diagnose the result but do not mandate a
particular implementation. Cold registration is reported separately rather than
hidden in the average.

If any primary workload cannot meet the predeclared latency and throughput
budgets without invalidating the epoch-edge or durability proofs, reject this
snapshot design. Do not add a strict-only database format or make snapshot
correctness conditional on an opt-out.

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

FoundationDB gives a transaction one read version; its transaction system and
storage servers provide the ordered frontier that GlassDB must synthesize from
object-store CAS. FoundationDB normally retains only a short multi-version
window—its documentation describes reads older than roughly five seconds as
potentially `transaction_too_old`. Its term "snapshot read" also has a narrower
meaning inside a read-write transaction: the read omits conflict ranges rather
than creating the long-lived read-only facility designed here. See the official
[read/write path](https://apple.github.io/foundationdb/read-write-path.html) and
[ReadTransaction API](https://apple.github.io/foundationdb/javadoc/com/apple/foundationdb/ReadTransaction.html).

GlassDB trades substantially more retained object history for hour-scale,
serverless snapshots and keeps strict read-write transactions as a separate
mode.

## Validation

The protocol needs deterministic tests at its externally visible and recovery
boundaries, not only unit tests of the epoch state machine. At minimum, the test
plan must cover:

- admission append versus lane close in both CAS orders, a next-generation
  commit before the fence reply, competing sealers, and crash recovery after
  every transition;
- certificate reuse and expiry before, during, and after the final control read,
  proving that acquisition retries neither invoke the closure nor reset the
  original begin timeout, and that exhaustion selects exactly fallback or
  `FreshSnapshotUnavailable`;
- partial manifests, commit versus forced abort of a live lease, lost
  acknowledgements, root tombstone/recreate versus delayed reclamation, and
  delayed artifacts after an epoch seals;
- serialization-edge tests for point, absence/membership, range, catalog, and
  structure predicates, including delayed older installs and later-epoch
  readers;
- point, range, pagination, split, and catalog reads checked against an oracle
  reconstructed from the transactions certified in each sealed epoch;
- create/delete/recreate history, committed holders awaiting write-back,
  malformed predecessor chains, and exact GC floor-version boundaries; after a
  delete and pruning at each boundary, point lookup plus forward and reverse
  scans must agree on whether the historical key exists;
- expiry around every storage await and while the user closure future is
  pending, including simulated process suspension, with late results discarded
  and page failure remaining atomic;
- qualified and unqualified duration-clock behavior under suspension, forward
  jumps, disagreement with the coarse detector, and recovery: an unhealthy
  reader discards its result while an unhealthy GC worker retains history;
- bind versus disable in both object-orderings, plus delayed GC operations
  across disable/drain/rebuild at exact retention boundaries, with crash/restart
  after every ownerless transition and rebuild step;
- clone-family acquisition singleflight and independent-client fence contention
  under the workload and regression budgets in the performance gate.

The existing deterministic-simulation tape replay, PCT schedules, cycle and
membership workloads, fault injection, and byte-identical operation replay are
the basis. A new epoch oracle must verify the exact logical state of every
sealed cut; serializability-only ring checks do not prove cut selection or
freshness.

## Constituent ADRs

- **[ADR-035](../adr/035-bounded-staleness-snapshot-transactions.md) —
  Bounded-staleness snapshot transactions.** *Proposed.* Defines the public
  read-only contract, fallback, fixed cut/deadline, and persisted policy.
- **[ADR-036](../adr/036-cooperative-sealed-epochs.md) — Cooperative sealed
  epochs.** *Proposed.* Defines the global frontier, sparse admission lanes,
  intention-first writers, cooperative sealing, and fail-closed liveness.
- **[ADR-037](../adr/037-epoch-versioned-key-history.md) — Epoch-versioned key
  history.** *Proposed.* Defines independently reclaimable values and indexed
  per-key history.
- **[ADR-038](../adr/038-snapshot-history-retention.md) — Snapshot history
  retention.** *Proposed.* Defines pin-free retention, floor versions,
  supersession-based GC, and the admission disable switch.
- **[ADR-039](../adr/039-epoch-versioned-collection-catalog.md) —
  Epoch-versioned collection catalog.** *Proposed.* Makes collection existence
  and parent-child membership part of the same global cut as data.

## Open questions / future work

- Complete the mandatory performance gate before accepting any constituent ADR.
  Reject the design if any of the four primary workload families misses its
  predeclared latency or throughput budget.
- Consider an epoch-aware single-write fast path only if profiling justifies its
  complexity; it is not an acceptance prerequisite.
- Qualify the supported platform clock matrix and runtime health detector for
  the BOOTTIME-class contract, including suspension tests and fail-closed
  behavior.
- Tune lane segmentation, local batch size, and the number of lanes per active
  client without adding an intentional batching delay. Choose final-phase grace
  from measured admission-to-terminal and backend tail latency.
- Choose history-chunk and sparse-index sizing from hot-key and range-scan
  benchmarks while preserving a bounded lookup.
- Add safe online `SnapshotPolicy` enlargement/shrinkage if operational demand
  justifies its transition protocol.
- Define collection drop and physical topology reclamation using the reserved
  incarnation identity and forwarding lifetime.

## Relationship to other designs / ADRs

This design extends the object-storage-native transaction protocol and the
dynamic range-sharding B-link topology. On acceptance:

- ADR-036 inserts epoch admission into ADR-020's commit sequence.
- ADR-037 supersedes ADR-019's unified value placement and adds retained per-key
  history to the current-writer model.
- ADR-038 supersedes ADR-022's current-reference-only liveness for committed
  values and its cleanup of outcome evidence needed as an epoch fence, while
  retaining its pending-lock recovery machinery.
- ADR-039 supersedes ADR-016, ADR-018, and ADR-031 where they make the physical
  `_i` root authoritative for collection existence and parent-child membership,
  plus ADR-022's unconditional deletion of reusable root paths.
- ADR-031/032's copy-before-shrink topology remains the physical routing proof;
  history retention adds the no-premature-teardown constraint.
- ADR-035 supersedes ADR-033's page-per-transaction and stateless-pagination
  choices for `ReadTransaction`: snapshot pages share one cut, while strict
  fallback pages share and validate one retryable OCC attempt. Both directions
  are supported.

On acceptance, ADR-036 partially supersedes ADR-027 by replacing its current
parallel first-intent path with the intention-first baseline. A future certified
fast-path ADR may optimize that baseline without changing snapshot semantics.
