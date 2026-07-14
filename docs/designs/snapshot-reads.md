# Bounded-staleness snapshot reads

## Status

**Proposed.** This design adds long-lived, internally consistent, read-only
transactions over a fixed historical database cut. The umbrella API decision is
[ADR-035](../adr/035-bounded-staleness-snapshot-transactions.md); sealed cuts,
historical data, retention, and the collection catalog are split into the
focused ADRs indexed below.

This document is the living companion to those frozen decisions. In particular,
the numeric defaults and the possible single-read-write optimization may evolve
here without changing the snapshot contract.

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
untrusted cross-client timestamp. It samples its monotonic clock immediately
before issuing the admission-generation fence CAS, then freezes the registered
lanes and seals every pre-fence admission or proves the frozen suffix empty.
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

The deadline clock must be monotonic, advance through process and machine
suspension, and stay within the policy's bounded duration uncertainty over the
full retention horizon. Wall-clock adjustment cannot extend a deadline. A platform
that cannot provide that duration contract cannot safely enable pin-free
snapshots; otherwise a suspended reader could resume after GC reclaimed its
history.

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

| Setting | Proposed default | Purpose |
|---|---:|---|
| Activity-driven epoch target | 5 seconds | Normal cut cadence while writes are active |
| Maximum snapshot staleness | 90 seconds | Hard omission-age budget for sealing and acquisition |
| Snapshot begin timeout | 30 seconds | Time allowed to help produce an admissible cut |
| Maximum read lifetime | 1 hour | Supports cold object-store scans and analytics |
| Maximum duration-clock uncertainty | 30 seconds | Bounds freshness, lifetime, and GC duration error; clocks include suspension |
| Final-phase writer grace | 15 seconds | Time before a stalled admitted writer is resolved or aborted |
| Minimum history retention | 70 minutes | Derived safety floor; see ADR-038 |

The 90-second value is a hard admission boundary, not normal lag: under healthy
operation snapshots should usually trail by no more than the roughly five-second
epoch target. A caller may choose a smaller bound and accept more sealing work or
more strict fallbacks. With a one-hour lifetime, the 70-minute retention floor
leaves an 8.5-minute guard beyond maximum staleness plus lifetime for history
duration-clock uncertainty, history certification, GC cadence, and operation
margin.

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

### Errors and observability

- `FreshSnapshotUnavailable`: no admissible cut before the begin timeout and
  strict fallback was disabled.
- `ReadTransactionExpired`: the execution crossed its fixed deadline. At or
  after the deadline this error wins over a simultaneous backend result.
- Missing, cyclic, non-monotonic, or uncertified history inside the promised
  window is a corruption/invariant error, never `NotFound`.
- Backend unavailability cannot be turned into a freshness promise; snapshot
  begin fails closed or uses the explicit strict fallback.

Statistics should distinguish snapshot selection, strict fallback, helped
sealing, freshness rejection, expiry, and historical objects traversed. These
are operational outcomes, not changes to user-visible consistency.

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

Admission happens only after the serialization dependencies are fixed. Thus any
transaction that depends on this writer must observe its intent or wait for its
outcome before entering a later epoch. Every serialization edge `T -> U`
therefore implies `epoch(T) <= epoch(U)`.

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
It may wait the full retention interval from its own monotonic observation of
the supersession; after a crash or ownership change, inability to prove elapsed
time restarts that conservative wait. A bounded persisted clock may shorten the
delay only with its configured uncertainty deducted. This can over-retain but
cannot reclaim early.

Epoch and lane records provide an ordered GC candidate stream; the current small
paged walk of transaction objects is not sufficient at the target write rate.
GC may retain excess history during an outage, but it never deletes promised
history early. During the operational `disabled` state it retains latest-state
roots and compact epoch fences; rebuilding a new history floor is required
before snapshot admission resumes.

## The single read-write fast path

ADR-027's exact one-wave fast path deliberately publishes its transaction object
and its first write intent in parallel. That is the opposite of the baseline's
intent-before-admission ordering. Running epoch admission beside those two writes
is not sufficient: a later-epoch transaction can read the old value while the
older install is delayed, creating a serialization edge that crosses epochs in
the wrong direction.

The current fast path therefore falls back to the intention-first protocol under
this design. Epoch admission remains part of the write protocol even while the
operational switch rejects new snapshots. Preserving the exact critical path
remains an isolated design goal, not a prerequisite for snapshot correctness.

The credible future optimization remains deliberately narrow: one put of an
existing key, with reads limited to that same key or none. It would issue lane
admission, immutable candidate payload, and provisional leaf install in one
parallel wave, but only when a lane is already registered in that epoch. A cold
client or newly opened epoch first pays lane registration unless registration
and first append gain a separately proved atomic form.

The leaf install is authoritative for the actual predecessor. Every ordinary
read-write point reader must, after receiving its epoch, durably raise the
entry's monotonic `max_rw_reader_epoch` in the same CAS that releases its read
lock. A delayed older install that reloads after that release is rejected; an
install racing earlier still encounters the lock. Aborted readers do not raise
the frontier. This post-admission release write is the principal cost of the
optimization. A common resolver for readers, wound-wait, sealing, and GC must
prove or fence all three candidate artifacts before declaring committed or
aborted.

Creates, deletes, scans, and catalog writes remain ineligible, so general
membership or predicate epoch frontiers are unnecessary. This optimization needs
its own ADR and deterministic proof before it can supersede ADR-027; until then,
the baseline is always available with identical snapshot semantics.

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
- partial manifests, commit versus forced abort of a live lease, lost
  acknowledgements, root tombstone/recreate versus delayed reclamation, and
  delayed artifacts after an epoch seals;
- point, range, pagination, split, and catalog reads checked against an oracle
  reconstructed from the transactions certified in each sealed epoch;
- create/delete/recreate history, committed holders awaiting write-back,
  malformed predecessor chains, and exact GC floor-version boundaries;
- expiry around every storage await and while the user closure future is
  pending, including simulated process suspension, with late results discarded
  and page failure remaining atomic;
- bind versus disable in both object-orderings, plus delayed GC operations
  across disable/drain/rebuild at exact retention boundaries; and
- mixed baseline and future fast-path writers, should the one-wave optimization
  be admitted by a later ADR.

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

- Prove and benchmark the certified one-wave single read-write path before
  recording the decision that supersedes ADR-027.
- Tune lane segmentation, local batch size, and the number of lanes per active
  client without adding an intentional batching delay.
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

On acceptance, ADR-036 partially supersedes ADR-027 by replacing its parallel
first-intent path with the intention-first baseline. A future certified
fast-path ADR may supersede that fallback without changing snapshot semantics.
