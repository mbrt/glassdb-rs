# Crate structure review

> Archived review record. Preserved for historical reference; not maintained.

## Scope and criteria

This review covers all 139 `Cargo.toml` and Rust files under `crates/`, plus the
six manifest and fuzz-target files under `fuzz/`. Generated code was inspected for
ownership and integration boundaries, but not judged as handwritten code. Build
output was excluded.

The review looked specifically for missing abstractions, functions whose control
flow hides important invariants, responsibilities spread across several objects,
and modules or structs that own unrelated policy. File size by itself was not a
finding. Several large modules are cohesive and are called out as such in the
coverage appendix.

The proposed changes are intended to preserve the principles in
[`docs/principles.md`](../principles.md):

- Do not add backend operations to warm single-value reads.
- Keep locks, CAS operations, and transaction-log writes at their existing
  correctness boundaries.
- Preserve deterministic ordering and simulation draw order unless a change is
  explicitly characterized and accepted.
- Preserve persistent formats and object-path bytes unless a migration is an
  explicit goal.
- Keep conflicts and inconsistent read snapshots internal to validated retries.

No P0 issue was found. P1 means that the current structure obscures a live
correctness, liveness, or resource-lifetime invariant and should be addressed
first. P2 items materially improve maintainability at important boundaries. P3
items are worthwhile organization work, but should follow the protocol-facing
changes.

## Prioritized findings

### P1. Correctness, liveness, and protocol state

#### 1. Make the background-task registry self-pruning

`glassdb-concurr/src/background.rs` retains every completed task until shutdown:
tasks are appended to `best_effort` or `waited` at lines 90-135, completion at
lines 51-81 does not remove them, and shutdown clones the entire historical list
at lines 141-160. High-frequency transaction paths create waited tasks from
`glassdb-trans/src/algo.rs:720-731` and `:1109-1121`, so memory and shutdown work
grow with the lifetime transaction count rather than the number of live tasks.

Suggested implementation:

- Replace the vectors with registries keyed by monotonically increasing task IDs.
- Give each completion guard a weak registry reference and remove its own entry
  when the task completes.
- At shutdown, snapshot only live handles while holding a `std::sync::Mutex`,
  release the mutex, then await the snapshot. Preserve the existing waited and
  best-effort lanes and future-drop cancellation semantics.
- Add a regression that completes thousands of sequential waited tasks and
  asserts the live registry returns to zero, plus a transaction-level shutdown
  test after many commits.

This is a bounded change with an observable resource-lifetime payoff and should
be the first fix.

#### 2. Replace invalid `DelayOptions` states with a validated rate policy

`glassdb-backend/src/middleware/delay.rs:74-129` accepts raw signed rates and
constructs limiters without validation. `same_obj_write_ps == 0` makes
`try_acquire_token` permanently false at lines 284-287, while `backoff` retries
forever at lines 154-163. A zero write-delay mean also prevents model time from
advancing. Prefix-rate zero has different, actually-disabled semantics at lines
351-365. Thus a public configuration described as disabled can make every write
an infinite future.

Suggested implementation:

- Introduce `RateLimit::{Unlimited, PerSecond(NonZeroU32)}` and validate the full
  delay configuration at construction.
- Separate provider latency from rate limiting, for example
  `ProviderLatencyProfile` and `WriteRateLimits`.
- Skip object backoff entirely for `Unlimited`; for an enabled limiter, either
  reject a zero retry interval or return the precise reservation duration as the
  prefix limiter does.
- Add timeout-based tests proving unlimited writes finish, invalid rates are
  rejected, and enabled limiters cannot spin without advancing model time.

#### 3. Decompose `CachedStore` around one explicit knowledge protocol

`glassdb-storage/src/cached_store.rs` currently contains one consistency protocol
distributed across path admission and read coalescing (lines 333-457), expected
state and cancellation reconciliation (459-676), three repeated mutation outcome
paths (857-1017), and L2 lookup/publication/fencing/evidence logic (1088-1507).
`MutationGuard::drop` at lines 667-675 performs correctness-significant
invalidation. The loose structure makes it easy for create, CAS, and delete to
diverge on conflict, in-doubt failure, or cancellation.

Suggested implementation:

- Retain `CachedStore` and `TypedCachedStore` as the public facade.
- Extract private components: `knowledge` for observations and evidence merging,
  `path_lane` for per-path serialization/coalescing, `mutation` for mutation
  leases and reconciliation, and `persistent_bridge` for all L2 interactions.
- Normalize backend results to one internal `MutationOutcome` and apply them
  through a single `finish` transition. Reserve `Drop` for the explicit
  invoked-but-cancelled transition.
- Replace the `Arc<dyn Any>` lifetime coupling at `cached_store.rs:1060` and
  `disk_cache.rs:318` with a semantic `PathLease` or `FenceContext`.
- Characterize create/CAS/delete crossed with committed, missing, conflict,
  unavailable, definitive error, and cancellation, both with and without L2.

The extraction must preserve the existing ordering of L1 publication, L2
fencing, observation evidence, and path-lane release.

#### 4. Turn `CasWorker::run_shard` into a fold plan plus a small retry loop

`glassdb-trans/src/shard_coord.rs:385-654` combines batch preparation, leaf load,
routing, resolver order, logless exclusion, capacity admission, CAS persistence,
in-doubt recovery, retry, and result delivery. Per-member state is split between
an `in_doubt` set, a result tuple whose boolean means "staged in this attempt",
and a separate `logless` set (`:414`, `:472`, and `:476`). That tuple flag controls
both uncertainty attribution and receipt creation at lines 615-639.

Suggested implementation:

- Introduce named private values such as `MemberFold { id, outcome,
  participation }`, `Participation::{Skipped, Staged}`, and `FoldPlan { entries,
  locks, members, dirty }`.
- Extract `fold_round(loaded, members, cause) -> FoldPlan`; it may remain async
  because resolvers are async.
- Put capacity admission in one helper returning an accepted stage or a
  member-specific rejection.
- Normalize persistence to `PersistResult::{Landed, PreconditionMiss,
  InDoubt(staged_ids)}`.
- Leave `run_shard` as load, fold, persist, update uncertainty, and deposit.

Keep the existing `Dedup`, `ShardResolver`, and coordinator ownership boundaries.
Add plan-level tests in which one member stages while another skips or rejects,
then retain all deterministic cancellation and in-doubt regressions.

#### 5. Model structural-split phases instead of passing a recovery boolean

`glassdb-trans/src/split.rs` duplicates the root and non-root lifecycle in
`split_nonroot` (lines 1104-1151) and `split_root` (1260-1303): prepare intent,
join topology, coordinate, conditionally delete intent, leave topology, finalize,
and wake recovery. Both coordination paths pass `recovery_pending: &mut bool`
(`:1155-1163` and `:1365-1372`) and toggle it around the durable
`Preparing -> Ready` transition. They also receive an untyped structural-log
observation and revalidate phase and kind fields independently.

Suggested implementation:

- Add private phase wrappers such as `PreparedSplit` and `ReadySplit`; make the
  durable transition consume the former and return the latter.
- Only allow `ReadySplit` to create children. Return
  `Completed`, `RetryCleanly`, or `RecoveryRequired(ReadySplit)` instead of
  mutating an out-parameter.
- Extract the shared lifecycle into a concrete `StructuralSplitAttempt` selected
  by a root/non-root enum. Do not add a one-implementation capability trait.
- Move recovery scanning and separator publication into private collaborators
  behind the existing `Splitter` facade after the phase model is in place.
- Add a transition table proving which failures may discard a `Preparing` intent
  and which failures must retain a `Ready` intent.

The persisted `StructuralLog` representation does not need to change.

#### 6. Extract a pure conditional-mutation state machine from the S3 backend

`glassdb-backend-s3/src/lib.rs:141-240` mixes request issuance, conditional-write
validation, retry accounting, lost-ack taint, backoff, SDK error classification,
and final mapping. Lines 472-596 then classify overlapping properties of the same
SDK errors again. Classifier precedence is correctness-critical: an ambiguous
write followed by a precondition failure must not be downgraded to a confident
conflict.

Suggested implementation:

- Classify each SDK error once into a provider fact such as `Precondition`,
  `Conflict`, `Throttle`, `Ambiguous`, `NotFound`, or `Other`.
- Add a pure state machine holding `attempts` and `may_have_applied`, returning
  actions such as `Retry(delay)`, `Precondition`, `InDoubt`, or `Terminal`.
- Keep the async loop responsible only for sending, sleeping, and executing the
  returned action. Reuse provider facts in read/list/delete while retaining each
  operation's distinct policy.
- Use table tests for ambiguous-to-precondition, throttle-to-precondition,
  conflict exhaustion, ambiguous terminal failure, and success without a usable
  version. Preserve the existing lost-ack integration tests.

#### 7. Give the public database transaction loop an explicit attempt driver

`glassdb/src/db.rs:462-600` (`DbInner::tx_impl`) owns user-closure execution,
statistics, lazy engine-handle creation, validation of body errors, wound/retry,
commit, cancellation, and final cleanup. Begin/reset logic is duplicated at lines
503-510 and 519-526, and wound cleanup/rebegin is split across lines 534-544 and
558-574. An `Option<EngineTransaction>` and a separate abort guard must remain in
sync by convention.

Suggested implementation:

- Add a private `AttemptDriver` that owns the engine handle and cancellation/end
  guard as one state.
- Give it named operations for installing accesses, validating a returned body
  error, restarting after a wound, committing, and finishing.
- Represent idle/active/finished states explicitly so a handle cannot exist
  without its guard.
- Keep `tx_impl` as the policy loop around user closure, attempt result, and retry
  decision. Do not move transaction-body errors out of read validation.
- Add focused cancellation tests at every await boundary and assert begin/end
  accounting remains balanced through wound restarts.

#### 8. Make recovery-manifest projection single-source

`TxRecoveryManifest` is an anemic field bag at
`glassdb-trans/src/monitor.rs:194-201`. Its fields are manually copied into abort,
pending, refresh, and observed-abort logs at lines 392-397, 638-644, 776-780, and
1257-1267. `collection_commit.rs:63-98` independently constructs and projects the
collection subset. These backreferences are what GC and crash recovery use; a new
field can currently be missed in one lifecycle path.

Suggested implementation:

- Give `TxRecoveryManifest` one `apply_to(&mut TxLog)` projection and, where
  useful, `from_log`, `pending_log`, and `aborted_log` constructors.
- Keep committed writes separate, but use the same projection in pending,
  refresh, observed abort, and collection preparation paths.
- Add a round-trip/equality test that enumerates every manifest field. A new field
  should make that test and the central projection fail to compile until handled.

### P2. Important ownership and abstraction boundaries

#### 9. Separate persistent-cache format/recovery from worker policy

`glassdb-storage/src/disk_cache.rs` contains geometry/layout (lines 30-205), the
facade and fences (207-716), queues/shutdown/work reservations (718-997), the
admission filter (998-1080), disk I/O and recovery (1082-1650), worker/promotion
policy (1652-1846), and binary encoding/checksums (1848-1969). `run_worker` at
lines 1678-1784 manually balances counters, reservations, fences, error disabling,
and shutdown.

Suggested implementation: split private modules into `format`, `disk`, `worker`,
`admission`, and `fence`, leaving `PersistentCache` in the facade. Give optional
work an RAII reservation and move each `Work` branch into a named handler that
returns `ControlFlow`. Preserve the byte format exactly and add fixed golden
vectors for headers, slots, markers, and records before moving code.

#### 10. Separate transaction-log model, codec, and persistence

`glassdb-storage/src/tlogger.rs` mixes ten domain types (lines 21-162), cached
persistence (164-347), and wire encoding/validation (386-738).
`decode_tx_log_from_proto` at lines 420-530 decodes status, writes, four lock
shapes, collection changes, and prepared collections in one function.
`txobject.rs:18-20` reaches into its private codec, while `TValue` in this module
is really a transaction-reader result.

Suggested implementation: introduce `transaction/model.rs`, `codec.rs`, and
`store.rs`; break decoding into one helper per repeated field; expose
`TxLogCodec::decode_status`; and move `TValue` to `glassdb-trans`. Preserve current
re-exports during migration and keep the golden transaction-object bytes. Remove
`Result` from `marshal_write` and `marshal_lock` if they remain infallible.

#### 11. Split transaction data staging from collection-catalog staging

`glassdb/src/tx.rs` makes `TransactionInner` own key/value reads, staged values,
scans, collection catalogs, lifecycle changes, created/dropped collections,
reservations, and abort state (`:44-55`). KV operations occupy lines 87-240;
collection lifecycle work occupies 243-501 and 599-708; reset/access serialization
is interleaved at 530-590.

Suggested implementation: extract `DataOverlay` and `CatalogOverlay` into private
`tx/data.rs` and `tx/catalog.rs`, with the `Transaction` facade delegating. Each
overlay should own its reset and access-extraction logic. Replace boolean creation
modes at lines 599-604 with a `CreateMode` enum. Resolve a child collection once
instead of `collection_path_exists` performing an existence check followed by an
open at lines 350-364.

#### 12. Make validation evidence opaque across `glassdb-trans` and `glassdb`

`glassdb-trans/src/access.rs:17-23` and `reader.rs:44-52` expose physical
`LeafObservation` values. Scan evidence is copied as parallel `keys`, `covered`,
and `frontier` fields in `access.rs:60-73` and `key_resolver.rs:20-25`.
`glassdb/src/tx.rs` imports the storage observation directly and manually rebuilds
read and scan access tokens. The facade also exposes resolver-only controls that
the public caller always passes as `None`.

Suggested implementation: introduce opaque `ReadEvidence` and `ScanEvidence`
types in `glassdb-trans`; construct `ReadAccess` from logical key plus evidence;
and provide a consuming `ScanResult -> ScanAccess` conversion. Keep lock-holder
and scan-cap controls on crate-private resolver APIs. Change both crates together
and preserve phantom/read-validation integration tests.

#### 13. Bind a leaf edit to the observation it will CAS

`LoadedLeaf` stores public cloned entries, cloned locks, an observation, and the
original node in `glassdb-storage/src/shard_store.rs:34-61`. `store_leaf` then
accepts path, entries, locks, and observation independently at lines 401-428.
This permits staged contents from one load to be paired accidentally with another
observation, while `node()` still exposes the pre-edit node.

Suggested implementation: make `LoadedLeaf` fields private and add
`into_edit() -> LeafEdit`, where `LeafEdit` owns the exact observed node and
observation. Provide bounded entry/lock mutations and make `store_leaf` consume
the edit. Migrate through a compatibility wrapper, then remove the loose-argument
form. Test topology preservation, stale-observation conflict, and immutable path.

#### 14. Centralize lock representation while retaining scope-specific rules

`glassdb-storage/src/lock.rs` only defines `LockType`; reusable holder state is
named `NodeLock` in `node.rs:204-294` but is also used by collection records.
Entry locks expose independently mutable `lock_type` and `locked_by` fields in
`shard.rs:87-98`; decode rejects invalid combinations while encode can serialize
them. Protobuf mapping and holder-shape validation are repeated across
`node.rs`, `shard.rs`, `collection_store.rs`, and `tlogger.rs`.

Suggested implementation: move a neutral, field-private `LockState`/`HolderSet`
to `lock.rs`, centralize canonical ordering and protobuf conversion, and wrap it
with scope-specific types such as `EntryLockState`, `SharedExclusiveLock`, and
`ExclusiveGate`. Make invalid entry states unrepresentable before encoding. A
temporary `NodeLock` alias can keep migration mechanical.

#### 15. Make physical object paths one validated, typed vocabulary

`glassdb-data/src/paths.rs` has useful typed references but also multiple
independent parsers. Constructors such as `LeafRef::node` (`:102-107`) and
structural-log formatting accept strings that their parsers reject. Transaction
parsing is repeated at lines 249-268, 297-313, and 461-467. In addition,
`glassdb-bench-scale/src/backend_breakdown.rs:221-246` re-parses the physical
object naming scheme for reporting.

Suggested implementation: split collection/tree, transaction, and structural-log
codecs; add validated `DbRoot`, `NodeToken`, and `StructuralRecordId`; and define
one `ObjectPath` enum with `Display` and `TryFrom<&str>`. Make benchmark breakdown
classify the typed result instead of duplicating storage knowledge. Preserve every
encoded byte and existing golden vector, so no data migration or ADR is needed.

#### 16. Move structural-log persistence out of `ShardStore`

`glassdb-storage/src/shard_store.rs` owns node and structural-log typed stores
(`:27-32`), both codecs (`:63-118`), node/leaf operations (`:129-301` and
`:379-485`), and interleaved structural-log CRUD/listing (`:303-377` and
`:487-513`). The two object families evolve for different reasons.

Suggested implementation: extract `NodeStore` and `StructuralLogStore`; keep a
temporary `ShardStore` facade exposing `nodes()` and `structural_logs()`. Make
`TreeRouter` depend only on `NodeStore`, and pass `StructuralLogStore` explicitly
to split recovery and GC. Preserve participant validation and pagination tests.

#### 17. Centralize B-link traversal in a private descent cursor

`glassdb-storage/src/tree_router.rs` repeats root bootstrap in `leaf_for`,
`first_leaf_at`, `leftmost_leaf`, `token_reachable_at_key`, and
`parent_index_for` (lines 103-149, 231-265, and 354-438). Reachability and parent
lookup also reimplement right-step/child-load logic already represented by
`descend_to_leaf` and `step_right_until_owns` at lines 446-498.

Suggested implementation: add a private `DescentCursor` carrying the current
located node, requirement, prefix, and accumulated cache-hit state, with
`normalize_at(key)` and `advance_for(key)`. Express lookup, reachability, and
parent lookup as stopping policies over it; use a separate `LeafChain` for sibling
scans. Retain the existing hot-path freshness distinction and test all entry
points against stale roots, stale parents, and an interior right hop.

#### 18. Give deduplication an explicit lock-contained `KeyMachine`

`glassdb-concurr/src/dedup.rs` represents phase through several collections, map
membership, and an optional operation token (`:108-126`). Batch mutation,
driving, handoff, waiter cancellation, and a correctness-significant `Drop` path
are spread across lines 173-256, 401-523, 603-640, and 697-745.

Suggested implementation: define phases such as `RunningInline`, `RunningOwner`,
and `Closing`; accept events such as submit, round-finished, driver-dropped,
waiter-dropped, and close; and return side-effect-free actions to wake, spawn,
deliver, or remove. Execute actions after releasing the shard lock and use a
`VecDeque` for FIFO submissions. Keep the public `Dedup` API stable and add
deterministic transition tests for the one-driver/no-orphan invariants.

#### 19. Return a complete shard-lock receipt from coordination

`glassdb-trans/src/shard_coord.rs:91-95` already produces entry and membership
strength. `KeyLocker::acquire` records it into the mutable `tlocks` map at
`tlocker.rs:1082-1085`, then `lock_shard` discards it and returns only a leaf
observation. `lock_at` later reconstructs the successful receipt by rereading the
bookkeeping map at lines 826-849.

Suggested implementation: make the locked outcome carry
`{ observation, held: HeldLeaf }`; return those per-path receipts from shard
locking; and build `LockedTx` directly from them. Keep `tlocks` only for
cancellation cleanup, serial fallback, release, and diagnostics. Add a regression
for a point write plus scan holding both strengths without consulting `tlocks`.

#### 20. Replace `Algo::Handle`'s correlated flags with validated transitions

`glassdb-trans/src/algo.rs:112-153` combines status, attempt count, `engaged`,
`lock_reads_on_retry`, and backoff. Read mode is inferred from three fields, while
status/engagement are updated in multiple distant paths. `begin` also hardcodes
`RetryConfig::default()` at lines 568-579 even though `EngineConfig` exposes retry
tuning used by the other coordination components.

Suggested implementation: introduce constrained state such as
`AttemptPhase::{New, Engaged, Committed}` and
`ReadValidationMode::{Optimistic, Locked}`, plus named `engage`, `commit`,
`force_locked_reads`, and `renew` transitions. Derive whether abort is needed from
the phase. Inject the configured retry/backoff factory into `Algo`, or explicitly
name and document a separate acquisition policy. Test transition legality and
configured retry timing with paused model time.

#### 21. Extract the direct-commit subprotocol from `Algo`

`glassdb-trans/src/algo.rs` combines direct-commit types and eligibility
(`:198-478`), direct predecessor resolution/execution (`:949-1098`), general
locked orchestration (`:785-947`), and read validation (`:1273-1433`). Dependencies
used primarily by the direct path consequently live on the general `Algo` object,
and direct-path tests share a roughly 3,000-line inline test module.

Suggested implementation: move the resolver and helpers to
`algo/direct_commit.rs` and add a private concrete `DirectCommit` collaborator
owning its coordinator, inline policy, hints, GC hinting, and counters. Keep
`Algo` as the architectural policy owner and dispatch on `DirectAttempt`; do not
introduce a single-implementation trait. Move direct-path tests with the code and
verify statistics and uncertain-CAS/replay behavior remain identical.

#### 22. Share effective-writer resolution in `KeyStateResolver`

`glassdb-trans/src/key_state_resolver.rs:169-296` has three projections over the
same entry state. `resolve_writer` and `resolve_holders` duplicate exclusive-holder
cardinality validation, status lookup, committed value load, writer advancement,
and tombstone handling; `entry_exists` adds another projection.

Suggested implementation: extract one private helper that validates an entry and
resolves an exclusive holder into a named effective writer/value/deleted result.
Let holder resolution extend that core with live compatible holders, preserving
the writer-only fast path. Add a matrix over absent/external/inline/tombstone
values, pending/committed/aborted exclusive holders, and shared readers.

#### 23. Express `SplitPolicy` in actual wire-size budgets

`glassdb-storage/src/node.rs:141-186` documents `leaf_max_bytes` as leaf-only, but
the index path also applies it at lines 598-609. `key_fits` estimates bytes with a
fake transaction and a magic 24-character node token rather than sizes owned by
the actual codecs.

Suggested implementation: either rename the limit to `node_soft_max_bytes` or
separate leaf and index budgets; expose maximum encoded lengths from `TxId` and a
validated `NodeToken`; and add codec functions such as
`worst_case_leaf_entry_len` and `worst_case_parent_separator_len`. Validate that
headroom cannot exceed the hard cap and tie boundary tests to actual encoded
maximum-size nodes.

#### 24. Make backend listing invariants executable once

The list contract is prose in `glassdb-backend/src/lib.rs:223-233`; prefix
validation is repeated in the memory, S3, and GCS backends; and the same core
create/read/CAS/delete/list pagination cases are copied into each backend's tests.
A new backend can implement the trait while overlooking an invariant.

Suggested implementation: introduce a validated `ListRequest` or initially a
shared validator, associate cursor use with its prefix while leaving provider
tokens opaque, and add a reusable backend conformance suite behind test support.
Keep transport faults, retry behavior, and provider-specific version semantics in
local tests. Stage the validated request after the conformance suite if changing
the trait atomically would be disruptive.

#### 25. Consolidate simulation-aware entropy and latency sampling

Native-versus-simulation randomness is independently selected in
`glassdb-concurr/src/exec.rs`, `glassdb-data/src/entropy.rs`, and
`glassdb-concurr/src/retry.rs`. Delay middleware and the S3 fake server sample the
process RNG directly and duplicate lognormal parameterization. Composing them
with deterministic simulation can therefore escape seeded replay.

Suggested implementation: expose one all-build entropy facade from
`glassdb-concurr`, including byte filling and uniform-unit sampling; define a
reusable latency distribution; and inject an explicit seeded source into the
separately threaded fake server. Record that consolidation changes deterministic
draw order and deliberately refresh affected replay seeds/corpora rather than
silently accepting drift.

#### 26. Give provider fakes owned lifecycles and smaller roles

`glassdb-backend-s3/src/fake_server.rs` combines store/fault state (lines 56-110),
server lifecycle (112-269), protocol handlers (271-560), codecs (562-666), and
latency (668-743). Its thread and four-worker runtime intentionally live for the
process lifetime. GCS's `src/tests.rs` similarly embeds a complete fake server,
multipart/query codecs, and the actual tests in one file.

Suggested implementation: give `FakeS3` a shutdown signal and native thread
handle, select shutdown against accept, provide explicit shutdown plus defensive
`Drop`, and split server/store/protocol/fault/codec modules. Give the GCS fake an
owned accept-task handle. Add repeated start/shutdown tests and retain the public
feature-gated fake interface for benchmarks.

#### 27. Centralize cache weight arithmetic using checked `usize`

`glassdb-storage/src/cache.rs:27-48` uses saturating subtraction, which hides a
broken accounting invariant, while replacement at lines 93-109 converts `usize`
weights through signed `i64` arithmetic. A public `Weighable` can report values
above `i64::MAX`.

Suggested implementation: add one checked `replace_weight(old, new)` helper in
`usize`, assert that removal never exceeds the current size, and choose an
explicit overflow policy (saturating at `usize::MAX` followed by eviction is safer
than wraparound). Use it for deletion, replacement, and eviction, with synthetic
tests near `usize::MAX`.

#### 28. Offer infallible iterator APIs for already-materialized results

`glassdb/src/iter.rs:11-29` and `:47-63` wrap every item in `Ok`, while
`Collection::keys` materializes all keys at `collection.rs:157-164` and collection
listing materializes results in `tx.rs:392-406`. The exposed fallibility suggests
streaming I/O that cannot actually occur and makes ordinary callers handle an
impossible error per item.

Suggested implementation: in the next API-compatible window, add infallible
iterators whose item types are the values themselves and deprecate the legacy
fallible wrappers. If future streaming is desired, expose it as a separate API
whose failure and lifetime semantics are real. This is lower than the protocol
items because it is primarily API clarity.

### P3. Verification and tooling organization

#### 29. Split the public simulation code by executable role

`glassdb/src/sim/harness.rs:432-595` has one `run_generic` that creates streams,
backbone, media, transports, crash/restart client tasks, observer, nemeses, heal,
and verification. The file also owns configuration, nemeses, fuzzing, and PCT
scheduling. `sim/api.rs` combines arbitrary-program generation, exact model,
oracle helpers, a large action executor, the program loop, and final verification.

Suggested implementation: introduce `RunPlan`, `RunContext`, and `ClientRunner`;
move nemeses and scheduling into their own modules; and split API simulation into
generator, model, executor, and oracle. Preserve spawn order, entropy draw order,
and operation-log ordering, and prove old corpus inputs produce identical traces.

#### 30. Give the disk-cache simulation harness typed commands

`glassdb-storage/src/disk_cache/sim_harness.rs:46-266` represents operations as a
`u8`, decodes with `% 10`, and executes them in a 147-line numeric dispatcher that
also owns cache lifecycle, media faults, oracle state, and event recording.

Suggested implementation: decode into a stable `CommandKind` enum, introduce a
`HarnessState`, and move each command to a short handler without changing the byte
mapping. Add a mapping test and compare event traces for the existing corpus.

#### 31. Split runtime facade from scheduler and executor mechanics

`glassdb-concurr/src/rt.rs` contains dedicated-task lifecycle, native runtime
adapters, and simulated clocks/tasks/timeouts. `exec.rs` contains scheduling
policies, timer/task/waker primitives, runtime setup, draining, and the executor
loop.

Suggested implementation: use private modules such as `rt/clock.rs`,
`rt/task.rs`, `rt/dedicated.rs`, `sim/scheduler.rs`, and `sim/executor.rs`, retaining
`rt` as the stable facade. Keep native/simulation parity tests at the facade so
cfg-specific APIs do not drift.

#### 32. Break the mixed benchmark into scenario phases

`glassdb-bench-scale/src/bin/perfbench/mixed.rs` combines CLI options, dimension
generation, result/statistics types, cell lifecycle, workers, split settlement,
and setup in roughly 1,000 lines. `run_cell` alone spans lines 446-601 and changes
between configuration, database setup, concurrent execution, measurement, and
cleanup.

Suggested implementation: split options, result reporting, scenario setup, cell
execution, and workload workers. Extract a small reusable scenario lifecycle only
after comparing the contention and inline-pressure benchmarks; avoid a generic
framework whose only purpose is deduplicating a few setup lines.

#### 33. Split oversized integration-test files by public behavior

`glassdb/tests/integration.rs` is roughly 1,900 lines covering unrelated basic
API, statistics, scans, shutdown, and cancellation behaviors; it also embeds the
`PauseControl` hook fixture at lines 1541-1675. The production API is not at fault,
but locating regression ownership is unnecessarily difficult.

Suggested implementation: split into behavior-oriented integration targets such
as `basic`, `stats`, `scan`, `shutdown`, and `cancellation`, with the pause backend
in shared test support. Keep collection behavior in its existing coherent test
target. Do this mechanically so test concurrency and feature gating do not change.

## Recommended implementation sequence

This is an execution checklist, not a set of suggested mega-PRs. The 33 findings
are decomposed into 123 tracked changes. One checkbox is the intended scope of
one agentic implementation session and one human-reviewable local diff. A
checkbox should introduce one behavior change or perform one mechanical
extraction, never both. If a task uncovers a larger semantic change, stop and
add another checkbox rather than expanding its scope silently.

The order is priority-first and value-oriented. Complete each workstream
vertically: add the characterization or migration protection, immediately perform
the changes it protects, and leave the system in a useful state before beginning
the next workstream. The few prerequisites promoted ahead of their original
priority are called out explicitly. Release-gated removals are collected at the
end so they do not block compatible improvements.

Keep the `FNN-X` identifiers stable. On completion, change `[ ]` to `[x]` and
stop for human review. If an item becomes blocked or is superseded, leave it
unchecked and append the reason and replacement ID; do not delete it, because
later dependencies refer to these identifiers.

Every checkbox has the same completion gate:

- Keep persistent bytes, backend operation counts, retry semantics, and
  deterministic draw/spawn order unchanged unless the checkbox explicitly says
  otherwise.
- Run the focused acceptance checks named by the checkbox.
- Run `make format`, inspect the resulting diff for unrelated changes, then run
  `make test-all`.
- Leave compatibility shims in place until the checkbox that explicitly removes
  them. Do not mix their removal into an earlier migration.
- Mark the checkbox complete only when its hard dependencies are complete and the
  diff can be explained without relying on a later checkbox.

`Depends on` identifies a semantic prerequisite. `Schedule after` only avoids
overlapping edits or review churn; it is not a blocker if the items are performed
in separate worktrees.

Coverage labels use these meanings:

- **Long-term regression** — keep the coverage after the refactor. This is the
  default for correctness matrices, liveness/cancellation tests, persistent and
  wire-format goldens, backend operation-count assertions, conformance suites,
  and deterministic corpus replay.
- **Migration guard** — keep it through the named migration endpoint, then remove
  it or replace it with narrower semantic coverage. Exact internal trace digests,
  test-location audits, and “old and new API agree” checks commonly belong here.
- **Mixed** — retain the semantic assertions long term, but remove compatibility-
  shim, exact-location, or exhaustive internal-trace assertions when the named
  cleanup lands.

Coverage is long-term unless a workstream is explicitly labeled otherwise.

### Priority 1 — Correctness and liveness

These are the highest-value items. Characterization and implementation
stay adjacent so protocol coverage starts paying for itself immediately.

#### F01 — Self-pruning background tasks

**Coverage:** Long-term regression. Keep the task-retention and shutdown-race tests.

- [x] **F01-A — Replace historical task vectors with live registries.** Use
  monotonically increasing IDs for waited and best-effort tasks, register before
  spawn, and let a completion guard remove its entry through a weak registry
  reference. Add test-only live counts. **Depends on:** nothing. **Accept when:**
  thousands of sequential completions leave both counts at zero and the existing
  shutdown/drop tests retain their behavior.

- [x] **F01-B — Harden completion/shutdown races.** Add deterministic cases for a
  task completing before, during, and after shutdown begins, including cancelled
  and resumed shutdown. Limit production changes to race fixes exposed by those
  tests. **Depends on:** F01-A. **Accept when:** every waited task is either joined
  or remains registered until completion, and both registries finish empty.

#### F02 — Validated delay and rate policy

**Coverage:** Long-term regression. Keep validation, unlimited-rate, and model-time
liveness coverage.

- [x] **F02-A — Make rate-limit states explicit.** Add
  `RateLimit::{Unlimited, PerSecond(NonZeroU32)}`, validate prefix depth and
  enabled retry timing, and make an unlimited object limiter skip backoff.
  **Depends on:** nothing. **Accept when:** paused-time tests cover unlimited,
  one-per-second, invalid construction, and the former zero-rate infinite wait.

- [x] **F02-B — Separate latency profiles from rate limits.** Introduce distinct
  provider-latency and write-rate configuration values, migrate built-in profiles,
  fake-server options, benchmarks, and all constructors, and preserve every
  existing nonzero profile. **Depends on:** F02-A. **Accept when:** profile
  equality tests and the model-time delay integration test pass with all features.

#### F03 — Cached-store correctness protocol

**Coverage:** Long-term regression. Keep the outcome/evidence/operation-count
matrix after the extraction.

- [x] **F03-A — Freeze the cache protocol matrix.** Add table-driven coverage for
  read coalescing and cancellation and for create/CAS/delete against matching,
  stale, absent, and L2-backed knowledge. Assert returned result, evidence
  watermark, next-read result, and backend operation count. **Depends on:**
  nothing. **Accept when:** the matrix captures all current success, conflict,
  unavailable, definitive-error, and cancellation outcomes.

- [x] **F03-B — Extract path admission and read flights.** Move path state,
  permits, leaders/waiters, cancellation, and weak-map cleanup to
  `cached_store/path_lane.rs`. **Depends on:** F03-A. **Accept when:** coalescing,
  leader cancellation, parallel-path, and path-state cleanup tests pass.

- [x] **F03-C — Extract cached knowledge.** Keep the exported observation and its
  private evidence cell with the facade; move present/absent state, erased decoded
  values, type checking, merge, peek, install, and invalidate to
  `cached_store/knowledge.rs`. **Depends on:** F03-A; may proceed alongside F03-B.
  **Accept when:** evidence merging, absence, observation lifetime, and wrong-codec
  tests are unchanged.

- [x] **F03-D — Centralize mutation reconciliation.** Add one
  `MutationOutcome` and one RAII mutation round for success, conflict, definite
  failure, uncertainty, and cancellation; migrate create, CAS, and delete.
  **Depends on:** F03-B and F03-C. **Accept when:** F03-A remains byte- and
  operation-count-identical and duplicated reconciliation branches are gone.

- [x] **F03-E — Isolate the persistent-cache bridge.** Move L2 lookup timeout,
  decoding/rejection, hit recording, fencing, replace/invalidate, and the semantic
  path lease to `cached_store/persistent_bridge.rs`. **Depends on:** F03-D.
  **Accept when:** corrupt candidate, slow lookup, reopened session, mutation invalidation,
  and promotion tests retain their operation counts.

#### F04 — Shard fold planning

**Coverage:** Mixed. Keep in-doubt, staging, capacity, and receipt behavior tests.
Private `FoldPlan` shape assertions are migration guards and may be removed after
F04-C if the same branches remain covered behaviorally.

- [x] **F04-A — Name round state without moving control flow.** Replace the result
  tuple and staged boolean with `Participation`, `MemberFold`, and `FoldPlan`, and
  add `staged_ids` and `is_dirty`. **Depends on:** nothing. **Accept when:** stage,
  skip, and in-doubt attribution regressions pass with persistence still inline.

- [x] **F04-B — Extract folding and capacity admission.** Move member ordering,
  ownership, logless exclusion, resolver calls, and capacity checks into async
  `fold_round -> FoldPlan`; it must perform no shard-store/CAS work. **Depends on:**
  F04-A. **Accept when:** plan-level tests cover stage+skip, same-key logless
  exclusion, and stage+capacity rejection.

- [x] **F04-C — Normalize persistence and shrink `run_shard`.** Add
  `PersistResult::{Landed, PreconditionMiss, InDoubt(staged_ids)}` and leave the
  driver with load, fold, persist, sticky-uncertainty update, retry, and deposit.
  **Depends on:** F04-B. **Accept when:** in-doubt followed by precondition or
  capacity rejection retains exactly the currently attributed member set.

#### F05 — Structural split phases and publication

**Coverage:** Long-term regression. Keep the durable phase-transition table and
split/recovery behavior tests. F05-E appears at its earliest legal point after
F16-D.

- [x] **F05-A — Replace the recovery out-parameter with phase types.** Add
  `PreparedSplit`, `ReadySplit`, and
  `SplitAttemptResult::{Completed, RetryCleanly,
  RecoveryRequired(ReadySplit)}`; make the durable ready transition consume the
  prepared value. Keep root/non-root outer flows duplicated for now.
  **Depends on:** nothing. **Accept when:** a transition table proves which failure points
  delete `Preparing` and which retain the typed `ReadySplit` witness.

- [x] **F05-B — Unify the root/non-root outer lifecycle.** Add a concrete
  `StructuralSplitAttempt` selected by a target enum and share prepare, topology
  join, coordinate, cleanup, topology leave, finalization, and recovery wake.
  Preserve storage-operation order and bytes. **Depends on:** F05-A.
  **Accept when:** root leaf, root index, non-root leaf, and already-settled split tests pass.

- [x] **F05-C — Extract separator queue ownership mechanically.** Move the pending
  queue and missing-separator computation into a concrete `SeparatorPublisher`,
  leaving the existing retry loop and parent-split behavior at the call site.
  **Depends on:** F05-B. **Accept when:** queue order, deduplication, missing-
  separator results, and lost-parent regressions are unchanged.

- [x] **F05-D — Move the publication driver and parent action.** Move CAS retry and
  publication into `SeparatorPublisher` and return a named parent-requires-split
  action rather than a callback trait. **Depends on:** F05-C. **Accept when:** lost
  parent CAS, unpublished sibling, retry timing, and cascading split ordering are
  unchanged.

#### F06 — S3 conditional mutation state machine

**Coverage:** Long-term regression. Keep provider-fact and conditional-mutation transition tables.

- [x] **F06-A — Normalize each S3 failure once.** Introduce a private provider-fact
  enum and one SDK-to-fact conversion for precondition, confirmed-not-applied,
  ambiguous/lost acknowledgement, retryable transport, and terminal failures.
  Leave retry policy outside it. **Depends on:** nothing. **Accept when:** HTTP
  status, service-code, and transport-error tables retain all current public error
  classifications.

- [x] **F06-B — Move conditional PUT policy into a pure state machine.** Store
  attempt count and `may_have_applied`, expose a pure event-to-action transition,
  and leave the async loop responsible only for send, sleep, and return. Remove
  superseded predicates. **Depends on:** F06-A. **Accept when:** tables cover
  ambiguity followed by precondition, retry exhaustion, terminal failure, and
  success without increasing normal-path operations.

#### F07 — Public transaction attempt driver

**Coverage:** Long-term regression. Keep lifecycle balance, cancellation,
body-error validation, and wound-restart tests.

- [x] **F07-A — Add attempt-lifecycle regressions.** Instrument begin/end balance
  in tests and cover cancellation while the body, read validation, wound restart,
  and commit are pending; also cover a body error that must be read-validated.
  Make no structural production change yet. **Depends on:** nothing.
  **Accept when:** every case leaves no active engine attempt or abort task behind.

- [x] **F07-B — Bind the engine handle to its abort guard.** Introduce one private
  `AttemptResources` state that owns both or neither, and migrate the current loop
  without extracting policy yet. **Depends on:** F07-A. **Accept when:** no
  independent `Option<EngineTransaction>`/guard pairing remains and all lifecycle
  regressions are unchanged.

- [x] **F07-C — Extract `AttemptDriver` transitions.** Move access installation,
  body-error validation, wound restart, commit, and finish into named methods;
  reduce `tx_impl` to closure execution and retry policy. **Depends on:** F07-B.
  **Accept when:** duplicate begin/reset branches are gone and cancellation/end
  accounting remains balanced at every tested await point.

#### F08 — Single-source recovery manifests

**Coverage:** Long-term regression. Keep exhaustive projection and recovery-field
preservation tests.

- [x] **F08-A — Centralize `TxRecoveryManifest` projection.** Add `apply_to` and
  `from_log`, then migrate pending, refresh, abort, and observed-abort paths. The
  projection must not alter ID, status, timestamp, or committed writes.
  **Depends on:** nothing. **Accept when:** an exhaustive explicit-field round trip passes
  along with the existing manifest preservation and refresh tests.

- [x] **F08-B — Reuse manifest projection in collection commit.** Make
  `CollectionAttempt` produce named pending and committed projections and preserve
  existing backreferences during pending updates. **Depends on:** F08-A.
  **Accept when:** renewed-attempt, prepared-root, committed-manifest, and GC recovery tests
  pass without direct recovery-field assignment outside the projection.

### Priority 2 — Core storage, transaction, and backend boundaries

These workstreams follow the original P2 ranking, except where a dependency
or migration guard must land first.

#### F09 + F30 — Persistent cache, protected by corpus replay

**Coverage:** Long-term regression. Keep format goldens, committed-corpus replay,
decoder mapping, and crash/corruption oracles. The one-time “mechanical move”
diff checks do not need to become tests.

- [x] **F09-A — Freeze persistent bytes and recovery.** Add fixed vectors for one
  header, slot, marker, and record using fixed geometry and identity, plus clean
  and unclean reopen assertions. **Depends on:** nothing. **Accept when:** existing
  corruption, crash, and ring-reuse tests agree with the byte vectors.

- [x] **F30-A — Freeze existing corpus traces.** Record an expected trace digest
  for every committed disk-cache corpus input in addition to the repeatability
  assertion. **Depends on:** nothing. **Accept when:** every corpus entry matches
  its baseline under the simulation build.

- [x] **F30-B — Type the command decoder.** Add a stable `CommandKind` with ten
  variants and one `from_byte` preserving `byte % 10`; retain the emitted numeric
  operation code. **Depends on:** F30-A. **Accept when:** an exhaustive 0-255
  mapping test and every trace digest pass.

- [x] **F09-B — Extract the binary format.** Move constants, geometry, layout,
  alignment, digests, sizing, and encode/decode to `disk_cache/format.rs`, with no
  media I/O or policy. **Depends on:** F09-A. **Accept when:** every F09-A golden is
  byte-identical.

- [x] **F09-C — Extract disk and recovery mechanics.** Move disk state, slots,
  records, writer state, open/recovery scanning, segment publication, and raw
  media I/O to `disk_cache/disk.rs`. **Depends on:** F09-B. **Accept when:** clean
  and unclean reopen, identity mismatch, corruption, segment reuse, and full
  bucket tests pass.

- [x] **F09-D — Extract fences and admission.** Put epoch guards in `fence.rs` and
  the hit filter, payload reservations, and optional-work admission in
  `admission.rs`. **Depends on:** F09-B and F03-E. **Accept when:** epoch
  cancellation, second chance, reset, and oversize admission tests pass.

- [x] **F09-E — Move the worker mechanically.** Put work messages, queues,
  lifecycle, and the existing worker loop in `worker.rs`, leaving behavior and
  manual bookkeeping unchanged and the facade/configuration in `disk_cache.rs`.
  **Depends on:** F09-C and F09-D. **Accept when:** the diff is an ownership move
  and shutdown, failure, pressure, promotion, and format tests are unchanged.

- [x] **F09-F — Give worker branches named handlers and RAII reservations.** Move
  each work variant to a handler returning `ControlFlow` and make optional-work
  reservations balance bytes, counters, and fences on every exit. **Depends on:**
  F09-E. **Accept when:** cancellation/error injection at every handler exit leaves
  accounting at zero and existing worker behavior is unchanged.

- [x] **F30-C — Extract harness state and handlers.** Make `HarnessState` own
  cache/media/identity/sequence/oracle/events and give each command a short named
  handler; leave `run` with initialization, dispatch, close, and final assertion.
  **Depends on:** F30-B and F09-F. **Accept when:** trace digests and fabricated-
  record/out-of-bounds oracles remain identical.

#### F10 — Transaction-log model, codec, and store

**Coverage:** Mixed. Keep codec goldens, malformed-wire, lifecycle,
operation-count, and pagination tests. Re-export/shim compile checks live only as
long as those compatibility shims.

- [x] **F10-A — Extract the transaction-log domain model.** Move status, log,
  write, lock, and collection-change types to `transaction/model.rs`, retaining
  existing `glassdb_storage` re-exports. **Depends on:** nothing. **Accept when:**
  downstream crates compile unchanged and this diff contains no codec/store logic
  changes.

- [x] **F10-B — Extract the canonical codec.** Add `transaction/codec.rs` with
  encode, decode, status-only decode, and one helper per repeated field; make
  infallible protobuf builders infallible and route `txobject` through the codec.
  **Depends on:** F10-A. **Accept when:** golden bytes, every status and lock shape,
  relocation checks, and malformed-wire tests pass.

- [x] **F10-C — Extract transaction-log persistence.** Move `TLogger`, lifecycle
  validation, status caching, CRUD, and listing to `transaction/store.rs`; remove
  or reduce `tlogger.rs` to a re-export shim. **Depends on:** F10-B. **Accept when:**
  lifecycle operation counts, final cache, pending freshness, and pagination are
  unchanged.

- [x] **F10-D — Move `TValue` to its transaction owner.** Define it beside the
  monitor/reader code, migrate all uses, retain a compatibility re-export if
  semver requires it, and remove storage ownership. **Depends on:** F10-A.
  **Accept when:** monitor/read tests and public-crate compilation pass with no storage
  implementation depending on `TValue`.

#### F12 — Opaque validation evidence (promoted before F11)

**Coverage:** Mixed. Keep point/scan validation and phantom tests. Old/new
adapter parity and compatibility-compile checks are migration guards through
F12-C.

- [x] **F12-A — Encapsulate point-read evidence.** Add field-private
  `ReadEvidence` and an evidence-bearing `ReadOutcome`/`ReadAccess` construction
  path; migrate `glassdb` while retaining the old physical fields and constructors
  as deprecated adapters. **Depends on:** nothing. **Accept when:**
  `crates/glassdb/src` contains no `LeafObservation` reference, compatibility
  callers still compile, and point-read validation regressions pass.

- [x] **F12-B — Encapsulate scan evidence and narrow `Engine`.** Add field-private
  `ScanEvidence`, a consuming result-to-access conversion, and a narrowed scan
  entry point; migrate `glassdb` while retaining the old holder/cap controls as a
  deprecated adapter. **Depends on:** F12-A. **Accept when:** manual scan-access
  assembly is gone from `glassdb`, compatibility callers compile, and range,
  overlay, phantom, and algorithm scan-validation regressions pass.

#### F11 — Transaction data and catalog overlays

**Coverage:** Long-term regression. Keep public KV, scan-overlay, retry-reset,
and collection lifecycle behavior tests; ownership-only diff checks are one-time
review aids.

- [x] **F11-A — Extract `DataOverlay`.** Move staged key/value state, point reads,
  scans, data reset, and data-access extraction to `tx/data.rs`; keep facade method
  signatures. **Depends on:** nothing. **Schedule after:** F12-B to avoid
  overlapping evidence edits. **Accept when:** public KV, scan overlay, retry
  reset, and access-validation tests pass.

- [x] **F11-B — Extract `CatalogOverlay`.** Move catalog snapshots, lifecycle
  changes, created/dropped sets, and reservations to `tx/catalog.rs`, and replace
  boolean creation mode with `CreateMode`. **Depends on:** F11-A. **Accept when:**
  collection create/drop/list/retry tests pass with no public behavior change.

- [x] **F11-C — Simplify the transaction facade.** Give each overlay ownership of
  reset/access serialization, resolve a child collection once instead of exists+
  open, and leave `TransactionInner` with abort state plus the two overlays.
  **Depends on:** F11-B. **Accept when:** duplicate resolution and reset branches
  are gone and collection backend-operation counts are unchanged.

#### F13 — Observation-bound leaf edits

**Coverage:** Mixed. Keep topology, stale-observation, and CAS tests.
Loose-wrapper and public-field compatibility checks remain only through F13-D.

- [x] **F13-A — Introduce `LeafEdit` additively.** Bind one observed node to its
  exact observation, expose immutable path/topology and bounded shard/lock
  mutation, and add `commit_leaf(LeafEdit)` while retaining the old wrapper.
  **Depends on:** nothing. **Accept when:** topology, immutable path, successful
  edit, and stale-observation conflict tests pass.

- [x] **F13-B — Migrate storage-side leaf-edit callers.** Convert storage internals,
  fixtures, and tests to `LeafEdit` while retaining the loose wrapper for
  transaction callers. **Depends on:** F13-A. **Accept when:** storage production
  code uses only bound edits and its root, leaf, topology, and CAS tests pass.

- [x] **F13-C — Migrate transaction callers and deprecate the loose API.** Convert
  coordinator, GC, resolver, locker, and algorithm call sites while retaining the
  public fields/wrapper as deprecated compatibility surfaces. **Depends on:**
  F13-B. **Accept when:** workspace production code has no loose call or direct
  field access and all focused transaction tests pass.

#### F14 — Lock-state representation

**Coverage:** Mixed. Keep wire goldens, invalid-state, transition, and conflict
tests. Alias/raw-field compatibility checks remain only through F14-G.

- [x] **F14-A — Centralize neutral holder state and wire conversion.** Put
  field-private `HolderSet`/`LockState`, ordering, protobuf mapping, and legacy
  empty normalization in `lock.rs`; remove duplicate codec mappings.
  **Depends on:** F10-B. **Accept when:** the wire matrix covers unknown/none/read/write/create,
  duplicates, cardinality, and ordering while golden bytes remain unchanged.

- [x] **F14-B — Add scope-specific non-entry wrappers.** Introduce
  `SharedExclusiveLock` and `ExclusiveGate`, temporarily alias `NodeLock`, and
  migrate node and collection internals. **Depends on:** F14-A. **Accept when:**
  invalid scope combinations cannot decode/construct and goldens are unchanged.

- [x] **F14-C — Add the entry-lock transition API.** Introduce `EntryLockState`
  and `ShardEntry` query/acquire/replace/release methods while retaining raw fields
  temporarily. **Depends on:** F14-A. **Accept when:** shared, write, create,
  release, idempotence, and canonical encoding transitions are covered.

- [x] **F14-D — Migrate lock coordination modules.** Convert `tlocker`,
  `node_locking`, `collection_coordination`, and `shard_coord` from direct field
  mutation to the API. **Depends on:** F14-C. **Accept when:** those files contain
  no direct entry-lock fields and focused conflict/retry tests pass.

- [x] **F14-E — Migrate algorithm and key resolvers.** Convert `algo`,
  `key_resolver`, and `key_state_resolver`, retaining raw compatibility fields for
  split and GC. **Depends on:** F14-D. **Accept when:** those modules have no direct
  field access and inline, tombstone, validation, and effective-writer tests pass.

- [x] **F14-F — Migrate split, GC, and remaining tests.** Convert the remaining
  production and fixture construction sites to `EntryLockState` without changing
  the representation yet. **Depends on:** F14-E. **Accept when:** repository-wide
  search finds no direct raw-field read or mutation outside `shard.rs` compatibility
  code.

#### F15 — Typed physical object paths

**Coverage:** Mixed. Keep byte goldens, parser rejection, round-trip, and
classification tests. Remove superseded string-wrapper checks with the wrappers.

- [x] **F15-A — Add validated path components.** Introduce `DbRoot`, `NodeToken`,
  and `StructuralRecordId` with `TryFrom`, `Display`, and maximum encoded lengths.
  **Depends on:** nothing. **Accept when:** boundary tables,
  random-token round trips, and existing path goldens pass unchanged.

- [x] **F15-B — Add the canonical `ObjectPath` codec.** Split private tree,
  transaction, and structural codecs and implement one `ObjectPath` `Display` and
  `TryFrom<&str>`; make existing parsers delegate. **Depends on:** F15-A.
  **Accept when:** every constructible variant round-trips and all malformed/shard-mismatch
  cases retain their classification.

- [x] **F15-C — Migrate storage path ownership.** Use typed components and
  `ObjectPath` throughout storage constructors, codecs, listing, and routing,
  updating transaction callers atomically where signatures change. **Depends on:**
  F15-B. **Accept when:** storage has no independent transaction or structural-log
  parser and all path bytes remain golden-identical.

- [x] **F15-D — Migrate transaction path ownership.** Convert transaction GC,
  splitting, locking, monitoring, and engine wiring to the typed components,
  leaving raw strings only at backend boundaries. **Depends on:**
  F15-C. **Accept when:** transaction production code contains no physical-path
  parser and its operation-count tests are unchanged.

- [x] **F15-E — Remove benchmark path parsing.** Make backend breakdown classify
  `ObjectPath`, delete its marker parser, and remove redundant public path helpers.
  **Depends on:** F15-D. **Accept when:** classification covers all
  variants and misleading embedded markers.

- [x] **F15-F — Carry parsed paths through storage.** Introduce one storage key
  carrying both the canonical backend string and its `ObjectPath`; make typed
  cache codecs consume the parsed path, retain it in observations, and parse
  backend listings once at ingress. Validate path/body identity before encoded
  mutations. **Depends on:** F15-E. **Accept when:** constructed paths are never
  reparsed, listed paths are parsed once, and transaction and structural-log
  path/body mismatches are rejected without changing persistent bytes.

#### F16 — Node and structural-log stores

**Coverage:** Mixed. Keep store, routing, participant, and pagination behavior
tests. Temporary façade-delegation checks can retire when F16-D removes that
façade path.

- [x] **F16-A — Extract `NodeStore`.** Move node codec and node/root/leaf CRUD,
  including `LeafEdit`, to `node_store.rs`; keep `ShardStore::nodes()` delegation.
  **Depends on:** F13-C and F15-D. **Accept when:** hot/current reads, root CRUD,
  pagination, and topology-preserving leaf CAS pass.

- [x] **F16-B — Extract `StructuralLogStore`.** Move its codec, path/participant
  validation, CRUD, and paginated listing to `structural_log_store.rs`; delegate
  temporarily. **Depends on:** F15-D and F16-A for conflict-free file movement.
  **Accept when:** scoped pagination, participant mismatch, update conflict, and
  deletion tests pass.

- [x] **F16-C — Narrow `TreeRouter` to `NodeStore`.** Change its dependency and
  retain compatibility construction only at outer wiring. **Depends on:** F16-A.
  **Accept when:** router tests pass and it has no structural-log-capable handle.

- [x] **F16-D — Wire structural logs explicitly.** Give split/recovery and GC a
  `StructuralLogStore`, update composition fixtures, and remove structural-log
  methods from the old facade after callers migrate. **Depends on:** F16-B.
  **Accept when:** split recovery, participant cleanup, GC, pagination, and restart tests
  pass with no split protocol change in this diff.

#### F05 — Deferred P1 completion: structural recovery

**Coverage:** Long-term regression. Keep orphan recovery, writer fencing,
deferral, and roll-forward tests.

- [x] **F05-E — Extract structural recovery behind `Splitter`.** Move log scans,
  record recovery, source-writer fencing, and participant settlement to a concrete
  `StructuralRecovery`; keep `Splitter::start` as facade/loop owner. **Depends on:**
  F05-D and F16-D. **Accept when:** startup orphan recovery, live-writer deferral,
  roll-forward, and aborted-writer fencing regressions pass.

#### F17 — Shared B-link traversal

**Coverage:** Long-term regression. Keep the stale-root, stale-parent, right-hop,
freshness, and sibling-order matrix.

- [x] **F17-A — Add the traversal matrix.** Exercise every router entry point with
  a stale root, stale parent, and interior right hop, recording path and cache-hit/
  freshness behavior. **Depends on:** F16-C. **Accept when:** the matrix passes
  before production traversal moves.

- [x] **F17-B — Introduce `DescentCursor`.** Carry prefix, requirement, current
  location, and accumulated hit state; migrate leaf lookup/bootstrap and the old
  descend/right-step helpers. **Depends on:** F17-A. **Accept when:** lookup rows
  and backend-currentness assertions remain unchanged.

- [x] **F17-C — Reuse the cursor for topology queries.** Express reachability and
  parent lookup as stopping policies and remove their independent loops.
  **Depends on:** F17-B. **Accept when:** root, absent token, stale parent, and interior-hop
  cases agree across both queries.

- [x] **F17-D — Extract `LeafChain`.** Centralize right-sibling iteration for next,
  bounded leaves, full leaves, and grouping scans without changing return types.
  **Depends on:** F17-B. **Accept when:** ordered multi-leaf, upper-bound, stale-
  link, and cache-hit aggregation tests pass.

#### F18 — Explicit dedup key machine

**Coverage:** Long-term regression. Keep FIFO, handoff, cancellation, one-driver,
and seeded state-machine coverage.

- [x] **F18-A — Extract queue and compatible-batch mechanics.** Add a private
  `KeyQueue` backed by `VecDeque` and centralize enqueue, batch formation, requeue,
  and close without changing the public API. **Depends on:** nothing.
  **Accept when:** FIFO, compatibility, merge, and requeue tests pass.

- [x] **F18-B — Introduce explicit phases, events, and actions.** Model idle,
  driven, handoff/completing, and closed phases; migrate round completion and
  driver `Drop`; perform wake/spawn/delivery actions after releasing the lock.
  **Depends on:** F18-A. **Accept when:** transition tests prove one driver, no
  orphaned waiter, correct handoff, and unchanged uncontended task count.

- [x] **F18-C — Add deterministic cancellation model coverage.** Generate seeded
  enqueue, waiter cancellation, driver drop, completion, and close events and
  check invariants after each transition. **Depends on:** F18-B. **Accept when:**
  fixed regression seeds and a bounded multi-seed simulation run pass.

#### F19 — Complete shard-lock receipts

**Coverage:** Long-term regression. Keep mixed-strength receipt, cancellation,
release, and diagnostic-boundary coverage.

- [x] **F19-A — Thread `ShardLockReceipt` directly into `LockedTx`.** Carry the
  leaf observation and held strengths through shard outcomes and construct groups
  from them; remove `held_membership` and keep `tlocks` only for cleanup, release,
  fallback, and diagnostics. **Depends on:** F04-C. **Accept when:** a point write
  plus scan records both strengths, `held_membership` is gone, structural search
  finds no successful receipt-building lookup in `tlocks`, and cancellation and
  snapshot tests remain unchanged.

#### F20 — Validated `Algo::Handle` transitions and backoff

**Coverage:** Long-term regression. Keep transition legality and configured
paused-time backoff tests.

- [x] **F20-A — Replace correlated flags with validated transitions.** Introduce
  `AttemptPhase` and `ReadValidationMode`, plus `engage`, `commit`,
  `force_locked_reads`, `renew`, and `needs_abort`; forbid direct field assignment.
  **Depends on:** nothing. **Accept when:** a transition table covers direct
  commit, optimistic retry, engagement, wound renewal, abort need, and illegal
  reset after commit.

- [x] **F20-B — Inject configured acquisition backoff.** Pass the configured
  retry policy or backoff factory from `Engine` into `Algo::begin`, leaving other
  coordination backoffs with their current owners. **Depends on:** F20-A.
  **Accept when:** a paused-time forced-conflict test observes the configured initial and
  maximum intervals.

#### F21 — Direct-commit implementation unit

**Coverage:** Mixed. Direct-commit behavior tests stay long term. Test
name/count/location checks in F21-C are migration-only audit steps.

- [x] **F21-A — Move direct-commit vocabulary mechanically.** Create
  `algo/direct_commit.rs` and move attempts, eligibility/predecessor types,
  helpers, and resolver while leaving execution fields/methods on `Algo`.
  **Depends on:** nothing. **Accept when:** the diff is ownership-only and every
  direct eligibility/fallback test is unchanged.

- [x] **F21-B — Introduce the concrete `DirectCommit` collaborator.** Move its
  resolver, coordinator, inline policy, hints, GC hint clone, and counters behind
  `try_commit`; leave general locked-path ownership on `Algo`. **Depends on:**
  F21-A and F20-A. **Accept when:** normal, uncertain CAS, replay, same-key loser,
  fallback, and statistics tests retain exact behavior.

- [x] **F21-C — Relocate direct-path tests.** Move only direct-specific fixtures
  and tests beside the collaborator with minimal `pub(super)` support and no
  production changes. **Depends on:** F21-B. **Accept when:** test names/counts
  remain present and general locked/read-validation tests stay in `algo.rs`.

#### F22 — Effective-writer resolution

**Coverage:** Long-term regression. Keep the full value/holder state matrix and
fast-path operation-count assertion.

- [x] **F22-A — Add the resolution behavior matrix.** Cover absent, external,
  inline, and tombstone current values crossed with pending, committed, and
  aborted exclusive holders, shared readers, own-holder exclusion, and invalid
  exclusive cardinality. Assert writer and holder projections, deletion, cache
  evidence, and operation counts. **Depends on:** nothing. **Accept when:** the
  matrix passes before production code moves.

- [x] **F22-B — Share the exclusive-holder resolution core.** Return one named
  effective writer/value/deleted/cache result and express `resolve_writer`,
  `resolve_holders`, and `entry_exists` as projections; only holder resolution may
  extend it with compatible shared holders. **Depends on:** F22-A. **Accept when:**
  the writer-only shared-reader path performs no added status/backend lookup and
  the full behavior matrix is unchanged.

#### F23 — Wire-size split budgets

**Coverage:** Mixed. Keep size, varint-boundary, and exact-limit tests.
Compatibility checks for the old field name remain through F23-D.

- [x] **F23-A — Clarify and validate the current shared soft limit.** Document the
  public `leaf_max_bytes` field as applying to both node kinds for compatibility,
  add checked construction/validation at every configuration boundary, and reject
  headroom above the hard cap. **Depends on:** nothing. **Schedule after:** F14-B
  to avoid overlapping node edits. **Accept when:** leaf/index boundaries and
  invalid headroom are covered without breaking downstream struct literals.

- [x] **F23-B — Add codec-owned size calculations.** Expose maximum encoded
  lengths from `TxId` and `NodeToken` and add worst-case leaf-entry and parent-
  separator size functions. **Depends on:** F15-A and F14-F. **Accept when:**
  predicted and actual protobuf sizes agree at all varint boundaries and maximum
  component sizes.

- [x] **F23-C — Remove synthetic budget probes.** Rewrite key/entry admission to
  use codec size functions, remove the fake transaction and token, and use checked
  limits. **Depends on:** F23-A and F23-B. **Accept when:** exact-limit and
  one-byte-over tests build real leaf and index nodes, including the maximum key.

#### F24 — Executable backend listing contract

**Coverage:** Mixed. The conformance suite and request validation stay
permanently. Compatibility forwarding and old-signature compile checks remain
only through F24-G.

- [x] **F24-A — Centralize list argument validation.** Add one shared validator
  for prefix, cursor, and limit rules and migrate memory, S3, and GCS, deleting
  provider copies. **Depends on:** nothing. **Accept when:** boundary tables return
  the same public error categories for every provider.

- [x] **F24-B — Add a reusable conformance suite.** Run identical ordering,
  pagination, empty-page, prefix-isolation, continuation, invalid-input, and
  duplicate-free traversal cases against memory, S3 fake, and GCS fake factories;
  keep transport tests local. **Depends on:** F24-A. **Accept when:** all three
  implementations instantiate and pass the same suite.

- [x] **F24-C — Bind cursors to their originating prefix.** Store prefix identity
  with the opaque provider token and centralize cursor/prefix validation.
  **Depends on:** F24-B. **Accept when:** reuse under another prefix is rejected and
  ordinary continuation succeeds for every backend.

- [x] **F24-D — Add `ListRequest` additively.** Introduce the validated value and a
  default request-taking trait entry point that delegates to the old method; do
  not migrate callers or providers in this diff. **Depends on:** F24-C.
  **Accept when:** constructor boundary tests pass and every existing backend compiles
  unchanged.

- [x] **F24-E — Migrate backend middleware.** Route delay, fault, hook, logger,
  recording, scheduled, and statistics decorators through `ListRequest` while
  providers and higher-level callers still use compatibility entry points.
  **Depends on:** F24-D. **Accept when:** every decorator preserves request fields,
  results, and operation counts in focused forwarding tests.

- [x] **F24-F — Migrate storage and transaction callers.** Construct validated
  requests at listing boundaries and remove raw prefix/cursor/limit assembly from
  production callers. **Depends on:** F24-E. **Accept when:** repository production
  code outside provider implementations has no call to the old signature and
  pagination/operation-count tests are unchanged.

#### F29 + F25 — Trace-protected entropy and latency migration

**Coverage:** Migration guard plus permanent core. Keep exact pre-migration trace
digests through F25-D, F29-K, and F31-D; afterwards retain same-seed replay,
seed-divergence, semantic event boundaries, and distribution vectors, while
exhaustive historical internal traces may retire.

- [x] **F29-A — Add a stable harness trace schema.** Instrument spawn decisions,
  entropy draws, client lifecycle, operations, nemesis/heal events, and final
  verification in one structured test-only trace without adding baselines yet.
  **Depends on:** nothing. **Accept when:** schema unit tests cover every event kind
  and enabling tracing does not change the existing operation stream.

- [x] **F29-B — Freeze tape-scheduled harness traces.** Record reviewed digests for
  representative corpus inputs covering normal operation, faults, crash/restart,
  healing, and final verification. **Depends on:** F29-A. **Accept when:** every
  tape baseline is byte-identical across repeat runs and its first/last events are
  asserted semantically.

- [x] **F29-C — Freeze PCT-scheduled harness traces.** Record reviewed digests for
  representative PCT seeds and change-point boundaries, including spawn order and
  entropy consumption. **Depends on:** F29-B. **Accept when:** same-seed traces are
  byte-identical, selected different seeds diverge, and existing PCT invariants
  still pass.

- [x] **F25-A — Provide one all-build entropy facade.** Expose byte filling and
  uniform-unit sampling from `glassdb-concurr`, migrate data IDs and retry jitter,
  and preserve their simulation draw count/order. **Depends on:** F29-C.
  **Accept when:** seeded vectors and frozen tape/PCT traces pass without baseline refresh.

- [x] **F25-B — Extract a validated lognormal latency distribution.** Sample from
  an injected uniform source, migrate delay middleware through an adapter for its
  current process RNG, and remove its local formula without changing the source
  yet. **Depends on:** F25-A. **Accept when:** deterministic distribution vectors,
  zero deviation/latency, invalid parameters, and model-time delay tests pass.

- [x] **F25-C — Move delay middleware to simulation-aware entropy.** Replace the
  process-RNG adapter with the shared facade. This item is an explicit exception
  to the global byte-identical-draw gate. **Depends on:** F25-B. **Accept when:**
  the first trace divergence is reviewed and explained, affected F29 tape/PCT
  baselines or corpora are updated in the same diff, and same-seed replay is stable.

- [x] **F25-D — Give the S3 fake explicit seeded entropy.** Add a seed/source
  option, use the shared distribution, and remove process RNG and duplicate
  lognormal code. This is also an explicit deterministic-stream migration.
  **Depends on:** F25-C. **Accept when:** equal seeds produce equal delays,
  different seeds diverge, failure injection stays deterministic, and any changed
  fake-server baseline is reviewed and updated deliberately.

#### F26 — Owned provider-fake lifecycles

**Coverage:** Long-term regression. Keep repeated start/stop, listener release,
thread/task cleanup, and provider behavior tests.

- [x] **F26-A — Own and stop the S3 fake thread.** Add shutdown signal and join
  handle, make accept cancellation-aware, and provide explicit shutdown plus
  bounded defensive `Drop` while retaining the public feature API. **Depends on:**
  nothing. **Schedule after:** F25-D to avoid overlapping fake-server edits.
  **Accept when:** repeated start/request/drop releases every listener and leaves
  no server thread.

- [x] **F26-B — Split S3 fake responsibilities.** Mechanically separate lifecycle,
  routing, parsing, object state, faults, and latency, keeping facade/options
  re-exports. **Depends on:** F26-A. **Accept when:** the fake-server feature and
  full S3 tests pass with no wire or behavior diff.

- [x] **F26-C — Extract and own the GCS test server.** Move it to test support,
  own its accept task and shutdown signal, and expose the factory used by backend
  conformance tests. **Depends on:** F24-B. **Accept when:** repeated construction/
  drop leaves no listener/task and all GCS tests pass.

#### F27 — Checked cache weight accounting

**Coverage:** Long-term regression. Keep overflow, underflow, replacement,
eviction, and oversized-singleton tests.

- [x] **F27-A — Centralize weight replacement in `usize`.** Use one checked helper
  for deletion, replacement, and eviction; assert removal cannot underflow; and
  define overflow as saturation followed by eviction/recomputation while
  retaining the oversized-single-MRU rule. **Depends on:** nothing.
  **Accept when:** tests cover grow, shrink, delete, invariant failure, `usize::MAX`,
  multi-entry overflow, and oversized singleton retention.

#### F28 — Honest iterator APIs

**Coverage:** Mixed. Keep plain-item ordering, lifetime, empty, and paging tests.
Old/new parity, deprecation, and legacy compile checks remain only through F28-C.

- [x] **F28-A — Add infallible iterator variants.** Expose plain-item key and
  collection iterators over the already-materialized data while retaining old
  fallible methods; share one underlying iteration implementation. **Depends on:**
  F11-C. **Accept when:** item order, ownership/lifetimes, and empty/paged results
  match the legacy APIs.

- [x] **F28-B — Deprecate fallible materialized iterators.** Update examples,
  internal callers, and docs to the infallible variants and add deprecation notes
  describing the replacement. **Depends on:** F28-A. **Accept when:** repository
  production code has no legacy call and compatibility tests still compile.

### Priority 3 — Organization and verification tooling

Production protocols and their protective coverage are in place before
these lower-risk module and test-layout changes begin.

#### F29 — Public simulation roles

**Coverage:** Mixed. Model, executor, oracle, replay, and semantic event tests stay
long term. Exact exhaustive internal traces follow the retirement rule defined
in the F29 + F25 workstream.

- [x] **F29-D — Extract `RunPlan` and `RunContext`.** Move immutable decoded
  configuration and owned run resources out of `run_generic` without moving
  client/nemesis behavior. **Depends on:** F29-C. **Accept when:** trace baselines
  are byte-identical and `run_generic` delegates setup/teardown through the types.

- [x] **F29-E — Extract `ClientRunner`.** Move client crash/restart task lifecycle,
  request streams, and result collection behind one concrete collaborator.
  **Depends on:** F29-D. **Accept when:** panic propagation, restart, cancellation,
  and client operation ordering match the frozen traces.

- [x] **F29-F — Extract nemesis behavior.** Move outage, fault, crash/restart, and
  heal actions to a focused module while leaving schedule selection in the
  harness. **Depends on:** F29-E. **Accept when:** fault-tape traces and healing
  order remain byte-identical.

- [x] **F29-G — Extract harness scheduling.** Move fuzz/tape/PCT schedule selection
  and decisions to a focused module without moving nemesis execution.
  **Depends on:** F29-F. **Accept when:** fixed seeds consume the same bytes and produce the
  same task-selection trace.

- [x] **F29-H — Extract the API program generator.** Move arbitrary input decoding
  and action generation without moving the model or executor. **Depends on:**
  F29-C. **Accept when:** every fixed byte input generates the identical action
  program.

- [x] **F29-I — Extract the exact API model.** Move pure model state and transition
  logic without moving action execution or final verification. **Depends on:**
  F29-H. **Accept when:** a transition-vector suite produces identical states and
  errors.

- [x] **F29-J — Extract the API action executor.** Move action handlers and the
  program loop behind typed step results, leaving final oracle checks in place.
  **Depends on:** F29-I. **Accept when:** per-step operation logs and all API corpus
  outcomes remain unchanged.

- [x] **F29-K — Extract the API oracle.** Move final-state reads, comparison, and
  diagnostics to a focused verifier with no execution responsibilities.
  **Depends on:** F29-J. **Accept when:** success and intentionally corrupted-model fixtures
  produce the same verdicts and diagnostics.

#### F31 — Runtime facade, scheduler, and executor

**Coverage:** Long-term regression. Keep native/simulation parity, scheduling,
wake-order, timeout, panic, and replay tests.

- [x] **F31-A — Extract simulation schedulers.** Move scheduler traits and FIFO,
  random, PCT, and replay implementations with their tests to
  `sim/scheduler.rs`, retaining compatibility re-exports. **Depends on:** F25-A
  and F29-C. **Accept when:** seeded selection and replay results are byte-identical.

- [x] **F31-B — Extract the simulation executor kernel.** Move runnable queues,
  timers, setup, draining, and the run loop to `sim/executor.rs`, retaining
  crate-visible entry points. **Depends on:** F31-A. **Accept when:** model time,
  wake order, panic propagation, step budget, and tape replay pass.

- [x] **F31-C — Isolate dedicated-task mechanics.** Move worker state, native
  thread lifecycle, and join/error propagation to `rt/dedicated.rs`, leaving `rt`
  as facade. **Depends on:** F31-B. **Accept when:** native and simulated success,
  panic, cancellation, and shutdown tests retain spawn order.

- [x] **F31-D — Split native and simulated runtime adapters.** Reduce `rt.rs` to
  facade exports and put clock/task adapters in `rt/native.rs` and `rt/sim.rs`;
  add parity coverage. **Depends on:** F31-C. **Accept when:** both build modes
  agree on time, spawn, entropy, and dedicated tasks with no corpus refresh.

#### F32 — Mixed benchmark phases

**Coverage:** Mixed. Keep CLI validation, cell enumeration/order, metrics,
serialized schema, and workload selection tests. Exact human-readable text
snapshots may retire after F32-D unless that text is an external contract.

- [x] **F32-A — Extract options and dimension generation.** Move CLI/config
  parsing, validation, and cell-dimension enumeration without changing scenario
  execution. **Depends on:** nothing. **Accept when:** snapshot tests enumerate the
  same cells in the same order for representative arguments.

- [x] **F32-B — Extract results and reporting.** Move counters, latency summaries,
  aggregation, and output formatting to a result module with fixed snapshot tests.
  **Depends on:** F32-A. **Accept when:** existing metrics and serialized/text
  output are byte-identical for fixed samples.

- [x] **F32-C — Extract scenario setup and settlement.** Move database seeding,
  split settlement, and teardown into named phases, leaving `run_cell` to invoke
  them. **Depends on:** F32-B. **Accept when:** setup object counts, split quiet-
  period behavior, and cleanup are unchanged.

- [x] **F32-D — Extract workload workers and shrink `run_cell`.** Move worker
  selection/loops to a workload module and leave cell execution with phase
  orchestration and measurement only. **Depends on:** F32-C. **Accept when:** fixed
  seeds select the same keys/operations and logical transaction counters agree.

#### F33 — Behavior-oriented integration-test targets

**Coverage:** Mixed. Every moved behavior test stays permanently.
Name/count/location and exactly-once discovery audits in F33-B/C are
migration-only.

- [x] **F33-A — Extract shared integration fixtures.** Move `PauseControl`, hook
  backends, builders, and common assertions to `tests/sim_support` or a focused
  support module without moving tests yet. **Depends on:** nothing. **Schedule
  after:** F07-C, F11-C, and F12-B to avoid churn in shared fixtures.
  **Accept when:** the original integration target passes unchanged and support
  exposes only required helpers.

- [x] **F33-B — Move basic, stats, and scan tests.** Mechanically create focused
  integration targets, preserving names, feature gates, and test bodies.
  **Depends on:** F33-A. **Accept when:** the moved test count/names match and each new target
  passes independently.

- [x] **F33-C — Move shutdown and cancellation tests and retire the monolith.**
  Move the remaining behavior groups, remove the emptied original file, and avoid
  semantic edits. **Depends on:** F33-B. **Accept when:** repository test discovery
  contains every old test exactly once and parallel full-suite behavior is stable.

### Release-gated cleanup

These removals require explicit breaking-release approval. They are last
because the compatible migrations above deliver value without waiting for
a release boundary. Remove only compatibility-specific coverage when a shim
goes away; retain the semantic regression suites named above.

#### Deferred public API removals

**Coverage:** Mixed. Remove deprecated-adapter compile/parity checks with the
corresponding shim, but retain opaque-evidence, leaf-edit, lock, split-budget,
listing, and iterator behavior tests.

- [x] **F12-C — Remove physical evidence adapters in a breaking release.** Remove
  the deprecated observation fields, constructors, and resolver controls once
  downstream migration is authorized. **Depends on:** F12-B and explicit approval
  for the next breaking release. **Accept when:** logical opaque evidence is the
  only public construction path and the migration note lists each removed surface.

- [x] **F13-D — Encapsulate `LoadedLeaf` in a breaking release.** Make fields
  private and delete the loose storage wrapper after downstream migration is
  authorized. **Depends on:** F13-C and explicit breaking-release approval.
  **Accept when:** only bound edits can reach leaf persistence and the migration
  note identifies every removed field and method.

- [x] **F14-G — Encapsulate the entry lock representation.** Store
  `EntryLockState` privately in `ShardEntry` and delete raw compatibility fields
  and constructors in a breaking release. **Depends on:** F14-F and explicit
  breaking-release approval. **Accept when:** invalid in-memory lock states cannot
  be constructed or encoded, malformed protobufs remain rejected, and the
  migration note lists the removed fields.

- [x] **F23-D — Rename and encapsulate split-policy fields in a breaking release.**
  Replace `leaf_max_bytes` with `node_soft_max_bytes`, make construction validated
  by default, and migrate downstream struct literals once the public break is
  authorized. **Depends on:** F23-C and explicit breaking-release approval.
  **Accept when:** no ambiguous field remains and the migration note includes the
  one-to-one replacement.

- [x] **F24-G — Make `ListRequest` the provider contract.** Migrate each backend
  implementation to the request-taking required method, then remove the old
  delegate and redundant validation in a breaking release. **Depends on:** F24-F
  and explicit breaking-release approval. **Accept when:** no old signature
  remains, the conformance suite passes, and external implementers have a
  migration note.

- [x] **F28-C — Remove legacy iterator wrappers in the next breaking release.**
  Delete impossible per-item errors and their adapters only after the release
  milestone authorizes the API break. **Depends on:** F28-B and explicit approval
  for the next breaking release. **Accept when:** public API tests use plain item
  types and the changelog/migration note identifies each removed method.

Most recommendations are behavior-preserving and do not merit an ADR. Add one
only if a refactor changes persistent bytes, backend operation counts, public
retry/rate semantics, or an architectural owner. If an ADR is needed, keep it to
that decision and its trade-offs rather than the file-move plan.

## File-by-file coverage appendix

The lists below account for all reviewed files. "No standalone finding" means the
file is cohesive or thin enough that changing it independently is not currently
worth the disruption. A file may still be a migration call site for a finding
owned elsewhere.

### `glassdb`

Finding-bearing files:

- `src/db.rs` — finding 7.
- `src/tx.rs` — findings 11 and 12; also the collection-resolution simplification.
- `src/iter.rs`, `src/collection.rs` — finding 28.
- `src/sim/api.rs`, `src/sim/harness.rs` — finding 29.
- `tests/integration.rs` — finding 33.

Reviewed with no standalone finding:

- `Cargo.toml`
- `benches/transactions.rs`
- `src/diagnostics.rs`
- `src/error.rs`
- `src/lib.rs`
- `src/scan.rs`
- `src/sim/cycle.rs`
- `src/sim/membership.rs`
- `src/sim/mod.rs`
- `src/sim/rmw.rs`
- `src/sim/slow_backend.rs`
- `src/stats.rs`
- `src/version.rs`
- `tests/api_sim.rs`
- `tests/collections.rs`
- `tests/concurrent_sim.rs`
- `tests/cycle_sim.rs`
- `tests/fuzz_corpus.rs`
- `tests/in_doubt.rs`
- `tests/membership_sim.rs`
- `tests/proptest_concurrent.rs`
- `tests/read_unavailable.rs`
- `tests/runtime_seam.rs`
- `tests/sim_support/mod.rs`
- `tests/transaction_body_policy.rs`

### `glassdb-trans`

Finding-bearing files:

- `src/access.rs`, `src/reader.rs`, `src/engine.rs` — finding 12.
- `src/algo.rs` — findings 20 and 21; transaction background-task call sites for
  finding 1.
- `src/collection_commit.rs`, `src/monitor.rs` — finding 8.
- `src/key_state_resolver.rs` — finding 22.
- `src/shard_coord.rs` — findings 4 and 19.
- `src/split.rs` — finding 5; migration call site for findings 15 and 16.
- `src/tlocker.rs` — finding 19; migration call site for finding 14.

Reviewed with no standalone finding:

- `Cargo.toml`
- `src/collection_catalog.rs`
- `src/collection_coordination.rs`
- `src/collections.rs`
- `src/collections/lifecycle.rs`
- `src/error.rs`
- `src/gc.rs`
- `src/key_resolver.rs`
- `src/lib.rs`
- `src/node_locking.rs`
- `src/wound_wait.rs`

`gc.rs`, `key_resolver.rs`, and `tlocker.rs` are large, but their production
responsibilities match the architectural owners in `docs/architecture.md`; they
were not flagged for size alone.

### `glassdb-storage`

Finding-bearing files:

- `src/cache.rs` — finding 27.
- `src/cached_store.rs` — finding 3.
- `src/collection_store.rs`, `src/lock.rs`, `src/node.rs`, `src/shard.rs` — finding
  14; `node.rs` also owns finding 23.
- `src/disk_cache.rs` — findings 3 and 9.
- `src/disk_cache/sim_harness.rs` — finding 30.
- `src/shard_store.rs` — findings 13 and 16.
- `src/tlogger.rs`, `src/txobject.rs` — finding 10; `tlogger.rs` also participates
  in finding 14.
- `src/tree_router.rs` — finding 17.

Reviewed with no standalone finding:

- `Cargo.toml`
- `benches/cache.rs`
- `src/cache_stats.rs`
- `src/disk_cache/file_media.rs`
- `src/disk_cache/media.rs`
- `src/disk_cache/sim_media.rs`
- `src/error.rs`
- `src/inline.rs`
- `src/lib.rs`
- `src/structlog.rs`
- `src/timeline.rs`
- `src/version.rs`
- `tests/fuzz_corpus.rs`

### `glassdb-concurr`

Finding-bearing files:

- `src/background.rs` — finding 1.
- `src/dedup.rs` — finding 18.
- `src/exec.rs`, `src/retry.rs` — findings 25 and 31.
- `src/rt.rs` — finding 31.

Reviewed with no standalone finding:

- `Cargo.toml`
- `src/lib.rs`
- `src/rng.rs`
- `src/shard.rs`
- `src/tape.rs`
- `tests/model_time_accelerated.rs`
- `tests/model_time_locked.rs`

`rng.rs` and `tape.rs` are focused primitives and are good implementation
building blocks for finding 25 rather than refactoring targets themselves.

### `glassdb-backend`

Finding-bearing files:

- `src/lib.rs`, `src/memory.rs` — finding 24.
- `src/middleware/delay.rs` — findings 2 and 25.

Reviewed with no standalone finding:

- `Cargo.toml`
- `src/middleware/fault.rs`
- `src/middleware/hook.rs`
- `src/middleware/logger.rs`
- `src/middleware/mod.rs`
- `src/middleware/recording.rs`
- `src/middleware/scheduled.rs`
- `src/stats.rs`
- `tests/model_time_delay.rs`

The fault and hook middleware are substantial but cohesive around their explicit
interception contracts.

### `glassdb-backend-s3`

Finding-bearing files:

- `src/lib.rs`, `src/tests.rs` — findings 6 and 24.
- `src/fake_server.rs` — findings 25 and 26.

Reviewed with no standalone finding:

- `Cargo.toml`
- `src/dns.rs`
- `src/tuned_http.rs`

### `glassdb-backend-gcs`

Finding-bearing files:

- `src/lib.rs` — finding 24.
- `src/tests.rs` — findings 24 and 26.

Reviewed with no standalone finding:

- `Cargo.toml`

### `glassdb-data`

Finding-bearing files:

- `src/entropy.rs` — finding 25.
- `src/paths.rs` — findings 15 and 23.

Reviewed with no standalone finding:

- `Cargo.toml`
- `src/base64.rs`
- `src/collection_id.rs`
- `src/database_id.rs`
- `src/lib.rs`
- `src/txid.rs`

### `glassdb-proto`

Reviewed with no standalone finding:

- `Cargo.toml`
- `src/bin/regen.rs`
- `src/generated.rs`
- `src/lib.rs`

`generated.rs` should continue to change only through the regeneration path.

### `glassdb-bench-scale`

Finding-bearing files:

- `src/backend_breakdown.rs` — finding 15.
- `src/bin/perfbench/mixed.rs` — finding 32.

Reviewed with no standalone finding:

- `Cargo.toml`
- `src/bench.rs`
- `src/bin/backendbench.rs`
- `src/bin/perfbench/backend.rs`
- `src/bin/perfbench/contention.rs`
- `src/bin/perfbench/inline_pressure.rs`
- `src/bin/perfbench/main.rs`
- `src/lib.rs`
- `src/run.rs`
- `tests/model_time_bench.rs`

### `glassdb-bench-score`

Reviewed with no standalone finding:

- `Cargo.toml`
- `src/bin/autoresearch/main.rs`
- `src/bin/autoresearch/metrics.rs`
- `src/bin/autoresearch/workloads.rs`

The crate is small and its command, metrics, and workload boundaries are already
clear.

### Fuzz workspace

Reviewed with no standalone finding:

- `fuzz/Cargo.toml`
- `fuzz/fuzz_targets/api_correctness.rs`
- `fuzz/fuzz_targets/concurrent_tx.rs`
- `fuzz/fuzz_targets/cycle.rs`
- `fuzz/fuzz_targets/disk_cache.rs`
- `fuzz/fuzz_targets/membership.rs`

These are intentionally thin entry points over the simulation harnesses. Findings
29 and 30 should preserve their input mappings and corpus replay behavior.
