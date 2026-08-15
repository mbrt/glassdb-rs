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
