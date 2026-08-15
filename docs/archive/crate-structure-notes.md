# Crate structure implementation summary

> Archived implementation record. Preserved for historical reference; not maintained.

This is the condensed outcome record for the
[crate-structure review](crate-structure-review.md). It records the durable
boundaries and verification owners after the refactor, rather than the sequence
of migration steps. Tests named below are the primary behavioral coverage, not
an exhaustive inventory.

## Retained invariants

- Conflicts and inconsistent snapshots do not escape into user code. Conditions
  derived from transaction reads return errors from the transaction body so the
  attempt can be read-validated and retried.
- Read-only transactions normally take no locks and perform no writes. Warm
  single-value transactions remain on the one-backend-operation path.
- Lock, CAS, transaction-log, structural-log, and recovery boundaries preserve
  their ordering and error classifications.
- Persistent bytes, object paths, evidence ordering, and L1/L2 fencing and
  publication order remain stable unless an explicit migration says otherwise.
- Await order, key/member order, entropy draws, task selection, and simulation
  input mappings remain deterministic where they are part of replay behavior.
- Cancellation, drop, shutdown, and background-task ownership have bounded
  lifetimes and complete without holding protocol mutexes across effects.
- Validated configuration and checked arithmetic make invalid limits, rates,
  paths, wire budgets, and cache weights unrepresentable or explicitly rejected.
- Migration-only parity tests and compatibility shims were removed after their
  callers reached the new endpoint. Durable tests assert interfaces and behavior.

## Finding outcomes

| ID | Current implementation | Primary durable verification |
| --- | --- | --- |
| F01 | Background work uses live task registries with completion and shutdown race handling. | Registry reclamation and shutdown-after-many-commits tests. |
| F02 | Rate limits and provider latency are validated separately; latency sampling uses the shared `Lognormal`. | Unlimited/invalid-rate and deterministic delay tests. |
| F03 | `CachedStore` is split into path-lane, knowledge, mutation, and persistent-cache responsibilities. | Read/mutation protocol matrices, cancellation, and L2 fencing tests. |
| F04 | Shard coordination produces named fold plans, participation, and persistence outcomes. | Mixed-member, conflict, retry, and in-doubt coordination tests. |
| F05 | Structural splits use typed phases, shared lifecycle code, a separator publisher, and explicit recovery. | Root/non-root transition, recovery, fencing, and publication tests. |
| F06 | S3 errors are classified once and conditional writes use a pure transition machine. | Classifier tables and lost-ack conditional-write integration tests. |
| F07 | `AttemptDriver` binds an engine attempt to its abort/cancellation lifecycle. | Cancellation-boundary, wound-restart, and balanced begin/end tests. |
| F08 | Recovery-manifest projection is centralized instead of copied into each log lifecycle. | Abort, pending, refresh, observed-abort, and GC recovery tests. |
| F09 | Disk-cache format, media, fencing, admission, and worker ownership are separate and RAII-bound. | Reopen, corruption, cancellation, reservation, and shutdown tests. |
| F10 | Transaction-log model, codec, persistence, and `TValue` ownership have focused modules. | Golden codec, lifecycle, recovery, and conditional-store tests. |
| F11 | Transaction data staging and collection-catalog staging use separate overlays. | Data/collection composition and retry behavior tests. |
| F12 | Read and scan evidence is opaque across the public and transaction layers. | Inconsistent-snapshot retry and phantom-validation tests. |
| F13 | `LeafEdit` is bound to the observation and path it will compare-and-swap. | Stale-edit, wrong-path, and successful bounded-edit tests. |
| F14 | Lock wire state is neutral while scope-specific validated states own policy. | Decode rejection, canonical encoding, acquisition, and release matrices. |
| F15 | Storage carries a validated `ObjectPath` vocabulary instead of raw path strings. | Path parsing, scope, round-trip, and invalid-path tests. |
| F16 | Node persistence and structural-log persistence are separate capabilities; `ShardStore` currently forwards node operations to `NodeStore`. | Node-store and structural-log lifecycle/pagination tests. |
| F17 | `DescentCursor`, named stopping policies, and `LeafChain` centralize B-link traversal. | Stale-hop, cache-hit, no-prefetch, dangling-link, and ordering tests. |
| F18 | `KeyQueue` and `KeyMachine` own per-key batching, drivers, deferred effects, and handoff. | Focused transition, FIFO, cancellation, close, drop, and liveness tests. |
| F19 | Coordination returns a complete `ShardLockReceipt`, which `LockedTx` carries directly. | Commit, fallback, partial acquisition, cancellation, and release tests. |
| F20 | Algorithm attempts use validated phase transitions and configured acquisition backoff. | Transition tables, retry timing, wound cleanup, and renewal-race tests. |
| F21 | Direct commit has its own vocabulary, collaborator, module, and behavior tests. | One-CAS, operation-count, fallback, conflict, and lost-ack tests. |
| F22 | Effective-writer resolution has one shared core used by commit paths. | Direct/logged resolution and overwrite-order tests. |
| F23 | `SplitPolicy` is validated at construction and uses checked wire-size budgets. | Real Prost boundary, varint, headroom, and oversized-key tests. |
| F24 | Listing uses a nonzero limit, prefix-bound opaque cursors, shared provider validation, and common conformance coverage. | Memory, S3, and GCS pagination/cursor conformance tests. |
| F25 | Entropy is simulation-aware, latency sampling is shared, and Fake S3 owns a seeded stream. | Distribution vectors, draw-order checks, and same-seed replay tests. |
| F26 | Provider fakes own their lifecycle and separate routing, parsing, state, faults, and latency. | Explicit shutdown/join, panic propagation, fault ordering, and parity tests. |
| F27 | Cache weights use checked `usize` arithmetic. | Overflow, accounting, and eviction regressions. |
| F28 | Already-materialized results expose infallible iterators; legacy wrappers are gone. | Public iterator ordering and integration tests. |
| F29 | Simulation code is split by run, client, nemesis, scheduling, generator, executor, model, and oracle roles. | Corpus replay, operation-stream equality, PCT breadth, and workload invariants. |
| F30 | The disk-cache harness uses typed commands and explicit `HarnessState`. | Command-byte mapping and deterministic media-state tests. |
| F31 | `exec` owns deterministic execution and scheduling; `rt` owns in-run native/simulated runtime services. | Scheduler replay, runtime lifecycle, model-time, and seam-policy tests. |
| F32 | Mixed benchmark options, results, setup, and workload execution have separate owners. | CLI validation, setup/shutdown, metric arithmetic, and workload-selection tests. |
| F33 | Integration fixtures are shared and behavior is split across focused test targets. | Basic, scan, stats, and shutdown/cancellation suites. |

## Accepted current-state differences

- F18's exhaustive mirror model was retired. Focused transition regressions and
  public asynchronous liveness tests now own the cancellation and handoff contract.
- F20 permits renewal from a terminal-looking phase because wound cleanup can
  race with the terminal outcome; this is a recoverable transition, not a panic.
- F23 rejects an invalid split policy when the builder constructs it, rather
  than repeating validation when a database or engine opens.
- F24 uses the raw `Backend::list(prefix, cursor, limit)` signature. The opaque
  cursor remains prefix-bound and providers use the shared validation helpers;
  the staged `ListRequest` API and its migration tests were removed.
- F25 moved delay sampling into simulation entropy and gives Fake S3 a separate
  seeded stream. Draw sites remain deterministic within each component.
- F26 `Drop` only signals fake-server shutdown and detaches. Explicit shutdown
  owns the guaranteed join and panic reporting, avoiding a blocking destructor.
- F29's generic trace schema, observers, canonical JSON, and digest fixtures were
  migration scaffolding and were removed. Corpus and semantic operation replay
  are the durable determinism contract.
- F31's `exec` module is the simulation control plane, not a compatibility
  facade; `rt` remains the stable runtime-service boundary used during a run.
- F32 exact human-readable output snapshots were retired because wording and
  layout are not compatibility contracts; schema and calculations remain tested.
- F33 intentionally changed libtest grouping from one integration target to
  focused targets; fixtures are isolated so parallel execution remains safe.

## Verification and decision record

The integrated branch was verified with `make test-all`, including formatting,
Clippy with warnings denied, native tests, deterministic simulation tests, and
committed corpus replay. Persistent-format or protocol decisions remain in the
existing ADRs; the structural refactor itself did not require a new ADR.
