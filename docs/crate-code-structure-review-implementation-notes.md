# Crate code-structure review implementation notes

This file records implementation details, deviations, and review points for the
checkboxes in `crate-code-structure-review.md`. The review checklist remains a
working document and is intentionally not committed with these changes.

## F16-A — Extract `NodeStore`

- Moved the node codec, node/root/leaf persistence, and observation-bound
  `LeafEdit` into `node_store.rs`. `ShardStore` retains explicit one-line node
  delegates and exposes `nodes()` for the staged migration; it does not use
  `Deref` or a generic compatibility conversion.
- The extracted store is built from a clone of the same `CachedStore`, preserving
  cache evidence, timeline ordering, warm-read operation counts, and CAS error
  mappings.
- Moved the six existing node-store behavior tests to the new owner and kept
  their semantic and operation-count assertions. Added one long-term pagination
  regression covering a listing larger than the 128-object backend page.
- No persistent bytes, retry policy, deterministic scheduling, or public runtime
  behavior changed. The only compatibility surface added is `NodeStore` plus
  `ShardStore::nodes()`, as required by the migration plan.
- Adversarial review found no correctness, operation-count, format, scope, or
  long-term test-value issues.

## F16-B — Extract `StructuralLogStore`

- Moved the structural-log codec, path/participant validation, conditional CRUD,
  and paginated listings into `structural_log_store.rs`. `ShardStore` keeps
  explicit delegates and exposes `structural_logs()` until F16-D migrates the
  protocol owners.
- Both typed stores are still derived from the same `CachedStore`, preserving
  cache coordination, observations, sequence points, and backend operation
  ordering.
- Moved the existing participant and pagination regressions to the new owner.
  Added one compact long-term lifecycle test for the successful phase update,
  stale-observation conflict, and deletion path.
- Persistent bytes, page size, prefix scope, retry behavior, and CAS error
  mappings are unchanged.
- Adversarial review found no correctness, safety, API, pagination, or excessive-
  test issue; generic cached-store coverage already owns missing-delete
  convergence, so it was not duplicated here.

## F16-C — Narrow `TreeRouter` to `NodeStore`

- `TreeRouter` now owns only `NodeStore`; its production module has no
  structural-log-capable handle. All composition sites explicitly pass the node
  capability through `ShardStore::nodes()`.
- Routing control flow, read requirements, cache-hit aggregation, stale-parent
  correction, and terminal freshness checks were not changed. Existing router
  behavior and backend-operation-count tests remain the long-term acceptance
  coverage.
- No compatibility conversion or one-implementation abstraction was added, and
  no migration-only tests were introduced.
- Adversarial review found no code issue or capability backdoor. It identified
  one stale architecture sentence naming `ShardStore`; that documentation now
  names the narrowed `NodeStore` handle.

## F16-D — Wire structural logs explicitly

- `Splitter` and `Gc` now receive `StructuralLogStore` explicitly, while
  `ShardStore` has lost its structural-log state, accessor, and delegation
  methods. This completes the temporary façade migration without a compatibility
  backdoor.
- Engine and test composition derive node and structural-log stores from clones
  of the same `CachedStore`, preserving shared cache evidence, timeline identity,
  observations, and backend-operation counts.
- Every split, recovery, participant-settlement, and GC structural-log operation
  changed only its receiver. Protocol awaits and ordering, retry behavior,
  random draws, task spawn order, and persistent bytes are unchanged.
- No migration-only tests were added. Existing structural-log lifecycle and
  pagination tests plus split recovery, restart, participant cleanup, and GC
  regressions remain the long-term coverage.
- Adversarial review found no code or protocol issue. It identified stale storage
  architecture documentation, which now shows the explicit structural-log
  component and its `_s` recovery records.

## F05-E — Extract structural recovery behind `Splitter`

- Added a private concrete `StructuralRecovery` that owns durable log discovery,
  source-writer fencing, recovery classification and cleanup, separator
  publication state, and topology-participant settlement. `Splitter` remains the
  background-loop facade and owns recursive parent splitting.
- Recovery-to-split coordination uses resumable named actions rather than a
  callback or backreference. The same separator-publication freshness epoch and
  retry budget survive a requested parent split before recovery deletes its
  exact structural-record observation.
- Freshness barriers, source-fencing and reachability order, child cleanup order,
  participant scoping and final removal, background wake/delay/spawn behavior,
  random draws, backend operations, and persistent bytes are unchanged.
- No migration-only tests were added. The existing roll-forward regression was
  strengthened after adversarial review to force the recovery-specific parent
  split action after separator publication and retain it as long-term
  crash-recovery coverage.
- The architecture module inventory now identifies the extracted recovery owner.
  No ADR was added because this extraction changes neither protocol nor external
  architectural contract.

## F17-A — Add the traversal matrix

- Added a test-only routing matrix for stale roots, stale leaf parents, interior
  right hops, and sibling-chain traversal before changing production traversal.
  All public `TreeRouter` entry points are covered where the topology applies.
- Independent cache views and exact ordered backend traces pin cold reads, warm
  zero-read routing, conditional freshness checks, terminal-only fresh routing,
  and cumulative cache-hit evidence. Complementary warm-prefix/cold-right and
  cold-prefix/warm-terminal cases prove every visit is ANDed and a later hit
  cannot erase an earlier miss.
- A three-leaf chain pins complete sibling order and inclusive middle-bound
  stopping without reading past the bound. `next_leaf`, which starts from a
  retained locator rather than a root descent, has direct right-link freshness,
  cumulative-hit, and malformed-reference coverage.
- The broader matrix replaced narrower stale-parent, stale-root, leaf-order,
  interior-freshness, and ordinary parent-lookup tests. The router suite now has
  nine tests rather than ten; distinct single-root, absence, separator-boundary,
  grouping-order, classification, and single-leaf-parent contracts remain.
- The first adversarial review rejected shared warm-cache rows and discarded
  operation logs. The revised matrix uses independent rows, asserts every trace,
  and adds mixed-hit, bounded-chain, and observation-kind/membership checks
  identified across the two review rounds.

## F17-B — Introduce `DescentCursor`

- Added a private cursor that carries the typed collection prefix, current
  requirement, location, observation, and route-wide cache-hit evidence.
  Leaf lookup, optional bootstrap, leftmost descent, and terminal freshness now
  share its root/right/child traversal.
- `leaf_for_fresh` still descends interiors at its original requirement, reloads
  only the exact terminal path when requirements differ, and resumes descent if
  that refreshed node became an index. No public API or error classification
  changed.
- Topology queries retain only a thin cursor-normalization adapter until F17-C;
  sibling-chain APIs remain deliberately unchanged for F17-D. The committed
  F17-A matrix passed without modification.
- Adversarial review found no semantic, currentness, cache-hit, or operation-
  ordering issue. Its only finding was an avoidable temporary `String` allocation
  on stale right hops; the cursor now parses the borrowed token directly.

## F17-C — Reuse the cursor for topology queries

- `token_reachable_at_key` and `parent_index_for` now use one cursor-owned
  traversal loop with synchronous stopping policies. Target reachability compares
  the normalized path before leaf termination; parent lookup retains the deepest
  normalized index only after its selected child loads successfully.
- The policies preserve their intentionally different absence contracts:
  reachability maps a dangling selected link to `false`, while parent lookup
  propagates `NotFound`. Neither query loads a requested target directly, and the
  returned parent evidence excludes the terminal child read as before.
- Public APIs, requirements, error messages, cache-hit aggregation, and backend
  operation order are unchanged. Sibling-chain traversal remains separate for
  F17-D, and no migration-only test was added.
- Adversarial review found the production extraction clean but identified missing
  long-term coverage for absent roots/targets and dangling links. One compact
  interface test now pins those results, the asymmetric errors, and the absence
  of a direct target read.

## F17-D — Extract `LeafChain`

- Added a private sibling-chain context that owns the router, typed collection
  address, and requirement. `next_leaf`, bounded leaf enumeration, and full leaf
  enumeration now share one successor load and one collection loop.
- Inclusive-bound ownership is checked before loading a successor, so bounded
  scans still avoid reading beyond their terminal leaf. Cache-hit evidence is
  ANDed through the bootstrap and every sibling, and invalid or dangling links
  retain their prior errors.
- Point-key grouping intentionally remains independent per-key descent: changing
  it to a sibling scan would alter mixed-collection ordering, observation timing,
  and backend operations. Existing scan consumers already use the migrated
  bounded/full interfaces.
- No new tests were added. The committed F17-A matrix already owns three-leaf
  order, middle-bound/no-prefetch, warm/cold cumulative evidence, stale-link, and
  absence behavior.

## F19-A — Thread `ShardLockReceipt` directly into `LockedTx`

- Successful shard acquisition now returns a complete receipt containing the
  coordinator's exact CAS precondition and the entry/membership strengths that
  actually landed. `LockedTx` groups are assembled only after every requested
  path pairs with exactly one receipt.
- Validation and write-back consume retained receipt observations directly.
  `held_membership` and the successful-path lookup in `tlocks` are removed, so a
  complete lock result no longer reconstructs proof from cleanup bookkeeping.
- `tlocks` remains for synchronous landed-lock recording, partial-acquisition and
  cancellation cleanup, deterministic release/fallback, and diagnostics. There
  is still no await between a successful coordinator result and recording the
  landed lock.
- The existing scan-plus-write regression now asserts the durable receipt carries
  both an entry `Write` and the actual membership `Read`; no migration-only test
  was added. Existing cancellation, snapshot, validation, and operation-count
  coverage remains unchanged.
- Adversarial review found no receipt-completeness, ordering, cleanup, evidence,
  or test-pruning issue. No persistent bytes, backend operations, retry behavior,
  or public API changed.

## F20-A — Replace correlated attempt flags with validated transitions

- Added a private `algo::attempt` state machine with explicit `New`, `Engaged`,
  and `Committed` phases and `Optimistic` or `Locked` read-validation modes.
  `Handle` no longer exposes separately mutable status, engagement, retry-mode,
  and attempt-count fields to the commit algorithm.
- Named transitions now own engagement, commit, locked-read escalation, wound
  renewal, abort eligibility, and reset validation. Engagement is idempotent and
  reports whether monitor/manifest initialization is required; commit and reset
  after commit are validated terminal boundaries.
- Renewal keeps the original priority/identity behavior, collection attempt,
  acquisition backoff, and serial-fallback count. The existing consumed-handle
  boundary may renew from any phase: wound cleanup can discover a concurrent
  terminal outcome before `restart_after_wound` re-begins the attempt. Preserving
  that boundary avoids adding a panic, while every renewal still forces locked
  validation exactly as the old nonzero attempt count did.
- Direct and optimistic read-only commits remain unengaged; logged attempts stay
  engaged until abort or commit. Monitor calls, persistent transaction bytes,
  backend operations, retry results, random identity draws, and acquisition
  delays are unchanged. Configured acquisition backoff remains deferred to
  F20-B.
- Added two compact long-term state tests: one transition table covers direct
  commit, optimistic escalation, repeated engagement, abort need, wound renewal,
  engaged commit, and renewal after a terminal cleanup race; one preserves the
  exact reset-after-commit panic. Two raw `engaged` assertions were replaced by
  ending each replayed handle before asserting its transaction object remains
  absent, preserving the stronger long-term interface behavior without exposing
  state-machine shape.

## F20-B — Inject configured acquisition backoff

- `Algo` now retains the engine's configured retry policy as a factory for each
  transaction's same-identity lock-acquisition schedule. A new attempt starts
  from that policy, while wound renewal continues carrying the already-advanced
  schedule instead of resetting it.
- The only consumers remain the existing conflict and leaf-capacity retry
  branches, after partial-lock release and after the capacity timeout check.
  First acquisition, successful acquisition, deadlock escalation, genuine
  wounds, optimistic validation, direct replay, and other independently owned
  coordination schedules are unchanged.
- The existing paused-time sustained-CAS regression now opens a real `Engine`
  with a ten-to-twenty millisecond policy, observes the first and capped
  acquisition gaps at exhausted coordinator rounds, and retains its same-ID and
  converged-value assertions. Jitter is checked by documented bands rather than
  exact timing; no second internal backoff test was added.
- Builder documentation now includes same-identity lock reacquisition in the
  existing retry options. Defaults, persistent bytes, backend-operation order,
  retry classification, and random draws on contention-free paths are unchanged.

## F21-A — Move direct-commit vocabulary mechanically

- Moved the direct-commit resolver, attempt/result vocabulary, shape recognizer,
  predecessor value, and eligibility policy into the private
  `algo::direct_commit` module. `Algo` still owns predecessor lookup, execution,
  counters, GC hinting, coordination, and attempt transitions for the later
  collaborator extraction.
- Temporary `pub(super)` visibility is limited to the values and fields the
  parent algorithm already constructs or consumes. The new module has no
  dependency on `Algo`, `Handle`, `Gc`, or the coordinator owner, so ownership
  remains one-way without a compatibility abstraction or public re-export.
- Resolver bodies, eligibility ordering, requirements, admission policy,
  split-hint behavior, outcome classification, and one-CAS commit point are
  unchanged. Production imports moved with their code; test-only coordination
  imports remain local to the existing test module.
- No tests were added, removed, or relocated. Existing direct-path and fallback
  behavior coverage remains in `algo.rs` until F21-C; this finding changes no
  persistent bytes, backend operations, retries, random draws, task spawning,
  statistics, or public API.

## F21-B — Introduce the concrete `DirectCommit` collaborator

- Added one private concrete collaborator owning the direct path's shared
  resolver, shard coordinator, inline policy, split-hint sink, GC hint clone,
  and counters. `Algo` retains its own resolver and GC clones for general read
  validation, abort, locked write-back, and cleanup; its public constructor and
  every caller remain unchanged.
- Moved predecessor lookup and direct execution behind `DirectCommit::try_commit`.
  The boundary receives only the transaction id, data, and F20's validated
  `AttemptState`; collection eligibility and dispatch among committed, replay,
  and locked fallback remain architectural policy in `Algo`.
- Candidate and landed counter points, `Requirement::Any` cache behavior,
  eligibility order, inline admission, split-pressure hints, coordinator outcome
  mapping, and one-CAS semantics are unchanged. After a landed coordinator
  result, the collaborator still increments `landed`, commits the attempt state,
  and enqueues the predecessor GC hint synchronously with no intervening await.
- `DirectCommitStats` and its arithmetic moved with the counters and are
  re-exported from `algo` at the existing path. Resolver construction and
  eligibility visibility remains `pub(super)` only for the unchanged tests that
  stay in `algo.rs` until F21-C; a test-only split-hint accessor serves the same
  temporary boundary.
- No tests were added, removed, renamed, or relocated. Existing normal,
  uncertain-CAS, replay, same-key loser, fallback, statistics, inline-pressure,
  and cancellation coverage remains the durable acceptance suite.
