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

## F23-A — Clarify and validate the shared node soft limit

- Clarified that the compatibility-named `leaf_max_bytes` field is the encoded-
  content soft cap for both leaf and index nodes. Public fields and downstream
  struct-literal construction remain available.
- Added one checked `SplitPolicy` invariant: reserved split headroom may equal,
  but not exceed, the hard node cap. The public database builder maps violations
  to `InvalidInput` before metadata/backend work, while `Engine::open` repeats
  validation before persistent-cache or permanent-root assembly for direct
  internal callers.
- `content_limit` now treats an unvalidated underflow as an invariant violation,
  and split publication/root sizing reuse that checked calculation instead of
  independently saturating to zero. Valid policies retain identical limits,
  encoded bytes, backend operations, retries, and background behavior.
- Existing node and configuration tests now cover exact and one-byte-over soft
  caps for leaves and indexes, equal/over-cap headroom, the public error class,
  and zero backend operations on invalid open. No migration-only test was added.

## F23-B — Add codec-owned wire-size calculations

- Exposed the 16-byte maximum for transaction IDs minted or renewed by GlassDB
  and reused the validated node token's existing 22-byte encoded maximum.
  `TxId::from_bytes` intentionally accepts arbitrary persisted IDs, so the new
  constant is named `MAX_GENERATED_ENCODED_LEN` rather than claiming a false
  bound; exact entry sizing continues to honor those IDs' actual lengths.
- Added allocation-free protobuf field arithmetic beside the storage codecs.
  `ShardEntry` now reports its exact canonical encoded length, and `Node`
  reports the exact one-entry leaf content length plus worst-case generated-ID
  leaf and maximum-token parent shapes.
- The leaf bound models one generated write holder and one external writer. The
  parent bound models the leftmost child plus the candidate separator, except
  for the empty separator where the map can contain only that one entry.
- One compact entry-state matrix and one boundary table compare the calculations
  with prost's actual encoded lengths across absent/external/inline/tombstone
  states, maximum components, and every relevant nested varint-width change.
- Split-policy callers and their synthetic probes remain deliberately unchanged
  for F23-C. This finding changes no admission decision, persisted bytes,
  backend operation, or retry behavior.

## F23-C — Remove synthetic budget probes

- `SplitPolicy` now delegates exact entry admission and worst-case key admission
  to the codec-owned wire-size calculations. The fake transaction, cloned shard,
  temporary index, allocation-heavy key copies, and magic 24-character token
  have been removed from the validation path.
- Entry admission still reserves half of the checked content limit, rounded
  down, while a key's parent separator may use the full limit. Both boundaries
  remain inclusive; the key calculation uses generated 16-byte transaction IDs
  and validated maximum-length 22-byte node tokens.
- A compact real-node test constructs the maximum admitted key's canonical leaf
  and two-child parent. It pins exact-limit acceptance, rejection when that same
  shape is one byte over budget, and rejection of the next longer key.
- Existing public `InvalidInput` mapping, pre-lock rejection, inline fallback,
  capacity retry, split hints, persisted bytes, and backend operation ordering
  are unchanged. The compatibility-named public policy fields remain for the
  separately authorized F23-D breaking release.
## F18-A — Extract queue and compatible-batch mechanics

- Added a private `KeyQueue` backed by `VecDeque` for reorderable and strict FIFO
  arrivals while retaining the active batch as an ordered `Vec`. Submission,
  compatible batch formation, stable pruning, abandonment, requeue, completion,
  and diagnostic counts now share that owner.
- Merge order remains active batch, compatible reorderable arrivals in stable
  order, then the compatible strict FIFO prefix. The first incompatible FIFO
  request remains a barrier, and a fixed-length reorderable scan cannot cycle on
  incompatible work.
- Driver phases, notification, spawning, result delivery, token cancellation,
  and public snapshots are deliberately unchanged for F18-B. Strict active work
  is requeued before existing FIFO work; reorderable active work is appended
  after older reorderable arrivals.
- One redundant single-call test was folded into the stronger uncontended
  no-spawn test. One compact queue regression covers mixed requeue ordering, so
  the dedup suite remains at thirteen tests.
- Adversarial review found no ordering, cancellation, lifecycle, diagnostics, or
  excessive-test issue. No public API, runtime schedule, or persistent state
  changed.

## F18-B — Make keyed lifecycle transitions explicit

- Replaced the implicit per-key flags with a private `KeyMachine`. Its stored
  phases are `Driven` (identified inline/owner driver plus ready/running round),
  `Completing` (the key reservation held through deferred result delivery), and
  `Handoff` (identified reserved owner); idle and closed keys are removed
  atomically from the shard map rather than represented as lingering states.
- Submitting, starting or refreshing a round, finishing, driver/waiter drop,
  owner start, and close now enter through named transitions. Every driver-facing
  transition checks `DriverId`, so a stale inline future or owner cannot alter a
  successor's queue or round token.
- Transitions mutate state under the shard mutex and return deferred effects.
  Cancellation, notification, result delivery, retired sender/request drops,
  and owner spawning run in a fixed order after unlock. Delivery precedes a
  successor spawn, preserving the existing externally visible ordering.
- A handoff reserves the active-owner count before releasing the shard lock and
  moves an RAII permit into the spawned owner. Close therefore cannot miss the
  interval between committing a handoff and starting its task, and every exit
  releases exactly one reservation.
- The uncontended inline/no-spawn path, batching and FIFO semantics, result fan-
  out, cancellation, shutdown, snapshots, and statistics remain covered by the
  existing behavior tests. Two focused tests add the requested transition/action
  table and prove cancellation, wake, and delivery effects remain deferred; no
  migration-only compatibility test was added.
- Adversarial review found that immediately removing a just-completed key exposed
  a short idle window before deferred delivery. Completion now enters
  `Completing`; submissions in that window only queue, and a post-delivery
  transition either removes the still-empty key or commits exactly one handoff.
  The transition table covers the no-successor finish followed by such an
  interleaving submission.
- The same review found two destructor/allocation details: a stale completion's
  error payload could be dropped under the shard lock, and pruning replaced the
  active batch allocation. Stale outcomes now join the deferred-drop effects;
  in-place extraction and ordinary completion draining retain the batch `Vec`'s
  allocation without changing member order.
- A follow-up allocation audit removed the remaining one-element effect vectors.
  Completion moves the existing batch allocation into one deferred delivery,
  returns it empty after delivery, and reinstalls it only for the same completing
  driver; stale finalization retires it after unlock. Cancellation, wake, retired
  state, and stale outcome effects use optional slots, so ordinary uncontended
  completion adds no effect-staging heap allocations or owner spawn.
- A final hot-path review found that stale/close safety retained an unconditional
  clone of the merged request, and owner rounds rebuilt their handle key each
  time. Starting a round now moves the already-computed merged request into one
  driver-owned fallback slot after unlock, while normal refresh recomputes the
  queue value and only a stale, closed, or emptied-batch lookup clones the
  fallback. Inline and owner drivers each reuse one handle for their lifetime,
  so ordinary rounds add neither that request clone nor a per-owner-round key
  allocation.

## F18-C — Add deterministic cancellation model coverage

- Added one synchronous state-machine model over the private `KeyMachine`
  transitions in a dedicated test module. The crate's shared deterministic
  `Rng` drives three named regression seeds plus a bounded 64-seed, 120-step
  sweep without Tokio task scheduling.
- Independent phase, driver-id, receiver, member, and delivery bookkeeping checks
  every transition for exact live-work accounting, one driver or one identified
  reserved owner, no orphan or duplicate member, and strict FIFO order through
  cancellation and requeue. A non-close `Remove` is valid only with zero live
  members, and all four inline/owner completion-flow outcomes are checked.
- Completion effects can remain pending while enqueue, cancellation, or close
  interleaves. Closing in that window retires only queued work; the already
  completed batch must still receive its success or error exactly once afterward.
  Owner and inline driver drops are generated in both ready and running phases;
  running drops must defer cancellation, preserve batch-before-queue FIFO, and
  reserve exactly the reported successor id.
- A required 21-bit coverage mask includes distinct success/error and post-close
  delivery, pending-delivery close, both phases of both driver kinds, owner-drop
  requeue, and the full completion-flow matrix. Failures print the replay seed,
  step, expected and actual phase, member ledger, coverage, and recent event
  trace.
- The model deliberately submits only strict FIFO mergeable/barrier requests;
  the compact queue regression remains the single owner of reorderable merge
  policy. The F18-B migration-only transition table was pruned because the model
  now exercises its complete flow matrix; deferred-effects and async interface
  regressions remain. No production behavior or public API changed.
## F24-A — Centralize list argument validation

- Added one provider-independent listing validator in `glassdb-backend` and
  migrated the memory, S3, and GCS implementations to call it before cursor
  decoding or provider request construction. Invalid prefixes retain the exact
  `BackendError::Other` message and source shape.
- Prefix shape is the only raw argument rule requiring a runtime check today:
  `ListLimit` is positive by construction, while provider-issued cursors remain
  opaque and provider-rejected tokens remain `BackendError::InvalidCursor`.
- Cursor/prefix binding, a validated request value, middleware/caller migration,
  and provider page-size clamping intentionally remain unchanged for F24-C
  through F24-F. No backend request, pagination, or transport behavior changed.
- A compact shared boundary table covers valid and invalid prefix shapes,
  cursor presence, minimum/maximum limits, the zero-limit type boundary, and the
  exact public error. Existing provider pagination tests now also prove all
  three concrete backends invoke the shared invalid-prefix boundary; no copied
  standalone test suite was added ahead of F24-B.
- Adversarial review found that the initial provider smoke assertions did not
  fully demonstrate cursor classification or validation precedence. Each
  existing provider pagination test now runs the same compact boundary rows:
  malformed prefixes return `Other` regardless of cursor presence, while an
  arbitrary or empty cursor under a valid prefix returns `InvalidCursor`. This
  remains local characterization rather than introducing F24-B's reusable
  conformance suite early.

## F24-B — Share backend list conformance coverage

- Added a doc-hidden `glassdb_backend::conformance` module behind tests or the
  additive `test-support` feature. Its single async harness exercises the public
  `Backend` interface and is enabled for the S3 and GCS crates only as a dev
  dependency feature; production dependency graphs remain unchanged.
- One deliberately unordered fixture covers recursive descendants, a sibling,
  and a near-prefix key. Traversal compares membership rather than order, caps
  every page at the requested limit, rejects duplicate objects and cursors,
  bounds total progress, requires termination, and checks an empty terminal
  listing.
- Invalid-prefix validation precedence and provider-rejected arbitrary or empty
  cursor classifications now live in the same conformance harness. Cursor
  binding and request normalization remain deferred to F24-C; no `ListRequest`
  type or ordering guarantee was introduced.
- Replaced the three copied recursive-pagination tests with thin memory, S3,
  and GCS invocations. Provider transport, retry, conditional-operation, and
  error-normalization tests remain local and unchanged. The existing
  memory-only wrong-prefix check for a provider-issued cursor remains a compact
  local regression until F24-C moves cursor binding into the shared contract.

## F24-C — Bind listing cursors to their prefixes

- `ListCursor` now carries one versioned, byte-length-framed envelope containing
  the originating prefix and the still-opaque provider token. A central binding
  helper and a central validation/unwrapping helper are doc-hidden but public so
  the separate provider crates can share the boundary without changing the
  existing `Backend::list`, `ListCursor`, or `validate_list_args` signatures.
- Prefix shape is validated before cursor decoding. Malformed, empty, unknown-
  version, truncated, non-character-boundary, and cross-prefix cursors return
  `InvalidCursor`; an invalid prefix retains `Other` precedence. Empty tokens
  returned by a provider are treated as provider faults by the binding helper.
- Memory uses a tagged local continuation token inside the envelope, while S3
  and GCS unwrap only the raw provider token for their requests and wrap only
  nonempty response tokens. Valid listings therefore preserve request order,
  page limits, provider continuation bytes, and backend operation counts; only
  the caller-visible cursor representation changes.
- The shared three-provider harness now retains a provider-issued cursor, proves
  normal continuation, rejects reuse under another valid prefix, and separates
  malformed envelopes from a correctly enveloped token rejected by the
  provider. This subsumes and removes the memory-only wrong-prefix test; all
  transport-specific provider tests remain local.
- Cursor compatibility is intentionally one-way: cursors are task-local in the
  repository, so no persistent format migrates, while externally retained raw
  or pre-F24-C memory cursors are rejected and callers restart the prefix. No
  `ListRequest`, backend-instance identity, middleware migration, or caller
  migration from F24-D and later was introduced.

## F24-D — Add `ListRequest` additively

- Added a field-private, borrowed `ListRequest` that validates prefix, cursor,
  and positive limit together without allocating. Its constructor preserves the
  existing prefix-first error precedence, and accessors expose only the already
  validated arguments.
- Added an object-safe request-taking `Backend` entry point whose default calls
  the existing required `list` method. The blanket `Arc<B>` implementation
  forwards it explicitly so type erasure remains transparent to future
  middleware overrides.
- Existing providers, middleware, storage, and transaction callers remain on
  the old signature for F24-E/F. No provider request, cursor bytes, page result,
  operation count, or public error classification changed.
- The existing validation table now exercises `ListRequest` construction and
  accessors at the same boundaries. One compact trait-object test pins default
  forwarding, prefix isolation, limit/cursor continuation, and termination;
  this compatibility check may retire with the old signature in F24-G.

## F24-E — Migrate backend middleware

- Delay, fault, hook, logging, recording, scheduled-delay, and statistics
  decorators (including the benchmark's role-attribution counters) now implement
  the request-taking entry point and forward the same borrowed request through
  every layer. Their old methods remain compatibility boundaries that construct
  one validated request; providers and higher-level callers are intentionally
  unchanged until F24-F/G.
- Valid listing requests preserve decorator order, delay/fault decisions, hook
  fields, log/record bytes, result pages, and the single list-operation count.
  Compatibility calls with invalid raw arguments deliberately continue through
  the legacy path, preserving every decorator effect and Hook/Fault error
  precedence before the provider rejects the request.
- Two composed forwarding tests cover the backend crate's seven decorators
  without duplicating a suite per wrapper. They carry a real prefix-bound cursor
  and limit through both compatibility and erased request dispatch, and pin
  delays, hook fields and counts, exact recording bytes, provider-facing/outer
  statistics, result pages, plus invalid-call effects and error override
  behavior. The benchmark attribution test separately pins its compatibility,
  request-pagination, invalid-input, and exact-count contracts.
- No provider, storage, transaction, benchmark workload, or conformance caller
  was migrated. Persistent bytes, provider requests, valid-operation counts,
  scheduling order, and random draws are unchanged.

## F24-F — Migrate storage and transaction callers

- `CachedStore` adds a request-taking entry point and its private typed facade
  accepts only a constructed `ListRequest`; node, structural-recovery, and
  transaction-log listing boundaries construct that validated value immediately
  before each page read. Raw prefix/cursor/limit triples therefore no longer
  cross production storage layers or reach the legacy backend entry point.
- The existing public `CachedStore::list` signature remains as an additive
  compatibility wrapper. Like backend middleware, its invalid-input branch
  deliberately retains the old raw call so validation failures preserve
  invocation ordering and arbitrary wrapped-backend effects; production callers
  use only `list_request`.
- Valid pagination loops retain the same prefixes, page limits, cursors,
  sequential page/read ordering, invocation watermarks, filtering, and error
  mapping. The migration adds no backend operation, retry, task, random draw, or
  persistent byte change to requests that reach a provider.
- Deliberate deviation: storage-owned malformed or cross-prefix cursors are now
  rejected while constructing the request, before allocating a cache invocation
  watermark or entering middleware. This is the validated-boundary behavior
  introduced by F24-D; the existing GC invalid-cursor contract still restarts
  the affected shard. `CachedStore::list` retains legacy effects only for
  external compatibility callers that still pass raw invalid arguments.
- Existing node, structural-log, and transaction-log pagination tests remain the
  long-term behavior coverage; no old-vs-new migration test was retained. The
  one split recovery assertion that calls `CachedStore` directly was updated to
  construct the same request rather than keeping a test-only raw storage API.
- Providers and compatibility tests deliberately retain the old method until
  F24-G. That release-gated removal remains outside this finding.
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

## F21-C — Relocate direct-path tests

- Moved the 18 existing direct-commit eligibility, landing, replay,
  uncertain-CAS, batching, cache-reuse, and locked-fallback tests unchanged into
  `algo::direct_commit::tests`. Their names and assertions are unchanged; the
  30 general transaction tests remain in `algo::tests`, including CAS/deadlock
  retry, locked orchestration, and point/scan read validation.
- Moved only direct-specific fixtures with that suite: the deterministic fold
  driver, coordinator seed gate, in-doubt CAS hook, same-leaf sibling and store
  counter helpers, and the deliberately over-inline-budget value. The common
  engine/transaction setup and operation-count helpers remain single-sourced in
  `algo::tests` and expose only the members consumed by the relocated tests as
  test-only `pub(super)` support.
- Removed the temporary sibling-test visibility on the direct resolver,
  eligibility classifier, and their fields, along with the test-only split-hint
  accessor introduced during F21-B. The colocated child test module can inspect
  those private implementation details without widening the collaborator's
  boundary.
- This is a test ownership change only: no durable tests were added, removed, or
  renamed, and no production control flow, backend operations, retry behavior,
  persistent bytes, randomness, or task lifetime changed.
## F28-A — Add infallible materialized iterators

- Added `KeyIter` and `CollectionIter` plus `iter_keys` and
  `iter_collections` entry points on the public collection/transaction surfaces.
  All fallible I/O and decoding finish before an owned iterator is returned;
  per-item iteration is now truthfully infallible. Transaction-scoped directory
  observations are still validated when their enclosing attempt completes.
- One private generic materialized iterator owns each vector. The legacy
  `KeysIter` and `CollectionsIter` remain API-compatible adapters over the same
  plain iterators, so there is no second listing, materialization, or ordering
  implementation.
- The plain iterators expose exact remaining length and fused exhaustion, while
  deliberately avoiding reverse traversal or borrowed lifetimes that were not
  part of the existing contract. Child handles remain bound to their listed
  incarnations after the source handle or transaction scope ends.
- Existing key-order and child-incarnation tests now also cover empty results,
  owned lifetime, exact remaining length, and ordered old/new parity. The
  existing directory-retry test covers transaction-level parity using a returned
  transaction error rather than asserting inside the body. These parity checks
  are temporary through F28-C; long-term ordering, ownership, empty, paging, and
  incarnation behavior remains.
- No legacy method/type was deprecated or migrated in this finding. Backend
  operations, transaction retry behavior, persistent bytes, and public failure
  boundaries are unchanged.

## F28-B — Deprecate fallible materialized iterators

- Deprecated `KeysIter`, `CollectionsIter`, and their three producing methods
  with direct links in the diagnostic text to the plain-item replacements.
  Their compatibility implementations and public re-exports remain intact for
  the release-gated removal in F28-C.
- Production simulation clients and repository behavior tests now use
  `iter_keys` and `iter_collections`; listing errors continue to occur at the
  awaited materialization boundary, while iteration itself is infallible.
- Retained only the existing focused old/new parity rows for collection keys and
  collection- and transaction-scoped child listings. Their deprecated calls are
  locally allowed so ordinary production code remains warning-free; these
  compatibility assertions retire with F28-C.
- This migration changes no listing I/O, ordering, snapshots, transaction
  validation, persistent bytes, retries, random draws, or task scheduling.
## F29-A — Add a stable harness trace schema

- Added a versioned, structured trace behind the simulation-only feature. It
  records harness role spawn decisions, actual executor spawn IDs and selected
  tasks, every simulated runtime byte, supplied versus fallback `Tape` bytes,
  and supplied-tape versus PCT-RNG scheduler draws. Client/restart and operation
  boundaries, crash/outage/final-heal actions, and final verification are in the
  same event stream.
- Tracing is opt-in. The ordinary harness uses a zero-allocation disabled sink;
  the executor observer reads already-produced entropy bytes and assigned task
  IDs without making an additional draw or scheduling decision.
- One compact simulation test exercises every top-level event kind, all entropy
  sources, every nemesis/heal action, and compares backend operation streams
  with tracing disabled and enabled for uncached tape, cached tape, and PCT
  runs. Exhausted scheduling tapes emit selected-task events but no fabricated
  entropy draw.
- The schema has a canonical, schema-version-prefixed JSON encoding but no
  digest or corpus baseline. Those reviewed fixtures remain F29-B/F29-C work.
  Adding optional `serde`/`serde_json` dependencies to the existing `sim`
  feature is the only dependency change; the trace does not expand the default
  feature surface.
- No ADR was added: this is a temporary migration guard plus long-term replay
  diagnostic, not a production protocol or architecture decision.

## F29-B — Freeze tape-scheduled harness traces

- Froze three small inputs copied from the committed `concurrent_tx`, `history`,
  and `api_correctness` corpora. Their source basenames are the SHA-1 of the
  copied bytes, so reviewers can verify provenance without coupling the guard to
  later corpus minimization.
- One table-driven simulation test runs every input twice, compares the complete
  schema-v1 canonical bytes, and then checks a reviewed SHA-256 digest. It also
  asserts cache-free/cached run boundaries and final verification semantically.
- The normal RMW and API fixtures perform successful operations without enabled
  fault nemeses. The History fixture records an admissible failure, crash and
  same-client restart work, outage down/heal, final healing, and successful final
  verification. Every fixture consumes supplied scheduler-tape bytes.
- These exact digests are migration guards through F25-D, F29-K, and F31-D.
  F25-A/B and structural/runtime extractions must not refresh them; an F25-C
  entropy-source migration may update only an affected digest after its first
  divergent event is reviewed and documented. After all three endpoints land,
  exact digests may retire while same-input replay and semantic boundaries stay.
- Hashing is confined to the simulation integration test through a dev-only
  dependency. No production path, persistent bytes, backend operation, retry,
  or scheduling decision changed, and no baseline update mode was added.

## F29-C — Freeze PCT-scheduled harness traces

- Froze the complete schema-v1 canonical traces for one small contended RMW
  workload with fallback-tape faults at seeds `12780` and `12980`. Their two PCT
  change points are `[1, 15]` and `[9, 29]`: both runs cross both boundaries,
  while the pair covers immediate and later preemption points without adding a
  large synthetic workload solely to reach the scheduler's 2048-step estimate.
- The table reruns every seed and compares the full canonical bytes before
  checking its reviewed SHA-256 digest; it also requires the selected seeds to
  produce distinct traces. `pct_trace` performs the existing RMW final-state
  invariant, and the established PCT seed-breadth suites remain unchanged.
- Semantic checks distinguish the two initial change-point draws from task
  priority draws, require eight bytes per PCT RNG draw and exactly one priority
  draw immediately before every sequential task spawn, and reject selecting an
  unspawned task. They pin client/nemesis role spawn order, prove selection
  reaches both change points, and require runtime, fallback-tape, and scheduler
  entropy without any supplied-tape consumption.
- These PCT digests follow the F29-B migration-guard lifecycle: F25-A/B and
  F29/F31 structural moves must preserve them; F25-C may refresh only an
  affected digest after documenting the first deliberate entropy divergence.
  Exact digests retire only after F25-D, F29-K, and F31-D have all landed, while
  same-seed replay, seed divergence, semantic event boundaries, scheduler
  entropy accounting, and distribution vectors remain long term.
- No production implementation, harness behavior, scheduling decision, or ADR
  changed, and there is no baseline update mode.
## F33-A — Extract shared integration fixtures

- Added a focused `tests/integration_support` module rather than mixing these
  Tokio integration fixtures into deterministic-simulation `sim_support`.
  Database and collection setup, integer/RMW helpers, collection-listing setup,
  and the commit-pipeline `PauseControl` now have one reusable owner for the
  behavior-oriented targets introduced by F33-B/C.
- Replaced the two remaining inline hook closures with typed
  `ParentWriteControl` and `LoglessCommitControl` fixtures. Their public test
  surface exposes only the backend, synchronization points, and observations
  consumed by the existing assertions; hook matching and channel state remain
  private.
- The parent-write control is armed explicitly after database and parent setup,
  at the same point as the original hook installation. This ordering is
  essential: arming it during construction would intercept the setup CAS rather
  than the concurrent registrations under test.
- No test was added, removed, renamed, or moved, and all 39 original integration
  tests still run in the original target. Existing assertions, paused-clock
  choices, transaction-error handling, hook predicates, CAS landing points, and
  cancellation/release ordering remain unchanged.
- This test-only extraction changes no production API, persistent bytes,
  backend-operation behavior, retry policy, random draws, task spawning, or
  shutdown semantics. No migration-only coverage was introduced.

## F33-B — Move basic, stats, and scan tests

- Created three behavior-oriented integration targets: `integration_basic`
  owns 15 database and transaction tests, `integration_stats` owns five
  statistics/diagnostics tests, and `integration_scan` owns 12 key-scan and
  collection-listing tests. The seven shutdown/cancellation tests remain in the
  original `integration` target for F33-C.
- Moved each test body, name, Tokio test attribute, deprecated allowance, and
  behavior comment mechanically. The targets reuse F33-A's support module;
  making that module public only within each standalone test crate prevents
  target-local dead-code warnings without adding a library or production API.
- Focused Cargo discovery reports 15 + 5 + 12 + 7 = 39 tests. Its sorted union
  exactly matches all 39 names from the pre-move target, with 39 unique names,
  so no behavior test was duplicated or lost.
- The source-name/count/location and Cargo exactly-once comparisons were run as
  shell audits only; no migration-only test or manifest machinery was added.
  These migration audits are complete for F33-B and should not be retained as
  long-term tests. F33-C must repeat the repository-level exactly-once discovery
  audit after moving the last seven tests and deleting `integration.rs`.
- Test bodies, Tokio attributes, paused-clock choices, hook arm points, and
  in-test task scheduling are unchanged. Libtest process grouping intentionally
  changes from one target to four; every target constructs its own backend,
  hook controls, and synchronization fixtures, so the groups remain isolated
  and safe to run in parallel.
- Production behavior, transaction-error handling, backend operations,
  persistent bytes, retries, randomness, and task lifetimes are unchanged.

## F33-C — Move shutdown and cancellation tests and retire the monolith

- Moved the remaining seven shutdown, cancellation, logless-abort, abandoned-
  holder, and async-wound behaviors verbatim into `integration_shutdown` and
  deleted the now-empty `tests/integration.rs` monolith. Test names, Tokio
  attributes, comments, paused-clock choices, hook arm points, assertions, and
  in-test task scheduling are unchanged.
- The durable integration suite now consists of four focused targets: 15 basic
  database/transaction tests, five statistics/diagnostics tests, 12 scan and
  listing tests, and seven shutdown/cancellation tests. All behavior coverage
  remains; no test was added, removed, merged, or renamed.
- Package-wide Cargo discovery finds every one of the original 39 test names
  exactly once, and attempting to select the retired `integration` target
  fails. This was a one-time migration audit only; no name/count/location test
  or auxiliary manifest machinery remains in the repository.
- Each target passes independently, and all four also pass when launched as
  concurrent test processes. Shared support is compiled target-locally, while
  every test constructs its own in-memory backend, hook controls, channels,
  and synchronization state; there is no cross-process mutable fixture.
- This completes the behavior-oriented ownership change without modifying
  production APIs, persistent bytes, backend operations, retry policy,
  randomness, runtime task lifetimes, or shutdown/cancellation semantics.
## F25-A — Provide one all-build entropy facade

- Added the public `glassdb_concurr::entropy` facade with byte filling and
  uniform `[0, 1)` sampling in every build. Native execution still calls the
  same process-RNG operations; simulation builds use the existing executor
  entropy only while an executor is active and retain the process-RNG fallback
  for ordinary Tokio tests.
- Data ID minting now calls the shared byte filler directly for transaction,
  database, collection, and B-link node identities. The existing data shuffle
  helper follows the same facade because it previously shared that local byte
  source. `glassdb-data` no longer needs its own `rand` dependency or duplicated
  build-mode selection.
- Retry jitter now calls the shared unit sampler. Under simulation it still
  consumes one eight-byte fill and maps the high 53 bits exactly as before, so
  the entropy draw boundary, value, and trace event remain unchanged; native
  jitter still uses `rand`'s `f64` sampler.
- One focused seeded vector covers an uneven byte fill, an interleaved unit
  sample, and a following fill, pinning shared-stream order and chunk count.
  Both reviewed tape and PCT trace digests pass without a baseline change.
- No persistent bytes, identifier layout, retry schedule, operation ordering,
  task spawn order, or existing public signature changed. The new facade is the
  intended additive API. No ADR was added because this centralizes an existing
  runtime policy rather than selecting a new one; there were no deviations from
  the staged finding.
## F32-A — Extract mixed benchmark options and dimensions

- Moved the mixed scenario's Clap arguments, defaults, validation, contention
  mode parsing, and mode-by-affinity cell enumeration into `mixed/options.rs`.
  The scenario runner now consumes the resulting ordered dimensions and still
  performs the same setup and execution for each cell.
- Cell generation remains mode-major and affinity-minor, retaining both the
  caller-provided order and any repeated values. Validation still runs before
  mode parsing and before the invocation timestamp is generated, so invalid
  configurations retain the existing error precedence and cause no scenario-cell
  or backend-operation effects after factory initialization.
- Added one table-driven snapshot test for the default sweep and a representative
  explicitly reordered sweep, plus one table-driven test covering the existing
  validation rules through parsed CLI arguments. These are durable option and
  cell-order contracts rather than migration-only parity tests.
- No setup, settlement, worker selection, random draws, database naming, metrics,
  output schema, backend operations, or task scheduling changed. Results,
  scenario phases, and workloads intentionally remain in `mixed.rs` for F32-B
  through F32-D.

## F32-B — Extract mixed benchmark results and reporting

- Moved shape latency and throughput summaries, database-counter aggregation,
  normalized operation/protocol metrics, serialized result types, and the three
  mixed progress/status line formatters into `mixed/result.rs`. The scenario
  passes owned timing snapshots and post-shutdown counter deltas across this
  boundary; measurement and shutdown ordering are unchanged.
- The result module retains logical committed samples as every per-transaction
  metric's denominator while reporting physical `Database::tx` attempts
  separately. Empty sample sets, zero protocol denominators, confidence-target
  classification, percentile interpolation, millisecond conversion, and
  settlement-duration saturation retain their previous behavior. Timing
  snapshots are reduced lazily, preserving the previous one-shape-at-a-time
  peak sample-vector memory.
- Replaced the two narrow counter tests with one fixed-sample snapshot through
  the same `Serialize`-to-`serde_json::Value` path used by `perfbench`. It pins
  shape ordering, latency summaries, convergence, every serialized field and
  aggregate metric, including a wholly empty cell whose zero-denominator metrics
  must remain finite valid JSON. A second compact snapshot pins the existing
  progress, capped-cell, and setup-settlement text byte-for-byte.
- No CLI behavior, cell order, setup, settlement, worker selection, random
  draws, database naming, backend operations, result values, serialized schema,
  task scheduling, or stderr text changed. Scenario setup and workload execution
  intentionally remain in `mixed.rs` for F32-C and F32-D.

## F32-C — Extract mixed benchmark setup and settlement

- Added `mixed/setup.rs` with explicit prepare, begin-measurement, and teardown
  phases. Preparation builds the same collection paths, seeds the same fixed
  values in batches of 100, waits for the completed-split counter to stay quiet,
  shuts down the throwaway setup Database, then opens every collection on the
  ordered measurement clients.
- `run_cell` still builds its unchanged shape plans after collection opening.
  Only then does `begin_measurement` capture client statistics, immediately
  before the existing benchmark timers start. This retains the original counter
  window and keeps all setup reads and structural convergence outside results.
- Teardown still ends timers first, uses the worker drain deadline for concurrent
  Database shutdown, gives a worker failure precedence over a simultaneous
  shutdown failure, and reads counter deltas only after successful shutdown.
  Collection handles remain alive through shutdown as before, and setup-error
  paths retain their drop-based cancellation behavior.
- Moved the existing deterministic quiet-period reset test alongside the phase
  and extended it with the inclusive timeout boundary. Added one real
  `MemoryBackend` lifecycle test that verifies each measurement client opens both
  seeded collections, every collection contains the configured three keys, and
  a retained client clone rejects work after teardown. No mock lifecycle or
  migration-only parity suite was added.
- CLI behavior, cell order, seeding transaction order, setup and measured backend
  operations, error precedence, database naming, random draws, worker tasks,
  metrics, serialized output, and status text are unchanged. Workload selection
  and execution intentionally remain in `mixed.rs` for F32-D.

## F32-D — Extract mixed benchmark workload execution

- Added `mixed/workload.rs` as the single owner of shape order, worker-slot
  distribution, deterministic worker seeds, collection/key selection, worker
  loops, transaction bodies, and adaptive stopping. `run_cell` now contains only
  setup, measurement bracketing, worker orchestration, teardown, and reporting.
- The extraction preserves shape-major/database-major/worker-major spawn order,
  the initial seed and increment, both collection-selection draws at every
  affinity, sorted distinct key selection, and the existing read-only versus
  read-modify-write transaction bodies. Selection remains outside the measured
  closure, and one successful logical transaction still records one sample.
- Setup completion and statistics baselines still precede timer start; timers
  still end before teardown; the shared drain deadline, worker-error precedence,
  convergence/cap behavior, counter deltas, and serialized/text results are
  unchanged.
- The existing affinity test was folded into one deterministic worker-contract
  test driven by the production worker-spec sequencer. It freezes the complete
  shape/database/worker seed order for a multi-database plan, representative
  collection/key vectors, and one successful sample per shape in reporting
  order. No old-versus-new migration harness or duplicate end-to-end benchmark
  was kept.
- Retired the exact stderr-line snapshot at the documented F32-D endpoint.
  Configuration, dimension ordering, metrics, and serialized-schema snapshots
  remain as long-term contracts; the human-readable progress wording is not a
  public interface and remains covered by its ordinary scenario call sites.
- No public API, database operation, persistent byte, retry, random draw, task
  count/order, metric, or CLI behavior changed. No deviation from the plan or
  architecture decision was required.
## F29-H — Extract the API program generator

- Moved only `ApiWorkload`'s `Arbitrary` decoder and action-generation logic
  into the private `sim::api::generator` module. `ApiAction`, `ApiTransaction`,
  `ApiWorkload`, and its default remain declared in `api.rs`, preserving their
  existing public paths and fuzz-target bounds without a re-export shim.
- The exact model, transaction action handlers, program loop, `SimWorkload`
  implementation, and final oracle remain in `api.rs` for F29-I through F29-K.
  The generator reads the parent module's existing key and collection
  cardinalities; its per-transaction generation limit remains private.
- Decoder branches, modulo arithmetic, client ownership, allocation order,
  action order, abort decoding, and byte draws moved mechanically. The existing
  fixed-input test was extended into two compact vectors covering every action
  variant, owned-key projection, client assignment, abort outcomes, and the
  number of bytes left after decoding.
- The reviewed API tape-trace digest remains byte-identical, and all 1,509
  committed `api_correctness` corpus inputs still replay successfully. No
  persistent bytes, backend operations, retry semantics, runtime entropy,
  spawn order, executor/model/oracle behavior, compatibility shim, or ADR
  changed.
## F29-D — Extract `RunPlan` and `RunContext`

- Added private `RunPlan` and `RunContext` types in the simulation harness.
  `RunPlan` owns the immutable workload, fault, seed, and tape inputs; consuming
  it performs the existing seeding path and produces a context that owns the
  backbone, operation log, media, oracle state, client inputs, and transports.
- `run_generic_with_trace` still contains client crash/restart behavior, spawn
  order, nemesis scheduling, and join order. It now delegates resource setup to
  `RunPlan::setup` and final healing, invariant verification, database shutdown,
  and log return to `RunContext::teardown`.
- The three tape-scheduled and two PCT-scheduled canonical trace baselines remain
  byte-identical without a digest update. Existing harness tests also retain
  traced/untraced operation parity and client-panic propagation, so no temporary
  structural or duplicate regression test was added.
- Backend operations, persistent bytes, retry/error boundaries, entropy draw
  order, task selection, client/nemesis behavior, cache-media ownership, and
  public APIs are unchanged. No deviation from the finding plan and no ADR were
  needed.
## F31-A — Extract simulation schedulers

- Added the private `sim::scheduler` module and moved the `Scheduler` trait plus
  tape/replay, seeded-random, and PCT policies into it without changing their
  fields or decision algorithms. `exec` re-exports the existing public types so
  every `glassdb_concurr::rt` path and signature remains unchanged.
- The checklist's FIFO role corresponds to the existing test-only
  lowest-task-id scheduler used as the executor's fixed-order baseline. It moved
  with the policies and remains test-only; this finding does not introduce a
  new public scheduler or claim readiness-queue FIFO semantics for the
  executor's sorted ready set.
- Moved tape replay, PCT seed/divergence, and explicit change-point tests beside
  their implementations. Executor ready-set, virtual-time, wake, panic-budget,
  and Tokio select-seeding tests remain with the executor because those assert
  kernel behavior rather than a policy in isolation.
- Added one compact fixed selection vector for `RandomScheduler`, the only
  seeded policy that previously lacked an exact local baseline. Existing tape
  and PCT tests were moved rather than duplicated; no migration-only test was
  retained.
- All three tape and both PCT reviewed harness digests remain byte-identical
  without baseline edits. Scheduler draw boundaries, task spawn/selection order,
  replay fallback, public visibility, persistent bytes, backend operations, and
  runtime error behavior are unchanged. No ADR was needed for this mechanical
  ownership move.
