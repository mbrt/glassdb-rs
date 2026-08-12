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
