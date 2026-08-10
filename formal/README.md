# Formal verification

## Status and boundary

This directory contains the bounded verification suite described in
[the formal protocol verification design](../docs/designs/formal-protocol-verification.md).
It implements the Stage 0 semantic inventory, the Stage 1 logged transaction
core, Stage 2 direct/coordinator behavior, the recovery interfaces that the
first slice reduced away, and the Stage 4 composed subprotocol models. The
Stage 3 implementation-history checker lives in
[`crates/glassdb/src/sim/history.rs`](../crates/glassdb/src/sim/history.rs).

This remains an exploration rather than an accepted architecture gate. It is
manually invoked and is not run by CI.

The suite also reproduces a known correctness defect in the current four-status
protocol: a suspended owner can outlive a foreign wound's finite `Aborted`
tombstone and recreate its transaction object after GC. A successful runner
exit means this required counterexample was reproduced alongside the clean
graphs; it does not mean the wound/GC seam is safe.

The checked boundary is split into deliberately small, exhaustive models:

- logged transactions over two keys and same- or cross-leaf locking, including
  read-only validation, validated body errors, wound-wait, lease races,
  cancellation, ambiguous finalization, and write-back;
- two- and three-member direct-commit coordinator rounds, including same-key
  exclusion, oldest-first folding, replay/fallback, per-member uncertainty,
  post-notification remote effects, and fair healthy convergence;
- explicit delayed lock requests, revision-token reuse, attempt renewal with
  stable priority, observer-relative missing/pending grace, crash cleanup, and
  a fair eventually-healthy recovery run; and
- separate B-link split, collection lifecycle, cache-evidence, and
  transaction-object reclamation models with named transaction-core
  assumption/guarantee boundaries; plus a focused owner/wound/GC composition
  witness for the violated anti-resurrection boundary.

The proposed long-lived snapshot-read protocol is intentionally excluded. The
models and history checker cover only strong latest-value transactions.

## Running the pilot

The runner requires Java 11 or newer. It uses TLA+ tools 1.7.4 and verifies the
JAR against the pinned SHA-256 before executing it. With an existing offline
copy:

```bash
TLA2TOOLS_JAR="$HOME/tools/tlaplus/tla2tools.jar" make verify-formal
```

Without `TLA2TOOLS_JAR`, the runner downloads the pinned artifact into
`target/formal-tools/`; this additionally requires `curl`. TLC checkpoints,
counterexample traces, and complete logs are written below `target/formal/`.
Both locations are covered by the repository's existing `target` ignore rule.

Every top-level `tla/*.cfg` file registers one run through its first-line
`@verify-formal` comment. The runner discovers the files in deterministic name
order and validates the complete catalog before starting TLC. Safety and
liveness entries name their wrapper module; mutants and known-protocol
witnesses additionally name the exact invariant and seeded action their trace
must contain. Adding an unannotated or malformed configuration fails the run
instead of silently omitting it. Log and metadata names are the configuration
basename converted from CamelCase to kebab-case.

```tla
\* @verify-formal safety MC_TxCore
\* @verify-formal liveness MC_TxCoreLiveness
\* @verify-formal mutant MC_TxCoreMutants S1_TerminalState ReverseCommitted
\* @verify-formal known-protocol WoundResurrectionWitness WR1_NoResurrectionAfterForeignWound LateOwnerCommit
```

The command intentionally remains separate from `make test-all`: the suite is
not yet a required project toolchain or CI check.

## Modules

| File | Responsibility |
| --- | --- |
| `tla/Common.tla` | Finite sequence helpers used by the history refinement check. |
| `tla/Backend.tla` | The dispatch/effect/uncertainty vocabulary for conditional mutations. |
| `tla/TxCore.tla` | Logged transaction, locking, recovery, write-back, and named invariants. |
| `tla/MC_TxCore.tla` | The finite two-transaction workload and topology mappings. |
| `tla/MC_TxCoreLiveness.tla` | Fair cleanup and equal-priority convergence wrappers for the logged path. |
| `tla/WoundWait.tla` | Explicit distinct-priority wait/wound decisions and wait-graph acyclicity. |
| `tla/MC_TxCoreMutants.tla` | Isolated, deliberately invalid transitions used by seven negative controls. |
| `tla/TxCore*.cfg` | Required safety and expected-counterexample configurations. |
| `tla/DirectCore.tla` | Direct/logless commit, coordinator folding, unresolved requests, fallback, and liveness. |
| `tla/MC_DirectCore*.tla` | Two-/three-member workloads and direct-path negative controls. |
| `tla/DirectCore*.cfg` | Direct safety, liveness, and expected-counterexample configurations. |
| `tla/RecoveryLifecycle.tla` | Delayed CAS, ABA-capable revision tokens, renewal, observer grace, expiry, and crash cleanup. |
| `tla/MC_Recovery*.tla` | Focused recovery safety/liveness workloads and negative controls. |
| `tla/RecoveryLifecycle*.cfg` | Delayed-request, observer, renewal, liveness, and mutant configurations. |
| `tla/BLinkSplit.tla` | Non-root split publication, right-link routing, and reference preservation. |
| `tla/CollectionLifecycle.tla` | Incarnation binding, participant fencing, topology settlement, and drop. |
| `tla/CacheEvidence.tla` | Path-lane ownership, freshness evidence, cancellation, and detached mutation effects. |
| `tla/GarbageCollection.tla` | Reference/horizon eligibility, pending abort, lock release, and final-object deletion. |
| `tla/WoundResurrectionWitness.tla` | Faithful current-protocol counterexample composing a lazy absent holder, foreign abort, finite GC, and same-owner resumption. |
| `tla/TxBLinkComposition.tla` | Shared-state TxCore/B-link split-gate composition and its mismatch mutant. |

The model imports no Rust types or implementation code.

## Semantic inventory

### Abstract database and public outcomes

The sequential database is a map from keys to abstract values. Absence and a
tombstone are separate values in the finite model. A successful logged commit
atomically applies its complete write map. A read-only transaction and a
validated body error occupy one serial point without changing the map. An
explicit abort or definite failure has no logical event.

Public outcomes have these completion rules:

| Outcome | Admissible logical effect |
| --- | --- |
| Success | Exactly once. |
| Validated body error | One read-only serial event. |
| Definite failure or explicit abort | No effect. |
| `InDoubt` | Zero or one effect. |
| Abandoned future/client | Zero or one effect if commit dispatch occurred; otherwise none. |

The logged core also uses `Abandoned` to close a finite prefix when a wounded
attempt would renew its ID and continue the same public operation in
production. Such a state represents no public response; the aborted attempt
has no possible effect. `RecoveryLifecycle` separately checks that renewal
allocates a fresh attempt while preserving the public operation and its
wound-wait priority.

An `InDoubt` notification and cancellation are not upper bounds on the
linearization point. `Backend.tla` therefore separates request dispatch from
its effect, and `ApplyCommit` remains enabled after either public outcome. The
terminal transaction-object predicate prevents a second application; the
model checks the resulting count independently.

### Physical-to-logical refinement

The reduced physical leaf state contains a materialized base value/writer plus
read and write locks. The durable transaction object is absent, pending,
committed, or aborted. The logical view of a key is:

1. the logged value of a committed exclusive holder that has not yet been
   written back; otherwise
2. the materialized base value.

The central invariant is `LogicalView(key) = logical_db[key]`. The final
transaction-object CAS changes both sides atomically. Per-key write-back may
occur in any order but must leave the logical side unchanged. A materialized
logged value always names an immutable committed object; the pilot contains no
logless inline values.

Pending objects are lazy. A transaction may acquire locks while its object is
absent, refresh into pending, or commit directly from absent. Wounding or
expiry may instead install aborted, which the commit CAS cannot replace while
that tombstone remains present. `WoundResurrectionWitness` demonstrates why a
finite tombstone is not a durable fence for a suspended owner. The commit
mutation records the complete expected semantic status and lease rather than an
ever-increasing revision.

### Reduction choices

The model preserves every logical commit ambiguity but reduces recoverable
pre-commit CAS detail:

- `AcquireLeafLostAck` installs the complete leaf lock change without adding a
  local receipt. A later acquire re-reads and recovers it; cancellation can
  leave it for wound or expiry cleanup. The logged state space collapses a
  physically delayed acquisition to `install; cancel`. `RecoveryLifecycle`
  checks the expanded dispatch/effect/reconcile automaton, including an effect
  after crash, precondition failure, semantic-state rewrites with revision-token
  reuse, and at-most-once application. Timeout cleanup in `TxCore` still
  releases every installed lock for the modeled ID, including an
  unacknowledged one; production's receipt-recovery retry remains the interface
  between the two models.
- A wound terminates the attempt inside `TxCore`. `RecoveryLifecycle` composes
  the missing step by allocating a fresh attempt for the same public operation,
  preserving priority, and prohibiting a superseded attempt from committing
  after it observes the wound. That reduction does not establish retirement of
  a suspended pre-materialization owner; the focused resurrection witness keeps
  that owner live across GC instead.
- An older contender may create an aborted tombstone for a younger holder whose
  pending object is still absent. Missing-object grace applies to
  observer-driven expiry, not to the wound-wait priority rule. The pilot reduces
  the observer-relative progress timer to one saturating expiry choice and does
  not verify its exact duration.
- A failed optimistic read-only/error validation sets `lock_reads`, reruns the
  body, acquires shared locks for the complete read set, and validates again.
  A successful read-only attempt commits the lock-carrying no-op transaction;
  a validated body error records its read-only serial point, durably aborts the
  lock carrier, discards staged writes, and then releases the locks.
- Fixed keys encode absence and tombstones as values. A put over absence and an
  overwrite intentionally share one exclusive lock mode because their
  compatibility table is identical in this domain. Key membership/phantom
  behavior is checked by normalized scans in the implementation history
  workload; catalog incarnation creation is checked in `CollectionLifecycle`.
- The reclamation clock is not modeled in `TxCore`. The focused resurrection
  witness adds only recent/expired tombstone classes, enough to refute a finite
  retention fence without assigning them wall-clock durations. An unresolved
  final mutation can remain unresolved indefinitely, which conservatively
  admits ADR-057's public `InDoubt` result and never repairs an absent decision.
  ADR-057's individual status-read and pending-to-redispatch recovery steps are
  collapsed into the model's acknowledged, clean-precondition, and unresolved
  branches.
- Time is a finite saturating class. It selects refresh-versus-expiry races; no
  wall-clock duration is inferred from its numeric value. Observer progress is
  a one-way bounded token within an attempt and never wraps; reusable backend
  revision tokens are modeled separately.
- Backend revision tokens are absent from `TxCore`, whose CAS compares complete
  reduced semantic state. `RecoveryLifecycle` adds finite reusable tokens and
  permits equivalent leaf rewrites before a delayed request applies. The
  provider guarantee that an equivalent predicate remains semantically safe is
  still trusted; monotonic revisions are never assumed.

These reductions are assumptions of this pilot, not claims that the omitted
implementation paths have been verified.

## Trusted environment assumptions

Safety is conditional on the following storage and environment contract:

1. A point read and conditional mutation are linearizable for one object.
2. A definitive conditional success performs exactly its requested effect.
3. A clean precondition response proves that effect did not occur.
4. An unavailable mutation may have no effect or exactly one conditional
   effect; it is never evidence of definite failure.
5. A dispatched request abandoned locally may still resolve remotely.
6. Revisions are equality evidence, not monotonic counters; a semantically
   equivalent state may satisfy the predicate again.
7. A crash destroys volatile client state but preserves acknowledged and
   possibly-landed backend changes.
8. Transaction IDs are unique and priority comparisons are stable. Uniqueness
   does not imply that the same owner retires within a reclamation horizon; the
   resurrection witness deliberately retains one identity across that horizon.
9. Only conforming GlassDB clients mutate protocol objects.

The safety configurations do not assume an eventually healthy backend or fair
scheduling. The separately named liveness configurations add both assumptions
explicitly. Logged equal-priority completion additionally uses strong fairness
for intermittent leaf admission and timeout actions; weak fairness alone is
enough to select sorted fallback but not to prevent a retry loop after it.

## Modeled transitions

The next-state relation explores all interleavings of:

- invoke, individual point reads, body completion, explicit abort, and
  read-only/error validation or retry;
- atomic same-leaf lock folding and partial cross-leaf acquisition;
- acknowledged and lost-ack lock installation;
- older-wounds-younger conflict handling, blocked younger/equal acquisition,
  timeout release, and globally sorted reacquisition for equal-priority cycles;
- lazy absent-to-pending materialization, lease refresh, time advance, and
  absent/pending expiry;
- validation only after all required locks are held, or body retry while those
  locks remain held;
- final-CAS dispatch, effect, clean precondition loss, acknowledgement,
  `InDoubt`, abandonment, and post-response effect; and
- independent per-key write-back, read-lock release, and aborted-lock cleanup.

## Checked safety properties

The normal configurations check each property separately:

| ID/operator | Property |
| --- | --- |
| `S0_TypeOK` | Every state and receipt has the declared finite shape. |
| `S1_TerminalState` | Committed and aborted are disjoint, immutable terminal decisions. |
| `S2_LockCompatibility` | Reads may share; a write is exclusive; local receipts name installed locks. |
| `S3_DurableReferences` | Materialized and committed-holder writers resolve to complete committed values. |
| `S4_Refinement` | The physical logical view equals the ghost database in every state. |
| `S5_LoggedAtomicity` | The commit event records its complete intended write set and occurs at most once; S4 and S12 jointly check atomic visibility. |
| `S9_PostLockValidation` | Every linearized read matches its writer/value pre-state, and each logged commit validated while holding all locks. |
| `S10_CommittedCannotAbort` | Wound and expiry cannot reverse a winning commit; this protocol-specific statement intentionally overlaps S1. |
| `S11_UncertaintyIsConservative` | Definite failure has no possible effect; unresolved outcomes remain optional. |
| `S12_StrictSerializableHistory` | Replaying the unique logical order reproduces reads, real-time edges, and `logical_db`. |

`S12` uses the actual logical commit sequence as a serial witness, recursively
replays it from `InitialDb`, and checks every recorded read against the prefix
state. This is a refinement check, not merely a final-value assertion.

The logged-path temporal wrappers additionally check eventual committed
write-back/read-lock release, the absence of a two-member wait cycle under
distinct wound-wait priorities, weakly fair selection of sorted fallback for
equal priorities, and strongly fair completion of at least one bounded
contender. The last property is deliberately stronger than the first three and
uses the explicit fairness boundary described below.

### Direct commit and coordinator

`DirectCore` keeps public operations distinct from one physical coordinator
CAS. A round folds members in stable oldest-first order, stages at most one
member for each key, and may carry disjoint-key members in the same mutation.
Each staged member owns its own result and sticky uncertainty bit; a skipped
member cannot inherit another member's unavailable result.

The unavailable path snapshots the exact staged members, values, and
precondition in `pending`. That request may resolve without effect or apply
after the caller has returned `InDoubt` or abandoned the round. While it is
unresolved, the member cannot enter replay or locked fallback. An exact writer
marker proves success; the exact original predecessor permits a safe restage;
a moved predecessor leaves the result in doubt. Direct commits remain logless,
while a genuine ineligible blind write may enter the ordinary logged fallback.

| Operator | Property |
| --- | --- |
| `D0_TypeOK` | Coordinator, pending-request, client, and leaf state have their declared finite shapes. |
| `S6_DirectAtomicityAndExclusion` | Each public operation applies at most once across direct/fallback paths, staged keys are disjoint, fold order is stable, and replaying the logical order reproduces the leaf. |
| `S6_DirectWritersAreLogless` | A direct writer has no transaction object/lock marker; a fallback writer has both. |
| `S7_ReplayIsEffectFree` | Replay has no direct effect, unresolved request, lock, object, or commit marker. |
| `S8_PerMemberUncertainty` | Only staged members become uncertain, and uncertainty cannot be downgraded to replay/fallback. |
| `AllEventuallyComplete` | With a fair scheduler and eventually definitive backend, every bounded direct member completes. |

### Recovery lifecycle

`RecoveryLifecycle` expands interfaces intentionally collapsed by `TxCore`.
A lock CAS records its expected semantic holder and a reusable revision token,
then separates dispatch, unavailable response, remote effect, acknowledgement,
and read-back recovery. Equivalent leaf rewrites can cycle the token back to
the request value before a delayed effect. An applied request is nevertheless
counted at most once and every local receipt names a physical lock.

The same model represents lazy absent/pending objects, absolute pending leases,
observer-relative no-progress grace, crash, terminal lock release, and a wound
renewing the attempt ID while preserving public priority. Its liveness wrapper
stops injecting failures, applies weak fairness only after the environment
becomes healthy, and checks that a crashed holder's lock is eventually
released and an observing waiter eventually consumes the final state.

`RecoveryLifecycle` deliberately has no final-object deletion and makes the
victim observe an atomic wound before renewal. It therefore does not cover a
suspended original owner resuming after a foreign abort record has been
collected; `WoundResurrectionWitness` isolates that missing composition.

| Operator | Property |
| --- | --- |
| `R0_TypeOK` | Request, observer, attempt, time, revision, receipt, and decision state is well formed. |
| `R1_TerminalImmutable` | Committed and aborted decisions are disjoint and agree with durable status. |
| `R2_DelayedRequestAtMostOnce` | A delayed conditional request has at most one remote effect, including after crash and token reuse. |
| `R3_ReceiptsArePhysical` | Every lock receipt names a currently installed physical lock. |
| `R4_RenewalPreservesPublicPriority` | Every renewed attempt retains its public operation's original wound-wait priority. |
| `R5_ExpiryHasObserverEvidence` | Expiry requires an unchanged absent/pending observation for the full grace class or an expired absolute lease. |
| `R6_PublicEffectAtMostOnce` | Renewal cannot give one public operation two logical commits. |
| `R7_SupersededAttemptsCannotCommit` | Only the current attempt for a public operation may commit. |

### Composed subprotocols

The later protocol models remain separate to avoid multiplying unrelated state
spaces. Each module names both the facts it assumes from `TxCore` and the
`TxCore*Guarantee` it exports back:

- `BLinkSplit` models copy, atomic source-shrink/right-link publication, and
  parent separator publication. `TxCoreSplitGuarantee` preserves exactly one
  authoritative owner, stale-parent routing, every entry/reference, prepared
  sibling invisibility, and the source high-key bound.
- `CollectionLifecycle` models prepare/publish, incarnation-bound handles,
  ordinary data/topology participants, freeze, per-node fence, abortable drop,
  final parent removal, and name rebinding. `TxCoreLifecycleGuarantee` makes the
  parent binding authoritative and rejects commits through a stale incarnation.
- `CacheEvidence` models one path lane, lower-bound sequence points, definitive
  reads/mutations, clean conflicts, unavailable responses, cancellation, and a
  detached mutation that can apply remotely after lane ownership ends.
  `TxCoreCacheGuarantee` allows reuse only for definitive evidence newer than
  every invalidation.
- `GarbageCollection` models ordinary logged objects, lock/value references,
  recent/old horizons, forced pending abort, lock release, and final deletion.
  `TxCoreGcGuarantee` preserves referenced/recent objects and forbids guessed
  recreation of a deleted final decision. Its normal graph deliberately reduces
  away the lazy holder-before-object window, so this guarantee assumes the
  same-owner resurrection path is absent.
- `WoundResurrectionWitness` removes that assumption for the current committed
  four-status protocol. It preserves the original owner across lazy holder
  publication, a foreign `Aborted` wound, lock release, finite-horizon deletion,
  and create-if-absent finalization. TLC then reaches the known terminality
  violation.

These are separate assume/guarantee contract checks, not one monolithic state
graph.
Root B-link height growth, arbitrary directory fanout, multiple cache paths,
and wall-clock horizon magnitudes are data-independent reductions documented in
the individual modules. `TxBLinkComposition` additionally couples the highest
risk adjacent boundary through shared leaf-entry/reference state: the split
gate must exclude lock installation and write-back between sibling copy and
atomic source shrink/right-link publication. The wound-resurrection witness is
the focused composition check for the lazy-owner/GC boundary, and it currently
fails as required.

### Implementation-history refinement

The simulation-only history checker executes bounded public transaction
programs through the real `Database`/`Transaction` API. It records every body
retry, ordered point read, canonical unordered concurrent-read group,
normalized key-range scan, local write/delete, validated user error, response,
`InDoubt`, and abandoned invocation. Its oracle is an independent abstract map
with local overlays, not transaction objects, leaf entries, coordinator
outcomes, or production resolver code.

An exact deterministic search chooses legal zero/one completions for uncertain
operations and a total order respecting definitive real-time edges. It accepts
only if replay explains every read and scan and reaches the quiescent final
state. Unit fixtures cover legal reorderings, lost update, fractured commit,
stale read, phantom membership, real-time inversion, read-your-writes,
validated errors, torn concurrent-read groups, and double application. The
memoized search is also cross-checked against a simple brute-force enumerator
over small generated histories. `HistoryWorkload` runs under tape and PCT
scheduling, transport faults, crashes, slow mutations, and both cache modes;
`fuzz/fuzz_targets/history.rs` provides the seeded fuzz entry point.
Long-lived snapshot reads remain outside this checker.

## Finite configurations and baseline

The required runs differ along the two dimensions that affect locking:

| Configuration | Leaf mapping | Priorities | Generated states | Distinct states | Pilot runtime |
| --- | --- | --- | ---: | ---: | ---: |
| `TxCoreSameLeafDistinct.cfg` | Both keys on one leaf | older/younger | 3,726,164 | 981,944 | 55 s |
| `TxCoreSameLeafEqual.cfg` | Both keys on one leaf | equal | 1,087,463 | 284,869 | 19 s |
| `TxCoreCrossLeafDistinct.cfg` | One key per leaf | older/younger | 6,846,744 | 1,566,901 | 2 min 6 s |
| `TxCoreCrossLeafEqual.cfg` | One key per leaf | equal | 5,233,832 | 1,228,624 | 1 min 47 s |

These measurements were recorded on 2026-08-10 with TLC 1.7.4, OpenJDK 21,
one TLC worker, fingerprint polynomial 0, and no depth bound. Counts are stable;
runtime is a local reference rather than a budget guarantee. The same-leaf
distinct run includes multi-key writers, a mixed transaction with a shared
read lock and one write lock, read-only validation, a validated error with
discarded staged writes, and explicit abort. The other configurations use one
multi-key writer plus the mixed read/write program because the public outcome
classes add no topology behavior.

The extension configurations use the same TLC/JVM settings:

| Configuration | Purpose | Generated | Distinct |
| --- | --- | ---: | ---: |
| `TxCoreLoggedCleanupLiveness.cfg` | Fair committed write-back and read-lock cleanup | 68,702 | 21,854 |
| `WoundWaitSafety.cfg` | Distinct-priority wait/wound policy | 4 | 3 |
| `TxCoreEqualPriorityLiveness.cfg` | Weakly fair selection of sorted fallback | 1,865 | 824 |
| `TxCoreEqualPriorityCompletionLiveness.cfg` | Strongly fair completion of one bounded contender | 1,865 | 824 |
| `DirectCore2Safety.cfg` | Two same-key RMW members, uncertainty and fallback | 47,543 | 20,274 |
| `DirectCore3Safety.cfg` | Three mixed members over two keys | 1,536,235 | 585,662 |
| `DirectCore2Liveness.cfg` | Fair eventually-healthy direct convergence | 92,849 | 40,548 |
| `RecoveryLifecycle.cfg` | One delayed request with revision reuse | 109 | 44 |
| `RecoveryLifecycleObserver.cfg` | Two-attempt observation, crash, grace, and cleanup | 30,379 | 10,288 |
| `RecoveryLifecycleRenewal.cfg` | Wound, renewal, and superseded-attempt safety | 298 | 153 |
| `RecoveryLifecycleLiveness.cfg` | Fair reclamation and final-state observation | 901 | 374 |
| `BLinkSplitSafety.cfg` | One non-root split publication | 4 | 4 |
| `CollectionLifecycleSafety.cfg` | Two incarnations and two nodes | 11,825 | 2,795 |
| `CacheEvidenceSafety.cfg` | Two operations on one cache/path lane | 4,822 | 2,742 |
| `GarbageCollectionSafety.cfg` | Two logged objects and two reference kinds | 1,157 | 289 |
| `TxBLinkCompositionSafety.cfg` | Shared leaf/reference state across TxCore and split | 57 | 40 |

Every row exhausted its complete finite graph with zero states left on the
queue. Temporal rows additionally completed TLC's full liveness pass. These
counts are deterministic for the recorded tool, seed, fingerprint polynomial,
and one-worker configuration; runtime remains machine-dependent.

## Seeded counterexamples

`make verify-formal` runs two kinds of required failure: synthetic negative
controls that validate invariant sensitivity, and a faithful witness for a
known defect in the current protocol. A syntax error, tool failure, unexpected
invariant, missing seeded action, or unexpectedly successful counterexample
fails the runner.

| Mutant | Required violation | Counterexample |
| --- | --- | --- |
| `TerminalReversalSpec` | `S1_TerminalState` | Replaces a committed record with aborted. |
| `EarlyPublicationSpec` | `S4_Refinement` | Publishes one logged leaf value while the transaction is still non-final. |
| `MissingValidationSpec` | `S9_PostLockValidation` | Commits observations made stale before lock acquisition. |
| `MissingValidationSpec` with same-value input | `S9_PostLockValidation` | Changes only the writer token, then commits without revalidating it. |
| `UnlockedRetryErrorSpec` | `S9_PostLockValidation` | Returns a read-derived error from an escalated retry without the promised shared-lock barrier. |
| `ExpiryAfterCommitSpec` | `S10_CommittedCannotAbort` | Materializes pending, commits, advances beyond that lease, then lets expiry reverse the decision. |
| `UncertaintyMisclassificationSpec` | `S11_UncertaintyIsConservative` | Returns definite failure while the dispatched commit can still apply. |

The direct/coordinator controls exercise the Stage 2 boundaries:

| Mutant | Required violation | Counterexample |
| --- | --- | --- |
| `BlindRestageSpec` | `S6_DirectAtomicityAndExclusion` | Treats a blind member whose uncertain marker was superseded as fresh and publishes it twice. |
| `SameKeyExclusionSpec` | `S6_DirectAtomicityAndExclusion` | Stages two same-key logless members in one coordinator CAS. |
| `UncertaintyAttributionSpec` | `S8_PerMemberUncertainty` | Copies a staged member's uncertainty to a skipped peer. |
| `UncertainReplaySpec` | `S7_ReplayIsEffectFree` | Returns replay while that member still owns an unresolved request. |
| `FoldOrderSpec` | `S6_DirectAtomicityAndExclusion` | Folds youngest-first and breaks the stable serial witness. |
| `PendingFallbackSpec` | `S6_DirectAtomicityAndExclusion` | Starts logged fallback while a detached direct request can still apply, producing the same public operation twice. |

The recovery controls check terminality, delayed effects, renewal, and grace:

| Mutant | Required violation | Counterexample |
| --- | --- | --- |
| `TerminalMutantSpec` | `R1_TerminalImmutable` | Reverses a committed attempt to aborted. |
| `RequestReplayMutantSpec` | `R2_DelayedRequestAtMostOnce` | Applies one captured backend request twice; revision reuse is explored by the normal delayed-request graph. |
| `RenewalPriorityMutantSpec` | `R4_RenewalPreservesPublicPriority` | Gives a renewed attempt a different wound-wait priority. |
| `PrematureExpiryMutantSpec` | `R5_ExpiryHasObserverEvidence` | Aborts an absent/pending holder before observer grace or lease evidence exists. |

The composed contracts, GC phases, and wound-wait policy have focused negative
controls:

| Mutant action | Required violation | Counterexample |
| --- | --- | --- |
| `ShrinkWithoutRightLink` | `BS2_RoutingReachesOwner` | Shrinks the source before installing the sibling link. |
| `CommitThroughStaleHandle` | `CL4_StaleIncarnationCannotCommit` | Uses an old handle after its name is rebound to a fresh collection ID. |
| `AbandonWithoutInvalidation` | `CE3_UncertainKnowledgeNotReusable` | Releases a cache lane but retains knowledge older than a detached mutation. |
| `GuessDeletedObjectCommitted` | `GC4_DeletedFinalNeverRecreated` | Recreates a reclaimed final object by guessing that it committed. |
| `ReleasePendingLocks` | `GC3_PendingAbortedBeforeRelease` | Removes a lock reference before the pending decision is durably aborted. |
| `DeleteReferencedFinal` | `GC1_ReferencedObjectsRemainPresent` | Deletes a final transaction object while a physical lock still references it. |
| `AcquireWhileSplitGateHeld` | `TCBS4_LiveTxReferenceReachable` | Installs a lock after sibling copy, so source shrink loses the live transaction reference. |
| `WaitInsteadOfWound` | `WW1_WaitGraphAcyclic` | Makes the older requester wait after the younger already waits, creating a two-node cycle. |

The deterministic mutant traces are retained in
`target/formal/*mutant*.log`. They are separate wrapper actions rather than a
bug-mode constant in the intended protocol.

### Known protocol counterexample

`WoundResurrectionWitness.cfg` models the current committed four-status
transaction-object lifecycle; it contains `Absent`, `Pending`, `Aborted`, and
`Committed`, and deliberately contains no proposed repair state. Its ghost wound
and commit histories only observe real actions and never enable them.

TLC reaches this trace:

1. `PublishLazyLock` publishes the owner's ID while its object is absent.
2. `SuspendOwner` leaves the same operation live.
3. `ForeignAbortWound` creates an ordinary `Aborted` object without retiring
   the owner.
4. `ReleaseAbortedLock` removes the durable holder only after the abort lands.
5. `AdvanceTombstoneAge` crosses the finite retention horizon.
6. `DeleteExpiredAbort` returns the physical object path to `Absent`.
7. `ResumeOwner` resumes the original operation under the same ID.
8. `LateOwnerCommit` performs its create-if-absent final write.

The final action violates `WR1_NoResurrectionAfterForeignWound`. TLC explores
27 states, finds 20 distinct states, and leaves one queued state at the expected
safety violation (depth 9). The trace is retained in
`target/formal/wound-resurrection-witness.log`. This is not a mutant and is not a
model of any proposed fix; it is executable evidence that finite `Aborted`
retention does not fence an unboundedly suspended owner.

## Hand histories

These examples define how public histories map into the model:

### Successful logged commit

1. The body reads `k1 = absent` and stages writes to `k1` and `k2`.
2. Both dependencies are locked and their writer tokens validate.
3. The final object CAS changes both logical keys in one `ApplyCommit` step.
4. Success is acknowledged; either key may be written back first.

The history contains exactly one transaction at step 3.

### Explicit abort

1. The body returns its explicit abort result before commit dispatch.
2. The caller receives a definite failure.

The operation is absent from the serial history and has no durable effect.

### Validated body error

1. The body reads `k1` and derives a user error from that observation.
2. The read-only validation barrier confirms the observed writer.
3. The error returns without applying any staged writes.

The history contains one read-only event explaining the observation.

### Lost commit acknowledgement

1. The final CAS is dispatched and may apply.
2. Its acknowledgement is unavailable.
3. If the committed status is established while the caller is active, success
   is returned; otherwise the caller may receive `InDoubt`.

The history contains zero or one commit, never two.

### Completion after notification

1. The final CAS is dispatched.
2. The caller receives `InDoubt`, or abandons the future, while the request is
   unresolved.
3. The backend effect lands later and changes the logical database once.

The notification is not a serialization upper bound. Later strong reads and
the final state determine whether the optional completion occurred.

## Clarifications found while modeling

The first executable history invariant incorrectly required a definitively
aborted operation to appear before a later read-only operation. TLC produced the
minimal valid trace `abort; invoke read; validate read`, showing that the rule
contradicted the contract that definite failures have zero logical events. The
real-time prefix now contains only definitively completed operations that have a
logical event: successes and validated body errors.

The model also makes two protocol refinements explicit rather than inheriting
older ADR wording:

- a short logged transaction can commit `Absent -> Committed`; pending is lazy,
  and an earlier `Aborted` wound defeats both refresh and commit only while its
  tombstone remains present; and
- after an ambiguous logged finalization, absence is never repaired. Failure to
  establish the decision remains `InDoubt`, as required by ADR-057.

The direct mutant exposed a model-to-code mismatch: the Rust direct resolver
checked sticky uncertainty only when eligibility declined, so an uncertain
blind overwrite could remain eligible against a superseding writer and stage a
second effect. The production resolver now carries its exact preflight
predecessor into recovery. It may restage only while that predecessor is still
current; otherwise it returns `InDoubt` before staging. A deterministic Rust
regression covers unchanged-predecessor retry, moved-predecessor refusal, and
fresh blind last-writer-wins behavior.

The first recovery liveness run also found that a late acquire acknowledgement
could move a crashed attempt back to `Ready`. `AcknowledgeAcquire` now requires
an active acquiring/waiting phase; otherwise it only settles the internal
request and leaves crash recovery responsible for the installed lock.

The focused owner/wound/GC composition then invalidated an assumption shared by
the earlier core and standalone GC models. Transaction-ID uniqueness does not
prove owner retirement: one operation can publish a lazy holder, remain
suspended beyond every finite abort-retention horizon, and later resume under
the same ID. Keeping the physical deletion and original owner in one state graph
turns the previously synthetic recreation mutant into the causal eight-step
counterexample above.

The logged liveness model found a second useful boundary: weak fairness selects
the sorted fallback in an equal-priority cycle, but still permits one contender
to acquire the lowest leaf and repeatedly time out behind an intermittently
enabled peer. The separate completion check therefore states strong fairness
for leaf admission and timeout explicitly. In production, the corresponding
obligation is a persistent request/backoff guarantee; the pilot does not claim
unconditional per-client starvation freedom.

## Acceptance status and exclusions

The bounded Stage 0-4 exploration is executable end to end, but the known
wound/GC failure prevents treating it as a successful protocol-verification
gate:

- every intended-safe graph listed above exhausts with no error and no depth
  bound;
- all fair eventually-healthy liveness graphs satisfy their stated temporal
  properties, with the equal-priority completion assumption called out above;
- every seeded mutant reaches its named bad action and violates its expected
  invariant;
- the faithful four-status wound-resurrection witness reaches
  `LateOwnerCommit` and violates its anti-resurrection invariant;
- the implementation-history checker rejects its seeded public anomalies and
  replays deterministic fuzz inputs under the real engine; and
- the model decomposition, implementation-refinement boundary, and known-bug
  trace received independent read-only review.

This is finite-state evidence, not a proof of arbitrary deployments. The
following remain explicit trust or scale boundaries rather than hidden claims:

- S3/GCS single-object linearizability, conditional-error classification,
  bounded clock error, unique IDs, and conforming writers are environment
  assumptions. The existing in-process S3/GCS adapter suites exercise their
  mappings, but this command does not contact live cloud services.
- The composed models check named assumption/guarantee interfaces instead of a
  state-space product of every protocol and production data size. The
  TxCore/B-link split-gate seam has a clean shared-state composition check.
  Collection and cache retain standalone contracts. The focused lazy-owner/GC
  composition instead exposes that finite `Aborted` retention does not establish
  the GC anti-resurrection guarantee for the current protocol.
- DirectCore reduces logged fallback to one atomic transition rather than
  composing it with all TxCore phases. Mixed coordinator rounds containing
  acquisition, direct, write-back, release, and GC resolvers are covered by
  production simulation, not by one formal state-space product.
- CacheEvidence covers one volatile path lane. Persistent-L2 session chains,
  write-behind, and multiple simultaneously open database instances are sampled
  by existing cache-mode tests but are not an exhaustive formal product.
- Collection preparation/seed/catalog publication and GC candidate-feed,
  back-reference completeness, and cleanup liveness remain implementation
  contracts around their checked lifecycle and eligibility state machines.
  Durable owner retirement or an equivalent anti-resurrection fence is an
  unresolved protocol obligation, not a trusted fact supplied by this model.
- The Rust checker samples deterministic schedules and faults; TLC exhausts
  only the recorded finite domains. Neither is a deductive proof of Tokio,
  Rust, the object-store clients, or arbitrary user-closure side effects.
- Kani was evaluated but not adopted for this pilot: no installed verifier or
  stable production-used pure coordinator/lifecycle kernel justified adding an
  unexecuted proof-only shadow. The design explicitly permits declining Stage 5
  tooling without weakening the independent TLA+/history layers. Loom was
  likewise not added because the selected protocol claims are exercised at the
  deterministic executor and backend-CAS boundaries, not presented as C11
  memory-model proofs.
- Long-lived snapshot reads, their history retention, and their checkpoint
  cache semantics are excluded by request. Ordinary latest-value point reads,
  concurrent read groups, and normalized transaction scans are included.

No CI job or required `test-all` dependency is added. An ADR should still wait
for an explicit decision to make the manual suite a maintained project gate.
