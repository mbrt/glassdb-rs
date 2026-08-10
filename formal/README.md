# Formal verification

## Status and boundary

This directory contains the bounded verification suite described in
[the formal protocol verification design](../docs/designs/formal-protocol-verification.md).
It implements the Stage 0 semantic inventory, the Stage 1 logged transaction
core, Stage 2 direct/coordinator behavior, the recovery interfaces that the
first slice reduced away, and the Stage 4 composed subprotocol models. The
Stage 3 implementation-history checker lives in
[`crates/glassdb/src/sim/history.rs`](../crates/glassdb/src/sim/history.rs).
The manual Stage 5 Kani harnesses are colocated with the production lifecycle,
median-policy, and split-finalization kernels under `#[cfg(kani)]`.

This remains an exploration rather than an accepted architecture gate. It is
manually invoked and is not run by CI.

The owner/wound/GC composition includes ADR-059's pinned `Wounded` marker and
owner acknowledgement protocol. The intended graph now excludes resurrection;
a historical mutant that writes a foreign wound directly as GC-eligible
`Aborted` still reproduces the original failure.

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
  check for the pinned-wound anti-resurrection boundary.

The proposed long-lived snapshot-read protocol is intentionally excluded. The
models and history checker cover only strong latest-value transactions.

## Running the pilot

### TLA+/TLC

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
liveness entries name their wrapper module; expected-counterexample entries
additionally name the exact invariant and seeded action their trace must
contain. Adding an unannotated or malformed configuration fails the run
instead of silently omitting it. Log and metadata names are the configuration
basename converted from CamelCase to kebab-case.

```tla
\* @verify-formal safety MC_TxCore
\* @verify-formal liveness MC_TxCoreLiveness
\* @verify-formal mutant MC_TxCoreMutants S1_TerminalState ReverseCommitted
```

The command intentionally remains separate from `make test-all`: the suite is
not yet a required project toolchain or CI check.

### Kani

The Stage 5 runner requires exactly Kani 0.67.0. Installation is explicit
because Kani and its supporting tools are not part of the normal Rust
toolchain:

```bash
cargo install --locked kani-verifier --version 0.67.0
cargo kani setup
make verify-kani
```

`hack/verify-kani.sh` rejects a missing, incomplete, or different Kani version
before invoking the Kani proxy, so the proxy cannot trigger first-run setup.
The runner requires GNU `timeout` (named `timeout` or `gtimeout`), serializes
invocations, and checks the complete seven-harness catalog. It invokes one exact
fully-qualified harness at a time, requires every coverage point and the exact
success mode, and requires each `#[kani::should_panic]` negative control to
report its one named failed assertion. Complete logs live under `target/kani/`;
stale logs are removed at the start of a run. When GNU `/usr/bin/time` is
available, elapsed time and peak resident memory are included in each log.

The production-used kernels checked by this pilot are:

- the exact transaction-record lifecycle relation over all 25 normalized-state
  pairs, plus transition validation over all 36 encoded input pairs including
  invalid persisted `Unknown`;
- the shared median-index policy for every machine cardinality representable by
  `usize` from 2 upward; and
- `finish_split_metadata`, the production kernel called by
  `Node::finish_split` after a body has already been partitioned. Presence of
  the old high-key, right link, and delete intent is symbolic, as is the
  membership version. It checks B-link inheritance, complete source-lock
  preservation, and removal of transient holders from the new sibling without
  invoking a container split.

Kani 0.67.0 did not make `std::collections::BTreeMap::split_off` a maintainable
pilot target: a four-entry harness exceeded the 300-second cutoff, and a
reduced fixed ordered three-entry harness still produced no result in a
90-second feasibility run. Those content-conservation, ordering, disjoint
ownership, promoted-boundary, and routing properties remain covered by the
deterministic Rust tests for `Shard::split_off_median`,
`IndexNode::split_off_median`, and `Node::split`. They are an explicitly
declined Kani target, not a proof result; a timeout provides no evidence that
the property holds or fails.

An upper-median stub is a representative policy-maintenance check: the same
nonempty, balanced-cardinality contract must hold without editing the proof.
Three negative controls stub the production kernels to permit direct `Wounded`
reclamation, create an empty lower split half, or retain transient lock holders
in split metadata. These stubs exist only under both `cfg(kani)` and the
`proof-mutants` feature; they are not production alternatives.

#### Stage 5 Kani metrics

These local reference measurements come from a clean verifier build in the
final Kani 0.67.0 run on 2026-08-10. Elapsed time is the complete per-harness
command measured by GNU `time`; peak RSS is reported in KiB. The colocated Kani
modules contain 268 lines, plus 238 lines in the fail-closed runner.

| Harness | Role | Elapsed time | Peak RSS |
| --- | --- | ---: | ---: |
| `lifecycle_transition_validation_matches_policy` | positive lifecycle proof | 9.78 s | 393,664 KiB |
| `median_split_index_keeps_bounded_halves_balanced` | positive full-cardinality median policy proof | 1.70 s | 329,580 KiB |
| `node_finish_split_preserves_b_link_bounds_and_lock_ownership` | positive split-finalization metadata proof | 13.91 s | 577,468 KiB |
| `median_split_contract_survives_upper_bias` | upper-median maintenance check | 1.65 s | 329,132 KiB |
| `lifecycle_rejects_direct_wounded_reclamation_mutant` | expected-panic lifecycle mutant | 1.90 s | 361,600 KiB |
| `median_split_contract_rejects_empty_lower_mutant` | expected-panic split mutant | 1.44 s | 328,884 KiB |
| `node_finish_split_rejects_inherited_lock_holders_mutant` | expected-panic split-finalization mutant | 10.88 s | 600,340 KiB |

Like `make verify-formal`, `make verify-kani` remains separate from
`make test-all` and is not invoked by CI.

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
| `tla/WoundRetirement.tla` | ADR-059 composition of lazy holder publication, pinned foreign wounds, owner retirement, acknowledgement, and GC. |
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
read and write locks. In `TxCore`, the durable transaction object is absent,
pending, committed, or aborted. The logical view of a key is:

1. the logged value of a committed exclusive holder that has not yet been
   written back; otherwise
2. the materialized base value.

The central invariant is `LogicalView(key) = logical_db[key]`. The final
transaction-object CAS changes both sides atomically. Per-key write-back may
occur in any order but must leave the logical side unchanged. A materialized
logged value always names an immutable committed object; the pilot contains no
logless inline values.

Pending objects are lazy. A transaction may acquire locks while its object is
absent, refresh into pending, or commit directly from absent. `TxCore` collapses
terminal-for-commit `Wounded` and acknowledged `Aborted` because it has no
transaction-object deletion. `WoundRetirement` refines that reduction: a
foreign wound installs pinned `Wounded`, only proven owner retirement permits
`Wounded -> Aborted`, and only `Aborted` is GC-eligible. The commit mutation
records the complete expected semantic status and lease rather than an
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
  after it observes the wound. `WoundRetirement` separately keeps a suspended
  pre-materialization owner live while a foreign actor installs `Wounded`, and
  checks the retirement-before-acknowledgement ordering.
- `TxCore` represents a foreign wound of an absent/pending holder as aborted;
  this is safe only because that model never reclaims transaction objects.
  `WoundRetirement` restores the production distinction between pinned
  `Wounded` and GC-eligible `Aborted`. Missing-object grace applies to
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
- The reclamation clock is not modeled in `TxCore`. `WoundRetirement` adds one
  saturating age class and checks that elapsed time never deletes `Wounded`,
  while owner acknowledgement resets the ordinary `Aborted` retention horizon.
  An unresolved final mutation can remain unresolved indefinitely, which
  conservatively admits ADR-057's public `InDoubt` result and never repairs an
  absent decision.
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
   pinned-wound check and its negative control retain one identity across that
   horizon.
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
victim observe an atomic wound before renewal. `WoundRetirement` supplies the
missing deletion boundary and checks that the original owner cannot resume past
a foreign wound until it retires and acknowledges that identity.

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
  recreation of a deleted final decision. Its normal graph reduces away the
  lazy holder-before-object window and relies on ADR-059's pinned-wound contract.
- `WoundRetirement` composes that contract explicitly. It preserves the original
  owner across lazy holder publication and arbitrary bounded delay, permits
  cleanup while `Wounded` remains present, requires retirement before
  acknowledgement as `Aborted`, and permits deletion only after the fresh abort
  horizon. Same-identity resurrection remains unreachable.

These are separate assume/guarantee contract checks, not one monolithic state
graph.
Root B-link height growth, arbitrary directory fanout, multiple cache paths,
and wall-clock horizon magnitudes are data-independent reductions documented in
the individual modules. `TxBLinkComposition` additionally couples the highest
risk adjacent boundary through shared leaf-entry/reference state: the split
gate must exclude lock installation and write-back between sibling copy and
atomic source shrink/right-link publication. `WoundRetirement` is the focused
composition check for the lazy-owner/GC boundary; its fixed graph
now exhausts cleanly, while the superseded foreign-`Aborted` behavior remains a
negative control.

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
| `TxCoreSameLeafDistinct.cfg` | Both keys on one leaf | older/younger | 3,726,164 | 981,944 | 57 s |
| `TxCoreSameLeafEqual.cfg` | Both keys on one leaf | equal | 1,087,463 | 284,869 | 21 s |
| `TxCoreCrossLeafDistinct.cfg` | One key per leaf | older/younger | 6,846,744 | 1,566,901 | 2 min 6 s |
| `TxCoreCrossLeafEqual.cfg` | One key per leaf | equal | 5,233,832 | 1,228,624 | 1 min 48 s |

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
| `WoundRetirementSafety.cfg` | Pinned foreign wound, owner acknowledgement, and GC | 39 | 26 |
| `TxBLinkCompositionSafety.cfg` | Shared leaf/reference state across TxCore and split | 57 | 40 |

Every row exhausted its complete finite graph with zero states left on the
queue. Temporal rows additionally completed TLC's full liveness pass. These
counts are deterministic for the recorded tool, seed, fingerprint polynomial,
and one-worker configuration; runtime remains machine-dependent.

## Seeded counterexamples

`make verify-formal` requires every synthetic negative control to expose its
named defect. A syntax error, tool failure, unexpected invariant, missing seeded
action, or unexpectedly successful mutant fails the runner.

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
| `ForeignAbortWound` | `WR1_NoResurrectionAfterForeignWound` | Uses the superseded foreign-`Aborted` transition, allowing finite GC and same-identity resurrection. |
| `AcquireWhileSplitGateHeld` | `TCBS4_LiveTxReferenceReachable` | Installs a lock after sibling copy, so source shrink loses the live transaction reference. |
| `WaitInsteadOfWound` | `WW1_WaitGraphAcyclic` | Makes the older requester wait after the younger already waits, creating a two-node cycle. |

The deterministic mutant traces are retained in
`target/formal/*mutant*.log`. They are separate wrapper actions rather than a
bug-mode constant in the intended protocol.

### Pinned wound and owner retirement

`WoundRetirementSafety.cfg` models the five durable lifecycle states from
ADR-059. A foreign actor writes `Wounded`, holder cleanup may proceed, and time
may reach the finite horizon without making the marker deletable. If the owner
returns, it first establishes retirement, then acknowledges
`Wounded -> Aborted`; that transition resets the age before ordinary GC may
delete the record. A dropped/unresolved owner leaves `Wounded` pinned.

TLC exhausts this graph with 39 generated states, 26 distinct states, and zero
queued states at depth 10. The log is retained in
`target/formal/wound-retirement-safety.log`.

`WoundRetirementMutantForeignAbort.cfg` restores the superseded foreign wound
transition. Its trace is:

1. `PublishLazyLock` publishes the original identity while its object is absent.
2. `SuspendOwner` leaves that owner operation live.
3. `ForeignAbortWound` writes GC-eligible `Aborted` without retirement proof.
4. `ReleaseTerminalLock` removes the durable holder.
5. `AdvanceMarkerAge` reaches the finite retention horizon.
6. `ResumeOwner` returns the original operation under the same identity.
7. `DeleteExpiredAbort` returns the physical path to `Absent`.
8. `LateOwnerCommit` recreates it as committed.

The final action violates `WR1_NoResurrectionAfterForeignWound`. TLC generates
60 states, finds 38 distinct states, and leaves one queued state at depth 9.
The trace is retained in
`target/formal/wound-retirement-mutant-foreign-abort.log`.

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
  and ADR-059 makes a foreign wound durably `Wounded` until the owner proves
  retirement and acknowledges it as `Aborted`; and
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

The focused owner/wound/GC composition invalidated the former finite-tombstone
assumption: transaction-ID uniqueness does not prove owner retirement. ADR-059
responds with pinned `Wounded` plus an explicit owner acknowledgement. The
updated graph checks that fix, while `ForeignAbortWound` preserves the original
causal counterexample as a negative control.

The logged liveness model found a second useful boundary: weak fairness selects
the sorted fallback in an equal-priority cycle, but still permits one contender
to acquire the lowest leaf and repeatedly time out behind an intermittently
enabled peer. The separate completion check therefore states strong fairness
for leaf admission and timeout explicitly. In production, the corresponding
obligation is a persistent request/backoff guarantee; the pilot does not claim
unconditional per-client starvation freedom.

## Acceptance status and exclusions

The bounded Stage 0-4 formal and history-checking exploration is executable end
to end. The Stage 5 Kani harnesses and manual runner also execute successfully
with the pinned verifier and have measured local cost. The combined pilot
remains a manual exploration rather than an accepted project gate:

- every intended-safe graph listed above exhausts with no error and no depth
  bound;
- all fair eventually-healthy liveness graphs satisfy their stated temporal
  properties, with the equal-priority completion assumption called out above;
- every seeded mutant reaches its named bad action and violates its expected
  invariant;
- the ADR-059 owner/wound/GC composition exhausts cleanly, while its historical
  four-status mutant still reaches `LateOwnerCommit` and violates the
  anti-resurrection invariant;
- the implementation-history checker rejects its seeded public anomalies and
  replays deterministic fuzz inputs under the real engine; and
- the model decomposition, implementation-refinement boundary, and wound-fix
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
  composition checks ADR-059's pinned-wound handoff directly.
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
  `WoundRetirement` reduces the production owner-operation accounting and
  unresolved-mutation proof to one explicit retirement transition; the Rust
  lifecycle regressions cover the implementation detail behind that transition.
- The Rust checker samples deterministic schedules and faults; TLC exhausts
  only the recorded finite domains. Neither is a deductive proof of Tokio,
  Rust, the object-store clients, or arbitrary user-closure side effects.
- The Stage 5 Kani harnesses execute the production lifecycle relation,
  transition validator, full-cardinality median-index policy, and the split
  metadata kernel called by `Node::finish_split`. Prior B-link field presence,
  delete-intent presence, and the membership version are symbolic; concrete
  field values are fixed. Kani 0.67.0 did not make the production
  `BTreeMap::split_off`
  transforms tractable within the pilot budget, so Shard/Index content
  conservation remains in deterministic Rust tests and is not claimed here.
  The accepted proofs also do not cover arbitrary-size trees, split
  persistence, crash recovery, async I/O, Tokio scheduling, or object-store
  behavior. Test-only stubs are used only for three seeded defects and the
  upper-median maintenance check, never to replace a dependency in a positive
  production proof.
- Loom was declined for this pilot because the candidate synchronization seams
  combine standard mutexes and atomics with Tokio notification, channel,
  selection, spawning, and cancellation primitives. Covering that code would
  require a broad production synchronization adapter; a separate Loom-only copy
  would be an untrustworthy shadow implementation. Verus and Creusot were also
  declined: the finite lifecycle graph is covered directly by Kani, while an
  unbounded topology proof would first require a justified production protocol
  core and verified container abstractions.
- Long-lived snapshot reads, their history retention, and their checkpoint
  cache semantics are excluded by request. Ordinary latest-value point reads,
  concurrent read groups, and normalized transaction scans are included.

No CI job or required `test-all` dependency is added. An ADR should still wait
for an explicit decision to make the manual suite a maintained project gate.
