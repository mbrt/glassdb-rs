# Formal verification

## Status and boundary

This directory contains the initial GlassDB model-checking pilot described in
[the formal protocol verification design](../docs/designs/formal-protocol-verification.md).
It implements the design's semantic inventory and first bounded
logged-transaction safety slice. It is an exploration, not an accepted
architecture gate, and it is not run by CI.

The checked boundary is deliberately small:

- two public transaction operations and two transaction identities;
- two keys on either one shared leaf or two separate leaves;
- fixed topology and strong latest-value point reads;
- logged multi-key commits, read-only validation, and validated body errors;
- shared read locks, exclusive write locks, wound-wait, timeout into sorted
  acquisition, lazy pending records, lease expiry, cancellation, and write-back;
- an ambiguous final transaction-object mutation that may apply after an
  `InDoubt` response or abandonment; and
- absent, tombstone, and two non-empty logical values.

Direct/logless commit, coordinator batching, scans, collection lifecycle,
B-link topology, cache evidence, transaction-object reclamation, attempt-ID
renewal, and liveness properties are deferred. In particular, properties
S6-S8 from the design belong to the direct-commit stage and are not claimed by
this model.

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

The command intentionally remains separate from `make test-all`: this pilot is
not yet a required project toolchain or CI check.

## Modules

| File | Responsibility |
| --- | --- |
| `tla/Common.tla` | Finite sequence helpers used by the history refinement check. |
| `tla/Backend.tla` | The dispatch/effect/uncertainty vocabulary for conditional mutations. |
| `tla/TxCore.tla` | Logged transaction, locking, recovery, write-back, and named invariants. |
| `tla/MC_TxCore.tla` | The finite two-transaction workload and topology mappings. |
| `tla/MC_TxCoreMutants.tla` | Isolated, deliberately invalid transitions used by six negative controls. |
| `tla/TxCore*.cfg` | Required safety and expected-counterexample configurations. |

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

The model also uses `Abandoned` to close a finite prefix when a wounded attempt
would renew its ID and continue the same public operation in production. Such a
state represents no public response; the aborted attempt has no possible
effect. Attempt renewal is deferred rather than misreported as a definite
public failure.

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
expiry may instead install aborted, which the commit CAS cannot replace. The
commit mutation records the complete expected semantic status and lease rather
than an ever-increasing revision.

### Reduction choices

The model preserves every logical commit ambiguity but reduces recoverable
pre-commit CAS detail:

- `AcquireLeafLostAck` installs the complete leaf lock change without adding a
  local receipt. A later acquire re-reads and recovers it; cancellation can
  leave it for wound or expiry cleanup. A physically delayed acquisition is
  reduced to `install; cancel`: if its semantic CAS predicate still holds, the
  install commutes before local cancellation, and otherwise the request has no
  effect. This checks the installed/not-installed branches without another
  per-leaf request automaton. Timeout cleanup also releases every installed
  lock for the modeled ID, including an unacknowledged one; production's
  receipt-recovery retry is a reduced interface here.
- A wound terminates the modeled attempt instead of allocating a renewed ID.
  Renewal preserves priority and begins a new attempt, so excluding it narrows
  retry/liveness behavior without removing a logged commit interleaving.
- An older contender may create an aborted tombstone for a younger holder whose
  pending object is still absent. Missing-object grace applies to
  observer-driven expiry, not to the wound-wait priority rule. The pilot reduces
  the observer-relative progress timer to one saturating expiry choice and does
  not verify its exact duration.
- A failed optimistic read-only validation reruns the read phase. Production's
  escalation to a locked read attempt is a convergence mechanism and is
  deferred from this safety-only slice.
- Fixed keys encode absence and tombstones as values. A put over absence uses
  the same exclusive-lock abstraction as an overwrite; the distinct collection
  create-lock protocol is outside this model.
- The reclamation clock is not modeled. An unresolved final mutation can remain
  unresolved indefinitely, which conservatively admits ADR-057's public
  `InDoubt` result and never repairs an absent decision. ADR-057's individual
  status-read and pending-to-redispatch recovery steps are collapsed into the
  model's acknowledged, clean-precondition, and unresolved branches.
- Time is a finite saturating class. It selects refresh-versus-expiry races; no
  wall-clock duration is inferred from its numeric value.
- Backend revision tokens and an explicit A-to-B-to-A request trace are not
  state variables. CAS uses equality over the complete reduced semantic object;
  safe reuse of an equivalent predicate is a trusted backend reduction, not an
  explored monotonic-revision proof.

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
8. Transaction IDs are unique for the modeled reclamation horizon and priority
   comparisons are stable.
9. Only conforming GlassDB clients mutate protocol objects.

The pilot does not assume an eventually healthy backend or fair scheduling,
because it checks safety only.

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

## Finite configurations and baseline

The required runs differ along the two dimensions that affect locking:

| Configuration | Leaf mapping | Priorities | Generated states | Distinct states | Pilot runtime |
| --- | --- | --- | ---: | ---: | ---: |
| `TxCoreSameLeafDistinct.cfg` | Both keys on one leaf | older/younger | 3,303,304 | 866,696 | 42 s |
| `TxCoreSameLeafEqual.cfg` | Both keys on one leaf | equal | 1,087,463 | 284,869 | 16 s |
| `TxCoreCrossLeafDistinct.cfg` | One key per leaf | older/younger | 6,846,744 | 1,566,901 | 1 min 47 s |
| `TxCoreCrossLeafEqual.cfg` | One key per leaf | equal | 5,233,832 | 1,228,624 | 1 min 25 s |

These measurements were recorded on 2026-08-09 with TLC 1.7.4, OpenJDK 21,
one TLC worker, fingerprint polynomial 0, and no depth bound. Counts are stable;
runtime is a local reference rather than a budget guarantee. The same-leaf
distinct run includes multi-key writers, a mixed transaction with a shared
read lock and one write lock, read-only validation, a validated error with
discarded staged writes, and explicit abort. The other configurations use one
multi-key writer plus the mixed read/write program because the public outcome
classes add no topology behavior.

## Seeded counterexamples

`make verify-formal` also runs six negative controls that must fail for the named
reason. A syntax error, tool failure, unexpected invariant, or unexpectedly
successful mutant fails the runner.

| Mutant | Required violation | Counterexample |
| --- | --- | --- |
| `TerminalReversalSpec` | `S1_TerminalState` | Replaces a committed record with aborted. |
| `EarlyPublicationSpec` | `S4_Refinement` | Publishes one logged leaf value while the transaction is still non-final. |
| `MissingValidationSpec` | `S9_PostLockValidation` | Commits observations made stale before lock acquisition. |
| `MissingValidationSpec` with same-value input | `S9_PostLockValidation` | Changes only the writer token, then commits without revalidating it. |
| `ExpiryAfterCommitSpec` | `S10_CommittedCannotAbort` | Materializes pending, commits, advances beyond that lease, then lets expiry reverse the decision. |
| `UncertaintyMisclassificationSpec` | `S11_UncertaintyIsConservative` | Returns definite failure while the dispatched commit can still apply. |

The complete deterministic traces are retained in
`target/formal/mutant-*.log`. They are separate wrapper actions rather than a
bug-mode constant in the intended protocol.

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
  and an earlier `Aborted` wound defeats both refresh and commit; and
- after an ambiguous logged finalization, absence is never repaired. Failure to
  establish the decision remains `InDoubt`, as required by ADR-057.

For the direct-commit stage, the first required scenario should be an uncertain
blind overwrite that is subsequently superseded before recovery. That stage
must check that recovery cannot restage the public operation as a second
logical effect.

## Remaining acceptance work

This implementation establishes the first bounded Stage 0/1 safety slice: TLC
completed each full bounded graph search, subject to its reported fingerprint
collision probability, and the logged-path mutants are detected. It does not
by itself satisfy the full Stage 1 exit criteria or the overall design pilot:

- the abstraction/refinement mapping still needs review by someone other than
  the author;
- explicit delayed lock requests, attempt renewal, observer-relative lease
  progress, and coordinator member folding remain reduced interfaces;
- direct commit and coordinator properties/mutants are Stage 2;
- implementation history conformance is Stage 3;
- liveness needs a separate fair, eventually healthy configuration; and
- later protocol models must cover topology, collections, cache evidence, and
  reclamation.

No ADR should be extracted and no required CI gate should be enabled until the
broader pilot criteria in the design are met.
