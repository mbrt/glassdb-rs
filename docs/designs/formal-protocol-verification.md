# Formal protocol verification

## Status

Proposed. This design adds a layered verification program for GlassDB's
transaction and storage protocols: an independent TLA+/TLC model, an exact
transaction-history checker exercised by deterministic simulation, and targeted
code-level proofs for pure transition kernels. There is no umbrella ADR yet;
the pilot must demonstrate useful counterexamples, a maintainable abstraction,
and an acceptable CI cost before the toolchain becomes an accepted project
requirement.

The bounded Stage 0-5 pilot is implemented under
[`formal/`](../../formal/README.md), with the implementation-history checker in
`crates/glassdb/src/sim/history.rs`. It includes direct/coordinator behavior,
explicit recovery interfaces, safety and fair eventually-healthy liveness
runs, normalized range-scan histories, composed topology/lifecycle/cache/GC
models, a shared-state TxCore/B-link boundary check, seeded mutants, and
independent review. The focused owner/wound/GC composition now models
[ADR-059](../adr/059-pin-foreign-wounds-until-owner-retirement.md): a foreign
wound remains pinned as `Wounded` until the owner proves retirement and
acknowledges it as `Aborted`. Its intended safety graph exhausts cleanly, while
a synthetic mutant retains the superseded foreign-`Aborted` transition and
still reproduces the original resurrection failure. The Stage 4 boundaries are
therefore covered for the implemented scope. The suite remains a manually
invoked exploration with no formal CI job or accepted architecture gate. The
Stage 5 code-level pilot uses pinned Kani 0.67.0 against production-used
transaction-lifecycle, median-policy, and split-finalization kernels, with three
positive proofs, three seeded expected-panic mutants, and an upper-median
maintenance check.
The final pinned run and its per-harness time/memory measurements are recorded
in the formal-suite README. Loom and Verus/Creusot were evaluated and declined
for the concrete reasons recorded below.

## Goal & scope

GlassDB promises strict serializability over a protocol whose logical commit can
take either of two forms:

- a logged transaction commits by changing one transaction object to its final
  committed state; or
- an eligible single-key transaction commits by publishing its value in one
  conditional leaf mutation without a transaction object.

Locks, write-back, help-forwarding, coordinator batching, B-link splits,
collection lifecycle fencing, cache evidence, leases, cancellation, and garbage
collection all surround those commit points. They must preserve one logical
database history despite crashes and conditional mutations whose outcome may be
unknown.

The goal is to make the corresponding correctness argument executable and
reviewable. The verification program must:

1. state the public consistency contract precisely, including `InDoubt`, scans,
   collection changes, and errors derived from transaction reads;
2. exhaustively explore a finite abstract protocol model across concurrency,
   crash, recovery, and uncertainty choices;
3. check executions of the production transaction engine against an independent
   sequential specification;
4. prove selected pure Rust transition functions preserve their local
   invariants; and
5. make a protocol change fail close to the semantic layer it invalidates.

The initial scope is latest-value, strongly consistent transactions over the
in-memory backend model. It includes logged commits, logless direct commits,
read-only validation, point reads and writes, crashes, unavailable conditional
mutations, wound-wait, leases, write-back, and the shard-mutation coordinator.

The following are modeled as separately composed finite interfaces rather than
being multiplied into the transaction-core state space:

- range scans and phantom prevention in the implementation-history checker;
- B-link split publication and routing;
- transactional collection creation and drop;
- transaction-object garbage collection;
- the lazy-owner/foreign-wound/GC boundary, through the focused pinned-wound and
  owner-retirement composition;
- decoded and persistent cache evidence; and
- S3 and GCS adapter behavior through their in-process contract suites; live
  provider semantics remain a trusted environment assumption.

The following are explicitly out of scope:

- proving the correctness of S3, GCS, Tokio, Rust, LLVM, the operating system,
  protobuf, or cryptographic/random identifier generation;
- proving availability during a permanent backend outage or under an unfair
  scheduler;
- proving the behavior of arbitrary side effects in a user transaction closure;
- the deliberately weaker `read_stale` API;
- the proposed long-lived snapshot-read design; and
- immediately proving the entire async Rust implementation in a deductive proof
  assistant.

## Design at a glance

Verification is layered so that no single artifact is asked both to define the
protocol and to validate its own implementation:

```text
         Sequential transactional map + collection catalog
                              ^
                              | refinement
                              |
                Independent TLA+/TLC protocol model
                              ^
                              | matching public histories
                              |
       Production engine under deterministic schedules and faults
                    /                         \
                   /                           \
        bounded/deductive proofs          local concurrency checks
          of pure Rust kernels             for shared-memory seams
```

The first two executable artifacts are deliberately independent:

- `formal/tla/` describes protocol state and nondeterministic environment steps
  without importing or executing Rust code.
- A simulation-only Rust history checker invokes the public transaction API,
  records logical inputs and results, and asks whether the resulting history is
  admitted by a small sequential database specification. It does not inspect
  shard objects or duplicate the production commit algorithm.

The intended-safe formal configurations provide exhaustive finite-state evidence
for their stated boundaries, while required mutants and any faithful
known-protocol witnesses must produce their named failures. `WoundRetirement`
checks that ADR-059's pinned marker composes with lazy holder publication,
physical cleanup, owner retirement, acknowledgement, and finite GC. Its
superseded foreign-`Aborted` mutant preserves the causal counterexample that
motivated the repair without representing it as current protocol behavior. The
history checker closes part of the model-to-code gap over the many real
executions explored by the existing deterministic executor. Kani closes a
smaller code-level gap for the production transaction-lifecycle relation and
the pure median and split-finalization metadata kernels. It is not an end-to-end
protocol proof: Loom was not adopted for the mixed standard-library/Tokio
synchronization seams, and no Verus or Creusot protocol core was introduced.

No verification code participates in a production build, changes the public
API, or adds work to a database operation.

## Correctness contract

### Terminology

The model distinguishes four identities that are easy to conflate:

- A **public operation** is one call to `Database::tx`. It has one invocation
  visible to the caller and one returned result or in-doubt notification.
- A **body execution** is one invocation of the user's closure. Validation may
  cause the same public operation to execute its body more than once.
- A **logged transaction identity** is a transaction ID that has engaged the
  monitor, may own locks, and may have a transaction object.
- A **logical transaction** is the single atomic state transition, if any, that
  appears in the abstract serial history.

Failed speculative body executions and lock retries are internal. At most one
body execution of a public operation may supply its logical transaction.

### Sequential specification

The abstract database contains:

- a permanent root collection;
- a mapping from each live parent incarnation and child name to a child
  incarnation;
- a key-to-value map for each live collection incarnation; and
- no lock, transaction-log, cache, topology, or GC state.

A transaction executes against one abstract state with a local overlay. Point
reads, directory reads, and scans observe the state plus earlier changes in the
same transaction. A successful commit applies every final key and directory
change atomically. A validated body error exposes its read results but applies no
changes. An explicit abort and a definitive engine failure expose no database
effect.

This specification intentionally describes behavior rather than the current
implementation. Writer IDs, leaf versions, transaction IDs, and collection
object paths do not appear in it.

### Strict serializability

A finite public history is accepted when it has a completion for which there is
a total order of logical transactions satisfying all of the following:

1. Every operation returning success appears exactly once.
2. Every definitively aborted or failed write operation appears zero times.
3. Every operation returning `InDoubt`, and every invocation abandoned by
   cancellation or client crash, appears either zero or one times.
4. If a definitive operation A returned before operation B was invoked, A
   precedes B.
5. Executing operations in that total order from the initial abstract state
   reproduces every point-read, scan, directory-read, and validated-body-error
   observation.
6. The resulting state equals the quiescent final state observed by the
   verifier.

This is transaction-level linearizability: each transaction is one operation on
the abstract database. It implies serializable isolation and real-time order for
definitively completed operations.

### In-doubt operations

`Error::InDoubt` is not treated as an abort and is not treated as a successful
logical response. It is evidence that the public caller stopped waiting while a
commit may or may not exist. A public future abandoned by cancellation or client
crash has the same optional-effect semantics but has no notification. The
history checker therefore represents both as unresolved invocations that a
completion may either omit or complete exactly once; it retains whether the
caller received an in-doubt notification.

Initially, the in-doubt notification is not an upper bound on that operation's
linearization point. An abandoned backend mutation may still execute remotely,
and local path coordination deliberately makes no ordering claim after such an
abandonment. Later definitive reads and the final quiescent snapshot constrain
whether and where the unresolved operation can be completed.

This is the conservative contract. If every production backend can establish a
stronger response-time upper bound for non-cancellation `InDoubt`, that narrower
contract can be proposed separately. The model must not assume it implicitly.

An operation whose direct mutation may have landed can never be completed twice,
renewed into a second logical write, or classified as a body replay. Conversely,
an operation may be replayed only when the protocol has proved that its body
execution staged no durable state.

### Read-derived errors

A user error computed from transaction reads is observable database behavior.
The public retry loop validates those reads before returning the error. The
sequential specification represents such a result as a read-only logical
transaction: its observations must be explainable at one serial point, while
its staged writes and collection changes are discarded.

Errors that do not depend on transaction reads need not be inserted into the
serial history, although recording them is harmless. The generated history
workload records all body observations so the checker does not have to infer
which error text depended on which read.

### Trusted assumptions

Safety results are conditional on the following environment contract:

1. A backend point read or conditional mutation is linearizable for one object.
2. A read invoked after a definitive mutation response observes that mutation
   or a later state.
3. A clean precondition failure proves the named predicate was false at the
   mutation's linearization point.
4. `Unavailable` for a mutation permits either no effect or exactly the requested
   conditional effect. It creates no definitive local ordering edge.
5. Conditional predicates are safe if a semantically equivalent state makes
   them true again; backend revisions may exhibit ABA.
6. A process crash destroys volatile state and tasks but preserves acknowledged
   and possibly-landed backend mutations.
7. Transaction IDs are unique while relevant, and renewal preserves the
   wound-wait priority.
8. Clock error is bounded by the configured skew allowance wherever a lease or
   reclamation horizon relies on time.
9. Only conforming GlassDB clients mutate protocol objects. Arbitrary external
   corruption is outside the protocol guarantee.

Liveness checks add stronger assumptions: continuously enabled tasks are
eventually scheduled, the backend eventually returns definitive results, model
time eventually advances, and the workload eventually stops introducing older
conflicting transactions.

### Physical-to-logical refinement

The transaction-core model carries a ghost `logical_db` and derives a
`LogicalView` from physical protocol state. For a key, that view is:

1. the value or tombstone of a committed exclusive holder that is ahead of
   write-back; otherwise
2. the leaf's `Inline` bytes, `External` committed transaction value,
   `Tombstone`, or absence.

The principal refinement invariant is:

```text
LogicalView(physical_state) = logical_db
```

Only three classes of transition may change `logical_db`:

- the final transaction-object CAS for a logged read-write transaction applies
  all of that transaction's writes and collection changes;
- a successful coordinator leaf CAS applies the one-key writes of every staged
  direct member, in fold order; disjoint direct members may share the physical
  CAS but remain distinct logical transactions; and
- successful read-only validation records a serial point without changing the
  map.

Same-key exclusion makes direct members sharing one CAS disjoint, and direct
eligibility gives them no cross-key read dependency. Their writes therefore
commute; fold order supplies one deterministic serial witness without claiming
that the backend performed several physical writes.

Prepare, acquire, wait, wound, abort, lease refresh, write-back, help-forward,
split, cache reconciliation, and GC steps must refine to stuttering steps. This
turns statements such as "partial cross-shard write-back is harmless" into
machine-checked preservation obligations.

## Independent TLA+/TLC model

### Tool choice

The pilot uses TLA+ directly and TLC as its required checker. The reasons are:

- the protocol is naturally expressed as nondeterministic state transitions;
- TLC exhaustively explores every reachable state of a finite configuration and
  can check both invariants and temporal properties;
- TLA+ expresses stuttering refinement and fairness directly;
- the model remains independent of Rust implementation choices; and
- the tooling does not enter the Cargo dependency graph or production binary.

Quint with the TLC backend remains a viable authoring alternative if direct
TLA+ proves to be a maintenance barrier. Apalache may supplement TLC for larger
bounded symbolic searches, but it is not the primary checker because its checks
are step-bounded and its temporal-property support is narrower. The pilot should
not maintain equivalent TLA+ and Quint sources simultaneously.

### Repository layout

The implemented source layout is:

```text
formal/
  README.md                    tool setup, model scope, and commands
  tla/
    Common.tla                 finite maps, ordering, and shared helpers
    Backend.tla                abstract point-read/CAS/uncertainty contract
    TxCore.tla                 logged transaction and locking protocol
    MC_TxCore.tla              finite constants and model-checking wrappers
    DirectCore.tla             direct commit and coordinator protocol
    MC_DirectCore.tla          two-/three-member wrappers and liveness
    RecoveryLifecycle.tla      delayed requests, renewal, grace, and cleanup
    MC_Recovery*.tla           focused recovery and temporal wrappers
    WoundWait.tla              explicit distinct-priority wait graph
    BLinkSplit.tla             separately composed topology model
    CollectionLifecycle.tla    collection catalog and drop model
    CacheEvidence.tla          cache/path-lane evidence model
    GarbageCollection.tla      reachability and safety-horizon model
    WoundRetirement.tla        pinned wound and owner-retirement composition
    WoundRetirement*.cfg       ADR-059 safety and historical mutant runs
    TxBLinkComposition.tla     shared split-gate/reference boundary
    *.cfg                      safety, liveness, and mutant configurations
```

Model-checker output, downloaded tools, checkpoints, and state graphs live under
`target/formal/` and are not committed.

`Common.tla` contains mathematical helpers only. Protocol policy stays in the
module that owns it: backend uncertainty in `Backend`, commit and locking in
`TxCore`, topology in `BLinkSplit`, and so on. This mirrors the implementation's
policy/mechanism boundary without reproducing its module graph.

### Initial transaction-core state

`TxCore` models these variable families:

- `logical_db`: the ghost abstract key/value state;
- `entries`: per-key current state and lock state for each modeled leaf;
- `tx_objects`: absent, pending, committed, or aborted objects with lock intents
  and committed writes;
- `clients`: public invocation, body, acquire, validate, commit, write-back,
  returned, crashed, or unresolved phases;
- `reads` and `writes`: the active body execution's logical accesses;
- `held`: the lock receipt each transaction believes it owns;
- `pending_mutations`: dispatched conditional mutations that may still land
  after local cancellation or an unavailable response;
- `now_class`: a finite, saturating time class used for lease and timeout
  transitions; and
- `linearized`: the abstract operation order and at-most-once marker.

This reduced transaction-object vocabulary is local to `TxCore`, which never
deletes a final object and can therefore collapse terminal-for-commit `Wounded`
into `Aborted`. `WoundRetirement` restores the production lifecycle distinction:
an unacknowledged foreign wound is pinned as `Wounded`, while an
owner-acknowledged `Aborted` record is eligible for finite GC.

Transaction-object bodies and leaf entries use abstract values, not encoded
bytes. Backend revisions are equality tokens with possible reuse, not monotonic
integers. The model must not accidentally prove safety by assuming revisions
never exhibit ABA.

### Initial transitions

The first complete model includes:

- invoking a public transaction and executing point reads or staged writes;
- creating or refreshing a pending transaction object;
- acquiring compatible read, write, or create locks;
- folding multiple same-leaf members in wound-wait order;
- waiting on, wounding, or observing another holder;
- validating read writers after all locks are held;
- committing a logged transaction object;
- committing an eligible direct blind overwrite or read-modify-write in a leaf;
- classifying a direct attempt as landed, replayable, locked fallback, or
  in-doubt;
- asynchronous write-back and lock release;
- client crash or cancellation at every phase;
- definite CAS conflict, unavailable-before-effect, and unavailable-after-effect;
- lease refresh, expiry, force-abort, and orphan-lock release; and
- public success, validated body error, definitive failure, in-doubt
  notification, and abandonment with no response.

The model treats one backend mutation as dispatch, optional effect, and local
reconciliation steps. That split is necessary to represent a mutation that
continues remotely after the owning future is dropped.

### Finite configurations and state reduction

The checked model uses deliberately small domains:

- two or three transaction identities;
- priorities containing older, younger, and equal cases;
- two keys and two leaves, with separate same-leaf and cross-leaf
  configurations;
- two non-empty values plus absence and tombstone;
- finite client phases and saturating time classes; and
- at most one unresolved backend mutation per client and modeled physical path,
  allowing parallel cross-leaf acquisition while retaining same-path
  serialization.

These bounds cover the smallest witnesses for lost updates, partial multi-key
visibility, deadlock cycles, same-key coordinator exclusion, cross-shard
write-back, and ambiguous commits. Symmetry reduction may rename equal-priority
transactions and interchangeable keys, but it must not identify transactions
whose priority or real-time position differs.

TLC checks executions of arbitrary length within this finite state space. No
fixed transition-depth bound is used for the required safety configuration.
Liveness runs use a separate configuration because fairness substantially
increases checking cost and because their environment assumptions must remain
visible.

### Safety properties

The model names each property independently so a failure identifies the broken
argument:

| ID | Property |
| --- | --- |
| `S0` | Every variable is well typed and every persisted object is structurally valid. |
| `S1` | A final transaction is never replaced; no transaction is both committed and aborted. |
| `S2` | Read locks may share, while write and create locks remain exclusive according to the compatibility table. |
| `S3` | A committed holder and every `External` current state resolve to a committed durable value; only `Inline` may name a logless writer. |
| `S4` | `LogicalView(physical_state)` always equals `logical_db`. |
| `S5` | A logged commit applies its complete write set exactly once; write-back order cannot expose a partial effect. |
| `S6` | Each direct transaction changes exactly one key once; a coordinator CAS may carry disjoint direct transactions, but at most one same-key logless member stages. |
| `S7` | `Replay` implies the transaction has no pending mutation, transaction object, lock, commit marker, or other durable effect. |
| `S8` | Uncertainty belonging to one coordinator member is never transferred to a skipped member, and an uncertain staged member is never downgraded to a definitive loss without evidence. |
| `S9` | A transaction commits only after every read and predicate dependency is valid while its required locks are held. |
| `S10` | A committed transaction cannot be wounded or self-aborted, including when commit races lease expiry. |
| `S11` | Cancellation after dispatch either reconciles a definitive result or leaves the mutation unresolved; the client never treats it as a definite no-effect result. |
| `S12` | Every definitive successful public history is strict serializable, and every in-doubt or abandoned operation has at most one admissible completion. |

Properties for topology, collections, cache evidence, and GC are added in their
own models rather than weakening `TxCore` with unrelated state.

### Liveness properties

Under the explicitly fair and eventually healthy environment, the model checks:

- a committed transaction is eventually written back or safely superseded;
- a crashed pending transaction is eventually finalized and its locks become
  reclaimable;
- a younger waiter eventually observes the holder's final state after the
  holder stops refreshing;
- distinct-priority wound-wait does not form a wait cycle;
- equal-priority contention eventually reaches the sorted serial fallback; and
- once new conflicting work stops, at least one contender completes.

These are convergence properties, not unconditional per-client starvation
freedom. An infinite stream of older transactions or permanent unavailable
responses is allowed to prevent progress and is excluded from the liveness
configuration by assumption.

### Model decomposition after the core

Each later model exposes an assumption/guarantee boundary to `TxCore`:

- **`BLinkSplit`** guarantees that exactly one authoritative leaf owns a key,
  traversal reaches it through parent or right links, split preserves entries
  and transaction references, and sibling creation is invisible until source
  shrink.
- **`CollectionLifecycle`** guarantees that a parent binding is the logical
  existence authority, preparation is unreachable before commit, drop fences
  every participant, and stale incarnations cannot accept a later data commit.
- **`CacheEvidence`** guarantees that an accepted observation was current at its
  claimed lower bound, clean conflicts cannot publish replacement knowledge,
  and abandoned or unavailable mutations leave no reusable knowledge.
- **`GarbageCollection`** guarantees that referenced or recent objects are not
  deleted, pending objects obtain a durable terminal decision before release,
  and an absent old final object is never recreated with a guessed result. Its
  standalone graph reduces away the lazy holder-before-object window and relies
  on ADR-059's pinned-wound contract for that boundary.

The integrated transaction model assumes these guarantees. Small composition
checks then combine a reduced form of two adjacent models to catch mismatched
interfaces without multiplying all state spaces together.

`WoundRetirement` checks that omitted composition explicitly. It preserves one
exact owner identity across lazy holder publication and suspension, lets a
foreign actor install `Wounded`, and permits physical holder cleanup and time
advancement while the marker remains pinned. A returning owner must establish
retirement before conditionally acknowledging `Wounded -> Aborted`; that
acknowledgement resets the ordinary abort-retention horizon, after which GC may
delete the record. An abandoned or not-yet-retired owner cannot make the marker
deletable, and the same identity cannot commit after the foreign wound.

`WoundRetirementSafety.cfg` checks type safety, no same-identity resurrection,
pinning until retirement, and acknowledgement before `Aborted`. The focused
model reduces production's owner-operation accounting and unresolved-mutation
proof to an explicit retirement transition. Production lifecycle regressions
cover the implementation detail that establishes that proof.

### Counterexamples and model validation

A model that only checks its intended protocol can accidentally encode a real
bug away as an assumption. The runner therefore distinguishes deliberate
mutants of an intended-safe model from faithful current-protocol witnesses.
TLC must reject deliberate mutants for at least these cases:

- permit `committed -> aborted`;
- make logged writes visible one leaf at a time before the commit object is
  final;
- allow a direct attempt to replay after an unavailable mutation may have
  landed;
- allow two same-key logless members to stage in one coordinator CAS;
- attribute one member's uncertain CAS to a skipped member;
- allow read validation before locking with no post-lock recheck;
- let lease expiry delete or abort a transaction after its commit won;
- reclaim a referenced or within-horizon transaction object in the later GC
  model; and
- write a foreign wound directly as GC-eligible `Aborted` without owner
  retirement proof.

Each mutant is a separate model-checking wrapper or configuration, not a
long-lived alternate protocol branch. The verification test succeeds only when
TLC finds the expected counterexample class. This validates that the model and
properties are capable of observing the failures they claim to exclude.

A known-protocol witness instead uses only actions admitted by the current
design; the runner retains this category so a newly discovered current defect
cannot be mislabeled as a mutant. There is no required known-protocol failure in
the present catalog. `WoundRetirementMutantForeignAbort.cfg` deliberately adds
`ForeignAbortWound`, the transition superseded by ADR-059 and excluded from the
intended `Next`. It then follows ordinary holder release and finite GC until
`LateOwnerCommit` violates `WR1_NoResurrectionAfterForeignWound`. Treating this
historical transition as current protocol behavior would misstate the repair;
dropping the mutant would instead lose the model-sensitivity check that
reproduces the original defect.

TLC counterexamples are reduced manually to a named scenario in
`formal/README.md` and, where applicable, to a deterministic Rust history test.
Automatic translation from a TLC trace to a scheduler tape is deferred: the
abstract model omits many executor and cache steps, so a direct translation
would create a brittle false correspondence.

## Implementation-history checker

### Purpose and boundary

The history checker exercises real `Database` and `Transaction` methods over the
existing deterministic executor, `MemoryBackend`, transport-fault middleware,
and simulated persistent cache. Its oracle is the sequential specification, not
the final invariant of one specialized workload.

It remains a test at the public API boundary. It must not widen engine
visibility, inspect transaction logs or leaf entries, or share the production
resolver and validation functions. This avoids making an implementation detail
part of the oracle and follows the project's preference for behavioral tests.

### Explicit transaction programs

A new `HistoryWorkload` owns per-client sequences of small explicit transaction
programs. A program is interpreted inside `Database::tx`; internal retries
naturally execute the interpreter again. Initial instructions include:

- point read into a local register;
- write or delete a key;
- write a literal, copied, or incremented register value;
- read a bounded group concurrently into distinct local registers;
- compare a register and return a modeled user error;
- explicit transaction abort; and
- no-op/yield points that change scheduling without changing semantics.

Normalized range scans are also included. Collection lifecycle remains covered
by the dedicated formal model and existing API workload rather than this
key-history interpreter. Generated programs use small key, value, register, and
instruction bounds so the serialization search remains exact.

The interpreter records the actual ordered instruction results and staged
mutations of every body execution. It checks expression evaluation locally, so
the history checker only needs to decide whether the observed database values
and final effects fit a serial history.

Concurrent reads within one transaction are represented as a canonical
unordered read group against the same transaction snapshot. Task completion
order is deliberately absent from the trace.

### History representation

The simulation-only representation contains:

```text
PublicOp {
    op_id,
    client_id,
    invocation_point,
    body_executions: [BodyTrace],
    notification_point,
    outcome,
}

BodyTrace {
    body_number,
    ordered_actions,
    final_mutations,
    body_result,
}

Outcome = Success
        | ValidatedBodyError
        | DefiniteNoEffect
        | InDoubt
        | Abandoned
```

Sequence points come from a checker-owned monotonically increasing counter under
a mutex. They describe harness event order only; they are unrelated to
`CachedStore::SequencePoint` and never enter protocol code.

`ordered_actions` contains reads and the intervening local writes/deletes, so
the sequential interpreter can reproduce read-your-writes instead of inferring
an overlay from the final mutation set.

For success, the last body execution that returned normally supplies the
logical transaction. Earlier executions are invisible. For a validated body
error, the successfully validated final body trace supplies a read-only logical
operation. A definite failure supplies no mutation. For `InDoubt`, the final
commit-eligible body trace is an optional operation. For `Abandoned`, it is
optional only when the last body execution completed successfully and returned
control to the database commit driver; a body dropped while still executing
cannot have a logical database effect. A recorder entry that has an invocation
but no notification when the deterministic run finishes is finalized as
`Abandoned`; dropping the public future therefore needs no engine
instrumentation.

The recorder is scoped to `HistoryWorkload`; it is not a global callback on all
database operations. This keeps production and unrelated tests free of
instrumentation and makes each recorded action explicit in the workload.

### Serialization algorithm

The checker performs an exact depth-first search over legal completions and
serialization orders:

1. Build mandatory operations from successes and validated body errors.
2. Add a Boolean include/omit choice for every `InDoubt` or `Abandoned`
   operation.
3. Build real-time predecessor edges for definitive operations whose response
   precedes another invocation.
4. Starting from the seeded abstract database, choose any unplaced operation
   whose mandatory predecessors are already placed.
5. Interpret that operation's recorded body trace against a local overlay of the
   candidate state. Reject the branch if a point read, scan, directory read, or
   user-error observation differs.
6. Atomically apply its final mutations when its outcome is mutating.
7. Memoize `(abstract_state, remaining_operations, included_in_doubt_choices)`
   to avoid exploring equivalent suffixes.
8. Accept only when every required operation is placed and the resulting
   abstract state equals the final quiescent snapshot.

Candidate order is deterministic: lowest stable operation ID first. Maps and
sets use ordered representations. A failure prints the history, real-time edges,
final state, and the reason each currently minimal operation was rejected. The
existing fuzz input remains the reproduction artifact; generated history output
does not need a separate persisted corpus format.

The search is exponential in the worst case. Workload bounds and real-time edges
keep it tractable. A branch-count budget is a harness failure, not a successful
"unknown" result: the workload bounds must be reduced or the checker improved
rather than silently weakening the oracle. SAT/SMT encoding is deferred until a
measured workload exceeds the exact search budget.

### Final-state observation

After client tasks and fault nemeses finish, the harness heals every transport,
opens a fresh database, and drives recovery through strong reads. The history
workload then reads its complete bounded key universe in one validated
transaction. That snapshot is both:

- another required read-only history operation; and
- the exact final-state constraint used to resolve optional in-doubt effects.

Later catalog workloads similarly enumerate their bounded collection-name and
key universes. Physical orphan objects are intentionally absent from this
logical snapshot; their safety belongs to the GC and lifecycle models.

### Checker validation

The checker itself is trusted reference code, so it receives focused tests:

- accepts every permutation of a set of independent transactions;
- rejects a lost update, fractured multi-key write, stale point read, phantom,
  and real-time inversion;
- accepts either completion of one in-doubt increment but rejects two
  applications;
- accepts a validated user error only at a state explaining its reads;
- handles read-your-writes through the local overlay;
- matches a simple brute-force enumerator on very small generated histories;
  and
- rejects deliberately corrupted histories produced by test-only workload
  adapters.

At least one accepted and one rejected history fixture corresponds to each
formal safety property that has a public-history manifestation.

### Integration with deterministic simulation

`HistoryWorkload` implements the existing `SimWorkload` boundary and therefore
inherits:

- tape-guided task scheduling;
- PCT seed breadth;
- dropped requests, lost acknowledgements, outages, and client crashes;
- cache-free and simulated-cache replay;
- byte-identical backend operation-stream checks; and
- libFuzzer corpus minimization.

A new fuzz target supplies bounded history programs. Its corpus replay runs the
same history check twice, just like the current workloads. The existing RMW,
cycle, membership, and API workloads remain valuable independent oracles; the
general checker supplements rather than replaces them.

The first implementation does not attempt to replay every physical backend
operation as a TLA+ action. Public-history conformance is a stable semantic
boundary, while physical trace refinement would require test hooks throughout
the cache, coordinator, monitor, splitter, and GC and could perturb scheduling.
Such instrumentation requires a separate design if the remaining refinement gap
justifies it.

## Targeted code-level verification

### Kani bounded proofs

The Stage 5 pilot pins Kani 0.67.0 and verifies production-used pure kernels in
`glassdb-storage`. Three positive harnesses cover:

- the exact lifecycle relation over all 25 normalized-state pairs, plus the
  transition validator over all 36 encoded source/destination pairs: absent or
  persisted `Unknown`, `Pending`, `Wounded`, `Committed`, and `Aborted`,
  including the pairs normalization rejects;
- the shared median-index policy for every `usize` cardinality from 2 upward,
  proving that both halves are nonempty, conserve the cardinality, and differ
  in size by at most one; and
- `finish_split_metadata`, the production kernel called by
  `Node::finish_split` after a body has already been partitioned. Presence of
  its prior high-key, right link, and delete intent is symbolic, as is the
  membership version. It proves B-link inheritance, complete source-lock
  preservation, and removal of transient lock holders from the new sibling.

The initial pilot also attempted direct Kani harnesses for
`Shard::split_off_median`, `IndexNode::split_off_median`, and their composition
through `Node::split`. A four-entry `std::collections::BTreeMap::split_off`
harness exceeded the 300-second cutoff, and a reduced fixed ordered three-entry
harness still produced no result in a 90-second feasibility run. Those Kani
targets were declined rather than weakened into shadow container
implementations. The
production content-conservation, ordering, disjoint-ownership,
promoted-boundary, and index-routing behavior remains exercised by deterministic
Rust tests. A timeout is an unknown result, not evidence that any of those
properties holds or fails.

Proof harnesses are colocated under `#[cfg(kani)]` so they can reach private
functions without widening production visibility. The `proof-mutants` feature
adds only Kani-gated stubs. One stub selects the upper median for odd sizes as a
representative maintenance check; the unchanged nonempty, balanced-cardinality
contract must still prove. Three `#[kani::should_panic]` harnesses seed local
defects—direct `Wounded` reclamation, an empty lower split half, and inherited
transient sibling lock holders in split metadata—and require the
corresponding named assertion to fail. Positive proofs never stub an async
dependency or replace a production kernel with a proof-only implementation.

General `CurrentState` and lock structural validity, a pure coordinator
admission/fold kernel, and a pure GC eligibility predicate remain possible Kani
extensions. They were not factored merely to create proof targets. A transition
should move into a pure production kernel only when doing so also clarifies
ownership and policy.

Kani is not used to claim an end-to-end concurrency or storage proof. The
harnesses do not execute async I/O, Tokio scheduling, conditional object-store
mutations, container partitioning, split recovery, arbitrary tree sizes, or
arbitrary deployments.

### Loom local-concurrency checks

The deterministic executor controls async poll order on one thread but does not
explore the C11 shared-memory model of a production multi-threaded Tokio runtime.
Loom was evaluated and declined for Stage 5. The most interesting candidate,
`Dedup`, combines standard mutexes and atomics with Tokio notification,
oneshot, selection, and spawning plus `tokio-util` cancellation. Cache lanes,
coordinator publication, and background shutdown have the same mixed-runtime
shape. Faithful Loom coverage would therefore require a broad production
synchronization/runtime adapter; copying the logic into a Loom-only harness
would create the shadow implementation this design rejects.

The unclosed memory-model gap is explicit. A future narrow production adapter
could justify focused Loom tests for:

- `Dedup` request ownership, cancellation, and delivery;
- per-path cache lane admission and mutation guards;
- shard-coordinator outcome-slot publication; and
- background shutdown/abort races.

Any such tests would require bounded scenarios. They would not model
object-store CAS, transaction serializability, or lease time and must not be
presented as protocol proofs.

### Deductive verification gate

Verus or Creusot becomes attractive only if a stable, pure protocol-core crate
emerges from the preceding work. Adoption requires all of the following:

- the verified functions are called by production code;
- the tool supports the Rust subset and data structures without extensive
  duplicate wrappers;
- contracts correspond directly to properties already exercised by TLA+ and
  the history checker; and
- proof maintenance survives one representative protocol change.

Verus is the preferred experiment if concurrent ghost state or a refinement
proof is required. Creusot is a viable alternative for functional contracts on
safe, sequential kernels. The project will choose at most one primary
deductive verifier; maintaining equivalent proof annotations in two systems is
out of scope.

Neither tool is adopted in Stage 5. The finite lifecycle relation is exhausted
directly by Kani, while an unbounded B-link refinement would first require a
production-used pure protocol core and verified container abstractions. Adding
duplicate maps, vectors, or protocol wrappers solely to fit either verifier
would fail the gate above.

## Tooling and CI

### Reproducible TLC setup

The implementation adds `make verify-formal`, backed by
`hack/verify-formal.sh`. The script:

- accepts an optional task-specific `TLA2TOOLS_JAR` override;
- otherwise downloads one pinned TLA+ tools release into
  `target/formal-tools/`;
- verifies its recorded SHA-256 before execution;
- requires a supported Java runtime;
- writes checkpoints and traces below `target/formal/`; and
- discovers every top-level `formal/tla/*.cfg` in deterministic filename order
  and runs the safety/liveness configuration, mutant, or known-protocol witness
  declared by its first-line metadata. The complete catalog is validated before
  TLC starts; expected failures must have TLC's safety exit status, their exact
  invariant, and their seeded action somewhere in the trace.

No JAR or generated model output is committed. A container image is avoided for
the initial toolchain because Java plus one checksummed artifact is the smaller
and more transparent dependency.

### Reproducible Kani setup

The implementation adds `make verify-kani`, backed by
`hack/verify-kani.sh`. Kani is deliberately not downloaded by the runner. A
maintainer installs the pinned release and its supporting tools explicitly:

```bash
cargo install --locked kani-verifier --version 0.67.0
cargo kani setup
make verify-kani
```

The runner:

- rejects a missing or incomplete Kani setup and any version other than 0.67.0
  before an ordinary proxy invocation can perform first-run setup;
- requires GNU `timeout` (or `gtimeout`), serializes invocations, checks that the
  source catalog still contains exactly seven proofs, and selects each harness
  by its exact fully-qualified name;
- applies a process-group 300-second per-harness cutoff plus a 10-second kill
  grace by default, and treats a timeout as a failed, unknown result rather
  than a pass;
- enables unstable stubbing and `proof-mutants` only for the isolated
  maintenance and negative-control harnesses;
- requires every coverage point, exact ordinary/expected-panic success mode,
  and the one exact named mutant assertion;
- clears stale evidence before writing complete output under `target/kani/`;
  and
- records elapsed time and peak resident memory in each log when GNU
  `/usr/bin/time` is available.

The final Kani 0.67.0 per-harness elapsed-time and peak-RSS measurements are
recorded in
[`formal/README.md`](../../formal/README.md#stage-5-kani-metrics).

### CI tiers

The intended future verification tiers are split by cost:

- **Rust PR gate:** history-checker unit tests and committed simulation corpora
  run through `make test-all` with the rest of the Rust suite.
- **Formal PR gate:** a dedicated job installs Java and runs the exhaustive
  safety configurations through `make verify-formal`.
- **Main/scheduled deep gate:** larger same-leaf/cross-leaf configurations and
  liveness checks run with explicit timeouts and upload counterexample artifacts
  on failure. Kani could join this tier only after the manual pilot has measured
  stable cost; no Loom suite currently exists.

No formal job is currently wired into CI. The pilot would begin non-required
while state-space size is measured and become required only after it runs
reliably within the agreed PR budget. Once such a job exists, a timeout, state
explosion, or checker error is a failed verification job, never a passing
unknown result.

`make test-all` remains the complete Rust format/lint/test command. It does not
silently download Java or Kani tooling. `make verify-formal` and
`make verify-kani` remain additional manual commands; neither is invoked by CI.
Review documentation lists `make verify-formal` as an additional requirement
once an accepted ADR makes the model a gate for protocol changes. Kani would
require its own measured gate decision.

### Change policy

Once a model layer is accepted, a change to its protocol vocabulary or invariant
must include:

1. the implementation and ordinary regression tests;
2. an update to the living architecture/design text;
3. an update to the owning TLA+ module and its properties;
4. a model scenario or mutant showing that the changed property is observable;
5. a history-checker regression when the behavior is public; and
6. successful `make test-all` and `make verify-formal` runs.

Pure refactors that preserve the modeled action boundary need no artificial
model diff. Conversely, passing TLC without connecting a changed public behavior
to the history checker is not sufficient evidence that the Rust implementation
still refines the model.

## Delivery plan and acceptance criteria

### Stage 0: semantic inventory

- Record the trusted backend and time assumptions above in `formal/README.md`.
- Define the abstract database, public outcome categories, and conservative
  in-doubt completion semantics.
- Write hand histories for success, abort, validated body error, lost
  acknowledgement, and post-notification completion.

**Exit:** reviewers can classify every current public transaction outcome
without referring to implementation control flow.

### Stage 1: logged transaction core

- Implement `Backend`, `TxCore`, and the safety model-checking wrapper.
- Cover fixed topology, logged multi-key transactions, read-only validation,
  lock acquisition, wound-wait, crash, lease expiry, and write-back.
- Establish `LogicalView = logical_db` and terminal-state invariants.
- Demonstrate expected counterexamples for the logged-path mutants.

**Exit:** TLC exhaustively checks the required two-transaction same-leaf and
cross-leaf configurations, and every seeded mutant is detected.

### Stage 2: direct commit and coordinator batching

- Add logless eligibility, same-key exclusion, oldest-first fold order,
  per-member participation, unavailable CAS recovery, replay, and locked
  fallback.
- Check two- and three-member coordinator rounds.
- Add direct-path mutants and liveness checks for contention convergence.

**Exit:** the model distinguishes definitive loss from uncertainty and proves
that no modeled operation can apply twice across direct replay/fallback.

### Stage 3: end-to-end history checker

- Implement the point-read/write transaction program and exact serialization
  checker.
- Add deterministic unit histories and corrupted-history tests.
- Integrate `HistoryWorkload` with tape, PCT, transport faults, crashes, and both
  cache modes.
- Add and seed its fuzz target.

**Exit:** the checker rejects injected lost-update, fractured-commit, stale-read,
real-time-order, and double-apply histories while accepting their legal controls;
committed corpus inputs replay byte-for-byte.

### Stage 4: composed subprotocols

- Add scans and membership validation to the history workload.
- Model and test B-link split publication.
- Model transactional collection lifecycle and stale-handle fencing.
- Model cache evidence/cancellation and transaction-object GC separately.
- Compose lazy holder publication, foreign wound, owner retirement, and GC.
- Add reduced interface-composition checks with `TxCore`.

**Exit:** every correctness-critical protocol named in `architecture.md` either
has a checked model contract or is explicitly listed as a trusted/deferred
assumption. The implemented scope meets this exit: `WoundRetirementSafety.cfg`
checks ADR-059's lazy-owner/foreign-wound/GC handoff, and the superseded
foreign-`Aborted` mutant demonstrates that the anti-resurrection invariant is
sensitive to removing the pinned marker.

### Stage 5: code-level proof pilot

- Pin Kani 0.67.0 and add a manual `make verify-kani` runner without changing
  `make test-all` or CI.
- Check three positive harnesses over the production lifecycle relation,
  full-cardinality median policy, and the production split-metadata kernel used
  by `Node::finish_split`.
- Reuse the median-cardinality contract under an upper-median stub as the
  representative maintenance change.
- Require three expected-panic mutants to expose direct `Wounded` reclamation,
  an empty split half, and inherited transient sibling lock holders.
- Decline Kani proofs of the production Shard/Index `BTreeMap::split_off`
  transforms after bounded feasibility runs fail the pilot cost test; retain
  their content invariants in deterministic Rust tests.
- Record exact per-harness elapsed time, peak RSS, and proof footprint after the
  pinned run.
- Decline Loom until a narrow production synchronization adapter exists, and
  decline Verus/Creusot until a production-used pure protocol core warrants an
  unbounded refinement proof.

**Exit:** met for the bounded manual Kani pilot. The harnesses exercise
production code rather than a proof-only shadow, all seven configured checks
finish under Kani 0.67.0, and the three negative controls make local property
sensitivity explicit. The upper-median maintenance change preserves the proof
without changing its assertions. No Stage 5 tool becomes a CI or architecture
gate in this change. Declining the `BTreeMap` target, Loom, and deductive
verification does not invalidate the TLA+/history-checking layers.

### Pilot success criteria

Before extracting an ADR, the pilot must show:

- an independent model with documented assumptions and no hidden implementation
  imports;
- exhaustive safety completion for the agreed finite configurations;
- detection of all required protocol mutants;
- deterministic reproduction and resolution of every faithful known-protocol
  counterexample before the suite is accepted as a correctness gate;
- at least one useful counterexample, ambiguity, or invariant clarification found
  while modeling, rather than only restating existing tests;
- an implementation history checker that catches all seeded end-to-end
  anomalies;
- deterministic reproduction from model traces and fuzz inputs;
- a stable CI runtime within the agreed budget; and
- a review by someone other than the model author of the abstraction and
  refinement mapping.

If the model cannot meet these criteria, it remains an exploration and does not
become a required gate.

## Alternatives considered

### Extend deterministic simulation only

The current simulator is exceptionally useful and already controls schedules,
faults, time, entropy, and cache media. More workloads and assertions will find
more bugs. It still samples rather than exhausts its schedule/fault space, and a
workload invariant can accept a non-serializable history that happens to preserve
that invariant. The history checker strengthens the oracle; TLA+/TLC provides
finite exhaustive exploration independent of executor coverage.

### Use Stateright as the primary model

Stateright would keep the model and checker in Rust, integrate naturally with
unit tests, and produce convenient counterexample paths. It also makes it easy
to reuse production types or transition functions, which increases the risk of
a common-mode specification bug. TLA+ is preferred for the independent first
model and direct temporal/refinement vocabulary. Stateright remains a reasonable
fallback if TLA+ maintainability fails the pilot, or a future implementation
model below the independent spec.

### Use Quint as the only source language

Quint offers a more executable authoring experience and can use TLC or Apalache.
Direct TLA+ removes a translation layer, exposes TLC's native temporal model,
and has the longer-lived proof/refinement ecosystem. The project should try
Quint rather than abandon the model if direct TLA+ syntax is the only adoption
barrier, but it should not maintain both sources.

### Begin with full Rust deductive verification

The current protocol crosses async traits, Tokio synchronization, object-store
I/O, caching, background tasks, and cancellation. Proving it directly would
require a substantial verified abstraction before the desired strict
serializability theorem could even be stated. The layered model first clarifies
that abstraction and identifies pure kernels worth proving.

### Use only a linearizability library

Generic register checkers do not directly represent multi-operation transaction
bodies, internal closure retries, range scans, collection incarnations,
read-derived errors, or the project's in-doubt completion rule. A small custom
checker can model exactly those semantics and exploit the deliberately bounded
simulation workloads. Its sequential transition function remains small enough
to audit and cross-check.

### Model all subprotocols at once

One model containing transaction execution, cache evidence, B-link topology,
collection drop, GC, persistent media, and every retry state would obscure
ownership and exhaust memory before exploring useful depth. Separate
assumption/guarantee models make each invariant reviewable and allow small
composition checks at their boundaries.

### Instrument every internal protocol transition immediately

Internal trace conformance could connect the Rust code more directly to TLA+
actions. It would also add hooks across nearly every correctness-sensitive
component, risk perturbing deterministic schedules, and bind the model to
implementation control flow. The public history checker offers a stable first
refinement boundary. Internal semantic events can be designed later if a
specific unclosed gap warrants them.

## Decision records

There are no ADRs for this proposal yet. The design remains a self-contained
exploration until the remaining pilot success criteria, including an agreed CI
budget and architecture-gate decision, are satisfied. ADR-059 independently
records the production owner/wound/GC repair; `WoundRetirement` is executable
bounded evidence for that decision, not an ADR for the verification toolchain.

If accepted, one focused ADR should record:

- TLA+/TLC as the independent protocol-modeling tool;
- the abstract sequential contract and conservative in-doubt completion rule;
- public-history checking as the implementation conformance boundary; and
- which verification jobs are required for protocol changes.

Additional ADRs are warranted only if verification causes a significant
production architecture change, such as extracting a verified protocol-core
crate. Tool experiments that remain test-only do not each need an ADR.

## Open questions / future work

- Can every production backend tighten the in-doubt linearization upper bound,
  or must cancellation and transport failure retain the conservative pending
  completion indefinitely until observed?
- What exact finite configurations fit the PR TLC budget, and which require a
  scheduled deep job?
- Does direct TLA+ remain understandable to maintainers after one non-author
  protocol change, or should the source move to Quint with TLC?
- How should a final-state checker bound collection names and incarnations while
  retaining useful stale-handle schedules?
- Is public-history conformance sufficient, or does one high-risk boundary such
  as coordinator in-doubt attribution justify internal semantic trace events?
- Which pure coordinator decisions can be factored for Kani without moving
  policy into the generic fold engine?
- Can a later deductive proof establish the refinement for arbitrary numbers of
  keys and transactions after TLC has established the finite design?
- Should the focused wound model expand production's owner-operation accounting
  and unresolved-mutation proof, or is its explicit retirement transition plus
  deterministic lifecycle regressions the right long-term abstraction boundary?
- How should the proposed snapshot-read protocol compose with the strict latest
  transaction history if that design proceeds?

## Relationship to other designs / ADRs

This proposal does not select a transaction or storage repair. ADR-059
independently records the implemented pinned-wound decision; this verification
design formalizes and tests that current contract alongside the other protocol
boundaries:

- [object-storage-native.md](object-storage-native.md), the umbrella design for
  content-CAS transactions, locks, leases, write-back, and GC;
- [dynamic-range-sharding.md](dynamic-range-sharding.md), the B-link topology and
  split protocol;
- [ADR-009](../adr/009-in-doubt-conditional-writes.md), mutation uncertainty;
- [ADR-011](../adr/011-guided-interleaving-executor.md), deterministic schedule
  and fault exploration;
- [ADR-019](../adr/019-unified-transaction-object.md) and
  [ADR-020](../adr/020-commit-write-back-protocol.md), logged commit and
  write-back;
- [ADR-021](../adr/021-wound-wait-leases-shard.md), foreign wound and lease
  behavior;
- [ADR-022](../adr/022-garbage-collection-mark-sweep.md), finite transaction
  tombstone retention and reclamation after an acknowledged terminal decision;
- [ADR-024](../adr/024-hold-and-wait-conflict-resolution.md), lazy transaction
  object materialization, lock conflict, and liveness behavior;
- [ADR-028](../adr/028-shard-mutation-coordinator.md) and
  [ADR-029](../adr/029-gc-through-shard-coordinator.md), coordinated leaf
  mutation;
- [ADR-043](../adr/043-causally-coordinated-backend-operations.md), cache
  evidence and cancellation ordering;
- [ADR-047](../adr/047-transactional-collection-management.md), transactional
  collection lifecycle;
- [ADR-051](../adr/051-inline-latest-values.md),
  [ADR-053](../adr/053-replay-definitive-logless-rmw-losses.md), and
  [ADR-054](../adr/054-reserve-inline-publication-for-logless-commits.md), direct
  commit and current-value representation;
- [ADR-057](../adr/057-bounded-in-doubt-commit-recovery.md), the interaction
  between ambiguous finalization and reclamation; and
- [ADR-059](../adr/059-pin-foreign-wounds-until-owner-retirement.md), pinned
  foreign wounds, owner retirement proof, acknowledgement, and subsequent GC.

The current deterministic-simulation comparison in
[testing-dst.md](../guides/testing-dst.md) remains valid. Formal checking is an
additional exhaustive design layer and stronger oracle, not a replacement for
schedule-guided execution of the real engine.
