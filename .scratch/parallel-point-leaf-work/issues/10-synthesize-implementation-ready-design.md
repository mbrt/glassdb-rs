# Synthesize the implementation-ready design

Type: prototype
Status: resolved
Blocked by: 03, 04, 05, 06, 07, 08, 09

## Question

What final interfaces, invariants, file-level changes, implementation order, regression tests, benchmarks, and minimal ADR or documentation updates combine the resolved decisions into one implementation-ready design without expanding the destination?

## Prototype

The [implementation-ready design prototype](../assets/implementation-ready-design-prototype.html)
combines the final interfaces, capacity ownership, file changes, implementation
order, verification gates, and difficult state transitions. Human review
accepted the design after these final changes:

- a private five-field phase-limit bundle, with every initial value set to 16;
- no shared GlassDB backend-work limit or public concurrency setting;
- temporary benchmark instrumentation only when it has no measurable effect;
- logical validation for every locked point read, independent of how its hold
  was acquired;
- owner-level transaction identity renewal before sorted serial acquisition;
  and
- best-effort local hold observations for diagnostics only.

This artifact is a planning prototype. It does not implement the design.

## Answer

### Design shape

Add one generic bounded-join module. Keep all combination and ordering rules in
the domain module that understands them:

- `TreeRouter` combines point keys that currently route through the same
  physical path.
- `NodeStore` combines physical checks only for the same exact leaf state.
- `KeyResolver` resolves the logical point state for all keys in one routed
  leaf group.
- `KeyLocker` combines every intention for one leaf into one atomic
  coordination member.
- committed write-back keeps one bounded position for each original
  `LockedTx` group.

`AccessSet` remains the shared logical fact. Do not add a persistent routed
point-leaf plan. A `RoutedLeafGroup<T>` is a temporary routing result. A
`LockedTx` is the retained in-memory result of one complete lock-acquisition
pass. The transaction object remains the one cross-leaf commit point.

### Transaction-local limits

Add this crate-private policy value in `glassdb-trans`:

```rust
struct PointLeafLimits {
    routing: NonZeroUsize,
    physical_validation: NonZeroUsize,
    logical_validation: NonZeroUsize,
    normal_locking: NonZeroUsize,
    write_back: NonZeroUsize,
}

impl PointLeafLimits {
    fn all(limit: NonZeroUsize) -> Self;
}
```

Production engine assembly uses `PointLeafLimits::all(16)`. The fields have the
same initial numeric value, but they do not share runtime admission state. Each
limit applies to one invocation of one transaction phase:

- distinct node-path work during point-key routing;
- distinct leaf-state work during optimistic physical point validation;
- routed leaf-group work during logical point validation;
- complete leaf-group work during normal lock acquisition; and
- original `LockedTx` group work during committed write-back.

A wait remains incomplete and consumes one position. Unused positions reserve
no work or backend capacity. The sorted serial lock path does not use the
normal-locking limit.

Do not add a `DatabaseBuilder` setting, database-wide or process-wide
semaphore, shared backend handle, or global fairness rule. The backend and the
provider own aggregate queues, calls, connections, retries, and throttling.
The value 16 is an absolute transaction-local submission bound. It is not a
capacity percentage.

### Final internal interfaces

#### Bounded foreground join

```rust
pub async fn join_all_bounded<I>(
    futures: I,
    limit: NonZeroUsize,
) -> Vec<<I::Item as Future>::Output>
where
    I: IntoIterator,
    I::Item: Future;
```

The zero-input and one-input cases use direct paths. For multiple inputs, admit
in input order, keep at most `limit` incomplete futures, run every input, and
return outputs in input order. A returned value does not stop later admission.
Do not spawn one task per input. Dropping the join drops both admitted and
stored futures.

#### Point-key routing

```rust
pub struct RoutedLeafGroup<T> {
    pub observation: LeafObservation,
    pub keys: Vec<(Vec<u8>, T)>,
}

pub async fn group_keys_by_leaf<T>(
    &self,
    items: impl IntoIterator<Item = (KeyRef, T)>,
    requirement: Requirement,
    limit: NonZeroUsize,
) -> Result<Vec<RoutedLeafGroup<T>>, StorageError>;

pub async fn group_keys_by_leaf_fresh<T>(
    &self,
    items: impl IntoIterator<Item = (KeyRef, T)>,
    interior: Requirement,
    leaf: Requirement,
    limit: NonZeroUsize,
) -> Result<Vec<RoutedLeafGroup<T>>, StorageError>;
```

`RoutedLeafGroup<T>::path()` comes from its leaf observation. It has no duplicate
path, separate currentness flag, or durable ownership meaning. Return groups in
physical-path order. Preserve original input order inside each group. Keep an
internal input ordinal until routing and stable error selection finish.

Zero and one item use direct descent. Multiple items use a path-aware ready set
inside `TreeRouter`. Spend the limit on distinct physical paths, not keys. Keep
B-link right-link correction, path convergence, and reprocessing when a former
leaf becomes an index. Existing garbage-collection callers pass a limit of one;
this work does not parallelize garbage collection.

#### Physical and logical point validation

```rust
pub async fn check_leaves_current(
    &self,
    observations: &[LeafObservation],
    validation_start: SequencePoint,
    limit: NonZeroUsize,
) -> Vec<Result<LeafObservationCheck, StorageError>>;

pub(crate) async fn effective_point_states(
    &self,
    keys: &[KeyRef],
    own_lock_holder: Option<&TxId>,
    validation_start: SequencePoint,
    limit: NonZeroUsize,
) -> Result<Vec<PointValidationState>, StorageError>;
```

`Algo` samples `validation_start` once and supplies it to every leaf and
transaction-status read in that validation episode. Neither batch interface
samples time.

Physical checking is the optimistic path before point locks are held. Group by
physical path in first-input order. One path checks its distinct states
serially. Combine only observations for which `Observation::same_state` is
true, and advance the evidence of every combined observation. Do not combine
different revisions or independent absence observations. Run every path future
and return one result for each input position.

Logical validation routes the complete normalized point-read set. It does not
route only a changed leaf. Use `Requirement::Any` for interior index nodes and
`Requirement::AtLeast(validation_start)` for terminal leaves. Resolve keys
serially inside one routed leaf group and different leaf groups through the
bounded join. Return results in normalized point-read order and select an error
by the smallest original input ordinal.

Optimistic validation first tries the physical batch and uses the complete
logical batch on a changed observation. Locked point reads always use the
complete logical batch. They pass their transaction identity as
`own_lock_holder`; optimistic validation passes `None`. A completed `LockedTx`
has already proved one complete hold receipt for every group. Its successful
CAS or retained-hold load also supplies cache evidence at the shared lower
bound, so a warm locked point validation adds no backend operation. Cache
eviction or topology churn can require a backend check.

Compare the effective writer for every point read. For an absent read, also
compare its membership version. Keep transaction-dependent interpretation in
`KeyStateResolver`, including committed, not-written, deleted, pending,
unknown, aborted, and wounded holders. Keep the exact `Installed` state shortcut
only for unchanged range coverage, where the alternative is a complete range
re-scan. Range-validation phase order otherwise does not change.

#### Lock acquisition and hold receipts

```rust
struct KeyLocker {
    normal_limit: NonZeroUsize,
    write_back_limit: NonZeroUsize,
    observed_holds: Arc<Sharded<LockerShard>>,
}

enum LeafHoldReceipt {
    Installed {
        cas_precondition: LeafObservation,
        held: HeldLeaf,
    },
    Observed {
        loaded_observation: LeafObservation,
        held: HeldLeaf,
    },
}

pub(crate) struct CoordinatedOutcome {
    pub(crate) outcome: FoldOutcome,
    pub(crate) cas_precondition: Option<LeafObservation>,
    pub(crate) loaded_observation: Option<LeafObservation>,
}
```

An `Installed` receipt means this acquisition member staged work in a successful
leaf CAS. An `Observed` receipt means a bounded load found every required hold
for the same transaction identity, so no CAS was needed. Both receipt kinds
prove a complete hold and supply a currentness anchor for write-back. Point
validation does not branch on the receipt kind.

`AcquireOperation` uses `Requirement::AtLeast(validation_start)` for its first
leaf load because it can return `Observed` without a following CAS. This makes
the cached leaf evidence sufficient for locked logical validation. A successful
`Installed` CAS advances its precondition evidence past the same bound.

Build a `LockedTx` only after one complete pass returns exactly one receipt for
every current routed group. Never carry partial receipts into another pass.
Normal parallel acquisition uses the private bound. A foreign-holder wait keeps
its bounded position while other positions can finish. Run every complete group
and select the first non-`Locked` result in stable leaf-path order.

A completed `Conflict` or `LeafFull` pass keeps any physical locks that landed.
Discard its partial receipts, route the complete access set again, and retry the
complete group set. A complete same-identity hold returns `Observed`. A partial
same-identity hold runs one idempotent complete-leaf CAS. If a CAS landed but
its result was lost, the next full-set pass observes the complete hold and does
not add a second CAS.

Remove `KeyLocker::held_paths`, aggregate `release_locks`, and every normal
retry or serial-transition call that uses them. Keep exact-path release for
garbage collection and durable-log recovery. The durable recovery manifest
written by `Monitor::record_tx_locks` remains protocol state.

#### Serial transition

```rust
pub enum AttemptControl {
    Complete,
    RenewForSerial { replay_body: bool },
}

Engine::retire_for_serial(
    &self,
    &mut EngineTransaction,
) -> Result<(), TransError>;

Engine::rebegin_for_serial(
    &self,
    EngineTransaction,
) -> EngineTransaction;
```

`Algo` returns the control value when the parallel acquisition episode must
end. `AttemptDriver` owns the renewal loop. It closes admission for the old
transaction identity, persists `Wounded`, and only then creates the new
transaction identity. A dropped or timed-out conditional write remains an
unresolved owner operation until this retirement makes its late result
abort-side.

The new identity samples a new validation lower bound, reacquires collection
directory locks, and enters sorted serial acquisition directly. The serial path
has one incomplete leaf operation at a time and no timeout. Point and range
transactions retain the transaction body's access set and normal outcome.
Collection create or drop retains its existing transaction-body replay rule.

#### Committed write-back

Bound the original `LockedTx` groups. Each bounded position owns one original
group through all rerouting. If a split moved its keys, route that group's
descendants and process them serially inside the same position. A wait or
structural deferral keeps that position incomplete.

Run every original group even when another group fails. A local leaf failure or
deferral does not change the committed transaction result. Record it through a
stable `glassdb::write_back` diagnostic event. Collect superseded transaction
identity hints only after all positions finish, then sort and remove duplicates
before garbage-collection notification. Graceful shutdown drains committed
write-back debt through the existing background-work owner.

#### Diagnostics

Keep the public `Database::diagnostics().transactions` shape. Rename the private
`KeyLocker::tlocks` field to `observed_holds`. Record an observation only after
this process receives a complete `Locked` outcome, including `Installed` and
`Observed` receipts. Keep its snapshot and lifecycle-clear operations.

This map is best-effort local operator data:

- absence does not prove that no physical lock exists;
- presence can become stale after recovery or another process acts;
- it does not include holds known only to another process; and
- it must not build `LockedTx`, select a receipt, drive release or retry, prove
  cleanup, or control serial mode.

Document its per-acquisition maintenance cost. Do not describe it as pull-only
or zero-cost. Keep `Monitor::record_tx_locks` separate and unchanged.

#### Benchmark-only opening and telemetry

Production builds expose no limit setting. Add a non-default
`bench-internals` feature in `glassdb`. Under that feature, expose a doc-hidden
free opening function and a five-field benchmark value that converts to the
private `glassdb-trans` policy. Do not add a builder setter or global state.

```rust
#[cfg(feature = "bench-internals")]
#[doc(hidden)]
pub mod benchmark {
    pub struct PointLeafLimits {
        pub routing: NonZeroUsize,
        pub physical_validation: NonZeroUsize,
        pub logical_validation: NonZeroUsize,
        pub normal_locking: NonZeroUsize,
        pub write_back: NonZeroUsize,
    }

    pub async fn open_with_point_leaf_limits(
        builder: DatabaseBuilder,
        limits: PointLeafLimits,
    ) -> Result<Database, Error>;
}
```

Use source-qualified optional stress fields:

```rust
enum TelemetryAvailability {
    Measured,
    External,
    NotExposed,
}

struct TelemetryField<T> {
    value: Option<T>,
    source: String,
    availability: TelemetryAvailability,
}
```

Prefer an existing transport or provider observation when it measures the
required quantity. If no accurate source exists, a throwaway benchmark branch
can add a temporary hook. Keep the instrumentation commit and raw results, then
remove the hook from the implementation branch. Accept its measurement only
when:

- a deterministic check proves that it adds no backend operation; and
- at least three converged, alternating instrumented and uninstrumented pairs
  keep throughput, p95, and p99 inside the existing two-percent comparison
  band.

Otherwise record `NotExposed`. Do not infer queue depth or connection count
from active-call or incomplete-leaf-future counters.

### File-level changes

- `crates/glassdb-concurr/src/join.rs` and `src/lib.rs`: add and export
  `join_all_bounded`; keep direct zero and one paths.
- `crates/glassdb-storage/src/tree_router.rs`, `src/lib.rs`, and `Cargo.toml`:
  add `RoutedLeafGroup`, path-batched descent, stable grouping and error
  selection, and the required futures dependency.
- `crates/glassdb-storage/src/node_store.rs`: add input-aligned batch
  currentness checks, grouped by physical path.
- `crates/glassdb-trans/src/engine.rs`: own the private phase-limit bundle and
  production defaults, and pass each limit to its domain owner.
- `crates/glassdb-trans/src/algo/direct_commit.rs`: depend directly on
  `TreeRouter`; reject zero inline policy and oversized values before routing;
  remove `KeyResolver::route_one_leaf` after callers move; keep one-key backend
  operation count.
- `crates/glassdb-trans/src/key_resolver.rs` and `key_state_resolver.rs`: route
  the complete logical validation set, pass `own_lock_holder` into entry
  interpretation, return input-aligned point states, bound leaf futures, and
  select stable errors.
- `crates/glassdb-trans/src/gc.rs`: pass a routing limit of one at the two
  existing group-routing calls.
- `crates/glassdb-trans/src/shard_coord.rs`, `node_locking.rs`, and
  `tlocker.rs`: return loaded observations for skipped members, add hold
  receipts, recognize complete retained holds, bound normal acquisition and
  write-back, and remove foreground release control.
- `crates/glassdb-trans/src/algo.rs`, `algo/attempt.rs`, `engine.rs`, and
  `crates/glassdb/src/db.rs`: add batch point validation, always use logical
  validation for locked point reads, return owner-level renewal control, and
  retire and renew inside `AttemptDriver`.
- `crates/glassdb/src/diagnostics.rs`: document best-effort local hold
  observations and stable write-back failure events without changing the
  public result shape.
- `crates/glassdb/Cargo.toml`, `crates/glassdb/src/db.rs`,
  `crates/glassdb/src/lib.rs`, and `crates/glassdb-trans/Cargo.toml`: add the
  non-default benchmark opening and its internal conversion.
- `crates/glassdb-bench-scale/Cargo.toml` and
  `src/bin/perfbench/{main,point_leaves,telemetry,backend}.rs`: add exact
  point-leaf fixtures, active-call telemetry, source-qualified optional data,
  and drain-inclusive timing.
- `hack/aws-bench/run-perfbench.sh`, `deploy.sh`, `cloudformation.yaml`,
  `README.md`, `compare.py`, and `test_compare.py`: add the limit matrix,
  500-by-32 stress modes, provider and host sampling, paired comparison, and
  stable result capture.

### Implementation order

1. Add `join_all_bounded` and prove its complete contract.
2. Add `RoutedLeafGroup` and path-batched routing. Move direct commit to
   `TreeRouter` before removing the old one-key resolver helper.
3. Add the input-aligned physical and logical point-validation batches. Use one
   shared lower bound and make locked point validation uniformly logical.
4. Add `Installed` and `Observed` hold receipts and complete retained-hold
   recognition.
5. Add bounded full-set normal acquisition and owner-level serial renewal as
   one safety change. Only then remove aggregate foreground release as control
   state.
6. Bound original committed write-back groups and keep split descendants inside
   their original positions.
7. Add deterministic regressions, calibrate 8/16/32 against the parent, run the
   backend stress cases, update the decision record and living documentation,
   and run `make test-all`.

Do not combine steps 4 and 5 partly. Retained locks without complete receipts,
or release removal without durable old-identity retirement, is not an accepted
intermediate protocol.

### Deterministic verification

Use gates and counters, not elapsed-time assertions.

- Bounded join: zero and one input, limits below and above input count, stable
  admission, reverse completion, value errors, 17 gated futures at limit 16,
  every input, and dropping admitted plus stored futures.
- Routing: shared paths; cold 1, 2, 8, and 32-leaf fixtures; stale separators;
  converging right links; a former leaf that becomes an index; stable smallest
  error ordinal; and no added one-key operation.
- Direct commit: zero inline policy, oversized values, one eligible leaf,
  multi-leaf fallback, reroute, locked result, stable candidate counters, and
  independent fallback routing through `KeyLocker`.
- Physical point validation: exact-state clones with evidence propagation,
  different revisions on one path, independent absences, stable mixed errors,
  and 17 gated paths.
- Logical point validation: every holder status and value case, stable result
  and error ordinals, `Installed` and `Observed` locked point receipts using the
  same path, own-holder interpretation, and a warm post-acquisition pass with no
  backend operation.
- Normal acquisition: a foreign wait that occupies one position; mixed
  `Locked`, `Conflict`, `LeafFull`, and operational errors; stable leaf-path
  selection; complete and partial same-identity holds; and a landed CAS whose
  result is lost.
- Serial renewal: gate the old conditional write after dispatch and before
  result delivery; cover timeout and conflict threshold; prove old `Wounded`
  before new publication, a new lower bound, collection-directory lock
  reacquisition, no point or range transaction-body replay, and collection
  create/drop replay.
- Write-back: 17 original groups, split descendants, structural deferral, one
  local failure, one routing failure, stable duplicate hints, every original
  group, and graceful shutdown.
- Simulation: replay every new difficult case under normal and simulation builds
  with the same seed and schedule. Require the same selected result and backend
  operation stream.
- Every bounded interface: zero and one input. Zero returns directly. One does
  not build the multi-future queue or add a backend operation.

### Performance and stress verification

Build exact 1, 2, 8, and 32 distinct-leaf fixtures by putting one pre-created
key in each of that many collections. Use point reads, logged blind overwrites
with `InlinePolicy::none()`, and logged point read-modify-write transactions.
Run each with a primed decoded cache and with cache capacity zero.

Use memory, simulated GCS, and simulated S3 backends with explicit `none`,
`gcs`, and `s3` delay profiles. Preserve provider throttling in the GCS and S3
profiles. Run one and multiple `Database` instances through the existing worker
sweep. Use at least three converged paired runs and cross-run medians.

The primary selection cells are the 8-leaf and 32-leaf combinations of every
workload, cache state, delay profile, and converged one- and multi-`Database`
plateau. Keep 1-leaf and 2-leaf cells as operation and shape regressions. Record
p50, p95, p99, transactions per second, backend reads and writes per
transaction, retries, lock calls, maximum active backend operations, and
drain-inclusive shutdown debt.

Keep the existing direct-commit benchmark groups as one-leaf gates. There is no
repository-wide absolute transaction-latency objective now. Report absolute
stress p95 and p99 until an accepted deployment objective exists.

Run 500 concurrent 32-leaf transactions as separate cold-burst and
long-duration cases on real S3 and GCS. Start with limit 16 and a paired parent
run. At limit 16, the first admission can contain 8,000 incomplete leaf futures;
this is not a request or connection count. Also record backend queue depth,
provider requests, connections, retries, provider errors, CPU, memory, file
descriptors, network use, and applicable deployment limits. Use a bounded
percentile accumulator for long runs.

Use a new transport client for each cold-pool cell. Reuse one cell client for
its warm run and for every `Database` in that cell. Store each optional field's
source, availability, and sampling interval. Use transport interception or
fake-provider counters where available, and provider or process monitoring for
external data.

Retain the production default of 16 only when all these conditions hold:

- every one-leaf backend operation sequence and count matches the parent;
- 16 reaches at least 95% of the best stable 8/16/32 throughput in every
  primary cell;
- limit 16 gives one transaction-local wave through 16 leaves and no more than
  two through 32 equal-wait leaves;
- accepted workload latency objectives are met;
- no repeatable parent throughput regression exceeds 5%; and
- stress runs have no crash, resource exhaustion, sustained provider-error
  instability, or unbounded benchmark memory.

Use 32 if it gives more than 5% additional stable throughput or an accepted
workload requires one transaction-local wave through 32 leaves. If one phase
reaches backend saturation earlier, lower only that private phase field. Do not
derive a shared GlassDB limit or capacity percentage from the stress result.

### Decision record and living documentation

During implementation, add
`docs/adr/064-bounded-point-leaf-work-and-renewed-serial-fallback.md`. Keep it
focused on these significant decisions and trade-offs:

- domain-owned grouping over one generic bounded join;
- independent private transaction-phase limits with initial value 16;
- no GlassDB aggregate backend scheduler;
- retained physical locks across normal full-set retries;
- durable old-identity retirement before forced serial renewal; and
- bounded best-effort committed write-back.

The new decision supersedes ADR-024's same-identity foreground-release
serial-fallback rule and ADR-025's assumption that receipt-based release can
always clear a late cancelled acquire. It narrows ADR-026's statement that
release runs during serial fallback; release remains valid for abort and
recovery work. Add only forward status or supersession links from those older
decision records to ADR-064. Do not edit their accepted bodies. Other older
decision records keep their current status unless implementation finds a direct
contradiction.

Update `docs/architecture.md`, `docs/designs/object-storage-native.md`, and
`docs/guides/caching.md` for the implemented interfaces, hold receipts, locked
logical point validation, serial renewal, diagnostics, and write-back behavior.
Update `docs/guides/perf.md` only after measurements exist. Do not update
`CONTEXT.md`; its terms already cover this design. Do not add a second design
document or a public configuration guide. Do not update `PORTING.md`.

### Explicit non-goals

This design does not add a public batch point-read interface, change
`Transaction::read`, parallelize range scans or garbage collection, remove the
sorted serial lock path, add a cross-leaf logless commit, add a shared backend
scheduler, or clean up unrelated historical terminology. It makes the current
point-access protocol bounded and parallel without changing its commit point.
