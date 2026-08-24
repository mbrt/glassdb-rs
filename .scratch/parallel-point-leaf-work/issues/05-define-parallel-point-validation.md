# Define parallel point validation

Type: grilling
Status: resolved
Blocked by: 01, 03, 04

## Question

Which batch interfaces should physical point observation checks and logical point revalidation expose, and how should their implementations use the bounded join while sharing one validation lower bound, returning stable ordered results, resolving transaction status correctly, and removing the current repeated receipt search and serial per-key routing?

## Answer

Add two internal, validation-specific batch interfaces. Their Rust lifetimes can
follow their callers, but their semantic shapes are:

```rust
NodeStore::check_leaves_current(
    observations: &[LeafObservation],
    validation_start: SequencePoint,
    limit: NonZeroUsize,
) -> Vec<Result<LeafObservationCheck, StorageError>>

KeyResolver::effective_point_states(
    keys: &[KeyRef],
    validation_start: SequencePoint,
    limit: NonZeroUsize,
) -> Result<Vec<PointValidationState>, StorageError>
```

`Algo` samples `validation_start` once. It supplies that same lower bound and
the same internal point-validation limit to physical checking, path-batched
routing, terminal-leaf loads, and transaction-status resolution. These modules
do not sample time. The exact limit remains for
[Choose concurrency limits and verification](09-choose-concurrency-limits-and-verification.md).

### Physical point observations

`NodeStore::check_leaves_current` keeps each input ordinal and returns one result
in that position. It groups observations by physical path in first-input order
and runs one `join_all_bounded` future per path. Distinct states of one path run
serially in input order, including after an earlier `Changed` result or storage
error. Thus the limit is spent on distinct leaves, every supplied observation
gets a real result, and every supplied path future runs.

Combine only observations for which `Observation::same_state` is true. This
includes clones that share evidence and present observations of the same path
and revision after cache eviction. A successful shared check must advance every
matched observation's currentness evidence to the lower bound. Do not combine
different revisions. Do not combine independent absence observations only
because both have no revision; keep them in the same path future and check them
separately. Expand a combined result back to every original ordinal.

The zero-path and one-path cases use the direct paths from
[Define the bounded distinct-leaf execution contract](01-define-bounded-distinct-leaf-execution-contract.md).
A one-leaf validation adds no queue and no backend operation beyond the check it
already requires.

For locked validation, keep `LockedTx::validated` as the small interface. Replace
its scan of all receipts with `groups.get(observed.path())`, then require the
successful lock CAS receipt's observation to satisfy `same_state`. Do not add a
second receipt index or pass `LockedTx` into `NodeStore`.

An exact receipt can also validate an observed `Write` or `Create` holder when
the holder is this transaction. The transaction cannot commit before its own
validation, and a wound does not make it the effective writer. This exception
avoids a complete logical pass and its possible backend reads during a
transaction-body replay. An exclusive holder with a different transaction ID
always disables the physical shortcut because its status can change the
effective writer without changing the leaf. A receipt mismatch also disables
the shortcut.

After all physical path futures finish, `Algo` interprets the input-aligned
results in normalized point-read order. For each read, an operational error is
considered before `Changed`, a receipt mismatch, or the exclusive-holder rule.
An earlier need for logical fallback keeps the current behavior and suppresses
a later physical error, although the later check has run.

### Logical point revalidation

When the physical shortcut fails, revalidate the complete point-read set. Do
not route only the changed leaf. Carry each input ordinal as the payload through
the path-batched `TreeRouter` design from
[Choose the point-key batch routing design](02-choose-point-key-batch-routing-design.md).
Use cached `Requirement::Any` descent for interior nodes and
`Requirement::AtLeast(validation_start)` for terminal leaves. Reprocess a
refreshed terminal path if it became an index, so B-link routing remains
self-correcting.

After routing, run one bounded future per stable `RoutedLeafGroup<T>`. A leaf future
checks collection liveness once, reads the membership version once, and resolves
its keys serially in their input order. Waiting for collection state or a
transaction status keeps that leaf future incomplete and therefore consumes one
bounded position. Place each `PointValidationState` into its original input
position.

Keep transaction-dependent entry interpretation in `KeyStateResolver`. Use the
shared lower bound for every status lookup:

- `Ok` with a final-log write for the key makes the holder the effective writer,
  including a delete.
- `Ok` without that key keeps the predecessor writer. A transaction can retain
  an old exclusive lock after a transaction-body replay without writing that key
  in its final access set.
- `Pending`, `Unknown`, `Aborted`, and `Wounded` keep the predecessor writer.
- More than one holder on an exclusive entry remains an invariant error.

Do not add a global holder batch. Keys on one leaf resolve in order, while
different leaf futures can resolve status concurrently. Existing transaction
object caching and same-path coordination can combine their physical reads.

Every supplied leaf future runs. One leaf future can stop at its first key-state
error. After all leaf futures finish, select the error attached to the smallest
original key ordinal, independent of completion or leaf-path order. A routing
ambiguity uses object path only as the tie-break already defined by
[Choose the point-key batch routing design](02-choose-point-key-batch-routing-design.md).
Only after successful state resolution does `Algo` compare every read predicate
in normalized input order. A mismatch is a normal validation outcome and keeps
the transaction snapshot-transparent through the existing retry path.

Range-scan validation and its phase ordering do not change.

### Verification obligations

[Choose concurrency limits and verification](09-choose-concurrency-limits-and-verification.md)
must cover the following validation cases in addition to its existing question:

- input-aligned physical results, the incomplete-future limit, expected waves,
  all-path execution, and stable outcome selection;
- exact-state combination with evidence propagation, different revisions on
  one path, and independent absence observations;
- direct keyed receipt lookup, the exact own-holder shortcut, and mandatory
  fallback for a foreign exclusive holder;
- stable multi-leaf logical results and errors for committed, not-written,
  deleted, pending, unknown, aborted, and wounded holders; and
- zero-key and one-key direct behavior, a warm post-lock replay with no added
  backend read, and deterministic replay of the same operation stream.
