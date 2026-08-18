# ADR-061: Atomic logless commits within one leaf

## Status

Accepted — implemented.

Supersedes [ADR-051](051-inline-latest-values.md)'s initial direct-commit
eligibility, which was limited to one `Put` of an existing key and reads of that
same found key. ADR-051's authoritative inline representation and bounded
admission remain unchanged.

Refines [ADR-053](053-replay-definitive-logless-rmw-losses.md)'s replay and
locked-fallback policy for whole multi-key members, and
[ADR-056](056-demand-driven-inline-pressure-splits.md)'s pressure policy by
declining multi-key direct commits without requesting a split. It extends
[ADR-028](028-shard-mutation-coordinator.md)'s member-atomic fold with
multi-entry logless publication.

[ADR-062](062-splitter-driven-tombstone-reclamation.md) defines the lifetime of
the tombstones published here and the absence evidence needed once they can be
reclaimed.

## Context

The current direct path commits one small overwrite by publishing its value in
one conditional leaf rewrite. It needs no transaction object, lock, or
write-back because the leaf contains both the validated predecessor and the new
authoritative value.

The same argument applies to a larger transaction when its complete dependency
set and complete result share one leaf. The coordinator already folds one
member's keys atomically, and the backend CAS already makes one leaf the
linearization unit. Sending these transactions through the logged protocol adds
preparation, a transaction object, locks, and write-back without adding an
atomicity boundary.

Broader coverage must not weaken the defining properties of direct commit:
there is one logless commit CAS, every value is durable in that CAS, and an
uncertain outcome is reported honestly. In particular, validating a read on
another leaf before writing this one would leave a race between the validation
and commit and can admit cross-leaf serialization cycles.

## Decision

### Admit complete point-access transactions on one leaf

A data transaction is a direct candidate when all of the following hold:

- it has at least one final `Put` or `Delete`;
- it has no range scan or collection-catalog access;
- every point read and write in its complete dependency set currently routes to
  the same leaf; and
- the complete post-commit leaf state is directly publishable.

Reads need not target written keys, and writes may mix creates, overwrites, and
deletes. The engine treats every recorded read as a dependency because it
cannot infer which reads influenced the transaction body.

There is no direct-specific key-count limit. Leaf admission bounds durable
output, while coordinator cost from a very large read set is measured and may
justify a later policy limit.

A clean topology change before an uncertain CAS is rerouted. Direct commit is
retried only if the complete dependency set still shares one leaf; otherwise
the transaction uses the regular locked protocol.

### Publish the complete result in one CAS

Every `Put` publishes `Inline { writer: txid, value }`, and every `Delete`
publishes `Tombstone { writer: txid }`. A tombstone is authoritative absence
evidence and, like an inline value, its logless writer need not have a
transaction object.

All put values must satisfy the per-value inline limit. Admission then evaluates
the aggregate inline budget and exact encoded size against the complete
post-state, accounting for values replaced or deleted by the same transaction.
Every output is admitted or none is.

The coordinator resolves the member against one running leaf state, validates
all point dependencies, and stages all output entries together. One conditional
leaf rewrite is the commit point. Direct commit creates no transaction object,
installs no lock, performs no preparatory mutation, and needs no write-back.

An actual absent-to-present or present-to-absent transition advances the leaf's
membership generation in that same CAS. It may proceed only while the
structural gate and collection-deletion fence are absent and no live or unknown
membership holder conflicts. Finalized entry or membership holders may be
reconciled as part of the fold; direct commit never waits for, wounds, or
otherwise changes a live holder before its commit CAS.

Independent direct transactions may share one coordinator CAS. The fold gives
them a deterministic serial order, but each transaction remains a separate
commit member with its own output markers and outcome.

### Keep admission failure detached from splitting

A multi-key candidate that fails per-value, aggregate, or exact-size admission
uses the locked protocol without requesting an inline-pressure split. A split
could divide the dependency set and permanently remove its direct eligibility;
the failed transaction does not justify that irreversible topology change.

ADR-056's existing single-key pressure request remains, as do ordinary
post-mutation soft-cap splits. A rejected multi-key transaction neither waits
for structural work nor requests that work.

### Exclude overlapping members atomically

No later member in one coordinator round may overwrite an earlier logless
member's output marker. If their written-key sets overlap, the later member is
excluded as a whole before staging anything.

A transaction with any point-read dependency replays its body after a
certified stale read or same-round exclusion. A blind transaction uses the
regular locked path after exclusion, preserving bounded progress rather than
resubmitting indefinitely. A live or unknown holder, structural gate,
collection-deletion fence, stable admission failure, or other state requiring
coordination also selects the locked path.

These decisions are member-atomic: direct commit never publishes a subset,
never combines direct publication with a logged remainder, and never replays a
member whose own CAS may have landed.

### Recover uncertainty from this transaction's evidence

After an unavailable commit CAS, any exact surviving output marker belonging to
this transaction proves the entire member landed. The leaf CAS was atomic, so
one marker proves every output even when other markers were later overwritten
or reclaimed.

With no marker, the resolver may retry only when current state proves that the
attempted CAS did not land and the read predicate remains valid. That proof must
account for ADR-062 tombstone reclamation. Ordinarily, every written target still
naming its recorded predecessor supplies the non-landing proof. It does not do
so when the only distinguishing output could have been a tombstone reclaimed
back to unmarked absence. If neither landing nor non-landing is provable, the
result is `InDoubt`.

Recovery evidence is transaction-local. A marker from another transaction is
not evidence for this one, even when both were staged in the same physical CAS.
Consequently one member may report success while a peer from the same CAS
reports `InDoubt`.

If an uncertain CAS is followed by a split that moves any relevant key, recovery
does not chase markers across leaves. It reports `InDoubt` unless the outcome
was already proved on the original leaf. This deliberately expands uncertainty
for the strictly logless protocol.

Cancellation retains ADR-051's boundary: before dispatch it leaves no state;
after dispatch the CAS may have committed and cancellation is crash-equivalent.

### Keep observability transaction-based

Direct `candidates` and `landed` statistics count transactions, not keys or
backend CAS attempts. Detailed fallback classifications remain diagnostic
rather than becoming permanent public counters.

Before acceptance, performance validation covers 2-, 8-, and 32-key blind put,
mixed put/delete, and cross-key read-modify-write transactions, under low and
same-leaf contention and near both inline admission boundaries.

## Consequences

- A transaction whose complete point dependency set shares a leaf can commit
  atomically with one backend mutation regardless of how many keys it changes.
- Creates and deletes gain the logless path, and tombstone writer IDs no longer
  imply transaction-object existence.
- Transactions with cross-leaf reads cannot use direct commit even when every
  write shares one leaf.
- Admission and conflict are all-or-nothing; a single ineligible dependency
  moves the whole transaction to replay or locked fallback.
- Large dependency sets can occupy a coordinator round for longer because no
  arbitrary count cap is imposed.
- More direct outcomes can be in doubt, especially all-delete transactions
  whose markers are reclaimed or transactions racing a split.
- Co-batched transactions share a physical linearization point but not recovery
  evidence.

## Alternatives considered

### Validate other leaves before the commit CAS

Another leaf can change after validation and before the commit leaf is written.
Without a lock or shared CAS this can create a serialization cycle, so the
complete dependency set, not only the write set, must share the leaf.

### Publish the fitting subset directly

A direct subset plus a logged or rejected remainder introduces another
multi-protocol atomic commit. It contradicts both member atomicity and the
single-CAS objective.

### Request a split after aggregate rejection

The split is irreversible without merge and may separate the very dependencies
that justified direct commit. Multi-key rejection therefore supplies no
pressure hint.

### Re-submit excluded blind members directly

Reusing their computed values is semantically possible, but repeated overlap
can starve indefinitely. The regular locked protocol remains the bounded
fallback for a blind member that loses its round reservation.

### Add an explicit key-count limit immediately

Such a limit can protect coordinator latency but would reject transactions that
fit naturally in one leaf. Benchmarks and operational evidence should choose a
limit if one is needed.
