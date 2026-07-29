# ADR-051: Inline latest values in leaf entries

## Status

Accepted — implemented (`glassdb-storage::CurrentState` + `InlinePolicy`, the
resolver's inline read short-circuit, and the transaction engine's direct-commit
resolver).

This supersedes:

- [ADR-017](017-shard-object.md)'s `current_writer` / `deleted` current-value
  representation;
- [ADR-019](019-unified-transaction-object.md)'s decision that transaction
  objects are the only durable home for values and that write-back never copies
  values; and
- [ADR-027](027-single-rw-parallel-lock-publish.md)'s two-write single
  read-write path when the new value is eligible for inlining.

[ADR-053](053-replay-definitive-logless-rmw-losses.md) refines the fallback
policy: ADR-027 is removed entirely, so an ineligible attempt either replays its
body or takes the regular locked protocol. The direct-commit decisions here,
including its in-doubt contract, are unchanged.

[ADR-054](054-reserve-inline-publication-for-logless-commits.md) supersedes this
ADR's ordinary write-back and help-forward inlining while retaining
authoritative inline values and logless direct commit.

This also refines
[ADR-022](022-garbage-collection-mark-sweep.md)'s transaction-object liveness
model and extends [ADR-028](028-shard-mutation-coordinator.md)'s shard-mutation
coordinator. Existing transaction objects retain their current reachability
rules, but an inline writer ID need not have a transaction object.

This changes the unreleased v2 layout in place. Development databases use the
new format directly; there is no migration or compatibility fallback.

## Context

A latest-value read currently resolves the key's leaf entry and then loads the
transaction object named by `current_writer`. This second object lookup is
necessary even for a tiny value and can transfer unrelated values written by
the same transaction.

The single read-write fast path likewise creates a committed transaction object
and installs a leaf lock in parallel, then asynchronously converts the lock to a
writer pointer. A small overwrite already fits in the leaf CAS that validates
its predecessor. Keeping its value there would make that CAS a self-contained
commit and remove both the transaction-object write and write-back.

Inlining every value without bounds would have the opposite effect on writes:
all coordination mutations rewrite the complete leaf, so large inline bodies
increase CAS bandwidth, cache pressure, and split frequency. The representation
must therefore make inline values authoritative while treating their admission
as a bounded optimization.

## Decision

### Make current value state self-describing

Replace the independent `current_writer` and `deleted` fields with one tagged
current state, separate from the entry's lock state:

```text
Absent
External  { writer }
Inline    { writer, value }
Tombstone { writer }
```

`writer` remains the value's optimistic-validation token. It identifies the
transaction that produced the version, but it is no longer universally a
pointer to a transaction object.

An inline value is authoritative latest-value evidence. Its transaction object
may exist because an ordinary committed transaction was written back, or may
never have existed because the logless fast path committed it. The entry records
no provenance bit. Empty inline values remain distinguishable from an absent
inline field, and invalid combinations are rejected as corrupt state.

Every mutation that changes the effective current writer must atomically install
the matching new state. It must never leave predecessor inline bytes attached to
a new writer. A helper that knows the exact committed bytes may inline them; one
that does not publishes `External`. A delayed helper may backfill bytes only
while the entry still names that writer.

Readers resolve locks before interpreting current state. They return `Inline`
directly without consulting transaction status, load the named transaction
object for `External`, and treat `Tombstone` as absent. A committed exclusive
holder ahead of the recorded current state still resolves through its
transaction object; predecessor inline bytes are not its value. Read validation
continues to compare writer IDs even when two versions contain equal bytes.

### Bound inline admission

Use two internal, benchmark-tuned limits:

- a maximum payload size for one inline value; and
- a maximum aggregate inline payload size per leaf.

The exact encoded node must also fit its existing hard object cap. The numeric
limits are implementation policy, not persisted database configuration.

The aggregate budget is admission-only. Existing inline values are never
demoted or evicted merely to make room, because an inline value may have no
external backing object. Values already present when a policy is lowered, and
values brought together by a future merge, are grandfathered. Overwrite and
split may naturally free budget.

There is no background promotion pass. Inline admission is considered when:

- ordinary write-back publishes a committed `Put`;
- help-forwarding already has the exact committed bytes; or
- the logless single read-write path publishes its value.

Ordinary write-back considers each value independently. If bytes are
unavailable, either budget is exceeded, or the inline form cannot fit the node,
it publishes `External` and releases the lock. Inlining is never allowed to
delay commit convergence or lock release. Tombstones carry no value payload.

For ordinary transactions, an inline writer remains a normal ADR-022 reference:
an existing transaction object stays live while any current entry or lock names
its ID. Inlining does not make that object collectable earlier. This deliberately
duplicates some current small values between leaves and transaction logs.

### Commit eligible single read-write transactions in one leaf CAS

For the initial implementation, retain ADR-027's current static eligibility:

- exactly one `Put` of an already-existing key;
- either a blind overwrite or point reads only of that same found key; and
- no scans or collection-management changes.

When the new value satisfies both inline budgets, submit a direct commit through
the shard coordinator. One conditional leaf CAS re-resolves the effective
predecessor, validates an observed read when present, and publishes
`Inline { writer: txid, value }`. It installs no lock, creates no transaction
object, and needs no write-back. The CAS is the commit point.

An already-committed holder awaiting write-back may be help-forwarded and
replaced in the same CAS. A live pending or unknown conflicting entry holder, a
live structural gate, or a collection-deletion fence makes the direct path
ineligible before it writes. Leaf membership locks do not conflict with an
overwrite because it cannot change the key set. Ineligible attempts fall back
to the existing logged protocol; values that miss only the inline size or leaf
budget retain ADR-027's logged single read-write optimization.

All entry mutations continue to flow through ADR-028's coordinator. At most one
direct commit for a given key may stage in one coordinator CAS round, so another
batched blind writer cannot erase the first commit's recovery evidence within
that same uncertain write. Direct commits for disjoint keys may still share a
round.

The direct attempt publishes no pre-commit identity and cannot participate in
wound-wait. Another database instance cannot wound or abort it. Cancellation
before dispatch leaves no state; cancellation after dispatch is crash-equivalent
and the CAS may have committed. Cancellation must not create an aborted
transaction object for the invisible logless ID. Once an attempt falls back and
registers with the logged lock protocol, the existing abort and lease behavior
applies.

### Preserve honest in-doubt outcomes

After an unavailable direct CAS:

- observing the exact inline state with this writer ID proves commit;
- observing the unchanged predicate permits an idempotent retry; and
- observing that the entry moved after the uncertain write is irreducibly
  in-doubt.

The last case surfaces `InDoubt` rather than re-running the transaction and
risking double application. A concurrent split may require rerouting recovery;
if the commit marker can no longer be proven, the same conservative result
applies. This is ADR-009's existing contract for a logless conditional commit,
not a new availability guarantee.

A logless writer ID may later appear as a predecessor or GC hint even though no
transaction object exists. That absence is expected. Log listing remains the
completeness mechanism for real transaction objects, and the usual reverse
reference check retains or reclaims them unchanged.

### Leave broader atomic leaf commits and snapshot history to follow-ups

One CAS could eventually commit multiple reads and writes whose complete
dependency set remains in one leaf. Creates and deletes could also participate
if the CAS handles membership coordination. These extensions require set-wide
validation, capacity admission, split-aware recovery, and membership rules, so
they are not part of the initial path.

Snapshot history is also outside this decision. ADR-039's future snapshot
protocol always emits immutable history and certification for every writer,
even while new snapshot admission is operationally disabled. It retains inline
current values as a latest-read optimization but supersedes this ADR's guarantee
that an eligible commit needs only one CAS and no external record. Preserving a
specialized one-CAS path while emitting mandatory history remains a research
goal, not a runtime latest-only database mode.

## Consequences

- An inline latest-value read needs only the leaf object and benefits directly
  from the decoded and persistent object caches.
- An eligible small single read-write transaction commits with one conditional
  leaf write, no transaction object, no lock publication, no write-back, and no
  orphan log.
- Ordinary committed values may become faster to read after best-effort
  write-back without changing their atomic commit or GC lifecycle.
- Leaf bodies, leaf CASes, cache entries, and split copies become larger.
  Per-value and aggregate budgets bound this amplification but reduce the
  fraction of values that can be inlined.
- Logged inline values consume duplicate durable and cached space for as long as
  their transaction objects remain referenced.
- Inline admission is intentionally non-uniform and history-dependent. A small
  value may remain external because its leaf has no budget, and no background
  task later promotes it.
- Transaction IDs no longer imply transaction-object existence. Code that needs
  status or a logged value must be guided by the tagged current state or by a
  lock, not by the writer ID alone.
- The direct path retains ADR-009's user-visible in-doubt outcome and makes
  post-dispatch cancellation potentially committed, as any abandoned logless
  conditional write must be.
- Thresholds and budgets require benchmarks that measure saved read operations
  against leaf size, CAS latency, split rate, and cache pressure.

## Alternatives considered

### Keep all values only in transaction objects

This preserves small coordination objects and one value representation, but it
retains an avoidable object lookup on small reads and an avoidable object write
on the single read-write path.

### Treat inline bytes as a disposable cache

This would require a transaction object for every value and would preserve the
two-write fast path. It gives up the main latency improvement while adding
coherency states between the cache copy and its authority.

### Record whether each inline value has a backing log

A provenance bit would permit safe demotion of logged inline values and make a
missing backing log diagnosable. It adds another invariant and is unnecessary
with admission-only budgeting, so it is deferred.

### Rely only on the existing leaf hard cap

This preserves maximum inline coverage but lets small values inflate every leaf
rewrite up to the coordination object's full size. Separate per-value and
aggregate budgets make that trade-off explicit and tunable.

### Generalize immediately to every single-leaf transaction

The leaf is an attractive atomic commit unit, but set-wide dependencies,
membership changes, capacity, splits, and multi-entry in-doubt proof make this a
separate protocol decision. The representation chosen here permits that
extension without requiring it.
