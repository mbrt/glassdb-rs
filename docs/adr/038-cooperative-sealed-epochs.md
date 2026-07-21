# ADR-038: Cooperative sealed epochs

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md).

On acceptance, this partially supersedes ADR-027's parallel first-intent path;
read-write transactions use the intention-first baseline unless a separately
proved epoch-aware optimization is adopted later.

## Context

GlassDB has no global commit sequence. Transaction identifiers encode
wound-wait priority, backend versions order only one object, and client clocks
cannot prove that a delayed writer will not publish into an apparently old cut.
A snapshot therefore needs an explicit closed frontier.

The frontier cannot depend on a permanent coordinator or idle heartbeat, and a
stalled snapshot frontier must not stop otherwise valid strict read-write
traffic.

## Decision

Assign every committed read-write transaction to one monotonically increasing,
database-wide epoch. A snapshot-visible epoch is sealed only after every valid
admission is durably committed and discoverable from all of its writes, or
durably aborted and fenced against delayed publication. Advance
`latest_sealed` contiguously.

After the user body has produced a candidate read and write set, use the
following correctness-first writer order:

1. acquire every point, absence/membership, range, and catalog lock, while
   proving the structural gate absent for every ordinary node rewrite;
2. revalidate dependencies and capture predecessors while holding those locks;
3. durably prepare an authoritative manifest, then make every named immutable
   payload or physical root durable, with an immutable initialization witness
   for each mutable root;
4. admit that manifest identity and digest to an open epoch;
5. publish a terminal certificate only after verifying the manifest; and
6. certify per-key history and release locks asynchronously.

Locks and intents precede admission, so a dependent transaction observes or
waits for the earlier transaction before it can enter a later epoch. This makes
every sealed epoch a downward-closed prefix of the strict-serializable order.
Every epoch-bearing transaction must lock and revalidate all point,
absence/membership, range, and catalog predicates on which its writes depend,
and every ordinary node rewrite must prove its structural gate absent; an
optimization that loses one of those edges cannot use this proof. Concretely,
ADR-033 and ADR-044 require any transaction containing both a scan and a write
to take membership-read locks through every scan's effective frontier and
revalidate. If a limited frontier moves outward, the transaction retains its
locks and extends the range to a fixpoint before preparation and epoch
admission.

Implement admission with sparse per-client lanes. One logical `Database` client
and its clones may physically batch independent admissions into one lane CAS
while retaining per-transaction outcomes; independently opened clients are
never combined, even in one process. Closing the epoch root freezes its lane
set, and closing every registered lane orders appends against closure. Only
active clients create lanes.

Snapshot execution is coordination-free, but uncached acquisition mutates the
admission generation by fencing it. A clone family may share certificates and
singleflight acquisition; independently opened clients obtain their own proof.

Sealing is an ownerless, idempotent CAS state machine. Writers and snapshot
begin may help any step after a client crash. Once an epoch's admissions are
frozen, the next epoch may accept writers while resolution and sealing continue.
The final-phase grace starts when a lane closes; after it expires, a sealer may
race a terminal abort even against a live writer. The winning commit-or-abort CAS
and its durable fence, rather than elapsed time, determine the outcome. A helper
without conservative evidence of the close age waits one full grace interval
from observing it. If the sealed frontier exceeds the freshness policy,
read-write traffic continues and snapshot acquisition falls back or fails
closed. Admission follows durable payload and root preparation, so the grace
does not cover that work. A healthy pending writer may lose the terminal race
and retry, but a writer whose commit certificate landed cannot be aborted.

Snapshot acquisition samples its qualified suspension-aware duration clock
before issuing an admission-generation fence, then resolves every pre-fence
admission before binding the resulting frontier or a newer one. The sample is
retained through in-doubt fence recovery, so elapsed time since that pre-request
point bounds the age of every omitted commit. A client may reuse that proof
within its freshness budget after deducting the policy's maximum duration-clock
uncertainty. Create no empty sealed epochs while idle; fencing an empty
generation may validate an old cut without publishing a new epoch.

Do not use elapsed time as a writer-outcome safety fence. Compact epoch/lane
commit and abort evidence remains authoritative after bulky transaction cleanup,
and every admission, lock/install, commit, resolver, wound, recovery, and GC path
validates it. This supersedes ADR-022's deletion rule for outcome evidence needed
to prevent an arbitrarily delayed request from entering a closed lane or
resurrecting an aborted transaction.

ADR-027's current fast path publishes its first intent in parallel with the
transaction object and is therefore ineligible for this baseline ordering. It
uses the ordinary intention-first path under this design, including while new
snapshot admission is operationally disabled. A future epoch-aware optimization
requires a separate decision and proof but is not an acceptance prerequisite.
Snapshot support cannot be opted out to avoid this protocol; acceptance
therefore depends on the living design's
[performance gate](../designs/snapshot-reads.md#performance-acceptance-gate), and
an unreasonable mandatory cost rejects the proposal.

## Consequences

- Snapshot cuts are explicit and recoverable using only single-object CAS.
- There is no coordinator lease, idle heartbeat, or reader pin.
- Admission adds metadata and can add latency to read-write commit; local lane
  batching amortizes operations but never couples transaction outcomes.
- Later writers remain available while an older epoch is unresolved, at the
  cost of snapshot fallback or `FreshSnapshotUnavailable`.
- The baseline does not preserve ADR-027's current single-read-write critical
  path. That is acceptable only if the identified workloads pass the latency and
  throughput gate; a specialized optimization remains optional.
