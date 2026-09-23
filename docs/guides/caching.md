# CachedStore, requirements, and observations

GlassDB's cache is not just a map from object paths to values. `CachedStore` is
the boundary that combines decoded-object reuse, currentness evidence, and
same-path coordination for backend point operations. The transaction layer can
therefore execute optimistically from cached data and later prove that the
exact states it used were sufficiently current.

This guide introduces that model and explains why its evidence is sound when
the backend provides linearizable single-object reads and conditional
mutations. It states the rules that the code cannot state for itself: what a
piece of evidence proves, and what must never be derived from it. The complete
decisions are recorded in
[ADR-036](../adr/036-decoded-object-cache-with-bounded-freshness.md),
[ADR-043](../adr/043-causally-coordinated-backend-operations.md), and
[ADR-045](../adr/045-optional-persistent-encoded-body-l2-cache.md).

## Where CachedStore sits

One `CachedStore` belongs to each open `Database`. All typed physical-object
stores use it:

```text
transaction code
    -> reader / resolver / monitor
    -> typed object stores and codecs
    -> CachedStore
         -> decoded, byte-bounded L1
         -> optional persistent encoded-body L2
         -> per-path operation coordinator
    -> Backend
```

The L1 is a byte-weighted LRU keyed by physical object path. Codecs supplied by
the typed stores encode and decode values, validate paths, and report decoded
sizes. A path has exactly one decoded type; using the same path through another
codec is an internal error. Cached values are immutable and shared, so a caller
clones a value before modifying and submitting it.

The cache holds physical objects such as collection records, tree nodes,
transaction records, and structural intents. It does not maintain a separate
materialized key-value cache. Higher layers derive a logical key value from its
cached leaf and, when necessary, its writer's cached transaction record.

The optional L2 stores exact encoded present bodies, opaque revisions, and
their existing currentness points. It is best-effort: an unavailable, corrupt,
or overloaded L2 is a performance failure, not a new database failure mode. L1
remains the owner of decoded values and live shared evidence.

## Cached knowledge

For a physical path, the discoverable cache state is one of:

| State | Meaning |
| --- | --- |
| Present | A decoded value, its opaque backend revision, and evidence about when that state was current. |
| Absent | Definitive evidence that the object did not exist. |
| No entry | No usable knowledge; the path is uncached or in doubt. |

Absence is a real negative cache entry. Uncertainty is deliberately not an
entry variant: there is nothing an ordinary lookup can accidentally return. A
rejected mutation, an in-doubt mutation, or an undecodable changed object can
remove discoverable knowledge without inventing a replacement.

A revision wraps the backend's opaque content revision. Higher layers can
retain and compare it or pass it back to a conditional operation, but cannot
interpret or manufacture it. Revisions identify semantic content state rather
than an observable history of rewrites; equivalent contents may therefore
reuse a revision.

## Sequence points and currentness

Every open database owns a strictly ordered local timeline. Immediately before
dispatching a coordinated backend operation, `CachedStore` allocates a sequence
point from that timeline.

If an operation was invoked at `T`, a definitive result stamped with `T` means:

> The returned state was current at some backend linearization point no earlier
> than `T`.

This is a lower-bound proof, not a lease. It does not promise that the state is
still current when the call returns, and `T` is neither a wall-clock timestamp
nor an exact database snapshot. Another database instance may change the object
after the operation linearizes but before its response arrives, which is why
response time would be an unsound, overly strong watermark.

Sequence points are normally meaningful only within one open database. They
are not exchanged between database instances or independent database openings.
The persistent cache is the narrow exception: it persists points only so a new
opening of the same database identity can start its timeline strictly after all
recoverable cache evidence. Consequently, an old L2 body may satisfy `ANY`, but
a requirement created in the new session forces validation before that body can
satisfy it.

## Requirements

A read states the minimum evidence it needs as a freshness requirement:

| Requirement | Accepted cache state |
| --- | --- |
| `ANY` | Any discoverable present or absent state, regardless of its watermark. |
| `within(timeline, age)` | A discoverable state whose evidence reaches the approximate age cutoff. |
| `after(barrier)` | A discoverable state whose evidence reaches the opaque currentness barrier. |

`ANY` deliberately permits stale data. It is useful for optimistic transaction
execution and idempotent CAS loops, where a stale starting point can only fail
validation or lose its precondition. A known-obsolete or in-doubt state is no
longer discoverable, so even `ANY` cannot return it.

`after(barrier)` and `within(...)` first try the cache. If the entry's evidence is
too old, `CachedStore` checks the backend. A present state uses a
revision-conditional read: an unchanged response reuses the decoded body and
advances its evidence, a changed response transfers and decodes the new body,
and a definitive absence installs confirmed absence. An absent state has no
conditional revision, so validating it requires an ordinary read.

Concurrent reads may share one in-flight backend check only when its invocation
point satisfies every waiter's requirement. A stricter waiter queues and
rechecks the cache after the earlier operation finishes.

The duration-based requirement derives a cutoff from an elapsed duration for
stale reads. That duration-to-sequence conversion is intentionally an
approximate cache policy. Transaction validation, CAS receipts, and
recovery use exact sequence barriers without doing time arithmetic.

### Decisions from `ANY` reads

An `ANY` read supplies a usable state, not permission to make every decision
from it. State the proof beside a decision that accepts stale data, especially
an early return, skipped mutation, or absent-object result:

| Proof | Required constraint |
| --- | --- |
| Later validation | Retain the read or predicate dependency and validate it before accepting the body outcome. |
| Conditional mutation | Check the observed revision when applying the change. This does not justify a branch that skips the CAS. |
| Stable fact | Reuse only the fact that cannot change: for example, final committed contents or an exact publication marker for a transaction identity that is not reused. Historical contents do not prove current object presence. |
| Shared local knowledge | Use the cache that completed acquisition or fencing, or previously returned that holder. A no-holder result also needs to exclude later acquisition by that identity. This proof does not transfer to another database instance. |
| Publication and lifecycle | Establish why the caller cannot have cached absence before a later live publication. Nodes precede their published links, roots precede collection bindings, and retired identities are not reused. |

Shared local knowledge includes L2. Per-path coordination prevents older replies
from replacing established knowledge, and invalidation or replacement of older
persistent entries prevents an L1 miss from restoring them. Without a usable
entry in either cache, the backend read starts after the prerequisite work.

Fresh identities alone do not prove permanent absence. A retried create can
restore a deleted path. Structural recovery fences publication before treating
reserved nodes as unreachable; a late create can then leave an orphan, as
permitted by ADR-043. Do not use that exception to skip checks for a live
reference or for completion of participant departure.

Cached index-node routing uses right links to correct stale split routes. The
terminal leaf must meet the caller's requirement. If a root or child is absent,
there is no terminal leaf to check: the caller still needs the publication and
lifecycle proof for a negative route.

If none of these proofs applies, use a requirement from the policy's existing
barrier. Capture a new barrier only when the decision needs a later ordering
point.

## Currentness barriers

A currentness barrier is captured after prerequisite work completes and before
operations used as dependent evidence. The capture point belongs to the policy
that knows this ordering:

| Policy | Required capture point |
| --- | --- |
| Transaction validation | After the body, before the key and predicate lock CASes used as validation evidence. |
| GC eligibility | Before reading candidate status. |
| GC checks | After eligibility checks finish; the earlier status barrier cannot replace this one. |
| Structural recovery | After observing all intents in a discovery batch, before checking their sources and reachability. Later discoveries need a new barrier. |
| Separator publication | After observing the split, before routing and reading its child chain. Carry this barrier through reconciliation. |
| Missing-object retries | After observing the missing object, before rechecking dependent state. |

Transaction validation uses one barrier for every point read, scan, collection
directory, and transaction-status dependency. Each new validation attempt
captures a new barrier. Immutable committed and aborted status may still use
existing terminal-state proof. That proof does not establish object presence
after the barrier.

Decision interfaces that need this ordering require a barrier; shared read
interfaces receive the explicit conversion to a requirement. Only the timeline
constructs a barrier, and its numeric bound stays private to the timeline
module. Copying a barrier retains its original bound; it cannot open a new
validation attempt. A barrier does not make older evidence current by
association. The approximate duration-based requirement is a cache policy, not a
substitute for a barrier.

Do not add conversions from sequence points, observations, receipts, or
requirements to a barrier. Do not add public sequence-point accessors, default
values, implicit conversions, or serialization to barriers or requirements. Do
not add raw-point or observation-based requirement constructors.

Retained evidence is checked with predicates, without I/O and without extracting
a sequence point. Insufficient evidence requires a storage check under the
barrier. If a read starts before the barrier and finishes after it, the reply
still carries the older invocation point: completion time cannot upgrade
that read.

A requirement states what must be proved; it is not currentness evidence. Do
not expose setters that advance an observation from a sequence point or
requirement. Batch checks belong in the cached-store module, where reuse of a
successful check can advance another observation only from confirmed evidence
of the same exact state. A changed-state result cannot advance the old state's
evidence.

An observation must not become a requirement for reading other objects. A
structural gate acquisition instead returns its exact observation, and the next
mutation checks that revision. Separator publication also carries its captured
barrier to child reads; the parent's watermark does not replace it.

Owner-driven key write-back uses `ANY`, including on rerouted leaves, but must
use the same database instance that acquired the locks. The lock CAS installs
its state and invalidates old persistent entries before completing, and path
coordination prevents older backend reads from replacing that state afterward.
A later cached state without the holder therefore means the holder has already
been resolved, and a stale state containing the holder loses its write-back CAS.
This is not a recovery interface for another instance's locks, and its result
does not authorize deletion of the transaction record: GC checks references with
its own barrier.

## Observations

Every successful read or mutation establishes an observation of one exact
state. Reads and deletes return that observation directly; CASes return a
receipt that retains it. An observation contains:

- the physical path;
- a shared decoded value, or absence;
- an opaque revision for a present value; and
- shared, monotonically advanceable currentness evidence.

An observation and the matching cache entry normally hold the same evidence
cell. If a later conditional read proves that revision unchanged, advancing the
cell benefits every holder. Evidence advances by taking the maximum point and
never regresses.

The observation's lifetime is separate from the discoverable cache entry. An
observation retained by a transaction remains inspectable after LRU eviction or
invalidation. Removing the entry changes what a new read may discover; it does
not erase the historical fact that the retained state was current after its
existing watermark.

A currentness check uses that distinction: sufficient retained evidence costs no
I/O, a discoverable entry with the same revision and stronger evidence can
advance the retained observation, and otherwise the revision seeds a conditional
backend read (or an ordinary read, for absence). The result either confirms the
same state with merged evidence, or reports the newly established state.

Successful CAS confirms that the expected revision matched at the conditional
transition. It does not prove that the expected state was current throughout the
interval since its read: a content revision can recur after an intervening
change. The CAS advances the expected observation's watermark to its invocation
point and returns a receipt that retains the installed state.

## CAS receipts

`CachedStore` reports a mutation as applied only after a definitive successful
CAS.

The receipt retains the installed observation, expected revision, and immutable
invocation point, without the replaced body. For a conditional create, no
expected revision means that the precondition was absence. Checking a receipt
against a precondition compares both the path and state, and the original
invocation point against the barrier. It does not prove continuous currentness
since the read.

A later read may advance the installed observation's shared watermark. That read
confirms the installed state only; it must not advance the receipt's invocation
point or let the old precondition qualify for a newer validation barrier.

These are correctness constraints on the storage interface:

| Transformation | Rule |
| --- | --- |
| Successful conditional create or CAS to a receipt | Allowed only inside the storage mutation implementation, for that mutation. |
| Read observation, plan with no staged changes, rejected CAS, or in-doubt result to a receipt | Forbidden. |
| Receipt to installed observation | Allowed only through an explicit accessor. The observation carries state evidence, without mutation or batch-participation proof. |
| Implicit receipt conversion through `Deref`, `AsRef`, or `From` | Forbidden. Evidence changes must be explicit at the call site. |
| Mapping a receipt's payload or changing its precondition, path, body, or revision | Forbidden. The receipt must describe the exact mutation. |
| Expected revision plus installed body to an observation | Forbidden. They identify different sides of the transition. |
| Receipt to a sequence point or completion barrier | Forbidden. Its installed observation's watermark was allocated before the mutation. |

A receipt proves that the conditional transition succeeded. It does not promise
that the installed state is still current when the caller receives it. For
example, a peer may replace the object before the CAS reply arrives. The receipt
must retain the original installed state instead of reloading the peer's state.

## Coordinator mutation evidence

The leaf coordinator distinguishes evidence of a successful CAS from an
observation retained by a plan with no staged changes. Typed node storage
preserves the storage receipt, and the coordinator retains it after successful
persistence; it does not reconstruct it.

The coordinator owns round-member participation:

- A staged member may receive the receipt only from the CAS that carried its
  changes.
- A skipped member retains the loaded observation, even when another member's
  CAS succeeded. A storage receipt alone does not establish that a particular
  member participated in the mutation.
- A plan with no staged changes retains its original observation and must not
  complete a staged member.

A skipped member's loaded observation is not always sufficient to prove its
outcome. A member policy can skip because an earlier member has already staged
the required change; for example, a release can skip after an earlier acquire
removed its holder with a final status in the staged leaf. Such a result must
wait for the plan's CAS to succeed. A rejected CAS or in-doubt result must cause
a reload and a new mutation plan before completion.

Existing freshness requirements still govern decisions made from reads.
Exact-state shortcuts also require the validation barrier: installed evidence
checks the receipt's original invocation point, while observed evidence checks
the loaded state's currentness watermark. Neither a successful leaf CAS nor a
plan with no staged changes establishes a new currentness barrier after the
operation.

Each attempt takes its members and their combined requirement from one merged
request after the leaf load. Members that join during that load can strengthen
the requirement, and policy-requested bounds remain in force across retries. A
member policy's requirement applies to dependent object reads; it does not
guarantee that the loaded or staged leaf already satisfies the bound.

The first leaf load uses `ANY` as a speculative CAS precondition, including for
lock acquisition. This does not weaken the submitted requirement. Retries load
against the retained combined bound, and a missing initial leaf is rechecked
against the submitted bound before returning absence. A cached index can still
cause rerouting, because an index cannot become a leaf again.

A dirty plan can use its CAS to confirm the loaded state after the combined
bound, without a preliminary leaf read. A plan with no changes instead checks
the exact loaded state: sufficient evidence costs no I/O, an unchanged backend
state advances the original observation, and a changed state requires a reload
and a new plan. Do not return an old decision with evidence for a different
state. A leaf CAS cannot repair dependent reads made with a weaker requirement.
Member policies may retain only facts that remain valid when a plan is
discarded; this also applies to reconciliation of an earlier in-doubt CAS. An
exact historical own marker can prove that a mutation landed; a staged proposal
cannot.

Acquisition still uses the validation barrier to find current scan coverage and
to resolve transaction dependencies. Point and scan validation after locking
retain that barrier. An older seed does not justify omitting a leaf from a scan
or granting an exact-state shortcut: validation must use the actual observation
or CAS receipt, and fall back to logical validation when it is insufficient.

## Per-path coordination

`CachedStore` serializes actual backend point calls for the same physical path
within one open database:

```text
check cache
-> acquire path lane
-> check cache again
-> allocate invocation point
-> invoke backend
-> reconcile cache and observations
-> release lane
-> make the operation future ready
```

The second cache check avoids a redundant call when the preceding lane owner
already established sufficient evidence. Reconciliation happens before lane
release and before the completed operation becomes observable to its caller.
Different paths retain full backend concurrency.

An `ANY` cache hit bypasses the lane and may return an older usable state while
a mutation is in progress. This is part of `ANY`'s contract. Code that needs a
currentness barrier uses `after(barrier)` or retains and validates an observation.

Mutation outcomes are reconciled conservatively while holding the lane:

- Success publishes the exact installed state before returning.
- A clean rejected CAS invalidates only matching expected knowledge;
  it cannot erase a different state already known locally.
- An unavailable result after dispatch makes the whole path in doubt, because
  the mutation may or may not have landed.
- Cancellation, panic, or task failure after mutation dispatch follows the
  same in-doubt transition before releasing the lane.
- Cancellation before dispatch has no cache effect.

Read cancellation needs no invalidation because a read cannot mutate backend
state. A cancelled mutation may still apply remotely, but it can never publish
a delayed local result and is subsequently treated like a write from another
database instance.

## Why the evidence is correct

The required backend contract is linearizable single-object reads and
conditional mutations, including read-after-definitive-completion. A definitive
response creates an ordering edge; an unavailable result does not. Eventually
consistent backends are not supported.

The correctness argument has four parts.

### 1. Invocation points are sound lower bounds

A linearizable operation has one effective point between invocation and
response. Because `T` is allocated immediately before invocation, the state
returned by a definitive read was current at some point no earlier than `T`.
An unchanged conditional read proves the expected revision current at such a
point. A successful conditional mutation proves its predicate current at that
point and installs its returned state there. Stamping each result with `T` is
therefore conservative.

An eventually consistent read could return an old revision or false absence
after `T`, so the same inference would be invalid without backend
linearizability.

### 2. The path lane aligns local and backend order

Consider a read that observes absence before a concurrent create linearizes but
whose response arrives after the create succeeds. Publishing responses in
arrival order would incorrectly let the delayed absence overwrite the created
value.

The path lane removes that schedule among local definitive operations. The read
and create cannot be actual overlapping backend calls for that path. Either the
read runs first and the create publishes last, or the create completes and is
reconciled before the read is invoked; linearizability then makes the read see
the created state or something later. Publication order follows the backend's
real-time edge rather than invocation or response order alone.

### 3. Reconciliation never guesses

Same-state validation merges evidence with a maximum, while a different state
replaces discoverable knowledge. A rejected CAS removes only the exact state it
proved obsolete. An in-doubt or cancelled mutation removes usable
knowledge instead of choosing between the old and proposed states. Thus the
cache either exposes a state supported by a definitive operation or exposes no
state at all.

External clients and independently opened databases do not share the path lane
or timeline. They remain safe because freshness checks and mutations use the
backend's linearizable conditional revisions. Local coordination is needed to
order local publication; the backend remains the global authority.

### 4. Transactions validate speculative cache use

`ANY` is not itself a strong read. Transaction execution may use it because the
body is replayable and retains the physical observations on which it depended.
After the body, validation captures one currentness barrier and checks those
dependencies against it. If a state changed, the higher-level resolver compares
its logical writer or membership evidence and the transaction replays the body
when its result was invalidated.

Point validation batches this work by physical leaf path: it checks exact
retained leaf observations first, and resolves the complete logical point-read
set against current terminal leaves only if one changed. Validation after point
locks are acquired always uses the logical path and ignores the validating
transaction's own holder.

A successful CAS invoked after the validation barrier can both validate its
expected observation and install the mutation, so it needs no separate read.
For read-only transactions, a concurrent write after validation can be ordered
after the transaction; a write that invalidates the observed result is detected
during validation. This is how the public strongly consistent read path can
execute cheaply from cache without treating an arbitrary cache hit as current.

## Background work supplies its own proof

Background policies read with `ANY` far more often than the transaction path,
so each one owns the argument for why its negative or no-op result is safe.
Three rules recur:

- **Routing is not evidence.** Index-node descent may use `ANY` and rely on
  right links to correct stale placement, but the terminal leaf must meet the
  caller's requirement. A root cached as a leaf still needs that check, because
  a peer may have turned it into an index.
- **A missing route needs publication and lifecycle proof.** Children exist
  before their links are published, roots exist before collection bindings,
  identities are never reused, and published nodes remain until collection
  reclamation. Recovery may inspect a reserved node before creation, but only
  after fencing the split's exact source revision, so the fenced worker can no
  longer publish it. Reclaimed collections can remain absent.
- **Completion needs a removal CAS or a bounded no-op.** An applied conditional
  removal proves completion by itself. A present state without the holder proves
  completion only when its evidence meets the caller's requirement; otherwise the
  caller rereads under that requirement. Each obligation — key locks,
  membership locks, directory holders, topology participants, and the collection
  record — keeps its own completion evidence. Refreshing one object supplies no
  evidence about another.

Proof does not transfer across identities or across database instances, and an
aggregate progress flag never justifies skipping an individual object. Once a
committed identity's removal has applied, cache eviction cannot invalidate that
completion, because the identity cannot acquire the holder again.

Committed directory write-back keeps its two phases in order. GC first attempts
write-back with `ANY`, so directory progress survives a live entry that keeps the
record, and completes the remaining directories under its post-eligibility bound
only after the live-entry early return. Moving the bounded phase earlier would
add directory reads on every repeated check of a record that still stores live
values.

Discovery listings follow the same shape: a present cached body is an acceptable
recovery candidate under `ANY`, while a listed body observed as absent must be
rechecked under the supplied requirement, so a read error can never be reported
as an empty listing. Structural intent identities are never reused and their
only phase change is from preparing to ready. Ready contents are then immutable
until deletion, so deletion is checked against the exact observed revision and a
rejected CAS invalidates the obsolete cached state. A stale preparing
observation can defer peer help but can never authorize deletion of a ready
intent.

Structural recovery captures one barrier after a complete discovery batch and
keeps it with that batch. Do not substitute an intent's own watermark or the
discovery bound for it, and do not add observations under an earlier batch's
barrier. Participant settlement keeps its final listing bound: final transaction
status alone does not close the intent namespace, because recursive recovery can
create more intents.

## Boundaries of the guarantee

- `ANY` may return stale but still usable knowledge.
- `after(barrier)` means the exact state was current at some point at or after
  the barrier; it does not promise that state remains current at return.
- Sequence points are local causal evidence, not portable timestamps.
- The generic cache does not infer object-specific facts. For example, the
  transaction-record store may cache transactions with a final status
  indefinitely only because that type separately guarantees immutability.
- Listing is an uncached pass-through. Each page is strongly observed as one
  backend request, but a multi-page listing is not a snapshot.
- The persistent L2 preserves old bodies and evidence but introduces no
  coordination authority or freshness guarantee of its own.

These boundaries are what make the cache useful without making it unsound: it
records exactly what a strongly consistent backend has established, lets that
evidence be reused, and forces validation whenever a caller asks for more than
the retained evidence proves.
