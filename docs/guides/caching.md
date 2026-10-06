# Cached state and currentness evidence

GlassDB has two goals that pull against each other. Every transaction is
strongly consistent, and a single-value transaction with a warm cache should take
one backend operation. A cache that returns only a value cannot satisfy both,
because a cached value can be stale and the caller cannot know it. For this
reason, `CachedStore` keeps evidence of when each cached state was current.
A transaction body executes from cached state. Validation then proves that what
the body read was still current at or after the validation barrier.

This guide explains what that evidence proves, what the backend must guarantee
for the proof to hold, and the constraints that code must keep so that the
proof stays true. It does not describe the types. The
[storage evidence rules](storage-consistency.md) give the type rules, and
[GLOSSARY.md](../../GLOSSARY.md) defines the terms. The decisions are in
[ADR-036](../adr/036-decoded-object-cache-with-bounded-freshness.md),
[ADR-043](../adr/043-causally-coordinated-backend-operations.md), and
[ADR-045](../adr/045-optional-persistent-encoded-body-l2-cache.md).

## The backend must be linearizable per object

All cache evidence depends on one backend property. Single-object reads and
conditional mutations are linearizable, and a read after a definitive result
sees that result or a later state. A definitive result creates an ordering
edge. An in-doubt mutation or a failed read creates no edge.

Without linearizability, a read that starts after a mutation can return an old
revision or a false absence. The cache could not trust any evidence from such a
read. GlassDB does not support eventually consistent backends.

Revisions identify content, not history. Equal contents can reuse a revision,
so a revision can come back after an intervening change. An applied CAS proves
that the expected state was current when the CAS took effect. It does not prove
that the state stayed current during the whole interval after the read that
observed it.

## Evidence is a lower bound, not a lease

Each database instance has a strictly ordered local timeline. Immediately
before `CachedStore` invokes a backend operation, it allocates the invocation
point `T` from that timeline. A definitive result stamped with `T` means:

> The returned state was current at some backend linearization point no earlier
> than `T`.

The point is allocated before the call, not at the response, because another
database instance can change the object after the operation takes effect and
before the response arrives. A response-time stamp would claim more than the
backend proved.

The evidence also gives no promise about the future. A state with evidence at
`T` can already be obsolete when the call returns. A sequence point is neither
a wall-clock time nor a database snapshot.

Except for one narrow L2 handoff, sequence points have meaning only in the
database instance that allocated them, and instances never exchange them. L2
persists points only so that a new open of the same database starts its
timeline after all recoverable cache evidence. Thus an old L2 body can satisfy
`ANY`, but a requirement created in the new database instance forces a backend
check before that body can satisfy it.

## Uncertainty is the absence of an entry

A lookup for a physical path finds one of three things:

| State | Meaning |
| --- | --- |
| Present | A decoded value, its revision, and evidence of when that state was current. |
| Absent | A definitive result showed that the object did not exist. |
| No entry | The cache has no usable knowledge. The path is uncached or in doubt. |

There is intentionally no "unknown" entry. If uncertainty were a value, an
ordinary lookup could return it by mistake. Instead, a rejected mutation, an
in-doubt mutation, or a changed object that does not decode can remove the
entry. It never puts a guessed state in its place.

An observation and its discoverable cache entry are separate things with
separate lifetimes. Eviction or invalidation changes what a new read can find.
It does not change the historical fact that a retained observation was current
after its currentness watermark. A transaction can therefore hold an
observation after the LRU drops its entry, and still validate it.

An observation and its cache entry normally share one evidence cell. When a
revision-conditional read proves that the revision is unchanged, the cell
advances, and every holder of that observation gets the newer evidence.
Evidence advances only by taking the maximum point. It never moves back.

## Path lanes order local publication

Responses from overlapping backend calls arrive in an arbitrary order. Consider
a read that finds absence before a concurrent create takes effect, but whose
response arrives after the create succeeds. If the cache published responses in
arrival order, the delayed absence would overwrite the created value.

The path lane removes that schedule. Within one database instance, only one
backend read or conditional mutation of a path runs at a time. The operation
allocates its invocation point inside the lane, and it updates the cache and its
observations before it leaves the lane and before its caller sees the result.
Either the read runs first and the create publishes last, or the create
finishes and is reconciled before the read starts. In the second case,
linearizability makes the read see the created state or a later state.
Different paths do not wait for each other.

A waiter checks the cache again after it enters the lane. The earlier lane
owner often already established enough evidence, so the waiter does not need to
call the backend.

An `ANY` cache hit does not enter the lane. It can return an older state while a
mutation of the same path runs. That is part of the `ANY` contract. Code that
needs a newer state uses `after(barrier)`, or retains an observation and
validates it.

Reconciliation never guesses:

- An applied mutation publishes the exact installed state before it returns.
- A rejected CAS removes only the knowledge that matches its expected state. It
  cannot erase a different state that the instance already knows.
- An in-doubt mutation removes all knowledge for the path, because the instance
  cannot know whether the mutation took effect.
- Cancellation, panic, or task failure after dispatch is handled like an
  in-doubt result before the lane is released.
- Cancellation before dispatch has no effect on the cache. A
  cancelled read needs no invalidation, because a read cannot change backend
  state.

A cancelled mutation can still take effect in the backend. The instance never
publishes its delayed result, and from then on treats it like a write from
another database instance.

Other database instances and external writers do not share the path lane or
the timeline. They stay safe because currentness checks and mutations use
revisions that the backend compares. The path lane orders local publication.
The backend stays the global authority.

## Freshness requirements state what a read must prove

A read states the minimum evidence it needs as a freshness requirement:

| Requirement | Accepted cache state |
| --- | --- |
| `ANY` | Any discoverable present or absent state. |
| `within(timeline, age)` | A state whose evidence reaches an approximate age cutoff. |
| `after(barrier)` | A state whose evidence reaches the currentness barrier. |

`ANY` accepts stale data on purpose. A transaction body or an idempotent CAS
loop can start from a stale state, because the worst result is an invalidated
read or a rejected CAS. `ANY` cannot return a state that the cache knows
is obsolete or in doubt, because that state is no longer discoverable.

`within` converts a duration to a sequence-point cutoff. The conversion is
approximate, and it is acceptable only as a cache policy for stale reads.
Transaction validation, CAS receipts, and recovery use exact barriers and never
do time arithmetic.

When the cached evidence is too old for `after` or `within`, `CachedStore`
checks the backend. For a present state, it does a revision-conditional read,
which transfers no body if the revision is unchanged. An absent state has no
revision to send, so its check is an ordinary read.

Concurrent reads share one in-flight backend check only when its invocation
point satisfies every waiter. A waiter with a stricter requirement waits for
that check, and then checks the cache again.

### Decisions from `ANY` reads

An `ANY` read gives a usable state. It does not give permission to make every
decision from that state. A decision that accepts stale data needs one of the
proofs below, and the code must state the proof next to the decision. This is
most important for an early return, a skipped mutation, and an absent-object
result.

| Proof | Constraint |
| --- | --- |
| Later validation | Retain the read or predicate dependency, and validate it before the body outcome is accepted. |
| Conditional mutation | The change checks the observed revision. This does not justify a branch that skips the CAS. |
| Stable fact | Reuse only a fact that cannot change, for example final committed contents, or an exact publication marker of a transaction identity that is never reused. Historical contents do not prove that the object is present now. |
| Shared local knowledge | Use the same cache that completed the acquisition or fencing, or that returned the holder before. A result with no holder must also exclude a later acquisition by that identity. This proof does not transfer to another database instance. |
| Publication and lifecycle | Show why the caller cannot have cached absence before a later live publication. Nodes exist before their links are published, tree roots exist before collection bindings, and retired identities are not reused. |

Shared local knowledge includes L2. The path lane stops an older reply from
replacing established knowledge. Invalidation or replacement of older L2
entries stops an L1 miss from restoring them. If neither cache has a usable
entry, the backend read starts after the prerequisite work.

Fresh identities alone do not prove permanent absence. A retried create can
restore a deleted path. Structural recovery fences publication before it treats
reserved nodes as unreachable. A late create can then leave an orphan, which
ADR-043 permits. That exception does not justify skipping a check for a live
reference, or for completion of participant departure.

Index-node routing from cache uses right links to correct a stale split route,
and low keys to detect a stale copy of a merge target. The terminal leaf must
meet the caller's requirement. If a tree root or child is absent, there is no
terminal leaf to check, and a negative route still needs the publication and
lifecycle proof.

If no proof applies, use a requirement from the existing barrier of the policy.
Capture a new barrier only when the decision needs a later ordering point.

## Currentness barriers

A currentness barrier separates work that finished from work that has not
started. For this reason, a policy captures it after its prerequisite work and
before the operations that it uses as dependent evidence. A barrier captured
too early accepts evidence that predates the prerequisite. Only the policy
knows its ordering, so the policy owns the capture point:

| Policy | Capture point |
| --- | --- |
| Transaction validation | After the body, before the key and predicate lock CASes used as validation evidence. |
| GC eligibility | Before the read of candidate status. |
| GC checks | After the eligibility checks finish. The earlier status barrier cannot replace this one. |
| Structural recovery | After all intents in a discovery batch are observed, before their sources and reachability are checked. Later discoveries need a new barrier. |
| Parent reconciliation | After the split or merge is observed, before routing and reading its child chain. The barrier stays in use for the whole reconciliation. |
| Merge check | After the source is gated, before the read of the target for the final merge decision. |
| Stale merge-target copy | After observing the route that reached a copy whose low key is above the key, before that node is read again. |
| Missing-object retries | After the missing object is observed, before dependent state is checked again. |

Transaction validation uses one barrier for every point read, scan, collection
directory, and transaction-status dependency. Each validation attempt captures
a new barrier. A committed or aborted status never changes, so its dependency
can still use the existing proof of that status. That proof does not show that
the object is present after the barrier.

To choose between a `CurrentnessBarrier` and a `Requirement` parameter, ask
whether `ANY` would make the result false. If it would, take a barrier. If any
caller can correctly pass `ANY`, take a requirement.

The two types state different things. A requirement states which evidence a
read accepts. It can be `ANY`, which has no bound, or `within`, whose bound is
approximate. A barrier states that an ordering point exists after some
prerequisite work finished. For this reason a requirement can never become a
barrier.

A method that takes a requirement is mechanism. It returns a state, or completes
an operation, that meets the requirement it gets, and the caller owns the proof
that the requirement is enough. Lock release is an example. An owner that shares
the cache of the acquisition can release with `ANY`, so the release interface
takes a requirement. A method that takes a barrier is part of the policy that
owns the proof. Its result is a claim that is true only relative to the barrier,
for example "this transaction is valid" or "this record is unreachable". Only a
barrier can check evidence that is not a cache entry against that point, such as
the invocation point of a CAS receipt.

A policy keeps the barrier type while it passes the barrier between its own
steps, and converts it with `Requirement::after` once, at the call to a shared
interface. The types do not stop a policy from passing `ANY` to a shared
interface. That is correct when the policy has a different proof for that step,
so each conversion is still a decision that review must check.

A barrier does not make older evidence current. A read that starts before the
barrier and finishes after it still carries its older invocation point, because
completion time cannot upgrade a read. A copy of a barrier keeps the original
bound, so it cannot start a new validation attempt. `within` is not a
substitute for a barrier, because it does not prove that the prerequisite work
finished.

An observation must not become the requirement for reads of other objects. Its
watermark describes one state of one object. A structural gate acquisition
returns its exact observation instead, and the next mutation checks that
revision. Parent reconciliation also passes its captured barrier to child
reads. The parent's watermark does not replace it.

Owner-driven key write-back uses `ANY`, including on rerouted leaves. It is
safe only in the database instance that acquired the locks. The lock CAS
installs its state and invalidates older L2 entries before it completes, and
the path lane stops older backend reads from replacing that state. Thus a later
cached state without the holder means that the holder is already resolved, and
a stale state that contains the holder loses its write-back CAS. Write-back is
not a recovery interface for the locks of another instance. Its result also
does not authorize deletion of the transaction record. GC checks references
with its own barrier.

## CAS receipts

`CachedStore` reports a CAS as applied only after a definitive result shows that
it took effect.
The receipt proves that one conditional transition took effect. It keeps the
expected revision, the installed observation, and the original invocation
point. For a conditional create, there is no expected revision, because the
precondition was absence.

The receipt does not prove that the expected state was current during the whole
interval after the read that observed it, because a revision can come back. A
check of a receipt against a precondition compares its path and exact state,
and compares its original invocation point with the barrier.

A later read can advance the watermark of the installed observation. That read
confirms the installed state only. It must not advance the receipt's invocation
point, because the read proves nothing about the precondition. Otherwise an
old precondition could qualify for a newer validation barrier.

The receipt also does not promise that the installed state is still current
when the caller gets it. A peer can replace the object before the CAS reply
arrives. The receipt keeps the original installed state, and does not reload
the peer's state. The type rules that keep receipts exact are in
[E4](storage-consistency.md#e4-a-cas-receipt-proves-one-applied-cas).

## Coordinator mutation evidence

A leaf coordinator round can combine the leaf changes of many round members
into one mutation plan, which one CAS publishes. A storage receipt shows that
the CAS was applied. It does not show which members had changes in it. The coordinator owns that second proof:

- A staged member can get the receipt only from the CAS that carried its
  changes.
- A skipped member keeps its loaded observation, even when the CAS of another
  member was applied.
- A plan with no staged changes keeps its original observation, and must not
  complete a staged member.

Typed node storage passes the storage receipt through unchanged. The
coordinator keeps it after persistence and never builds a new one.

A skipped member's loaded observation does not always prove its outcome. A
member policy can skip because an earlier member already staged the change. For
example, a release can skip because an earlier acquire in the same plan already
removed the holder from the staged leaf, as the holder has a final status. That result must wait until the CAS of the
plan is applied. A rejected or in-doubt CAS forces a reload and a new plan
before completion.

Existing freshness requirements still govern decisions made from reads.
Exact-state shortcuts also need the validation barrier. Installed evidence
checks the original invocation point of the receipt. Observed evidence checks
the currentness watermark of the loaded state. Neither an applied leaf CAS nor
a plan with no staged changes creates a new currentness barrier after the
operation.

Each attempt takes its members and their combined requirement from one merged
request after the leaf load. Members that join during the load can make the
requirement stricter, and bounds that a policy requested stay in force across
retries. A member policy's requirement applies to dependent object reads. It
does not guarantee that the loaded or staged leaf already meets the bound.

The first leaf load uses `ANY` as a speculative CAS precondition, including for
lock acquisition. This does not weaken the submitted requirement. Retries load
against the retained combined bound. A missing initial leaf is checked again
against the submitted bound before the round returns absence. A cached index
can still cause rerouting, because an index node never becomes a leaf again.

A plan with changes uses its CAS to confirm the loaded state after the combined
bound, without a separate leaf read. A plan with no changes has no CAS, so it
checks the exact loaded state. Enough evidence costs no I/O. An unchanged
backend state advances the original observation. A changed state forces a
reload and a new plan. Never return an old decision with evidence for a
different state. A leaf CAS cannot repair dependent reads that used a weaker
requirement.

Member policies keep only facts that stay true when a plan is discarded. This
also applies to reconciliation of an earlier in-doubt CAS. An exact historical
marker of the member's own change can prove that a mutation took effect. A
staged proposal cannot.

Acquisition still uses the validation barrier to find current scan coverage
and to resolve transaction dependencies. Point and scan validation after
locking keep that barrier. An older seed does not justify the omission of a
leaf from a scan, or an exact-state shortcut. Validation uses the actual
observation or CAS receipt, and falls back to logical validation when that
evidence is not sufficient.

## Why speculative cache use is correct

The argument has four parts.

1. **Invocation points are sound lower bounds.** A linearizable operation takes
   effect at one point between invocation and response. `T` is allocated before
   invocation, so a definitive read found a state that was current no earlier
   than `T`. An unchanged revision-conditional read proves that the expected
   revision was current at such a point. An applied conditional mutation proves
   its predicate at that point and installs its state there.
2. **The path lane aligns local and backend order.** No two local backend calls
   with definitive results overlap on a path, so the publication order follows the real-time order of
   the backend, not the arrival order of responses.
3. **Reconciliation never guesses.** The cache shows a state that a definitive
   result supports, or no state at all.
4. **Transactions validate speculative reads.** The body can replay, and it
   keeps the physical observations it used. After the body, validation captures
   one barrier and checks those dependencies against it. If a state changed,
   the resolver compares the logical writer or membership evidence, and the
   transaction replays the body if its result is no longer valid.

Point validation groups work by leaf path. It first checks the exact retained
leaf observations. Only if one changed does it resolve the full set of logical
point reads against the current terminal leaves. Validation after the point
locks are acquired always uses the logical path, and ignores the locks that the
validating transaction identity holds.

An applied CAS invoked after the validation barrier both validates its expected
observation and installs the mutation, so it needs no separate read. A
read-only transaction can be ordered before a write that happens after its
validation. Validation detects a write that invalidates its result. This is why
a strongly consistent read can execute from cache without treating an arbitrary
cache hit as current.

## Background work supplies its own proof

Background policies read with `ANY` much more often than the transaction path,
so each policy owns the argument for why its negative or no-op result is safe.
Three rules recur:

- **Routing is not evidence.** Index-node descent can use `ANY` and rely on
  right links to correct stale placement, but the terminal leaf must meet the
  caller's requirement. A tree root cached as a leaf still needs that check, because
  a peer can have changed it into an index node.
- **A missing route needs publication and lifecycle proof.** Children exist
  before their links are published, tree roots exist before collection bindings,
  identities are never reused, and published nodes stay until the collection is
  reclaimed. Recovery can inspect a reserved node before its creation, but only
  after it fences the exact source revision of the split, so that the fenced
  worker can no longer publish the node. Reclaimed collections can stay absent.
- **Completion needs a removal CAS or a bounded no-op.** An applied conditional
  removal proves completion by itself. A present state without the holder
  proves completion only when its evidence meets the caller's requirement.
  Otherwise the caller reads again under that requirement. Key locks,
  membership locks, directory holders, topology participants, and the
  collection record each keep their own completion evidence. A refresh of one
  object gives no evidence about another.

Proof does not transfer across identities or across database instances. An
aggregate progress flag never justifies skipping an individual object. After
the removal of a committed identity's claim is applied, cache eviction cannot make that
completion false, because the identity cannot acquire the holder again.

Committed directory write-back keeps its two phases in order. GC first tries
write-back with `ANY`, so directory progress continues while a live entry keeps
the record. It completes the remaining directories under its post-eligibility
bound only after the live-entry early return. If the bounded phase moved
earlier, every repeated check of a record that still stores live values would
read its directories.

Discovery listings follow the same rule. A present cached body is an acceptable
recovery candidate under `ANY`. A listed body that the cache shows as absent
must be checked again under the supplied requirement, so a read error is never
reported as an empty listing. Structural intent identities are never reused,
and their only phase change is from preparing to ready. Ready contents do not
change until deletion. Deletion therefore checks the exact observed revision,
and a rejected CAS invalidates the obsolete cached state. A stale preparing
observation can delay peer help, but it can never authorize deletion of a ready
intent.

Structural recovery captures one barrier after a complete discovery batch, and
keeps it with that batch. Do not replace it with the watermark of an intent or
with the discovery bound. Do not add observations under the barrier of an
earlier batch. Participant settlement keeps its final listing bound. A final
transaction status alone does not close the intent namespace, because
recursive recovery can create more intents.

## The persistent cache adds no authority

L2 stores exact encoded bodies of present objects, their revisions, and their
existing currentness points. It is best-effort. An L2 that is unavailable,
corrupt, or overloaded makes GlassDB slower, but it does not cause a new
database failure. L1 still owns decoded values and the live shared evidence.
L2 adds no coordination and no freshness guarantee of its own.

## Limits of the guarantee

- `after(barrier)` means that the exact state was current at some point at or
  after the barrier. It does not mean that the state is still current at
  return.
- The generic cache infers no facts about object types. The transaction-record
  store can keep a committed or aborted record indefinitely only because that
  type guarantees that these statuses do not change.
- Listing is not cached. Each page is one backend request with its own
  invocation point, but a listing of many pages is not a snapshot.

The cache records only what a linearizable backend has established, lets
callers reuse that evidence, and forces a backend check when a caller needs
more than the retained evidence proves.
