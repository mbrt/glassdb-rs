# ADR-022: Garbage collection by mark-sweep

## Status

Accepted — implemented.

The lock-reclamation *mechanism* (the "stale-lock and empty-entry pruning" CAS)
is refined by [ADR-029](029-gc-through-shard-coordinator.md): GC's release now
flows through the shard-mutation coordinator ([ADR-028](028-shard-mutation-coordinator.md))
and vestigial-entry pruning becomes a fold property. The GC *policy* below —
reverse mark-sweep, the safety horizon, abort-then-release-then-delete, and
tombstone retention — is unchanged.

Deferred to a follow-up: subcollection teardown (reclaiming orphaned child roots
and shards), and persisting a wound victim's locks into the aborted object.

## Context

In v1 garbage collection is **time-based**
([architecture.md](../architecture.md) "Garbage Collection"): a finalized
transaction log is queued and deleted after a fixed delay, with a bounded queue.
That is safe in v1 only because values live in *per-key value objects* — the log
is transient bookkeeping, so deleting it after the unlock/write-back loses
nothing live.

v2 inverts the relationship. [ADR-019](019-unified-transaction-object.md) makes
the committed transaction object (`{db}/_t/<txid>`) the **only** home for values:
a key's current version is whatever the object its shard entry's `current_writer`
points at ([ADR-017](017-shard-object.md)), and readers materialize values from
it (and help-forward through it in the commit→write-back gap,
[ADR-020](020-commit-write-back-protocol.md)). A timer that deletes a committed
object would therefore drop **live data**. GC must instead be a **reachability**
problem ([ADR-016](016-object-storage-native-layout.md)): a transaction object is
live exactly while some shard or root still references its txid.

Several earlier ADRs deliberately left their cleanup debt to this one:

- **Orphaned transaction objects** — every transaction leaves a finalized object
  that becomes garbage once unreferenced: a committed object whose values are
  later overwritten, and an aborted object from a wound or the serial-fallback
  release ([ADR-020](020-commit-write-back-protocol.md),
  [ADR-024](024-hold-and-wait-conflict-resolution.md)). Under the MVP's
  release-and-retry this was a *storm* — a fresh `TxId::renew` object per failed
  attempt and the dominant source of garbage; with hold-and-wait
  ([ADR-024](024-hold-and-wait-conflict-resolution.md)) the per-conflict renew is
  gone, so the volume drops to overwrites, wounds, and the occasional serial
  fallback. The *mechanism* GC needs is identical either way.
- **Uncontended dead locks** — [ADR-021](021-wound-wait-leases-shard.md)
  reclaims a conflicting lock *lazily*, only when contended. An abandoned lock
  that nobody contends lingers forever; ADR-021 explicitly promises it is
  "swept by GC."
- **Missing-object grace** — a `locked_by` entry can reference a *missing*
  transaction object: under hold-and-wait
  ([ADR-024](024-hold-and-wait-conflict-resolution.md)) a live transaction takes
  its locks first and materializes its pending object lazily, so the object is
  legitimately absent for a bounded window. The engine covers this with the
  `handle_unknown_tx` grace (ADR-021/024); GC must **respect** that grace — never
  treating a referenced-but-missing object as collectable — since GC is otherwise
  the only thing that can delete a referenced-looking object.
- **Subcollection teardown** — [ADR-018](018-collection-root-membership.md)
  removes a child name from the parent root but defers reclaiming the child's
  shards / transaction objects here.

This ADR specifies the collector that pays those debts. Compaction and the
explicit liveness-counter object remain out of scope (see
[Deferred](#deferred)).

## Decision

### Liveness is reachability from the coordination graph

The roots of the live set are the **shards** and **collection roots**; the
leaves are **transaction objects**. A transaction object is referenced when:

- a shard entry names its txid in `current_writer` (the MVCC pointer) or in
  `locked_by` (a held lock), per
  [`ShardEntry`](../../crates/glassdb-storage/src/shard.rs); or
- a collection root names its txid in `membership_locked_by`
  ([`CollectionRoot`](../../crates/glassdb-storage/src/root.rs)).

```
live = ⋃ over all shards   ( current_writer ∪ locked_by )
     ∪ ⋃ over all roots     ( membership_locked_by )
```

A transaction object `_t/<txid>` is **collectable** iff `txid ∉ live` **and** its
lease is past the safety horizon (below). Everything else is kept.

### A forward mark does not scale; check candidates in reverse

A forward mark — enumerate every collection, `list` every `_s/` shard, GET them
all, and union the referenced txids — is `O(non-empty shards + roots)` per cycle:
the size of the **whole database**, paid every cycle even when almost nothing is
garbage. For a large store that is infeasible.

The transaction object already records **its own back-references**. A
[`TxLog`](../../crates/glassdb-storage/src/tlogger.rs) carries the full set of
paths the transaction locked (`locks`) and wrote (`writes`), and every write
records the `prev_writer` it superseded
([`TxWrite`](../../crates/glassdb-storage/src/tlogger.rs)). So instead of marking
forward from all shards, GC works **backward from a batch of candidate `_t/`
objects**: each candidate names exactly the entries that can reference it, so
confirming it is dead costs a GET of *those* entries only — never a database-wide
scan.

### The candidate-driven reverse check

One cycle:

1. **Pick candidates.** Take a batch of txids — from the write-back hint (the
   `prev_writer` a fresh commit just superseded, i.e. exactly the txids that *just*
   lost a reference; see [Trigger](#trigger-cadence-and-candidates)) and/or a paged
   `list` of `{db}/_t/`. Only a *subset* of `_t/` is touched per cycle.
2. **Read the candidate.** GET `_t/<txid>`: its `timestamp` drives the horizon
   check, its `status` selects the path below, and its `locks ∪ writes` name the
   entries that could reference it. **Within** the horizon → keep (it may be a live
   transaction whose references the non-atomic check has not observed yet).
3. **Resolve by status** (past the horizon):
   - **Committed** — GET each recorded shard/root and test whether it still names
     `txid` (a `current_writer` on a written key, a `locked_by` on a locked entry,
     or `membership_locked_by`). Any hit → **keep** (live values, or the
     commit→write-back gap); none → **delete**. A committed object is never pruned
     or force-aborted — its locks become `current_writer` through write-back, never
     through GC.
   - **Aborted** (already finalized) — **release** its recorded `locked_by` entries
     (idempotent CAS) promptly, but **delete** the object only once it is past the
     horizon *measured from the abort* (it is a tombstone — see
     [Aborted objects are tombstones](#aborted-objects-are-tombstones-until-a-lease-after-the-abort)).
   - **Pending but dead** — do **not** drop its locks or delete it in place; first
     **force-abort** it (next section), which either reveals it was alive (keep) or
     finalizes it into the aborted (tombstone) case.

A candidate's reference set is **monotonically shrinking** while it is checked:
`current_writer` only ever moves *forward* to newer transactions, and a candidate
worth deleting is finalized or past its lease (its locks are only released, never
taken). Once a key stops naming `txid` it never names it again, so a concurrent
writer cannot invalidate the check.

### Reclaiming a dead pending transaction: abort, then release, then delete

A pending candidate past the horizon has a stale lease, but GC must **not** act on
that by dropping its locks or deleting its object while it is still `pending`. Doing
so would be a correctness hazard: if the lease judgment raced a live owner (clock
skew, or a refresh GC did not observe), the owner could commit believing it still
holds a lock that GC had already handed to someone else. GC instead reclaims it with
the **same official sequence the engine uses for a contended expiry**
([ADR-021](021-wound-wait-leases-shard.md)):

1. **Abort officially.** CAS the transaction object `pending → aborted`
   (`try_abort_remote_tx`). This is the single synchronization point: if the owner
   committed or refreshed first, the CAS fails — the transaction was *not* dead, so
   GC leaves it untouched; if the CAS succeeds, the death is now **durable and
   final** and the owner's own commit will fail.
2. **Release known locks.** Only now, with the status finalized, drop its recorded
   `locked_by` entries from their shards/root (idempotent CAS) — the aborted path.
3. **Delete after the tombstone lease.** The abort stamps the object with a fresh
   `timestamp`; GC removes it only once it is past the horizon *from that abort*, not
   immediately (see [Aborted objects are tombstones](#aborted-objects-are-tombstones-until-a-lease-after-the-abort)).

This ordering is what **minimizes uncertainty for observing clients**: a
transaction's status only ever moves forward (`pending → aborted`), its locks are
released only *after* its death is durable, and the object is removed last. A peer
therefore always sees one of `pending`, `aborted`, or — after deletion — `missing`
(resolved by the `handle_unknown_tx` grace), and never a lock that vanished out from
under a live owner. Pruning and deletion thus only ever touch a **finalized**
transaction.

### The safety horizon reuses the ADR-021 lease

The horizon is exactly [ADR-021](021-wound-wait-leases-shard.md)'s expiry
predicate, evaluated on the transaction object's `timestamp` (which doubles as
the lease, ADR-019/021):

```
collectable-by-age  ⟺  now > timestamp + PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW
```

reusing the `is_expired` seam and constants in
[`monitor.rs`](../../crates/glassdb-trans/src/monitor.rs) (15s + 30s), against
the `Clock` abstraction so the horizon is deterministic under the DST executor
(ADR-008/013).

**Why an age horizon is necessary and sufficient.** The reverse check is
non-atomic — it reads the candidate, then its referencing entries — so it can race
concurrent writes. The only dangerous error is *under*-approximating liveness
(deleting a still-referenced object); *over*-approximation (keeping garbage) is
harmless and reclaimed next cycle. The horizon precludes under-approximation:

- A **pending** object is materialized **lazily**, *after* its transaction has
  taken locks ([ADR-024](024-hold-and-wait-conflict-resolution.md)). When GC reads
  a recent pending candidate, the `locked_by` entries it records may not be durable
  yet (an in-doubt lock CAS, [ADR-009](009-in-doubt-conditional-writes.md)), so the
  back-reference GETs can momentarily find none even though the transaction is live.
  A live holder keeps its `timestamp` fresh through the refresher (ADR-024), so a
  pending candidate *within* the horizon is kept; one whose lease has fallen past it
  has stopped refreshing and is genuinely dead — however long it was legitimately
  pending before that.
- A **committed** object never gains a *new* reference after its `timestamp`:
  commit converts its pre-acquired locks to `current_writer` on the same shards
  (write-back, ADR-020) and takes no further locks, so its reference set is
  **stable-or-shrinking**. Re-checking past the horizon therefore never misses a
  later reference — there are none — so an observed-clear committed candidate stays
  clear (completeness of the *recorded* set is the next section).
- An **aborted** object holds no value, so a late reference cannot endanger live
  data. A stuck owner *can* still add one more `locked_by = txid` after the abort,
  but that names a valueless tombstone, and the tombstone retention (next section)
  keeps the object present for the whole window in which the owner could still be
  acting — so the late lock resolves cleanly, never against a prematurely-missing
  object.

So past the horizon a candidate's recorded reference set is observed in full, and
the delete is safe.

### Aborted objects are tombstones until a lease after the abort

Releasing an aborted candidate's locks is safe immediately, but **deleting its
object is not** — the object is a *tombstone* that other parties may still consult.
The client that owned the transaction may have been stuck (a long pause, a slow
shard write) and, on waking, still issue **one more lock CAS** under that `txid`, or
its refresher may still run. So GC keeps the aborted object until a full safety lease
has elapsed **since the abort**:

```
deletable  ⟺  now > abort_timestamp + PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW
```

The abort — a self-abort, a wound, or GC's own force-abort — therefore stamps the
object with a fresh `timestamp` (the abort instant), and GC measures the horizon
from *that*, never from the dead pre-abort lease. Two things depend on the tombstone
outliving the owner:

- **No resurrection.** Under lazy creation ([ADR-024](024-hold-and-wait-conflict-resolution.md))
  a live transaction's refresher (re)writes its object with **create-if-absent**
  semantics; finding it present-and-aborted is exactly how a wounded owner learns it
  lost and gives up. If GC deleted the tombstone too early, that create-if-absent
  would *succeed* against the now-absent object and **resurrect the transaction as
  `pending`**, erasing the wound. Retaining the tombstone a full lease guarantees the
  owner's next refresh (its cycle is shorter than `PENDING_TX_TIMEOUT`) still finds
  it.
- **No missing-object ambiguity for a late lock.** A lock the stuck owner installs
  after the abort points at a *present aborted* object, which a contender drops on
  sight, rather than a *missing* one that would cost a `handle_unknown_tx` grace
  window. After the lease, the owner can no longer be acting (it has self-fenced or
  observed the tombstone), so any residual stale lock degrades to the benign
  missing-object case below.

### Why the recorded back-references are complete

Checking only a candidate's recorded entries is equivalent to scanning every shard,
*because an entry can name `txid` only if `txid` put it there*:

- `current_writer = txid` is published by write-back for keys in `txid.writes`;
- `locked_by = txid` (and the root's `membership_locked_by`) is installed by
  validate-and-lock for paths in `txid.locks`.

So `txid.locks ∪ txid.writes` is a **superset** of everything that can name `txid`,
*provided the object faithfully records its full lock + write set*. A committed
object always does (the commit writes the complete `TxLog`); a self-abort does too.
A **wound** must therefore preserve the victim's lock intentions when it flips
`pending → aborted` (copy `locks` from the pending object it overwrites) so the
aborted object stays self-describing.

The record that cannot be complete is a wound of a victim that took locks but had
**not yet materialized** its pending object: the wounder writes a minimal aborted
object with no `locks` (it cannot know them). (More generally, a finalized object's
recorded locks can lag the locks it actually took.) This never costs **value**
safety, because only a **committed** object holds values and a committed object
always records completely — so the reverse check never deletes a value-bearing
object whose references it has not fully seen. A finalized **valueless** object with
an incomplete record is still reclaimed past the horizon; any `locked_by` entry it
failed to list degrades to a *missing-object* reference, which the engine's
`handle_unknown_tx` grace reclaims exactly as it would a still-present aborted lock
(ADR-024). The only cost is that such an uncontended stale lock, once contended,
takes one grace window to reclaim instead of dropping on sight — bounded and rare,
never a correctness loss, and **no forward scan required**.

### The commit→write-back gap is covered for free

A transaction that has committed but not yet written back is still referenced via
`locked_by` — locks are released only by write-back (ADR-020) — so it is in the
live set and never swept during the gap. This is the same fact that lets readers
help-forward through it: visibility and liveness are both gated on the single
commit object, not on write-back.

### Stale-lock and empty-entry pruning

A **finalized** candidate that is itself a dead holder can still look referenced
because its own `locked_by` entries were never cleared — an *uncontended* dead lock
that lazy contention (ADR-021) never reached, so the object can never become
collectable on its own. As part of releasing the candidate's locks GC prunes these:
for each entry in its recorded `locks` whose shard/root still names `txid`, a single
idempotent CAS (`write_if`, inheriting
[ADR-009](009-in-doubt-conditional-writes.md) in-doubt parity) drops the `locked_by`
holder, and removes an entry then left **vestigial** — no lock, no `current_writer`,
not a live tombstone.

Pruning runs **only for a finalized candidate** — a dead *pending* one is
force-aborted first (above), never pruned in place. It keeps shards tidy and lets
the object be reclaimed without leaving a missing-object reference behind, and is the
*proactive*, now **targeted**, analog of ADR-021's contention-driven clearing: GC
visits only the shards the candidate itself recorded, never the whole shard space.
(A record-less minimal abort has nothing to prune; it is still reclaimed past the
horizon.) `current_writer` is **never** cleared (it is the live value pointer); it is
replaced only by a newer writer's write-back.

### GC's invariant settles the missing-object case

GC's contract is twofold and load-bearing:

1. it never deletes an object **holding live values** while it is still referenced
   (a committed object that is some key's `current_writer`, or one still in its
   commit→write-back gap), and
2. it never deletes any object **within the safety horizon** — for an aborted
   object, measured **from the abort**, so the tombstone outlives any client that
   could still act under that `txid`.

(1) follows from the reverse check plus completeness: a committed object always
records its full back-references, so "its recorded entries no longer name it" means
it is genuinely unreferenced before deletion. A **finalized valueless** object
(aborted) carries nothing to lose, so GC may delete it once past the horizon even if
an uncontended stale `locked_by` still names it — but, by (2), only past the horizon
measured from the abort, so the tombstone outlives the owner (no resurrection under
ADR-024, no prematurely-missing object). A **pending** object is never deleted or
pruned in place: a dead one is first **force-aborted** (above), so its death is
durable before any lock moves — which is what keeps a slow-but-live owner from
losing a lock under it.

A `locked_by` entry that points at a **missing** `_t/` object is therefore one of
three benign cases: a live transaction in its lazy-creation window
([ADR-024](024-hold-and-wait-conflict-resolution.md)), an in-doubt / failed pending
create ([ADR-009](009-in-doubt-conditional-writes.md)), or a valueless dead object
GC has reclaimed. All three are resolved by the engine's `handle_unknown_tx` grace
(ADR-021/024) — treat the holder as live for the grace window, then reclaim — so a
missing referenced object is always safe, whatever its cause. This is what lets GC
reclaim aggressively while never endangering live data.

### Subcollection teardown

A subcollection delete removes the child's name from the parent root under the
parent's membership write lock (ADR-018), after which the child root and its
shards are unreachable from the membership tree. GC reclaims them as a reachability
case: an `_s/` or `_i` object under a prefix that no live parent lists is garbage
and is deleted; once the child's shards are gone, the transaction objects they
referenced fall out of the live set and the normal sweep removes them. Removing
the name *before* deleting the child's objects guarantees no new reference to
them can appear during teardown. Aggressive single-shot recursive teardown (vs.
letting the periodic collector drain it) is an optimization, not required for
correctness.

### Trigger, cadence, and candidates

The collector is a single loop on the
[`Background`](../../crates/glassdb-concurr/src/background.rs) executor — the inert
`Gc` already holds the `Weak<Background>` and `TLogger` it needs — timed off the
`Clock` seam so cycles are deterministic under DST, batching its GETs and deletes.
Candidates come from two sources:

- **Write-back hint (primary).**
  [`Gc::schedule_tx_cleanup`](../../crates/glassdb-trans/src/algo.rs) is
  **repurposed** from v1's "delete after a delay" queue (unsafe in v2 — a
  freshly-written-back object is *referenced* as `current_writer`) into a
  **candidate feed**: when a commit's write-back overwrites a key it supersedes that
  key's `prev_writer`, precisely a txid that just lost a reference and is worth
  re-checking. Wounds and serial-fallback aborts
  ([ADR-024](024-hold-and-wait-conflict-resolution.md)) feed their own ids the same
  way.
- **Paged `_t/` list (completeness).** GC also walks `{db}/_t/` a page per cycle, so
  **every** transaction object is eventually visited regardless of lost hints. This
  is a `list` of one flat directory — never a database-wide shard scan — and is what
  makes the candidate set complete without any forward mark.

The reverse check is authoritative, so a dropped or stale hint only delays a delete,
never causes an unsafe one.

### Deferred

- **Compaction.** A cold key can pin a fat committed object full of
  otherwise-dead values (ADR-019). Splitting/rewriting such blobs to reclaim the
  dead values is deferred to v2; GC here reclaims whole objects only.
- **Explicit liveness-counter object.** A per-object reference counter maintained
  by write-back / overwrite would make liveness `O(1)` per candidate — removing even
  the candidate's shard GETs — at the cost of a second source of truth to keep
  consistent. The MVP uses the candidate-driven reverse check only; the counter is a
  later optimization.

## Consequences

- The unbounded-growth limitation of ADR-016/019/020 is bounded: finalized
  transaction objects (overwritten commits, wound / serial-fallback aborts, and
  any residual release-and-retry orphans), uncontended dead locks, and torn-down
  subcollections are all reclaimed, so ADR-016–021 become operationally complete,
  not just correct on the happy path.
- GC's value-safety + horizon **invariant is load-bearing**: a committed object is
  deleted only once its complete record proves it unreferenced — which is what
  retroactively justifies ADR-021's lazy (contention-only) reclaim — while finalized
  valueless objects are reclaimed past the horizon regardless, their residual stale
  locks absorbed by the engine's `handle_unknown_tx` grace
  ([ADR-024](024-hold-and-wait-conflict-resolution.md)). Pruning and deletion only
  ever touch finalized transactions; a dead *pending* one is force-aborted
  (`pending → aborted` CAS) before any lock moves, and an **aborted object is kept
  as a tombstone a full safety lease past the abort** — so a stuck owner cannot be
  resurrected by its own create-if-absent refresher (ADR-024) and never sees a lock
  vanish out from under it. GC never endangers live data and needs no database-wide
  scan.
- The cost is **proportional to garbage, not database size** — there is no
  database-wide scan at all: per cycle a GET of each candidate object and of the
  handful of shards/root it records. The first passes are GET-heavy (cold shard
  reads), but repeated checks hit the same hot shards, so the retained `Global` /
  shard cache (ADR-023) amortizes them sharply. The deferred liveness counter would
  remove even the candidate's shard GETs.
- Reusing the lease predicate, `Clock`, and `Background` keeps the collector
  deterministic under the DST executor, so the existing oracles can exercise it.
  Targeted regression tests follow directly from the safety argument: a committed
  object that is overwritten becomes collectable; a referenced committed object
  and a *recent* unreferenced pending object are **not** collected; an
  uncontended aborted lock is pruned and then its object swept on the next cycle;
  a removed subcollection's shards/root/objects are reclaimed.
- Compaction of fat blobs and the absence of an explicit liveness counter remain
  accepted MVP limitations (ADR-016/019).
