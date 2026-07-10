# ADR-029: Garbage collection through the shard-mutation coordinator

## Status

Accepted (implemented).

Closes the one exception carved out by
[ADR-028](028-shard-mutation-coordinator.md) (the shard-mutation coordinator
invariant) and refines the stale-lock / empty-entry pruning **mechanism** of
[ADR-022](022-garbage-collection-mark-sweep.md); ADR-022's GC _policy_ (reverse
mark-sweep, the safety horizon, abort-then-release-then-delete, tombstone
retention) is **unchanged**: this ADR moves _where_ GC's lock reclamation runs,
not _what_ it decides.

## Context

[ADR-028](028-shard-mutation-coordinator.md) established a single invariant:
**every shard/root entry mutation flows through one `ShardCoordinator`**, which
loads each coordination object once, folds the round's installed resolvers over
it, CASes once, and recovers precondition/in-doubt by reload-and-re-fold. Lock
acquisition, commit-install, write-back, and release all became resolvers the
callers (`Locker`, `Algo`) install. The invariant is what removed the "racing
CASes" on a hot shard: contending mutations serialize and batch through one
single-flight keyspace instead of competing on the object's version.

ADR-028 documented exactly **one** deliberate exception: ADR-022's mark-sweep,
whose lock reclamation still issues its **own** shard/root CAS with a private
reload/retry loop, out-of-band from the coordinator. That reclamation is the
last shard-entry mutation that bypasses the coordinator, so it is the last place
where a CAS races the deduplicated rounds on the same shard — the exact cost
ADR-028 removed everywhere else, reintroduced on the collector's release path.

Underneath the racing CAS sit two structural facts:

- **Duplicated release semantics.** GC's "drop this dead txid's holds from a
  shard/root, publish nothing" is _identical_ to the release the `Locker`
  already drives through the coordinator. GC re-implements it as a separate CAS
  loop only because the `Locker`'s per-object unlock step was not exposed as a
  callable operation.
- **Pruning is the collector's private extra.** GC does one thing the `Locker`
  release does not: when clearing the last holder leaves an entry **vestigial**
  (no lock, no `current_writer`, not a live tombstone) it removes the entry
  (ADR-022 "stale-lock and empty-entry pruning"). The coordinator's fold, by
  contrast, only ever stages entries; it never removes one. So even a GC release
  routed through the coordinator would leave the vestigial entries GC exists to
  reclaim, and nothing else prunes them.

The result is that ADR-028's invariant is "true except for GC," and the one
exception carries a real cost (a racing CAS) and a real duplication (a second
copy of release).

## Decision

Bring GC's lock reclamation inside the coordinator, and make vestigial-entry
pruning a property of the fold — so the ADR-028 invariant holds with no
exceptions.

### The invariant, strengthened

> **All shard/root entry mutations flow through one `ShardCoordinator` — with no
> exceptions.**

GC stops being the documented carve-out. Every mutation of a shard entry or a
root — acquire, commit-install, write-back, `Locker` release, and now **GC's
reclamation release** — is a resolver folded by the one coordinator. "No shard
CAS outside the coordinator" becomes a checkable property of the whole engine,
not an aspiration with a footnote.

### Responsibilities

- **ShardCoordinator (mechanism, unchanged in spirit).** Still the sole owner of
  the shard/root `Dedup`, the single load + fold + CAS + reload-recover round,
  and the per-member outcome side-channel. Its one new mechanism duty is
  **finalizing the fold by dropping vestigial entries** (below). It remains
  ignorant of locks, transaction ids, wound-wait, commit, and _now_ GC.
- **GC (policy, one responsibility relocated).** GC keeps every ADR-022 policy
  decision — candidate selection, the safety horizon, reverse liveness check,
  the abort-then-release-then-delete ordering, tombstone retention. What changes
  is purely _how it applies a release_: instead of its own CAS loop, GC calls the
  `Locker`'s per-object **unlock methods** (shard-holder release and
  root-membership release), driving them from the dead candidate's recorded lock
  set. GC still reads shards and roots directly for its reverse check (a read
  path, untouched).
- **The release resolvers stay private to the `Locker`.** The "drop this id's
  holds, publish nothing, idempotent and best-effort" shard and root release
  resolvers remain an implementation detail of the `Locker`; they are never
  shared or installed by another module. GC reaches that behaviour only through
  the `Locker`'s unlock methods, so there is one definition and one installer.

### Vestigial pruning belongs to the fold, not to any caller

Pruning moves from a GC-private CAS step to a **mechanism** guarantee: when the
coordinator finalizes a round it drops any entry left **vestigial** — no holder
and no `current_writer` (a live tombstone always keeps its `current_writer`, so
it is never vestigial). A vestigial entry carries **no information**: it names no
transaction and reports "key absent" identically to having no entry at all. So
removing it is a semantic no-op that only shrinks the object.

Making this a fold property (rather than a capability each release resolver
opts into) means:

- GC's release becomes _exactly_ the `Locker`'s release — no GC-specific
  resolver, no removal signal threaded through the fold contract.
- **Every** mutation path leaves no dead entry behind: an aborted create that a
  `Locker` release clears, a read lock dropped off a never-committed key, and a
  GC reclamation all tidy up in the same CAS that clears the last holder —
  instead of relying on a later GC cycle to prune. GC's reclamation is now the
  same clearing, no longer also a special pruner.

## Correctness

The claim is that routing GC's release through the coordinator and pruning in
the fold **preserves ADR-022's safety argument exactly**, while removing the
racing CAS.

- **GC's safety invariant is untouched.** ADR-022's contract — never delete an
  object holding live values while referenced, never delete within the horizon
  (for an aborted object, measured from the abort) — is a property of _what GC
  decides_ (status resolution, the horizon, force-abort before any lock moves),
  not of _how the release CAS is issued_. Relocating the release changes none of
  those decisions. GC still releases only for **finalized** candidates (aborted,
  or dead-pending only after a successful `pending → aborted` force-abort) and
  **never** releases a committed candidate, so a coordinator release can never
  race a live owner's own write-back on the same object.
- **Release is idempotent and commutes in the fold.** A release stages the
  removal of exactly one txid from the entries that name it and publishes
  nothing. Folded alongside other members (a disjoint acquire, a write-back, a
  second release), it neither reads nor writes their keys, so the round's
  linearization is independent of fold order — the same property ADR-028 already
  relies on for the `Locker`'s release. Re-running it on reload (precondition or
  in-doubt recovery) re-clears an already-absent holder: a no-op, inheriting
  ADR-009 parity. So the coordinator's existing reload-recover loop subsumes GC's
  hand-rolled retry with no new CAS site and no new in-doubt case.
- **The reference set still only shrinks.** ADR-022's reverse check is safe
  because a candidate's references move monotonically away from it. A release
  through the coordinator only ever _removes_ the candidate's txid, never adds a
  reference, so the monotonic-shrink property the check depends on is preserved.
- **Pruning cannot drop a live reference.** The fold removes an entry only when
  it is vestigial — no holder and no `current_writer`. Such an entry references
  **no** txid, so pruning it removes nothing from the live set of ADR-022's
  reachability graph. In particular it can never make a still-referenced
  transaction object look collectable: a referenced object is named by a
  `current_writer` or a `locked_by`, neither of which a vestigial entry has. The
  prune is safe on **every** fold path, not just GC's — an acquire or write-back
  round that incidentally leaves an unrelated entry vestigial may drop it with
  the same reasoning.
- **`current_writer` is still never cleared by GC.** Pruning removes only
  entries with no `current_writer`; the live value pointer is replaced solely by
  a newer writer's write-back, exactly as before.

## Consequences

- **The ADR-028 invariant becomes exception-free and checkable.** No shard/root
  CAS exists outside the coordinator; the last racing CAS (GC's release) is gone,
  extending ADR-028's single-flight batching to reclamation — a GC release now
  merges with any in-flight acquire/write-back on the same shard instead of
  competing with it.
- **Release semantics live in one place.** The release resolvers stay private to
  the `Locker`, and GC drives them through the `Locker`'s unlock methods rather
  than re-implementing or sharing them; ADR-022's "targeted pruning" stops being
  a GC-private CAS and becomes a fold guarantee that tidies every path. GC's
  reclamation shrinks to candidate policy plus a call into the `Locker`'s unlock
  step.
- **The DST op-stream shape changes.** Fewer, differently-shaped shard writes
  (vestigial entries pruned in the clearing CAS rather than a later GC cycle, GC
  releases interleaved with other rounds). This stays deterministic under the
  simulation executor via the existing `Clock` / `Background` seams; the
  serializability / cycle oracles and the run-to-run op-stream self-check remain
  the safety net, and the minimized fuzz corpus is regenerated against the new
  shape — the same DST discipline ADR-028 already imposed.
- **Cost.** GC's release now flows through the shared `Dedup`, so a burst of
  reclamations contends the same single-flight keyspace as live traffic on a hot
  shard (serialized and batched, not racing) rather than issuing independent
  CASes. This is the intended trade — the same one ADR-025/026/028 accepted for
  every other mutation — and it removes GC's private retry budget as a second
  tuning knob.
- **Out of scope.** Creating an empty collection root (an idempotent
  create-if-absent of a brand-new root, not a mutation of an existing entry) and
  test seed helpers that write shards directly to set up preconditions are not
  runtime coordination CAS and are unaffected by the invariant.
