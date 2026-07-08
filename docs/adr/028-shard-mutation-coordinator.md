# ADR-028: Unified shard-mutation coordinator (installed resolvers, monotonic fold)

## Status

Accepted (implemented).

The one documented exception to the invariant below — GC's out-of-band
mark-sweep CAS — is closed by
[ADR-029](029-gc-through-shard-coordinator.md), which routes GC's lock
reclamation through this coordinator and makes vestigial-entry pruning a fold
property, so the invariant holds with no exceptions.

Routing the single read-write install through the coordinator made it load the
shard a second time (the fast path's eligibility pre-check already loaded it).
[ADR-030](030-seed-shard-loads.md) restores the single-load commit by letting a
round reuse the shard the transaction already cached for its first fold attempt.

Generalizes the deduplication **mechanism** of
[ADR-025](025-dedup-shard-lock-acquisition.md) and
[ADR-026](026-dedup-shard-release-write-back.md), and **supersedes the bespoke
single read-write shard CAS** of [ADR-027](027-single-rw-parallel-lock-publish.md)
(its step 2 "W2 — one shard CAS" and the private retry/reclassify loop); ADR-027's
*parallel-commit* decision (two concurrent writes, the commit point, the one
irreducible in-doubt) is retained. The concurrency semantics of
[ADR-002](002-wound-wait-locking.md) / [ADR-021](021-wound-wait-leases-shard.md) /
[ADR-024](024-hold-and-wait-conflict-resolution.md) are **unchanged**: this ADR
moves *where* they run, not *what* they decide.

## Context

The shard (`{prefix}/_s/<i>`) and collection root (`{prefix}/_i`) are the v2 CAS
coordination units ([ADR-017](017-shard-object.md),
[ADR-020](020-commit-write-back-protocol.md)). Every concurrency operation is a
read-modify-write of one such object: acquiring locks, publishing
`current_writer` pointers on write-back, and releasing locks.

Two ADRs already batch these across transactions. ADR-025 routes lock
**acquisition** through a per-object `Dedup`: contenders on one shard merge into a
single load + CAS when they do not exclusively conflict, and a merge **predicate**
(same-key writers conflict; disjoint keys and shared reads do not) decides
admission. ADR-026 extends the same `Dedup` to **release** and **write-back**.
Both live *inside* the `Locker`.

ADR-027's single read-write fast path is the one mutation that **opts out**. To
commit a lone overwrite in ~1 RTT it issues its committed object and its shard
lock install *concurrently*, and — because the batching machinery is private to
`Locker` — it installs the lock with its **own** direct shard CAS plus a private
reload/reclassify retry loop. That raw CAS then **races** the deduplicated rounds
on the same shard: full-path acquires, other single read-write installs, and
in-flight write-backs. It is the exact "racing CASes" cost that ADR-025/026 removed
everywhere else, reintroduced on the fast path's install.

Underneath this is a structural conflation. The `Locker` carries two separable
concerns:

- **Policy** — wound-wait priority, lock types, help-forwarding, hold-and-wait,
  the parallel/serial acquisition strategy, and per-transaction lock bookkeeping.
- **Mechanism** — "load the object once, resolve every member into one staged
  entry set, CAS once, recover precondition/in-doubt by reload, deposit each
  member's outcome."

Because the mechanism is not a reusable object, `Algo` cannot share it, and the
merge predicate (policy: "which transactions conflict") sits inside the
mechanism's admission control. The result is a batching win that one hot path
bypasses and a boundary that does not correspond to the two concerns.

## Decision

Introduce a **ShardCoordinator**: a single per-object *mechanism* over which
callers *install resolvers* that encode policy. Every shard/root entry mutation —
acquire, commit-install, write-back, release — flows through one coordinator
instance and is resolved by folding the installed resolvers over the loaded
object.

### The invariant

**All shard-entry mutations flow through one ShardCoordinator instance.** The only
documented exception is [ADR-022](022-garbage-collection-mark-sweep.md)'s mark-sweep,
which prunes dead locks/pointers out-of-band and is idempotent and best-effort by
construction. This invariant is what removes the racing CAS at its root: install,
acquire, write-back, and release for a shard all land in one single-flight
keyspace, so they are serialized and batched instead of competing on the object's
version.

### Mechanism: single-flight, monotonic fold, CAS retry

The coordinator, keyed on the object path, drives one round as:

1. **Single-flight.** At most one round runs per object; concurrent submissions
   join the in-flight round or wait for the next one. (This is the existing
   `Dedup` primitive, unchanged.)
2. **Load once.** Read the object a single time for the whole round.
3. **Fold.** Apply the round's installed resolvers, in a **policy-defined order**,
   over a running staged entry map, *threading the entry*: resolver N observes the
   entries as staged by resolvers 1..N.
4. **Commit the round.** One CAS. A precondition miss or in-doubt
   ([ADR-009](009-in-doubt-conditional-writes.md)) reloads and re-folds within a
   bounded budget. Deposit each member's outcome, then deliver.

The mechanism is **ignorant of locks, transaction ids, wound-wait, and commit**.
It folds opaque resolvers, threads entries, CASes, and retries. Nothing more.

### Policy: installed resolvers

A **resolver** is one operation's decision, supplied by its caller:

- `Locker` installs **Acquire**, **WriteBack**, and **Release**.
- `Algo` installs **CommitInstall** — the single read-write lock install formerly
  written as ADR-027's bespoke CAS.

A resolver, given the entry *as currently staged in this round*, either **stages**
a new entry (threaded to the next resolver) with its member outcome, or **stages
nothing** and returns an outcome (e.g. it must wait). It may `await` — it consults
the `Monitor` for wound-wait decisions and help-forwarding — and it emits its own
**heterogeneous** per-member outcome (`Locked`, `Wait(holder)`, `Conflict`,
`Landed`, `Moved`, `InDoubt`, `Released`).

Conflict admission — ADR-025's merge predicate — **dissolves**. Every operation
joins the round; conflicts resolve *in the fold*: the loser's resolver observes
the winner's staged lock and returns `Wait`. "Merge" becomes "both resolvers
stage"; "queue behind a conflict" becomes "the loser stages nothing and
re-submits." Reorderability survives only as a scheduling hint (a read-only or
release request may join any round rather than block behind an unrelated writer).

### Contracts

These are the load-bearing guarantees the split rests on. They are the ADR's real
content; the rest is relocation of proven code.

1. **Monotonic fold order (policy-supplied).** The fold visits members in
   wound-wait order — **oldest-first** — so that once a member has staged or been
   decided, no later resolver can invalidate it. Folding out of order would let an
   older resolver need to *wound* an already-staged younger member and rewrite its
   already-emitted outcome — backtracking the fold. Wound-wait supplies this order
   for free: nobody younger may wound the older who staged first. The equal-priority
   tiebreak used for ordering is **round-local only** and must let one contender
   make progress each round; it is never a *persistent* winner (a persistent
   prefix tiebreak flips under `renew` and livelocks — the `should_wound` rule of
   ADR-024).

2. **Member atomicity (member-major fold).** A resolver resolves its whole key set
   atomically: it stages **all** of its keys or **none** (it returns `Wait`/`Moved`
   without staging). Sequencing across conflicting members emerges because a later
   member observes an earlier member's staged entries — with no key-major
   transpose and no per-member rollback. This preserves the multi-key
   lock-acquisition atomicity the current member-wise resolution already has.

3. **Idempotent, cancel-safe resolvers.** The engine drops and re-drives rounds
   (the `Dedup` cancel contract) and re-folds on every reload, so a resolver
   re-run whose effect is already present must be a no-op: re-installing one's own
   lock is idempotent, and a write-back publishes only its own monotonic pointer.
   This is precisely what makes precondition/in-doubt recovery *free* — the same
   resolver runs on the first attempt and on every reload.

4. **Per-member outcome side-channel.** `Dedup` fans out one shared result, but
   members have heterogeneous outcomes. Each member's outcome is deposited into its
   own slot **before** the shared result is delivered (deposit happens-before
   delivery), so each caller reads exactly its own outcome. This is ADR-025's side
   channel, retained.

5. **Explicit in-doubt attribution.** The engine surfaces the store outcome to the
   resolver on reload; the resolver classifies **for itself**. For a pre-commit
   `Acquire`/`Release`/`WriteBack` this is a blind idempotent retry (nothing has
   committed). For **CommitInstall** — whose committed object is being written in
   parallel — the resolver reaches `Landed` (its lock present or help-forwarded),
   re-stage (still eligible), `Moved` (a precondition miss with the entry moved
   past it), or `InDoubt` (an `Unavailable` CAS *and* the entry then moved, so
   whether it committed is unknowable). The engine must **never** silently
   "recover" a commit-critical member by blind retry; the classification is the
   resolver's, on the reloaded entry. This is ADR-027's single irreducible in-doubt,
   now expressed as one branch of one resolver instead of a bespoke loop.

### Where responsibilities land

- **ShardCoordinator** (mechanism + fold): owns the `Dedup`, the shard store, the
  shared `Resolver`, the `Monitor` handle, the retry budget, the diagnostic
  snapshot, and shutdown. It exposes submit-and-await entry points per operation
  kind and knows nothing of transaction strategy.
- **Locker** (policy): grouping keys by shard, the parallel-vs-serial strategy
  *across* shards, the hold-and-wait loop (`Wait` → poll the holder → re-submit),
  the collection-root membership step, and per-transaction lock bookkeeping and
  stats. It calls the coordinator for each per-object step.
- **Algo**: the single read-write pre-check and its parallel commit — the
  committed-object write issued concurrently with a **CommitInstall** submission
  to the coordinator, replacing the direct shard CAS.
- **Resolver**, **ShardStore**, **Monitor**: unchanged, and still shared by the
  read path.

### Correctness

- **Serializability is preserved.** The set of decisions — wound-wait priority,
  help-forwarding a committed holder, hold-and-wait, membership locking, in-doubt
  recovery — is identical; only their location moves into resolvers. One round is a
  **deterministic linearization** of that round's mutations to one object, made
  atomic by the single CAS. Because the fold is monotonic (contract 1) and members
  are atomic (contract 2), the linearization is exactly a valid interleaving of the
  same operations the un-batched path would have applied across separate CASes.
- **The within-shard equal-priority livelock is removed.** Single-flight plus a
  deterministic per-round fold winner means there is no racing CAS on a shard: one
  contender stages and makes progress each round, then releases, so equals no
  longer need the serial fallback *for a single object*. The serial fallback of
  ADR-024 is **retained** for **cross-shard** deadlock — the fold serializes one
  object, it says nothing about lock ordering *across* objects, so `Algo`'s
  deadlock timeout and serial re-acquire still bound a wait-for cycle spanning
  shards.
- **ADR-027's fast-path correctness is preserved.** CommitInstall participates in
  wound-wait (it holds a lock during the pre-commit window), records the same
  back-references for GC ([ADR-022](022-garbage-collection-mark-sweep.md)),
  help-forwards the resolved predecessor into `current_writer`, and surfaces the
  same single irreducible in-doubt — all now as one resolver rather than a private
  path.
- **No new CAS site, no new in-doubt case.** The coordinator has exactly the CAS
  sites ADR-025/026/027 already reasoned about; it only routes the fast-path
  install through the shared one. ADR-009 recovery is unchanged.

### Determinism (DST)

The backend op stream changes **shape** — fewer rounds, priority-ordered folds,
and the fast-path install now interleaved with other shard operations rather than
a standalone CAS — but stays deterministic under the simulation executor via the
`Clock` / `Background` seam ([ADR-008](008-deterministic-simulation-fuzzer.md) /
[ADR-013](013-deterministic-scheduling-test-coverage.md)). The run-to-run
op-stream self-check and the serializability / cycle oracles remain the safety net,
and the minimized fuzz corpus is regenerated against the new shape.

## Consequences

- The single read-write install **stops racing**: it batches with acquires,
  write-backs, and releases on its shard, extending ADR-025/026's win to the last
  path that bypassed it. The invariant "all shard-entry mutations flow through one
  coordinator" becomes a checkable property rather than an aspiration.
- **Policy and mechanism separate cleanly.** The engine folds opaque resolvers;
  all lock/transaction semantics live in resolvers. Two pieces of machinery
  disappear as *special cases* of the fold: ADR-025's merge predicate (subsumed by
  fold order) and ADR-027's bespoke reclassify loop (subsumed by the resolver's
  reload branch).
- The **cost** is that resolvers are asynchronous and consult the `Monitor`
  mid-round, so the `Dedup` cancel-safety contract propagates outward into policy,
  and implementers must uphold three contracts the un-split code enforced
  implicitly: the monotonic fold order, member-major atomic staging, and
  idempotent re-fold. These are the places a defect would corrupt state (a
  double-applied or lost commit), so they warrant deterministic regression tests
  ahead of enabling conflicting-member batching.
- The within-shard serial fallback trigger may be **retired**; the cross-shard one
  is kept. The concurrency behaviour is otherwise identical to ADR-024.
- The one structural addition beyond ADR-025's side channel is the **policy-supplied
  fold comparator**. Everything else is relocation and generalization of code that
  ADR-025/026/027 already run under the fuzzer.
