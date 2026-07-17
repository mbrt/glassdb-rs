# ADR-030: Reusing cached shard loads across a transaction's coordinator rounds

## Status

Superseded by
[ADR-036](036-decoded-object-cache-with-bounded-freshness.md).

Refines the `ShardCoordinator` mechanism of
[ADR-028](028-shard-mutation-coordinator.md) (a round may reuse a cached shard
for its first fold attempt; the fold, CAS, and reload-recover loop are unchanged)
and restores the single-load commit of
[ADR-027](027-single-rw-parallel-lock-publish.md) that ADR-028 split in two. It
turns off the object-cache revalidation cost of
[ADR-023](023-slimmed-backend-trait.md) on the reuse path and preserves the
read-validation semantics of
[ADR-024](024-hold-and-wait-conflict-resolution.md). It changes _how many times a
transaction loads a shard_ and, for a superseded single-RW read, _which
transparent-retry path it takes_ — never _what a committed transaction decides_
(see Correctness).

## Context

Every shard mutation is a load-modify-CAS of one coordination object through the
coordinator (ADR-028): load once, fold the round's resolvers, CAS once, recover
by reload. The object cache (ADR-023) serves a cached shard by revalidating it
with a version-conditional `read_if_modified`, which is a **backend read even
when the object is unchanged** (the backend answers "not modified" but the round
trip — and the op-count it costs — still happen). So _every_ `load_shard` costs
one backend read regardless of cache warmth: caching removes the body transfer,
not the load op.

Against that cost, the single read-write fast path loads the same shard more
than once per transaction:

1. **Commit eligibility → install.** The fast path pre-checks dynamic eligibility
   (no live pending holder, not a create, read not superseded) by loading the
   shard and resolving its holders. ADR-027 then reused that same load for its
   bespoke lock CAS — one load per commit. ADR-028 routed the install through the
   coordinator, which loads the shard _itself_ to fold. So the pre-check and the
   fold each loaded the object: **two loads per single-RW commit**, one more than
   ADR-027, the extra `read_if_modified` that regressed the `singleRMW` cost/tx.

2. **The transaction body's read → commit.** A read-modify-write first _reads_
   the key: the read resolves the key's effective writer
   (`Resolver::effective_writer`, which loads the shard), materializes the value,
   and caches the shard **and** the value. At commit the fast path loads the
   shard _again_ for eligibility. So a steady-state single-RW touches its shard up
   to **three** times for what is logically one coordination object across one
   transaction (read-resolve, commit-eligibility, commit-CAS).

The waste is structural, but the fix is already in hand: each of those loads
targets a shard an earlier phase of the _same transaction_ just placed in the
object cache. The only reason a cached load still costs a backend op is the
revalidation round-trip.

## Decision

Add a **`Freshness` flag** to the object-cache read path (`ObjectCache::read`,
and through it `ShardStore::load_shard` and `Resolver::resolve_key`):

- `Latest` revalidates a cached copy with the version-conditional
  `read_if_modified`, exactly as before. Every reader that must observe the
  newest state keeps `Latest`.
- `AllowStale` serves a cached copy _as-is_, skipping the round-trip, and falls
  through to a real read only on a cache miss.

The single read-write fast path reads `AllowStale` for **both** loads it would
otherwise duplicate:

- its **commit-eligibility** resolve, and
- its **commit-install** fold (the coordinator's first fold attempt).

So a shard the transaction already cached — from its body read, or from the
eligibility check moments earlier — is reused with **no backend op**. A
steady-state single-RW loads its shard once (during the read) and the commit adds
none; a blind put whose shard is cached adds none either. On a cache miss
(evicted or never loaded) `AllowStale` degrades to a normal `Latest`-shaped load,
so the fast path still pays at most its own single load.

`AllowStale` is a first-attempt reuse only. If a round reloads — a precondition
miss or in-doubt recovery — it uses `Latest`. A round that merges more than one
contending member also loads `Latest`: the cache-reuse is a lone-round
optimization, dropped as soon as contenders join.

### What stays `Latest`

Everything that must see current state, and everything that has no earlier
in-transaction load to reuse: the `Locker`'s acquire / write-back / release, GC's
coordinator-driven releases (ADR-029), the general multi-key commit (no
eligibility pre-check), and every transaction body read. The `Freshness` seam is
general on `ObjectCache::read`, so any future caller that only needs a CAS seed
can opt in.

## Correctness

The claim is that `AllowStale` changes only _which bytes the first fold attempt
folds over_, never the precondition logic or any commit decision — so it cannot
cause a lost update, a double-apply, or a stale commit.

- **A stale cached shard self-corrects.** A fold over a stale snapshot produces a
  store whose `expected` version no longer matches the backend, so the CAS
  misses; the round then reloads (`Latest`) and re-folds on fresh bytes — the
  exact precondition-miss recovery ADR-028 already runs. The idempotent re-fold
  contract (ADR-028 contract 3) holds identically whether the first attempt read
  the cache or the backend.
- **Stale eligibility cannot commit, only re-run.** Between the read and the
  commit a concurrent writer may move the shard. With `AllowStale` the single-RW
  eligibility check may run on the cached (stale) snapshot and _pass_ a
  read-modify-write whose read was in fact superseded. It still cannot commit on
  outdated state: the version-conditional install CAS misses, the coordinator
  reloads fresh, re-folds, and finds the read superseded, so the fast path renews
  (`Wounded`). The lock CAS never landed, so no lock is held and the
  speculatively-written committed object is in no shard's `locked_by` — it cannot
  be help-forwarded and is an orphan GC reclaims (no lost update, no
  double-apply). A `Wounded` renew is a **transparent re-run** at the user level:
  the db retry loop treats it exactly like the full path's `Retry`, and the
  re-run's read (`Latest`) refreshes the cache so it converges. The only
  observable change is _which_ retry a superseded read takes: `Wounded` (renew,
  no lock held) when the stale snapshot passed the check, vs. the full path's
  in-place `Retry` (holding its locks, ADR-024) when the eligibility snapshot was
  fresh — whether it was fresh depends only on cache warmth. The ADR-024 "retry
  holding locks" guarantee is a full-path property, where a lock is actually held;
  a superseded fast-path read holds none.
- **No new CAS site, no new in-doubt case.** The coordinator's CAS sites, version
  conditions, and ADR-009 in-doubt recovery are untouched; `AllowStale` only
  chooses whether attempt zero revalidates the cache.

## Determinism (DST)

The backend op stream loses redundant `read_if_modified` ops but keeps the same
CAS/store shape and ordering, so it stays deterministic under the simulation
executor (ADR-008/013). A stale `AllowStale` snapshot that races a concurrent
writer produces a deterministic extra fold + CAS miss + reload, exercised by the
fuzzer. The retry flavour for a superseded single-RW read (`Wounded` vs `Retry`)
is a function of cache warmth, which is deterministic per executor; both are
transparent re-runs that converge, so neither the serializability / cycle oracles
nor the op-stream self-check are affected. The existing minimized corpus still
passes against the leaner op shape (no regeneration was required).

## Consequences

- **The ADR-028 `singleRMW` regression closes**, and when the read loaded the
  shard the commit's remaining load disappears too: a steady-state single
  read-write commits with the shard loaded once (its read) — never the two/three
  of before.
- **The object cache gains a general freshness seam.** "A cached load costs a
  revalidation" becomes "a cached load costs a revalidation _unless the caller
  accepts a first-attempt stale copy_", available to any future caller that only
  needs a CAS seed.
- **No transaction-scoped state.** The reuse rides the object cache the read
  already populated — no retained shard set, no reader-to-commit plumbing.
  `AllowStale` is a _pure optimization_: a miss degrades to a normal load, never
  to incorrectness, so it needs no consistency guarantee of its own.
- **The rare stale-reuse path does strictly more work** (a wasted fold + CAS miss
  - reload) than a cold load would, in exchange for removing a _guaranteed_
    backend read on the common path — the standard optimistic trade, and the same
    one ADR-028's reload-recover loop already makes.
- **Deduplicating the decode is a separate concern.** `AllowStale` removes the
  backend round-trip but each reuse still decodes the cached bytes; caching
  decoded objects would remove that too, and is left to a future change.
