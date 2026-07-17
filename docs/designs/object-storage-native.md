# Algorithm v2: object-storage-native layout

## Status

**Shipped.** The object-storage-native layout is the current on-disk format and
commit protocol. This document is its design overview and decision index: the
umbrella decision is [ADR-016](../adr/016-object-storage-native-layout.md) and each
sub-decision has its own ADR (see [Decision records](#decision-records)). It is
the living, mutable companion to the frozen ADRs and to the user-facing
[architecture.md](../architecture.md). Remaining gaps are tracked as
[Future improvements](#future-improvements) — ordinary work on top of the current
design, not a follow-on version.

For the motivation (the S3 metadata-update problem) and the umbrella decision,
see [ADR-016](../adr/016-object-storage-native-layout.md).

## Design at a glance

MVCC for values + S2PL for isolation, with a fixed set of `C` shard objects per
collection acting as a content-CAS coordination directory in place of per-object
tags. No object tags anywhere.

- **Objects** —
  - _Collection root_ (`_i`): existence + constant shard count + subcollection
    list. The membership-coordination point — create/delete lock it; listing
    OCC-validates it (read-lock under contention).
  - _Shard_ (`_s/<i>`, `C` per collection): lock table + MVCC version index +
    per-shard key directory; the unit of CAS (`If-Match`). Read/write of an
    existing key touches only its shard.
  - _Transaction_ (`_t/<ss>/<txid>`): unified; pending (small: lease + lock
    intentions) → committed (fat: value map) → aborted. The first two encoded
    txid symbols select one of 4,096 deterministic listing shards.
- **Protocol** — execute → lock (one shard GET + one CAS per shard) → validate
  reads (re-resolve effective writers in `Algo`, post-lock) → commit (CAS the
  transaction object to committed, attaching values) → async per-shard write-back
  (publish current-writer pointers + release locks).
- **Membership** — key create/delete write-lock the collection root (phantom
  prevention) and CAS the key's shard; listing OCC-validates the root version and
  enumerates, falling back to a root read lock under contention. Subcollections
  are listed from the root.
- **Reads** — shard (conditional GET) → current-writer txid → value from the
  immutable transaction object (cacheable indefinitely). Read/write of an
  existing key needs no root lock; read-only stays lock-free.
- **GC** — mark-sweep; live set = `current-writer ∪ locked-by` across shards.
- **Backend trait** — `read / read_if_modified / write / write_if /
write_if_not_exists / delete / list` (seven methods; tags, nonce, `set_tags_if`,
  `get_metadata`, `delete_if` all gone).

## Design decisions

Rationale lives in [ADR-016](../adr/016-object-storage-native-layout.md) and the
per-decision ADRs.

- Full redesign; format **replaced wholesale** (S3 + GCS); Go on-disk
  compatibility dropped.
- Values live **only in unified transaction objects**.
- **Fixed compile-time `C`** shards per collection (`C = 1024`, not
  configurable); split-resharding is a [future improvement](#future-improvements).
- **Mark-sweep GC**; the explicit liveness counter and compaction are
  [future improvements](#future-improvements).

## Current limitations

- Collections larger than `C × keys-per-shard` (needs split-resharding, a
  [future improvement](#future-improvements)).
- **Within-shard false sharing**: transactions sharing a shard serialize on its
  CAS (~`1 / RTT` per shard). The deliberate trade for removing S3 value-rewrite
  amplification; `C` is the write-parallelism knob. Mitigated by the
  **shard-mutation coordinator** ([ADR-028](../adr/028-shard-mutation-coordinator.md),
  generalizing [ADR-025](../adr/025-dedup-shard-lock-acquisition.md)/026): compatible
  contenders (disjoint keys or shared reads) merge into one owner-driven GET+CAS
  fold instead of racing separate ones.
- No compaction (a cold key can pin a fat transaction blob of otherwise-dead
  values); no explicit GC counter (full mark-sweep only).

## Engine overview

The `glassdb-trans` engine is verified end-to-end on the in-memory backend:
single-key get/put/delete, multi-key transactions, optimistic list, the
serializability stress tests (concurrent counter, cross-shard transfer
invariant, same-shard merge, wound-wait progress), and **crash recovery** (an
expired-lease holder's shard + root locks are reclaimed by a younger writer).

The data layer lives in `glassdb-storage`: `Shard` (ADR-017), `CollectionRoot`
(ADR-018), and the `txobject` codec over `TransactionLog` (ADR-019), each
golden-anchored.

ADR-016…021 are behavior-complete: locking (with read validation lifted into
`Algo`, post-lock, ADR-024), wound-wait with
**lease expiry** (crash recovery), the commit flip, and synchronous write-back,
all over content CAS on shards / root / transaction objects, with priority
preserved across retries (`TxId::renew`).

Lock acquisition keeps ADR-020's two modes, now with "wait" realised as
**hold-and-wait** ([ADR-024](../adr/024-hold-and-wait-conflict-resolution.md),
implemented): a transaction keeps the locks it holds and waits for a conflicting
holder it cannot wound, bounded by a deadlock timeout that escalates to the
serial order, with a load-bearing lease refresher keeping its held locks alive.

- **Parallel by default** — all touched shards are locked concurrently (then the
  root last). An older transaction wounds a younger holder and proceeds; a
  younger-or-equal one **waits** for the holder to finalize, then re-resolves and
  proceeds (committed → help-forward, aborted → drop). Reads are validated in
  `Algo` **after** locking; a read whose value moved before it was locked re-runs
  the body **holding its locks** (`Retry`), not via a released-and-renewed
  restart. Deadlock-/livelock-free for distinct priorities.
- **Deadlock timeout → serial sorted fallback (ADR-020/024)** — a parallel wait
  that exceeds `MAX_DEADLOCK_TIMEOUT` (5s) makes `Algo` release the locks
  (`Locker::release_locks`) and re-acquire them one shard at a time in ascending
  index order (`SERIAL_FALLBACK_AFTER` failed attempts is a backstop trigger).
  This happens **inside `Algo::commit`** — an internal loop over the locking step
  that keeps the **same id** and re-runs no user body (v1's `serial_validate`);
  the `LockTimeout` signal never reaches the `db.rs` retry loop. Two
  _equal-priority_ transactions can wait-cycle on the parallel path (each holds a
  different shard); under the single global lock order they instead queue on the
  lowest shard, where first-CAS-wins picks a winner that always finishes.
  Wound-wait uses **no prefix tiebreak** (it would flip under `renew`); equal
  priorities are resolved only by this fallback.
- **CAS contention → same-id retry (ADR-020/024)** — losing a shard/root CAS race
  (the bounded retry budget exhausted under churn) is resolved by the same
  internal loop: release the partial locks and re-acquire under the **same id**
  after a backoff, escalating to the serial order if it persists. A lost race no
  longer aborts-renews-and-re-runs (`Wounded`); the executed body is preserved.
  Body re-runs are limited to a **stale read** (`Retry`, holding locks) and a
  **genuine wound** (renew + re-run, the only case whose id is dead).

The lease refresher is now **load-bearing (ADR-021/024)**: under hold-and-wait a
live transaction can hold locks far longer than `PENDING_TX_TIMEOUT`, so the
background refresher keeps its lease alive. Its first write _creates_ the pending
object (create-if-absent), so it can never resurrect itself over a wound; expiry
combines an absolute (skew-padded) lease check with an observer-relative
(no-skew) no-progress check.

**GC ([ADR-022](../adr/022-garbage-collection-mark-sweep.md))** is implemented: a
candidate-driven reverse mark-sweep whose live set is `current-writer ∪ locked-by`
across shards plus `membership-locked-by` on roots, with the ADR-021 lease as the
safety horizon. A `locked_by` entry pointing at a _missing_ object is given the
`handle_unknown_tx` grace (load-bearing under hold-and-wait,
[ADR-024](../adr/024-hold-and-wait-conflict-resolution.md), since a live tx
materializes its pending object lazily); GC respects that grace and never deletes
a still-referenced or within-horizon object.

Caching and batching are in place: the `ObjectCache` / `ValueCache` (ADR-023),
asynchronous background write-back, and dedup-batched CAS on acquisition,
release, and write-back (ADR-025/026).

The **single read-write fast path** ([ADR-027](../adr/027-single-rw-parallel-lock-publish.md))
commits a lone overwrite of an existing key with two **parallel** writes — the
committed transaction object and one shard CAS that installs a write lock — then
an asynchronous write-back converts the lock to a `current_writer` pointer.
Ineligible transactions (create/delete, multi-key, cross-key reads, stale reads,
locked entries) fall back to the full locked path.

All shard/root entry mutations flow through **one shard-mutation coordinator**
([ADR-028](../adr/028-shard-mutation-coordinator.md)): a single per-object mechanism
that loads once, folds the round's installed resolvers (acquire, commit-install,
write-back, release) in wound-wait order, CASes once, and recovers
precondition/in-doubt by reload. Policy (wound-wait, help-forwarding,
hold-and-wait, the parallel↔serial strategy) lives in the resolvers `Locker` and
`Algo` install; the coordinator is ignorant of locks and transaction ids. GC's
lock reclamation flows through the same coordinator
([ADR-029](../adr/029-gc-through-shard-coordinator.md)), so the invariant holds with
no exceptions and vestigial-entry pruning becomes a fold property. A single
read-write transaction reuses its cached shard load across the round via an
`AllowStale` freshness flag ([ADR-030](../adr/030-seed-shard-loads.md)), so a
steady-state commit loads its shard only once.

The **cutover** is done: the DST oracles run on this engine, the legacy
tag-based layout is retired, and the `Backend` trait is slimmed (ADR-023).

## Decision records

Every design decision is captured in its own ADR.

- **[ADR-016](../adr/016-object-storage-native-layout.md) — Object-storage-native
  layout.** ✅ Written. The umbrella decision: move coordination state from tags
  into content; MVCC + S2PL on a sharded directory; the three-object model;
  wholesale format replacement.
- **[ADR-017](../adr/017-shard-object.md) — Shard object: model, mapping,
  encoding.** ✅ Written & implemented. The shard's data model (per-key lock
  state + current-writer + tombstone), key→shard mapping (`C = 1024`, FNV-1a),
  the `_s` path, the protobuf encoding, and the pure read-side lookup.
  Deliberately inert (no mutation policy, no I/O) so it can be implemented and
  unit-tested in isolation — the first verifiable increment. Landed in
  `glassdb-data::shard` / `paths` and `glassdb-storage::shard`.
- **[ADR-018](../adr/018-collection-root-membership.md) — Collection root &
  membership.** ✅ Written & implemented (data layer + engine). Collection root
  (`_i`, `CollectionRoot` protobuf:
  shard count + subcollection list + membership lock) as the membership-
  coordination point: create/delete take its write lock, listing OCC-validates
  its version (read-lock fallback). Key directory stays sharded; the root version
  summarizes the whole cross-shard membership read set. Atomic sequencing deferred
  to ADR-020.
- **[ADR-019](../adr/019-unified-transaction-object.md) — Values in unified
  transaction objects.** ✅ Written & implemented (data layer + engine). Values
  live only in the `_t/<ss>/<txid>` object
  (shards point via `current_writer`); the object is unified (status + values, no
  split), with a pending (lease + lock intentions) → committed (fat value map) →
  aborted lifecycle whose commit point is the single flip-to-committed CAS.
  Encoding evolves `TransactionLog`. Sequencing deferred to ADR-020, lease to
  ADR-021.
- **[ADR-020](../adr/020-commit-write-back-protocol.md) — Commit & write-back
  protocol.** ✅ Written & implemented. Five
  phases (execute → prepare pending object →
  validate-and-lock as one RMW CAS per shard → commit flip CAS → idempotent
  per-shard write-back). Shard-CAS contention vs lock conflict; effective-current-
  writer / help-forward resolution; cross-shard non-atomicity gated on the commit
  object; in-doubt parity at every CAS site; read-only fast path (the single-RW
  fast path is **deferred to a follow-up** — see Group B below).
  Lock acquisition is **parallel by default** with a **serial sorted-by-index
  fallback** after `SERIAL_FALLBACK_AFTER` failed attempts; both abort-and-retry
  rather than block (the serial path's single global lock order is what gives
  equal-priority transactions progress, with no prefix tiebreak). Write-back is
  still synchronous; GC → ADR-022.
- **[ADR-021](../adr/021-wound-wait-leases-shard.md) — Wound-wait & leases at shard
  granularity.** ✅ Written & implemented (wound-wait + lease expiry / crash
  recovery). The lease is the transaction object's existing
  `timestamp` (no new field): last-refresh while pending, commit-time once
  committed, expired past `PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW`. Reclaiming a
  conflicting `locked-by` entry combines wound-wait priority with the lease (wound
  if older _or_ expired), uniformly for shard entries and the root membership lock.
  Discovery is lazy via `locked-by` (no pending registry); reads never consult the
  lease; abort CAS keeps ADR-009 in-doubt parity. Re-frames
  [ADR-002](../adr/002-wound-wait-locking.md) for the new layout. Two MVP-era
  simplifications are both made load-bearing again by **hold-and-wait
  ([ADR-024](../adr/024-hold-and-wait-conflict-resolution.md))**: the **background
  refresher** returns (a transaction can now block while holding locks), and the
  **`handle_unknown_tx` grace period** for a missing object becomes load-bearing in
  the engine (a live tx materializes its pending object lazily); GC merely respects
  that grace (ADR-022).
- **[ADR-022](../adr/022-garbage-collection-mark-sweep.md) — Garbage collection by
  mark-sweep.** ✅ Written & implemented. Liveness = reachability: live set =
  `current-writer ∪ locked-by` across
  shards plus `membership-locked-by` on roots, with the ADR-021 lease as the
  safety horizon (a recent pending object found unreferenced is kept — its lock may
  post-date the check, and under ADR-024 the object is materialized lazily after its
  locks are taken). Rather than a database-wide forward mark (infeasible at scale),
  GC is **candidate-driven and reverse**: each transaction object records its own
  back-references (`locks ∪ writes`, plus the `prev_writer` an overwrite
  supersedes), so GC checks a _batch_ of `_t/` candidates against only the
  shards/root they name, deletes those past the horizon with no remaining reference,
  and prunes their own stale locks. Paged, shuffled walks over the 4,096
  `_t/<ss>/` prefixes make the candidate set complete (no forward scan), with a
  bounded number of listing requests per GC cycle; a committed object is freed
  only once proven unreferenced, while a finalized valueless object is reclaimed
  past the horizon regardless (a residual stale lock degrades to a missing-object
  reference the `handle_unknown_tx` grace absorbs). Pruning/deletion touch
  **only finalized** transactions: a dead _pending_ one is first **force-aborted**
  (`pending → aborted` CAS, the ADR-021 reclaim) — which loses the race to a
  slow-but-live owner's commit — then has its known locks released, then is deleted,
  so an observing client never sees a lock vanish from under a live owner. An
  **aborted object is a tombstone**, deleted only a full safety lease _after the
  abort_ (the abort stamps a fresh `timestamp`), so a stuck owner cannot be
  resurrected by its own create-if-absent refresher (ADR-024) and a late lock it
  installs resolves against a present aborted object rather than a missing one.
  Proactive stale-lock / empty-entry pruning
  (the
  uncontended-dead-lock case ADR-021 left to GC); subcollection teardown
  (ADR-018). The commit→write-back gap is covered because a committed object stays
  referenced via `locked-by` until write-back. Explicit liveness counter and
  compaction deferred.
- **[ADR-023](../adr/023-slimmed-backend-trait.md) — Slimmed `Backend` trait.** ✅
  Written & implemented. The reduced seven-method surface (`read`, `read_if_modified`,
  `write`, `write_if`, `write_if_not_exists`, `delete`, `list`); removal of tags /
  nonce / `set_tags_if` / `get_metadata` / `delete_if`; content CAS as the only
  coordination primitive. `read_if_modified` is **re-keyed from the writer tag to
  the object version/ETag** so the retained `Global` cache can revalidate the
  tagless coordination objects with a conditional GET (Group E point 2). `Global`
  and `Locker` are retained and adapted, not deleted. Relates to
  [ADR-009](../adr/009-in-doubt-conditional-writes.md) for in-doubt parity at the
  new CAS sites.
- **[ADR-024](../adr/024-hold-and-wait-conflict-resolution.md) — Hold-and-wait
  conflict resolution.** ✅ Implemented. Reinstates v1's **hold-and-wait**: a
  younger-or-equal transaction **waits** (polling via `wait_for_tx`) for a
  conflicting holder while **keeping its locks and transaction object** instead
  of aborting-and-retrying, with the lease refresher (ADR-021) made load-bearing.
  The pending object **stays lazily created** (no upfront prepare); a held lock is
  bridged by the `handle_unknown_tx` grace period until the refresher
  materializes it (create-if-absent, so a wound is never resurrected). A
  **deadlock timeout** (`MAX_DEADLOCK_TIMEOUT` = 5s) bounds the wait and is broken
  **inside `Algo`** (v1's `serial_validate`): on timeout it releases its locks
  (`Locker::release_locks`) and re-acquires them in the serial sorted-by-path
  order under the **same id**, re-running no body and never surfacing
  `LockTimeout` to the db retry loop; older-wounds-younger and ADR-009 in-doubt
  parity are unchanged. **CAS contention** is handled by the same internal loop —
  release the partial locks and re-acquire under the same id after a backoff — so
  a lost race no longer renews-and-re-runs (`Wounded`); body re-runs are limited
  to a stale read found when `Algo` validates after locking (re-run **holding its
  locks**, `Retry`) and a genuine wound (renew + re-run). Supersedes the MVP-only
  release-and-retry /
  no-refresher deviations of ADR-020/021 and **refines ADR-021's expiry
  predicate**: the observer-relative grace (timestamp-advance / object-appearance
  within `PENDING_TX_TIMEOUT`) owes no `MAX_CLOCK_SKEW`; only the absolute
  `timestamp`-vs-`now` check does. Activated the dormant `wait_for_tx` /
  `refresh_pending` machinery and added `Locker::release_locks` for the serial
  fallback's release step.
- **[ADR-025](../adr/025-dedup-shard-lock-acquisition.md) — Deduplicated
  shard-batched lock acquisition.** ✅ Implemented. Re-introduces v1's request
  **deduplication** in the `Locker`, re-keyed from per-key objects onto the v2
  CAS objects (shards + collection roots) via the dormant
  `glassdb_concurr::Dedup`. `Locker::lock_shard` / `lock_root` route through one
  `Dedup` keyed on the object path: several transactions contending the same
  shard **merge** (disjoint keys or shared reads) so one owner loads it once,
  installs every compatible contender's lock, and CASes **once** — N GET+CAS
  round-trips collapse to one. Same-key exclusive contenders (and root membership
  requests) queue and resolve by the unchanged wound-wait / hold-and-wait. A
  per-transaction outcome side channel carries the heterogeneous
  `Locked{membership}` / `Wait` / `Conflict` results (`Dedup` fans out one shared
  result). Write-back / release stay direct CAS. Confined **beneath** the
  per-object lock step, so the ADR-020/021/024 protocol, priorities, serial
  fallback, and ADR-009 in-doubt recovery are all unchanged; re-activates the
  dormant `Dedup`, `Locker::close`, and `Locker::dedup_snapshot`.
- **[ADR-026](../adr/026-dedup-shard-release-write-back.md) — Deduplicated shard
  release and write-back.** ✅ Implemented (follow-up to ADR-025). Extends the
  ADR-025 `Dedup` to also batch **write-back** and
  **release** on the same object path, so N committing/aborting transactions on
  one shard collapse from N GET+CAS to one — closing the release-side half of the
  within-shard false sharing that ADR-025 removed from acquisition. Releases are
  the most mergeable operations (they never lock-conflict; write-back sets only
  the committing tx's own monotonic pointer), so a release-only request is
  reorderable — the analog of marking unlocks reorderable. Stays beneath
  the per-object step, leaving the ADR-020/021/024/009 semantics unchanged.
- **[ADR-027](../adr/027-single-rw-parallel-lock-publish.md) — Parallel single
  read-write commit via a lock.** ✅ Implemented. Supersedes ADR-020's single
  read-write fast path: it publishes a **write lock** instead of a bare
  `current_writer` pointer, issues its committed-object write and its shard lock
  install **concurrently** (~1 RTT on the critical path), and converts the lock
  to the pointer asynchronously. Committed iff both land; a live-pending holder,
  a create/delete, a missing key, or a superseded read makes it ineligible and it
  falls back to the full locked path under the same id. It now holds a lock during
  the pre-commit window, so it participates in wound-wait and lease/GC bookkeeping;
  one irreducible in-doubt (an `Unavailable` install CAS whose entry then moved)
  surfaces as `InDoubt`.
- **[ADR-028](../adr/028-shard-mutation-coordinator.md) — Unified shard-mutation
  coordinator.** ✅ Implemented. Generalizes ADR-025/026's deduplication into a
  single per-object **mechanism** over which callers install **resolvers** that
  encode policy: load once, fold the round's resolvers in wound-wait order, CAS
  once, recover by reload. Every shard/root mutation — acquire, commit-install
  (ADR-027's install), write-back, release — flows through one coordinator, so the
  last racing CAS (the fast-path install) stops racing. ADR-025's merge predicate
  and ADR-027's bespoke reclassify loop dissolve into the fold; the concurrency
  semantics of ADR-002/021/024 are unchanged.
- **[ADR-029](../adr/029-gc-through-shard-coordinator.md) — GC through the
  shard-mutation coordinator.** ✅ Implemented. Closes ADR-028's one exception:
  GC's lock reclamation stops issuing its own CAS and instead calls the `Locker`'s
  per-object unlock methods, which route through the coordinator. Vestigial-entry
  pruning becomes a **fold property** (the coordinator drops any entry left with
  no holder and no `current_writer`), so every mutation path tidies up in the same
  CAS that clears the last holder. ADR-022's GC policy is unchanged.
- **[ADR-030](../adr/030-seed-shard-loads.md) — Reusing cached shard loads across a
  transaction's coordinator rounds.** ✅ Implemented. Adds an `AllowStale`
  freshness flag to the object-cache read path so the single read-write fast path
  reuses a shard the transaction already cached (its body read, or the eligibility
  pre-check) for its first fold attempt with **no backend op**, restoring the
  single-load commit ADR-028 had split in two. A stale snapshot self-corrects: the
  version-conditional CAS misses, the round reloads `Latest` and re-folds — a pure
  optimization with no new CAS site or in-doubt case.

## Design questions resolved

An index of the questions the redesign had to answer and where each one landed.
The few that remain open are collected under
[Future improvements](#future-improvements).

Group A — layout & encoding:

- [x] Concrete value of the constant `C` — `1024` (ADR-017).
- [x] Key→shard hash function — FNV-1a over raw key bytes, masked to `C`
      (ADR-017).
- [x] On-disk encoding of shards — protobuf, entries sorted by key, golden-
      anchored (ADR-017).
- [x] Path type marker for shards — `_s` (ADR-017).
- [x] On-disk encoding of the unified transaction object (pending vs committed
      forms; value-map representation; evolve the `glassdb-proto` `TransactionLog`
      message) — ADR-019; lease field pinned by ADR-021.
- [x] Collection-root format: shard count, subcollection list, and membership
      lock/version state; how the shard count is recorded/validated (ADR-018).

Group B — protocol details:

- [x] Exact validate+lock CAS algorithm for multiple keys per shard: one RMW CAS
      per shard, merge-on-retry, retry-with-locks-held (ADR-020).
- [x] Resolution of the "effective current writer" when a committed-but-not-
      written-back write-lock holder exists (relocated `validate_locked_read` +
      help-forward) (ADR-020).
- [x] Deadlock/livelock fallback: parallel locking by default; a wait that
      exceeds `MAX_DEADLOCK_TIMEOUT` (or `SERIAL_FALLBACK_AFTER` failed attempts)
      escalates to serial sorted-by-shard-index acquisition (ADR-020/024),
      **inside `Algo`**: it releases its locks and re-acquires them under the same
      id (no renew, no body re-run, no error surfaced — v1's `serial_validate`).
      Under hold-and-wait a younger-or-equal transaction **waits** holding its
      locks rather than aborting; the serial path's single global lock order
      (first-CAS-wins on the lowest shard) is what gives equal-priority
      transactions progress. Wound-wait uses **no prefix tiebreak** (it would flip
      under `TxId::renew` and livelock); priority is preserved on retry.
      Regression-tested for the wound decision, the wait-then-proceed paths, the
      deadlock-timeout escalation, and cross-shard liveness.
- [x] Lease refresh cadence and the expiry/wound CAS sequence; reuse of existing
      timeout constants — lease is the object `timestamp`, reclaim if older-or-
      expired past `PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW` (ADR-021). The background
      refresher is **load-bearing** under hold-and-wait (ADR-024): it lazily
      _creates_ the pending object (create-if-absent, wound-safe) and CAS-bumps the
      `timestamp` every `PENDING_TX_TIMEOUT / 2`. Expiry combines the absolute
      (skew-padded) check with an observer-relative (no-skew) no-progress check.
- [x] In-doubt (`Unavailable`) handling parity at the new CAS sites (pending
      create, shard lock CAS, commit CAS, write-back CAS) — ADR-009 carries over
      (ADR-020). The single-RW fast path's lock CAS is a further in-doubt site
      (see below): a lost ack resolves by reading the shard back, leaving one
      irreducible in-doubt (a fast follow-on writer moved the entry, ADR-027).
- [x] Read-only fast-path shape in the new layout (ADR-020).
- [x] Single-RW fast path (ADR-020, revised by ADR-027). A read-write transaction
      that overwrites a single existing key commits with two **parallel** writes —
      the committed transaction object and one shard CAS that installs a write
      lock — then an **asynchronous** write-back converts the lock to a
      `current_writer` pointer. It commits iff both writes land (object committed
      and lock in the chain); ineligible transactions (create/delete, multi-key,
      cross-key reads, stale reads, locked entries) fall back to the full locked +
      logged path.

Group C — listing, snapshots, phantoms (the collection root is the coordination
point; see ADR-018):

- [x] Where the key directory physically lives: per-shard (listing reads the `C`
      shards and unions them), with the root version as the single OCC token for
      membership changes (ADR-018).
- [x] `list` / iteration: OCC-validate the root version, enumerate, fall back to
      a root read lock under contention; cross-shard snapshot consistency via the
      root version summarizing the membership read set (ADR-018).
- [x] Create/delete: write-lock the root (phantom prevention) + CAS the key's
      shard; every membership change writes the root, so its version bumps to
      invalidate concurrent listers (ADR-018).
- [x] Subcollection list in the root: authoritative directory, OCC-listed; add/
      remove under the root membership write lock (ADR-018; teardown → ADR-022).

Group D — GC & lifecycle:

- [x] GC trigger cadence, batching, and bounds (LIST/read cost) — a background
      loop on the `Clock` / `Background` seam whose steady-state cost is
      proportional to _garbage_, not database size: a **candidate-driven reverse**
      check of a `_t/` batch against only the shards/root each candidate records
      (cache-amortized), batched deletes, and **no** database-wide scan — paged,
      shuffled walks over deterministic `_t/<ss>/` prefixes make the candidate
      set complete while bounding listing calls per cycle. The write-back
      `schedule_tx_cleanup` hook becomes the primary **candidate feed** (the
      `prev_writer` an overwrite just superseded), not a delete queue (ADR-022).
- [x] Safety horizon to avoid sweeping in-flight transactions: reuse ADR-021's
      lease expiry (`now > timestamp + PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW`) as
      the sweep horizon, so a recent pending object (or in-doubt create) is never
      swept even when the non-atomic mark has not yet observed its lock — which,
      under ADR-024's lazy materialization, post-dates the lock (ADR-022).
- [x] Compaction and the explicit liveness-counter object are specified as
      deferred (see [Future improvements](#future-improvements)); the engine does
      full mark-sweep of whole objects (ADR-022).

Group E — backends:

- [x] Final `Backend` trait signature and error semantics on the reduced surface
      — [ADR-023](../adr/023-slimmed-backend-trait.md) implemented: seven methods
      (`read`, `read_if_modified`, `write`, `write_if`, `write_if_not_exists`,
      `delete`, `list`); content CAS only; ADR-009 in-doubt parity
      (`glassdb-backend`). ADR-035 refines `list` to one recursive prefix page
      with an opaque provider cursor, positive limit, and distinct invalid-cursor
      error.
- [x] Cache tagless coordination objects via version/ETag-conditional reads.
      Today `ShardStore` full-fetches every shard/root read (the writer-tag
      `read_if_modified` is unusable on these tagless objects).
- [x] S3 mapping — `S3Backend` on the slimmed trait: `If-Match` /
      `If-None-Match` conditional writes, ETag versions, no nonce/tags and
      `delete_if` removed (`glassdb-backend-s3`).
- [x] GCS mapping — `GcsBackend` on the slimmed trait: content CAS via
      generation `ifGenerationMatch` (and `ifGenerationNotMatch` for
      create/`read_if_modified`); no metadata patch (`glassdb-backend-gcs`).
- [x] In-memory backend semantics for the new trait (`MemoryBackend`) plus DST
      fault injection (the `fault` middleware).

Group F — testing & migration:

- [x] Re-point DST oracles (serializability, cycle ring) at the new layout — the
      `glassdb` DB runs on this engine, so `concurrent_sim` / `cycle_sim` /
      `fuzz_corpus` run against the sharded layout.
- [x] Regenerate golden vectors and `RecordingBackend` byte-stream expectations —
      golden encodings anchor the shard / root / transaction-object formats
      and `RecordingBackend` records the slimmed-trait op stream.
- [x] Update the design docs — `architecture.md` and this overview describe the
      shipped layout; `PORTING.md` is retired (the project is independent of the
      Go original).

Group G — open questions:

- [x] Does the unified transaction object ever need a `list`-discoverable pending
      registry, or are shard `locked-by` entries sufficient to discover all live
      transactions for GC and recovery? — No registry; `locked-by` entries suffice,
      with lazy contention-driven reclamation and GC for the uncontended remainder
      (ADR-021).

## Future improvements

Work deliberately left out of the current design, tracked as ordinary follow-ups
rather than a follow-on version:

- **Split-resharding.** `C` is a fixed compile-time constant, so a collection is
  capped at `C × keys-per-shard`; growing past that needs dynamic resharding.
  Now designed in [ADR-031](../adr/031-dynamic-range-sharding.md) /
  [dynamic-range-sharding.md](dynamic-range-sharding.md): a dynamic,
  order-preserving, range-partitioned directory (B-link tree) that also adds
  sorted/range listing and per-range membership; supersedes the fixed hash
  mapping of ADR-016/017/018.
- **Compaction / bounded history.** A cold key can pin a fat transaction blob of
  otherwise-dead values; there is no compaction of fragmented blobs. If the
  proposed [snapshot-read design](snapshot-reads.md) is accepted, it replaces
  this value placement with independently reclaimable per-key values, indexed
  epoch history, and history-aware GC.
- **Explicit liveness counter.** GC is full reverse mark-sweep of whole objects;
  an explicit per-object liveness counter could make reclamation cheaper.
- **Benchmark the false-sharing knee.** Locate the throughput knee vs `C` and
  confirm the S3 write-amplification win with a documented benchmark plan.
- **Hot-shard S3 PUT ceiling.** Decide whether a hot shard hitting S3's
  per-prefix PUT rate limit is an accepted, documented limit or whether shard
  paths should be spread to mitigate it.
- **Root creation and catalog visibility.** Proposed
  [ADR-041](../adr/041-epoch-versioned-collection-catalog.md) precreates a
  physical root bound to an incarnation, then atomically publishes collection
  existence and parent membership; an aborted reusable root is CAS-compacted to
  a tombstone rather than unconditionally deleted.
