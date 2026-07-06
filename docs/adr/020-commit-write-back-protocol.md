# ADR-020: Commit and write-back protocol

## Status

Accepted — implemented. Shard locking runs **in parallel by default** and falls
back to **serial sorted-by-path acquisition** after a few
failed attempts (see [Deadlock prevention](#deadlock-prevention-and-the-serial-fallback)).

One deliberate simplification remains, **MVP-only**:

- Write-back is **synchronous** (not yet async/batched; GC and caching are
  ADR-022/perf follow-ups).

The conflict-resolution model was **release-and-retry** in the MVP; it is now
back to **hold-and-wait** (locks preserved across waits, deadlock-timeout →
serial fallback, load-bearing lease refresh), implemented per
[ADR-024](024-hold-and-wait-conflict-resolution.md). The MVP-only realisation
notes below are retained for history but are **superseded by ADR-024**; see
[Deadlock prevention](#deadlock-prevention-and-the-serial-fallback).

The **single read-write fast path** decision below (two sequential writes: the
committed object then a `current_writer` pointer CAS, no lock, no write-back) is
**superseded by [ADR-027](027-single-rw-parallel-lock-publish.md)**, which
publishes a write lock instead of the pointer so the two writes issue in
parallel, followed by an asynchronous write-back. The rest of this ADR is
unchanged.

## Context

[ADR-017](017-shard-object.md), [ADR-018](018-collection-root-membership.md), and
[ADR-019](019-unified-transaction-object.md) define the static pieces of the v2
layout — the shard (lock table + MVCC index + key directory, the CAS unit), the
collection root (membership coordination), and the unified transaction object
(values + status, with the commit point at its flip to `committed`). This ADR
defines the **protocol that drives them**: how a transaction validates reads,
acquires locks, commits, and publishes its writes.

The reference is the v1 algorithm (`crates/glassdb-trans/src/algo.rs`):
optimistic read validation, parallel lock acquisition with wound-wait
([ADR-002](002-wound-wait-locking.md)), a serial sorted-locking fallback for
suspected deadlocks, a logged commit point, and asynchronous lock release /
write-back. The isolation level (strict serializable) and the wound-wait rule are
unchanged. What changes is the **granularity and the medium**: locks move from
per-key object _tags_ (mutated with `set_tags_if`) to entries inside per-shard
objects mutated with **content CAS** (`write_if`), and values are published as a
`current_writer` pointer in the shard rather than written into a per-key object.

This is the ADR that makes ADR-017–019 behavior-complete. Lease/expiry mechanics
are ADR-021; reclamation is ADR-022; the slimmed backend trait is ADR-023.

## Decision

A read-write transaction runs five phases. Phases 1–4 are synchronous (the commit
critical path); phase 5 is asynchronous and idempotent.

1. **Execute** — run the body, collecting a read set (each key with the
   `current_writer` it was read at) and a write set (each key with its value or a
   delete), via the read path. No coordination.
2. **Prepare** — create the pending transaction object
   (`write_if_not_exists` of `_t/<txid>`: lease + lock intentions, ADR-019), so
   any lock the transaction takes is resolvable by peers to a live transaction.
3. **Validate-and-lock** — per shard (and the collection root when needed), one
   read-modify-write CAS validates reads and installs locks together.
4. **Commit** — one CAS flips the transaction object to `committed` with its
   value map. This is the commit point.
5. **Write-back** — asynchronously, per shard, publish `current_writer`
   pointers / tombstones and release locks; then schedule the transaction object
   for GC once unreferenced.

### Validate-and-lock: one read-modify-write CAS per shard

The transaction groups its accessed keys by shard. For each shard it touches it
does a single GET + single CAS:

1. **GET** the shard (cached / conditional).
2. For each of _its_ keys in that shard:
   - **Validate the read**: the entry's `current_writer` must equal the version
     the transaction read; a key read as absent must still be absent or
     tombstoned. A mismatch is a conflict (refresh the read and retry).
   - **Apply wound-wait** against any conflicting holder of the entry's lock: if
     the transaction is older it **wounds** the holder (durably aborts that
     transaction's object, pending → aborted CAS) and takes the lock; if younger
     it **waits**. Only the contended _entry_ matters — different keys in the same
     shard do not conflict.
   - **Stage the lock** on the entry: read keys add the txid to `locked_by`
     (shared); written keys set `write` (existing) or `create` (absent) with the
     txid as sole holder.
3. **CAS** the shard (`write_if` on its version) with all staged changes for the
   transaction's keys at once.

The CAS is a **read-modify-write that preserves entries the transaction does not
own**: on a `Precondition` (another transaction mutated the shard) it re-GETs,
re-applies only its own entry changes, and retries (bounded). This is the key
distinction from v1's per-key locking: **shard-CAS contention is not lock
conflict**. Two transactions writing _different_ keys in one shard do not
lock-conflict — they only serialize on the shard's CAS and both merge in; only
two transactions contending the _same_ key invoke wound-wait. (CAS contention on
a hot shard is the false-sharing cost from [ADR-016](016-object-storage-native-layout.md);
`C` is the knob.) Batching all of a shard's keys into one CAS is the efficiency
win the sharded directory buys over per-key locking.

Reads are validated against the _same shard version_ the CAS commits, so within a
shard validate-and-lock is atomic. Across shards, S2PL holds: the transaction
acquires every lock before committing. A conflict discovered while locking a later
shard triggers a retry that **keeps the locks already held** (refresh the stale
read, re-validate), as in v1; locks are dropped only on the serial fallback or on
abort.

### Resolving the effective current writer

When validating or reading a key whose entry is **write/create-locked** by
another transaction `L`, the reader cannot use `current_writer` blindly — `L` may
have committed but not yet written back. It resolves `L`'s status from `L`'s
transaction object (this is v1's `validate_locked_read`, relocated):

- `L` **committed** → `L` is the effective current writer; its value (from `L`'s
  object) is current. A read that read the _old_ `current_writer` is stale →
  retry. The reader may **help-forward** by performing `L`'s shard write-back, but
  is not required to.
- `L` **pending** → not yet effective; the effective writer is the entry's
  `current_writer` (the last committed value).
- `L` **aborted** → same as pending: the entry's `current_writer` stands.

### Commit: the single flip CAS

With every lock held and every read validated, the transaction CASes its pending
object to `committed`, embedding the full value map (ADR-019). This single CAS is
the commit point and linearization point. If it fails because the object is
already `aborted`, the transaction was wounded or reclaimed → abort. Because the
flip only succeeds from `pending`, **a committed transaction can never be
wounded**: the commit is the point of no return.

### Write-back: asynchronous, per-shard, idempotent

After commit, for each shard the transaction locked, a single CAS:

- sets `current_writer = txid` for the keys it wrote (publishing the new MVCC
  pointer), sets `deleted` for its deletes, and
- releases the transaction's locks (drops it from `locked_by` / clears the entry
  lock).

Each write-back CAS is **idempotent**: its target state (`current_writer = txid`,
lock released) is the same however many times it runs, so a retry, a crash-resume,
or a peer's help-forward all converge. Once all shards are written back and the
locks released, the transaction object is unreferenced and scheduled for GC
(ADR-022).

### Why cross-shard write-back can be non-atomic

Write-back touches `M` shards in `M` independent CASes; there is no moment when
all `M` flip together. This does not break atomicity because **visibility is gated
on the single commit object, not on write-back**:

- A reader resolves a write-locked key through the locking transaction's
  _committed status_ (above), so it sees the committed value whether or not that
  key's shard has been written back yet. Write-back is an optimization that lets
  _future_ readers skip the help-forward by reading `current_writer` directly.
- Therefore a partially-written-back transaction is always resolvable to exactly
  its committed value map: a reader that catches some keys published and others
  still locked-by-the-committed-writer computes the same values for both. The
  transaction's effects appear atomically as of the commit CAS.

This is the v1 invariant (commit = log finalized; unlock/write-back is async and
readers resolve via the locker's status) carried to shard granularity.

### Membership operations (create / delete / list)

A create or delete adds the **collection root's membership write lock** to its
lock set (ADR-018): the root is just one more CAS object in the validate-and-lock
and write-back loops, with its lock state in the `CollectionRoot` body.

- **Create `K`**: lock `shard(K)` (`create`) + root membership (`write`),
  validate `K` absent, commit (object carries `K`'s value), write back
  (`current_writer = txid` for `K`; release shard lock; release root membership,
  which bumps the root version and invalidates concurrent listers).
- **Delete `K`**: write-lock `shard(K)` + root membership, commit (object carries
  `K`'s tombstone), write back (set `deleted` + `current_writer`; release).
- **List**: optimistic on the root version with a read-lock fallback (ADR-018);
  this is a read-side protocol, not a commit, but it participates in the same lock
  table on the root.

### Deadlock prevention and the serial fallback

Locks are acquired in parallel and resolved proactively by wound-wait (priority
from the txid timestamp, ADR-002). A per-transaction deadlock budget bounds the
parallel attempt; on timeout the transaction releases everything and re-acquires
in a **globally sorted order by object path** (shards and the root have
deterministic, sortable paths), which cannot deadlock. Equal-priority transactions
that wound-wait cannot order fall through to this serial path, exactly as in v1.
The mechanism is unchanged; only the lock targets differ (shards + root instead of
per-key objects), so there are typically _fewer_ objects to sort and lock, at the
cost of coarser conflicts.

> **Superseded by [ADR-024](024-hold-and-wait-conflict-resolution.md):** the MVP
> realised "wait" as release-and-retry; the engine is now back to hold-and-wait
> (a transaction keeps its locks and waits for a conflicting holder, bounded by a
> deadlock timeout that escalates to the serial order, with a load-bearing lease
> refresher). The MVP description below is retained for history only.
>
> **MVP realisation (historical):** the MVP engine kept the two-mode
> structure above but realised "wait" as **release-and-retry** rather than
> hold-and-wait, so no transaction ever held locks while blocked. This was a
> deliberate **MVP simplification** — see [the migration note](#mvp-vs-the-v1-hold-and-wait-model)
> below for why and what replaced it.
>
> - **Default (parallel) path.** All touched shards are locked concurrently (one
>   RTT for the whole set), then the root last (it is the highest object in the
>   lock order). A transaction that meets a holder it cannot reclaim — a live,
>   non-expired peer that does **not** lose wound-wait to it — aborts immediately,
>   releases its locks, and retries with its priority preserved (`TxId::renew`:
>   same timestamp, fresh prefix). For *distinct* priorities this is deadlock- and
>   livelock-free: the strictly-older transaction always wounds the younger and
>   makes progress. Two *equal-priority* transactions, however, can **livelock**
>   here — each may grab a different shard first and then abort on the other.
> - **Serial fallback.** After a few failed attempts (`SERIAL_FALLBACK_AFTER`) a
>   transaction escalates to locking shards one at a time in **ascending shard
>   index order** (then the root). Under this single global order every contender
>   queues on the lowest contended shard, and whoever CASes its lock there first
>   wins it while the others abort; the winner is never wounded by an equal peer,
>   so it acquires the rest and commits. This guarantees global progress (the
>   holder of the highest-ordered lock always finds everything above it free), so
>   equal-priority transactions cannot livelock.
>
> Because acquisition never blocks while holding locks, the bounded "deadlock
> budget / release-and-retry" of the design above is realised directly by the
> retry loop, and there is **no prefix tiebreak** in wound-wait: a tiebreak would
> flip under `renew` and reintroduce the equal-priority livelock the serial path
> exists to prevent. Write-back remains synchronous (GC/async batching →
> ADR-022/perf).

#### MVP vs. the v1 hold-and-wait model

> **Resolved by [ADR-024](024-hold-and-wait-conflict-resolution.md):** the
> reversion to hold-and-wait described in this section is now implemented. The
> text is kept for the rationale.

The release-and-retry realisation above was an **MVP-only** choice. It traded the
efficiency of holding locks across retries for a much smaller, refresher-free,
DST-friendly engine while the new shard/transaction-object layout was validated end
to end. Its costs are real: on a conflict the whole transaction body is re-run and
every shard is re-CAS'd, and each `TxId::renew` orphans an aborted transaction
object (GC debt) until ADR-022 lands.

When the engine is ported onto **v1's logic and data structures** it reverts to
**hold-and-wait**, exactly as the design body above (and v1) describe:

- a transaction **keeps** the locks it has acquired and **waits** for a conflicting
  holder instead of aborting (wound-wait: older wounds younger, younger/equal
  waits), so work already done is not thrown away;
- the parallel attempt is bounded by a **deadlock timeout**; only on timeout does
  it release and re-acquire in the sorted serial order;
- a transaction that waits while holding locks runs the **lease refresher**
  ([ADR-021](021-wound-wait-leases-shard.md)) so its held locks are not falsely
  reclaimed.

This restores v1's behaviour; the only reason it is not in the MVP is that, on a
backend with no wait/notify primitive, "wait" degrades to polling, so the
simplicity of release-and-retry is the better starting point. The serial fallback,
wound-wait, and lease-expiry semantics are identical across both models — only the
conflict action (release vs. wait) changes. This reversion is specified by
[ADR-024](024-hold-and-wait-conflict-resolution.md), with one departure from the
design body above: the pending object is **not** prepared up front (phase 2) but
kept lazily created as in the MVP — materialized by the now load-bearing refresher
and bridged by the `handle_unknown_tx` grace until it exists.

### In-doubt outcomes at the new CAS sites

Every CAS site inherits [ADR-009](009-in-doubt-conditional-writes.md): an
`Unavailable` (in-doubt) outcome means the write may or may not have landed.

- **Pending-object create**, **shard lock CAS**, and **write-back CAS** are all
  recoverable in place by read-back: the pending object's existence, the
  transaction's presence in an entry's `locked_by`, and `current_writer == txid`
  respectively are observable facts that reveal whether the write took, and
  re-applying each is idempotent. These never escalate to a user-visible retry.
- **Commit CAS**: idempotent via the txid-keyed object (ADR-019); a re-issue
  either lands or observes the object already `committed` by this txid.
- **Single read-write fast path**: the one place an irreducible in-doubt can still
  surface. v2 _narrows_ it relative to v1: because the value's committed
  transaction object exists and the published pointer names the writer, the common
  lost-ack case resolves by reading the shard back (`current_writer == txid` ⇒
  committed). The residual ambiguity (a fast follow-on writer overwrote the
  pointer) is surfaced as `InDoubt`, as before.

### Fast paths

- **Read-only**: validate each read by re-checking its entry's `current_writer`
  against the read version (no locks, no transaction object); all-match ⇒ commit.
  A conflict escalates to taking read locks (the same shard-entry read locks) and
  revalidating, mirroring v1. Multi-key snapshot consistency follows from every
  read still being current at validation; listing read-only uses the root-version
  OCC of ADR-018.
- **Single read-write** (_superseded by
  [ADR-027](027-single-rw-parallel-lock-publish.md)_): write the committed
  transaction object (`write_if_not_exists`), then CAS the key's shard to set
  `current_writer = txid` _only if_ the entry is unchanged and unlocked — two
  operations, no lease, no lock round-trips, no write-back. The shard CAS is the
  effective commit point; if it never lands, the orphan transaction object is
  unreferenced and GC'd. This is the v2 form of v1's logless fast path, and unlike
  v1 it leaves a discoverable committed object (so the [ADR-007](007-single-rw-cache-lost-update.md)
  lost-update anomaly cannot recur). ADR-027 keeps the discoverable committed
  object but publishes a **write lock** instead of the pointer, so the object and
  shard writes issue **in parallel**, and an asynchronous write-back converts the
  lock to the pointer.

## Consequences

- ADR-017–020 together are behavior-complete: a transaction can validate, lock,
  commit, and publish entirely on content CAS over shards, the root, and
  transaction objects — no tags, no per-key value objects, no metadata mutation.
  This is the first point in the v2 effort where an end-to-end path is
  implementable and testable against the DST oracles.
- Multi-key transactions whose keys share a shard pay **one** lock CAS and **one**
  write-back CAS instead of one per key — the headline efficiency of the sharded
  directory.
- The new cost is **shard-CAS contention** distinct from lock conflict:
  unrelated writers in a hot shard serialize on its CAS (merge-on-retry) even
  though they never lock-conflict. `C` bounds it.
- Help-forward keeps reads correct in the commit→write-back gap, so write-back can
  be lazy, batched, and crash-resumable without affecting correctness; this is
  what lets cross-shard write-back be non-atomic.
- Commit is a single small CAS — cheaper and simpler than the multi-step v1
  finalize — and it is the only synchronous durability point besides lock
  acquisition.
- The protocol leans on leases to reclaim locks of crashed transactions
  (ADR-021) and on mark-sweep to reclaim committed objects once unreferenced
  (ADR-022); the slimmed backend trait it targets is ADR-023.
- The layout-independent DST oracles (RMW serializability, the cycle ring) carry
  over as the safety net; golden vectors and `RecordingBackend` byte streams are
  regenerated for the new CAS sequences.
