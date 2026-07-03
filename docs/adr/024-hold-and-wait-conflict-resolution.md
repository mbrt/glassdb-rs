# ADR-024: Hold-and-wait conflict resolution (waiting-while-holding + lease refresh)

## Status

Accepted and **implemented**. The supporting machinery that was dormant in the
MVP is now wired into the conflict path: `Monitor::wait_for_tx` backs the wait,
the `refresh_pending` / `start_refresh_tx` lease refresher is load-bearing (and
made wound-safe), the expiry predicate is split into an absolute (skew-padded)
lease check and an observer-relative (no-skew) progress check, and the
deadlock-timeout → serial fallback is handled **autonomously inside `Algo`**: on
timeout the transaction releases its locks (`Locker::release_locks`) and
re-acquires them in the serial sorted order under the **same id**, looping
internally — it never renews, never re-runs the body, and never surfaces the
timeout to the `db.rs` retry loop (`TransError::LockTimeout` is only an internal
control signal). **CAS contention** is resolved the same way: a lost shard/root
CAS race releases the partial locks and re-acquires under the **same id** after a
backoff (escalating to the serial order if it persists), so a transaction that
merely lost a race never discards its executed body. Read-version validation runs
**in `Algo`, after locking** (not in the shard CAS): once every touched key is
locked and frozen, `Algo` re-resolves the read set's effective writers — reusing
the read-only validation routine — and a read whose value moved re-runs the body
holding its locks (`TransError::Retry`) instead of releasing and renewing. Body
re-runs are therefore limited to the two cases that prove they are needed: a
**stale read**, and a **genuine wound** (a higher-priority peer aborted us, so our
id is dead and must be renewed).

It **supersedes the MVP-only deviations** of
[ADR-020](020-commit-write-back-protocol.md) (§ "MVP realisation" and § "MVP vs.
the v1 hold-and-wait model") and [ADR-021](021-wound-wait-leases-shard.md) (the
"no background refresher" note): those sections promised the hold-and-wait model
would return once the engine moved past the MVP. It is now past the MVP. It also
**refines ADR-021's expiry predicate** so the observer-relative grace owes no clock
skew (see "Deciding whether a holder is live" below).

## Context

[ADR-020](020-commit-write-back-protocol.md) defines the commit protocol and its
conflict-resolution model, but realises "wait" — **for the MVP only** — as
**release-and-retry**: a transaction that meets a holder it cannot wound aborts
its whole attempt, releases its locks (its transaction object is marked
`aborted`), mints a fresh id with `TxId::renew`, and re-runs the body from
scratch ([`crates/glassdb/src/db.rs`](../../crates/glassdb/src/db.rs), the
`Wounded` arm). This keeps the MVP small and refresher-free, but its costs —
spelled out in ADR-020 — are real under contention:

- the entire transaction **body is re-executed** and **every shard is re-CAS'd**,
  discarding work already done;
- each `renew` **orphans an aborted transaction object**, which is the dominant
  source of garbage for [ADR-022](022-garbage-collection-mark-sweep.md) to sweep.

The v1 model ([ADR-002](002-wound-wait-locking.md),
[architecture.md](../architecture.md) "Deadlock Handling") is the opposite and is
what this ADR restores onto the v2 shard layout:

- A transaction **keeps the locks it has acquired** and **waits** for a
  conflicting holder under wound-wait (an *older* transaction wounds a *younger*
  holder and proceeds; a *younger-or-equal* one waits) instead of throwing its
  work away.
- The parallel acquisition attempt is bounded by a **deadlock timeout**; only on
  timeout does the transaction release everything and re-acquire in the global
  **serial sorted-by-path** order, which cannot deadlock.
- A transaction that waits **while holding locks** runs a **lease refresher** so
  its held locks are not reclaimed as expired (ADR-021).

The reason the MVP avoided this is that object storage has **no wait/notify
primitive**, so "wait" must be realised by **polling** the holder's status. Past
the MVP, the contention win (no body re-execution, no orphaned objects, less GC
debt) justifies the polling cost — and the polling primitive (`wait_for_tx`)
already exists and is deterministic under the DST executor, so the only missing
piece is wiring the conflict path to wait rather than abort.

This ADR changes **only the conflict action and its supporting lifecycle**. The
five-phase protocol, the shard-CAS lock step, wound-wait priority, the
serial-sorted lock order, the commit flip, write-back, and the on-disk formats
are all unchanged from ADR-017–021. (Read-version validation was later lifted out
of the shard CAS into `Algo` — see "Read-version validation lives in `Algo`,
after locking" — but the lock CAS itself is otherwise as in ADR-020.)

## Decision

### A younger-or-equal transaction waits, holding its locks

When the shard lock step meets an entry locked by a live **pending** holder `L`
(in a shard or the collection root) and this transaction does **not** outrank `L`
by wound-wait priority, it no longer returns a conflict that aborts the attempt.
Instead it **waits** for `L` to finalize — `Monitor::wait_for_tx(L)` — while
**retaining every lock it has already acquired** on other shards/entries and its
transaction object. When `L` finalizes it re-resolves the entry and proceeds:

- `L` **committed** → help-forward `L`'s write-back and take the lock (ADR-020);
- `L` **aborted** (self-abort, wound, or lease expiry) → drop `L` and take the
  lock.

The wound case is unchanged: an **older** transaction wounds `L`
(`pending → aborted` CAS) and takes the lock immediately, never waiting.

### Locks and the transaction object persist across the wait

The defining change from release-and-retry: a waiting transaction **does not
renew its id and does not release its locks**. Its acquired locks stay installed
in their shards/root and its transaction object stays `pending` (materialized
lazily, below) throughout the wait, so the work already done (executed body,
acquired locks) is preserved. A fresh id (`TxId::renew`) and a body re-run are
reserved for the one case that genuinely requires them:

- **Wounded** — `L` was *this* transaction and a higher-priority peer aborted it:
  its id is dead, so it must restart with a renewed id (preserving priority),
  exactly as today.

The **serial fallback** (a deadlock timeout, below) does release its locks, but
it neither renews the id nor re-runs the body: `Algo` releases and re-acquires in
sorted order internally, keeping the same transaction identity (v1's model).
**Losing a CAS-contention race** is handled identically — release the partial
locks and re-acquire under the same id after a backoff, escalating to the serial
order if contention persists — so a lost race is no longer a renew-and-re-run
`Wounded`; it never discards the executed body.

A **stale read** re-validates against the refreshed state holding the locks
already taken (the dormant `ValidateRetry` path); only a read whose value actually
moved forces a body re-run, and even then under the same held locks rather than a
released-and-renewed attempt. This validation now lives in `Algo`, **after** every
read is locked (so its value is frozen), reusing the same effective-writer check
as the read-only fast path (see the next section). This is the hold-and-retry that
the MVP replaced with release-and-retry.

### Read-version validation lives in `Algo`, after locking

A v2 shard entry co-locates a key's **lock state** (`locked_by` / `lock_type`)
and its **version pointer** (`current_writer`) in one object, so the first cut
folded optimistic read validation into the shard CAS: the lock attempt compared
the read's observed token against the resolved entry and bailed out with a
`StaleRead` restart. That coupled the locker to optimistic-concurrency *policy*
and left the read's shard **unlocked** on a stale bail-out, so the body re-run had
to re-acquire it.

Validation is now `Algo`'s responsibility and runs **after** all locks are held.
Once `Locker::lock` returns `Locked`, every touched key is locked and its value
frozen — only the write-lock holder (this transaction) can move it — so `Algo`
re-resolves each read's effective writer through the path-based `Reader` and
compares it to the observed token, reusing the exact routine the read-only fast
path uses. A mismatch re-runs the body holding the locks (`Retry`). Because the
moved key is itself locked during the re-run, this restores v1's guarantee that
the retry holds **all** its locks (the shard-CAS variant left the stale key
unlocked). The locker no longer carries read tokens or returns a stale-read
outcome — `LockOutcome` is just `Locked | Conflict` — so it is a pure locking
mechanism and read validation exists in exactly one place.

Validating after locking is correct and is *not* a new TOCTOU window: a peer can
only change a read key's effective writer by committing a write to it, which
requires the write lock this transaction holds; a peer that committed *before* we
locked is caught by the post-lock re-resolve, exactly as the folded check was. The
cost is one effective-writer resolve per read after locking instead of folding it
into the lock CAS — the same shape v1 used (`GetMetadata` after lock).

### The pending object stays lazily created (no upfront prepare)

The MVP's lazy creation is **kept**: a transaction does **not** write a pending
object before locking, so a short transaction that commits quickly still writes
its transaction object exactly once (directly as `committed`) and pays no extra
round-trip on the fast path. The object is materialized only when a hold outlives
the refresh interval — written by the refresher (below) after `PENDING_TX_TIMEOUT/2`.

A held lock whose pending object does not exist yet is still safe, because the two
facts a peer needs are available without it:

- **Priority** is embedded in the txid itself, so wound-wait orders the holder and
  the contender with no object to read.
- **Liveness** is supplied by the observer-relative grace period
  ([ADR-021](021-wound-wait-leases-shard.md)'s `handle_unknown_tx`, refined
  below): a peer that resolves a `locked_by` entry to a *missing* object treats the
  holder as pending until the object fails to **appear within `PENDING_TX_TIMEOUT`
  of first sight**, so it neither reclaims it prematurely nor blocks forever. The
  refresher materializes the real pending object well inside that window (at
  `PENDING_TX_TIMEOUT/2`), so protection passes seamlessly from the grace period to
  the lease.

So an upfront prepare is unnecessary: the grace period covers the brief window
before the object exists, and the refresher covers the rest of the hold. The cost
is that the engine relies on `handle_unknown_tx` rather than a guaranteed-present
pending object — but that grace path already exists and is exercised by crash
recovery, so it is not new surface.

### Deciding whether a holder is live: absolute lease vs. observer-relative progress

A waiter must decide whether the holder `L` of a contended entry is still alive.
Two checks combine, and they differ in exactly one respect — whether
`MAX_CLOCK_SKEW` applies:

- **Absolute lease (foreign clock — skew applies).** When `L`'s pending object
  exists, its `timestamp` is a value from `L`'s wall clock; comparing it against
  the observer's `now` crosses machines, so it is padded by the skew:

  ```
  now > timestamp + PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW   ⇒  L is dead
  ```

  This is ADR-021's `is_expired`, unchanged. It lets a waiter immediately reclaim a
  holder whose last refresh is already ancient the first time it is seen.

- **Observer-relative progress (one clock — no skew).** The waiter also remembers,
  per holder it watches, the last *progress* it observed and the observer-clock time
  it saw it. If no progress occurs within `PENDING_TX_TIMEOUT` measured on the
  observer's **own** clock — with **no** skew pad — `L` is dead:

  ```
  now > last_progress_seen_at + PENDING_TX_TIMEOUT        ⇒  L is dead
  ```

  "Progress" is the pending object's `timestamp` **changing** or, when the object is
  *missing*, the object **appearing** — so a missing object must materialize within
  `PENDING_TX_TIMEOUT` of first sight. No skew is added because both endpoints are
  the observer's own clock, and the `timestamp`-changed test compares two values
  from the *same* foreign clock, whose offset cancels. The skew pad is owed only to
  the single cross-clock comparison in the absolute check. The refresher's
  `PENDING_TX_TIMEOUT/2` cadence guarantees a live holder makes progress (a
  `timestamp` bump, or the first materialization) comfortably inside this window.

This **refines [ADR-021](021-wound-wait-leases-shard.md)**: its `handle_unknown_tx`
reuses the skew-padded `is_expired` on `first_check`, which over-grants the
missing-object window by `MAX_CLOCK_SKEW`. The implementation must (a) drop the skew
from that observer-relative window so the grace is `PENDING_TX_TIMEOUT`, not
`PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW`, and (b) add the `timestamp`-changed
relative check for an existing object that stops refreshing while a waiter watches
it. The absolute check keeps the skew exactly as today.

### The lease refresher is load-bearing

A transaction that holds any lock runs the background refresher
(`start_refresh_tx` → `refresh_pending`, [ADR-021](021-wound-wait-leases-shard.md))
for the duration of the hold. Its **first** write *creates* the pending object
(status `pending`, lease `timestamp`, lock intentions, ADR-019) with
create-if-absent semantics; thereafter it CAS-bumps the `timestamp` every
`PENDING_TX_TIMEOUT/2` over the object's version until the transaction commits or
aborts. This is what stops a peer from reclaiming a *waiting* holder's locks as
expired: under hold-and-wait a live transaction can legitimately hold locks far
longer than `PENDING_TX_TIMEOUT`, which is exactly the case the MVP did not have to
handle (it never blocked while holding).

Creating (rather than blindly writing) the object is what keeps lazy
materialization **wound-safe**: if an older peer already wounded this transaction
by writing an `aborted` object before it materialized its own pending one, the
create-if-absent loses, the refresher observes `aborted`, stops, and the
transaction aborts — it can never resurrect itself over a wound. A later refresh
CAS that finds the object `aborted` is the same wound signal once it has
materialized. (This is the one behavioural fix the lazy path needs: today's first
refresh write is unconditional, which is harmless only because the MVP refresher
almost never fires.)

This wound-safety relies on the `aborted` object still being **present** when the
refresher retries; GC must therefore not delete it out from under a stuck owner.
[ADR-022](022-garbage-collection-mark-sweep.md) guarantees this by retaining an
aborted object as a tombstone for a full safety lease *after the abort*, so the
create-if-absent always finds it.

### A deadlock timeout bounds the wait and escalates to the serial order

Waiting reintroduces the possibility of a wait-for cycle. It is prevented exactly
as in v1:

- Distinct priorities cannot cycle: wound-wait makes the strictly-older
  transaction wound its way forward, so it never waits on a younger holder.
- Equal priorities can cycle (each waits on the other). A per-transaction
  **deadlock timeout** bounds every wait. On timeout `Algo` **releases all its
  locks** (`Locker::release_locks`, which clears the holder from each shard/root
  it touched without publishing a value) and **re-acquires them in the global
  serial sorted-by-object-path** order (the existing `serial` mode, ADR-020).
  Under one global order the holder of the highest lock always finds everything
  above it free, so one contender always completes — no livelock, no deadlock.

This release-and-re-lock happens **entirely inside `Algo::commit`** (an internal
loop over the locking step): the transaction keeps its **same id**, re-runs **no
user code**, and surfaces **no error** to the `db.rs` retry loop — exactly v1's
`serial_validate`. `TransError::LockTimeout` exists only as the internal signal
the parallel select arm raises to trigger the release-and-serial step; it never
escapes the engine. Releasing the out-of-order locks before re-acquiring is
essential: re-locking serially while still holding a lock grabbed out of order
would recreate the very cycle the sorted order exists to break.

This replaces the MVP trigger for the serial fallback ("after
`SERIAL_FALLBACK_AFTER` failed *abort-and-retry* attempts") with the v1 trigger
("a wait exceeded the deadlock budget"); the sorted-acquisition mechanism itself
is unchanged. (`SERIAL_FALLBACK_AFTER` is retained only as a secondary backstop
that starts a heavily-restarted transaction directly in the serial order.)
Wound-wait still uses **no prefix tiebreak** (a tiebreak would flip under
`renew`), so equal priorities are resolved only by this serial path, as in
ADR-020.

### Waiting is poll-based

With no wait/notify primitive on object storage, `Monitor::wait_for_tx` realises
the wait by **polling** `L`'s status to a final state with backoff, abandoning the
poll when every waiter's future is dropped (the existing
`poll_tx_status_with_liveness` loop). The cost is extra status GETs while blocked,
bounded by backoff and by `L`'s short commit critical path; the poll runs against
the `Clock` / `Background` seam, so wait/expiry timing stays deterministic under
the DST executor (ADR-008/013).

### In-doubt parity is unchanged

Every CAS site keeps [ADR-009](009-in-doubt-conditional-writes.md): the lazy
pending-object create (`write_if_not_exists`), the refresher CAS, the shard/root
lock CAS, the wound/abort CAS, the commit flip, and write-back all recover an
`Unavailable` outcome in place by read-back and idempotent re-apply. Waiting adds
no new CAS site — it only consumes a peer's status — so it introduces no new
in-doubt case.

## Consequences

- Under contention a transaction no longer discards executed work and acquired
  locks: it waits for the conflicting holder and proceeds. Fewer body
  re-executions, far fewer orphaned transaction objects, and therefore **less GC
  debt** — the "dominant source of garbage" in
  [ADR-022](022-garbage-collection-mark-sweep.md) shrinks to wounds and the serial
  fallback.
- It **reintroduces blocking**, hence the possibility of deadlock, prevented by
  wound-wait plus the deadlock-timeout → serial-sorted fallback — the v1 guarantee
  ([ADR-002](002-wound-wait-locking.md)) now on the shard layout.
- The **lease refresher becomes load-bearing**: it lazily *creates* the pending
  object (create-if-absent, wound-safe) once a hold outlives `PENDING_TX_TIMEOUT/2`
  and refreshes it thereafter. The MVP's lazy creation is kept — there is **no
  upfront prepare**, so the fast path still writes the transaction object only once
  (at commit) — and a held lock is bridged by the `handle_unknown_tx` grace period
  until the object exists. This re-activates ADR-021's refresher without restoring
  ADR-020's prepare phase.
- "Wait" is **polling**, so a blocked transaction issues periodic status GETs;
  this is the documented price of having no notify primitive on object storage,
  and it is why the MVP deferred this model. It stays DST-deterministic via the
  `Clock` / `Background` seam, so the existing serializability and cycle oracles
  exercise it unchanged.
- The change is **localised to the conflict path and the transaction lifecycle**;
  no on-disk format, no proto, and no change to the shard lock CAS, the commit
  flip, or write-back. (Read-version validation was additionally lifted out of the
  shard CAS into `Algo`, post-lock — see "Read-version validation lives in `Algo`,
  after locking" — leaving the locker a pure locking mechanism.) The dormant
  `wait_for_tx` / `refresh_pending` machinery is
  activated rather than newly built, and both the deadlock-timeout serial
  fallback and CAS-contention retry are handled inside `Algo` (release + re-lock
  under the same id) exactly as v1's `serial_validate` did, rather than surfaced
  to the db retry loop.
- After this ADR the v2 engine matches **v1's concurrency behaviour**; the only
  remaining difference from v1 is the lock **granularity** (per-shard entries vs.
  per-key objects), which is unchanged here.
- The MVP's release-and-retry remains a valid fallback shape (it is strictly
  simpler and refresher-free); this ADR is the decision to pay its costs no
  longer now that correctness and tooling are past the MVP.
