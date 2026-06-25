# ADR-021: Wound-wait and leases at shard granularity

## Status

Partially implemented — wound-wait is live in `glassdb-trans::v2`; leases /
expiry (crash recovery) are the remaining follow-up

## Context

[ADR-020](020-commit-write-back-protocol.md) reclaims the locks of _live_
contending transactions with wound-wait, but defers the mechanism that reclaims
the locks of a **crashed or abandoned** one — the lease — to this ADR.
[ADR-019](019-unified-transaction-object.md) similarly left a placeholder for a
lease field. Without leases the protocol is only happy-path correct: a dropped
client holding a write lock on a shard entry blocks that key forever, and a
younger transaction that must _wait_ on a dead older holder hangs (wound-wait
never aborts the older one).

The v1 mechanism (`crates/glassdb-trans/src/monitor.rs`) is the reference:

- Priority comes from the txid timestamp ([ADR-002](002-wound-wait-locking.md));
  the lock state lives in each key object's tags.
- The **lease** is the pending transaction log's timestamp, refreshed every
  `PENDING_TX_TIMEOUT / 2` (`refresh_pending`); a peer treats a holder as dead if
  `now` is past `last_refresh + PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW`
  (`is_expired`, with `PENDING_TX_TIMEOUT = 15s`, `MAX_CLOCK_SKEW = 30s`).
- A peer reclaims by forcing the holder's log to `aborted` with a CAS
  (`wound_tx` / `try_abort_remote_tx`); a holder referenced by a lock but with no
  log yet gets a grace period then the same treatment (`handle_unknown_tx`).

This ADR relocates that mechanism to the v2 layout: locks are entries
(`locked_by`) inside shard objects ([ADR-017](017-shard-object.md)) and the
collection root ([ADR-018](018-collection-root-membership.md)); the lease and
status live in the transaction object. The wound-wait _rule_ is unchanged; only
the storage sites move.

## Decision

### Priority and the wound-wait rule are unchanged

Priority is the txid's embedded timestamp; an older transaction **wounds** a
younger holder, a younger one **waits** (ADR-002). Two timestamps must not be
confused:

- **Priority** — the immutable timestamp inside the txid. Never refreshed.
- **Lease** — a mutable "last refreshed at" time on the transaction object,
  bumped while the transaction is alive.

### The lease lives in the transaction object (no new field)

The lease is carried by the transaction object's existing `timestamp` (ADR-019),
resolving its placeholder: while the object is **pending**, `timestamp` is the
last-refresh time; once **committed**, it is the commit time and never expires.
A pending object is **expired** when, on the observer's clock,

```
now > timestamp + PENDING_TX_TIMEOUT + MAX_CLOCK_SKEW
```

reusing v1's constants (15s timeout, 30s skew slack, refresh every 7.5s). The
clock is the `Clock` abstraction, anchored to virtual time under `--cfg sim`, so
wound/expiry stay deterministic for DST (ADR-008/013).

### Lease refresh

A transaction that holds any lock runs a background refresher that CAS-rewrites
its pending object's `timestamp` every `PENDING_TX_TIMEOUT / 2`, over the object's
version, until it commits or aborts. The pending object is small (no values), so a
refresh is cheap. If a refresh CAS finds the object already `aborted`, the
transaction was wounded: the refresher stops and the owner's commit will fail.
This is `refresh_pending`, relocated to the transaction object.

### Reclaiming a conflicting lock — the interplay with `locked_by`

When a transaction `T` needs a lock on an entry (in a shard or the root) already
held by `L`, it resolves `L`'s transaction object and acts:

- `L` **committed** → not a wound; `T` help-forwards `L`'s write-back (publish
  `current_writer`, release `L`'s lock here) and proceeds (ADR-020).
- `L` **aborted** → a stale lock; `T` drops `L` from the entry in its own shard
  CAS and takes the lock.
- `L` **pending** → wound-wait **combined with the lease**: `T` wounds `L` and
  takes the lock if `T` is **older than `L`** _or_ `L`'s **lease has expired**;
  otherwise `T` waits and periodically re-resolves `L`. The lease term is what
  lets a _younger_ `T` reclaim a _dead older_ `L` it would otherwise wait on
  forever.
- `L` referenced but its **object is missing** (GC'd, or an in-doubt pending
  create) → a grace period: treat as pending, record first-seen, reclaim after the
  same expiry window (this is `handle_unknown_tx`, relocated).

Wounding `L` is a CAS of `L`'s object `pending → aborted`; a CAS that instead
finds `L` already `committed` resolves to help-forward. After `L` is aborted, its
`locked_by` entries in _other_ objects are stale and cleared **lazily** — by the
next transaction that contends each entry (it sees `L` aborted and drops it in its
CAS), or by GC. `T` clears only the entry it is contending now.

This is uniform across every lock-bearing object: shard entries and the
collection-root membership lock are reclaimed by the identical sequence.

### Wounds target only pending objects

The abort CAS only succeeds from `pending`; a committed transaction cannot be
wounded (ADR-020). Commit is the point of no return, which bounds wounding and
keeps the reclaim logic total (every observed status maps to exactly one action).

### Reads never need the lease

A reader resolving a key that is write-locked by a _pending_ `L` uses the entry's
`current_writer` (the last committed value) regardless of `L`'s lease — a pending
transaction's value is simply not effective. Only lock _acquisition_ consults the
lease, so a dead lock never blocks or slows the read path; it only matters to a
writer that wants the same entry.

### Discovery is lazy, via `locked_by` — no pending registry

This settles the open question of whether the engine needs a list-discoverable
registry of in-flight transactions: it does **not**. A dead transaction is
discovered and reclaimed only when one of its `locked_by` entries is contended; an
uncontended dead lock blocks nobody and is swept by GC (ADR-022). Shard and root
`locked_by` entries are a sufficient discovery mechanism, at the cost of
**contention-driven (lazy) reclamation** — the simple MVP choice.

### In-doubt on the abort CAS

The wound / expiry abort inherits [ADR-009](009-in-doubt-conditional-writes.md):
forcing a not-yet-final object to `aborted` is idempotent and convergent, so an
`Unavailable` outcome is resolved by re-reading the status and retrying over the
refreshed version (the `try_abort_remote_tx` loop). A pre-commit reclaim is always
recovered in place, never surfaced to the caller.

### Equal priority

Transactions sharing a timestamp are not ordered, so neither wounds the other
(ADR-002); a deadlock between them is broken by the serial sorted-by-object-path
fallback (ADR-020), and if one is genuinely dead the lease reclaims it. No change
from v1 beyond the lock targets being shards and the root.

## Consequences

- Crash-liveness is restored, so ADR-016–021 form a **robust** end-to-end, not a
  happy-path demo: a dead holder's locks are always reclaimable.
- **No new proto field**: the transaction object's `timestamp` doubles as the
  lease, closing ADR-019's placeholder and matching v1's log-timestamp trick.
- Reclamation is **lazy**: an abandoned, uncontended lock lingers until GC. This
  is harmless for liveness but couples GC to leases — GC (ADR-022) must treat a
  _non-expired_ pending object as live (reachable) so it never deletes the lease
  of a transaction that is merely slow.
- The unit of wounding stays the **whole transaction**: aborting `L` invalidates
  all of `L`'s entries at once (one object CAS), and contenders clear individual
  stale entries lazily — fewer wound writes than per-key, coarser blast radius.
- Reusing `PENDING_TX_TIMEOUT` / `MAX_CLOCK_SKEW` and the `Clock` seam keeps
  wound/expiry timing deterministic under the DST executor, so the existing
  oracles exercise it unchanged.
- The read path is provably lease-free: leases gate only lock acquisition, so they
  add no cost to reads of existing keys.
- With this ADR the remaining v2 work is reclamation (ADR-022) and the slimmed
  backend trait (ADR-023); neither blocks correctness, so implementation can now
  target a complete, crash-safe protocol.
