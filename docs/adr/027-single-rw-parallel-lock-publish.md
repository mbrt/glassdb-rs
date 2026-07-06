# ADR-027: Parallel single read-write commit via a lock

## Status

Accepted — implemented. Supersedes the **single read-write fast path** decision
of [ADR-020](020-commit-write-back-protocol.md) (its "Fast paths →
Single read-write" bullet); the rest of ADR-020 is unchanged.

## Context

ADR-020's single read-write fast path commits a lone overwrite of an existing
key with **two sequential** writes: first the committed transaction object
(`_t/<txid>`), then one shard CAS that publishes `current_writer = txid`. The
order is load-bearing, not incidental: a bare `current_writer` pointer is trusted
_directly_ by readers (it names the effective committed value), so the object it
names must be discoverable **before** the pointer is published — otherwise the
[ADR-007](007-single-rw-cache-lost-update.md) lost-update anomaly recurs (a
reader resolves `current_writer = txid` but finds no object, poisoning its
cache). The two writes therefore cannot overlap, so the fast path pays two
serial round-trips on its critical commit path.

The observation behind this ADR: the ordering constraint is a property of the
**pointer**, not of the shard write. A `current_writer` pointer is believed on
sight; a `locked_by` write lock is not. Every consumer interprets a locked entry
through the holder's transaction status via the shared resolver
(`resolve_holders`), which tolerates a missing or pending object by falling back
to the entry's existing `current_writer` (the previous committed value). A lock
that names a transaction whose object is not yet discoverable is simply resolved
as "not effective yet" — never as a poisoned value. So a lock write carries no
happens-before requirement against the object write.

## Decision

The single read-write fast path publishes a **write lock** instead of the
`current_writer` pointer, and issues its two writes **concurrently**:

1. **Pre-check** (unchanged): load the shard and confirm dynamic eligibility
   (`single_rw_committable`: the key exists, is fully unlocked, and — if read —
   its `current_writer` still matches the read version). On ineligibility
   nothing has been written and the transaction falls back to the full locked
   path under the same id.
2. **Issue in parallel**:
   - **W1** — write the committed transaction object (`set_final_log`, status
     `Ok`), now recording its held lock (`locks = [key: write]`) so GC's reverse
     check can prune it ([ADR-022](022-garbage-collection-mark-sweep.md)).
   - **W2** — one shard CAS that installs `lock_type = Write`,
     `locked_by = [txid]`, leaving `current_writer` untouched, conditional on
     the loaded shard version and the entry still being committable.
3. **Write-back** (new, asynchronous): once both land, convert the lock to the
   pointer — publish `current_writer = txid` and release the lock — through the
   same deduplicated write-back path the full commit uses
   ([ADR-026](026-dedup-shard-release-write-back.md)).

### Commit point and correctness

A transaction is committed iff **both** hold: its committed object exists (W1),
and it is inserted into the shard's version chain (W2 landed, or a later writer
has help-forwarded it so `current_writer == txid` is observable). The object side
is unambiguous — the object is keyed by the txid and only this transaction ever
writes it `committed` (create-if-absent, idempotent). The shard side is a lock,
resolved exactly like the full path's lock: an in-doubt lock CAS is recovered in
place by reloading the shard (our lock present ⇒ landed; still committable ⇒
re-issue the idempotent CAS).

Because the transaction now holds a lock during the (short) window before its
object commits, it becomes a first-class wound-wait participant: an older
concurrent writer on the same key may wound it. That is correct serialization —
the fast path renews its id (priority preserved, `TxId::renew`) and re-runs,
exactly as the full path does. It could not be wounded before only because it
held nothing.

### The one retained in-doubt

The single irreducible in-doubt of ADR-020 remains, unchanged in shape. If the
lock CAS returns `Unavailable` (in-doubt) _and_ the shard has moved past us by
the time we read it back, two histories are indistinguishable: the lock landed
and a follow-on writer help-forwarded our committed value into the chain (we
committed), or the lock never landed and the follow-on writer built on the old
value (our object is an orphan and we must renew). This surfaces as `InDoubt`
rather than risk a double-apply. It is _narrowed_: while a follow-on writer has
help-forwarded but not yet written back, `current_writer == txid` is directly
observable and resolves to committed.

## Consequences

- Fast-path commit latency drops from two serial writes to two parallel ones
  (~2 RTT → ~1 RTT on the critical path).
- The cost is one additional **asynchronous** write-back CAS per fast commit
  (off the critical path), reintroducing on the fast path the write-back that
  ADR-020 had elided — but batched and idempotent via ADR-026. Total write
  amplification rises by one background CAS; commit latency falls.
- The fast path now holds a lock, so it participates in wound-wait and lease/GC
  bookkeeping like the full path (its committed object records the lock). A crash
  after commit leaves a lock behind a committed object that readers help-forward
  and the next writer or GC reclaims — the same lifecycle as the full path.
- The in-doubt contract is unchanged; `InDoubt` is still reachable from the fast
  path in the moved-after-`Unavailable` case, so its regression test stays.
- The backend op stream for a fast commit changes shape (a lock CAS plus a
  deferred write-back CAS instead of a single pointer CAS), but stays
  deterministic under the simulation executor, so the op-stream self-check and
  the serializability / cycle oracles remain the safety net.
