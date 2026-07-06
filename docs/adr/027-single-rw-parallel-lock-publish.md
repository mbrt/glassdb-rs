# ADR-027: Parallel single read-write commit via a lock

## Status

Accepted — implemented. Supersedes the **single read-write fast path** decision
of [ADR-020](020-commit-write-back-protocol.md) (its "Fast paths →
Single read-write" bullet); the rest of ADR-020 is unchanged.

## Context

ADR-020's single read-write fast path commits a lone overwrite of an existing
key with **two sequential** writes: the committed transaction object
(`_t/<txid>`), then a shard CAS that publishes `current_writer = txid`. The order
is load-bearing: a bare `current_writer` pointer is trusted _directly_ by
readers, so the object it names must be discoverable **before** the pointer, or
the [ADR-007](007-single-rw-cache-lost-update.md) lost-update anomaly recurs (a
reader resolves the pointer, finds no object, and poisons its cache). Two serial
round-trips on the critical commit path.

Two observations remove that constraint:

1. The ordering is a property of the **pointer**, not of the shard write. A
   `locked_by` write lock is _not_ believed on sight: every consumer interprets a
   locked entry through the holder's status via the shared resolver
   (`resolve_holders`), which falls back to the existing `current_writer` when
   the holder's object is missing or pending. So a lock write carries no
   happens-before requirement against the object write — the two can be parallel.
2. A lock held by an **already-committed** writer is not a conflict. The resolver
   help-forwards such a holder to its committed value (the effective writer),
   treating only *live pending* holders as blockers. So the window in which a
   fast-path writer holds its lock before its write-back runs does not force the
   next single-key writer off the fast path.

## Decision

The fast path publishes a **write lock** instead of the pointer, issues its two
writes **concurrently**, and converts the lock to the pointer asynchronously:

1. **Pre-check**: load the shard and resolve the entry's holders. A
   committed-but-not-yet-written-back holder is help-forwarded to the effective
   writer; a *live pending* holder, a non-existent/tombstoned key, or (for a
   read) an effective writer that no longer matches the read version makes the
   transaction ineligible — nothing has been written, so it falls back to the
   full locked path under the same id.
2. **Issue in parallel**:
   - **W1** — write the committed transaction object (`set_final_log`, status
     `Ok`), recording its held lock (`locks = [key: write]`) so GC's reverse
     check can prune it ([ADR-022](022-garbage-collection-mark-sweep.md)).
   - **W2** — one shard CAS that installs `lock_type = Write`,
     `locked_by = [txid]` and **help-forwards the resolved predecessor into
     `current_writer`** (so taking over a committed holder never orphans it),
     conditional on the loaded shard version and the entry still being eligible.
3. **Write-back** (asynchronous): once both land, publish `current_writer = txid`
   and release the lock via the deduplicated write-back path
   ([ADR-026](026-dedup-shard-release-write-back.md)).

### Commit point and correctness

A transaction is committed iff **both** hold: its committed object exists (W1),
and it is in the shard's version chain (W2 landed, or a later writer
help-forwarded it so `current_writer == txid` is observable). The object side is
unambiguous — keyed by the txid, written `committed` create-if-absent
(idempotent). The shard side is a lock, resolved exactly like the full path's: an
in-doubt lock CAS is recovered by reloading the shard (our lock present ⇒ landed;
still eligible ⇒ re-issue the idempotent CAS). Help-forwarding the predecessor
into `current_writer` is safe — it publishes only an already-committed value, the
same thing that holder's own write-back would publish.

Because the transaction now holds a lock during the short pre-commit window, it
is a first-class wound-wait participant: an older concurrent writer on the same
key may wound it, and it renews (priority preserved, `TxId::renew`) and re-runs,
exactly as the full path does. It could not be wounded before only because it
held nothing.

### The one retained in-doubt

The single irreducible in-doubt of ADR-020 remains. If the lock CAS returns
`Unavailable` _and_ the shard has moved past us by the time we read it back, two
histories are indistinguishable: the lock landed and a follow-on writer
help-forwarded us into the chain (committed), or it never landed and the object
is an orphan (renew). This surfaces as `InDoubt` rather than risk a double-apply.

## Consequences

- Fast-path commit latency drops from two serial writes to two parallel ones
  (~2 RTT → ~1 RTT on the critical path).
- The cost is one additional **asynchronous** write-back CAS per fast commit (off
  the critical path), reintroducing the write-back ADR-020 had elided — batched
  and idempotent via ADR-026.
- Resolving holders keeps concurrent single-key writers on the lock-free fast
  path across the write-back window instead of diverting them to the full path;
  it adds no backend ops on the common (unlocked) path, since `resolve_holders`
  makes no calls when the entry has no holders.
- The fast path now holds a lock, so it participates in wound-wait and lease/GC
  bookkeeping like the full path (its committed object records the lock). A crash
  after commit leaves a lock behind a committed object that readers help-forward
  and the next writer or GC reclaims — the same lifecycle as the full path.
- The backend op stream for a fast commit changes shape (a lock CAS plus a
  deferred write-back CAS instead of a single pointer CAS) but stays deterministic
  under the simulation executor, so the op-stream self-check and the
  serializability / cycle oracles remain the safety net.
