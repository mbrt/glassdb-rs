# ADR-025: Deduplicated shard-batched lock acquisition

## Status

Accepted — implemented. Re-introduces v1's request **deduplication** in the v2
`Locker`, re-keyed from per-key objects onto the v2 CAS coordination objects
(shards and collection roots). It builds on the dormant
[`glassdb_concurr::Dedup`](../../crates/glassdb-concurr/src/dedup.rs) primitive
(retained but unused since the v2 cutover) and changes **only** the internals of
`Locker::lock_shard` / `Locker::lock_root`; the five-phase protocol (ADR-020),
wound-wait/lease semantics (ADR-021), and hold-and-wait conflict resolution
(ADR-024) are unchanged.

## Context

In v1 the `Locker` wrapped every per-**key** lock/unlock request in
`glassdb_concurr::Dedup`: concurrent requests for the same key **merged** (a
shared-read batch, or a lock coalesced with an unlock) or **queued**, and a
single per-key worker drove the conditional writes. N transactions read-locking
one key therefore cost **one** backend operation, not N — the deduplication win.

The v2 engine ([ADR-017](017-shard-object.md), [ADR-020](020-commit-write-back-protocol.md))
moved the coordination unit from the per-key object to the **shard**
(`{prefix}/_s/<i>`) and the **collection root** (`{prefix}/_i`), each mutated by
a single content CAS. A transaction groups its accessed keys by shard and locks
each shard with one read-modify-write CAS. But the v2 `Locker` dropped the
deduplication: every transaction independently does `load_shard → resolve → CAS`
per shard, so unrelated transactions touching the same shard **serialize on its
CAS with bounded retries** (`CAS_RETRIES`). This is the "within-shard false
sharing" cost documented in ADR-020 and [`docs/algo-v2.md`](../algo-v2.md): two
transactions writing _different_ keys in one shard never lock-conflict, yet each
pays its own GET + CAS and races the other, wasting round-trips under contention.

ADR-020 already names the fix — _"batching all of a shard's keys into one CAS is
the efficiency win the sharded directory buys"_ — but only realised it _within_
one transaction. The remaining win is to batch it **across** transactions: when
several transactions contend the same shard, one owner should load it once, apply
every contender's compatible lock, and CAS once. That is exactly what v1's dedup
did per key; this ADR restores it at shard/root granularity.

## Decision

Route `Locker::lock_shard` and `Locker::lock_root` through a single
`Dedup<CasReq, StorageError, CasWorker>` keyed on the **object path** (the shard
or root path — the CAS unit). Everything above `lock_shard` / `lock_root`
(`lock_shards`' parallel and serial modes, `lock`, the `LockedTx` handle,
write-back, and `release_locks`) is unchanged: the deduplication lives strictly
beneath the per-object lock step.

### One request per transaction per object; merge on compatibility

A `CasReq` carries **one** transaction's intent for **one** object:

- `Shard { prefix, idx, members }` — `members` maps `TxId → intents` (the keys
  that transaction touches in this shard, with a Read / Put / Delete desire). A
  single submission has exactly one member; a _merged_ request accumulates
  several.
- `Root { prefix, tx }` — a membership write-lock request from one transaction.

Two shard requests **merge** iff, for **every key touched by both**, both sides
hold only a `Read` intent. Disjoint key sets always merge; a shared key with any
Put/Delete on either side does **not** (the two transactions genuinely
lock-conflict on that key and must be ordered by wound-wait). A non-merge leaves
the loser **queued** (FIFO); it runs in a later round, sees the winner as a
holder, and resolves by the usual wound-wait / hold-and-wait — identical to
today, minus the CAS retry storm (the queue replaces the racing CASes). Root
requests take the single exclusive membership lock, so they never merge; dedup
only **serializes** them through one owner, still removing the CAS contention.

`can_reorder()` is `true` for a pure read-only shard request (so it can join a
later read batch without FIFO-blocking behind an unrelated writer) and `false`
otherwise — the v2 analog of v1 marking unlocks reorderable.

### The worker: one load, resolve all members, one CAS

`CasWorker` drives a batch in a single round (dispatching on the `CasReq`
variant):

1. `batch.merged()` yields the merged request (all mergeable members absorbed).
2. Load the shard/root **once**.
3. For **each member**, resolve its keys exactly as the single-transaction path
   does (`resolve_and_lock` / `try_reclaim`): help-forward committed holders,
   drop aborted ones, and under wound-wait **wound** a younger external holder
   (proceeding past it) or, if the member cannot wound a live holder it does not
   outrank, mark that member **`Wait(holder)`** and stage **none** of its keys.
   Members are independent (disjoint or shared-read by the merge rule), so their
   staged changes never conflict.
4. CAS the object **once** with every proceeding member's staged locks.
5. Deposit each member's per-transaction outcome, then return.

A `Precondition` (the object moved under the owner) or an `Unavailable` (in-doubt,
ADR-009) reloads and retries within the bounded `CAS_RETRIES` budget; exhaustion
deposits `Conflict` for all members. Re-installing a transaction's own lock is
idempotent (the resolve step skips holders equal to the member), so an in-doubt
CAS recovers in place, never surfacing — unchanged from the single-transaction
path.

Crucially the owner is **non-blocking**: a member that must wait does not park
the owner; it is delivered `Wait(holder)` and _its own caller_ waits and
re-submits (below), so unrelated members on the same object keep making progress
in the same round.

### Per-transaction outcomes via a side channel

`Dedup` delivers one shared `Result<(), E>` to every batch member, but members
now have **heterogeneous** outcomes: `Locked { membership }` (with the
per-transaction membership-change flag that decides whether it must also lock the
root), `Wait(holder)`, or `Conflict`. `membership` cannot be recomputed by the
caller without re-reading the object — defeating the purpose — so the worker must
communicate it.

A member is delivered its result **iff** it was in the worker's final
`batch.merged()` snapshot (new arrivals sit in `pending`/`queue` and get their
own round). The worker therefore deposits an outcome for **exactly** the members
in that snapshot into a shared `Mutex<HashMap<(object_path, TxId), CasOutcome>>`
**before returning** — so the deposit happens-before `Dedup`'s delivery — and
each caller `take`s its own `(path, tx)` entry after `dedup.run` resolves. The
worker's `Result<(), E>` is thus `Ok(())` for any completed round (outcomes in
the side channel) and `Err` only for a genuine unrecoverable storage error.

### Caller loop and interplay with the existing protocol

`lock_shard` / `lock_root` become: submit the request, take the outcome, and

- `Locked { membership }` → return it (record the held lock for diagnostics);
- `Conflict` → return `Conflict` (bubbles up to `Algo`, which releases and
  re-acquires under the same id, ADR-024);
- `Wait(holder)` → `wait_for_holder` (poll `Monitor::wait_for_tx` with backoff),
  then **re-submit** to the dedup — the hold-and-wait loop, now expressed as a
  re-submission rather than a direct reload.

Because the change is confined beneath the per-object step:

- **Parallel vs serial acquisition** (ADR-020) is unaffected: a transaction still
  locks its shards concurrently (or in sorted order under the serial fallback);
  the dedup only merges _different_ transactions on the _same_ shard.
- **The equal-priority serial guarantee** holds: same-key exclusive contenders
  queue rather than merge, so the FIFO front always installs its lock and makes
  progress; wound-wait keeps its **no prefix tiebreak** rule.
- **Write-back and release stay direct CAS** (`write_back_shard`,
  `release_shard_locks`, `release_root`): they are idempotent, version-
  conditional, and best-effort, so a concurrent acquire CAS merely retries.
  Deduplicating write-back is a possible future extension, deliberately out of
  scope here to keep the change confined to acquisition.
- **Cancellation** stays safe: on the `MAX_DEADLOCK_TIMEOUT` drop, `Algo` drops
  the `lock` future, and `Dedup`'s `DriverGuard` / `WaiterDropGuard` hand off or
  prune the in-flight round. A handed-off owner may still install the dropped
  transaction's **own** lock, but that is idempotent and harmless: `release_locks`
  clears it, the serial re-acquire skips it as self, and write-back releases it.

## Consequences

- Under shard contention, N transactions with compatible (disjoint or shared-
  read) accesses to one shard cost **one** GET + **one** CAS instead of N of each
  plus retry churn — v1's deduplication win, now at shard granularity. This is the
  direct payoff of the sharded directory that the v2 MVP left unrealised.
- The false-sharing cost of ADR-020 shrinks from _"racing CASes"_ to _"one merged
  CAS"_ for compatible contenders; genuinely conflicting same-key contenders
  still serialize through wound-wait, exactly as before.
- The concurrency semantics are **unchanged**: same wound-wait priority, same
  hold-and-wait, same serial fallback, same in-doubt (ADR-009) recovery. The
  dedup is a pure batching/serialization layer beneath the per-object lock step.
- The dormant `Dedup` primitive, `Locker::close`, and `Locker::dedup_snapshot`
  become live again (the latter now reports shard/root coordination state for
  operators investigating hangs), rather than being newly built.
- Introducing per-transaction outcomes requires a side channel because `Dedup`
  fans out a single shared result; this is the one structural addition, and it is
  ordered against delivery so a caller always reads its own outcome.
- The backend op stream changes shape (fewer ops, merge-dependent ordering) but
  stays **deterministic** under the simulation executor, so the run-to-run op-
  stream self-check and the serializability / cycle DST oracles remain the safety
  net (v1 ran the same primitive under the fuzzer).
