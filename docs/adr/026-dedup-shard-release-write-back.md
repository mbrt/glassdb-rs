# ADR-026: Deduplicated shard release and write-back

## Status

Implemented. Follow-up to [ADR-025](025-dedup-shard-lock-acquisition.md), which
deduplicated lock **acquisition** but deliberately left the release path direct.

The release and write-back deduplication **mechanism** is generalized by
[ADR-028](028-shard-mutation-coordinator.md) into the shared `ShardCoordinator`
(Release and WriteBack become installed resolvers); the batching decision is
unchanged.

## Context

ADR-025 routed `Locker::lock_shard` / `lock_root` through a `Dedup` keyed on the
CAS object path, so several transactions contending one shard collapse to a
single load + CAS. It left **write-back** and **release** as direct per-object
CAS (`write_back_shard`, `release_shard_locks`, `release_root`), scoped out to
keep that change confined to acquisition.

This leaves the win asymmetric. Write-back is on the hot path of **every**
successful commit, and release runs on every abort and on the serial fallback.
Under shard contention, N committing/aborting transactions each still pay their
own GET + CAS and race each other with the bounded `CAS_RETRIES` budget — the
same within-shard false sharing (ADR-020) that ADR-025 removed from acquisition,
still present on the release side.

Releases are in fact the **most** mergeable operations the `Locker` has. A
release never lock-conflicts: it only drops the releasing transaction's own
holder from the touched keys, and write-back additionally sets that
transaction's own `current_writer` / `deleted`, which is monotonic and
idempotent (only the committing writer sets it, only once its status is `Ok`).
This is precisely why v1 marked unlocks reorderable and merged them into any
non-create round.

## Decision

Extend the ADR-025 `Dedup` to also carry **release / write-back** members on the
shard request (and the analogous release beneath `release_root`), keyed on the
same object path, so a release or write-back folds into any round for that
object:

- A release-only request is **reorderable** (`can_reorder == true`) — the v2
  analog of v1 marking unlocks reorderable — so it joins any in-flight batch
  instead of FIFO-blocking behind an unrelated acquirer.
- A release / write-back member always merges (it cannot exclusively conflict),
  and the worker applies it in the single merged round: remove the
  transaction's holder from its keys, and for write-back set its committed
  pointer. This is the same per-object mutation the direct methods do today,
  moved inside the batched round.
- The per-transaction outcome for a release is trivial (done), so it needs no
  `membership` / `Wait` channel — only the acquisition members do.

The change stays **beneath** the per-object step, so the commit/write-back
protocol (ADR-020), wound-wait priorities (ADR-021), hold-and-wait (ADR-024),
and in-doubt recovery (ADR-009) are unchanged.

## Consequences

- N committing / aborting transactions on one shard collapse from N GET+CAS to
  one, extending ADR-025's deduplication win to the commit/abort hot path —
  arguably the larger win, since write-back runs on every successful commit.
- Merging couples fates only weakly: a precondition miss reloads the whole
  batch, but releases are idempotent so a merged retry is always safe. For the
  same reason a cancelled/handed-off release is harmless — safer than the
  acquisition hand-off ADR-025 already reasoned about.
- The backend op stream changes shape again (fewer ops on the release path,
  merge-dependent ordering) but stays deterministic under the simulation
  executor, so the op-stream self-check and the serializability / cycle oracles
  remain the safety net.
