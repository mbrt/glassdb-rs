# ADR-033: Transactional key iteration (in-transaction scans)

## Status

Proposed — **deferred**. Builds on [ADR-032](032-node-locking-and-coordinated-splits.md)
(the node-level lock taxonomy and the membership-vs-object-version separation this
scan relies on) and on [ADR-031](031-dynamic-range-sharding.md) (the
order-preserving B-link topology that makes ordered/range scans native).

> **Revision note.** This ADR's original body (which assumed the fixed
> hash-sharding of ADR-001/017 and the single collection-root membership of
> ADR-018) is **superseded and has been removed** — it conflicted with ADR-031
> (order-preserving, range-partitioned keys) and ADR-032 (per-leaf membership,
> validated by status-aware resolution with a per-leaf **membership version** as
> the OCC fast-path token). Only the settled decisions for the eventual rewrite
> remain, recorded next; the ADR is still deferred and must be written out in
> full before acceptance.

### Decisions settled for the rewrite

- **API shape.** A real range API, cost proportional to the range scanned (not the
  collection): half-open `[start, end)` over raw key bytes plus a `prefix` helper;
  **keys-only** (values via `tx.read`, each its own tracked read); an optional
  `limit`; **no reverse** in v1 (right-only sibling links make it costly). One-shot
  **materialized page**. Paging must not re-return the last key: since `start` is
  **inclusive**, the API exposes an **excluded lower bound** (an
  `after`/exclusive-start variant) or an **opaque continuation token**, and paging
  re-issues with that set to the last returned key. (The `last_key` + `0x00`
  successor trick is _not_ used: it is only a valid lexicographic successor when
  keys can always grow by a byte, which fails if a maximum key length exists.)
  `limit = 0` returns an empty page and validates only the `[start, start)` empty
  predicate (`U = start`). Each page is its own serializable transaction.
- **Conflict scope.** Cost proportional to what was read: the effective upper bound
  is `U = greatest key returned in the page` when `limit` is reached, else `end`;
  only leaves overlapping `[start, U]` contribute to the read set. `U` is the
  greatest key actually surfaced to the caller (after the merge with staged
  writes), because the frontier must protect the whole key-space the page
  examined — bounding by "last _committed_ key consumed" would leave a gap between
  it and a larger staged key that was also returned, through which a phantom could
  slip.
- **Validation.** Membership is authoritatively judged by **status-aware
  resolution** over the covered range (help-forwarding committed holders, skipping
  the transaction's own pending writes — the same resolution reads use), **not** by
  a token materialized in the leaf. Being status-aware, value overwrites (key still
  present), pure splits (same key set), and the transaction's own staged create
  (still pending) do not falsely invalidate the scan. The per-leaf **membership
  version** (ADR-032) is the OCC fast path over that resolution: a scan may skip
  re-resolving a covered leaf iff **(a)** its membership version is unchanged
  _and_ **(b)** every pending membership-W holder the scan observed there is still
  non-committed at validation. Condition (b) is essential — a create that commits
  mid-scan but has not written back does not bump the version, so version-equality
  alone would let it slip past as a phantom. A membership change in a covered leaf,
  or a covered-set change a split cannot absorb via right-links, triggers a retry.
- **Isolation.** A transaction that performs **only** scans and reads runs on the
  OCC fast path (membership-version compare, no locks). A transaction that
  contains **any scan and any write** must **predicate-lock every scanned range**
  (structure-R + membership-R over each covered leaf) — the engine cannot tell
  whether the write depends on the scan, so the rule is syntactic, not
  dependency-based. Holding the lock is not enough on its own: because the
  optimistic scan ran before the locks, the transaction must **re-resolve (or
  revalidate) membership over the range while the locks are held** before commit;
  locking only the previously observed leaves does not prove the earlier scan is
  still current. OCC-only validation is unsound for scan-then-write (it cannot
  serialize two "create iff count == N" transactions, which would both commit and
  produce a phantom write-skew). Create/delete conflict with the held
  membership-R lock; a concurrent split conflicts with the held structure-R.
  For a **limited** scan, revalidation under lock can move the frontier: if the
  last returned key was deleted between the optimistic scan and lock acquisition,
  filling the page needs a key beyond the locked `U`, which the original coverage
  neither validates nor gap-protects. Revalidation is therefore an
  **expanding-frontier fixpoint**: (1) recompute the page and `U`; (2) acquire any
  additional leaf locks up to the new `U`; (3) revalidate under the enlarged
  coverage; (4) repeat until both the covered set and the page are stable, then
  commit.
- **Read-your-own-writes.** A scan reflects the transaction's staged mutations
  (creates added, deletes removed, overwrites leave membership unchanged) before
  `limit` and before `U` are computed. Validation runs before write-back, and the
  transaction holds its own membership-R and membership-W locks (a tx does not
  conflict with itself), so a transaction that scans and writes the same range does
  not invalidate its own scan.
- **Non-existent collection.** Scanning a collection whose root `_i` does not exist
  is a collection-missing error, not an empty result.
- **Out of scope.** Reverse scans; a stateful in-transaction cursor; transactional
  sub-collection enumeration.

---

## Context

Iteration must become a first-class transactional operation: a scan performed
inside a `Database::tx` body participates in that transaction's read set, is
validated at commit, is phantom-protected, reflects the transaction's own staged
mutations, and composes with the reads/writes around it under strict
serializability. Today's `Collection::keys`
(`crates/glassdb/src/collection.rs`) already runs inside a read-only
`Database::tx` (calling `tx.keys`), but its validation has the pending-commit hole
of F2 (a create that has committed but not written back can be missed), and there
is no *in-transaction* scan a caller can interleave with its own reads and
writes — so a caller cannot atomically combine "enumerate the keys" with reads and
writes derived from that enumeration.

Under ADR-031/032 keys are order-preserving and range-partitioned, so a bounded
scan is native and its cost is proportional to the range, not the collection —
the settled decisions above are written against that model. The full Decision and
Consequences will be authored when this ADR is picked up; they were intentionally
removed here because the previous draft described the superseded hash-sharded
design and would mislead if kept as normative text.
