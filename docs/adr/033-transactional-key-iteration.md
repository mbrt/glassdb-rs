# ADR-033: Transactional key iteration

## Status

Accepted — implemented.

Builds on [ADR-031](031-dynamic-range-sharding.md) for ordered B-link traversal
and [ADR-032](032-node-locking-and-coordinated-splits.md) for per-leaf structure
and membership coordination.

[ADR-044](044-cas-fenced-structural-gate.md) supersedes this ADR's structure-R
requirement for escalated scans. Membership-R remains the predicate lock and
cross-conflicts with structural-gate acquisition.

## Context

Applications need to enumerate a collection inside a transaction and derive
reads or writes from the result. Treating enumeration as an untracked utility
read permits phantoms: a concurrent create or delete can change the predicate
without conflicting with any key the transaction observed. Scans must therefore
participate in serializable validation while remaining proportional to the
range and page actually read.

## Decision

GlassDB provides forward, keys-only transactional scans over raw key bytes:

- `KeyScan::range(start, end)` scans the half-open range `[start, end)`;
  `prefix` and `all` are conveniences.
- `after(key)` supplies an exclusive lower bound for paging, and `limit(n)`
  bounds a materialized `KeyPage`. Reverse and stateful cursors are out of scope.
- `Transaction::scan_keys` composes scans with other transactional operations.
  `Collection::scan_keys` is the one-transaction convenience form, and `keys`
  remains the compatibility wrapper for a full scan.
- A missing collection is an error. Scans are sorted and reflect the staged
  creates, deletes, and overwrites that precede them in the same transaction.

A scan records both its logical result and the physical leaves through its
effective frontier. When a positive limit is filled, the frontier is the last
key returned; otherwise it is the range end. Validation first uses each leaf's
membership version and pending membership holders as an OCC shortcut. If that
physical evidence changed, it re-resolves the same predicate and accepts the
scan when the logical page is unchanged. Thus value overwrites and pure splits
do not cause false conflicts, while creates and deletes in the observed
predicate do.

Transactions containing both a scan and a write take structure-read and
membership-read locks on every covered leaf and revalidate after locking. If a
limited page's frontier moves outward, the transaction retries while retaining
its locks, extends the locked range, and repeats until the page is stable.

A read-only transaction uses OCC and takes no locks on its first attempt. If
validation fails, the next attempt validates with locks for its complete read
set: entry-read locks for point reads and predicate read locks for scans. This
bounds retry starvation under continuous value or membership churn without
taxing the uncontended read-only path.

## Consequences

- Scan-derived writes are phantom-safe under strict serializability.
- Work and contention are bounded by the scanned range and materialized page,
  not by the whole collection.
- Read-only scans normally remain lock-free, but a conflicted scan may block
  creates, deletes, or splits in its covered leaves while retrying.
- Pages obtained through separate `Collection::scan_keys` calls are separate
  serializable transactions; callers needing one atomic snapshot must perform
  the scans in one `Database::tx` body.
- Continuation is deliberately key-based rather than snapshot-based, so paging
  across separate transactions can observe intervening commits.
