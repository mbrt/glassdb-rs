# ADR-039: Epoch-versioned key history

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md).

On acceptance, this supersedes ADR-019's unified value-placement decision and
the corresponding ADR-020 clauses that make the committed transaction body the
only durable home of every value. The single atomic transaction outcome and the
lock/revalidate ordering remain.

## Context

The current shard entry retains only a current writer, ordinary full commits do
not record their actual predecessor, and values for all keys in a transaction
share one transaction object. That is sufficient for current-value resolution,
but an hour-long history window would let one cold key pin unrelated values from
a large transaction. A linear predecessor walk is also unbounded for a hot key.

Splitting values from status must preserve ADR-019's central durability
invariant: a terminal committed outcome cannot name a value that is absent.

## Decision

Separate small transaction certification metadata from independently
reclaimable, immutable per-key payloads. Before creating payloads, durably
prepare an authoritative manifest that names their paths and digests and acts as
a GC root. Before terminal commit, write and verify every named payload. Epoch
admission and the terminal certificate name the manifest identity and digest.
One certificate still gives all data and catalog writes one atomic outcome and
epoch. Durable abort releases prepared objects for reclamation.

Maintain one total, acyclic history per logical key. Each version records the
actual effective predecessor captured while its install lock is held;
transaction-body pre-checks are not authoritative. Tombstones are ordinary
versions, preserving create, delete, and recreate history.

Index retained immutable history by epoch so lookup finds the newest certified
version at or before a cut with bounded work rather than a linear walk through
every overwrite. The current leaf entry identifies the history head. After a
delete, retain that key-directory entry and head while any admissible or live cut
can resolve the key to a present version; prune it only after all such cuts
observe absence. Point lookup and forward `KeyScan` traversal use this
invariant.

Treat a committed certificate with a missing or mismatched manifest payload as
corruption, never as absence or a partial transaction.

## Consequences

- Per-key values can be reclaimed independently and hot-key lookup is bounded.
- Preparing and verifying the manifest adds work before the commit point.
- Multi-key atomicity depends on retaining the one certification record shared
  by every corresponding history entry.
- Deleted keys may retain directory entries and old floor versions until no
  permitted cut can observe them.
- The format creates more immutable objects and needs history-index compaction
  and sizing policies.
