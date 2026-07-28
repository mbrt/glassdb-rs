# ADR-039: Epoch-versioned key history

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md).

On acceptance, this supersedes ADR-019's unified value-placement decision and
[ADR-051](051-inline-latest-values.md)'s logless direct-commit guarantee.
ADR-051's inline current state remains a latest-read optimization, but every
snapshot-era writer also emits certified history. The single atomic transaction
outcome and the lock/revalidate ordering remain.

## Context

The latest-value engine may keep the authoritative current bytes directly in a
leaf entry or refer to a transaction object. Ordinary full commits do not record
their actual predecessor, and values for all keys in a logged transaction share
one object. That is sufficient for current-value resolution, but an hour-long
history window would let one cold key pin unrelated values from a large
transaction. An inline value is likewise only the current version and would be
lost when overwritten. A linear predecessor walk is also unbounded for a hot
key.

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

This is the mandatory write format once snapshot reads are implemented. There
is no runtime latest-only database mode: temporarily rejecting new snapshot
admission under ADR-040 does not stop history emission.

Maintain one total, acyclic history per logical key. Each version records the
actual effective predecessor captured while its install lock is held;
transaction-body pre-checks are not authoritative. Tombstones are ordinary
versions, preserving create, delete, and recreate history.

Index retained immutable history by epoch so lookup finds the newest certified
version at or before a cut with bounded work rather than a linear walk through
every overwrite. The current leaf entry identifies the history head and may
also retain ADR-051's inline bytes for strict latest reads. Inline bytes never
replace the immutable historical payload or shared certificate. After a delete,
retain that key-directory entry and head while any admissible or live cut can
resolve the key to a present version; prune it only after all such cuts observe
absence. Point lookup and forward `KeyScan` traversal use this invariant.

Treat a committed certificate with a missing or mismatched manifest payload as
corruption, never as absence or a partial transaction.

The baseline protocol does not retain ADR-051's one-CAS logless commit: every
writer must durably emit its history and certification. Preserving comparable
single read-write latency while satisfying that invariant remains a research
goal.

## Consequences

- Per-key values can be reclaimed independently and hot-key lookup is bounded.
- Strict latest reads may still avoid a history-object lookup when the leaf
  carries an inline current value.
- Preparing and verifying the manifest adds work before the commit point.
- The latest-value engine's one-CAS logless path is not available in the
  baseline snapshot protocol.
- Multi-key atomicity depends on retaining the one certification record shared
  by every corresponding history entry.
- Deleted keys may retain directory entries and old floor versions until no
  permitted cut can observe them.
- The format creates more immutable objects and needs history-index compaction
  and sizing policies.
