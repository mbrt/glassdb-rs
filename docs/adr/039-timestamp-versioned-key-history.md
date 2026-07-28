# ADR-039: Timestamp-versioned key history

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md). Versions are ordered by
the commit timestamps [ADR-038](038-hlc-snapshot-cuts.md) assigns.

On acceptance, this supersedes ADR-019's unified value-placement decision, the
corresponding ADR-020 clauses that make the committed transaction body the only
durable home of every value, and
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
a GC root. Before terminal commit, write and verify every named payload. The
terminal certificate names the manifest identity and digest and carries the
transaction's commit timestamp. One certificate still gives all data and catalog
writes one atomic outcome and one timestamp. Durable abort releases prepared
objects for reclamation.

This is the mandatory write format once snapshot reads are implemented. There is
no runtime latest-only database mode: temporarily rejecting new snapshot binds
under ADR-040 does not stop history emission.

Maintain one total, acyclic history per logical key, ordered by commit
timestamp. Each version records the actual effective predecessor captured while
its install lock is held; transaction-body pre-checks are not authoritative.
Tombstones are ordinary versions, preserving create, delete, and recreate
history.

Index retained immutable history by commit timestamp so lookup finds the newest
certified version at or before a cut with bounded work rather than a linear walk
through every overwrite. The current leaf entry identifies the history head and
may also retain ADR-051's inline bytes for strict latest reads. Inline bytes
never replace the immutable historical payload or shared certificate. After a
delete, retain that key-directory entry and head while any admissible or live
cut can resolve the key to a present version; prune it only after all such cuts
observe absence. Point lookup and forward `KeyScan` traversal use this
invariant.

Treat a committed certificate with a missing or mismatched manifest payload as
corruption, never as absence or a partial transaction.

The baseline protocol does not retain ADR-051's one-CAS logless commit: every
writer must durably emit its history and certification. An inline-eligible
overwrite therefore falls back to ADR-027's logged parallel path, which
[ADR-038](038-hlc-snapshot-cuts.md) leaves otherwise untouched. Preserving
comparable single read-write latency while satisfying this invariant remains a
research goal.

## Consequences

- Per-key values can be reclaimed independently and hot-key lookup is bounded.
- Strict latest reads may still avoid a history-object lookup when the leaf
  carries an inline current value.
- Preparing and verifying the manifest adds work before the commit point.
- The latest-value engine's one-CAS logless path is not available, so small
  single-key overwrites regress to the logged commit protocol. This is the
  largest single cost of the format and the design's performance gate measures
  it as its own cell.
- Multi-key atomicity depends on retaining the one certification record shared
  by every corresponding history entry.
- Deleted keys may retain directory entries and old floor versions until no
  permitted cut can observe them.
- The format creates more immutable objects and needs history-index compaction
  and sizing policies.

## Alternatives considered

### Keep ADR-019's unified transaction object and walk `prev_writer`

The accepted format already stores every value of a transaction in one object
and can reach older values by following the writer chain, so no new object class
would be needed. Over an hour-long window it fails twice: one cold key pins
every unrelated value written by the same transaction, and a hot key's chain
grows without bound, making a historical lookup linear in the number of
overwrites rather than in the number of cuts.

### Copy-on-write tree snapshots

Versioning the tree instead of the keys, as bbolt does, makes a cut nearly free
to create and to read. It depends on a single writer publishing a new root, and
on holding pages until the last reader releases them. This database has many
independent writers with no publication point to serialize on, and pin-free
retention is a requirement rather than a preference.

### Keep history inside the transaction certificate

Retaining superseded values in the certificate that already provides the atomic
outcome would preserve multi-key atomicity for free. It reproduces the pinning
problem exactly, and makes per-key reclamation impossible because the unit of
storage is the transaction rather than the key.

### Timestamp-named version objects with no index

Naming each version by key and commit timestamp would make lookup a direct read
if the exact timestamp were known. A reader knows a cut, not a version, so
finding the newest version at or before it would require either a linear walk or
listing, and listing is not a supported lookup primitive.
