# ADR-039: Timestamp-versioned key history

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md). Versions are ordered by
the commit timestamps [ADR-038](038-hlc-snapshot-cuts.md) assigns.

On acceptance, this supersedes ADR-019's unified value-placement decision, the
corresponding ADR-020 clauses that make the committed transaction body the only
durable home of every value, and
[ADR-051](051-inline-latest-values.md)'s logless direct-commit guarantee. It
also refines [ADR-031](031-dynamic-range-sharding.md)'s soft split cap to count
live entries only; its split protocol, right-link traversal, and hard object cap
are unchanged.
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

That order is total over versions as written, not over versions as retained.
ADR-040 coalesces away versions no admissible cut can observe, so a predecessor
reference may point at a reclaimed version. Predecessor capture exists to record
what was actually effective at install time, which is what makes the order
trustworthy; it is not a claim that the referent survives. Lookup accordingly
goes through the timestamp index rather than walking the chain.

Index retained immutable history by commit timestamp so lookup finds the newest
certified version at or before a cut with bounded work rather than a linear walk
through every overwrite. After a delete, the key must stay resolvable to its
pre-delete version while any admissible or live cut can observe one, and become
unresolvable only once every such cut observes absence. Point lookup and forward
`KeyScan` traversal depend on that enumeration invariant.

### Keep deleted-key residue out of the live leaf

Two obligations hold a deleted key's entry, and they differ by orders of
magnitude in duration. Strict optimistic validation needs the tombstone as a
validation token while a concurrent transaction may still validate against it,
which ADR-022 already bounds by the lease. Snapshot enumeration needs the key
resolvable for the whole retention window. Only the first belongs on the strict
path, and keeping both in the leaf is what puts an hour-long obligation into the
object every strict read and write loads.

Once a deletion's slot closes, move its residue — the key, its history head, and
the delete timestamp — out of the leaf into a side structure covering the same
key range, batched per slot rather than per deletion. The leaf then carries live
keys only. Strict reads and writes never load the side structure. A snapshot
scan reads it alongside the leaf under the same freshness rule and merges the
two ordered streams; a snapshot point read consults it only for a key the leaf
does not have. It splits with its leaf, is a GC root for the history it names,
and is reclaimed as a whole once its newest entry leaves the window.

Count only live entries toward [ADR-031](031-dynamic-range-sharding.md)'s soft
split cap, so reclaimable residue can never trigger a split. This refines that
trigger rather than replacing it: a leaf still splits when its encoded size
threatens the hard object cap, which is a limit rather than a policy choice.

The current leaf entry identifies the history head and additionally records that
version's commit timestamp, so that a reader can tell whether the current
version is the newest one at its cut without dereferencing anything. When it is,
and ADR-051's inline bytes are present, the entry answers a read at that cut
outright. This makes inline values a snapshot-read optimization and not only a
strict-read one, which matters because a cold key's current version lies below
almost every admissible cut. Recording the timestamp is what makes the test
local; reading the head to learn it would cost the object the optimization
exists to avoid.

Inline bytes never replace the immutable historical payload or shared
certificate. They are a redundant copy of the newest version, so a cut below it
still resolves through history.

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
- A leaf entry carrying an inline current value answers both strict reads and
  any cut at or above that value's commit timestamp without a second object.
  For cold keys that is most cuts, so a snapshot scan over such a leaf can be
  served from the leaf alone.
- Preparing and verifying the manifest adds work before the commit point.
- An inline value is stored twice, once in the leaf and once as its immutable
  history payload, and the duplicate now persists for the whole retention window
  rather than only until the transaction object is collected. ADR-051's per-value
  and per-leaf budgets were tuned without that term and should be revisited.
- The latest-value engine's one-CAS logless path is not available, so small
  single-key overwrites regress to the logged commit protocol. This is the
  largest single cost of the format and the design's performance gate measures
  it as its own cell.
- Multi-key atomicity depends on retaining the one certification record shared
  by every corresponding history entry.
- Deleted keys stay resolvable until no permitted cut can observe them, but off
  the strict path, so a delete-heavy workload no longer inflates the objects
  ordinary reads and writes load. This matters more than it otherwise would,
  because ADR-031 defers merge: a split driven by residue would never be undone.
- Snapshot scans read one object per leaf more than strict scans do, and a
  snapshot point read for an absent key may too.
- Migrating residue at slot closure is a new background obligation, and a leaf
  and its side structure must split together.
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

### Keep deleted-key residue in the leaf

Leaving the entry where it already is needs no second object, no migration, and
no split coordination, and an earlier revision of this decision assumed it. It
puts a retention-window obligation into the object on the strict path, and the
cost concentrates rather than spreading: a FIFO delete pattern leaves whole
leaves holding no live keys, splitting repeatedly on garbage, and with merge
deferred they stay split after the garbage is gone.

### Exclude residue from split accounting but leave it in the leaf

This alone stops garbage from driving splits and costs almost nothing. It does
not stop residue from being read and rewritten by every operation on the leaf,
and it leaves a leaf able to approach the hard object cap while holding few live
keys. It is worth doing, so it is part of the decision above rather than an
alternative to it.

### Derive absence from per-slot change logs

A reader could reconstruct which keys were deleted since its cut from a per-slot
log of changed keys, needing no side structure at all. A scan would then consult
one log per slot between its cut and the present, which at the maximum lifetime
is hundreds of objects, against one per leaf here. ADR-055 leaves such a log as
future work for a different reason, and if it is ever adopted the side structure
becomes derivable from it; keeping the two separable is what allows that.

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
