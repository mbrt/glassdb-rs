# ADR-062: Splitter-driven tombstone reclamation

## Status

Accepted — implemented.

Refines [ADR-051](051-inline-latest-values.md)'s tombstone lifetime and
[ADR-061](061-atomic-logless-single-leaf-commits.md)'s logless deletes.

Supersedes [ADR-022](022-garbage-collection-mark-sweep.md)'s rule that a current
writer is never cleared, only for `Tombstone` state. Inline and external current
writers remain live value references. It also refines
[ADR-029](029-gc-through-shard-coordinator.md)'s vestigial-entry rule and
[ADR-032](032-node-locking-and-coordinated-splits.md)'s membership version by
making that version the generation for unmarked point absence.

It refines [ADR-031](031-dynamic-range-sharding.md) and
[ADR-056](056-demand-driven-inline-pressure-splits.md) by compacting a leaf
before the final split decision. Their topology and pressure policies remain.

This is a protocol-incompatible change and establishes database protocol v3.

## Context

A tombstone is currently permanent until a later write replaces it. The
transaction-object collector never clears one: GC starts from transaction
objects and checks their recorded back-references, while a logless tombstone
has no transaction object or durable cleanup candidate at all.

Repeated deletion of one key does not accumulate entries, but distinct deleted
keys preserve the collection's historical key set in its leaves. Tombstones can
therefore consume enough entry and encoded-byte capacity to cause splits that
move only durable absence. Leaves and topology never shrink afterward because
merge is not implemented.

Naively removing a tombstone is unsafe. A point read made after removal records
unmarked absence; a later create, delete, and second removal could return to the
same evidence and let the old read validate across an ABA cycle. Tombstones are
also direct-commit recovery markers, so reclaiming them can make an uncertain
commit impossible to resolve.

The splitter already visits a complete leaf under the structural gate exactly
when retained entries justify structural work. This is the natural place to
compact absence before deciding whether the tree must grow.

## Decision

### Use the membership version as an absence generation

The leaf membership version also serves as the durable generation for a point
read that observes no writer. Such a read records both logical absence and the
leaf generation. After physical leaf evidence changes, it validates only if the
key is still absent and the generation is unchanged.

A read of `Tombstone { writer }` continues to record the exact writer. Removing
that tombstone invalidates the read because the writer disappears. A read made
after removal records the absence generation instead.

A leaf CAS that contains any real absent-to-present or present-to-absent
transition changes the generation. The regular locked protocol already records
membership-write activity; a logless ADR-061 create or delete changes it in the
direct commit CAS. A delete of an already absent key need not change it because
logical membership did not change.

Tombstone reclamation itself does not advance the generation: it changes the
representation of absence, not the live key set. A later membership cycle does
advance it and therefore cannot validate as the earlier unmarked absence.

Splits preserve the source generation in their output leaves. They relocate
membership without changing it; existing covered-leaf and topology validation
continues to detect relevant physical movement.

The generation is leaf-granular. A membership change to another key in the same
leaf may conservatively invalidate an unmarked absence read. This false retry
is accepted instead of retaining a permanent per-key generation.

### Compact before splitting

When processing a leaf split candidate, the splitter first acquires its ordinary
structural gate and quiesces the leaf under the existing split protocol. It then
removes every holder-free tombstone before making the final split decision.

Compaction is provenance-blind. It applies equally to tombstones written by
logged transactions and to logless direct commits. Distinguishing them would
require a new format bit or a transaction-object lookup per entry and changes
neither logical safety nor the desired leaf contents.

The splitter reevaluates the original split reason against the compacted leaf:

- if compaction removes the need, it persists the compacted leaf and cancels
  the split; and
- if the leaf still needs splitting, it partitions the compacted state through
  the ordinary recoverable split protocol.

All eligible tombstones are removed once the splitter has paid the coordination
cost, not merely enough to fall below a threshold. The source-shrink or
root-rewrite CAS remains the structural linearization point.

There is no periodic tree sweep, durable tombstone queue, direct-commit
compaction, or merge. A cold leaf that remains below every split threshold may
retain tombstones forever. Active ranges that create split pressure can receive
opportunistic cleanup; the tree itself remains at its historical high-water
mark.

### Hand removed logged writers to ordinary GC

After removal is durable, every removed writer ID is submitted as an ordinary
transaction-object cleanup hint. GC applies its existing reverse reference
check and safety horizon; another current value or holder still naming a logged
transaction keeps it live. A logless ID simply has no object to collect.

The existing transaction-object collector does not scan leaves and does not
become responsible for tombstone discovery. Its candidate-driven cost remains
proportional to transaction-object garbage.

### Accept loss of direct recovery evidence

Tombstones have no provenance or publication-age field. The splitter may
reclaim one immediately after it becomes quiescent, including while the client
is recovering an unavailable direct CAS.

An exact surviving marker still proves a whole ADR-061 transaction landed.
When cleanup removes the last marker, recovery reports `InDoubt` unless other
state independently proves the outcome. In particular, unmarked absence after
an all-absent delete attempt cannot prove that its tombstone CAS failed to land.
No marker belonging to a different transaction supplies that proof.

This availability loss is explicit. Delaying cleanup by transaction ID would
not provide a publication-age guarantee because transaction IDs record
transaction start, and adding durable age or provenance solely for recovery is
rejected.

Logged commit recovery remains based on the transaction object rather than the
leaf tombstone. Once the tombstone reference is removed, ADR-022's reverse
check and ADR-057's recovery horizon govern reclamation of that object.

### Require database protocol v3

An older v2 client validates unmarked point absence without the generation and
is unsafe once another client can remove tombstones. The database metadata
version therefore advances to v3 so old binaries fail closed.

Mixed v2/v3 operation and automatic migration are unsupported. Opening a v2
database with a v3 client fails with a clear version error; development
databases must be recreated. No durable feature-negotiation or staged activation
protocol is introduced.

### Expose compaction outcomes

Permanent splitter statistics report:

- the number of tombstone entries reclaimed; and
- split attempts avoided because compaction removed the need.

The second counter applies only when the candidate was actionable before
compaction and no longer requires a split afterward.

## Consequences

- Tombstone pressure can shrink a leaf instead of permanently widening the
  tree.
- Cold under-cap tombstones still have no eventual-reclamation guarantee, and
  empty or underfull nodes are not merged.
- Logged transaction objects may become collectable once their tombstone
  references disappear.
- Unmarked absence reads gain conservative leaf-wide membership conflicts but
  remain safe across create/delete/reclaim cycles.
- Splitter work becomes responsible for a logical compaction decision in
  addition to topology, while transaction-object GC remains candidate-driven.
- Direct delete commits have a larger `InDoubt` surface because their only
  markers may disappear immediately.
- Database protocol v3 prevents old absence-validation semantics from sharing
  the reclaimed representation.

## Alternatives considered

### Extend transaction-object GC to find tombstones

Logless tombstones have no transaction object or back-reference from which the
current collector can discover them. A forward leaf scan would make GC cost
proportional to database size rather than garbage.

### Enqueue every delete for background compaction

A volatile queue can be lost, while a durable queue adds another write to the
strictly one-CAS direct path. It also risks making background CAS volume
proportional to deletes even when leaves have ample capacity. Split pressure is
the workload signal that justifies compaction.

### Retain tombstones permanently

This preserves per-key recovery and absence versions but makes deleted history
consume leaf capacity forever and can cause avoidable irreversible splits.

### Keep a compact per-key absence generation

Per-key generations avoid leaf-wide false retries, but retain one durable entry
for every deleted key and therefore do not solve historical membership growth.

### Reclaim tombstones in a foreground direct CAS

Foreground compaction could create headroom without another write, but it would
erase unrelated recovery markers and couple direct admission to maintenance.
The splitter provides one coordinated owner for that policy.

### Delay reclamation for a fixed age

Without a publication timestamp, transaction age is not marker age. A finite
delay also only moves the recovery race, while adding a durable timestamp or
provenance field expands the format for an availability optimization.
