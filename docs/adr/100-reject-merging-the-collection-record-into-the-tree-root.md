# ADR-100: Reject merging the collection record into the tree root

## Status

Accepted.

Keeps [ADR-050](050-separate-collection-record-and-tree-root.md). This record
adds measurements to its "Keep the combined `_i`" alternative.

## Context

[ADR-050](050-separate-collection-record-and-tree-root.md) stores each
collection in two fixed-path objects: the collection record at `_i` and the
tree root at `_r`. Each collection thus has one more object. A merge into one
object would remove it again. The question is whether the merge saves enough
backend operations to pay for its costs.

Point accesses, scans, and commits route from `_r` and do not read the
collection record. A covered binding read takes no directory lock and no
validation ([ADR-092](092-key-accesses-cover-the-binding-of-their-collection.md)).
Only these operations use `_i`:

- **Collection changes.** A create prepares `_i` and `_r`. A drop freezes the
  topology in `_i`, fences `_r`, and later reclaims both objects.
- **Structural changes.** Each restructurer sweep adds its topology
  participants to the collection record of each collection that it changes,
  and removes them after the changes. Without contention, one sweep thus does
  two successful CASes on the record of each collection, for any number of
  splits and merges (`one_sweep_of_splits_writes_the_collection_record_twice`).
- **Collection opens.** A database open reads the record of the root
  collection. An open by name reads the binding in the record of the parent
  collection. Neither open reads the record of the opened collection.

In the perfbench experiments, the collection record has at most 1.8% of the
backend operations of a measure window, and 0% in all `ci/counters`
experiments. The investigation log
([hack/perf/investigations.md](../../hack/perf/investigations.md), entry of
2026-11-18) has the method and the results for each cell.

## Decision

Keep the collection record and the tree root in separate objects. The reasons
are:

- **The saving is small.** A merge removes one object write from each create,
  one fence from each drop, and one delete from each reclaim. An open saves a
  read only if the merged object of the parent is not in the cache and the
  caller also reads the keys of the parent. Collection changes are rare, and
  the principles let them be slow. A merge can remove at most 1.8% of the
  measured operations.
- **The merged object is the hot leaf.** A collection with one leaf has only
  `_r`. In a merged object, the topology participant writes of each sweep and
  each directory write would contend with the key writes on that leaf.
- **The costs of ADR-050 come back.** The directory and the root leaf share
  one revision and one size limit again. Storage, routing, splits, and drops
  must handle a root leaf differently from all other leaves.

## Consequences

- Each collection keeps two fixed objects. A create, a drop, and reclamation
  each cost one operation more than with one object.
- Key operations do not touch the collection record. A change to the
  collection record cannot make a leaf CAS fail.
- If a later workload makes collection changes frequent, the measurement can
  be repeated with that workload.

## Alternatives considered

### Move only the topology participants into `_r`

This removes the record writes of each sweep, but they then contend with the
key writes on the tree root. Without contention, each sweep already does only
two successful CASes on the record of each collection, so the move saves
little.
