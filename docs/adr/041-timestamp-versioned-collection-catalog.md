# ADR-041: Timestamp-versioned collection catalog

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md).

[ADR-046](046-incarnation-addressed-collections.md) defines the authoritative
model for collection identity and path resolution, and [ADR-047] defines its
ordinary-transaction semantics. This ADR versions those ID-based parent
directories; the earlier reusable name-derived `_i` tombstone model does not
carry forward.

[ADR-047]: 047-transactional-collection-management.md

On acceptance, this supersedes the ADR-016, ADR-018, and ADR-031 clauses that
make the physical `_i` root authoritative for collection existence and
parent-child membership. The B-link root remains the fixed routing entry point.

## Context

Collection existence and subcollection membership live in transactional,
root-local `name → CollectionId` directories. Their current-state transaction
outcome is atomic with data, but that state cannot yet be read at the same
historical cut as retained data versions.

## Decision

Version collection existence, stable incarnation identity, and parent-child
membership as timestamped records derived from ADR-047's transactional directory
effects. Catalog history uses the same transaction certificate, commit
timestamp, and retention protocol as data, so directory and data changes from
one transaction share one atomic outcome and one cut position. The current
`name → CollectionId` directory remains the authoritative non-snapshot lookup
structure.

Collection creation first records the path, incarnation, and digest of its
planned physical B-link root in the transaction's durable preparation manifest
and immutable initialization witness. That manifest is a GC root while the
transaction is pending. Creation then writes and verifies the root before
atomically committing the incarnation's existence record and its parent's
membership record. After visibility the root may change, so helpers verify the
immutable witness and the current root's stable incarnation binding rather than
requiring its initial digest forever.

Incarnation-unique collection prefixes remain conditionally deletable because
their IDs are never reused. Historical catalog records retain the dropped ID
through the snapshot horizon, so a recreated logical name cannot alias the
older incarnation. Visible catalog state never names a missing or differently
bound root.

## Consequences

- Collection existence, subcollection enumeration, and data reads share one cut
  across collections.
- Physical roots become routing objects rather than logical collection-history
  authorities.
- Creation writes physical state before logical visibility and needs aborted
  root tombstoning plus reclamation of never-reused child objects.
- Current root-local directories remain the live lookup index; the catalog adds
  historical versions rather than reintroducing name-derived physical roots.

## Alternatives considered

### Leave collection existence out of the cut

Data reads could resolve at the cut while collection existence and membership
resolved at current state. Enumerating subcollections would then report a
collection that did not exist at the cut, or omit one that did, and a scan over
it would disagree with a point read of the same key. Cross-collection internal
consistency is the property the snapshot contract sells; excluding the catalog
from it makes the promise conditional on what the caller happens to touch.

### Keep the physical root as the authority for existence

The current format infers existence from the presence of the routing object,
which needs no catalog at all. A physical root is mutable and is reclaimed on
its own schedule, so it cannot answer whether a collection existed at a past
cut, and treating its absence as historical absence would turn a reclamation
decision into a logical answer.

### Derive historical existence from data history

A collection could be treated as existing at a cut if any of its keys had a
visible version there. Empty collections would vanish and reappear, creation and
deletion of an empty collection would be invisible, and enumeration would depend
on retained data rather than on the transaction that actually changed
membership.

### Name-derived reusable roots with tombstones

An earlier revision of this decision reclaimed a reusable `_i` path to a
CAS tombstone so that a delayed reclamation could not erase a newer incarnation.
ADR-046 and ADR-047 replaced name-derived paths with incarnation-addressed
identity, which removes reuse entirely and makes the tombstone unnecessary.
