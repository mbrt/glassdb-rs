# ADR-041: Epoch-versioned collection catalog

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md).

[ADR-046](046-incarnation-addressed-collections.md) defines the authoritative
model for collection identity and path resolution, and [ADR-047] defines its
ordinary-transaction semantics. This ADR must version those ID-based parent
directories; its earlier reusable name-derived `_i` tombstone model does not
carry forward.

[ADR-047]: 047-transactional-collection-management.md

On acceptance, this supersedes the ADR-016, ADR-018, and ADR-031 clauses that
make the physical `_i` root authoritative for collection existence and
parent-child membership. It also supersedes ADR-022's unconditional deletion
rule for a reusable `_i` root. The B-link root remains the fixed routing entry
point.

## Context

Collection existence and subcollection membership live in transactional,
root-local `name → CollectionId` directories. Their current-state transaction
outcome is atomic with data, but that state cannot yet be read at the same
historical cut as retained data versions.

## Decision

Version collection existence, stable incarnation identity, and parent-child
membership as epoch records derived from ADR-047's transactional directory
effects. Catalog history uses the same transaction certificate and retention
protocol, so data and directory changes from one transaction have one atomic
outcome and epoch. The current `name → CollectionId` directory remains the
authoritative non-snapshot lookup structure.

Collection creation first records the path, incarnation, and digest of its
planned physical B-link root in the transaction's durable preparation manifest
and immutable initialization witness. That manifest is a GC root while the
transaction is pending. Creation then writes and verifies the root before
atomically committing the incarnation's existence record and its parent's
membership record. After visibility the root may change, so sealers verify the
immutable witness and the current root's stable incarnation binding rather than
requiring its initial digest forever.

Incarnation-unique collection prefixes remain conditionally deletable because
their IDs are never reused. Historical catalog records retain the dropped ID
through the snapshot horizon, so a recreated logical name cannot alias the
older incarnation. Visible catalog state never names a missing or differently
bound root.

## Consequences

- Collection existence, subcollection enumeration, and data reads share one
  global cut across collections.
- Physical roots become routing objects rather than logical collection-history
  authorities.
- Creation writes physical state before logical visibility and needs aborted
  root tombstoning plus reclamation of never-reused child objects.
- Current root-local directories remain the live lookup index; the catalog adds
  historical versions rather than reintroducing name-derived physical roots.
