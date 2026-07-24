# ADR-041: Epoch-versioned collection catalog

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md).

[ADR-046](046-incarnation-addressed-collections.md) proposes the authoritative
model for collection identity and path resolution, and [ADR-047] proposes its
ordinary-transaction semantics. If these proceed, this ADR must version their
ID-based parent directories; its reusable name-derived `_i` tombstone model does
not carry forward.

[ADR-047]: 047-transactional-collection-management.md

On acceptance, this supersedes the ADR-016, ADR-018, and ADR-031 clauses that
make the physical `_i` root authoritative for collection existence and
parent-child membership. It also supersedes ADR-022's unconditional deletion
rule for a reusable `_i` root. The B-link root remains the fixed routing entry
point.

## Context

Collection existence and subcollection membership currently live in mutable
root-local metadata. Creating a child root and registering it in the parent are
separate operations. That state cannot be read at the same historical cut as
data, and a crash can expose only half of collection creation.

## Decision

Store collection existence, stable incarnation identity, and parent-child
membership as epoch-versioned records in a system catalog. Catalog mutations use
the ordinary transaction certificate, history, and retention protocol, so data
and catalog writes from one transaction have one atomic outcome and epoch.

Collection creation first records the path, incarnation, and digest of its
planned physical B-link root in the transaction's durable preparation manifest
and immutable initialization witness. That manifest is a GC root while the
transaction is pending. Creation then writes and verifies the root before
atomically committing the incarnation's existence record and its parent's
membership record. After visibility the root may change, so sealers verify the
immutable witness and the current root's stable incarnation binding rather than
requiring its initial digest forever.

Never issue an unconditional delete for the reusable fixed `_i` path. Durable
abort CAS-compacts it to a permanent incarnation-bearing tombstone. A later
creation replaces only the exact observed tombstone by CAS, so delayed old
reclamation cannot erase a newer incarnation. Incarnation-unique non-root paths
remain deletable because their identities are never reused. Visible catalog
state never names a missing or differently bound root.

Collection deletion remains out of scope, but incarnation identity is part of
the format so future recreation cannot alias historical state.

## Consequences

- Collection existence, subcollection enumeration, and data reads share one
  global cut across collections.
- Physical roots become routing objects rather than logical collection-history
  authorities.
- Creation writes physical state before logical visibility and needs aborted
  root tombstoning plus reclamation of never-reused child objects.
- Existing root-local metadata and non-transactional parent registration are
  replaced rather than backfilled; the format is greenfield.
