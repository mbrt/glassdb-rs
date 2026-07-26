# ADR-050: Separate the collection record from the B-link tree root

## Status

Proposed.

On acceptance, this supersedes the ADR-031 and ADR-046 clauses that make `_i`
both the collection record and the B-link tree root. The B-link topology,
fixed-address root split, transactional collection semantics, and all-node
deletion fencing remain unchanged.

Implementation, format compatibility, and rollout are deferred.

## Context

ADR-031 placed collection metadata and the B-link root node in one `_i` object
to minimize object kinds and let a small collection use that object directly as
its only leaf. Transactional collection management has since added directory
coordination and topology-lifecycle state to the same object.

The two parts now have independent responsibilities but share a CAS revision
and size limit. A key mutation in a root leaf conflicts with a collection
metadata mutation, even though their logical fields are disjoint. The combined
representation also requires storage, routing, coordination, splitting, and
lifecycle code to treat a root leaf differently from every other node.

Collection creation, deletion, and root splits are rare, so this coupling is
not primarily a throughput concern. The stronger motivation is a stable
boundary between collection control state and key coordination state.

## Decision

Use separate fixed-path objects beneath each incarnation-addressed collection
prefix:

```text
_i             collection record
_r             B-link tree root node
_n/<node-id>   non-root B-link tree node
```

The collection record contains the direct-child directory and its transactional
coordination, plus collection-wide topology and lifecycle state. It contains no
B-link node or per-key coordination state.

The `_r` object has the same node representation and coordination semantics as
other B-link nodes. It is initially a leaf and becomes an index through the
existing in-place root-split protocol. Its path never changes and `_i` contains
no root pointer.

Key routing starts directly at `_r`. An ordinary point operation on an already
resolved collection does not read `_i`; collection deletion remains visible
through the delete intents installed on `_r` and every other node.

Transactional creation prepares both objects before publishing the parent
directory binding. A visible binding therefore implies that the collection
record and tree root have both been prepared. Partial preparation remains
undiscoverable and recoverable under the collection lifecycle protocol.

## Consequences

- Collection metadata and root-leaf data no longer share a CAS revision or
  object-size budget.
- Every key-bearing leaf has one node representation and mutation path.
  Collection metadata does not need to be preserved while rewriting a leaf.
- Root splitting remains structurally distinct because the fixed root cannot
  move, but its source and replacement are ordinary node states.
- Small collections retain a one-node data tree and a one-node cold data
  access; separation adds no root-pointer lookup or mandatory index level.
- Each collection uses one additional physical object. Creation, bootstrap,
  recovery, and reclamation must handle the two-object preparation invariant.
- The physical format changes. Compatibility and migration policy must be
  decided before implementation.

## Alternatives considered

### Keep the combined `_i`

This minimizes physical objects but preserves the representation branches,
conflict domain, and capacity coupling that motivate the separation.

### Make `_i` an index over an initial child leaf

This removes data entries from `_i`, but it retains collection metadata and
tree topology in one object and adds a mandatory tree level for small
collections.

### Store a movable root pointer in `_i`

This cleanly separates the objects, but every cold descent gains an indirection
and height changes require a root-pointer publication protocol. A fixed `_r`
provides the same separation without either cost.
