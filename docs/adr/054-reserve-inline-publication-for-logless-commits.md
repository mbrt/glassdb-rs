# ADR-054: Reserve inline publication for logless commits

## Status

Proposed.

If accepted, this supersedes
[ADR-051](051-inline-latest-values.md)'s decision to copy logged values into leaf
entries during write-back and help-forwarding. ADR-051's authoritative inline
representation, bounded admission, logless direct commit, and in-doubt contract
remain unchanged.

## Context

ADR-051 uses one representation for two different purposes:

- a logless commit must publish its value inline because the leaf is its only
  durable authority; and
- a logged transaction may duplicate its value inline to avoid a later
  transaction-object lookup.

The second use competes with the first for the aggregate leaf budget. Admission
is first-come and existing inline values cannot be demoted safely because the
state records no provenance and a value may have no transaction object. Logged
write-back can therefore fill a leaf with disposable duplicates, force later
logless candidates onto the locked protocol, and enlarge every subsequent leaf
CAS and split.

The transaction object already exists for a logged writer and remains reachable
while the leaf names it. Once observed final, it is immutable and may be served
indefinitely from the decoded cache while resident. The optional persistent
cache further reduces cold body transfers. Cache misses still exist, especially
after eviction, restart, or on another database instance, but duplicating every
small logged value is not free insurance against them.

The focused manual investigation found that suppressing logged write-back
inlining reduced one 1 KiB batch's node-write volume from `73.8` to `9.6 KiB`.
A cold scan then loaded the batch's one shared `65.7 KiB` transaction object,
with effectively unchanged scan latency in the simulated S3 and GCS profiles.
Deterministic batch-write time improved, while short mixed-workload throughput
remained at parity. See the
[performance investigation](../../hack/perf/investigations.md#2026-07-29-inline-admission-and-structural-amplification).

Suppression did not increase total inline capacity: it changed which keys could
consume the fixed budget. Capacity growth through splitting is a separate
decision.

## Decision

### Create inline state only when the leaf is authoritative

A new value is published as `Inline` only when no transaction object backs that
value and the leaf is intended to be its durable authority. The current case is
ADR-051's logless direct commit.

A committed value backed by a transaction object is published as `External`.
Ordinary write-back therefore releases its locks without copying value bytes
into the leaf. A helper that materializes a committed value from its transaction
object also publishes `External`.

This distinction follows the publication protocol, not a new persisted
provenance flag.

### Preserve existing authoritative state

An operation that merely confirms the writer already recorded in a leaf
preserves its current state, including `Inline`. It must not demote an inline
value because the writer may be logless. Existing logged inline values are
therefore grandfathered until an overwrite or other normal state transition
replaces them.

Tombstone and absent states are unchanged. Readers continue returning `Inline`
directly and resolving `External` through the transaction object.

### Keep capacity policy separate

Per-value and aggregate inline budgets continue to bound new authoritative
inline publication and count existing inline payloads. This decision neither
changes their numeric defaults nor adds a split trigger.

Whether aggregate inline pressure should request a background split is a
follow-up decision with separate tree-width and structural-work trade-offs.

## Consequences

- Logged write-back no longer enlarges leaves with a second durable copy of
  values already present in transaction objects.
- Inline capacity is reserved for protocols that require the leaf to carry the
  value, reducing history-dependent interference with direct commits.
- A cold or evicted read of an external value may need to load its transaction
  object and may transfer unrelated values written by the same transaction.
  Decoded and persistent caches mitigate this cost but are not required for
  correctness.
- The existing leaf and transaction-object formats remain valid. No migration
  or provenance bit is introduced, and grandfathered inline values remain
  readable.
- Removing optional publication does not solve saturation by authoritative
  inline values. Direct candidates may still fall back after the aggregate
  budget fills.
- The policy is simpler: inline bytes mean durable value authority for newly
  published states, rather than also acting as an opportunistic read cache.

## Alternatives considered

### Keep opportunistic write-back inlining

This avoids a transaction-object lookup on cold reads, but preserves duplicate
storage, whole-leaf rewrite amplification, and first-come competition with
logless commits. The measured write cost is not justified as the unconditional
default.

### Reserve separate portions of the budget

A sub-budget for logged values would limit interference but retain duplicate
bytes and history-dependent allocation. Any static division can still strand
capacity on the wrong workload.

### Record provenance and evict logged inline values under pressure

A persisted distinction would retain opportunistic read copies until direct
capacity is needed. It adds a new state invariant, eviction policy, and
transition surface when publishing and resolving values. Keeping logged values
external avoids that complexity.

### Couple removal with pressure-driven splitting

Splitting can create more aggregate authoritative capacity, but it changes tree
width and structural work. Combining it here would make the measured
write-back simplification depend on an unresolved topology policy.

### Remove inline values entirely

This restores a single durable value location but also removes ADR-051's one-CAS
logless commit, which provides the dominant single-RMW improvement.
