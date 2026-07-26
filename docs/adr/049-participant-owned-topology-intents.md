# ADR-049: Participant-owned topology intents

## Status

Accepted — implemented.

Refines the structural-record placement of
[ADR-034](034-separate-structural-log-namespace.md) and the topology-freeze
protocol of [ADR-047](047-transactional-collection-management.md).

## Context

A collection root records active topology participants so lifecycle changes can
exclude new structural work and settle existing work. A participant formerly
had no direct reference to its structural records. Settlement therefore listed
the database-wide structural namespace and filtered every record, and recovery
walked an entire collection to classify one split.

Merely changing the listing prefix would leave a race: a participant could
become visible in the root before writing its first record, allowing a freeze to
remove it while the split continued and later created a node.

## Decision

- A topology participant is a durable pending transaction with a topology lock
  back-reference.
- Before registering in the collection root, it writes a `Preparing` structural
  intent under `_s/<participant-id>/<intent-id>`. The intent reserves every node
  identity the operation could create.
- After acquiring the affected node's structural gate, the operation
  conditionally advances the intent to `Ready`. Node creation is permitted only
  after that transition succeeds.
- Recursive topology changes write another intent under the same participant
  before acquiring the next node gate. They retain at most one node gate at a
  time.
- A freeze settles a finalized participant by repeatedly listing only that
  participant's prefix. It cancels `Preparing` intents, recovers `Ready`
  intents, and removes the root participant only when the prefix is empty.
- Split recovery proves reachability by descending for the recorded split key,
  following B-link right siblings as usual. It does not enumerate the
  collection tree.
- Startup recovery may still list the database-wide `_s` prefix to discover
  abandoned participants. Lifecycle settlement and transaction GC do not.

This is a greenfield format; structural records in the former flat namespace
are not supported.

## Consequences

Lifecycle settlement is proportional to one participant's unfinished work,
and recovery of each split is proportional to tree height rather than collection
size. The prepare-before-register and conditional ready transition close the
late-node-creation race. Each structural operation pays an additional durable
intent write and conditional transition.
