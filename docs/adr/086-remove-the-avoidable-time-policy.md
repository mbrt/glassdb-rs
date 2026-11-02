# ADR-086: Remove the avoidable time policy

## Status

Accepted — implemented.

Supersedes [ADR-074](074-avoidable-time-drives-splits-and-merges.md),
[ADR-076](076-paid-rates-hold-the-topology.md),
[ADR-082](082-reject-write-limit-detection-in-the-topology-rule.md), and
[ADR-083](083-drop-stale-topology-requests.md).

## Context

Since [ADR-084](084-size-causes-are-the-default-topology-policy.md), the
avoidable time policy is a policy that a database instance can select, but not
the default. The default policy, now `TopologyPolicy::Writers`, gave the
writers more throughput on each load that the investigations measured, and it
settles. Under the same loads, the avoidable time policy keeps splitting and
merging leaves, and the writers of a contended leaf commit less. The entry of 2026-11-02 in
[`hack/perf/investigations.md`](../../hack/perf/investigations.md) has the
last comparison.

The avoidable time policy is the only user of a large part of the engine:

- The topology rule, its windows, and the measurements of avoidable time.
- The changes that the rule asks for, with their rates and their maximum age.
- The paid rates in the stored leaves.
- The measurement of the delays of the rounds of the leaf coordinator.
- The time that commits, conflicts, and scans report to the topology policy.

## Decision

Remove the avoidable time policy, and each part of the engine that only it
uses. The size causes and the writers policy stay. Both decide from the leaf
sizes and from the leaf entries, and the writers policy also from lost leaf
CASes (ADR-085).

A sweep takes the splits first and the merges last. There are no rates to
order the changes by.

## Consequences

- No topology policy merges two leaves because scans cross their boundary.
  A load that only scans keeps its leaf boundaries, unless the leaves are
  underfull. Scans over two leaves read one more leaf than they would read
  after a merge.
- Transactions, scans, and the leaf coordinator do not measure time for the
  topology policy.
- Stored leaves have no paid rate. The project has no release, so no
  migration is necessary.
- The public API loses `TopologyPolicy::AvoidableTime`, `TopologyTimeStats`,
  `DemandMergeStats`, and the counter of stale requests.

## Alternatives considered

### Keep the avoidable time policy as an option

It costs code and tests in the restructurer, the leaf coordinator, and the
storage format. No measured load needs it. Its only gain, a higher scan
throughput on leaves that it merged, comes from the same changes that slow
the writers of these leaves.

### Add a merge cause for scans to the default first

A merge cause for leaves that scans cross could keep this gain without the
rule. No measured load needs it yet. A later decision can add it to the
writers policy when a load shows the need.
