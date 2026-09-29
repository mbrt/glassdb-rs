# ADR-075: Avoidable time is the default topology policy

## Status

Accepted — implemented.

Refines [ADR-074](074-avoidable-time-drives-splits-and-merges.md). It replaces
the decision of ADR-074 that the size causes stay the default. The rules of the
two policies do not change.

## Context

ADR-074 added the avoidable time policy as an opt-in, and kept the size causes
of ADR-031, ADR-056, and ADR-073 as the default.

A database instance that does not select a policy gets the size causes, which do
not measure what the topology costs its transactions.

## Decision

The avoidable time policy is the default topology policy. The size causes stay
a policy that a database instance can select.

## Consequences

- A database instance that does not select a policy measures avoidable time, and
  runs the rule once in each window.
- By default, the underfull threshold of `NodeSizePolicy` does not apply to
  leaves. Non-root leaves with no live entries merge. Index nodes keep their
  underfull threshold.
- The simulation workloads and fuzz targets now run the rule and take its
  measurements. Their leaves merge only when deletes leave no live entries.
- The loss of ADR-074 with 8 database instances in the `mixed` hi mode is now in
  the default.

## Alternatives considered

### Keep the size causes as the default

The measurements of ADR-074 show more throughput for most loads with the
avoidable time policy. With the size causes as the default, only the database
instances that select the policy get this.

### Remove the size causes

The size causes are the only policy that splits on each inline rejection and
that merges leaves with live entries below a threshold. Tests of ADR-056 and
ADR-073 need them. A removal is a separate decision.
