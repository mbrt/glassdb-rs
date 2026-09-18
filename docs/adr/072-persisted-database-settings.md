# ADR-072: Persist correctness settings at database creation

## Status

Proposed — implementation in progress.

## Context

Each database instance currently supplies its own hard coordination limits and
transaction timing. Smaller limits can prevent a client from modifying existing
data. Different timing profiles disagree about lease expiry and the interval
during which an ambiguous commit can recover before GC deletes its record.

## Decision

Persist hard coordination limits and transaction timing with the database ID
when creating metadata. Every open loads the stored settings before starting
the engine. Concurrent creators use the settings from the winning metadata
write. Missing or invalid settings cause opening to fail.

Keep the builder interface for this first pass. Its hard limits and timing are
creation settings; opening an existing database ignores those proposed values.
Soft split thresholds, inline budgets, and resource limits remain local to each
database instance. A hard-cap split must not depend on soft thresholds.

Require a new protocol version for each incompatible metadata change. Older
clients must reject it. No automatic migration or online settings changes are
provided; development databases must be recreated.

This refines the configuration agreement in
[ADR-056](056-demand-driven-inline-pressure-splits.md). The value-preservation
rules for inline budgets remain unchanged.

## Consequences

A new database instance can operate with the original shared settings without
the caller supplying them again. Metadata, instead of deployment configuration,
owns the agreement between clients. Clients must still satisfy the configured
clock-skew allowance.

The existing split-policy type still contains both creation settings and local
thresholds. A separate public creation-options interface is deferred.

## Alternatives considered

- Require equal settings from every opener. This requires callers to retain
  configuration that the database can load itself.
- Persist every option. This prevents clients from independently choosing their
  resource use and maintenance schedule.
- Infer settings for existing databases. Stored objects cannot establish the
  original timing profile or admission limits.
