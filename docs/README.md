# GlassDB documentation

Current documentation has three owners:

| Area | Content | Lifecycle |
| --- | --- | --- |
| [`architecture.md`](architecture.md) | High-level current architecture and responsibility ownership. | Updated with the implementation. |
| [`adr/`](adr/) | Significant decisions, their reasons, and trade-offs. | Frozen after acceptance, except for status and links to newer ADRs. |
| [`guides/`](guides/) | Maintainer procedures and focused technical guidance. | Updated with the related workflow. |

## Architecture

[`architecture.md`](architecture.md) describes the current system. Exact
interfaces and behavior remain owned by code and tests.

## Decision records

ADRs use three-digit sequential numbers and
[`adr/000-template.md`](adr/000-template.md). Add one only for a significant,
hard-to-reverse trade-off whose reason is not evident from the code.

ADRs are frozen once accepted, except for their `Status` and links to
superseding ADRs.

ADR identifiers are never reused after assignment. Gaps can remain when a
proposal is retired or work develops on another branch.

## Guides

- [`guides/caching.md`](guides/caching.md) describes reusable currentness
  evidence in `CachedStore`.
- [`guides/releasing.md`](guides/releasing.md) describes the release process.
- [`guides/reviewing-changes.md`](guides/reviewing-changes.md) defines the review
  focus for ownership, policy, mechanism, and intended behavior.
- [`guides/perf.md`](guides/perf.md) records performance-affecting changes.
- [`guides/testing-dst.md`](guides/testing-dst.md) describes deterministic
  simulation testing and its trade-offs.
