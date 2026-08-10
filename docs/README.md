# GlassDB documentation

A map of what lives where. The layout separates **frozen decisions** (ADRs) from
the **living narrative** that indexes and explains them (architecture + designs).

| Area                | What it holds                                                        | Lifecycle |
| ------------------- | -------------------------------------------------------------------- | --------- |
| [`architecture.md`](architecture.md) | The high-level architecture and design of GlassDB. | Living — kept current. |
| [`adr/`](adr/)      | Architecture Decision Records: one significant decision each, in sequence. | Frozen — only status/links change after acceptance. |
| [`designs/`](designs/) | Design explorations and overviews for significant work. A design may precede ADR extraction or serve as an umbrella narrative and decision index. | Living — edited as the effort progresses. |
| [`guides/`](guides/) | Process and how-to: releasing, reviewing, performance tracking, testing. | Living. |
| [`archive/`](archive/) | Superseded / obsolete documents, preserved for reference. | Frozen — banner-marked, not updated. |

## Architecture

- [`architecture.md`](architecture.md) — the canonical, living high-level
  architecture. Start here; it links down to the designs and ADRs.

## Decision records (`adr/`)

Sequential, self-contained decision records, immediately actionable. ADRs are
**frozen** once accepted (per `AGENTS.md`/`CLAUDE.md`): only the `Status` and
forward/back links change; when a decision is superseded it is marked and linked
to the ADR that replaces it. New ADRs use [`adr/000-template.md`](adr/000-template.md).

**Numbering gap (003–006).** ADR numbers 003–006 are intentionally absent. They
exist in the upstream Go repository (`glassdb`) and record **Go-specific**
decisions, tied to that codebase's infrastructure. This Rust project is
independent of the Go original, so they were not ported; only the
design-relevant records (001, 002, 007) were carried over before the sequence
diverged.

**Archived, unaccepted records.** ADR numbers 037–041, 052, and 055 belong to
the discarded timestamp-history snapshot proposal. They were never accepted or
implemented, have moved to the
[`snapshot-reads-timestamp-history`](archive/snapshot-reads-timestamp-history/README.md)
archive, and remain reserved for link stability rather than being reused.

## Designs (`designs/`)

Living documents that explore or track significant redesigns. Before
implementation, a design may remain self-contained so speculative choices do
not leave a trail of proposed, unimplemented ADRs. Once decisions are accepted,
the design becomes the umbrella narrative and indexes its focused ADRs with
status. Designs are **named by topic, not numbered** (unlike ADRs): they are a
small, living set referenced by subject rather than a decision sequence. New
designs use [`designs/_template.md`](designs/_template.md).

- [`designs/object-storage-native.md`](designs/object-storage-native.md) — the
  content-CAS transaction and storage layout.
- [`designs/dynamic-range-sharding.md`](designs/dynamic-range-sharding.md) — the
  order-preserving B-link coordination directory.
- [`designs/snapshot-reads.md`](designs/snapshot-reads.md) — proposed
  always-on, dependency-consistent, long-lived read-only snapshots.

## Guides (`guides/`)

- [`caching.md`](guides/caching.md) — how `CachedStore`, requirements, and
  observations provide reusable currentness evidence over a strongly consistent
  backend.
- [`releasing.md`](guides/releasing.md) — how releases are cut and published.
- [`reviewing-changes.md`](guides/reviewing-changes.md) — what to look for when
  reviewing a change (policy vs. mechanism, ownership).
- [`perf.md`](guides/perf.md) — running log of performance-affecting changes.
- [`opaque-validation-evidence-migration.md`](guides/opaque-validation-evidence-migration.md)
  — migration from the removed physical transaction-evidence APIs.
- [`testing-dst.md`](guides/testing-dst.md) — the deterministic-simulation testing
  approach and its trade-offs vs. alternatives.

## Archive (`archive/`)

- [`porting-go.md`](archive/porting-go.md) — the original Go→Rust port decisions.
  Frozen; the engine is now independent of the Go version.
- [`snapshot-reads-timestamp-history/`](archive/snapshot-reads-timestamp-history/README.md)
  — discarded timestamp-versioned snapshot design, review, and unaccepted ADRs.
