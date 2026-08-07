# Discarded timestamp-history snapshot design

> **Archived — frozen.** This package preserves a snapshot-read proposal and
> seven ADRs that were discarded before acceptance or implementation. The
> active replacement is the
> [always-on dependency-checkpoint design](../../designs/snapshot-reads.md).

## Why it was discarded

The proposal assigned backend-derived timestamps to commits, retained certified
per-key history, and selected historical cuts by time. Review found that it:

- removed ADR-051's one-CAS direct path and added uncounted synchronous history
  work to logged commits;
- could not guarantee real-time ordering for disjoint commits;
- depended on a fleet-wide S3 or Cloud Storage clock-skew bound those services
  do not specify;
- required periodic backend time refresh during otherwise cache-complete reads;
  and
- described write-back and fallback behavior already superseded by ADR-053 and
  ADR-054.

The later low-cost sketch removed some foreground work but still preceded the
requirements interview that selected monotonic dependency checkpoints,
Database Timelines, optional serialized fences, and asynchronous reconciliation.

## Preserved documents

- [Original bounded-staleness design](designs/snapshot-reads.md)
- [Review and findings](designs/snapshot-reads-review.md)
- [Superseded low-cost sketch](designs/snapshot-reads-low-cost-proposal.md)

The following records were proposed only. Their numbers remain reserved for
historical link stability and must not be reused:

- [ADR-037: Bounded-staleness snapshot transactions](adr/037-bounded-staleness-snapshot-transactions.md)
- [ADR-038: Hybrid-logical-clock snapshot cuts](adr/038-hlc-snapshot-cuts.md)
- [ADR-039: Timestamp-versioned key history](adr/039-timestamp-versioned-key-history.md)
- [ADR-040: Snapshot history retention](adr/040-snapshot-history-retention.md)
- [ADR-041: Timestamp-versioned collection catalog](adr/041-timestamp-versioned-collection-catalog.md)
- [ADR-052: Backend server-time observation](adr/052-backend-server-time-observation.md)
- [ADR-055: Batched cache revalidation by listing](adr/055-batched-cache-revalidation-by-listing.md)

These files are design history, not current decisions. Do not update their
technical content as the active design evolves.
