# ADR-101: Hints wait while the write budget limits GC

## Status

Accepted — implemented.

Changes the candidate capacities of
[ADR-078](078-full-gc-and-gc-write-budget.md). The scan capacity still follows
the write budget. The hint capacity does not.

## Context

ADR-078 sizes the hint and scan candidate capacities from the spend limit of
the write budget: about the checks that the spend limit allows while a
candidate holds its slot, between a floor and the maximum of `GcLimits`. A
hinted candidate holds its slot for the retention period before its check.

While the write budget limits GC, the hint capacity is small, and the hint
queue drops hints. The spend limit grows slowly after a load starts, so this
also happens under a steady load. Dropping a hint saves no write: GC deletes
records at the rate of the budget either way. But it costs reads:

- A record whose hint is dropped stays until a scan finds it. A scan check
  reads more than a hint check: a scan lists records that are still
  referenced or that other instances delete first.
- Each dropped hint makes the scans faster (ADR-078), so all instances list
  and check the same records more often.

In the perfbench `mixed` cell with 5 instances at prefix depth 3, GC dropped
about 113,000 hints in the 120 s warm-up. A hint capacity at its maximum
dropped none, used about 5% fewer GC reads for each reclaimed record, and left
about 28% fewer records for the full GC after the warm-up.

## Decision

The hint capacity is `GcLimits::max_hint_candidates`, and it does not follow
the write budget. While the budget limits GC, hints wait in memory for their
checks instead of being dropped. Hints are dropped only at that maximum.

The scan capacity keeps the rule of ADR-078. A scan can always list more
records, so its candidates need not wait in memory.

## Consequences

- A busy instance can keep up to `GcLimits::max_hint_candidates` hint
  candidates in memory while the budget is small. The memory follows the hints
  that wait, so an idle instance uses none.
- Scans run fast because of dropped hints only when the hint queue is full at
  its maximum, or when the pending-hint queue drops reports.
- A hint can wait longer than the retention period for its check, until the
  budget allows it. A scan can still find and check the record first.

## Alternatives considered

- **Keep the capacity that follows the budget, with a larger floor.** A
  floor is a constant that fits one load. Under a larger load, hints are
  dropped again.
- **Keep dropping hints, and make scans cheaper.** Scans that start a LIST
  only after the check pipeline takes their earlier candidates, and a scan
  cadence that follows the share of scan checks that make progress, reduce the
  reads of a scan. A scan still reads more than a hint for the same record.
