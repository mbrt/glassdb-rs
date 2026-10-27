# ADR-084: The size causes are the default topology policy

## Status

Accepted — implemented.

Supersedes [ADR-075](075-avoidable-time-is-the-default-topology-policy.md).
The rules of the two policies of
[ADR-074](074-avoidable-time-drives-splits-and-merges.md) do not change, but
the new default soft cap of leaf entries applies to both.

## Context

With the avoidable time policy, the queue wait of one database instance pays
for most splits. Under sustained load, the rule keeps splitting, and the tree
does not settle. In the reduced `workers` grid of perfbench, the leaves at 200
workers had about 12 entries, and each coordinator round had 1.6 members.

The investigations of 2026-10-24 to 2026-10-27 in
[`hack/perf/investigations.md`](../../hack/perf/investigations.md) measured
this on the S3 delay model:

- A split removes most of the queue wait that the rule counts. But this queue
  wait is a small part of all queue wait under load, and each split also
  divides the rounds of the leaf.
- Neither a price for each backend write, nor an estimate of the rounds that a
  split adds, told the useful splits from the others.
- The size causes with a soft cap of 48 entries and an underfull threshold of
  12 entries had the same or more throughput than the avoidable time policy
  with 256 and 64 entries, in all cells of `workers`, `mixed`, and
  `load-shift`. At 200 workers, they had 1.16 times the throughput with 0.42
  of the writes for each transaction. All warm-ups were quiet.
- Smaller soft caps had more throughput in `workers` and `mixed`, and less in
  `load-shift`, where five database instances share one collection.
- The CPU of the host and the throttle of the delay model did not limit these
  cells.

## Decision

The size causes are the default topology policy. The default soft cap of leaf
entries is 48, and the default underfull threshold of leaves is 12. The byte
soft cap, the hard caps, and the thresholds of index nodes do not change.

The avoidable time policy stays a policy that a database instance can select.

## Consequences

- A database instance that does not select a policy takes no measurements of
  avoidable time. A leaf that it writes splits above 48 entries, and merges
  below 12 live entries when the merge rules of ADR-073 allow it. A merge
  stays vetoed when the merged leaf would have more than 24 entries, so a leaf
  next to a full one can stay below 12 entries.
- The soft thresholds are local to each database instance. An instance that
  opens an existing database splits each large leaf that it writes, down to
  leaves of the new soft cap. Leaves that it only reads keep their size.
- While instances with the old and the new defaults write one database, a leaf
  can split and merge again and again, because their thresholds differ. To
  avoid this, an upgrade can select the old thresholds, 256 and 64 entries,
  until all instances run the new version.
- An instance that selects the avoidable time policy with the default node
  size policy now splits leaves above 48 entries, and its rule cannot merge
  leaves to more than 24 entries. The measurements of ADR-074 to ADR-076 used
  256 entries.
- Keys that several database instances write in one leaf stay in one leaf, and
  lose leaf CASes to each other. In a load where four instances wrote four
  adjacent keys, each writer had about 2 tx/s with these defaults, and
  7.85 tx/s with one key in each leaf.
- Scans and transactions over many adjacent keys get more leaves. These loads,
  and the GCS delay model, were not measured.

## Alternatives considered

### Keep the avoidable time policy as the default

Under sustained load, it splits beyond the soft cap of 48 entries, writes more,
and does not settle. A write price, a correction of the queue wait, and an
estimate of the rounds that a split adds did not fix this without the loss of
useful splits.

### A smaller soft cap

Soft caps of 8 and 16 entries had the most throughput in `workers` and `mixed`.
In `load-shift`, they had down to 0.59 of the throughput of the avoidable time
policy, with up to 3.6 times the writes for each transaction.

### Lost CAS splits below the underfull threshold

The rule of ADR-074 with only lost CAS as a split cause, a split key between
the keys that conflict, and a merge veto, separated the hot keys of the load
above in all runs. But in the `crossing` cell of `load-shift`, it made about
1,600 splits below 12 entries, and had 0.82 to 0.85 of the throughput of the
size causes. No measured rate of lost CAS told the two loads apart.

### A soft cap in bytes

The leaves of the measured loads had small values, so their entries and their
bytes grew together. A soft cap in bytes needs an underfull threshold in bytes,
which the size causes do not have.
