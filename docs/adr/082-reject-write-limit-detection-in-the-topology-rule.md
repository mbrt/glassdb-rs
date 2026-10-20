# ADR-082: Reject write limit detection in the topology rule

## Status

Accepted.

This record replaces the proposal "Splits and merges at the write limit of a
collection". The proposal used this number on a branch, was not accepted, and
was never merged, so this record keeps the number.
[ADR-074](074-avoidable-time-drives-splits-and-merges.md) and
[ADR-076](076-paid-rates-hold-the-topology.md) do not change.

## Context

Under a heavy load, the avoidable time rule of ADR-074 splits leaves until a
leaf has about one member in each coordinator round. In the perfbench `mixed`
lo mode, the warm-ups made 4,600 to 7,700 splits and no merges in 300 s, and
did not become quiet. The investigation log
([hack/perf/investigations.md](../../hack/perf/investigations.md)) has the
runs of 2026-10-12, 2026-10-13, and 2026-10-20.

The rule counts the queue wait that a split removes. It does not count the
write that a split adds for each divided round. When the backend limits the
writes of a collection, the workers wait in the throttle instead of in the
queue, so the saved queue wait gives no throughput. In a variant where 5
collections shared one write limit, writes for each transaction rose by 45%,
and transactions in each second fell by 36%.

The proposal detected the write limit of each collection, and changed the rule
while a collection was at it:

- **Detection.** Each database instance compared the median leaf CAS time of a
  window with the lowest median over a long horizon. The collection was at its
  limit when one minus the ratio was more than 0.3, and left it below 0.2.
- **No queue wait at the limit.** Queue wait did not count as avoidable time.
- **A write price at the limit.** Each write that a split added cost the mean
  time of a transaction divided by its mean writes. Each write that a merge
  saved earned the same price.

Two other signals were measured: the throttle wait of the backend, and a check
that the landed rounds of a collection stop growing while the members of a
round fall.

## Decision

Do not detect the write limit in the topology rule. The reasons are:

- **The engine cannot locate the limit.** The backend throttles a partition,
  and the provider does not publish its partitions. The writes of another
  database instance, of transaction records, or of another collection can fill
  the same partition. A slow leaf CAS, a throttle error, or flat landed rounds
  show that writes are slow, but not which partition is at its limit, or whose
  writes fill it.
- **The signals of the engine have small margins.** Two runs with the same
  configuration and the same write rate had a median leaf CAS time of 59 to
  64 ms and of 104 to 132 ms, and both added writes without throughput. With
  own collections and no throttle wait, the median was 75 to 82 ms, so the
  baseline of the latency changes with the load. Flat landed rounds told the
  runs apart only over some minutes, and that depends on the history of the
  topology, not on its current traffic.
- **A limit that lasts is often temporary on real backends.** S3 and GCS give
  a hot key range more capacity under a sustained load, but slowly, and S3 can
  return 503 `SlowDown` while it does. A detector would change the topology for
  a state that the backend can remove, and each split and merge costs writes.
- **A write price needs no signal of the backend.** A split that adds writes
  and saves little queue wait does not pay, also below a limit. A write price
  for each added write uses only the traffic of the instance. It is a separate
  decision.

## Consequences

- The rule does not change. Under a write limit that lasts, it still splits
  leaves while the throughput stays the same, and some of these splits lower
  the throughput. A write price that suits a load below the limit costs less
  than an added write costs at the limit, so it does not stop all of these
  splits.
- The delay model limits each collection, and each first symbol of a
  transaction ID, on its own (`s3_delays`, depth 3). With 5 database instances
  on own collections, no limit was reached. The perfbench `mixed` option
  `--collections` makes the instances share a collection, and gives a cell at
  the limit.
- The waste below the limit stays: the backend writes doubled while the
  throughput stayed the same. A write price for each added write, and the
  symmetry of the split and merge causes, are the next decisions.
- A later design can detect a write limit if a backend gives a signal that
  locates it.

## Alternatives considered

### Count throttle errors in the backend adapters

The adapters retry S3 503 `SlowDown` and GCS 429 responses, and can count them.
In the runs, the throttle wait was 0 without a limit, 0 to 22 ms in the run
with little throttle, and 37 to 63 ms in the run with the most. So the count
tells that a throttle exists. But it does not tell which partition, or whose
writes fill it. It also needs a change of the memory backend, which waits
instead of rejecting.

### Decide from the throughput after earlier splits

The rule can stop splitting when the throughput of the instance does not rise
after its splits. This depends on the history of the topology, and not on its
current traffic. It cannot select the leaves to merge, because the throughput
is of the instance, and many splits land in the same minutes. A change of the
load in the same minutes makes the comparison wrong.
