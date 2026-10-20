# ADR-082: Splits and merges at the write limit of a collection

## Status

Proposed.

Refines [ADR-074](074-avoidable-time-drives-splits-and-merges.md). When the
backend limits the writes of a collection, the avoidable time rule does not
count queue wait. It charges a write price for each write that a split adds,
and credits it for each write that a merge saves.

Refines [ADR-076](076-paid-rates-hold-the-topology.md). The paid rate of a merge
includes the write credit that paid for it. The margin and the decay of paid
rates do not change.

Keeps the position of ADR-064 and ADR-074 that the `Backend` trait does not
report throttling. The engine detects the write limit from the leaf CAS time.

## Context

The perfbench `mixed` lo mode ran with 5 database instances on one shared
collection, 200 workers per shape, 20,000 keys, and the S3 delay model with a
write limit of 3,500 writes in each second for each collection. The
investigation log
([hack/perf/investigations.md](../../hack/perf/investigations.md)) has the
runs of 2026-10-20.

- Before the limit, splits raised the throughput by about 65%.
- After about 150 s, the landed coordinator rounds of the collection stayed at
  about the limit. The throughput stayed the same, while the rule made about
  2,700 more splits. The mean leaf CAS time rose from about 65 ms to about
  210 ms, and most of the rise was throttle wait.
- In an earlier variant, 5 collections shared one limit, and the same
  happened. In that variant, the 2026-10-13 entry measured that writes for each
  transaction rose by 45%, and transactions in each second fell by 36%.

At the limit, the backend completes a fixed number of writes in each second.
The workers of a database instance wait for each transaction before they start
the next one. So the throughput is the write limit divided by the writes of a
transaction, and the queue wait does not change it. A split that removes queue
wait moves the same time into the throttle. A divided round needs two leaf
CASes after a split, so the split also lowers the throughput.

The rule counts the queue wait that a split removes, and no write that it adds.
So at the limit, each split that the rule asks for makes the throughput worse,
and no merge undoes it.

## Decision

### Detect the write limit

For each collection, each database instance compares the median leaf CAS time
of a window with the lowest median of the instance over a long horizon. The
saturation of the collection is one minus the ratio of the lowest median to
the median of the window. The collection is at its write limit when the moving
average of its saturation is more than 0.3. It leaves the limit when the
average is less than 0.2.

The lowest median of the instance is the leaf CAS time that the backend does
not throttle. A median is steadier than the lowest single time, which comes
from the low tail of the latency. Over a long horizon, the lowest median does
not follow a throttle that lasts for minutes. Without a throttle, the
saturation is about 0.15 from jitter. The two thresholds are above it, and
stop the state from changing in each window.

### Do not count queue wait at the limit

While a collection is at its write limit, queue wait for an earlier round is
not avoidable time of its leaves. Lost CAS time, slow leaf CAS time, and
inline pressure still count. A split for these causes removes writes or the
wait for one object.

### Price writes at the limit

While a collection is at its write limit, each write that a change of its
leaves adds or saves has a write price: the mean time of a transaction of the
database instance divided by its mean backend writes for each transaction. At
the limit, one more write for each transaction makes all transactions slower
by about this time.

- The split side of a leaf subtracts the write price for each divided round.
- The merge side of two adjacent leaves adds the write price for each round of
  one leaf that starts before a round of the other leaf reads its members. In
  one leaf, these two rounds would be one round.

Below the write limit, writes have no price. A price for writes below the limit
is a later decision.

### Keep the rule

The thresholds, the moving averages, the hold-down window, and the paid rates
of ADR-074 and ADR-076 apply to the new terms as to the others. When a merge at
the limit lands, its paid rate includes its write credit, so the merged leaf
does not split again when the write price stops.

## Consequences

- At the limit, the rule stops splits that add writes, and it merges leaves
  whose rounds overlap until the collection leaves the limit. In the run of the
  context, the saturation of each window passed 0.3 at about 160 s, with mean
  leaf CAS times. The moving average passes it some windows later. This is an
  estimate from the counters, not a run of the rule.
- Around the limit, the topology can alternate: merges lower the writes, the
  collection leaves the limit, and the queue wait splits the leaves again. The
  two thresholds and the paid rates hold it, but this must be measured.
- A write limit that lasts longer than the horizon raises the lowest median. The
  rule then sees no limit, and the splits start again.
- A backend that becomes slower for another cause, for example a slower
  network, looks like a write limit. The rule then stops splits for queue wait
  until the horizon passes.
- The leaf coordinator measures more for each round: the divided rounds, the
  rounds of adjacent leaves that overlap, and the median leaf CAS time of each
  collection. The size causes policy does not measure these.
- Each database instance detects the limit from its own leaf CASes. The
  instances that share a collection all see the throttle wait, so they agree
  about the limit, also when the writes of another instance cause it.
- A per-object limit, such as the GCS limit of about one write in each second
  for one object, shows as slow leaf CAS of one leaf, and not as saturation of
  the collection. A split still removes it.
- Below the limit, the tree still splits until a leaf has about one member in
  each round, and the backend writes double without a gain in throughput. A
  write price below the limit is a separate decision. So is the symmetry of the
  split and merge causes after a change of the load.

## Alternatives considered

### Count throttle errors in the backend adapters

The adapters retry S3 503 `SlowDown` and GCS 429 responses, and can count them.
This is a direct signal, and it needs no baseline. But the count must reach the
engine without the `Backend` trait, and the memory backend must reject writes
instead of waiting. Only the leaf CAS time is the same signal on all backends.

### Price writes as a multiple of the leaf CAS time

The queue wait of a leaf grows with the time of its rounds, and so with the
leaf CAS time. A price in the same unit grows in the same way, so the throttle
does not change its decisions. With the throttle, the saved wait for each added
write rose from 0.8 to 5.7 leaf CAS times as the leaves got smaller, while the
throughput fell.

### Count a part of queue wait at the limit

The rule can count a part of queue wait that becomes smaller as the saturation
increases. But at the limit, all of the removed queue wait comes back as
throttle wait, so no part of it is a gain.

### Use the lowest single leaf CAS time as the baseline

This is the floor of the slow leaf CAS cause of ADR-074. It follows a throttle
that lasts, so it stops showing the limit after some minutes.

### Limit the write rate of the topology

A rule can stop splits that raise the writes for each transaction above a fixed
budget. This needs a value that suits each backend and load, and it does not
merge the leaves that a load at the limit split.
