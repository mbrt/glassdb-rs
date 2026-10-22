# ADR-083: Drop stale topology requests

## Status

Accepted — implemented.

Refines [ADR-074](074-avoidable-time-drives-splits-and-merges.md). A change
that the topology rule asked for is a candidate for at most three windows. The
engine retries a deferred change only in these windows.

## Context

Under a heavy load, the avoidable time rule asks for more splits than the
restructurer can make. In the perfbench `mixed` lo mode with 5 database
instances on one collection and 200 workers for each shape, the rule asked for
about 800 splits in each second of wall time, and the restructurer made 20 to
35. The candidate queue was at its cap of 4,096 candidates, and dropped the
oldest. The candidates that a sweep took were 1.5 to 3 minutes old.

After the workers paused, the restructurer made about 785 more splits from
this backlog, with almost no transactions. Under load, a split acted on a
measurement of minutes before, and the leaf had changed since. The rule drops
the moving average of a leaf when it asks for a split, so the queue was the
only state that kept these requests.

A candidate had no time of its request. Its wound-wait priority is the time of
its oldest observation, because coalescing keeps the oldest priority, so it
cannot tell how old the newest request is.

## Decision

Each candidate keeps the time when the newest observation of its cause was
queued. Coalescing keeps the newest time, and a deferred candidate keeps its
time when it is queued again.

A sweep drops a candidate of a topology rule (a demand split or a demand
merge) that is older than three windows of the rule. It checks the age when it
takes the queue, and again before each batch of splits and each merge, because
one sweep can take longer than three windows. Three windows let a deferred
change retry in the next sweeps. The candidates of the size causes do not
change, because their cause stays in the node until a change removes it.

The rule does not ask again at once. When it asks for a change, its average for
the leaf or the pair starts again, and the average must pay for the change
again. With a time of two times the threshold in each window, this takes about
10 windows.

The restructurer counts the dropped candidates (`stale_demand`).

## Consequences

- After the load stops, the restructurer starts no change that the rule asked
  for more than three windows before.
- A change starts on a measurement of at most three windows before.
- In the perfbench `load-shift` experiment, the restructurer dropped 665
  candidates in the 10 s measurement of the first low-conflict cell, and
  2,284 in the measurement of the crossing cell.
- This does not stop the splits of a stable load. In the same experiment, the
  first warm-up made 7,392 splits in 300 s without the change, and 7,678 with
  it, and neither became quiet. The rule asks for new splits in each window
  under this load.
- A change that stays deferred for more than three windows is dropped, and the
  rule asks for it only when its average pays again. A merge defers while a
  live transaction holds a lock on its leaves, so under a load that crosses the
  leaves, a merge can need more than one request. In the crossing cell of
  `load-shift`, the warm-up made 2,648 merges with the change and 1,992
  without it.

## Alternatives considered

### Check the current average of the leaf before the change

The sweep can ask the rule if the leaf still pays for the change. The rule
state is in the rule loop, and the sweep would need to share it. The rule also
starts the average of a leaf again when it asks for a split, so the average
does not show the time before the request. An age limit needs no shared state,
but it also drops a change whose time continues, until the average pays again.

### Limit the requests of each window to what the restructurer can make

The rule can ask for at most the changes that the restructurer made in the
last window. The rate of the restructurer changes with the backend and with the
parallelism of the sweep, and the rule would need to measure it. Old requests
would also stay in the queue when the rate falls.
