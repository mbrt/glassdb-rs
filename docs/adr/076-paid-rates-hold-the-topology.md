# ADR-076: Paid rates hold the topology

## Status

Accepted — implemented.

Refines [ADR-074](074-avoidable-time-drives-splits-and-merges.md). A split or
merge that a topology rule asks for keeps its paid rate in its leaves, and the
opposite change of these leaves must pay more. The measurements, the rule, and
its thresholds do not change.

## Context

Each database instance decides the splits and merges of a leaf on its own
transactions (ADR-074). Under a stable load, the instances can disagree. In the
perfbench `split-merge-fight` scenario, two instances write the two keys of one
leaf, and a third instance scans both keys. The writers lose leaf CASes to each
other, and their rule splits the leaf. Each scan then crosses into the right
leaf, and the rule of the scanner merges the leaves. In each run of 60 s, the
leaf split and merged 24 to 29 times.

Each change removes the time that paid for it, and the time of the opposite
change is in another database instance. No instance sees both, and nothing keeps
the time that paid for a change after it lands. The hold-down window of ADR-074
holds a leaf only in the instance that changed it, and only for two windows.

The two topologies of this load cost about the same. With one leaf, the writers
lose about 3.7 worker seconds in each second. With two leaves, the scanner loses
about 5.0. The fight costs more than either topology, because each change also
costs backend writes, and the instances wait for each change.

## Decision

### Keep the paid rate in the leaves

When a split or merge that a topology rule asked for lands, its leaves keep its
paid rate: its kind, the rate of avoidable time that paid for it, and the ID of
its structural intent. A split keeps it in both leaves that it writes. A merge
keeps it in the leaf that receives the entries, and a merge that is abandoned
removes it again. The rate is the mean of the window rates of the rule since the
last change of the leaf or the pair that the database instance knows of: the net
split-side time for a split, and the merge-side time for a merge. A window
without time counts as zero. A mean is used, because the rate of one window
varied from 0.3 to 1.7 times the steady rate.

The other structural changes keep no paid rate. Their leaves have none: a split
for a soft cap or a hard cap, a merge of a leaf with no live entries, also when
a rule asked for it, and the changes of index nodes. Transactions keep the paid
rate of a leaf when they write it.

### Make the opposite change pay more

When the engine checks a request of a rule again, it compares a request of
the opposite kind with the paid rate. A split request needs more than a margin
times the paid rate of its leaf. A merge request needs more than a margin times
the larger paid rate of its two leaves. The margin is 1.5. A request that does
not pay more is dropped, and the rule keeps measuring. A request of the same
kind as the paid rate is not compared.

The paid rate halves every 60 s, so that a change of the load can still change
the topology. Each database instance measures this age on its own monotonic
clock, from the first time that it compares a request with the paid rate. The
ID of the structural intent identifies a paid rate. No database instance
compares its clock with the clock of another instance.

### Keep the format compatible

The paid rate is an optional field of a node. The protocol version does not
change. A database instance of an earlier version ignores the field, and drops
it when it writes the node. The opposite change of that leaf is then not held,
as before this decision. A paid rate of a kind that a database instance does not
know is read as no paid rate, so that a later version can add kinds.

## Consequences

- The topology holds after a change, unless the opposite change pays more. An
  offline replay of the windows of `split-merge-fight` had about 0.9 changes in
  60 model seconds, against about 12 without paid rates.
- A paid rate is the time that one database instance measured. When several
  instances share a leaf, each one sees only its part of the time. In
  `split-merge-fight`, one writer saw about half of the split-side time of both
  writers.
- The merge side of `split-merge-fight` measured about 0.2 of its real cost, and
  the split side about 0.9. The offline replay then held two leaves for 0.94 of
  the time, but one leaf costs about 1.25 worker seconds less in each second.
  With paid rates, an error in the measurement of one side can hold the worse
  topology for a long time.
- A database instance that first compares a request after some time starts the
  age later. Such an instance holds the topology longer than the instance that
  made the change.
- Each database instance keeps the time when it first compared a request with
  each paid rate, for up to 4096 paid rates. Above this, it forgets the oldest
  times. A forgotten paid rate that the instance sees again holds as a new one.
- A leaf that receives a merge of another database instance keeps its ID, so the
  rule of an instance does not see that change. The mean rate of that leaf can
  then include windows from before the merge.
- A load that changes still changes the topology, but later: the block lasts
  about `60 s × log2(1.5 × paid rate / rate of the request)`.

## Alternatives considered

### Keep the time of the change in the paid rate

The changing database instance can write its wall-clock time with the paid rate,
so that every instance decays it from the time of the change. But each age then
depends on the clock skew between the instances. The instance that compares a
request first would see the paid rate soon after the change in most loads, so
its own clock gives about the same age.

### Grow the hold-down window after each reversal

Each instance can hold a leaf longer after each split and merge of it. This
needs no format change, but it slows the fight only, and the topology after the
last reversal is the one that holds. Paid rates also let the cheaper change win
when the measurements are right.

### Share the measurements of the database instances

The writers can keep their current split-side rate in the leaf with the CASes
that they already make. Each reader then sees the time of all instances. This
needs per-instance state in each leaf, and it changes what the instances
measure. It can be a later change. It does not change how paid rates hold the
topology.

### Fix the measurement of the merge side first

A scan over two leaves also resolves more lock records in its body, because the
writers commit faster, and this time is not merge-side time. A change of the
merge side changes only the rates that paid rates keep, so it can come later.
