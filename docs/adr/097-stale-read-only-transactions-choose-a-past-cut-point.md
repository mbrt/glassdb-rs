# ADR-097: Stale read-only transactions choose a past cut point

## Status

Accepted.

[ADR-098](098-a-stale-read-is-a-stale-read-only-transaction.md) makes
`Collection::read_stale` a stale read-only transaction with one read.

Refines [ADR-036](036-decoded-object-cache-with-bounded-freshness.md): a
cached state also records where its currentness interval starts. Refines
[ADR-045](045-optional-persistent-encoded-body-l2-cache.md): the persistent
cache stores that start next to the currentness watermark, and an index slot
can carry a newer watermark than its record. It does not reopen
[ADR-066](066-reject-timestamp-versioned-snapshot-history.md),
[ADR-067](067-reject-backend-versioned-snapshot-ingestion.md), or
[ADR-068](068-reject-demand-activated-dependency-checkpoints.md).

## Context

A read-only transaction takes no locks and does no writes. With a warm cache,
its body does no backend operation. Its validation still does one conditional
read for each leaf state that it read, because optimistic validation proves
that every read was current at a fresh validation barrier. A body that reads
keys in eight leaves therefore costs eight backend operations, even when no
key changed for hours.

Many readers do not need the latest state. They need a consistent state that
is not too old. Spanner and CockroachDB offer bounded-staleness reads for this
purpose: the database picks the newest timestamp within the bound that it can
serve without a wait. GlassDB has no commit timestamps, and ADR-066 to ADR-068
rejected the three designs that tried to add history. This decision keeps no
history. It uses only the states that are already in the cache, and it proves
that they were current together at one instant in the past.

The proof uses the argument that optimistic validation already uses. A read
completes before the validation barrier. A conditional read that starts after
the barrier finds the same state. So the state was current over the whole span
between the two operations, and the barrier lies in that span. Today the span
of each state ends at its currentness watermark, but the cache does not record
where the span starts.

## Decision

### Each cached state has a currentness interval

When the cache installs a new state of an object, it records the sequence
point at which the operation that produced the state completed. Call this
point the start. Each later confirmation of the same state advances only the
currentness watermark. The state was current at every instant between the
start and the watermark. This is the currentness interval of the state.

An interval with a watermark before its start is empty. A state that only one
operation observed has an empty interval. That operation shows that the state
was current at some instant between its invocation and its completion, but not
at which instant.

The argument that the state did not change inside the interval is the same
argument that optimistic validation uses between a read and its barrier. This
decision adds no new assumption about revisions.

### `Database::read_tx` returns the state at one cut point

`Database::read_tx(max_staleness, body)` runs a stale read-only transaction.
The body receives a `ReadTransaction` handle. The handle can read keys, scan
keys, and resolve and list collections. It cannot write.

The body reads the cache as a `Database::tx` body does. Before the caller
receives a body outcome, GlassDB chooses a cut point: one sequence point that
lies in the currentness interval of every state that validation depends on.
These states are the leaf states of the point reads, the leaf states of the
scan coverage, and the collection records of the directory reads. GlassDB
takes the latest such point. The point must also satisfy two bounds:

- It is not older than `max_staleness`, measured on the timeline of the
  database instance from the start of the call. `Duration::MAX` accepts any
  age.
- It is not before the monotonic floor of the database instance.

If a cut point exists, the transaction does no backend operation.

The transaction captures the staleness cutoff once, when the call starts, and
replays of the body keep it. Spanner and CockroachDB also measure bounded
staleness from the start of the read. A cutoff that is computed after the body
would be later than the fetches that the body made. Then a read of one state
that the cache does not have would need a second check with a zero bound,
because its fetch was invoked before that cutoff. The cutoff converts a
duration to a sequence point by approximation, so it is not a currentness
barrier. A zero bound subtracts nothing, though: its cutoff is a point
allocated at the start of the call. The cut point then follows every
operation that completed before the call, so the transaction sees every
transaction that committed before it started.

The caller gets the state of the database at the cut point. Every transaction
whose commit point is before the cut point is visible, and no later
transaction is visible. Read-write transactions are strict serializable, so
this state is the result of a prefix of their serial order. A stale read-only
transaction is therefore serializable, and its staleness has a bound.

A body that reads one state only needs no interval. Its cut point is the
instant at which its read took effect. The watermark of the state must satisfy
the two bounds.

### Refresh only the lagging states

If no cut point exists, GlassDB chooses a target point. The target is the
latest start among the states, raised to the two bounds. GlassDB checks each
state whose watermark is before the target, with a conditional read. The
checks run in parallel, under `transaction_leaf_parallelism`. An unchanged
state then has a watermark after the target, so the target becomes a cut
point.

If a check finds a changed state, GlassDB runs the validation of
`Database::tx` at a fresh barrier. That path, the body replay after a failure,
and the locked replay of
[ADR-091](091-read-only-validation-locks-after-repeated-failures.md) do not
change.

### Holders use the fresh barrier

The effective state of a key with a write or create holder depends on the
status of that holder. A read that depends on a holder, and a scan whose
coverage has a pending membership holder, make the transaction use the fresh
barrier. Holders are short-lived, so most reads do not depend on one.

### The monotonic floor orders stale read-only transactions

Each database instance keeps a monotonic floor in memory. After a stale
read-only transaction returns, the floor rises to the latest start among the
states that the cut used. After a fresh-barrier validation, the floor rises to
that barrier.

Suppose that one stale read-only transaction sees the effect of a transaction
`T`. The state that shows the effect has a start after the commit point of
`T`. Each later stale read-only transaction of the same instance has a cut
point after that start, so it also sees `T`. It also sees each transaction
that `T` depends on, because the state at an instant contains a prefix of the
serial order.

A transaction sees its own committed writes without the floor. A commit
installs the states that it wrote in the cache. The cache keeps at most one
state for each object, and it never replaces a state with an older one. So a
later read of the same object observes the write or a newer state.

### The persistent cache keeps the start

The persistent cache stores the start next to the currentness watermark, in
the record and in the index slot. A state that is loaded from the persistent
cache keeps its interval.

A confirmation advances the watermark in memory only. So when the decoded
cache evicts a state whose watermark is newer than its persistent copy, and at
a clean shutdown, the persistent cache rewrites the index slot of that exact
record with the newer watermark. The slot carries a check over its fields and
the digest of its record, so a torn or corrupt slot cannot claim a watermark
that a backend operation did not confirm. In the same database open, a state
that leaves the decoded cache and comes back from the persistent cache then
costs no backend operation. A crash loses the advances since the last write,
which costs one check round later.

After a reopen, ADR-045 clamps a finite staleness cutoff to the first point of
the new open. The process cannot measure its downtime without a wall clock. A
first stale read-only transaction with a finite `max_staleness` after a reopen
therefore checks its states once. With `Duration::MAX`, it can use the
persistent states without a check. The monotonic floor starts again for each
open.

## Consequences

- A stale read-only transaction over a stable, cached working set does no
  backend operation. The cost of a changing working set is at most the cost of
  `Database::tx` validation.
- The commit path, read-write transactions, `Database::tx`, and the backend
  trait do not change. A database that never calls `read_tx` pays only for the
  start of each cached state.
- Serializability does not depend on time. Only the staleness bound uses the
  timeline of the database instance, as a stale read already does.
- The floor gives monotonic reads across stale read-only transactions of one
  database instance only. A stale read-only transaction that follows a
  `Database::tx` call can miss a foreign transaction that the `tx` call saw.
  It does not miss a write of the `tx` call itself. Processes that must share
  an order need a token that crosses processes, which this decision does not
  add.
- A body that runs for a long time does not keep a fixed state. If its data
  changes, it replays as a `Database::tx` body does. Analytics and backup
  workloads that need a fixed state for minutes remain outside scope, as
  ADR-066 to ADR-068 record.
- The persistent cache format changes.

## Alternatives considered

- **A fresh barrier for every transaction.** This is the current behavior. It
  gives a consistent cache no benefit.
- **A database-wide staleness setting.** Different reads in one application
  accept different ages. The parameter of each call matches
  `Collection::read_stale`.
- **No monotonic floor.** Each transaction gets the most cache hits, but a
  later transaction can miss a transaction that an earlier one saw. Callers
  must know that rule to use the API correctly.
- **A floor that every transaction raises, including `Database::tx`.** Each
  transaction raises it to the latest start among the states that it read.
  This gives monotonic reads across all transactions of one database
  instance. A prototype measured it on the `workers` benchmark. Each
  read-write transaction raised the floor, so most stale read-only
  transactions had to check their states again. It kept only a small part of
  the gain over `Database::tx`.
- **A past cut point in logical validation and for holders.** The status of a
  holder at a past point needs a separate argument for each status, for a
  small gain.
- **A wall clock to measure the downtime between opens.** Correctness would
  then depend on the clock of the host.
- **An option on `Database::tx`.** A write in a stale transaction is then an
  error at run time, not at compile time.
