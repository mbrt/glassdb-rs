# ADR-087: Coordinator rounds wait for a batching window

## Status

Accepted.

## Context

A coordinator round collects its round members while it loads the leaf
([ADR-025](025-dedup-shard-lock-acquisition.md),
[ADR-028](028-shard-mutation-coordinator.md)). The leaf is usually in the cache,
so the round collects members only for the CPU time of one yield and one load.
Whether two operations on one leaf share a leaf CAS therefore depends on CPU
timing.

This has three effects:

1. A client that waits between transactions pays an extra round. The write-back
   of a locked commit runs after the commit returns. If the next transaction of
   the client locks the same leaf after the write-back round sent its CAS, the
   lock waits for that round, and then needs a round of its own. A short gap
   between transactions doubles the rounds of a client that writes one leaf.
2. Clients that write one shared leaf batch only when they submit at the same
   instant. A short gap between their transactions spreads the submissions, and
   the rounds and the latency about double.
3. Real time and virtual time give different round counts. In virtual time, CPU
   work takes no model time, so only the operations of one instant share a
   round. In real time, the CPU work of tasks on several threads decides it.

A wait in model time before the first leaf load of each round removes the
dependency on CPU timing. Operations that arrive during the wait join the round.
On a live clock, a Tokio timer cannot wait less than about one millisecond.

## Decision

Each coordinator round waits for the batching window before its first leaf load.
The operations on the leaf that arrive during the window join the round.

- The window applies only to the first attempt of a round. A retry already waits
  for its backoff before it loads the leaf.
- The batching window is an option of each database instance. The default is the
  shortest wait that a live clock gives. A window of zero gives the behavior
  from before this decision: one yield.

## Consequences

- A client whose gap between transactions is shorter than the window pays no
  extra round for the write-back of its previous transaction.
- Clients that write one shared leaf share rounds when their submissions are
  less than one window apart, not only when they are at the same instant.
- Real time and virtual time give the same round counts for operations that are
  less than one window apart.
- Each round of a transaction is one window longer. A transaction with one round
  takes about one window more, which is small next to a backend operation. In
  real time with a model-time speedup, the window takes about 1 ms of wall time,
  so it costs more model time than its length.
- Locks that a round installs stay one window longer. When transactions conflict
  on a key, a waiting transaction waits that much longer for each round of the
  holder.
- A client whose gap between transactions is longer than the window still pays
  the extra round.

## Alternatives considered

- **A window only for rounds that no caller waits for.** Write-backs and lock
  releases by GC wait for the window, and an operation that a caller waits for
  ends the window when it joins. No transaction pays the window, and the extra
  round after a write-back goes away. But rounds of only transactions still
  batch by CPU timing, so clients that share a leaf keep effect 2, and the two
  time modes keep effect 3.
- **Sub-millisecond waits with a yield loop.** A task that yields until the end
  of the window waits exactly, but it uses one CPU core for the whole window. It
  also does not work on a paused clock, which advances only when the runtime is
  idle.
- **No window.** Batching keeps its dependency on CPU timing, and the
  experiments that must agree in both time modes stay in real time only.
