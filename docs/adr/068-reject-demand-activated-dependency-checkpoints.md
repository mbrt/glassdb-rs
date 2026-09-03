# ADR-068: Reject demand-activated dependency checkpoints

## Status

Accepted.

This record replaces the last active snapshot-read design.

## Context

The feature under discussion was `Database::read_tx`: one read-only execution
that keeps a fixed logical database state for minutes, over point reads, key
scans, and collection enumeration, without data locks and without commit
validation.

The two earlier proposals failed because they charged every commit for the
feature, and because their cuts needed a clock guarantee that object storage
does not give. The third proposal removed both. Its shape was:

- **No clock in the cut.** A cut was dependency-closed instead of a prefix of
  the real-time order. A checkpoint could contain a transaction only if it also
  contained everything that transaction read, replaced, or observed. Two
  transactions in different sessions that neither conflict nor observe each
  other had no edge, so a checkpoint could contain either one alone.
- **No backend operation in the commit path.** A commit appended a local event
  with its effects and its dependencies. A background exporter wrote ranges of
  events as immutable session deltas.
- **Materialized checkpoints.** Background compilers applied deltas to an
  immutable, structurally shared tree that held values, key membership, and the
  collection catalog. Any process could compile; a conditional write on the
  checkpoint head chose the winner. A read bound one certified checkpoint and
  could then run with no backend operation at all.
- **Demand activation.** Every database supported snapshots, but a one-way
  durable latch started the maintenance work only when an application first used
  a snapshot operation. A database that never used one stayed dormant.
- **Fences.** A caller could serialize a local frontier and pass it to another
  process, which then waited for a checkpoint that covers it.
- **Pin-free retention.** A bound snapshot had a maximum lifetime. The collector
  kept a root reachable for that lifetime plus guards, measured on local elapsed
  clocks, instead of using a per-reader pin in the backend.
- **Reconciliation.** Events are asynchronous, so a crash can lose them. The
  design recovered through a full-state rebase that seals each mutable object
  against a stale writer.

## Decision

Do not implement demand-activated dependency checkpoints. The reasons are:

- **The recovery step is unproved, and it is the centre of the design.** A
  rebase may exclude a session that does not answer only with a proof that every
  data leaf, catalog root, absence condition, and deletion route is covered, and
  with a proof that each logged transaction lies wholly before or after the cut.
  The design states both as conditions for a future prototype, not as results.
  Until they exist, one crashed writer can hold snapshot progress and its
  retained state for an unbounded time.
- **Retention needs a clock contract that no portable API gives.** A reader may
  hold a root without a backend pin only if its clock and the collector's clock
  are monotonic, advance during machine suspension, and have a documented rate
  error. That must hold continuously, on each supported platform, for the whole
  execution. A collector on another machine reclaims the root whatever the
  reader later observes, so the invariant cannot be checked after the fact.
- **The change touches almost every subsystem, and the value does not pay for
  it.** It adds a second immutable tree beside the coordination tree, event
  capture in the commit path, background export and compilation, cooperative
  work claims, a second garbage collector, durable session records with
  keep-alives, and pre-cut bytes carried inside the authoritative conditional
  write. Each part needs its own correctness argument, and the demand latch only
  postpones the cost; it never removes it after first use.
- **The contract is hard to state.** A cut that is dependency-closed but not
  real-time ordered can omit a transaction that finished before another one it
  contains. Callers must understand that rule to use the API correctly.

## Consequences

- The commit path, the coordination tree, and reclamation stay as the accepted
  ADRs define them. No snapshot obligation constrains a future change to them.
- An application that needs a stable view over many keys must build it above
  GlassDB, or use one read-write transaction and accept its validation and
  lifetime.
- Nothing in this record reserves the approach. A later design may reuse parts
  of it, but it must argue them again with the evidence that is missing here.

## Alternatives considered

- **The two earlier proposals.** ADR-066 and ADR-067 record why timestamped
  history and provider-versioned ingestion were rejected.
- **A public switch for snapshot maintenance.** An option at creation time gives
  the baseline engine to applications that do not want snapshots, but it makes
  two database formats and leaves the objections above in the format that has
  the feature.
- **Read-time delta chains instead of materialized checkpoints.** Publishing a
  base plus a sequence of deltas lowers background write amplification, but each
  point read and each scan must then merge several sources, which complicates
  caching and retention.
- **Per-reader pins in the backend instead of a lifetime.** A pin removes the
  clock contract, but it puts a durable write and a keep-alive in each snapshot
  read, and a crashed reader then holds storage until another process proves it
  is gone.
