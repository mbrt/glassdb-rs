# ADR-064: Bounded parallel point-leaf work

## Status

Accepted.

## Context

A point access resolves one exact logical key to one leaf. A transaction that
touches `L` distinct leaves does `L` independent pieces of physical work in each
of its phases: routing, point validation, lock acquisition, and committed
write-back.

Today that work is not consistently parallel:

- routing descends for one key at a time, so `L` cold leaves cost `L` backend
  wait waves even though the descents are independent;
- physical point validation checks each leaf observation separately, and it
  repeats a check for observations of the same leaf state;
- logical point revalidation routes and resolves one key at a time; and
- committed write-back publishes one leaf group at a time.

Normal lock acquisition is the one phase that already runs its leaves together,
but it is unbounded: a transaction with many leaves submits all of them at
once. So the engine has both problems at the same time. Most phases are too
serial for object storage, where the wait is the backend round trip, and one
phase has no bound at all.

Two more facts constrain a solution. Work on one leaf must stay ordered,
because the leaf is the CAS unit and `ShardCoordinator` owns its mutation
stream. And a bound must be a property of the engine, not of each call site,
or the phases drift apart and each call site becomes a knob.

## Decision

Add one reusable bounded join to `glassdb-concurr`. It is a semantic variant of
`join_all`: it polls in the caller's task, admits inputs in input order, keeps
at most `N` incomplete futures, runs every input, and returns every output in
input order. A wait counts as incomplete and holds its position. Zero and one
input use direct paths, so a one-leaf transaction gets no queue and no added
backend operation.

The join stays generic. It has no parking, no replacement work, no terminal
outcome, and no cleanup protocol. Dropping it drops the admitted and the stored
futures, and each operation keeps its existing cancellation guard.

Every rule about *which inputs belong together* stays in the domain module that
understands the rule:

- `TreeRouter` combines point keys that currently route through the same
  physical path, and spends the bound on distinct paths instead of keys;
- `NodeStore` combines physical checks only for the same exact leaf state;
- `KeyResolver` resolves the logical point state for all keys in one routed
  leaf group;
- `KeyLocker` combines every intention for one leaf, point and range
  membership, into one atomic coordination member; and
- committed write-back keeps one bounded position for each original leaf group
  it acquired.

There is no shared routed point-leaf plan, no stateful point-leaf workflow, and
no domain-aware foreground executor. `AccessSet` stays the only point-access
fact shared between direct commit and the logged path. A routed leaf group is
the temporary result of one routing operation, not a durable ownership claim.

## Consequences

For `L` independent cold leaves and limit `N`, a phase needs approximately
`ceil(L/N)` backend-wait waves. One-leaf work adds no backend operation. Same-
leaf ordering, transaction phase ordering, and sorted serial acquisition remain
unchanged.

The value is a transaction-local submission bound, not an aggregate capacity
guarantee. Concurrent transactions can each admit up to the configured number
of incomplete leaf futures. Backend and deployment stress tests must therefore
verify the initial value of 16.

## Alternatives considered

- **An executable routed plan, a phase program, or a stateful point-leaf
  workflow.** Each one moves grouping, rerouting, and outcome interpretation
  into a shared type that must then understand every domain. The bounded join
  with domain batch interfaces keeps each rule where its evidence already lives.
- **A per-call limit, or one limit for each phase.** This turns one engine
  property into several unrelated knobs and lets the phases drift apart without
  a reason to.
- **A shared GlassDB active-backend-operation limit or global fairness rule.**
  The backend and provider already own aggregate queueing and throttling with
  better information. A second scheduler above them would compete with the
  first.
- **One task for each leaf.** Spawning removes the caller's polling context,
  the deterministic admission order, and the simple drop behavior, and it gives
  no benefit for work that waits on the backend.
- **A sorted leaf-chain sweep for point routing.** Right-sibling discovery is
  serial, and sparse keys make it read leaves that own no requested key. It
  stays the correct shape for ordered and range operations only.
- **Releasing locks between normal retries, as today.** It costs a CAS for each
  held leaf, and it cannot close the race with a leaf write that was dispatched
  before the acquisition future was dropped. See
  [ADR-065](065-renewed-transaction-identity-on-serial-fallback.md).
