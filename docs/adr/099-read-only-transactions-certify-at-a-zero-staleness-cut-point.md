# ADR-099: Read-only transactions certify at a zero-staleness cut point

## Status

Accepted.

Refines [ADR-097](097-stale-read-only-transactions-choose-a-past-cut-point.md):
a read-only pass of `Database::tx` also uses the cut search, so `Database::tx`
changes. The monotonic floor stays a mechanism of stale read-only transactions.

[ADR-102](102-a-stale-view-orders-its-stale-read-only-transactions.md) moves the monotonic floor to a stale view, so a read-only
`Database::tx` uses no floor.

## Context

A read-only pass of `Database::tx` captures a fresh validation barrier after
the body. Then it checks every state that it read with a conditional read
that starts after that barrier. A body that fetches one key from the backend
therefore costs two backend reads: the fetch and the check.

ADR-097 showed that a stale read-only transaction with a zero bound gives the
same guarantee. Its staleness cutoff is a sequence point allocated at the
start of the call, so its cut point follows every operation that completed
before the call. It accepts more evidence than a fresh barrier, though:

- A fetch that the body makes is invoked after the cutoff. When the body reads
  only that state, it needs no second check.
- A state that another operation of the same instance confirmed during the
  call needs no check.

## Decision

### Each transaction captures a zero staleness cutoff

`Database::tx` captures a staleness cutoff with a zero bound when the call
starts. Replays of the body keep it. A read-only pass certifies its reads with
the cut search of ADR-097, from that cutoff. The read-only passes are the
commit of a body with no writes and the validation of the reads of a body that
returns an error. The fallbacks of ADR-097 do not change. When the search
cannot plan a target, the pass validates at a fresh barrier. When a check of a
lagging state finds a change, the pass validates at the barrier of the plan.

A read-only `Database::tx` stays strict serializable. Its cut point follows
the cutoff, and the cutoff follows the invocation of the call. The cut point
is also before the certification, which is before the response. So the
transaction takes effect inside its call. It sees every transaction that
committed before the call started.

### A pass with writes does not use the cutoff

A replay that writes, after a read-only attempt, uses the commit path. That
path validates its reads with its locks or its CAS and takes effect at its
commit point, so it does not depend on the cutoff.

A pass that engaged an identity sets locked validation for the later passes
([ADR-091](091-read-only-validation-locks-after-repeated-failures.md)). A
read-only replay after such a pass therefore uses locked validation, not the
cut search.

### `Database::tx` does not raise the monotonic floor

Only a stale read-only transaction raises the floor, as in ADR-097. A
read-only `Database::tx` uses the floor as a lower bound for its cut point,
but it does not raise it.

## Consequences

- A read-only `Database::tx` whose body fetches one state and reads no other
  costs one backend read, not two. A state that an operation of the same
  instance confirmed during the call costs no check when it reaches the
  target.
- A body can observe a state that an operation inside the call replaced, for
  example a commit that the body itself starts on the same instance. The
  transaction then takes effect before that operation. Strict serializability
  permits this order, because the two operations overlap.
- A replay of a body that reads one state reads the state that the failed
  check fetched after the cutoff. That state is a cut point by itself, unless
  a stale read-only transaction raised the floor past it. So the locked
  validation of ADR-091 starts mostly for bodies with several states.
- `Database::read_tx` with `Duration::ZERO` has the guarantee and the cost of a
  read-only `Database::tx`. It is different only because it raises the floor.
- A stale read-only transaction that follows a read-only `Database::tx` can
  still miss a foreign transaction that the `tx` call saw, as ADR-097 states
  for every `Database::tx`.
- Each transaction allocates one more sequence point when it starts.

## Alternatives considered

- **Keep the fresh barrier for `Database::tx`.** A read-only body that fetches
  a key pays a check that a zero-bound cut skips.
- **Let a read-only `Database::tx` raise the floor.** A stale read-only
  transaction would then see what an earlier read-only `tx` call saw, but
  still not what a read-write `tx` call saw. The rule is harder to state than
  the rule of ADR-097, and a measurement on the mixed workload, with stale
  read-only transactions beside read-only `tx` calls, showed that the stale
  read-only transactions lost throughput.
