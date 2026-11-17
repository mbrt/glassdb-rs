# ADR-098: A stale read is a stale read-only transaction

## Status

Accepted.

Refines [ADR-036](036-decoded-object-cache-with-bounded-freshness.md): the
stale read keeps its API and gets the guarantees of
[ADR-097](097-stale-read-only-transactions-choose-a-past-cut-point.md). Does
what [ADR-043](043-causally-coordinated-backend-operations.md) expected of a
later snapshot design: a stale read now has a bounded-staleness guarantee, not
only an approximate cache policy.

## Context

`Collection::read_stale(key, max_staleness)` reads one key outside a
transaction. It accepts a cached leaf state whose currentness watermark is
within `max_staleness`, and it makes no other check.

ADR-097 added `Database::read_tx(max_staleness, body)` with a monotonic floor
for each database instance. A stale read does not use the floor. So a stale
read that follows a stale read-only transaction can return an earlier state
than that transaction saw, and the reverse is also possible. The two APIs have
the same staleness parameter but different guarantees.

## Decision

`Collection::read_stale(key, max_staleness)` runs a stale read-only
transaction whose body reads only `key`. It is the same as:

```rust
db.read_tx(max_staleness, |tx| async move { tx.read(&collection, key).await })
```

It has the guarantees of `read_tx`. The result is serializable and no older
than the bound. The monotonic floor orders it with the other stale reads and
stale read-only transactions of the instance.

## Consequences

- One stale read costs no backend operation when the leaf state reaches the
  floor and the staleness cutoff. Otherwise the cut checks the leaf once. With
  `Duration::ZERO`, a read that fetches the leaf therefore costs one more
  conditional read than before, because the cutoff is after that fetch.
- A stale read that depends on a holder uses the validation of
  `Database::tx`. After repeated failures it can use locked validation
  ([ADR-091](091-read-only-validation-locks-after-repeated-failures.md)),
  which writes to the backend. The earlier stale read never wrote.
- A stale read counts as a transaction in `Stats`.
- No read accepts a state by its age now. `Requirement::within` and the
  staleness parameter of the transaction read path are removed. Only the cut
  point converts a duration to a sequence point.
- The rule of [ADR-047](047-transactional-collection-management.md) does not
  change: a stale read can return data from before a drop of its collection
  while its bound allows a cut point before the drop, and it fails when it
  observes the drop.

## Alternatives considered

- **Keep the read path and add a cut of one state.** It would use the floor
  with no transaction, no replay, and no lock. But it would be a second
  implementation of the same guarantee, with differences in cost and
  behavior that a caller cannot see from the API. A stale read that behaves
  like `read_tx` with one read is less surprising.
- **Keep the stale read as it was and document the gap.** Callers would need
  to know which of two stale APIs respects the floor.
