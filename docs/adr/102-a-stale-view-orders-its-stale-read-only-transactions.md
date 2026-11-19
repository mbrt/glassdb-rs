# ADR-102: A stale view orders its stale read-only transactions

## Status

Accepted.

Changes the monotonic floor of
[ADR-097](097-stale-read-only-transactions-choose-a-past-cut-point.md): the
floor belongs to a stale view, not to a database instance. The other rules of
the floor do not change. It also changes the shared floor of
[ADR-098](098-a-stale-read-is-a-stale-read-only-transaction.md) and the floor
that a read-only `Database::tx` uses in
[ADR-099](099-read-only-transactions-certify-at-a-zero-staleness-cut-point.md).

## Context

ADR-097 gives each database instance one monotonic floor. After a stale
read-only transaction returns, the floor rises to the latest start among the
states of its cut. After a fresh-barrier validation, it rises to that barrier.
Each later stale read-only transaction of the instance has a cut point after
the floor, so it sees every transaction that an earlier one saw.

Under writes, the floor of an instance stays close to the present. Each stale
read that uses a new state of a leaf raises it, and so does each fallback to
validation. A cached state whose watermark is before the floor needs a check,
also when the state did not change and the bound accepts its age. So one write
that a stale read sees makes the next stale reads of every other leaf of the
instance check their states. Most of these checks find the state unchanged.
The floor, not the bound, then sets the cut target, and a larger bound does
not help.

A stale read-only transaction does not need the floor for a consistent result,
because it reads at one cut point. The floor only orders the stale read-only
transactions of the instance. Many callers do not need that order, for example
independent requests, or a dashboard that refreshes. A caller that needs it
can say so in the code.

## Decision

### A stale view has its own floor

`Database::stale` and `Collection::stale` return a new stale view with its own
monotonic floor. `StaleDatabaseView::read_tx` and the helpers of a view use
and raise the floor of the view, with the rules of ADR-097. Clones of a view
share its floor. A database instance has no floor, and a read-only
`Database::tx` uses none.

The calls through one view, and through its clones, are ordered as ADR-097
orders the stale read-only transactions of an instance. Calls through two
views are not ordered. A call through one view can miss, on one key, a write
that a call through another view saw on another key.

### The other guarantees stay

- A call reads at one cut point that is not older than the bound of its view.
- A call sees every write that its instance committed before the call. The
  cache gives this without the floor (ADR-097).
- For each object, a call never uses an older state than an earlier
  transaction of the instance saw. The decoded cache never replaces a state
  with an older one. Each change of a path also replaces or clears its
  persistent copy, so a reload cannot bring back an older state. A read routes
  through the newest cached states of the tree, so this also holds for each
  key.
- A call through a view with a zero bound sees every transaction that
  committed before the call started (ADR-099). Calls with a zero bound are
  ordered by real time, through any views.

### One way to run a stale read-only transaction

A stale read-only transaction always runs through a view. A one-off call with
a new view, `db.stale(bound).read_tx(body)`, is not ordered with other calls,
and the code shows it. A caller that needs the order keeps the view.

## Consequences

- A stale read-only transaction through a new view uses its cached states up to
  its bound, also while other leaves of the instance change.
- A view that a caller keeps and shares with many tasks has the cost of the old
  floor of the instance: each new state that one of its calls uses raises its
  floor. A short-lived view keeps its floor low.
- A cut over several states still needs checks when one state is newer than the
  watermarks of the others. That cost comes from the cut, not from the floor.
- The order does not cross database instances. A token that carries it to
  another instance is not part of this decision.
- The simulation history checker orders a stale read-only transaction only
  after the earlier ones of the same view. It does not check the per-object
  guarantee across views, which integration tests check.

## Alternatives considered

- **Keep the floor of the instance.** It orders all stale read-only
  transactions of the instance, but under writes the bound has almost no
  effect.
- **No floor.** It has the lowest cost, but a caller cannot get the order of
  ADR-097 at all.
- **A floor for each leaf or collection.** A transaction that writes several
  leaves breaks the order: a later read of another leaf that it wrote can miss
  it. Transitive dependencies need the tracking that
  [ADR-068](068-reject-demand-activated-dependency-checkpoints.md) rejected.
- **Cheaper checks, for example one listing for many leaves.** Under writes the
  floor moves often, so most calls still wait for a check. The backend contract
  also gives no revisions in a listing.
- **Raise the floor only for states with new effects.** A leaf state that only
  adds or releases a lock would not raise it, but each new effect still raises
  the floor of the whole instance.
- **An explicit ordering token.** It adds an API that the caller must pass
  between calls. A view carries the order without it, and keeps one entry
  point.
