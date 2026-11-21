# ADR-103: A stale view keeps its cut point

## Status

Accepted.

Changes [ADR-097](097-stale-read-only-transactions-choose-a-past-cut-point.md)
and [ADR-102](102-a-stale-view-orders-its-stale-read-only-transactions.md): a
stale view keeps its cut point while the point is within the bound, the decoded
cache keeps earlier states for the stale views, and the cut uses the interval
of each key, not of its leaf state. ADR-097 kept no history; this decision
keeps some in memory. It does not reopen
[ADR-066](066-reject-timestamp-versioned-snapshot-history.md),
[ADR-067](067-reject-backend-versioned-snapshot-ingestion.md), or
[ADR-068](068-reject-demand-activated-dependency-checkpoints.md): the history
is local, it costs the commit path nothing, and it does not depend on a clock.

## Context

A stale view orders its calls with its monotonic floor (ADR-102). After each
call, the floor rises to the latest start among the states of its cut. Under
writes, one of the states that a call uses is almost always new, so the floor
follows the present. Then most cached states of the next call have a
watermark before the floor, and the call checks them. In the canonical
`stale-reads` grid, a view call that reads 10 keys costs about as much as
`Database::tx`.

The bound does not help. The floor is almost always later than the cutoff, so
the floor, not the bound, sets the cut point.

Earlier states alone do not help either. A cut point must lie in the
currentness interval of every state of the cut, and an interval is short: it
ends at the last confirmation of the state. Under writes, the intervals of the
leaves of one call rarely have a common point. A cut point needs two things
together:

- A point that the view keeps, so that each confirmation after the point
  covers it for good.
- The state that each object had at that point, also after a writer replaced
  it.

A leaf state also changes when a writer changes any of its keys. Validation
of `Database::tx` already accepts a leaf that changed when the read key kept
its effective writer. The cut of ADR-097 does not: it fails when the leaf state
changed.

## Decision

### A stale view keeps its cut point within the bound

Each call of a stale view cuts at a point that is not older than the bound,
measured from the start of the call, and not before the monotonic floor of the
view, as ADR-097 and ADR-102 state.

A call cuts at the floor when the floor is within the bound and every state of
its cut reaches the floor. The floor then does not move. It moves only when a
call cannot cut at it: when the floor is older than the bound, or when a state
of the call does not reach the floor.

So the bound sets the use of a view:

- With `Duration::ZERO`, each call cuts after its own start, as a read-only
  `Database::tx` does (ADR-099).
- With a finite bound, the floor moves at least once in each bound while the
  view is in use.
- With `Duration::MAX`, the floor moves only when a state does not reach it.
  The view then gives a consistent state that changes as rarely as the cache
  allows. A scan of data that does not change, for example the data of an
  earlier day, can then use the cache for as long as the view lives.

### The decoded cache keeps earlier states

When the decoded cache replaces a leaf state with a newer one, it keeps the
replaced state with its evidence, as an earlier state of the path. The evidence
keeps growing after the replacement: a CAS of this instance already advances
the watermark of the state that it replaces to the invocation of the CAS.

The decoded cache keeps an earlier state only while a live stale view can use
it. For each path, it keeps the newest state that started at or before the
lowest floor of the live views, and every later state. Earlier states count
against the size limit of the decoded cache, and the cache evicts them before
the current states. With no live view, the cache keeps no earlier state.

The persistent cache does not store earlier states.

### A read in a view uses the state at the floor

When the floor is within the bound of a call, and the call reads a leaf, it uses
the state of the leaf whose interval covers the floor: the cached state, if it
started at or before the floor, or else an earlier state. If no state covers
the floor, the read uses the cached state. When the floor is older than the
bound, every read of the call uses the cached state. Index nodes, transaction
records, and collection records always use the cached state.

A replay of the body uses the cached states, so the floor moves.

### The cut uses the interval of each key

The committed writer of a key never comes back after it changes. So if two
leaf states show the same writer for a key, the key did not change at any point
between them, also across states that this instance did not observe. The
membership generation of a leaf only grows, so the same argument holds for an
absent key with the same membership generation.

The key interval of a point read starts at the earliest start, and ends at the
latest watermark, among the known states of its leaf that show the key with the
same writer. A state with an exclusive holder on the key is not in the key
interval, because the status of the holder at a past point is unknown. A state
whose range does not contain the key is not in the key interval either.

The cut of a view call uses the key interval for each point read. Range scans
use the interval of each covered leaf state, and directory reads use the
interval of each collection record, as in ADR-097.

### The cut point is the earliest point that every read can reach

The target is the latest start among the intervals of the call, raised to the
floor and to the staleness cutoff. It is the earliest point that every state
can cover. When the target is the floor, the view does not
move.

For a key whose interval does not reach the target, GlassDB checks the leaf
state that the call read. If the leaf state changed, but the new state shows the
key with the same writer, the key reaches the target. If the cached leaf state
already shows another writer for that key, the key cannot reach the target. The
call then replays its body with no check.

The fallbacks of ADR-097 do not change.

### What a view guarantees

- Each call reads at one cut point, so its result is serializable.
- The cut point of each call is not older than the bound, measured from the
  start of the call.
- The calls through one view and its clones are ordered, as ADR-102 states.

The bound applies to the writes of this instance too. So a call can miss a
write that this instance committed within the bound before the call. With a
zero bound, each call sees every transaction that committed before the call
started, as ADR-099 states. ADR-102 promised two more things, and this decision removes
both:

- That each call sees every write that its instance committed before the call.
- That a call never uses an older state of an object than an earlier
  transaction of the instance saw.

A caller that must read the writes of its own instance uses `Database::tx` or
a view with a zero bound.

## Consequences

- A view whose working set is cached costs no backend operation for a call that
  cuts at the floor. With a finite bound, each move of the floor costs about one
  check for each leaf that the view then reads. The key intervals also help a
  call through a new view. With writers in other instances, the gain is
  smaller. The measurements of a prototype are in
  [the performance investigations](../../hack/perf/investigations.md#2026-11-20-a-stale-view-that-keeps-its-cut-point).
- The floor still moves when a key that a call reads changed after the floor,
  and the view has no state of its leaf that covers the floor. A write of this
  instance leaves a gap between the invocation of its CAS and the install of
  the new state, because the write took effect at an unknown point between
  them. A write of another instance leaves a gap from the last confirmation of
  the replaced state.
- After the floor moves, each key reaches the new floor only after a
  confirmation of its leaf. Writes of other instances do not advance the
  intervals in this instance, so these checks are most of the remaining cost
  with several instances.
- The decoded cache uses more memory while views live.
- A large scan through a view is one call. Under writes to the scanned data, a
  leaf state that does not reach the floor is likely, and the call replays the
  whole scan. Range scans use the interval of each leaf state, so any write to
  a covered leaf after the floor can cause it. A scan of data that changes
  rarely does not have this cost.
- `Database::tx` and the commit path do not change. A database instance with no
  live view keeps no earlier state.
- The simulation history checker must use the new guarantees: a call can miss
  the writes of its own instance.
- When the decoded cache evicts an earlier state, the key intervals that the
  state extended become shorter, and the persistent cache does not restore
  them. A view can then move although its keys did not change. A working set
  larger than the decoded cache has this cost most.
- Two later changes can reduce these costs: a state that carries the start of
  each unchanged key forward, also in the persistent cache, and key intervals
  for range scans.

## Alternatives considered

- **A bound only on the first call of a view.** Later calls would cut at the
  floor with no age bound, so the floor would move only when a state does not
  reach it. A view with a zero bound would then give a strong first call and an
  old state after it, which contradicts the name of the bound. `Duration::MAX`
  gives the same use with a contract that a caller can read from the code.
- **Earlier states only, with the floor of ADR-102.** The floor still follows
  the present, so the earlier states rarely cover it.
- **Pins in each view.** Each view would keep the states that cover its floor.
  Views of one instance could not share them, and the decoded cache already
  holds the same states.
- **A refresh round over the working set of the view.** One round of checks
  makes every unchanged leaf cover one point. At 50 writers, most leaves change
  during the round, and the round costs more than the calls that it serves.
- **Leaf intervals only.** At 50 writers, a write is in flight on a large part
  of the leaves at any point, so most calls found a leaf with no state at the
  floor, and the view moved on most calls.
- **Earlier states for index nodes.** A read routes through the cached index
  nodes, and the range of each leaf state shows whether the state holds the key.
  A split after the floor makes the key use a new leaf, and the view moves.
  Splits are rare, so this does not pay for the cost.
- **Keep the read of the own writes of the instance.** Each write of this
  instance to a leaf of the view would move the floor. With writers in the same
  instance, most of the gain goes.
- **Earlier states in the persistent cache.** The persistent cache format would
  change, for views that end with the process.
