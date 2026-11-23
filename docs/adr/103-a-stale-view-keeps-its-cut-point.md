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

A stale view marks each leaf that one of its calls reads. When the decoded cache
replaces the state of a marked leaf with a newer one, it keeps the replaced
state with its evidence, as an earlier state of the path. It keeps no earlier
state of an object that no view read, so records have none, and index nodes
have none except the tree root. A split turns the tree root into an index node
in place. The root keeps its replaced index states until a view reads an index
node at the root, which removes the mark of the root. The evidence keeps growing
after the replacement: a CAS of this instance already advances the watermark of
the state that it replaces to the invocation of the CAS.

For each floor of the live views, a path keeps the newest earlier state that
started at or before the floor, and the next newer earlier state. While a floor
is raised, the path also keeps its newest earlier state. When a present state
replaces the latest state of the path, the cache drops the other earlier states
of the path. With no live view, the next change of a path drops its earlier
states.

Only the newest state that started at or before a floor can cover the floor. A
state that started after the floor cannot cover it, and an older state was
replaced before that newest state started. The same state gives the key
intervals of the floor their earliest start. The next newer state gives the end
of the interval of a key that the write after the floor did not change. When
the floor is in the gap of that write, the older state has no confirmation
after the floor, and the key reaches the floor only through the watermark of
the newer state. The rule uses the floors at the time of the replacement, but a
call that read the replaced state can raise its floor to it later. While a
floor is raised, the newest earlier state covers that for one write during the
call.

The mark and the earlier states of a path are one cache entry, and the latest
state of the path is another. The two entries share the size limit and the
least-recently-used order of the decoded cache. Only a read in a view uses the
earlier states. The cache
replaces the latest state of a path and updates its earlier states in one
step, so a read never finds a replaced state in neither entry.

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
  call through a new view, for the leaves that a view read before. With writers
  in other instances, the gain is
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
- The first read of a leaf through any view cannot use an earlier state of the
  leaf. If the leaf changed after the floor, the call checks it, or the floor
  moves.
- The decoded cache uses more memory while views live. A mark is a small entry
  for each leaf that a view read. A path keeps at most
  two earlier states for each distinct floor of the live views, and one more.
  A view with `Duration::MAX` costs at most three earlier states for each leaf
  that changed after its floor.
- Under memory pressure, earlier states that a view reads can stay while the
  cache evicts the latest state of an object that nothing reads.
- After a floor moves, the earlier states that it used stay until their path
  changes again, or until the cache evicts them. A read of the leaf through a
  view keeps them in the cache. After the last view that reads a leaf drops,
  no read refreshes its earlier states, so they are the first to go.
- When a leaf changes twice during one call, or once during a call while no
  floor is raised (the first call of a view), the state that the call read can
  go before the call raises its floor to it. The next call then checks the
  leaf, or the floor moves.
- Views with different floors keep different states of the same leaf. A view
  could move its floor to the floor of another view to share them. This
  decision does not do that.
- A large scan through a view is one call. Under writes to the scanned data, a
  leaf state that does not reach the floor is likely, and the call replays the
  whole scan. Range scans use the interval of each leaf state, so any write to
  a covered leaf after the floor can cause it. A scan of data that changes
  rarely does not have this cost.
- `Database::tx` and the commit path do not change. A database instance that
  never had a view keeps no earlier state.
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
- **Keep every earlier state after the lowest floor.** Under writes to a hot
  leaf, a view with `Duration::MAX` adds one earlier state for each write, and
  no state goes. Each write also copies the list.
- **Keep only the newest earlier state that started at or before each
  floor.** It covers every floor that the cache can cover, but a key that the
  write after the floor did not change loses the end of its interval. A
  prototype with this rule moved the floor more often under writes.
- **A limit on the earlier states of each path.** It adds a constant, and under
  writes the state that covers an old floor is the first to go.
- **Earlier states in the cache entry of the latest state.** A read of the
  latest state then also keeps the earlier states, and the cache evicts them
  together.
- **A separate cache for earlier states.** It needs its own size limit, and
  the memory of that limit is not available to the latest states when no view
  lives.
- **Evict earlier states before any latest state.** It needs a second eviction
  order, which every cache update keeps up to date, also with no live view. No
  measurement showed the need.
- **Keep the earlier states of every leaf while a view lives.** It also gives
  the first read of a leaf through a view its earlier states. But the cache
  must know which objects are leaves, through a rule of each codec, and it
  keeps the states of leaves that no view reads.
- **Earlier states in the persistent cache.** The persistent cache format would
  change, for views that end with the process.
