# ADR-070: Demand-driven garbage collection

## Status

Accepted — implemented.

Refines GC scheduling in
[ADR-022](022-garbage-collection-mark-sweep.md), transaction-object placement and
GC scans in
[ADR-035](035-paginated-listing-and-sharded-transaction-logs.md), and structural
recovery cadence in [ADR-034](034-separate-structural-log-namespace.md).
The paginated backend contract and reclamation safety remain as previously
accepted.

## Context

Each independently opened `Database` instance starts a periodic GC loop. Cloned
handles share that instance and its GC work; separate opens create separate
instances, including within one process. A cycle consumes local hints, makes up
to 64 LIST requests looking for a non-empty page among 4,096 transaction-log
prefixes, and checks GC candidates sequentially. Idle instances pay for sparse
scans. Busy instances cannot increase their GC rate beyond the work one loop
completes between fixed delays.

A check consumes its hint even when the candidate must wait for its safety
horizon. A later scan must find it again. Process failure and queue overflow
can also lose hints. Structural recovery adds separate periodic LIST requests.

ADR-035 chose two base64 characters to provide independent starting points for
scans. This gives 4,096 prefixes regardless of database size. Sparse prefixes
waste LIST page capacity; fewer prefixes make individual traversals longer in
large databases. A fixed count does not suit both cases.

GC cost should follow useful work, with an allowance for GC scans. GC backlog
must never make writers wait. Live values and pinned `Wounded` markers can
remain stored indefinitely; their presence alone is not GC backlog.

## Decision

### Writers do not wait for GC

GC candidate producers make bounded in-memory reports. Reporting never waits for
queue capacity, backend I/O, or GC completion. Queue overflow may discard a hint
and must be observable. GC scans find candidates whose hints were lost.
GC backlog must not delay transaction admission, completion, or retries, add
backend requests to the commit path, or transfer GC work to transaction bodies.
Protocol-required lock resolution and retirement still apply.

GC has separate limits for memory, concurrency, and backend requests.
Transaction work has priority when shared local resources are scheduled. At the
GC limit, garbage remains stored longer. Shared CPU, backend traffic, and leaf
mutations can still affect transaction latency; increasing backlog must not
cause unlimited GC concurrency. Cleanup delay can grow without bound during
sustained overload.

### Hints start GC directly

Local hints wake GC without causing a LIST. The `Database` instance de-duplicates
GC candidates and retains deferred checks with retry times in bounded memory.
The safety horizon controls eligibility, not worker frequency. Transient
failures receive delayed retries.

Ready candidates receive bounded parallel work without a fixed pause between
batches. With no ready work, GC sleeps until a hint arrives or a retry is
due. A completed check can retain an object: live objects return through later
hints or scans, and `Wounded` markers can require repeated checks for late
effects under [ADR-059](059-pin-foreign-wounds-until-owner-retirement.md).

### Transaction paths permit broad and narrow scans

Store transaction objects at:

```text
{db}/_t/{a}/{b}/{encoded-txid}
```

`a` and `b` are the first two characters of the existing encoded transaction
identity. Every `Database` instance can use the recursive backend contract at
three depths:

| Prefix | Scan scope |
| --- | --- |
| `{db}/_t/` | All transaction objects |
| `{db}/_t/{a}/` | One of 64 groups |
| `{db}/_t/{a}/{b}/` | One of 4,096 smaller groups |

Scan scope is a local choice. Changing it moves no objects and requires no
shared metadata. Request up to 1,000 identities per LIST, independently of the
number of candidates checked in parallel. Admit listing work only within the
instance's bounded GC buffering and processing capacity.

Keep the database format label at `v3`, which has not shipped. This replaces
the previous `{db}/_t/{ab}/{encoded-txid}` layout in place. No migration from v2
or the previous v3 layout is provided. Development databases using an older
layout must be recreated; mixed operation with older binaries is unsupported.
The unchanged v3 metadata label does not distinguish the two development
layouts. Existing v2 rejection remains in effect.

### Each Database instance adapts its GC scan scope and cadence

Each `Database` instance owns its GC queues, resource limits, scan prefixes,
cursors, and scheduling state. That state is disposable local memory. GC scans
require no claims, ownership leases, or coordination writes. Instances use
independent random scheduling to reduce simultaneous duplicate work;
overlapping scans remain safe under the existing reclamation rules.

Each instance selects one depth for new traversals: the root, 64 prefixes, or
4,096 prefixes. Those prefixes cover the whole transaction-log namespace.
Start at the root. Transaction identities have uniformly random leading bytes,
so a few randomly selected prefixes provide an estimate for the whole database.
Use that estimate to change the instance's scan depth, without separate choices
for individual regions or separate parent probes.

Use the following initial policy; the sample size and page thresholds must be
tested with representative workloads:

1. Visit prefixes in a locally shuffled order. Each turn issues one LIST page
   for one new or continuing traversal. Making all prefixes eligible does not
   issue requests for all of them at once. An empty result ends the turn instead
   of starting an immediate search through more prefixes. Only `next = None`
   completes a traversal.
2. For broader-depth decisions, use a moving window of four distinct sampled
   prefixes at the current depth. Select samples before observing their results.
   Count objects over each complete traversal; do not substitute the first four
   scans to finish. Incomplete scans and errors leave a sample pending, rather
   than contributing a zero. The first estimate waits for all four samples.
   Each later sample replaces the oldest sample in selection order; wait for an
   earlier selected sample if completions arrive out of order. Keep the four
   prefixes distinct within the window. Samples come from regular GC scans.
3. Estimate the total object count as the current prefix count multiplied by
   the average sampled count. Select the broadest depth expected to need at most
   32 pages per prefix, and change depth if that choice is broader than the
   current depth. The choice can skip a level. Subsequent regular scans supply
   new observations; no full sibling pass or confirmation probe is required.
4. If a traversal started under the current depth decision still has a cursor
   after 64 successful pages, select the next narrower depth, if one exists.
   This does not require a complete traversal. The separate broadening and
   narrowing thresholds reduce repeated depth changes near a threshold.
5. A depth change starts a shuffled schedule for new traversals and resets the
   sampling window. Require fresh observations before reversing the change.
   Preserve queued candidates and existing traversals with their original
   prefixes and cursors. Interleave their page work within the same GC budget;
   a large or failing traversal must not hold up the rest. Traversals started
   before the change still supply candidates, but do not update the new depth's
   sampling window or trigger another depth change.

For example, four complete samples of 100, 140, 90, and 150 objects at the
4,096-prefix depth estimate 491,520 objects in total. At the 64-prefix depth,
that is 7,680 objects per prefix, or about eight full pages. The instance can
select that depth after four LIST requests when each sample fits in one page.
There is no requirement to visit all 64 siblings first.

At 1,000 objects per page, an average of 500 objects per sampled prefix predicts
32 pages at the next broader depth. Page limits are upper bounds, so use observed
continuation-page capacity when the backend consistently returns shorter pages.
The estimate is not an exact count or a deletion condition: GC can temporarily
make retained objects uneven across prefixes. Random samples guide scan cost;
the existing reclamation checks continue to determine deletion safety.

Adjust the delay between scan turns from recent useful work per LIST. Resources
reclaimed or recovery advanced permit faster scans; repeated unproductive
results increase the delay. Smooth the observations and add random variation.
Live objects, unchanged pinned markers, and successful no-ops are not positive
demand signals. Errors use separate retry handling and are not evidence of an
idle database. Retain a finite maximum delay; never disable scans permanently.

Keep continuation work eligible while interleaving other prefixes. Unfinished
traversals must not repeatedly lose their cursors to long idle delays or scope
changes. Invalid provider cursors restart only their affected traversal.
Repeated passes remain necessary because LIST is not a snapshot. Reserve
bounded capacity for GC candidates found by scans so a continuous hint stream
cannot consume all GC scan capacity. Eventual reclamation requires running
`Database` instances, a working backend, and sufficient GC capacity and
successful traversal progress.

### Separate GC capacity, scan scope, and scan cadence

| Control | Signal | Response |
| --- | --- | --- |
| GC capacity | Age and amount of ready GC work | Increase parallel work within the GC budget |
| Scan scope | Sampled population estimate and observed traversal cost | Select one depth per instance; narrow for long traversals and broaden for sparse scans |
| Scan cadence | Useful recovery or reclamation progress per LIST | Scan faster when productive; back off otherwise |

Measure ready-candidate age from when its check becomes due. Also report dropped
hints, deferred checks, failures, GC throughput, traversal progress, LIST counts,
and page occupancy. These describe known work, not a complete count
of undiscovered garbage. Determine controller limits and thresholds through
experiments with idle, sparse, dense, overloaded, and changing workloads.

Structural recovery scans examine structural intents. They keep their own
namespace, recovery rules, and work budget, with an independent adaptive cadence
and local recovery signals. The transaction-log hierarchy applies to GC scans.

GC candidate scheduling and local scan state belong in the maintenance module,
behind the small producer hint interface. Transaction and structural recovery
modules retain their own reclamation rules. An optional dedicated process can
open a `Database` instance and run its background GC without submitting
application transactions. Object storage remains the only shared dependency.

## Consequences

- Small databases can be scanned with a few full pages instead of thousands of
  sparse-prefix requests. Large databases retain independent starting points
  and bounded scan turns. Restart can repeat work but needs no durable cursor.
- All instances still perform some idle LIST requests. The aggregate request
  floor grows with instance count and falls as the maximum scan delay increases.
  Duplicate scans and recovery overhead are accepted in exchange for removing
  shared claims.
- Sparse work left by lost hints can take longer to discover after backoff.
  Scope adaptation reduces the sparse-prefix cost but does not provide a fixed
  cleanup deadline under overload, backend failures, or repeated restarts.
- Resource limits protect transaction capacity at the cost of potentially
  unbounded retained garbage and cleanup delay during overload.
- Deterministic verification must cover lost hints, horizon and retry delays,
  sampled depth changes, incomplete and failed samples, short continuation pages,
  cursor progress across depth changes, overlapping GC scans from separate
  `Database` instances, changing workloads, and writer completion while GC is
  saturated or paused.
  Safety checks must continue to preserve live values and pinned wounds.

## Alternatives considered

- **Shared scan claims.** Acquisition, polling, and renewal add requests and
  durable state. Transferring ownership also loses local cursor progress.
  Independent adaptive scans avoid that machinery.
- **Always scan 4,096 prefixes.** This imposes too much sparse discovery work.
- **Use a fixed smaller prefix count.** This reduces sparse cost but commits
  large databases to longer traversals. The hierarchy permits both scan sizes.
- **Adapt individual regions after full sibling scans or parent probes.**
  Uniformly random transaction prefixes permit a shared estimate within each
  instance. Sampling regular scans reduces decision delay and avoids separate
  region state and probe requests.
- **Draw independent random prefixes with replacement.** This repeats prefixes
  before visiting others. Locally shuffled coverage retains random order while
  avoiding that cost within a pass.
- **Only change the interval or batch size.** This does not address sparse
  prefixes, lost deferred hints, and sequential cleanup together.
- **Use local hints alone.** Process failure and queue overflow can leave work
  undiscovered indefinitely.
- **Make writers pay cleanup debt or wait for queue space.** This violates
  writer independence. Unlimited GC concurrency also risks delaying writers.
- **Require durable publication of every cleanup hint.** Surviving a failure
  between mutation and publication would require a larger transaction-protocol
  change. Local hints plus scans keep that obligation off the commit path.
