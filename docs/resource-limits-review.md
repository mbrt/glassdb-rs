# Resource limits review

Review date: 2026-09-18.

GlassDB does not yet bound all operations with configurable limits. It has limits
for individual nodes and some background queues, but total memory, active work,
and operation duration remain unbounded in several paths.

This review covers the public interface, transaction engine, storage, caches,
backend adapters, and support middleware. The findings and defaults below record
the initial static review. Implementation progress is tracked separately here.

## Implementation progress

Item 1 is being split into small fixes. It remains incomplete.

- Added `DatabaseBuilder::transaction_limits` with 4 KiB logical-key and 1 MiB
  written-value defaults. Input checks precede copies and point I/O; scan bounds
  and stale-read keys use the same key limit. Existing values remain readable.
- Added a shared operation count per transaction-body execution, with a default
  of 4,096. Point accesses, scans, and collection operations count toward the same
  limit, including repeated calls. Admission occurs before copies or I/O.
- Added a cumulative write-input budget per transaction-body execution, with a
  default of 64 MiB. Keys and values count before copying; replacements and
  deletes consume budget without refunds. Read and scan observations are separate.
- Added a limit of 1,024 new collection-ID reservations per transaction identity.
  Existing reservations can be reused at capacity. Body retries and staged drops
  do not release reservations; recovery retains ownership of prepared resources.
- Full observation-memory and encoded-object budgets will be addressed with the
  related storage and scan work in items 2 and 7.

Each completed part has a separate commit, deterministic regression tests, an
adversarial review, and a passing `make test` run.

Item 2 is postponed at the user's request. Backend implementations and LIST
handling remain unchanged. Response limits need a later design that avoids
assigning this responsibility separately to each backend implementation.

Item 3 is being split into small fixes. It remains incomplete.

- Added `DatabaseBuilder::max_active_operations`, with a default of 256 concurrent
  transaction calls and stale reads per database instance. Clones share capacity;
  separate opens have independent limits. Transaction calls hold capacity across
  body retries and commit. Admission rejects excess work immediately with
  `LimitExceeded`, without creating a waiting queue.
- Background work, coordinator queues and batches, and aggregate backend work
  still need separate bounds.
- A retained `Transaction` handle can start work after its enclosing call ends.
  Such work, and parallel work inside a transaction body, need separate bounds.

Coordinator queue limits in item 3 and managed recovery budgets in item 4 are
postponed at the user's request. Lock acquisition, lock release, and write-back
share the coordinator. A later capacity policy must preserve recovery of already
admitted work, including retirement handoff and foreign transaction identities.
Independent small fixes continue.

Item 5 is being split into small fixes. It remains incomplete.

- Enforced the existing ambiguous-commit recovery deadline during status reads
  and retry delays. The configured pending timeout (15 seconds by default) is
  measured from the commit write attempt. An expired budget starts no new status
  read, and expiration returns `InDoubt` while preserving the uncertain outcome.
- Total transaction and shutdown deadlines, backend request timeouts, and other
  retry budgets remain unresolved.

## Prioritized fixes

The fixes below are in priority order:

- **P1:** Memory growth, stalled operations, or limits that are not fully enforced.
- **P2:** Maintenance and configuration.
- **P3:** Support tools.

### 1. P1 — Bound transaction size before accepting accesses

There is no maximum value size, transaction byte size, access count, scan count,
or collection-change count. Writes copy values into memory without a size check.
Collection reservations can also grow across transaction-body retries. The 1 KiB
inline limit only selects the commit protocol; larger values enter transaction
objects.

Add limits for logical-key bytes, value bytes, retained transaction bytes,
accesses, collection changes, and encoded transaction-object size. Check them
before allocation or protocol work. Include state retained across retries.

Sources: [transaction interface](../crates/glassdb/src/tx.rs#L138),
[collection changes](../crates/glassdb-trans/src/collection_commit.rs#L26).

### 2. P1 — Bound backend response bodies and decoding

S3 collects the entire response body; GCS does the same. Storage then decodes it
without an object-size check. Thus, write-side node limits do not protect readers
from oversized stored objects.

Add configurable response-byte limits, enforced during download, and object-type
limits before decoding. Include metadata, transaction objects, structural intents,
LIST responses, and decoded collection counts.

Sources: [S3 reads](../crates/glassdb-backend-s3/src/lib.rs#L283),
[GCS reads](../crates/glassdb-backend-gcs/src/lib.rs#L211),
[storage decoding](../crates/glassdb-storage/src/cached_store.rs#L536).

### 3. P1 — Bound active operations and waiting queues

Operation admission only increments a counter. The limit of 16 leaf operations
applies within transaction phases; it does not bound concurrent transactions or
parallel reads from transaction bodies. Coordinator queues and merged batches
have no capacity limit.

Add limits for active operations, queued operations, queue bytes, and
coordinator-round members. Bound waiting admission as well as running work. Put
aggregate backend request limits in the adapters, consistent with
[ADR-064](adr/064-bounded-parallel-point-leaf-work.md).

Sources: [database admission](../crates/glassdb/src/db.rs#L412),
[coordinator queues](../crates/glassdb-concurr/src/dedup.rs#L104).

### 4. P1 — Bound managed retirement and write-back work

Each retirement or committed write-back can spawn another task. The background
registry has no task or byte budget. Slow storage can therefore cause memory
growth even after foreground transactions return.

Reserve recovery capacity before admitting work that can require retirement. Use
bounded workers and bounded retained state. Capacity exhaustion must preserve a
recovery owner for admitted protocol resources.

Sources: [background registry](../crates/glassdb-concurr/src/background.rs#L121),
[retirement](../crates/glassdb-trans/src/algo.rs#L87),
[write-back](../crates/glassdb-trans/src/algo.rs#L895).

### 5. P1 — Add operation deadlines and enforce them during I/O

Transaction-body retries, serial lock acquisition, several recovery loops, and
shutdown have no total deadline. GCS constructs a client without request, read,
or connect timeouts.

There is also an enforcement gap: ambiguous commit recovery computes a deadline,
but checks it only after a read returns `Unavailable`. A stalled read can exceed
it indefinitely.

Add request deadlines, transaction deadlines, retry budgets, and a bounded
shutdown wait. Preserve `InDoubt` when a dispatched mutation may have applied.

Sources: [commit recovery](../crates/glassdb-trans/src/monitor.rs#L1224),
[transaction retries](../crates/glassdb/src/db.rs#L491),
[shutdown](../crates/glassdb/src/db.rs#L184).

### 6. P1 — Make the decoded-cache byte limit effective

Each cache partition retains its last entry even when that entry exceeds its
budget. Since transaction objects have no size cap, the overshoot has no byte
bound. `cache_size(0)` still retains entries. Accounting also omits cache-key and
container allocations; node and collection-record weights use encoded size
instead of decoded memory.

Add maximum-entry admission and conservative decoded-memory accounting. Charge
retained observations outside the cache to transaction or active-work budgets.

Sources: [cache eviction](../crates/glassdb-storage/src/cache.rs#L72),
[cache accounting](../crates/glassdb-storage/src/cached_store/knowledge.rs#L34),
[node weight](../crates/glassdb-storage/src/node_store.rs#L149).

### 7. P1 — Bound scans and remove full-list materialization from bulk operations

Key scans default to no limit. Even a limited result can visit many empty leaves
and retain all their validation evidence. Collection drop collects every
standalone node before fencing it. Participant recovery also collects all
matching structural intents.

Add limits for result bytes, visited leaves, retained evidence, and total scan
work. Process bulk listings page by page. Full-result interfaces must report
limit exhaustion rather than silently omit results.

Sources: [scan options](../crates/glassdb/src/scan.rs#L25),
[scan evidence](../crates/glassdb-trans/src/key_resolver.rs#L143),
[collection drop](../crates/glassdb-trans/src/collections/lifecycle.rs#L104).

### 8. P1 — Bound monitor state and remove abandoned observations

The final-status cache is bounded, but the runtime transaction map and waiter
vectors are not. A foreign pending observation can remain after its last waiter
leaves: poll completion removes the entry only when final status was observed.
Local waiter vectors also retain cancelled waiters until notification.

Add limits for foreign observations and waiters, remove cancelled registrations,
and safely evict unused foreign liveness observations. Keep owned recovery state
under reserved admission capacity.

Sources: [poll completion](../crates/glassdb-trans/src/monitor.rs#L1771),
[waiter registration](../crates/glassdb-trans/src/monitor.rs#L1902).

### 9. P1 — Enforce coordination-object caps on every growth path

Collection-directory validation checks size, but topology-participant admission
writes the collection record without that check. Collection locking and drop
fencing also write through stores that do not enforce the configured hard cap.

Centralize growth admission for coordination objects. Bound participants and
holders, reserve cleanup space, and permit shrinking cleanup of existing
oversized objects.

Sources: [topology participants](../crates/glassdb-trans/src/split.rs#L2020),
[collection coordination](../crates/glassdb-trans/src/collection_coordination.rs#L225),
[collection writes](../crates/glassdb-storage/src/collection_store.rs#L330).

### 10. P2 — Bound each maintenance job, not only its queue

GC limits concurrent checks, but waits for the complete batch before accepting
another batch. One stalled check can stop candidate processing. The splitter
processes its drained queue sequentially, so one stalled candidate can stop the
sweep.

Add per-job deadlines and budgets for objects, bytes, and backend requests.
Retain bounded continuation state and process completed jobs independently.
Separate maintenance scheduling from protocol-liveness timing.

Sources: [GC batches](../crates/glassdb-trans/src/gc.rs#L345),
[split sweeps](../crates/glassdb-trans/src/split.rs#L1380).

### 11. P2 — Bound complete tree traversals

The 4,096-hop bound applies to right-link correction. Descent depth is unbounded,
and batched routing resets its right-hop counter on descent. Ordered leaf
traversal has no cycle or total-work bound.

Add configurable depth, total-node, and reroute budgets, plus cycle or
forward-progress checks. This bounds work under topology churn and prevents
corrupt links from causing infinite traversal.

Sources: [tree descent](../crates/glassdb-storage/src/tree_router.rs#L269),
[ordered traversal](../crates/glassdb-storage/src/tree_router.rs#L574).

### 12. P2 — Expose existing limits and validate their relationships

Most limits in the table below are fixed constants. Even transaction leaf
parallelism is absent from `DatabaseBuilder`.

Configuration validation also needs fixes: the advertised 5-second retry maximum
permits delays up to 7.5 seconds after jitter; a pending timeout above 600 seconds
makes structural recovery's cadence bounds invalid; and an alphanumeric database
name above 255 bytes passes the builder check but can panic during root-address
construction.

Expose operational budgets through validated configuration, clamp actual delays,
use checked duration arithmetic, and validate names before I/O.

Sources: [engine configuration](../crates/glassdb-trans/src/engine.rs#L47),
[retry jitter](../crates/glassdb-concurr/src/retry.rs#L56),
[recovery cadence](../crates/glassdb-trans/src/split.rs#L1357),
[root-address construction](../crates/glassdb-data/src/paths.rs#L251).

### 13. P3 — Bound support middleware and the memory backend

Recording middleware stores all operations and payloads indefinitely. Delay
middleware retains all object/prefix rate-limit entries. Memory-backend LIST
allocates and sorts all matching paths before applying the page limit.

Add recording count/byte limits, safe expiry of idle rate-limit state,
memory-backend quotas, and listing with bounded temporary memory.

Sources: [recording middleware](../crates/glassdb-backend/src/middleware/recording.rs#L44),
[delay middleware](../crates/glassdb-backend/src/middleware/delay.rs#L336),
[memory-backend LIST](../crates/glassdb-backend/src/memory.rs#L151).

## Current limits and defaults

| Area | Current default or bound | Configuration and assessment |
| --- | --- | --- |
| Decoded cache | **512 MiB per database instance** | Public setting; approximate accounting and oversized-entry exception |
| Node sizing | **256** leaf entries; **256** index children; **256 KiB** soft size; **1 MiB** hard size; **64 KiB** reserved headroom | Public `SplitPolicy`; entry counts are split triggers, not hard caps |
| Logical-key size | Must fit the split policy's entry and separator budgets; entry budget is half of **960 KiB** under defaults | Indirect bound checked by commit; no separate early input limit |
| Inline values | **1 KiB/value**, **16 KiB/leaf** | Public `InlinePolicy`; zero disables inlining; external values remain unbounded |
| Collection directory | **960 KiB** content and **1 MiB** total during directory validation | Inherits split policy; incomplete enforcement on other mutations |
| Names and paths | Collection names **1–255 bytes**; database root maximum **255 bytes**; collection-path depth unbounded | Fixed name ceilings; database-name validation gap |
| Transaction leaf parallelism | **16** incomplete operations per bounded phase | Engine setting only; no public builder setter |
| Same-path backend operations | **1** active operation | Ordering invariant; waiter count is unbounded |
| Coordination backoff | **200 ms** initial; **5 s** base maximum; multiplier **1.5**; jitter **±50%** | Initial/base maximum public; actual delay can reach **7.5 s** |
| Point-read retries | **5 retries**, **6 attempts total** | Fixed; no elapsed-time bound |
| Leaf coordinator | **50 attempts** per round | Fixed; outer retries can continue |
| Lock release | **8 contention rounds** | Fixed; live-holder waits are separate |
| Serial fallback | After **3 conflict passes** or **5 s** parallel lock wait | Fixed; serial acquisition has no total deadline |
| Leaf-capacity wait | **30 s** per continuous capacity episode | Fixed; checked between attempts |
| Protocol timing | **15 s** pending timeout; **30 s** clock-skew allowance; refresh every **7.5 s** | Public timing; ambiguous-commit recovery uses the pending timeout |
| Final-status cache | **16,384 entries** | Fixed; separate from unbounded runtime state |
| GC queues | **4,096** incoming hints; **4,096** retained hint candidates; **2,000** scan candidates; **1,000** IDs per LIST page | Fixed; bounded admission and observable hint loss |
| GC execution | **1–8** concurrent checks; admission batches of **64** | Fixed |
| GC timing | Initial scan delay **15 s**; cadence **937.5 ms–600 s**; continuation interval capped at **15 s**; LIST timeout **60 s**; error backoff up to **240 s** | Derived from pending timeout rather than independently configurable |
| GC traversal control | **8** current-generation traversals; retained traversals can span up to **4,161** physical prefixes; **4** samples; narrow after **64** pages, broaden estimate **32** pages | Fixed |
| Split maintenance | **4,096** split candidates and **4,096** deferred separators; sweep interval **1 s**; recovery cadence normally **15–600 s** | Fixed count limits; no queue-byte limit |
| Structural work | **8** parent attempts; **4,096** reconciliation hops | Fixed |
| Routing | **4,096** self-correcting right-link hops | Fixed; no complete-descent bound |
| Storage listing | Node/structural pages **128**; S3/GCS provider page ceiling **1,000** | Storage page sizes fixed; backend callers supply a positive item limit |
| Persistent cache | **Disabled**; explicit capacity when enabled, minimum **131 MiB** | File capacity public |
| Persistent-cache memory/work | **4,096** queue items; **3,072** optional items; **64 MiB** queued payload; **4,096** active fences; **4 MiB** admission filter | Fixed; overload bypasses or disables the cache |
| Persistent-cache timing | Open, lookup, shutdown: **5 s** each; sync interval **5 s**, byte trigger **64 MiB** | Fixed |
| S3 retries | Idempotent operations: **10 attempts**. Conditional PUT: up to **6 attempts** for repeated conflicts or **11** for repeated ambiguous/throttled failures | Idempotent retry policy configurable; conditional policy fixed. Backoff **25 ms–1 s** |
| GCS read races | **3** metadata/body-generation attempts | Fixed; no configured HTTP deadline |
| Public scans | **No result limit by default** | Per-call count optional; no enforced instance maximum or byte budget |
| Transaction duration, total size, active-operation count, shutdown duration | **No finite default** | Missing |

The numeric defaults are spread across
[engine configuration](../crates/glassdb-trans/src/engine.rs#L35),
[GC](../crates/glassdb-trans/src/gc.rs#L45),
[GC traversal](../crates/glassdb-trans/src/gc/scan.rs#L12),
[split maintenance](../crates/glassdb-trans/src/split.rs#L83), and
[persistent-cache admission](../crates/glassdb-storage/src/disk_cache/admission.rs#L7).

## Configuration requirements

For new limits, choose finite defaults against a **combined instance memory
budget**. Item counts alone are insufficient when each item can retain a large
value or observation. Keep format ceilings and protocol ordering fixed; make
resource budgets configurable within those constraints. Each limit also needs a
defined exhaustion result and a counter.

These limits can bound GlassDB-owned work. Caller allocations and a shared
database's total stored size require separate controls. GC capacity limits alone
cannot guarantee a storage quota or a maximum reclamation delay.
