# ADR-036: Decoded object cache with bounded freshness

## Status

Accepted (implemented).

The decoded object cache is realized as a single `CachedStore`
([`crates/glassdb-storage/src/cached_store.rs`](../../crates/glassdb-storage/src/cached_store.rs))
keyed by physical path, with a per-entry validation watermark, `Present` /
`Absent` / `Missing` states, `Revision`, `Observation`, and coalesced
version-conditional revalidation. The typed `ObjectCache` facade caches raw
bodies over it, and `ValueCache` was removed — key values are derived from the
cached node plus the decoded transaction object. Reads serve any usable copy
(`Requirement::Any`) and commit-time validation revalidates against current
coordination state (`Requirement::AtLeast(now)`).

[ADR-030](030-seed-shard-loads.md)'s `Latest` / `AllowStale` freshness enum is
**removed**: call sites speak the `Requirement` model directly. A former
`AllowStale` read becomes `Any`. A former `Latest` read becomes either `Any`
(when a following version-conditional CAS is the real validation) or `AtLeast(T)`
against a bound **captured once at the OCC boundary and propagated** — never
`now()` re-sampled at each read site (see "Capturing and propagating the bound"
below, which was the ambiguity that misled the first implementation). This
refines only the caching part of [ADR-023](023-slimmed-backend-trait.md): the
slim `Backend` trait, opaque content versions, and version-conditional read
remain unchanged.

## Context

The storage layer currently has two cache facades over one LRU:

- `ObjectCache` stores encoded object bodies and opaque backend versions. A
  caller chooses either `Latest`, which always revalidates a hit, or
  `AllowStale`, which accepts it without a bound.
- `ValueCache` stores materialized key values with elapsed-time staleness and an
  `outdated` flag. These values are derived from a node's effective writer and
  that writer's transaction object.

This split repeats decoding, gives different object classes different freshness
semantics, and makes higher layers maintain a second cache of derived state. The
binary `Latest` / `AllowStale` choice also loses useful knowledge. A value may
already have been validated recently enough for an operation, while `Latest`
still incurs a backend round trip. Conversely, a value that a failed CAS proved
obsolete must not be treated as merely stale and returned again.

`Latest` is not a durable guarantee: an object can change immediately after a
read. What an operation can establish is that a particular value was current at
some point after a known lower bound. This is the guarantee unlocked optimistic
validation needs. A read-only transaction must revalidate its dependencies
after validation begins, while a read-write transaction can use a successful
post-barrier CAS as both validation and mutation.

## Decision

### One decoded cache below typed object stores

Introduce one database-local cached object store between `Backend` and every
typed storage abstraction:

```text
Backend -> cached object store -> node / transaction / structural-log stores
                                -> transaction and maintenance logic
```

The typed stores supply encoding, decoding, and decoded-size accounting. They
share one byte-bounded LRU, keyed by physical object path. A path has exactly one
decoded type; attempting to use it through a different typed store is an
internal error. Cached values are immutable shared values. A caller clones a
decoded value before changing it and submits the new value through the typed
store.

The cache holds decoded physical objects, including collection roots, nodes,
transaction objects, and structural logs. It does not hold a second materialized
key-value cache. A key value is derived from its cached node and decoded
transaction object; a decoded transaction object may index its writes to make
that derivation cheap. Dependency rules remain in the higher-level resolver,
not in a generic cache dependency graph.

All object body reads and mutations go through this boundary. Listing also goes
through it but remains an uncached pass-through because a prefix has no object
version. The one-off database-metadata check/create performed while opening a
database may continue to use `Backend` directly.

### Freshness is a local validation watermark

Each cache entry is one of:

```text
Present(decoded value, revision, validated-after)
Absent(validated-after)
Missing
```

The current cache entry and evidence retained by an existing reader have
different lifetimes. A successful read returns an `Observation` of an exact
state and references its monotonic validation evidence. Validating that exact
state may advance the evidence shared by its observations. An observation may
remain useful after that state is evicted or invalidated as the current cache
entry: invalidation changes what a new read may use, but does not revoke the
historical fact that the observed state was current after its watermark.

`Revision` is the cached-store-owned opaque content-CAS token. Higher layers may
retain, compare, pass, and where recovery requires it serialize a revision, but
do not interpret or manufacture one.

`ValidationTime` is monotonic, opaque, and meaningful only within one open
`Database`. Reads accept one of two requirements:

- `Any` accepts any usable cached entry without backend validation and reads the
  backend on a miss.
- `AtLeast(T)` accepts an entry only when `validated-after >= T`; otherwise it
  validates through the backend.

These requirements apply to reads of the current state. A new read never
returns an entry already known to be obsolete, even when its old watermark
would satisfy the requested bound. Higher layers may instead validate a
previously returned observation. That validation succeeds locally when the
observation's watermark satisfies `AtLeast(T)`, regardless of whether the same
state remains the current cache entry. If its proof is too old, validation uses
the observation's revision in a conditional backend operation. An absence
observation has no revision and therefore requires an ordinary read.

This distinction does not introduce a historical-value cache. Only a caller
that retained an observation can reuse its evidence; an arbitrary later read
still requires a usable current entry. It lets OCC retain precisely the
physical observations on which a transaction depends without making known
obsolete values discoverable through `Any`.

Each backend call has a local uncertainty interval from invocation to response.
The store records `started-at` immediately before invoking the backend. A
successful read or mutation linearized at some point after `started-at`, so that
is the result's `validated-after` watermark. An operation that started before
`T` cannot satisfy `AtLeast(T)`, even if it completes after `T`.

The response time has a different role: after a successful mutation, the store
publishes the submitted value in the cache after receiving the response and
before returning to its caller. This gives local call/publication ordering, but
response time is not a freshness watermark. Another client may overwrite the
object after the backend applies this mutation but before its response arrives;
stamping the submitted value with response time would then claim freshness it
never had. Reads and writes therefore use the same operation-start watermark,
despite publishing their results only on completion.

A present entry is revalidated with the existing version-conditional read. An
unchanged response advances its watermark without transferring or decoding the
body. A changed response replaces it with the newly decoded value and revision.
An absent entry has no conditional token, so revalidating it requires an
ordinary read. Successful creates replace cached absence, and successful
deletes install freshly validated absence.

The cache proves current semantic state, not an observable write history.
Canonical objects with identical contents are equivalent to an object that was
not rewritten; no nonce is introduced. Higher-level logical validation tokens
remain responsible for meaningful changes: point reads compare unique writer
transaction IDs, and scans compare membership versions plus pending-transaction
dependencies.

Object-specific invariants may be stronger than generic freshness. In
particular, a typed transaction-object store may serve a cached committed or
aborted object indefinitely because terminal transaction objects are immutable.
Pending objects still honor the caller's bound. The generic cache does not know
transaction states or other dependency semantics.

### OCC propagates one lower bound through its dependencies

Transaction execution may use `Any`: a stale execution is safe because the body
is retryable and commit validates what it observed.

Unlocked validation captures a `ValidationTime` after the transaction body and
requires `AtLeast(validation_start)` for every retained physical observation.
The same bound is propagated when node interpretation requires a pending
transaction's status. If another operation has already advanced that
observation's evidence after the bound, validation succeeds without another
backend call. If validation reports a change, the higher-level resolver applies
its dependency invariants and may request the current state; the generic cache
does not interpret those dependencies.

For a read-write transaction, the barrier is captured before lock acquisition
and other validating CAS operations. A successful CAS started after the barrier
installs a cache entry that satisfies the bound, so it can validate and mutate
without a separate read. This preserves the single-read/write optimization: its
leaf install CAS validates the observed writer while installing the lock. A CAS
performed by an earlier transaction cannot certify freshness for a later
transaction.

Consequently, an otherwise idle read-only transaction whose key and finalized
transaction object are cached still performs one conditional read per distinct
terminal leaf during validation. The database cannot infer that other clients
did not write after the cached leaf was last validated. Removing that floor
would require a stronger primitive such as a freshness lease, exclusive-client
mode, or change stream.

### Capturing and propagating the bound (implementation guidance)

The bound `T` in `AtLeast(T)` is one `ValidationTime` **captured once** per
transaction phase and **threaded** through every dependent read of that phase. It
is not re-sampled at the point of each read.

Introducing the concept of `now` (e.g. `AtLeast(now())`), re-sampling the clock
at a read site, is an anti-pattern that silently defeats the model. A `now`
bound is unsatisfiable by any existing cache entry and therefore always forces a
backend validation. That is exactly the unconditional "always revalidate"
behavior (the removed `Latest`) that this design replaces, wearing a bound as a
disguise: it carries no evidence, enables no reuse, and makes every call site
independently pessimistic. The whole value of a bound is that it is older than
the reads and locks a transaction already performed, so their evidence satisfies
it for free.

Two consequences for the API surface:

- The typed stores expose **no per-call "read latest" accessor**. A clock read
  appears only where a barrier is deliberately _captured_, at the OCC boundary.
  Every legitimate caller either accepts `Any` or already holds a captured bound
  to pass down.
- A read issued only to **seed a following version-conditional CAS uses `Any`**,
  not a fresh bound. The CAS is the validation: a stale seed makes it miss, and
  the retry reloads. Paying for a strong read first to avoid an occasional CAS
  miss is not worthwhile, and manufacturing a `now()` bound to force that read
  is the same anti-pattern.

### Locked dependencies validate at the lock's landing, not the leaf's version

A held lock pins a key's logical value independently of its physical leaf. Other
keys in the same leaf may be locked or written, advancing the leaf's backend
version, yet the locked key cannot change. Validation exploits this: the
read-write barrier is captured **before** lock acquisition, so each lock's
install CAS lands after the barrier and its write-through advances that leaf's
evidence past the bound. The locked key then validates against
`AtLeast(barrier)` from that evidence with **no additional read**, even when the
leaf's version has since moved for unrelated keys. This generalizes the
single-read/write optimization to every locked key across every leaf a
transaction touches, and is the primary source of the "several strong reads
saved" per commit.

A revision-conditional check ("is the leaf's version still the one I read?")
must **not** be substituted for the `ValidationTime` bound here. It conflates
the physical object version with the logical freshness of a locked key, so
unrelated churn in the same leaf reports a change and forces a wasteful re-read
and re-derivation of a value that provably did not change. The bound exists
precisely to separate "this object was rewritten" from "the value I depend on
was current as of `T`". That separation is what makes three distinct reuses
possible, all of which a revision-only "revalidate" flag would lose:

- **Batching** — many keys under one leaf, and many leaves under one range scan,
  are satisfied by evidence advanced once against the shared bound.
- **Cross-transaction** — another client's write-through that lands after the
  bound advances the same shared evidence, so this transaction's validation of
  that leaf becomes free while remaining correct (the writer/membership
  comparison still runs against the newer, consistent state).
- **Locked keys** — as above, validated from the lock's landing evidence without
  touching the backend.

### Mutation outcomes update knowledge conservatively

Every successful write or CAS installs the submitted decoded value with its new
revision and `started-at` watermark after the backend response and before the
store call returns. A successful CAS also proves that its exact expected
observation remained current after `started-at`, immediately before replacing
it, and may advance that observation's watermark. A successful delete installs
absence under the same rule.

A CAS or create conflict proves that the operation's starting revision or
cached absence is obsolete. The cache invalidates that exact starting entry only
if it is still current locally; it must not discard a different value or a later
validation installed concurrently. Conflict does not automatically fetch the
winner. A caller that needs it follows with an explicit `Any` or `AtLeast`
read. The conflict does not revoke or advance validation evidence previously
issued for the starting observation.

An in-doubt mutation likewise invalidates its exact starting knowledge but
installs neither the old nor the proposed state, because either may be wrong.
Protocol-specific recovery performs an explicit bounded read or its existing
read-back procedure. Existing observations retain their established
watermarks, but the uncertain outcome cannot advance them.

A positively known-obsolete value is `Missing`, not a form of stale data. `Any`
never returns it. An `Arc` already held by a caller remains inspectable, but a
new cache lookup cannot rediscover it. Its retained observation may still
satisfy an older validation bound: for example, evidence at `T2` remains valid
for `AtLeast(T1)` after an in-doubt write at `T3`, where `T1 < T2 < T3`.

Cache publication is conditional so delayed operations cannot replace newer
knowledge. Validation watermarks never regress. Concurrent validations of one
path may be coalesced only when the in-flight operation's start time satisfies
the waiter's requested bound.

### Correctness obligations and verification

The cache is a concurrency protocol, not only a performance optimization. Its
implementation is incomplete without deterministic tests of these obligations.
Tests use an injected monotonic clock and a controllable backend rather than
elapsed real time.

A small reference state machine covers `Present`, `Absent`, and `Missing`
entries; opaque revisions; validation watermarks; and read, create, CAS, delete,
conflict, and in-doubt outcomes. Model-based tests vary invocation, backend
linearization, and response order independently and assert that:

- `ValidationTime` and installed watermarks never regress;
- a result is stamped with its operation start even when the clock advances
  before completion, and a mutation is not published before backend success;
- `AtLeast(T)` never uses an operation started before `T`;
- `Any` never returns an entry already invalidated by conflict or an in-doubt
  mutation;
- an observation's established watermark remains usable after its current
  entry is invalidated, but invalidation does not advance that watermark; and
- a cache hit's decoded value, revision, and watermark always come from one
  permitted entry transition.

Deterministic gated race tests cover the cases that a sequential model alone
cannot expose:

- a delayed old read completing after a newer read or write cannot overwrite
  the newer entry;
- conflict or in-doubt invalidation removes only the exact starting knowledge
  and preserves a concurrently installed value or later validation;
- an invalidated observation can satisfy an older bound from its retained
  evidence, while a new read cannot rediscover it and a stricter bound requires
  backend validation;
- successful CAS advances both its expected observation's evidence and the
  installed state from its start time, while conflict and in-doubt outcomes do
  neither;
- waiters share an in-flight validation only when its start satisfies their
  bound, while a stricter waiter causes a later validation;
- unchanged conditional reads advance, but never regress, the watermark; and
- cached absence races safely with create, delete, and delayed reads.

Transaction-level deterministic simulation and multi-client integration tests
remain the end-to-end safety net. They assert serializability under reordered
responses, CAS conflicts, lost acknowledgements, and outages, and also pin the
intended operation shape: an idle cached read-only transaction performs one
conditional validation per terminal leaf, while a successful post-barrier CAS
requires no extra validation read for the object it covers.

## Consequences

- All physical object classes use the same freshness and mutation semantics,
  and no database component below the documented initialization exception can
  bypass them.
- Objects are decoded once per changed revision rather than once per cache hit.
  The separate `ValueCache`, its age policy, and its derived-entry invalidation
  paths disappear.
- `AtLeast` expresses the actual read guarantee and lets OCC, concurrent
  validation, and post-barrier CASes reuse sufficiently recent knowledge.
- CAS conflicts and in-doubt writes improve cache knowledge without forcing an
  automatic read, while exact conditional invalidation prevents response races
  from regressing the cache.
- Invalidation may remove a state from the current cache without wasting the
  validation evidence held by transactions that already observed it. This
  recovers the older-bound reuse opportunity without exposing obsolete values
  to new reads or retaining general history.
- Negative caching avoids repeated reads for missing objects, but refreshing
  absence costs a full read.
- Key reads perform cheap local derivation from decoded nodes and transaction
  objects. If that later proves expensive, a derived cache may be added above
  this layer with explicit dependency tokens.
- Freshness tokens cannot be persisted or exchanged between databases or
  processes. Revisions remain the portable opaque CAS identity.
- A decoded-size estimate governs eviction, and immutable values held by active
  callers may outlive their LRU entries, so live process memory can temporarily
  exceed the cache budget.
- Maximum/as-of bounds are not supported. They would require retained historical
  versions or MVCC semantics that the current object store does not provide.
