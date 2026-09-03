# CachedStore, requirements, and observations

GlassDB's cache is not just a map from object paths to values. `CachedStore` is
the boundary that combines decoded-object reuse, currentness evidence, and
same-path coordination for backend point operations. The transaction layer can
therefore execute optimistically from cached data and later prove that the
exact states it used were sufficiently current.

This guide introduces that model and explains why its evidence is sound when
the backend provides linearizable single-object reads and conditional
mutations. The complete decisions are recorded in
[ADR-036](../adr/036-decoded-object-cache-with-bounded-freshness.md),
[ADR-043](../adr/043-causally-coordinated-backend-operations.md), and
[ADR-045](../adr/045-optional-persistent-encoded-body-l2-cache.md).

## Where CachedStore sits

One `CachedStore` belongs to each open `Database`. All typed physical-object
stores use it:

```text
transaction code
    -> reader / resolver / monitor
    -> typed object stores and codecs
    -> CachedStore
         -> decoded, byte-bounded L1
         -> optional persistent encoded-body L2
         -> per-path operation coordinator
    -> Backend
```

The L1 is a byte-weighted LRU keyed by physical object path. Codecs supplied by
the typed stores encode and decode values, validate paths, and report decoded
sizes. A path has exactly one decoded type; using the same path through another
codec is an internal error. Cached values are immutable and shared, so a caller
clones a value before modifying and submitting it.

The cache holds physical objects such as collection records, tree nodes,
transaction objects, and structural intents. It does not maintain a separate
materialized key-value cache. Higher layers derive a logical key value from its
cached leaf and, when necessary, its writer's cached transaction object.

The optional L2 stores exact encoded present bodies, opaque revisions, and
their existing currentness points. It is best-effort: an unavailable, corrupt,
or overloaded L2 is a performance failure, not a new database failure mode. L1
remains the owner of decoded values and live shared evidence.

## Cached knowledge

For a physical path, the discoverable cache state is one of:

| State | Meaning |
| --- | --- |
| `Present(value, revision, evidence)` | A decoded value, its opaque backend CAS revision, and evidence about when that state was current. |
| `Absent(evidence)` | Definitive evidence that the object did not exist. |
| No entry | No usable knowledge; the path is uncached or uncertain. |

Absence is a real negative cache entry. Uncertainty is deliberately not an
entry variant: there is nothing an ordinary lookup can accidentally return. A
conflict, an indeterminate mutation, or an undecodable changed object can
remove discoverable knowledge without inventing a replacement.

A `Revision` wraps the backend's opaque content-CAS token. Higher layers can
retain and compare it or pass it back to a conditional operation, but cannot
interpret or manufacture it. Revisions identify semantic content state rather
than an observable history of rewrites; equivalent contents may therefore
reuse a token.

## Sequence points and currentness

Every open database owns a strictly ordered local `Timeline`. Immediately
before dispatching a coordinated backend operation, `CachedStore` allocates a
`SequencePoint` from that timeline.

If an operation was invoked at `T`, a definitive result stamped
`current_after = T` means:

> The returned state was current at some backend linearization point no earlier
> than `T`.

This is a lower-bound proof, not a lease. It does not promise that the state is
still current when the call returns, and `T` is neither a wall-clock timestamp
nor an exact database snapshot. Another client may change the object after the
operation linearizes but before its response arrives, which is why response
time would be an unsound, overly strong watermark.

Sequence points are normally meaningful only within one open database. They
are not exchanged between clients or independent database openings. The
persistent cache is the narrow exception: it persists points only so a new
opening of the same database identity can start its timeline strictly after all
recoverable cache evidence. Consequently, an old L2 body may satisfy `Any`, but
a requirement created in the new session forces validation before that body
can satisfy it.

## Requirements

A read states the minimum evidence it needs with `Requirement`:

| Requirement | Accepted cache state |
| --- | --- |
| `Any` | Any discoverable `Present` or `Absent` state, regardless of its watermark. |
| `AtLeast(T)` | A discoverable state whose `current_after` watermark is at least `T`. |

`Any` deliberately permits stale data. It is useful for optimistic transaction
execution and idempotent CAS loops, where a stale starting point can only fail
validation or lose its precondition. A known-obsolete or uncertain state is no
longer discoverable, so even `Any` cannot return it.

`AtLeast(T)` first tries the cache. If the entry's evidence is too old,
`CachedStore` checks the backend:

- For `Present`, `read_if_modified` uses the retained revision. An unchanged
  response reuses the decoded body and advances its evidence. A changed
  response transfers and decodes the new body. `NotFound` installs confirmed
  absence.
- For `Absent`, there is no conditional revision, so validation requires an
  ordinary read.

Concurrent reads may share one in-flight backend check only when its invocation
point satisfies every waiter's requirement. A stricter waiter queues and
rechecks the cache after the earlier operation finishes.

`Requirement::within` derives a cutoff from an elapsed duration for
`read_stale`. That duration-to-sequence conversion is intentionally an
approximate cache policy. Transaction validation, mutation receipts, and
recovery use exact sequence barriers without doing time arithmetic.

## Observations

Every successful read or mutation returns an `Observation<V>` of one exact
state. It contains:

- the physical path;
- a shared decoded value, or absence;
- an opaque revision for a present value; and
- shared, monotonically advanceable currentness evidence.

An observation and the matching cache entry normally hold the same evidence
cell. If a later conditional read proves that revision unchanged, advancing the
cell benefits every holder. Evidence advances by taking the maximum point and
never regresses.

The observation's lifetime is separate from the discoverable cache entry. An
observation retained by a transaction remains inspectable after LRU eviction or
invalidation. Removing the entry changes what a new read may discover; it does
not erase the historical fact that the retained state was current after its
existing watermark.

`check_current(observation, T)` uses that distinction:

1. If the observation already has evidence at least `T`, it is current under
   the requested bound without I/O.
2. If a discoverable entry with sufficient evidence has the same revision, its
   evidence advances the retained observation.
3. Otherwise, the observation's revision seeds a conditional backend read. An
   absence observation instead requires an ordinary read.
4. The result is `Current`, with merged evidence, or `Changed` with an
   observation of the newly established state.

Successful CAS also uses observations as receipts. Its precondition proves that
the expected observation remained current until the CAS linearized, so the CAS
advances that observation to its invocation point and returns a new observation
of the installed state.

## Per-path coordination

`CachedStore` serializes actual backend point calls for the same physical path
within one open database:

```text
check cache
-> acquire path lane
-> check cache again
-> allocate invocation point
-> invoke backend
-> reconcile cache and observations
-> release lane
-> make the operation future ready
```

The second cache check avoids a redundant call when the preceding lane owner
already established sufficient evidence. Reconciliation happens before lane
release and before the completed operation becomes observable to its caller.
Different paths retain full backend concurrency.

An `Any` cache hit bypasses the lane and may return an older usable state while
a mutation is in progress. This is part of `Any`'s contract. Code that needs a
causal lower bound uses `AtLeast` or retains and validates an observation.

Mutation outcomes are reconciled conservatively while holding the lane:

- Success publishes the exact installed state before returning.
- A clean precondition conflict invalidates only matching expected knowledge;
  it cannot erase a different state already known locally.
- `Unavailable` after dispatch makes the whole path uncertain because the
  mutation may or may not have landed.
- Cancellation, panic, or task failure after mutation dispatch follows the
  same uncertain transition before releasing the lane.
- Cancellation before dispatch has no cache effect.

Read cancellation needs no invalidation because a read cannot mutate backend
state. A cancelled mutation may still apply remotely, but it can never publish
a delayed local result and is subsequently treated like a write from another
database instance.

## Why the evidence is correct

The required backend contract is linearizable single-object reads and
conditional mutations, including read-after-definitive-completion. A definitive
response creates an ordering edge; `Unavailable` does not. Eventually
consistent backends are not supported.

The correctness argument has four parts.

### 1. Invocation points are sound lower bounds

A linearizable operation has one effective point between invocation and
response. Because `T` is allocated immediately before invocation, the state
returned by a definitive read was current at some point no earlier than `T`.
An unchanged conditional read proves the expected revision current at such a
point. A successful conditional mutation proves its predicate current at that
point and installs its returned state there. Stamping each result with `T` is
therefore conservative.

An eventually consistent read could return an old revision or false absence
after `T`, so the same inference would be invalid without backend
linearizability.

### 2. The path lane aligns local and backend order

Consider a read that observes absence before a concurrent create linearizes but
whose response arrives after the create succeeds. Publishing responses in
arrival order would incorrectly let the delayed absence overwrite the created
value.

The path lane removes that schedule among local definitive operations. The read
and create cannot be actual overlapping backend calls for that path. Either the
read runs first and the create publishes last, or the create completes and is
reconciled before the read is invoked; linearizability then makes the read see
the created state or something later. Publication order follows the backend's
real-time edge rather than invocation or response order alone.

### 3. Reconciliation never guesses

Same-state validation merges evidence with a maximum, while a different state
replaces discoverable knowledge. A conflict removes only the exact state it
proved obsolete. An indeterminate or cancelled mutation removes usable
knowledge instead of choosing between the old and proposed states. Thus the
cache either exposes a state supported by a definitive operation or exposes no
state at all.

External clients and independently opened databases do not share the path lane
or timeline. They remain safe because freshness checks and mutations use the
backend's linearizable conditional revisions. Local coordination is needed to
order local publication; the backend remains the global authority.

### 4. Transactions validate speculative cache use

`Any` is not itself a strong read. Transaction execution may use it because the
body is retryable and retains the physical observations on which it depended.
After the body, validation captures one `validation_start` point and checks
those dependencies against `AtLeast(validation_start)`. If a state changed, the
higher-level resolver compares its logical writer or membership evidence and
the transaction retries when its result was invalidated.

Point validation batches this work by physical leaf path. Optimistic validation
checks exact retained leaf observations first and expands each result back to
input order. If one changed, `KeyResolver` routes and resolves the complete
logical point-read set with current terminal leaves. Validation after point
locks are acquired always uses this logical path and ignores the validating
transaction's own holder. Each provider applies the engine's transaction-local
leaf parallelism bound; one shared lower bound applies to all work in the
validation episode.

A successful CAS invoked after the validation barrier can both validate its
expected observation and install the mutation, so it needs no separate read.
For read-only transactions, a concurrent write after validation can be ordered
after the transaction; a write that invalidates the observed result is detected
during validation. This is how the public strongly consistent read path can
execute cheaply from cache without treating an arbitrary cache hit as current.

## Boundaries of the guarantee

- `Any` may return stale but still usable knowledge.
- `AtLeast(T)` means the exact state was current sometime after `T`; it is not a
  promise that the state remains current at return.
- Sequence points are local causal evidence, not portable timestamps.
- The generic cache does not infer object-specific facts. For example, the
  transaction-object store may cache finalized transactions indefinitely only
  because that type separately guarantees immutability.
- Listing is an uncached pass-through. Each page is strongly observed as one
  backend request, but a multi-page listing is not a snapshot.
- The persistent L2 preserves old bodies and evidence but introduces no
  coordination authority or freshness guarantee of its own.

These boundaries are what make the cache useful without making it unsound: it
records exactly what a strongly consistent backend has established, lets that
evidence be reused, and forces validation whenever a caller asks for more than
the retained evidence proves.
