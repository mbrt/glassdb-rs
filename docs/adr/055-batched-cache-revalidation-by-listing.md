# ADR-055: Batched cache revalidation by listing

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md).

Extends [ADR-035](035-paginated-listing-and-sharded-transaction-logs.md)'s
`ListPage` with one field per reported object. Its pagination contract, opaque
cursor, unspecified result order, and explicitly non-snapshot traversal are
unchanged. [ADR-023](023-slimmed-backend-trait.md)'s opaque `Version` and
[ADR-036](036-decoded-object-cache-with-bounded-freshness.md)'s watermark rule
are used as they stand.

## Context

ADR-036 already revalidates a cached object without re-reading its body: a
version-conditional read that reports no change advances the entry's watermark
while transferring and decoding nothing. That costs one request per object,
which is the right shape for a point read and the wrong shape for a scan.

Snapshot reads turn this from an efficiency question into whether the design
meets its goal at all. A cut fixes a read timestamp, and a cached leaf may serve
it only when the leaf's watermark is at or after that timestamp plus the margin.
An analytical scan that ran an hour ago holds a warm cache whose watermarks all
precede the cut it binds today, so every leaf is re-read even when nothing in
the collection changed. Revalidation cost tracks the size of the data instead of
the size of the change, and a warm cache buys nothing.

Both providers already return per-object metadata in a listing, and the engine
already lists prefixes for GC and structural recovery. What a listing does not
carry is the one token that would let that metadata settle the question.

## Decision

`ListPage` reports each object's opaque `Version` alongside its path.

A listed revision equal to a retained observation's revision is exactly the
evidence a version-conditional read reports when nothing changed: the object's
current content is the content already held. The observation's watermark
advances to the list call's `started-at`, under ADR-036's existing rule that a
successful operation linearizes at some point after its invocation. One page
therefore revalidates as many objects as it reports, and a caller revalidates a
collection in pages rather than in objects.

A listing is a source of positive evidence only. ADR-035 traversals are not
snapshots and may omit objects, so an object's absence from a page carries no
information: it never installs absence and never marks an entry obsolete. A
reported revision that differs from a retained one may mark that observation
obsolete, but only when the observation's watermark precedes the list's
`started-at`; otherwise the page is older news than the cache already holds.

Equivalent content stays equivalent.
[ADR-042](042-conditional-only-backend-mutations.md) and ADR-036 already define
a revision as a content validator rather than a mutation identifier, so a rewrite
that produces identical bytes is indistinguishable from no rewrite and needs no
special handling here. A watermark asserts that the held content is current,
which is precisely what a matching revision establishes.

Nothing in this is specific to snapshot reads. It is a property of ADR-036's
cache, available to any caller holding observations it wants to revalidate in
bulk.

## Consequences

- Revalidating a collection costs one request per page rather than one per
  object, so a scan over an unchanged collection reads bodies only for the
  objects that actually changed.
- Cost now scales with the size of the listed prefix rather than with the volume
  of change. For a very large collection with a tiny change set, listing is
  itself the dominant cost, and a per-slot change log indexed by change rather
  than by object would beat it. That is the reason to revisit this, and the
  reason not to build it first.
- Whether to revalidate by listing is a caller's judgement: listing a whole
  collection to revalidate a handful of objects is worse than reading them.
- Backends must report the revision they already receive. S3 returns `ETag` in
  `ListObjectsV2` and GCS returns `generation`, so no provider capability is
  added; the in-memory and simulated backends must report it too.
- Absence is unchanged. An absent entry still has no conditional token, and a
  listing cannot supply one, so revalidating absence still requires an ordinary
  read.
- Nothing is added to the write path, no object class is introduced, no
  reclamation obligation follows, and no clock enters the argument.

## Alternatives considered

### Report each object's modification time instead of its revision

A timestamp looks like the more natural thing for a listing to carry, and it
would also answer questions a revision cannot, such as how long an object has
been untouched.

It is unsound for this purpose. S3 reports `LastModified` at one-second
granularity, always emitting `.000` milliseconds, so two writes within a second
are indistinguishable. Worse, comparing a truncated timestamp against a
full-precision watermark fails in the unsafe direction: a write at `10.7s` is
reported as `10`, and a reader holding a watermark of `10.5` would conclude that
an object changed after its watermark had not changed at all. That is a silent
wrong answer rather than a lost optimization, and because GCS reports
microseconds it would not reproduce on every backend.

A granularity allowance in the comparison would restore soundness, at the cost
of importing the reasoning
[ADR-052](052-backend-server-time-observation.md) needs for server time into a
place that does
not otherwise need a clock. The revision needs none of it and is exact.

### A per-slot change log

Writers, or a background builder, could record which objects changed in each
grid slot, letting a reader prove absence of change over an interval without
listing anything. Its cost scales with change volume rather than collection
size, which is asymptotically the better property.

It is also far more machinery: a new object class, its own reclamation, a
completeness proof tied to slot closure, and a fallback for when completeness
cannot be established. Having writers append to it puts a write on the commit
path, which the design's cost principles reject; deriving it in the background
instead requires a shuffle, because sharding by transaction forces every reader
to consult every shard while sharding by object forces every builder to read
every transaction. Listing needs none of this and reuses a trait operation that
already exists. Recorded as future work for collections large enough that
listing dominates.

### Keep per-object version-conditional reads

The mechanism already works and needs no ADR. It costs one request per object,
which leaves a warm cache worth nothing to a scan, and that is the case this
design has to serve.
