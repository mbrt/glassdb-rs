# ADR-023: Slimmed `Backend` trait

## Status

Accepted — implemented. The trait change is the **cutover** step of the v2
effort: it landed once the DST oracles were re-pointed at the v2 layout and the
v1 tag-based commit path was retired (it was the last consumer of tags). The
storage caching layer (`ObjectCache`, `ValueCache`, `Locker`) is **retained and
adapted**, not deleted (see
[Cutover](#cutover--keep-and-re-point-the-caching-layer)).

> Naming note: the caches were later renamed for clarity — `Global` →
> `ObjectCache` (backend-version-keyed) and `Local` → `ValueCache`
> (writer-keyed), both now built from a shared LRU. The names below use the
> current ones; the decision is unchanged.

## Context

The current trait ([`crates/glassdb-backend/src/lib.rs`](../../crates/glassdb-backend/src/lib.rs))
has ten methods, shaped by the tag-based layout it was built for
([ADR-016](016-object-storage-native-layout.md)):

- `set_tags_if` — the lock CAS: lock state lives in object metadata tags
  (`lock-type`, `locked-by`, `last-writer`) and is flipped by a conditional
  metadata update.
- `read_if_modified(path, &WriterId)` + the `last-writer` tag — cache
  revalidation: the read-through cache (`ObjectCache`) skips the body download when
  the object's *last-writer tag* is unchanged.
- `get_metadata` — a tag/version read with no body.
- `delete_if` — a conditional delete used on the GC / unlock-create path.

Because S3 has no metadata-only update, the S3 backend emulates this shape: it
prepends an 8-byte `nonce` to every body (to force a fresh ETag on a
metadata-only rewrite), stores tags in `x-amz-meta-*`, and implements `delete_if`
as a non-atomic HEAD-then-DELETE with a documented TOCTOU window. GCS rides its
native metadata PATCH (`ifMetagenerationMatch`) and encodes both `generation` and
`metageneration` in the opaque version token.

The v2 layout ([ADR-017](017-shard-object.md)–[ADR-021](021-wound-wait-leases-shard.md))
keeps **all** coordination state in object **content** and mutates it **only by
content CAS**. The shard/root I/O
([`crates/glassdb-storage/src/shardstore.rs`](../../crates/glassdb-storage/src/shardstore.rs))
uses just `read` / `write_if` / `write_if_not_exists`, always with `Tags::new()`.
So `set_tags_if`, `get_metadata`, `delete_if`, the S3 nonce, and all
tags / `WriterId` are dead weight once v1 is gone — **with one exception**.

The exception is **cache revalidation**. The coordination objects (shards, root)
are tagless, so the *writer-tag* form of `read_if_modified` cannot tell when their
content changed (a lock taken by a peer leaves the writer tag untouched). Today
`ShardStore` therefore bypasses the cache and full-fetches every shard/root read
(see the `TODO(perf)` in `shardstore.rs`). But the object's **ETag does change on
every content write** — exactly when a cached copy must be invalidated — so a
*version-conditional* GET would let a hot, unchanged shard revalidate without
re-transferring its body. The slimmed trait must keep a conditional read; it just
has to be keyed on the version, not on a tag.

## Decision

### Reduced surface (seven methods)

```
read                 (path)                         -> ReadReply        // contents + version
read_if_modified     (path, expected: &Version)     -> ReadReply        // or Precondition if unchanged
write                (path, value)                  -> Version
write_if             (path, value, expected: &Version) -> Version
write_if_not_exists  (path, value)                  -> Version
delete               (path)                          -> ()
list                 (dir_path)                      -> Vec<String>
```

Content CAS (`write_if` / `write_if_not_exists`) is the only coordination
primitive. The opaque `Version` token and the `BackendError` enum (including
`Unavailable`, the in-doubt marker of [ADR-009](009-in-doubt-conditional-writes.md))
are unchanged.

### Version-conditional `read_if_modified` (implements the cache-revalidation need)

`read_if_modified` is **re-keyed from the writer tag to the object version**:

```
read_if_modified(path, expected: &Version) -> Result<ReadReply, BackendError>
```

It returns the full object when the stored version differs from `expected`, and
`BackendError::Precondition` to mean *"not modified — your cached copy is still
current."* This is a deliberate reuse of the existing convention: `ObjectCache::read`
already treats a `Precondition` from `read_if_modified` as "unchanged, serve the
cached entry", so the cache contract carries over verbatim — only the condition
(version instead of writer tag) changes.

It maps to a native conditional GET on every backend:

- **S3** — `GetObject` with `If-None-Match: <etag>`; a `304 Not Modified` is
  reported as `Precondition`.
- **GCS** — `GET` with `ifGenerationNotMatch=<generation>`; a `304` is reported as
  `Precondition`.
- **Memory** — compare the stored version to `expected`.

Because the condition is the content ETag (not a tag), this stays within the
"content CAS only" principle of ADR-016 while making the cached *"shard
(conditional GET)"* read of *Direction at a glance* real. It refines ADR-016's
target surface: the *writer-tag* `read_if_modified` is dropped, and a
*version-conditional* one takes its place.

### Removed types and parameters

- Drop the `Tags` argument from every write, and the `tags` field from
  `ReadReply`.
- Drop the `Metadata` and `WriterId` types, `LAST_WRITER_TAG`,
  `encode_writer_tag`.
- Drop `set_tags_if`, `get_metadata`, and `delete_if` (with its TOCTOU window).

Writes return the new `Version`; `read` / `read_if_modified` return contents plus
`Version`. Nothing in the trait carries metadata any more.

### In-doubt parity carries over unchanged (ADR-009)

The CAS sites of the v2 protocol — pending-object create
(`write_if_not_exists`), the shard/root lock CAS, the commit-flip CAS, the
write-back CAS (all `write_if`), and the wound/abort CAS — inherit
[ADR-009](009-in-doubt-conditional-writes.md) verbatim: an ambiguous conditional
write whose outcome cannot be confirmed is reported as `Unavailable`, never as a
confident `Precondition`. The S3 backend's conditional-`PutObject` classification
loop is unchanged; only its tag/nonce handling is removed. `read_if_modified` is
an idempotent read, so a transient failure stays freely retryable (`Unavailable`)
and is never in-doubt. The ADR-009 per-backend obligation table shrinks to its
conditional-write rows.

### Per-backend simplification

- **S3** — drop the body nonce (a content write always changes the body, so the
  ETag is naturally fresh; there are no metadata-only rewrites any more), drop the
  `x-amz-meta-*` tags, and drop the HEAD-then-DELETE `delete_if` and its TOCTOU
  window (`delete` stays unconditional). Serve `read_if_modified` with
  `If-None-Match`.
- **GCS** — drop the metadata PATCH and the custom-metadata tags. The version
  token collapses to `generation` only: `metageneration` existed to CAS metadata
  independently of content, which no longer happens. Serve `read_if_modified` with
  `ifGenerationNotMatch`.
- **Memory** — drop tag storage and the tag-conditional operations; compare
  versions for `read_if_modified`.
- **Middleware** (delay, scheduler, logger, fault, recording, stats) — drop the
  removed methods, keep `read_if_modified`.

### Cutover — keep and re-point the caching layer

The trait can only shrink after the v1 tag-based commit path is gone, so the
sequencing is:

1. Re-point the DST oracles (serializability, cycle ring) at the v2 layout.
2. Retire the v1 tag-based commit path.
3. **Keep `ObjectCache` / `ValueCache` / `Locker`** and adapt them to the slimmed
   interface. In particular, re-point `ObjectCache`'s read-through revalidation
   from the writer-tag `read_if_modified` to the **version-conditional** one, and
   route shard/root reads back through the cache, so a hot unchanged shard
   revalidates without a body transfer. These modules are the substrate for
   re-introducing proper caching over the new interface; deleting them is **not**
   part of the cutover.
4. Slim the `Backend` trait to the seven methods above.
5. Simplify the three backends and the middleware.
6. Regenerate the golden vectors and the `RecordingBackend` byte-stream
   expectations.

## Consequences

- The trait is a small, store-agnostic, content-CAS-only surface where every
  method maps to a primitive S3 and GCS both provide natively — no emulation.
- The cached conditional-GET read of *Direction at a glance* becomes real: the
  version-conditional `read_if_modified` lets the cache revalidate the tagless
  coordination objects (Group E in [`docs/algo-v2.md`](../algo-v2.md)).
- The `ObjectCache` / `Locker` caching layer is **preserved and re-pointed**, not
  rewritten: the cache logic is reused, with only its revalidation condition
  swapped from the writer tag to the object version.
- S3 sheds the nonce and the `delete_if` TOCTOU window; GCS's version token
  simplifies to a single generation.
- It is a **breaking change** for any external `Backend` implementation, and the
  golden vectors / `RecordingBackend` byte-stream expectations regenerate once
  (the layout-independent DST oracles carry over unchanged as the safety net).
- The change is **gated on v1 retirement**; until then the trait keeps its v1
  shape so the existing suite stays green.
- The in-doubt contract of ADR-009 is preserved verbatim at the new CAS sites, so
  the lost-ack property continues to hold on every backend that can lose an
  acknowledgement.
