# API migrations

## Opaque transaction validation evidence

This change affects direct users of `glassdb-trans`; the public `glassdb` API
is unchanged. Physical leaf observations are now carried only by opaque
evidence, so logical accesses cannot be paired with unrelated validation state.

For point reads, these surfaces were removed:

- `ReadAccess::{last_writer, leaf}` and direct `ReadAccess { ... }`
  construction;
- `ReadOutcome::{last_writer, leaf}` and direct `ReadOutcome { ... }`
  construction.

Consume the outcome and move its evidence into the access record:

```rust,ignore
let (value, cache_hit, evidence) = outcome.into_parts();
let access = ReadAccess::new(key, evidence);
```

`ReadOutcome::new(value, cache_hit, evidence)` remains available when an
opaque evidence value must be paired with a reconstructed logical outcome.
`ReadOutcome::{value, cache_hit}` and `ReadAccess::key` remain public. There is
no replacement that exposes the physical writer or leaf observation.

For range scans, these surfaces were removed:

- the `ScanResult::{keys, covered, frontier}` fields and direct struct
  construction;
- the `ScanAccess::{keys, covered, frontier}` fields and direct struct
  construction;
- the public `LeafCoverage` export.

Read logical keys through `ScanResult::keys()`, then consume the result to
construct its matching validation access:

```rust,ignore
let keys = result.keys().to_vec();
let access = result.into_access(collection, range, overlay);
```

`ScanAccess::{collection, range, overlay}` remain public. Covered leaves and
the validation frontier deliberately have no public replacement.

The five-argument
`Engine::scan_keys(collection, range, overlay, own_lock_holder, cap)` entry
point was also removed. Use `Engine::scan(collection, range, overlay)`.
Holder exclusion and frontier caps are now resolver-owned controls and have no
public equivalent.

## Observation-bound leaf edits

`LoadedLeaf` no longer exposes mutable `entries`, `locks`, or `observation`
fields. Use the read-only `entries()`, `locks()`, and `observation()` accessors
when inspecting a load. `path()`, `node()`, and `owns()` expose the other
read-only state.

`ShardStore::store_leaf(path, entries, locks, observation)` was removed. Convert
the loaded value into an edit, change only its bounded contents, and commit that
edit:

```rust,ignore
let loaded = store.load_leaf(path, requirement).await?;
let mut edit = loaded.into_edit();
edit.set_entries(entries);
edit.set_locks(locks);
let committed = store.commit_leaf(edit).await?;
```

`NodeStore::commit_leaf` provides the same operation when using the narrower
node store directly. A `LeafEdit` retains the exact observation, immutable path,
high key, and right-sibling topology from its load; there is no loose-argument
replacement.

## Entry-lock state

`ShardEntry::lock_type` and `ShardEntry::locked_by` are no longer public fields,
and direct `ShardEntry { ... }` construction was removed because its private
lock state must remain valid. Construct an unlocked entry with
`ShardEntry::new(key)`, optionally set its committed value with `with_current`,
and use the lock API:

- inspect with `lock_type()`, `lock_holders()`, and `is_locked_by()`;
- transition with `acquire_read_lock()`, `replace_write_lock()`,
  `replace_create_lock()`, `replace_lock()`, and `release_lock()`;
- build a validated replacement with `EntryLockState::default()`, `read()`,
  `write()`, or `create()`.

The `NodeLock` compatibility alias was also removed. Use
`SharedExclusiveLock` for node membership locks. Invalid combinations of lock
type and holders now cannot be constructed through the public API.

## Split policy

`SplitPolicy` no longer supports public struct literals, and all five fields
are private. Start with the production defaults, override the desired values,
and build a validated policy:

```rust,ignore
let policy = SplitPolicy::builder()
    .leaf_max_entries(512)
    .node_soft_max_bytes(512 * 1024)
    .index_max_children(512)
    .node_max_bytes(2 * 1024 * 1024)
    .split_headroom_bytes(128 * 1024)
    .build()?;
```

The field and accessor migration is one-to-one:

- `leaf_max_entries` becomes `leaf_max_entries()`;
- `leaf_max_bytes` becomes `node_soft_max_bytes()`;
- `index_max_children` becomes `index_max_children()`;
- `node_max_bytes` becomes `node_max_bytes()`;
- `split_headroom_bytes` becomes `split_headroom_bytes()`.

The builder setters use those same new names. `SplitPolicy::validate()` and
`EngineConfig::validate()` were removed. There is no separate replacement for
either validation method: `EngineConfig::set_split_policy()` now accepts only a
policy already validated by the builder. An invalid headroom/hard-cap
relationship therefore returns `InvalidSplitPolicy` from `build()` instead of
an invalid policy reaching `DatabaseBuilder::open`. `SplitPolicy::default()`
keeps the previous production thresholds.

## Backend listing requests

External `Backend` implementations must replace the removed raw listing method:

```rust,ignore
async fn list(
    &self,
    prefix: &str,
    cursor: Option<&ListCursor>,
    limit: ListLimit,
) -> Result<ListPage, BackendError>;
```

with the required request-taking contract:

```rust,ignore
async fn list_request(
    &self,
    request: ListRequest<'_>,
) -> Result<ListPage, BackendError>;
```

Use `request.prefix()`, `request.limit()`, and `request.provider_cursor()` when
building the provider request. `provider_cursor()` is already validated and
unwrapped; implementations should not decode `request.cursor()` themselves.
Callers construct a request with `ListRequest::new(prefix, cursor, limit)?` and
pass it to `list_request`. The raw `Backend::list`, `CachedStore::list`,
`validate_list_args`, and `validate_list_args_and_cursor` compatibility APIs
have been removed. `bind_list_cursor` remains available for implementations to
bind a nonempty provider continuation token to the request prefix.

## Materialized iterators

Materialized key and child-collection iterators no longer wrap each item in an
impossible `Result`. The awaited method remains the I/O, decoding, and
validation boundary; iteration over the owned result is infallible.

The removed methods and their replacements are:

- `Collection::keys()` becomes `Collection::iter_keys()`;
- `Collection::collections()` becomes `Collection::iter_collections()`;
- `Transaction::collections(parent)` becomes
  `Transaction::iter_collections(parent)`.

The removed `KeysIter` type is replaced by `KeyIter`, whose item is `Vec<u8>`
rather than `Result<Vec<u8>, Error>`. The removed `CollectionsIter` type is
replaced by `CollectionIter`, whose item is `CollectionEntry` rather than
`Result<CollectionEntry, Error>`.
