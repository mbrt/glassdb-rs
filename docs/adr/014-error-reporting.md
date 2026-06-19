# ADR-014: Error reporting — preserve cause and classification

## Status

Accepted

## Context

The error types were a fairly literal port of the Go original, which reports
errors by string concatenation. Two habits leaked through and lost information:

- **Causes were flattened to strings.** Foreign errors (AWS SDK, `reqwest`,
  `prost`, base64/path decoding) and our own typed errors were folded into a
  message with `format!("…: {e}")`, so the underlying
  `std::error::Error::source` chain was discarded. Some sites went further and
  dropped the cause entirely with `map_err(|_| …)`.
- **Classification was downgraded by wrapping.** Adding context re-wrapped a
  classified error into the catch-all (`Other`/`Internal`). The dangerous case
  is the in-doubt outcome (ADR-009): an `Unavailable` storage error wrapped with
  a breadcrumb collapsed into `Other`, then into a generic `Internal`, silently
  losing the signal the engine relies on to avoid double-applying a write.

The error enums also derived `Clone`, `PartialEq` and `Eq`. Those derives are
incompatible with carrying a boxed/dynamic cause, and equality on errors was
barely used (two backend tests).

## Decision

### A typed, optional cause on the catch-all variants

The catch-all variants — `BackendError::Other`, `StorageError::Other`,
`TransError::Other`, and the public `Error::Internal` — became struct variants
carrying an optional cause:

```rust
#[error("{msg}")]
Other { msg: String, #[source] source: Option<Cause> }
```

Each type gained `other(msg)` / `with_source(msg, cause)` (and `internal` /
`with_source` for the public `Error`) constructors. The classified variants
(`NotFound`, `Precondition`, `Unavailable`/`InDoubt`, the `TransError`
sentinels) are unchanged: they _are_ the structured, matched-on part of the API;
`Other`/`Internal` remain the unclassified tail.

### `Cause`: an `Arc`-backed, `Clone`able cause (`crates/glassdb-backend/src/lib.rs`)

The cause is a small newtype `Cause(Arc<dyn std::error::Error + Send + Sync>)`
that implements `Error`, `Display`, `Debug` and `Clone`. The `Arc` is
deliberate:

- Errors are **fanned out** in the engine — the dedup batch shares one error via
  `Arc`, and `WaitTxResult` is broadcast to every waiter via `Clone`. A plain
  `Box<dyn Error>` would force dropping `Clone`, which in turn would force
  _lossy_ reconstruction on those fan-out paths — discarding the very cause we
  set out to keep. `Arc` keeps the error types cheaply `Clone` while preserving
  the chain.
- `thiserror`'s `#[source]` requires the field to implement `Error`; a bare
  `Arc<dyn Error>` does not, so the newtype bridges it.

`Cause` is **transparent**: its `Display`/`source()` forward to the inner error,
so the public error chain has no duplicated segments. The trade-off is that a
wrapped cause is not `downcast`-able _through_ `Cause`; this is acceptable
because classification lives in the variants, not in the catch-all.

### Context preserves classification and cause

`StorageError::context` / `TransError::context` prepend a breadcrumb to the
message while keeping the variant and the `source`. In-doubt stays `Unavailable`,
sentinels stay sentinels; only `Other` accumulates the breadcrumb. The
`From<…>` conversions move `msg` + `source` through each layer and map
classification across layers (`Unavailable → InDoubt`, etc.). Cross-layer
breadcrumbs stay string-based on purpose — a typed source is only worthwhile at
the leaf boundary.

### Typed sources at leaf boundaries; structured errors for status/code

Foreign and typed errors are attached as `source` where they originate (SDK,
`reqwest`, `prost`, base64, path parsing). The HTTP/SDK status mappings — which
carry several meaningful fields with no other typed carrier — use dedicated
structured leaf types with a single `Display` definition: `S3RequestError`
(`op`/`path`/`code`/`status` + SDK source) and `GcsStatusError`
(`op`/`path`/`status`). They are flattened into `BackendError` via an inherent
`into_backend_error()` method, **not** a `From` impl: adding another
`From<_> for BackendError` makes `?`/`Ok(())` inference ambiguous at call sites
that rely on `BackendError` being the only inferable error type.

Pure context wraps (`Read(path): reading object body`, …) and synthesized
invariant messages stay as breadcrumb strings — their only structurable content
is `op`/`path`, and the real cause is already a typed `source`.

### Dropped derives

`Clone` is **kept** (via `Arc`). `PartialEq`/`Eq` are **dropped** from
`BackendError` and `StorageError`; the two backend tests that compared errors
now use `matches!`.

## Consequences

- The full cause chain is preserved end to end: `err.source()` reaches the
  originating SDK/HTTP/parse error, and an in-doubt outcome is never silently
  downgraded to a generic internal error while propagating.
- Errors stay cheaply `Clone` (an `Arc` bump) but are no longer `PartialEq`;
  matches use `matches!`/destructuring.
- Causes wrapped in `Cause` are not directly `downcast`-able from the chain;
  programmatic decisions use the classified variants instead.
- New `From<_> for BackendError` impls are avoided to keep error-type inference
  unambiguous at `?`/`Ok(())` call sites; cross-type flattening is done with
  inherent methods.
- Regression tests document the behavior: `context()` preserving cause and
  classification (`error.rs` unit tests in the storage/trans/public crates), the
  full backend → storage → trans → public chain
  (`crates/glassdb/src/error.rs`), and the structured leaf errors
  (`crates/glassdb-backend-s3/src/tests.rs`,
  `crates/glassdb-backend-gcs/src/tests.rs`).
