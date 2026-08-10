# Opaque validation evidence migration

This breaking change affects callers that use `glassdb-trans` directly. The
public `glassdb` API is unchanged. Physical storage observations are now carried
only by opaque evidence so callers cannot separate a logical read or scan from
the exact state that validation must recheck.

## Point reads

The following surfaces were removed:

- `ReadAccess::last_writer`
- `ReadAccess::leaf`
- direct `ReadAccess { ... }` construction
- `ReadOutcome::last_writer`
- `ReadOutcome::leaf`
- direct `ReadOutcome { ... }` construction

Consume `ReadOutcome` and move its opaque evidence into `ReadAccess`:

```rust
let (value, cache_hit, evidence) = outcome.into_parts();
let access = ReadAccess::new(key, evidence);
```

`ReadOutcome::new(value, cache_hit, evidence)` remains available when an opaque
evidence value must be paired with a reconstructed logical outcome. The public
`ReadOutcome::value`, `ReadOutcome::cache_hit`, and `ReadAccess::key` fields are
unchanged; there is no replacement that exposes the writer or leaf observation.

## Range scans

The following surfaces were removed:

- `ScanResult::keys` as a field; use `ScanResult::keys()`
- `ScanResult::covered`
- `ScanResult::frontier`
- direct `ScanResult { ... }` construction
- `ScanAccess::keys`
- `ScanAccess::covered`
- `ScanAccess::frontier`
- direct `ScanAccess { ... }` construction
- the public `LeafCoverage` export

Read the logical keys before consuming the result, then let the result construct
the matching validation access:

```rust
let keys = result.keys().to_vec();
let access = result.into_access(collection, range, overlay);
```

`ScanAccess::collection`, `ScanAccess::range`, and `ScanAccess::overlay` remain
public. Covered leaves and the validation frontier deliberately have no public
replacement.

## Engine scan entry point

The five-argument
`Engine::scan_keys(collection, range, overlay, own_lock_holder, cap)` method was
removed. Use `Engine::scan(collection, range, overlay)`. Holder exclusion and
frontier caps are validation controls owned by the crate-private resolver and no
longer have a public equivalent.
