# Crate code-structure review implementation notes

This file records implementation details, deviations, and review points for the
checkboxes in `crate-code-structure-review.md`. The review checklist remains a
working document and is intentionally not committed with these changes.

## F16-A — Extract `NodeStore`

- Moved the node codec, node/root/leaf persistence, and observation-bound
  `LeafEdit` into `node_store.rs`. `ShardStore` retains explicit one-line node
  delegates and exposes `nodes()` for the staged migration; it does not use
  `Deref` or a generic compatibility conversion.
- The extracted store is built from a clone of the same `CachedStore`, preserving
  cache evidence, timeline ordering, warm-read operation counts, and CAS error
  mappings.
- Moved the six existing node-store behavior tests to the new owner and kept
  their semantic and operation-count assertions. Added one long-term pagination
  regression covering a listing larger than the 128-object backend page.
- No persistent bytes, retry policy, deterministic scheduling, or public runtime
  behavior changed. The only compatibility surface added is `NodeStore` plus
  `ShardStore::nodes()`, as required by the migration plan.
- Adversarial review found no correctness, operation-count, format, scope, or
  long-term test-value issues.

## F16-B — Extract `StructuralLogStore`

- Moved the structural-log codec, path/participant validation, conditional CRUD,
  and paginated listings into `structural_log_store.rs`. `ShardStore` keeps
  explicit delegates and exposes `structural_logs()` until F16-D migrates the
  protocol owners.
- Both typed stores are still derived from the same `CachedStore`, preserving
  cache coordination, observations, sequence points, and backend operation
  ordering.
- Moved the existing participant and pagination regressions to the new owner.
  Added one compact long-term lifecycle test for the successful phase update,
  stale-observation conflict, and deletion path.
- Persistent bytes, page size, prefix scope, retry behavior, and CAS error
  mappings are unchanged.
- Adversarial review found no correctness, safety, API, pagination, or excessive-
  test issue; generic cached-store coverage already owns missing-delete
  convergence, so it was not duplicated here.

## F16-C — Narrow `TreeRouter` to `NodeStore`

- `TreeRouter` now owns only `NodeStore`; its production module has no
  structural-log-capable handle. All composition sites explicitly pass the node
  capability through `ShardStore::nodes()`.
- Routing control flow, read requirements, cache-hit aggregation, stale-parent
  correction, and terminal freshness checks were not changed. Existing router
  behavior and backend-operation-count tests remain the long-term acceptance
  coverage.
- No compatibility conversion or one-implementation abstraction was added, and
  no migration-only tests were introduced.
- Adversarial review found no code issue or capability backdoor. It identified
  one stale architecture sentence naming `ShardStore`; that documentation now
  names the narrowed `NodeStore` handle.
