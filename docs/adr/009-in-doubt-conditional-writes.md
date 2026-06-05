# ADR-009: In-doubt conditional-write outcomes (`BackendError::Unavailable`)

## Status

Accepted

## Context

GlassDB builds atomic commits out of object-store compare-and-swap (CAS):
conditional writes (`write_if`, `write_if_not_exists`, `set_tags_if`,
`delete_if`) are the only tool for idempotency, because object storage offers
**no at-most-once request identity**. That creates a fundamental ambiguity:

> If a conditional write's first attempt *lands* but its acknowledgement is
> *lost*, any retry observes a precondition failure (S3 `412`/`409`, GCS
> `conditionNotMet`) that is **indistinguishable from a genuine conflict**.

The deterministic fuzzer (ADR-008) made this concrete. Once its `NetBackend`
modelled real object storage faithfully — at-least-once delivery, no dedup — a
single-key increment could be applied twice (`final > started`): a conditional
write landed, its ack was dropped, the retry saw a `Precondition`, the engine
treated it as a clean conflict and re-applied. The logless single-RW fast path
(ADR-007) is the sharpest case: it keeps no transaction log, so the outcome
cannot be reconstructed, and a *transparent, exactly-once* retry is impossible.

The danger is not hypothetical to the simulator only — it depends on whether a
backend **retries conditional writes and how it reports the result**:

- **Amazon S3** wraps every call in the SDK's adaptive retryer
  (`RetryConfig::adaptive()`). A conditional `PutObject` whose first attempt
  lands but whose response is lost (transport error / timeout / `5xx`) is
  *transparently re-sent by the SDK*; the retry hits the object the first
  attempt created (or whose ETag it changed) and returns `412`/`409`, which the
  backend mapped to a confident `BackendError::Precondition`. The lost-update bug
  was therefore **live in the S3 backend and invisible to the fuzzer**, which
  only exercises the memory and net backends.
- **Google Cloud Storage** uses bare `reqwest` with no retry layer, so a lost
  ack surfaced as a transport error mapped to the generic `BackendError::Other`.
  That does not double-apply (nothing re-sends), but it mislabels an in-doubt
  outcome as an ordinary failure.
- **Memory** is local and atomic — no network, so an ack is never lost; it can
  never be in-doubt.

So the contract had to be made explicit and uniform across *all* backends, and
tested for each one that can actually lose an acknowledgement.

## Decision

### At-most-once, with in-doubt surfaced to the caller

We do **not** attempt transparent exactly-once retries (impossible for the
logless path). Instead the contract is **at-most-once application + report the
uncertainty**:

1. **New error variant `BackendError::Unavailable(String)`** — "the operation's
   outcome is unknown; it may or may not have been applied." It is threaded
   through `StorageError → TransError → public Error` with `is_unavailable()`
   helpers at every layer, and is deliberately **not** classified as
   `retry`/`wounded`/`precondition`. The commit path preserves it (rather than
   flattening it into a string) wherever errors were previously stringified
   (`Algo::commit_writes`, `Monitor::commit_tx`).

2. **Backends own conditional-write retries and must report an uncertain
   outcome as `Unavailable`, never as a confident `Precondition`.** Concretely,
   a backend returns `Unavailable` when (a) it observes a precondition failure on
   a conditional write *after an attempt whose outcome was ambiguous* (a lost or
   possibly-applied earlier attempt), or (b) it exhausts its retry budget.
   Reads and unconditional (idempotent) writes are unaffected and may be retried
   freely.

3. **The engine surfaces it without a transparent retry.** `DB::tx`'s loop only
   re-runs on `retry`/`wounded`; an `Unavailable` commit breaks out as
   `Error::Unavailable`. The transaction may or may not have committed; the
   caller decides whether to retry (with its own idempotency) or accept the
   uncertainty. This reuses the existing "terminate on non-retryable error"
   behavior rather than adding a new mechanism.

This let us **keep the single-RW fast path**: it stays a logless CAS, and its one
unsafe interleaving (lost ack + re-observed precondition) is reported as in-doubt
instead of being retried into a double-apply.

### Per-backend obligations

| Backend | Conditional-write retry | Ambiguous outcome → |
|---------|-------------------------|---------------------|
| Memory  | n/a (local, atomic)     | cannot occur |
| Net (madsim) | bounded retry | `Precondition` after a lost response, or budget exhaustion → `Unavailable` |
| S3 | SDK retryer **disabled** for conditional `PutObject`; backend owns the loop | ambiguous attempt (timeout/dispatch/`5xx`) then `412` → `Unavailable`; budget exhaustion → `Unavailable` |
| GCS | none (single attempt) | transport error or `5xx` on a conditional request → `Unavailable`; a clean `412`/`409` is a genuine conflict (GCS applies conditional writes atomically and we never retry, so it did not take effect) → `Precondition` |

For S3, the backend now distinguishes attempt outcomes itself instead of
delegating to the SDK retryer (which hides intermediate attempts): a `409`
ConditionalRequestConflict means "not applied, retry"; a `503 SlowDown`/`429`
means "throttled, not applied, retry"; a timeout, dispatch failure, or
`500/502/504` means "may have applied" and taints any subsequent `412` as
in-doubt. Unconditional overwrites keep the SDK's adaptive retryer because
re-applying them is harmless.

### Garbage-collection interaction

Transaction logs are deleted asynchronously, well after the writes they describe
are materialized, so a finalized log being garbage-collected never turns a
genuine commit into a phantom conflict during commit or recovery. The in-doubt
outcome concerns the *acknowledgement* of a write, not the later absence of a
log, so GC does not widen the in-doubt window.

## Consequences

- The lost-update bug is fixed at its source in **every** backend that can lose
  an acknowledgement, not just in the simulator. A backend that masked an
  in-doubt result as a confident `Precondition` would re-introduce it, so the
  property is tested per backend.
- Applications must handle `Error::Unavailable`: a transaction that returns it
  may or may not have committed. This is the honest contract for a stateless
  store over object storage; callers add idempotency (e.g. a client-supplied
  write token) if they need to retry safely.
- The single-RW fast path is retained, preserving its latency win.

### Regression tests for the contract

- `crates/glassdb/tests/in_doubt.rs` — DB/engine level. A `FaultBackend`
  decorator injects a lost ack on a landed conditional write and asserts
  at-most-once application for both the single-RW and logged paths, and that a
  *clean* conflict still retries transparently.
- `crates/glassdb-backend/src/net.rs` — `conditional_write_with_lost_ack_is_in_doubt`.
- `crates/glassdb-backend-s3/src/tests.rs` — the in-process fake injects
  "apply-then-`500`" on a conditional `PutObject`; the backend must surface
  `Unavailable` (it would surface `Precondition` against the pre-fix code).
- `crates/glassdb-backend-gcs/src/tests.rs` — same fault on the GCS JSON insert;
  the backend must surface `Unavailable`.
- The deterministic fuzzer (ADR-008) is the system-level guard: with the
  no-dedup `NetBackend` it asserts `acked <= final <= started` under injected
  network/node faults.
