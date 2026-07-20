# ADR-009: In-doubt conditional-write outcomes (`BackendError::InDoubt`)

## Status

Accepted

The conditional mutation surface is refined by
[ADR-042](042-conditional-only-backend-mutations.md), and post-dispatch
cancellation and cache reconciliation are refined by
[ADR-043](043-causally-coordinated-backend-operations.md).

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

### At-most-once, with in-doubt scoped to the logless path

We do **not** attempt transparent exactly-once retries when there is nothing to
disambiguate them with (the logless single-RW path). The contract is
**at-most-once application + report the uncertainty only when the engine cannot
recover it on its own**:

1. **New error variant `BackendError::InDoubt(String)`** — "the operation's
   outcome is unknown; it may or may not have been applied." It is threaded
   through `StorageError → TransError → public Error` with `is_unavailable()`
   helpers at every layer, and is deliberately **not** classified as
   `retry`/`wounded`/`precondition`.

2. **Backends own conditional-write retries and must report an uncertain
   outcome as `Unavailable`, never as a confident `Precondition`.** Concretely,
   a backend returns `Unavailable` when (a) it observes a precondition failure on
   a conditional write *after an attempt whose outcome was ambiguous* (a lost or
   possibly-applied earlier attempt), or (b) it exhausts its retry budget.
   Reads and unconditional (idempotent) writes are unaffected and may be retried
   freely.

3. **The logged commit path retries `Unavailable` internally.** A transaction
   log is keyed by its tx id; only the owning client writes `committed`, and
   third parties only write to it to wound (status `aborted`). The conditional
   write (`write_if_not_exists` / `write_if`) is therefore idempotent across
   retries: as long as the log is not yet final, any race (our own
   `refresh_pending` advancing the pending log, a wound, or our own previously
   landed attempt) is safe to resolve by re-reading. `Monitor::set_final_log`
   therefore retries on `Unavailable`; if a previous attempt actually landed,
   the retry observes the existing log via `Precondition`, reads its commit
   status, and treats a final status matching its own intent as success
   (`committed==committed` is necessarily our own write; `aborted==aborted`
   converges on the desired outcome regardless of who wrote it). A mismatched
   final status (we wanted `committed` but found `aborted`) is still surfaced
   as `AlreadyFinalized`, which the commit path maps to a wound. This recovery
   is invisible to the caller.

4. **Pre-commit operations recover `Unavailable` in place.** An in-doubt
   outcome while acquiring a *lock* happens before the commit point: no
   user-visible value has been made durable yet, so re-reading the lock
   metadata reveals whether the conditional write took, and re-applying it is
   idempotent. The locker therefore retries the lock operation itself on
   `Unavailable` (`LockerWorker::run` reloads metadata and re-attempts,
   exactly as it already does for a stale `Precondition`), resolving the
   uncertainty in place. The exception is `LockType::Create`: its outcome
   under in-doubt is genuinely ambiguous from outside the writer (same
   reasoning as the single-RW fast path), so a `Create` in-doubt result is
   not retried by the locker. This recovery never escalates into a
   whole-transaction retry that would needlessly re-run the user's closure.

5. **The single-RW fast path first re-issues the idempotent CAS, then surfaces
   only the irreducible in-doubt.** The fast path is logless: its value write is
   the commit point. On an `Unavailable` outcome the engine re-issues the *same*
   conditional write unchanged (same expected version, same value). That write
   is idempotent under its own precondition — no re-read is needed, the
   precondition is what enforces "only when nothing changed":
   - it **lands** (`Ok`) only when the object is still at the expected version
     (no writer changed it, so our earlier attempt did not land either): the
     value is applied exactly once and the transaction commits. This recovers
     the common in-doubt case where the write never landed (e.g. the backend
     exhausted its retry budget on transient errors);
   - it fails the **precondition** when a change already happened. A precondition
     seen *after* an in-doubt attempt is itself in-doubt: our earlier attempt may
     have committed, and with no transaction log that is indistinguishable from a
     genuine conflict from outside the writer, so it is reported as `Unavailable`.

   `Database::tx`'s loop only re-runs on `retry`/`wounded`; an `Unavailable` that
   the fast path could not resolve breaks out as `Error::InDoubt`. The
   transaction may or may not have committed; the caller decides whether to retry
   (with its own idempotency) or accept the uncertainty. Re-issuing the CAS is
   safe — it cannot double-apply — because a write that already landed changed the
   object's version, so the precondition rejects the retry rather than applying it
   a second time.

This let us **keep the single-RW fast path**: it stays a logless CAS, and its one
unsafe interleaving (lost ack + re-observed precondition) is reported as in-doubt
instead of being retried into a double-apply.

### Per-backend obligations

| Backend | Conditional-write retry | Ambiguous outcome → |
|---------|-------------------------|---------------------|
| Memory  | n/a (local, atomic)     | cannot occur |
| `FaultBackend` (sim) | lost-ack injection | a landed conditional write reported as `Unavailable` (modelling a lost acknowledgement) |
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
- Applications must handle `Error::InDoubt`, but **only the single-RW fast
  path** can produce it: a logged-commit transaction recovers transparently. The
  honest contract for a stateless store over object storage is that the fast
  path is in-doubt under a lost ack; callers add idempotency (e.g. a
  client-supplied write token) if they need to retry safely.
- The single-RW fast path is retained, preserving its latency win.

### Regression tests for the contract

- `crates/glassdb/tests/in_doubt.rs` — database/engine level. The `FaultBackend`
  middleware injects a lost ack on a landed conditional write and asserts: the
  single-RW path surfaces `Unavailable` without double-applying (the re-issued
  CAS hits a real precondition because the earlier attempt landed); an in-doubt
  outcome on a write that did *not* land is recovered transparently by re-issuing
  the idempotent CAS, committing exactly once; the logged path recovers
  transparently (engine retries the log write and recognizes its own landed log
  via `Precondition`); a *clean* conflict still retries transparently on the fast
  path.
- `crates/glassdb-backend/src/middleware/fault.rs` — the `FaultBackend` lost-ack
  path (`active_eventually_injects_faults`, `same_seed_same_fault_sequence`).
- `crates/glassdb-backend-s3/src/tests.rs` — the in-process fake injects
  "apply-then-`500`" on a conditional `PutObject`; the backend must surface
  `Unavailable` (it would surface `Precondition` against the pre-fix code).
- `crates/glassdb-backend-gcs/src/tests.rs` — same fault on the GCS JSON insert;
  the backend must surface `Unavailable`.
- The deterministic fuzzer (ADR-011, on the in-repo executor) is the system-level
  guard: with the `FaultBackend` lost-ack/fault injection it asserts
  `acked <= final <= started` under injected faults and client crashes.
