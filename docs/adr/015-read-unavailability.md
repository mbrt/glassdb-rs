# ADR-015: Transient read unavailability — retry idempotent reads, surface `Error::Unavailable`

## Status

Accepted

## Context

A transactional read (`Transaction::read` -> `Reader::read` -> `Global::read` ->
backend) had two gaps under a backend outage:

- **No engine-level retry.** Only the backend SDK retried; the commit side
  already retries an in-doubt (`Unavailable`) outcome in place (the monitor's
  `set_final_log` / `try_abort_remote_tx`, the locker), but reads did not. A
  transient blip therefore failed the whole transaction.
- **Lost classification.** The catch-all in `Transaction::read` wrapped every
  non-`NotFound` storage error with `Error::with_source(...)`, collapsing it into
  a generic `Error::Internal` — discarding the `Unavailable` classification the
  rest of the stack preserves ([ADR-014](014-error-reporting.md)).

A read is idempotent: re-reading can never double-apply anything, so it is always
safe to retry ([ADR-009](009-in-doubt-conditional-writes.md): "reads and
unconditional (idempotent) writes ... may be retried freely"). The whole in-doubt
machinery exists *only* because a conditional write cannot be safely retried;
that reasoning does not apply to reads. Consequently `Error::InDoubt` — whose
contract is "a mutation may or may not have applied" — is the wrong
classification for a side-effect-free read that simply could not complete.

A further obstacle: S3 and GCS reads classified transient failures (`5xx`,
timeouts, transport errors) as `BackendError::Other`, never `Unavailable`, so an
engine retry-on-`Unavailable` would not even trigger for a real cloud read
outage.

## Decision

### Retry idempotent reads in place, then surface `Error::Unavailable`

1. **The reader retries an `Unavailable` read with bounded backoff.**
   `Reader::read` wraps a single attempt (`read_once`) in a loop that retries
   only `StorageError::Unavailable`, using the database's configured
   `RetryConfig` backoff, which is passed into `Reader::new` (and threaded
   through `Transaction::new` from the database). The attempt cap
   (`READ_UNAVAILABLE_RETRIES`) keeps a sustained outage from looping forever; a
   caller `timeout` still bounds the total wait by dropping the future at any
   `.await`. This mirrors the bounded-then-surface shape of the single-RW fast
   path's in-doubt handling.

2. **A new public `Error::Unavailable(String)` variant**, distinct from
   `Error::InDoubt`: "a read (or other idempotent operation) could not complete
   because storage was unavailable, even after retries; safe to retry." It is a
   `String`-carrying classified variant, consistent with `InDoubt` and
   [ADR-014](014-error-reporting.md)'s rule that classified variants carry a
   message, not a typed cause.

3. **A single chokepoint maps a read's `Unavailable -> Error::Unavailable`.**
   The blanket `From<StorageError> for Error` keeps mapping `Unavailable ->
   InDoubt` (the commit/write path depends on it). A `pub(crate)`
   `Error::from_read` helper makes the read-specific exception in one place, and
   every side-effect-free user read uses it: `Transaction::read`,
   `Collection::read_stale`, `keys`, `collections`, and `create`'s existence
   probe. The right classification depends on the *operation*, not just the
   error.

   This deliberately stays in the engine layer rather than in `Global` or the
   backend. Those layers know read-vs-write, but that distinction alone is not
   enough: some commit-path reads *confirm a pending write* (e.g.
   `Monitor::set_final_log` re-reads `commit_status` after an in-doubt log write;
   lock acquisition re-reads metadata while resolving an in-doubt writer). When
   such a read fails it inherits the mutation's uncertainty and must stay
   in-doubt. `Global::get_metadata` and the tx-log reads serve exactly those
   contexts, so a blanket downgrade there would mislabel real in-doubt commit
   outcomes as retry-safe. Hence the conservative default lives in `From`, and
   only genuine user reads opt out via `from_read`.

4. **Backends classify transient failures of idempotent requests as
   `Unavailable`.** Only with this does the reader's retry trigger for real
   outages. Reads are idempotent, so this never risks a double-apply:
   - **GCS**: `check_status` (the non-conditional mapping used by reads,
     `get_metadata`, unconditional write/delete, list) maps `5xx` to
     `Unavailable`; the `send` transport helper, which carries only those
     idempotent ops, maps a transport failure to `Unavailable` instead of
     `Other`.
   - **S3**: reads, HEADs, and the GET behind `set_tags_if` go through `run`,
     which now maps SDK errors via `annotate_read`: a throttle (`503`/`429`),
     timeout, dispatch failure, or `5xx` becomes `Unavailable`. The
     conditional-write path keeps `annotate` unchanged, so a transient failure
     that never landed is *not* mislabelled in-doubt.

Conditional writes / commit are explicitly **not** routed through this
read-style retry — that is the lost-update hazard
[ADR-009](009-in-doubt-conditional-writes.md) prevents.

## Consequences

- A transient read outage is recovered transparently, below `Database::tx`, so
  the user closure is not re-run. A sustained outage surfaces as a clean,
  matchable `Error::Unavailable` (never `InDoubt`, never `Internal`), which a
  caller can safely retry because the read had no side effects.
- `BackendError::Unavailable` now carries two related meanings: an in-doubt
  conditional-write outcome (the original ADR-009 use) and a transient failure of
  an idempotent request (safe to retry). Both are "the outcome could not be
  confirmed"; the distinction that matters — whether a retry could double-apply —
  is made by the *operation*, which is why only the read path reclassifies it as
  `Error::Unavailable`.
- Regression tests document the behavior: `crates/glassdb/tests/read_unavailable.rs`
  (transparent retry on a transient read outage; `Error::Unavailable` on a
  sustained one), and per-backend read reclassification in
  `crates/glassdb-backend-gcs/src/tests.rs` and
  `crates/glassdb-backend-s3/src/tests.rs`.
