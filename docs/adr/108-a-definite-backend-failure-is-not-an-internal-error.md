# ADR-108: A definite backend failure is not an internal error

## Status

Accepted — implemented.

Refines [ADR-014](014-error-reporting.md): a `BackendError::Other` keeps its own
class through the storage layer and the public error, instead of joining the
unclassified tail.

## Context

ADR-014 gave the catch-all variants a cause, and kept `Other` and `Internal` as
the unclassified tail. `BackendError::Other` became `StorageError::Other`, which
became `Error::Internal`. `StorageError::Other` also carries the storage
layer's own failures, for example decode errors and invariant violations.

So an expired credential or a rejected request reached the caller as
`Error::Internal`, the same class as a GlassDB bug. The doc of `Error::Internal`
says "an unexpected internal failure or invariant violation", which a backend
rejection is not.

The simulation must inject such failures to check that a definite error after
an in-doubt result keeps the operation in doubt. Two bugs of that kind went
unseen because the simulation injected only `Unavailable`. With one class for
both, the simulation could not admit a backend rejection without also admitting
a real bug, unless it inspected the error message.

## Decision

- **A definite backend failure has its own class.** An unclassified backend
  error is definite: the request had no effect, because a backend reports an
  unknown outcome as `Unavailable` (ADR-009). It keeps its own class through the
  storage layer and the public API, as `Error::Backend`. No change of the caller
  is in question, and a retry is safe once the cause is fixed.
- **An internal error means a GlassDB bug.** The storage layer's own failures,
  and requests that GlassDB built wrongly, stay internal. GlassDB checks what a
  backend validates as malformed before it sends a request, so a backend
  never rejects a malformed request of GlassDB as a backend failure.
- **In doubt wins.** An operation that can still have landed stays in doubt,
  also when a later step fails with a backend error.

## Consequences

- A caller can tell a backend failure from a GlassDB bug by the class of the
  error.
- The simulation can inject backend rejections and admit backend failures as
  client errors, without admitting internal errors and without inspecting an
  error.

## Alternatives considered

- **Keep `Internal`, and let the simulation match the message of injected
  errors.** A layer that greps an opaque error depends on its text.
- **Admit `Internal` in the simulation under failures.** It would hide the
  bugs that surface as `Internal` during a faulty run.
- **Map a backend rejection to `Error::Unavailable`.** `Unavailable` means a
  failure that a retry can overcome. An expired credential is not one.
