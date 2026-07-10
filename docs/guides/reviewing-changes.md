# Reviewing changes

What to look for when reviewing a change (or refactor) in this project. The
recurring question to ask of the diff is: **does each responsibility live with
its rightful owner?**

## Mechanism vs. policy

- Check that the *mechanism* (generic engines: dedup, fold loops, CAS/retry)
  stays free of *policy* (locking, wound-wait, commit, transaction ids). Policy
  should live in the pluggable pieces owners install (e.g. resolvers), not baked
  into the engine.
- Flag it when an engine names a domain concept (`LockType`, wound-wait,
  membership, `TxCommitStatus`): the concept has likely leaked in from a caller
  and should move out.
- Watch for **asymmetry**: when two similar things are handled differently (e.g.
  shards via installed resolvers but roots via engine-internal special-casing),
  suspect that the odd one out has policy baked into the mechanism. Ask for them
  to be made symmetric.

## Ownership of state and behavior

- Confirm state lives with its owner. Per-transaction bookkeeping belongs to the
  layer that owns that policy, not to a shared engine that merely mutates objects.
- Check that tightly-coupled operations stay together. An operation and its
  inverse (e.g. `lock`/`write_back`) that share a private structure belong in the
  same module, even if orchestrated from elsewhere.
- For each responsibility touched by the diff, ask "who owns this?" and push back
  on anything that belongs to a different layer.

## Minimal surface

- Question accessors/helpers added just to reach an already-shared object: if a
  caller already holds a handle, it should use that handle directly.
- Question a method whose sole purpose is to forward to another object's method;
  the caller should reach the target directly.
- Check visibility: a member only used internally should not be `pub(crate)`.
- Prefer tests that widened visibility to be reworked to use public APIs with
  realistic setup instead.

## Verifying the change

- Confirm `make test-all` passes (format, `clippy -D warnings`, tests).
- Expect a deterministic regression test with any bug fix; it doubles as
  documentation.
- Check that tests are behavioral and realistic: they should exercise intended
  behavior through public APIs, not internals.

## Documentation

- Check that module/type docs describe *purpose*, not implementation or callers.
- When a change alters an invariant stated in a doc comment or ADR, confirm the
  statement was updated to stay true (ADRs are frozen except status/links; module
  docs are not).
