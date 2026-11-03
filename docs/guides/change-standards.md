# Change standards

The standards that a change to GlassDB must meet. Authors apply them when they
write a change. Reviewers check a change against them. They come after the
[design principles](../principles.md), which every change must respect.

Each section covers one area. A reviewer reads the sections that the change
touches.

## Protocol

These rules apply to changes to the transaction algorithm, locks, transaction
records, the cache, evidence types, and the on-the-wire format.

- For decisions based on reads, check
  [the currentness barrier rules](caching.md#currentness-barriers).
- For conditional mutation, check the
  [CAS receipt semantics](caching.md#cas-receipts) and the
  [coordinator rules](caching.md#coordinator-mutation-evidence).
- For changes to `crates/glassdb-storage/src/timeline.rs` and
  `crates/glassdb-storage/src/cached_store/evidence.rs`, identify which
  [storage evidence rules](storage-consistency.md) each change affects. This
  includes visibility, exports, conversions, tests, and moves. Check that the
  forbidden transformations stay impossible.
- For changes to a backend, check the backend contract: per-object
  linearizability and conditional operations that apply exactly once.

## Ownership

Each responsibility lives with its owner.

- Keep the mechanism free of policy. Generic engines (dedup, member policy
  evaluation loops, CAS and retry) do not contain locking, wound-wait, commit,
  or transaction identities. Policy lives in the pieces that owners install,
  for example member policies.
- If an engine names a domain concept (`LockType`, wound-wait, membership,
  `TxCommitStatus`), the concept probably leaked in from a caller. Move it out.
- If two similar things are handled differently (for example, leaves through
  member policies but roots through special cases in the engine), the odd one
  probably has policy in the mechanism. Make them symmetric.
- Keep state with the layer that owns its policy. Per-transaction bookkeeping
  does not belong to a shared engine that only mutates objects.
- Keep an operation and its inverse (for example, `lock` and `write_back`)
  in the same module when they share a private structure.
- Implement a policy once. If two modules reconcile the same state, move the
  logic to the module that owns that state.
- Do not keep two copies of state that can drift. Share one owner.
- Keep foreground work in the foreground. Background work is for work that the
  caller does not wait for.
- Before you add state or a mechanism, check whether an existing state or
  observation can give the same answer.

## Interfaces

Design interfaces as [the interface design guide](interface-design.md) says.
These rules add the problems that reviews of GlassDB found most often.

- Before you add a method, check whether an existing entry point or seam can
  take the new behavior.
- Keep related operations symmetric in shape and in name (e.g. do not mix
  `start_` and `begin_` for the same kind of step).
- Return values. Do not use mutable out-parameters.
- Use the standard types. Do not hand-write a type that a `Result` or an
  `Option` already expresses.
- Do not add public setters that skip invariants. Put the change in a
  higher-level operation that keeps the invariants.
- Keep one concern per type. Do not mix settings, observations, and decisions
  in one struct.
- Do not tie a producer to a consumer through a flag or a callback that only
  one side needs.
- Add a trait method only when implementations differ. Do not add one only to
  support default methods or to save one small cost.
- A member that only its module uses is not `pub(crate)`.

## Code

- Write idiomatic Rust.
- In code that simulation tests run, use `glassdb_concurr::rt` for clocks,
  sleeps, timeouts, and spawns, not `tokio` or `std::time::Instant`.
- If two values must stay in sync, compute one from the other. Do not hard-code
  both and assert that they match.
- Do not keep compatibility shims, old names, or migrations. After a rename, no
  old name remains.
- Support a performance claim with a measurement. Do not add an optimization,
  such as a `Box`, without one.
- Give each default a reason, and state its cost for the foreground workload. A
  high parallelism limit for background work slows down transactions.
- Comments explain why, not what. If the code needs a comment to explain what it
  does, try to make the code clearer first.
- Complex code needs a reason, and a comment states it.
- Keep implementation methods in order of visibility.
- Do not add a guard, gate, fallback, or parameter for a risk with no known
  failure. Prefer less mechanism.

## Tests

- Test intended behavior through public interfaces, not internals.
- Prefer integration tests to mocks.
- Do not write tautological tests.
- Add a deterministic regression test with every bug fix. It also documents
  the bug.
- Test the default configuration. If a test passes only with a non-default
  policy, the default has lost coverage. Fix the cause and do not pin the
  policy.
- If a change makes a test vacuous, delete the test or make it test something
  again. Do not only correct its comment.
- In transaction bodies, return errors instead of assertions. Assertions are
  not snapshot-transparent.
- When a change alters what a benchmark or test measures, say so. Benchmark
  cells keep their intent.

## Docs

- Use the terms in [`GLOSSARY.md`](../../GLOSSARY.md).
- Module, type, and function docs describe purpose, not implementation or
  callers.
- ADRs record the decision, the reasons, and the trade-offs. They do not copy
  constants, thresholds, or experiment results from the code, because these go
  out of date.
- Keep new glossary terms and definitions short and plain. Avoid long noun
  strings.
- Before you remove a glossary term or a concept, check that no doc still uses
  it, including superseded ADRs and investigation logs.
- When a change alters an invariant in a doc comment or in `architecture.md`,
  update the statement. Accepted ADRs are frozen, except for their status and
  links to newer ADRs.
- Do not mention issue trackers, local paths, or anything that a reader of
  this repository cannot see. Public URLs are OK.
- Check every claim against the code, not against the earlier text of the doc.

## Verification

- For changes to code, run `make test`. It runs format checks,
  linting, ordinary tests, and simulation tests.
- For changes to benchmarks or CI comparison tools, also run `make test-bench`.
- Check that CI has every dependency of a new check. A check that passes only
  because a package is installed on your machine fails in CI.
- For benchmark changes, check the "Compare to main" job in
  `.github/workflows/performance.yml`. It runs the harness against the
  merge-base of the PR, which does not have new APIs.
- Check config files with the standard linter for their format.
- Run loaders and tools on real files, including files from earlier runs, not
  only on test fixtures.
- For a refactor of a benchmark or a measurement, run the same benchmark before
  and after the change and compare the results.
