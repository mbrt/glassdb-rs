# Design Principles

These are the principles from which GlassDB is founded:

- Strong consistency by default.
- Optimistic concurrency: we assume things to "go well" by default: there's no
  conflict, the cache is up to date. We revert to slower algorithms only when
  proven to be necessary.
- Read only transactions take no locks and do no writes by default.
- Single value transactions with warm caches should take a single backend
  operation.
- Conflicts and inconsistencies cannot be exposed to user code. If they are, the
  user code must be transparently retried.
- Correctness over speed. We prefer to be correct and slow than fast and wrong.
- The user doesn't pay for what they don't use. Expensive background work should
  happen only if the user's workload justifies it.
