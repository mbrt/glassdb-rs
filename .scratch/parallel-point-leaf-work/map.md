# Make point-leaf work consistently parallel

## Destination

Produce an implementation-ready design for bounded parallel execution of independent point-access work on distinct data leaves. The design covers point-key routing, direct-commit eligibility, validation, normal lock acquisition, retry release, and committed write-back. It retains same-leaf ordering, transaction phase ordering, and the sorted serial lock fallback.

## Notes

- Domain: GlassDB point-access transactions and data-leaf coordination.
- Use `codebase-design` for module, interface, and seam decisions. Use `grilling` with `domain-modeling` for behavior decisions. Use `prototype` for routing, interface, and measurement artifacts.
- A **leaf** is the physical B-link tree node and CAS unit. `Shard` names its entry body. `ShardCoordinator` names the module that orders mutations on one leaf path.
- Apply one internal concurrency limit to each phase. Do not add public concurrency configuration unless measurements make it necessary.
- Finish work that has started, stop submission of work that has not started after a terminal result, and select results in stable leaf order. Committed write-back remains best-effort across leaves.
- For `L` independent cold leaves and limit `N`, backend wait time should use approximately `ceil(L/N)` waves instead of `L` waves. A one-leaf transaction must add no backend operation, one phase must not exceed `N` active leaf operations, and a repeatable throughput regression greater than 5% rejects the design.
- Preserve the principles in `docs/principles.md`, deterministic simulation, self-correcting routing across splits, one ordered mutation stream per leaf, one transaction-object commit point for cross-leaf transactions, and all existing snapshot-transparent outcomes.
- This Wayfinder effort makes decisions and produces the implementation-ready design. It does not implement the design.

## Decisions so far

- [Define the bounded distinct-leaf execution contract](issues/01-define-bounded-distinct-leaf-execution-contract.md): Bound active attempts per phase invocation, drain the stable started set, and require cancel-safe retirement handoff without task spawning.
- [Choose the point-key batch routing design](issues/02-choose-point-key-batch-routing-design.md): Use path-batched descent for multiple point keys, spend the limit on distinct node paths, and keep direct one-key descent without an extra backend operation.

## Not yet specified

- Final module names, file changes, change order, and ADR shape depend on the routing, seam, and phase decisions.
- The exact initial limit for each phase, and whether measurements support one common value, depend on the prototypes and benchmark plan.

## Out of scope

- Implementing the design.
- A public batch point-read interface or a semantic change to `Transaction::read`; callers continue to poll independent point reads together when they need parallel reads.
- Range scans and sorted listings, collection create/drop coordination, B-link structural work, split maintenance, garbage collection, and recovery work.
- Removing or parallelizing the sorted serial lock fallback.
- A cross-leaf logless or distributed commit protocol.
- General cleanup of historical shard terminology or unrelated ADR status.
