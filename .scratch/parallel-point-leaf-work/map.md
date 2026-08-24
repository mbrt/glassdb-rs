# Make point-leaf work consistently parallel

## Destination

Produce an implementation-ready design for bounded parallel execution of independent point-access work on distinct data leaves. The design covers point-key routing, direct-commit eligibility, validation, normal lock acquisition, retry release, and committed write-back. It retains same-leaf ordering, transaction phase ordering, and the sorted serial lock fallback.

## Notes

- Domain: GlassDB point-access transactions and data-leaf coordination.
- Use `codebase-design` for module, interface, and seam decisions. Use `grilling` with `domain-modeling` for behavior decisions. Use `prototype` for routing, interface, and measurement artifacts.
- A **leaf** is the physical B-link tree node and CAS unit. `Shard` names its entry body. `ShardCoordinator` names the module that orders mutations on one leaf path.
- Apply one internal concurrency limit to each phase. Do not add public concurrency configuration unless measurements make it necessary.
- Use bounded `join_all` semantics: waiting futures count against the limit, every supplied input runs, and outputs return in stable input order. Do not add parking, replacement, or terminal-cutoff concepts to the generic interface. Committed write-back remains best-effort across leaves.
- Put input combination in the domain interface that owns physical routing, logical resolution, or atomic leaf mutation. Do not add a shared routed point-leaf plan.
- A **routed leaf group** (`RoutedLeafGroup<T>`) is the temporary `TreeRouter` output: one leaf observation and the ordered logical keys, with their domain payloads, routed to it. It records a routing result, not a durable ownership claim. The observation carries currentness evidence; there is no separate freshness field.
- For `L` independent cold leaves and limit `N`, backend wait time should use approximately `ceil(L/N)` waves instead of `L` waves. A one-leaf transaction must add no backend operation, one phase must not exceed `N` incomplete leaf futures, and a repeatable throughput regression greater than 5% rejects the design.
- Preserve the principles in `docs/principles.md`, deterministic simulation, self-correcting routing across splits, one ordered mutation stream per leaf, one transaction-object commit point for cross-leaf transactions, and all existing snapshot-transparent outcomes.
- This Wayfinder effort makes decisions and produces the implementation-ready design. It does not implement the design.

## Decisions so far

- [Define the bounded distinct-leaf execution contract](issues/01-define-bounded-distinct-leaf-execution-contract.md): Use a foreground bounded join that counts incomplete futures, runs every input, returns stable ordered outputs, and relies on existing cancellation guards.
- [Choose the point-key batch routing design](issues/02-choose-point-key-batch-routing-design.md): Use path-batched descent for multiple point keys, return stable `RoutedLeafGroup<T>` values, spend the limit on distinct node paths, and keep direct one-key descent without an extra backend operation.
- [Place the point-leaf planning and execution seams](issues/03-place-point-leaf-planning-and-execution-seams.md): Put `join_all_bounded` in `glassdb-concurr`, keep batching in its domain owners, and add no shared point-leaf plan or domain-aware executor.
- [Define routed leaf-group lifetime across commit paths](issues/04-reuse-one-point-leaf-plan-across-commit-paths.md): Share only logical access facts, treat routed leaf groups as temporary evidence, reuse cached `Any` descent through `TreeRouter`, retain successful `LockedTx` groups and receipts, and regroup only after stale evidence.
- [Define parallel point validation](issues/05-define-parallel-point-validation.md): Use input-aligned physical and logical batches over distinct paths and leaves, share one lower bound and limit, and use keyed receipts with an exact own-holder shortcut.
- [Bound normal leaf lock acquisition](issues/06-bound-normal-leaf-lock-acquisition.md): Bound the complete combined leaf set with stable outcome selection, and retire and renew the transaction identity before hard-timeout serial acquisition.

## Not yet specified

- Final file changes, change order, and ADR shape depend on the remaining phase decisions.
- The exact initial limit for each phase, and whether measurements support one common value, depend on the prototypes and benchmark plan.

## Out of scope

- Implementing the design.
- A public batch point-read interface or a semantic change to `Transaction::read`; callers continue to poll independent point reads together when they need parallel reads.
- Range scans and sorted listings, collection create/drop coordination, B-link structural work, split maintenance, garbage collection, and recovery work.
- Removing or parallelizing the sorted serial lock fallback.
- A cross-leaf logless or distributed commit protocol.
- General cleanup of historical shard terminology or unrelated ADR status.
- A timed collection delay in `ShardCoordinator`; keep its existing scheduler yield and natural backend-I/O collection window.
