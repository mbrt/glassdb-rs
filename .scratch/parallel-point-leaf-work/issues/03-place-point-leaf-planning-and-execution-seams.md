# Place the point-leaf planning and execution seams

Type: prototype
Status: resolved
Blocked by: 01, 02

## Question

Where should the interfaces for a routed point-leaf plan and bounded distinct-leaf execution live, and what is the smallest deep interface that gives routing, validation, locking, release, and write-back consistent scheduling without exposing a shallow generic concurrency wrapper? Use materially different interface prototypes and compare their depth, locality, and seam placement.

## Prototype

[Point-leaf seam logic demo](../assets/point-leaf-seam-prototype.html) compares an executable routed plan, a flexible phase program, a stateful point-leaf workflow, and a bounded join with domain batch interfaces. It includes guided cases for direct commit, foreign-holder waits, stable result selection, rerouting, and the sorted serial fallback.

Human review selected the bounded join with domain batch interfaces.

The validated primary source is on local throwaway branch `prototype/point-leaf-seam-bounded-join`, commit `23999cfc`, at `.scratch/parallel-point-leaf-work/assets/point-leaf-seam-prototype.html`.

## Answer

Do not add a routed point-leaf plan, a stateful point-leaf workflow, or a domain-aware foreground executor. Keep transaction phase policy at its current loop sites.

Add one reusable `join_all_bounded` interface to `glassdb-concurr`. It is a semantic variant of `futures::future::join_all`, not a source fork. It applies the contract in [Define the bounded distinct-leaf execution contract](01-define-bounded-distinct-leaf-execution-contract.md): foreground polling, at most `N` incomplete futures, stable admission and output order, all inputs run, and no task-per-leaf spawning. Waiting futures count against the limit. The interface has no `Park`, `Replace`, terminal-outcome, or cleanup protocol.

This shared seam earns its place because deleting it would repeat bounded admission, stable output collection, zero/one fast paths, and cancellation behavior at every loop. Input combination does not cross this seam because its rules differ by domain.

Put combination behavior in the domain interface that understands why inputs belong together:

- `TreeRouter::group_keys_by_leaf` owns path-batched descent, B-link correction, convergence on one path, and stable `RoutedLeafGroup<T>` output. This output includes a leaf observation but does not become a shared point-leaf plan.
- `KeyResolver::effective_point_states` accepts the complete point-key set and owns logical resolution against grouped leaves.
- `NodeStore` should accept a set of retained leaf observations when physical validation can share or remove duplicate checks.
- `KeyLocker` accepts the access set or held leaf set. It owns per-leaf intent grouping, bounded multi-leaf execution, hold-and-wait, retry release, write-back rerouting, held-lock bookkeeping, and the sorted serial fallback.
- `ShardCoordinator::coordinate` accepts one complete operation for one transaction on one exact leaf path. It keeps compatible cross-transaction merging and one ordered mutation stream per path. Do not add a shallow `coordinate_all` method that only calls the generic bounded join. Do not send raw keys or separate same-transaction operations for one leaf across this seam.

The current modules call `join_all_bounded` only after their domain interface has selected the correct work units. Exceptional conflict waits and split rerouting can remain slower inside their owning module. This keeps the normal independent-leaf path small and follows the optimistic-concurrency principle.

Do not add a timed collection delay to `ShardCoordinator`. Keep its existing scheduler yield for a cache-served first load and its natural backend-I/O collection window.
