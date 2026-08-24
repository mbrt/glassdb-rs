# Choose the point-key batch routing design

Type: prototype
Status: resolved

## Question

Which concrete batch-routing design gives bounded parallel descent for point keys while reading each required tree node as few times as possible, preserving stable leaf groups, correcting stale routes through B-link right links, and adding no backend operation to the one-key path? Compare concurrent per-key descent, tree-aware breadth-by-node descent, and any materially different design with a small prototype and operation-count evidence.

## Prototype

[Point-key routing logic demo](../assets/point-key-routing-prototype.html) compares serial independent descent, bounded independent descent, path-batched wave descent, and a sorted leaf-chain sweep. It shows backend reads, backend waves, stable leaf groups, cache state, and B-link right-link correction.

The validated primary source is on local throwaway branch `prototype/point-key-routing-wave-descent`, commit `ccfac476`, at `.scratch/parallel-point-leaf-work/assets/point-key-routing-prototype.html`.

## Answer

Choose path-batched descent for two or more point-key items. Keep the existing direct descent for zero or one item. The batch design is internal to point-leaf planning and does not add a public batch point-read interface. It does not change the sorted serial lock fallback.

A pending routing batch contains one current node path and its ordered point-key items. At most `N` distinct node-path loads are active. A path is loaded once at its required currentness, then all items for that path use the same node and observation:

1. Items that the node does not own move to its B-link right-sibling path.
2. Items that an index node owns are partitioned by child path.
3. Items that a leaf owns are complete when the leaf has the required currentness.
4. If the interior and leaf requirements differ, reload the terminal path at the leaf requirement. Process the result again because a refreshed former leaf can be an index.

Combine pending batches that reach the same path before admission. When one load completes, its child and right-sibling batches can enter the same bounded ready set. The implementation does not need a global level barrier; the prototype uses backend waves only to make causal depth and operation counts visible. Poll all routing work in the caller's task and apply the accepted bounded execution contract to admission, cutoff, and draining.

Retain each item's original ordinal. Return `LeafGroup` values in stable object-path order and keys in original input order inside each group. Retain one leaf observation for each final path. If several started path loads fail, select the error for the smallest affected input ordinal, with object path as the tie-break. Preserve collection-absence classification for that item.

This design keeps self-correcting routing. A stale index can send an item too far left; the loaded node high key moves that item to the right sibling. Batches that converge on that sibling combine. A stale cached root that looked like a leaf is reprocessed after its terminal refresh, so an index is never returned as a leaf.

The operation model gave this evidence with a cold cache and `N = 4`:

| Scenario | Bounded independent descent | Path-batched descent | Sorted leaf-chain sweep |
| --- | ---: | ---: | ---: |
| One key | 3 reads / 3 waves | 3 / 3 | 3 / 3 |
| Eight keys in one leaf | 3 / 3 | 3 / 3 | 3 / 3 |
| Sixteen clustered keys in four leaves | 9 / 9 | 9 / 3 | 9 / 9 |
| Four sparse keys | 9 / 3 | 9 / 3 | 10 / 10 |

Same-path store coordination prevents bounded independent descent from adding physical reads, but a key-count limit can let clustered keys occupy all slots on one branch. Path batches spend the limit on distinct paths and remove that serialization without extra reads. The stale-separator case also routed to `L0`, `L1`, and `L2`: the first leaf high key moved the misrouted item to `L1`.

Reject the sorted leaf-chain sweep for point access. Right-sibling discovery is causal and serial, and sparse keys make it read leaves that own no requested key. Keep it only for the existing ordered and range operations that are outside this design.

The later measurement plan must check real latency and throughput. This prototype decides the routing structure; it does not choose `N` or replace the required benchmark acceptance checks.
