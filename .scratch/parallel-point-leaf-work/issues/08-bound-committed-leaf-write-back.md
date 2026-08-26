# Bound committed leaf write-back

Type: grilling
Status: resolved
Blocked by: 01, 03, 04

## Question

How should `KeyLocker` use `join_all_bounded` for the original committed `LockedTx` groups while keeping split rerouting inside its domain interface, remaining best-effort and deterministic, preserving cancellation and shutdown safety, aggregating superseded transaction hints, and allowing independent leaves to finish when one leaf is deferred by structural work?

## Answer

`KeyLocker::write_back` supplies the original committed `LockedTx` groups to
`join_all_bounded` in stable leaf-path order. `KeyLocker` uses its
`parallelism`, which is the same value that it uses for normal acquisition and
that it received from `EngineConfig::transaction_leaf_parallelism` at
construction. Each input carries one group's path, ordered intentions, and
installed or observed lock-proof lower bound. The generic zero- and one-input
paths apply, so one original group does not construct the multi-future queue or
add a backend operation. Do not spawn one task per leaf.

One bounded position owns one original group for its complete lifetime. Its
first `ShardCoordinator` operation uses the original target and the proof's
currentness lower bound. If the coordinator returns `Reroute`, discard that
target and use `TreeRouter` to route only the affected intentions. Use cached
interior nodes and require the terminal leaf observations to satisfy the proof
lower bound. Process the resulting routed leaf groups serially in stable
leaf-path order inside the same bounded position. Thus one old group can become
several current groups without nested leaf-write concurrency or replacement
work in `join_all_bounded`.

Repeat that domain-owned rerouting until each affected current leaf is released,
structurally deferred, or fails. Do not add a total reroute limit, a delayed
scheduler, or successor background work. This keeps ordinary convergent
write-back and avoids durable holders that cause repeated transaction-object
reads and prevent reclamation. A shutdown can wait for ordinary backend or CAS
convergence. A live structural gate returns `Deferred` promptly for only its
current leaf; continue its routed siblings and every other original group.

Failure is also local to the current leaf. Keep superseded transaction hints
and other successful results already produced by the original group, record the
failed path for diagnostics, and continue its known siblings. A routing failure
ends only the affected intentions because their current targets are unknown.
The outer bounded join still runs every original input. Write-back does not
return these failures as an error after the transaction commit point. Panics
remain uncaught as required by the bounded execution contract.

After the bounded join completes, flatten successful superseded-transaction
hints in stable original-path, rerouted-path, and logical-key order. Preserve
duplicates and use the existing single `TxCleanupHints::schedule_all` call; the
hint queue keeps its existing drain-time deduplication. Do not schedule from
individual concurrent futures, because completion order is not deterministic.

Keep the complete pass in one `Background::spawn_waited` task. Dropping the
bounded join drops both admitted and stored group futures. Existing coordinator
cancellation handoff protects merged callers, while the committed transaction
object remains the durable authority for any unfinished publication. Lost
cleanup hints are safe because the paged GC scan is complete. Graceful shutdown
drains this task before it closes `ShardCoordinator`; the inline fallback uses
the same bounded interface and semantics.
