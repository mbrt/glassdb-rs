# Bound committed leaf write-back

Type: grilling
Status: open
Blocked by: 01, 03, 04

## Question

How should `KeyLocker` use `join_all_bounded` for the original committed `LockedTx` groups while keeping split rerouting inside its domain interface, remaining best-effort and deterministic, preserving cancellation and shutdown safety, aggregating superseded transaction hints, and allowing independent leaves to finish when one leaf is deferred by structural work?
