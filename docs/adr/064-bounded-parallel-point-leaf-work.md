# ADR-064: Bounded parallel point-leaf work

## Status

Accepted.

## Context

Independent point-access work on distinct leaves ran in serial phases. This
made backend wait time grow by one wave for each leaf. Unbounded parallel work
would remove these waves, but it could submit an unsafe amount of work and
would move physical grouping rules into a generic concurrency module.

## Decision

Use one foreground bounded join that admits at most a nonzero number of
incomplete futures, runs every supplied input unless the join is dropped, and
returns outputs in stable input order. Waiting futures consume the bound. Zero
and one input use direct paths. The join does not spawn a task for each input.

Each domain module combines its own physical work before it uses the join.
`TreeRouter` combines point keys by routed path, `NodeStore` combines compatible
leaf-state checks, `KeyResolver` combines logical resolution by routed leaf
group, and `KeyLocker` combines atomic leaf mutations. There is no shared
point-leaf plan or domain-aware executor.

Add one nonzero `EngineConfig::transaction_leaf_parallelism` value with an
initial value of 16. Copy this value to each point-leaf provider as
`parallelism` when the engine graph is built. Domain methods do not accept a
per-call limit. GlassDB does not add a database-wide or process-wide backend
scheduler; the backend owns aggregate queueing, connections, retries, and
provider throttling.

Normal lock acquisition keeps physical locks across complete access-set
retries. The leaf state is the retry memory; partial receipt sets and
foreground release are not control state. A successful full pass creates one
complete `LockedTx`. [ADR-065](065-renewed-transaction-identity-on-serial-fallback.md)
defines the transition to sorted serial acquisition.

Committed write-back bounds original `LockedTx` groups. Split descendants stay
serial inside their original bounded position. Every original group runs, and
a local failure or deferral does not change the committed transaction result.

## Consequences

For `L` independent cold leaves and limit `N`, a phase needs approximately
`ceil(L/N)` backend-wait waves. One-leaf work adds no backend operation. Same-
leaf ordering, transaction phase ordering, and sorted serial acquisition remain
unchanged.

The value is a transaction-local submission bound, not an aggregate capacity
guarantee. Concurrent transactions can each admit up to the configured number
of incomplete leaf futures. Backend and deployment stress tests must therefore
verify the initial value of 16.
