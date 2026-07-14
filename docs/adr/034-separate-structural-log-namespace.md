# ADR-034: Separate structural-log namespace

## Status

Accepted — implemented.

Refines only the structural-record placement in
[ADR-032](032-node-locking-and-coordinated-splits.md).

## Context

Transaction records and split-recovery records have different schemas,
cardinality, and lifecycles. Transaction records are finalized by commit or
abort and reclaimed by transaction GC. Structural records are short-lived
write-ahead notes whose outcome is decided from tree reachability and which need
an independent recovery cadence.

Embedding structural records in `_t` couples transaction status, GC, and split
recovery even though transaction status is not authoritative for a split's
outcome.

## Decision

- `_t/<txid>` contains only transaction status, values, lease, and lock
  back-references.
- A split's wound-wait identity is ephemeral and does not create a `_t` record;
  the tree and its `_s` record are the durable authority for split progress.
- A split writes a minimal record at the database-wide
  `_s/<record-id>` namespace before creating any node, and deletes it after the
  created nodes are published or reclaimed.
- The splitter owns `_s` recovery. It runs immediately at startup, retries live
  records on the transaction-liveness cadence, and backs off empty listings
  independently of split-candidate processing.
- Recovery fences the source's structure writer with a CAS before classifying
  created nodes as unreachable, so an in-flight shrink cannot race reclamation.
- This is a greenfield format: no migration or compatibility path is retained
  for embedded structural records.

## Consequences

Transaction logging, transaction GC, and structural recovery have independent
schemas and lifecycles. Recovery can list only the low-cardinality in-progress
split set, at the cost of one additional database-wide object namespace.
