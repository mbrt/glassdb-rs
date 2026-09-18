# ADR-072: Leaf merging from direct-commit hints

## Status

Accepted — implemented.

Extends [ADR-031](031-dynamic-range-sharding.md) with recoverable leaf merges.
Retains the inline-pressure split policy in
[ADR-056](056-demand-driven-inline-pressure-splits.md) and
[ADR-061](061-atomic-logless-single-leaf-commits.md).

## Context

Merging adjacent leaves can let a transaction commit directly when its point
accesses previously spanned both leaves. It also combines their inline demand
and can undo a useful split. The first version needs a small merge heuristic
that preserves existing split benefits and avoids expensive measurement in
normal transaction execution.

## Decision

### Merge policy

Keep size-based and inline-pressure median splits. Process split work first;
merge hints and waiting intervals never veto a split.

Use bounded local counts of missed direct commits. Reporting uses existing
routing observations and adds no foreground backend requests or transaction
lifetime tracking. Maintenance owns hint selection; the merge mechanism owns
publication and recovery.

The algorithm is:

1. Report a hint when an otherwise eligible point access set routes to exactly
   two standalone leaves in one collection. Count distinct transaction identities
   and retain the largest reported output size. Bound and expire these records.
2. Require repeated hints and a local waiting interval. Keep all policy state
   in memory; a restart requires fresh evidence and another wait.
3. Evaluate a bounded number of candidates after split work. Confirm adjacency
   and require spare entry, encoded-content, and inline capacity. Include all
   existing inline bytes and the largest hinted output without replacement
   credit. Recheck capacity against the source snapshots used for publication.
4. Consume the evidence for each attempt, including rejected or failed attempts.
   Another attempt requires fresh evidence and another wait.

More precise metrics can replace this heuristic without changing the merge
mechanism. Nodes contain no merge-policy metadata, and no policy records are
shared through object storage.

### Recoverable merge mechanism

Merge two adjacent leaves into the left leaf with this algorithm:

1. Record a structural intent and register a topology participant. Acquire the
   right structural gate and resolve its existing holders.
2. Read the left leaf without acquiring a gate. Skip the merge if it has holders,
   a structural gate, or a collection-delete intent. Check adjacency and capacity,
   then record both exact source revisions in the intent.
3. Bind the right gate to the intent. In one CAS against the recorded left
   revision, copy the union into the left leaf and install its intent-owned gate.
   Left writes can continue until this CAS; a conflict cancels the optional merge.
4. Redirect the right leaf to the left, release the left gate, and remove the
   intent. Recovery completes these steps if the worker stops.

An intent-owned gate survives its transaction owner's final status. Only
structural recovery can release it. The left gate proves that the merge applied;
cancellation must permanently fence that write before reopening the right leaf.
A changed revision alone is insufficient: temporary locks can restore the same
bytes on backends with content-based version tokens. Cancellation advances the
left membership generation unless a later structural change already fences the
write. If publication wins this race, recovery completes the merge instead.
This uses one structural gate mechanism for exclusion and recovery, without
separate merge pins or gate levels.

The transaction layer handles gate admission and retries for both splits and
merges. An intent-owned gate requests structural recovery and a fresh retry
through ordinary coordination. Storage returns the observed node without
waiting. Reads can continue while recovery is pending.

Installing an ordinary structural gate advances the membership generation;
release preserves it. This prevents cleanup from restoring earlier node bytes
and admitting delayed gate acquisitions on backends with content-based version
tokens. It can cause conservative scan and absence validation retries.

Preserve every authoritative inline value. Reads can continue during the merge.
After publication, the left gate prevents writes until the right redirect is
installed, so both copies of each imported value remain identical. Collection
deletion settles pending structural work before reclaiming nodes.

Retain the right identity as a redirect until collection deletion. This supports
stale routing and split recovery without a separate redirect reclamation
protocol. Routing refreshes stale links that lead back to the retained left
leaf; scans preserve their lower bound when following that redirect. Parent
contraction is deferred.

### Compatibility

This requires database format v4, with separate split and merge intent payloads.
Clients must preserve intent-owned structural gates and redirects. This version
rejects v3 databases and does not provide automatic migration.

## Consequences

- Existing inline-pressure hints can split a merged leaf when later demand
  needs more capacity. Fresh evidence and waiting intervals limit repeated
  split/merge cycles within one instance without blocking those splits. Waiting
  intervals are not coordinated across instances.
- Hints count attempted opportunities, including attempts that later abort.
  They do not predict total backend work or demand from other database instances,
  so a merge is not a guarantee of better performance.
- Durable redirects simplify recovery but add storage and cold routing reads.
  Repeated merges can create longer redirect chains.
- Separating the heuristic from publication keeps later policy changes local
  and preserves the merge correctness guarantees.

## Alternatives considered

- **A full transaction cost model:** more measurement overhead and state than
  the first version needs. Defer it until measurements justify the complexity.
- **Merge small leaves without demand evidence:** can undo useful pressure
  splits without creating direct-commit opportunities.
- **Shared demand summaries:** broader visibility requires extra backend work
  and a policy for delayed or incomplete observations.
