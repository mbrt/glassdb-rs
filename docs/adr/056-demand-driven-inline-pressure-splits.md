# ADR-056: Demand-driven splits for inline admission pressure

## Status

Proposed.

This extends [ADR-031](031-dynamic-range-sharding.md)'s background split policy
and resolves the capacity follow-up left by
[ADR-054](054-reserve-inline-publication-for-logless-commits.md). It does not
change [ADR-051](051-inline-latest-values.md)'s inline representation,
admission budgets, or direct-commit correctness contract.

## Context

ADR-051's aggregate inline budget bounds the value bytes rewritten with every
leaf mutation. Once authoritative inline values consume that budget, another
otherwise eligible direct commit falls back to the regular locked protocol.
ADR-053 deliberately made that the sole fallback, and measured it as materially
slower than a landing direct commit.

ADR-054 stopped logged values from consuming new inline capacity, but it could
not remove authoritative inline values. A leaf can therefore remain saturated
while still being well below ADR-031's ordinary entry-count and encoded-byte
split thresholds. Its stable admission failures do not currently cause the tree
to create more capacity.

Lowering the global split thresholds would widen every tree, including leaves
that never need more inline capacity. Conversely, splitting in response to one
failed admission changes durable shared topology for the benefit of a
best-effort optimization. There is no merge protocol yet, so that change is not
automatically reversed when demand subsides.

## Decision

### Treat aggregate rejection as demand for another leaf

When a direct publication is rejected only because the target leaf lacks
aggregate inline headroom, it requests a background split. One observed
rejection is sufficient evidence of demand.

Only potentially recoverable aggregate pressure does so. Disabled inlining, a
value above the per-value limit, and a value that cannot fit within the
aggregate budget even in an otherwise empty leaf continue directly to the
locked protocol without requesting a split. Exact encoded-object capacity
remains ADR-031's separate size-based concern.

The rejected mutation does not wait for structural work. It immediately uses
the regular locked fallback, with the same outcome and in-doubt rules as before.
The split request is detached from that transaction: cancellation does not
retract it, and observing pressure while classifying an in-doubt direct
mutation may still request it without changing the user-visible result.

### Reroute and revalidate the demand

A pressure request identifies the target key and the headroom that failed, not
just the leaf path observed by the mutation. Before acting, the splitter routes
the key through the current topology and rechecks aggregate admission against
the current owning leaf. This prevents a stale request from splitting the wrong
side of an intervening root or leaf split.

Revalidation asks only whether the observed capacity demand remains. It does
not reconstruct the failed transaction's complete direct-commit eligibility:
the locked fallback may itself have changed the writer or temporarily installed
a holder without invalidating future demand for inline headroom.

A request is no longer actionable when the key has disappeared, admission now
fits, the requested value cannot fit an otherwise empty leaf, or the owning
leaf cannot be divided. Such a request is discarded rather than retried.

### Perform one ordinary split per request

An actionable request performs at most one successful ADR-031 median split.
The split point remains balanced by entry count; inline-byte-aware partitioning
is deferred.

One median split need not create enough headroom for the target. Success
nevertheless completes that request. If real demand encounters aggregate
pressure again, it supplies another request and may drive another split.
Transient failures before a split succeeds retain ADR-031's bounded,
best-effort retry behavior.

The request itself is volatile and uses an independent background identity. A
crash before structural work starts loses it, and a later admission failure can
recreate it. Once structural work starts, ADR-031's existing durable,
recoverable split protocol applies unchanged.

### Keep topology policy operational and observable

Pressure requests are bounded and coalesced like ordinary split hints. Dropping
one affects only future direct-commit coverage; it cannot affect correctness.

All clients of one database are expected to use consistent `InlinePolicy` and
`SplitPolicy` settings. Persisting or enforcing that agreement is deferred.
Inconsistent clients remain safe, but may make conflicting performance choices
and permanently reshape the shared tree according to whichever client requests
a split.

Ordinary capacity splits and inline-pressure splits must be attributable
separately. Completed work, transient deferral, and requests discarded as no
longer actionable must also be distinguishable for performance evaluation.
This is an observability requirement, not a prescribed public statistics API.

## Consequences

- Inline capacity grows where failed direct commits demonstrate demand instead
  of through a globally lower leaf threshold.
- The mutation that discovers pressure still pays for the locked fallback.
  Splitting can improve only later similar mutations.
- One failure may permanently widen the tree. More leaves mean more structural
  work, parent entries, routing state, and cache entries; the absence of merging
  makes this an explicit trade-off.
- Median splitting preserves ADR-031's balancing policy and implementation
  model, but may move mostly external entries and provide little immediate
  inline relief. Repeated demand is required to drive further splits.
- The optimization provides no eventual-admission guarantee. Volatile hints,
  transient contention, an unsplittable leaf, or an unhelpful median can leave
  future mutations on the locked path.
- Direct-commit, transaction, node, and structural-log formats are unchanged.
  No correctness state or reclamation obligation is introduced.
- Client-local tuning already influences shared topology through `SplitPolicy`;
  inline pressure adds another reason consistent configuration matters.

## Alternatives considered

### Lower the ordinary split thresholds

This may create inline headroom, but widens trees based on entry count or
encoded bytes even when no direct publication needs the capacity. Failed
admission is a more specific demand signal.

### Wait for a split and retry the same direct mutation

This could avoid the first locked fallback, but couples foreground latency and
transaction cancellation to a multi-step background structural protocol. It
also needs a progress policy when splitting is delayed or insufficient. The
locked protocol already provides bounded semantic progress.

### Split repeatedly until the requested value fits

One request could then cause several irreversible topology changes after its
mutation has already completed through fallback. Requiring another real
failure before each additional split bounds structural amplification by
continued demand.

### Choose a split point by inline bytes

A pressure-aware separator could create headroom in fewer splits, but conflicts
with entry-count balance and introduces another tree-shape policy. Retaining the
median isolates this decision; load-aware splitting can be evaluated
separately.

### Hint only the originally observed path

This is smaller than a key-directed request, but concurrent topology changes can
move the target before the background worker runs. Splitting the stale source
may do nothing for the demand that justified it.

### Require repeated failures before the first split

Hysteresis reduces permanent growth from one-off demand, but requires another
stateful threshold and deliberately preserves a known slow fallback phase.
Authoritative inline saturation is sufficiently durable that one failure is
accepted as the initial signal.

### Demote existing inline values under pressure

An authoritative inline value may have no transaction object, so demotion can
destroy the only durable copy. Adding provenance and externalization would be a
different value-lifecycle protocol, not a split policy.
