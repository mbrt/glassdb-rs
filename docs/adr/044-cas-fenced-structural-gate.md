# ADR-044: CAS-fenced structural gate

## Status

Proposed.

This would supersede [ADR-032](032-node-locking-and-coordinated-splits.md)'s
requirement that every node mutation and escalated scan hold a shared structure
lock. ADR-032's split linearization, right-link, membership-lock, structural
recovery, and hard-cap decisions remain unchanged.

## Context

ADR-032 gives splits priority over hot-node traffic by making every node
mutation hold structure-R through write-back while a split takes structure-W.
That protocol prevents an unbounded stream of mutations from starving the
split, but makes a rare structural concern part of every stable-leaf
transaction.

In particular, an ordinary lock acquisition must reconcile the node's entry
and node-lock holders before it can establish structure-R. Each mutation also
adds a durable structure holder and transaction-log back-reference, retains
them through write-back, and later removes them. Measurements that separated
stable-leaf traffic from forced splits found that this holder reconciliation,
rather than routing or the split itself, dominates the regression.

The fix must remove that stable-path cost without returning to ADR-031's
lock-free split starvation. It must also remain correct across independently
opened `Database` instances, preserve phantom-safe membership locking, and stop
a delayed write-back from racing a structural change. Introducing structural
MVCC solely for this coordination would be disproportionate.

## Decision

### Make structure locking exclusive

Treat the node's structure lock as a persisted, exclusive **structural gate**.
Only an operation that changes the node's shape holds it. Ordinary data
mutations and escalated scans no longer acquire structure-R or record a
structure-lock back-reference.

The node protocols become:

| operation | structural requirement | other authority |
| --- | --- | --- |
| point read | none | none |
| overwrite | gate absent | per-key write |
| create | gate absent | membership-W and per-key create |
| delete | gate absent | membership-W and per-key write |
| escalated scan | gate absent | membership-R |
| node-shape change | hold the gate | its structural writes |

The gate cross-conflicts with both membership-R and membership-W. This keeps an
escalated scan or membership mutation from spanning a split without requiring
a shared structure holder. Non-escalated scans retain ADR-032's optimistic
covered-leaf validation.

Every conditional rewrite of a node that does not change its shape must observe
the gate absent in the state it conditionally replaces. This includes
foreground mutation, write-back, help-forwarding, release, lease reclamation,
and maintenance. Point reads remain gate-free because ADR-031's right-links and
self-correcting descent already tolerate a concurrent split.

Stable operations resolve only the entry holders they touch and the membership
lock when they use it. Full-node holder reconciliation is reserved for
structural-gate acquisition.

### Acquire the gate by quiescing one node

A structural operation loads the full node, resolves every live per-key and
membership holder under the existing wound-wait and transaction-status rules,
and conditionally installs the gate together with the resolved state. It waits
for an older pending holder, wounds a younger one, helps a committed holder
forward, and removes an aborted holder. It does not install the gate while any
holder can still perform a later rewrite of that node.

Gate installation and ordinary node mutation use the same backend conditional
write boundary. For a mutation `M` and gate installation `G` on one node:

- if `M` lands first, `G` loses its precondition, reloads the node, and
  reconciles `M`'s holder; and
- if `G` lands first, `M` loses its precondition, then observes the gate and
  waits or reroutes.

Therefore a successful `G` establishes node quiescence without enumerating
shared structure readers. This ordering comes from the strongly consistent
conditional backend required by ADR-042 and ADR-043, not from process-local
serialization, so it also covers concurrent `Database` instances.

The structural operation retains the gate through ADR-032's source shrink CAS
and then releases it. Parent publication remains a separate, independently
gated follow-on, and the shrink CAS remains the split's linearization point.

### Converge delayed write-back explicitly

A write-back using a pre-gate observation cannot mutate a gated node: its
conditional write either precedes gate installation or fails. After reloading,
it may finish without another write only when routing and the current node state
prove that its holder is gone, because successful gate acquisition has already
resolved it. A holder that is still present must be resolved normally; it is
not silently discarded.

This rule makes late write-back converge after help-forwarding while preventing
a committed value from being lost merely because a gate appeared. It applies
equally after cancellation or another instance's structural operation.

The structural gate is a restriction of the existing structure-lock concept,
not a new lock kind or object-format field. The backend object's conditional
revision is the ordering token; no persisted structural epoch is added.

## Discarded options

### Keep shared structure holders and optimize reconciliation

More selective status reads would reduce part of the current cost, but every
stable mutation would still create, log, retain, and remove a holder for a
structural event that is normally absent. A split would also continue to depend
on enumerating those shared readers. This retains the protocol coupling that
caused the regression.

### Return to a lock-free split

Removing structure-R without an exclusive gate restores ADR-031's failure mode:
write-back and foreground CAS traffic can repeatedly beat the split's shrink
CAS. It also provides no boundary at which all earlier holders are known to be
resolved.

### Serialize all mutations on a node

A process-local serializer would unnecessarily order disjoint key mutations and
would not coordinate other `Database` instances or recovery after a crash. A
persisted exclusive gate only during structural work provides the required
cross-instance boundary while retaining normal transaction concurrency.

### Add a structural epoch or structural MVCC

Validating a separate generation on every mutation could order it against a
split, but would add persisted state and another universal validation concern.
The conditional replacement of the node already orders a gate against every
competing rewrite, while ADR-031's changed shape prevents a pre-split mutation
from applying to the post-split source.

### Treat every stale write-back as complete

A failed pre-gate write does not by itself prove that the committed value was
published. Convergence is safe only after current routed state proves the holder
was removed by gate acquisition or another helper.

## Consequences

- Stable-leaf transactions no longer pay for a structure holder, its log
  reference, or full-node status reconciliation; their work is proportional to
  the entries and membership state they actually touch.
- Gate acquisition deliberately performs full-node reconciliation. Structural
  operations become heavier but remain rare, and once the gate lands, newer
  traffic cannot starve their structural CAS.
- Membership mutations and escalated scans retain phantom safety and cannot
  overlap a split on the same node.
- The quiescence proof applies across processes without serializing disjoint
  ordinary transactions.
- Write-back and maintenance paths must distinguish proven convergence from a
  holder that still requires resolution.
- ADR-032's one-node-at-a-time structural interval, object-size headroom, split
  recovery, and right-link behavior remain in force.
- Reusing route observations, removing duplicate root or shard loads, retry
  tuning, and split-threshold tuning are separate performance work.
