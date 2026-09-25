# ADR-073: Merge nodes into their right sibling

## Status

Accepted — implemented.

Refines [ADR-031](031-dynamic-range-sharding.md) by adding merges to the B-link
topology, and extends its background split policy into one maintenance policy
for splits and merges. It also replaces ADR-031's separator publication with one
parent reconciliation step that splits and merges share.

Refines [ADR-032](032-node-locking-and-coordinated-splits.md)'s structural
recovery: right links are no longer only added, so reachability of a created
node is no longer monotonic. It also defines the membership generation of a
merged leaf.

Refines [ADR-044](044-cas-fenced-structural-gate.md) with a polite gate
acquisition for merges and a merge reservation that transaction status cannot
revoke. Every installation of a structural gate now advances the membership
generation, so that late writes cannot land after a recovery fence. This also
applies to splits. Refines [ADR-049](049-participant-owned-topology-intents.md) with a
merge variant of the structural intent.

Refines [ADR-062](062-splitter-driven-tombstone-reclamation.md): merges compact
holder-free tombstones of their source and target, underfull leaves become merge
candidates, and a structural gate installation also advances the membership
generation.

This is a protocol-incompatible change and establishes database protocol v5.

## Context

The topology of a collection tree only grows. After many deletes, leaves stay
underfull. Scans read more leaves than necessary, fewer transactions have all
their keys in one leaf for a direct commit, and cold tombstones stay forever
(ADR-062).

The split policy has three fixed causes: soft cap, capacity, and inline
pressure. Future signals, such as contention or direct commits that miss because
their keys are in different leaves, need one place that can decide splits and
merges.

The existing protocol sets these constraints:

- A node stores a high key and a right link, but no low key. Routing only moves
  right.
- Any operation can remove a structural gate whose holder has a final status.
  This is safe for a split, because the split linearizes with a revision-guarded
  CAS on the gated source.
- Transactions wait for gate holders and never wound them. A structural
  operation that holds a gate and waits for another claim can cause a deadlock.
- Split recovery proves that a split landed from reachability of its created
  nodes. ADR-032 accepts this proof because right links are only added.
- ADR-043 allows a conditional write to land arbitrarily late, when its
  predicate is true again, and revisions can repeat. Today, a gate installation
  followed by a gate removal can restore the exact earlier bytes. A late copy of
  the gate write can then land again, and after it a CAS that expects the gated
  revision. This affects the recovery fence of splits too.

## Decision

### Drain the left node into its right sibling

A merge moves the key range and all entries of a source node L into a target
node R. R is the first node on L's right-link chain that is not drained. L and R
are at the same level. The tree root is never a source or a target, so the tree
height never decreases.

A merge uses the same steps as a non-root split:

| Step | Split                                        | Merge                                                             |
| ---- | -------------------------------------------- | ----------------------------------------------------------------- |
| 1    | Preparing intent, join topology              | same                                                              |
| 2    | Gate the source, quiesce, compact            | Gate L politely, compact L                                        |
| 3    | Ready: source revision, split key            | Ready: L's revision, R's token, boundary `b` (L's high key), R's membership generation |
| 4    | Create the sibling                           | Absorb: one CAS on R compacts R, and adds L's entries, L's low key, and the merge reservation |
| 5    | Shrink the source (linearization point)      | Drain L (linearization point)                                     |
| 6    | —                                            | Remove R's merge reservation                                      |
| 7    | Reconcile the parent (optional)              | same                                                              |
| 8    | Delete the intent, leave topology            | same                                                              |

Before the Ready step, the worker checks the merge decision again against the
compacted L and the current R. If the check rejects the merge, the worker stores
the compacted L and cancels the merge, as ADR-062 does for splits.

The absorb lands only while R has the membership generation recorded at Ready.
It stops the merge and releases L's gate if R is drained, if R has a different
membership generation, or if R has a live structural gate, a merge reservation,
or a drop intent. It also stops the merge if the merged content does not fit in
the hard cap. It removes a structural gate of R whose holder has a final status,
as any gate acquisition does. If R changed but kept its membership generation,
the worker retries the absorb against the new state of R.

The absorb also removes the holder-free tombstones of R. It does not need R's
gate for this: the CAS expects the exact state of R, so it removes only
tombstones that have no holder when it lands. ADR-062 needs the gate to quiesce
a split source, not to compact it. The compaction stays if the merge is
abandoned, because it does not change the live key set. After the absorb lands,
the writers of the removed tombstones become GC hints, as in ADR-062.

The drain is one CAS on L that expects the revision recorded at Ready. If it
fails, L changed, and the drain can never land. The worker then abandons the
merge: one CAS on R removes the entries below `b` and the merge reservation, and
sets R's low key back to `b`. Then the worker deletes the intent.

### Low keys

Every node stores its low key, the inclusive lower bound of its range. The low
key of the first node at a level is empty. A split gives the new sibling the
split key as its low key, and a root split gives the first child an empty low
key. Only a merge changes the low key of an existing node: the absorb sets R's
low key to L's low key, and an abandoned merge sets it back to `b`.

### Drained nodes

A drained node keeps its level, has an empty body, stores an explicit drained
marker, and has a right link to R. It covers no key, so routing and scans go
past it with the existing right-link logic. Its key range, body, and right link
never change again. A collection drop can still fence it like any other node.

A drained node has no membership domain. Escalated scans take no membership lock
on it, and no structural change starts on it.

Drained nodes stay until their collection is dropped. Stale routes therefore
never find a missing node, and no late write can create the token again.

A left neighbor keeps its right link to a drained node until a later structural
change of that neighbor rewrites the link past the drained node. Phase 1 has no
separate pass that shortens right links.

### Merge reservation

The merge reservation is a durable mark on R that names the structural intent of
the merge. It is not a claim: a transaction status does not decide its meaning.
Only the merge worker or the structural recovery of that intent can remove it.

The reservation blocks every other structural change of R: split, compaction,
drain, a merge into R, and parent reconciliation of R. These
operations wait or retry. They do not hold another claim while they wait. Data
operations on R ignore the reservation.

Nobody else can remove the reservation because R's copies of L's entries become
safe to discard only when the drain can no longer land, and safe to use only
after it landed. Only the intent can decide which of the two is true.

### Fence late writes with the membership generation

Installing a structural gate or a merge reservation advances the membership
generation of the node in the same CAS. Removing either one keeps the
generation. So the state of a node before an installation never comes back, and
a late copy of the installation CAS always fails. After a gated node leaves its
recorded revision, no CAS that expects that revision can land.

This makes the recovery fence sound for a split source, the tree root, L, and R,
also on backends whose revisions depend on content. It reuses the existing
generation instead of a new node field.

### Why the merge is correct

Every route that reaches a live node N for key `k` satisfies `k >= low(N)`. A
merge only decreases the low key of its target, and a split only decreases the
high key of its source. Parent reconciliation and a root split create a route to
N only for keys at or above the low key of N at that time. An abandoned merge
sets the low key of R back to `b`, but no route reaches R below `b` before a
drain. So the property stays true.

Before the drain, no route reaches R for a key below `b`. Point operations
cannot read R's copies of L's entries, scans ignore them (see below), and the
gated L is the authority for its range. The
drain changes L's revision at the same moment that R becomes the authority for
L's range. A transaction that read L fails the optimistic revision check and
resolves the key again at R, which holds the same entries.

L's gate stays revocable. If the worker's status is final, removing the gate
fences the drain in the same way as it fences a split shrink. The gate
installation advanced the generation, so L cannot return to the revision
recorded at Ready.

The merge never waits while it holds a claim: the gate acquisition is polite,
and the absorb does not wait. Other structural operations wait for a merge
reservation only when they hold no claim. So merges add no deadlock.

### Membership generation

At the absorb, R's membership generation becomes `max(g_L, g_R + 1)`. `g_L` is
the generation of the gated L, and the `+ 1` is the advance for the merge
reservation. An absence read of R's range from before the absorb fails
validation. An absence read of L's range made while L was gated recorded `g_L`.
It stays valid if `g_L` is the result, because L did not change after the read.
A generation never returns to an earlier value along the lineage of a key. If
the merge is abandoned, R keeps the new generation.

### Polite gate acquisition

A merge gets L's structural gate without waiting and without wounding. It
help-forwards committed holders and removes aborted holders. If any holder or
drop intent is still pending or unknown, it stops the merge and tries again
later. The planner reads L's claims before it writes the intent, so a skip
usually costs one read.

A merge is optional maintenance and must not abort user transactions. Index
nodes have no transaction holders, so this rule does not change index merges.
Other gate acquirers keep the ADR-044 behavior and can wound a merge worker. In
that case, the drain is fenced and the merge is abandoned.

### Parent reconciliation

One parent reconciliation step replaces separator publication. It makes one
range of a parent index node P agree with the right-link chain of its children.
A split runs it at its split key, and a merge runs it at its boundary `b`.
Recovery of a landed split or merge runs the same step.

The step gets P's structural gate and holds no other claim. For a key `k`, it
starts at the child that P names for keys just below `k`, and follows right
links until it reaches the node that covers `k`. On this path:

- it replaces each entry of P that names a drained node with the first node on
  that node's right-link chain that is not drained;
- for each node after the first one that is not drained, it adds a separator
  equal to the high key of the previous such node, if P does not have it; and
- it removes the second of any two adjacent entries that name the same child.

After the step, no entry of P on this path names a node that was drained before
the step. The step is idempotent. If P goes above a split threshold, P becomes a
split candidate, as with separator publication. If P becomes underfull, P
becomes a merge candidate.

The planner merges only adjacent children of one parent. The mechanism stays
correct if a concurrent split of the parent separates them.

### Recovery

A Preparing merge intent is recovered like a Preparing split intent.

For a Ready merge intent, recovery first fences L in the same way as a split
source. While L is at the recorded revision, recovery retries if the gate holder
is pending, and removes the gate if the holder has a final status. Then:

1. If L is drained and its right link is R, the merge landed. Recovery removes
   R's merge reservation if it is still there, reconciles the parent, and deletes
   the intent.
2. If not, the drain can no longer land. If R holds this merge reservation,
   recovery abandons the merge on R in the same way as the worker. If R does not
   hold it, is not drained, and still has the membership generation recorded at
   Ready, recovery advances that generation. Then recovery deletes the intent.

While R holds the reservation, no other merge can drain L into R. After the
reservation is removed, another merge can drain L into R. The actions of the
first case are idempotent, so they are also safe in that situation.

The generation advance in the second case fences a late absorb. A worker with a
final status can still run its absorb after recovery deleted the intent: a CAS
that is in flight, or a retry that reads R again. If that absorb landed, its
merge reservation would stay forever, because only the intent can remove it.
The in-flight CAS expects a revision of R from before the advance, so it fails.
A retry reads the new generation and stops. If R has a different generation, an
absorb cannot land, because a generation never returns to an earlier value.

Split recovery changes as follows. Each created node N has a test key `t`: the
split key for a non-root sibling and for the second root child, and the empty
key for the first root child. If N is drained, or if N's high key is at or below
`t`, the split landed. If not, N still covers `t`, and the existing reachability
test stays sound. Recovery never deletes a drained node.

This rule is sound because a node that was never linked receives no traffic and
no structural change, and neither condition can become true for it. A linked
node can lose its range to a merge, so reachability alone is no longer
monotonic.

### Detect stale copies of a merge target

A cached state of R from before the absorb is a real state of R, but it does
not include L's range. Routes reach R for L's range through the drained L and
through reconciled parent entries. An index copy of that state would send keys
of L's range to R's leftmost child, which is to the right of their owner. The
leaf high key covers them, so a transaction could write a key into the wrong
leaf. Validation routes through the same cached index nodes, so it does not
detect this. A stale read of a leaf copy would also report keys of L's range as
absent.

So every route or scan that reaches a copy of a node with a low key above the
key checks the node again. It reads the node at a currentness barrier allocated
after it observed the route. That read has a low key at or below the key: the
route exists only after a drain, the absorb set R's low key before the drain,
and a later abandoned merge only sets back an earlier low key. A copy that
passes the check is safe even if it is old. Splits do not need more: a split
creates its sibling before the shrink, so every copy of the sibling holds its
range.

Before the drain, R holds copies of L's entries below `b`. A scan reads all
entries of a leaf in its window, so a scan from L into R would return them
twice. The low key of R does not prevent this, because the absorb already
lowered it. A scan therefore ignores the entries of a leaf below the high key of
the previous live leaf on its path. After the drain, that key is the low key of
L, so the entries become visible in R.

### One maintenance policy for splits and merges

One candidate queue holds a node and a cause. The causes are soft cap, capacity,
and inline pressure for splits, and underfull for merges. New causes attach to
the same queue.

Each split cause also supplies a merge veto: the merged node must stay at or
below half of the threshold of that cause. Phase 1 vetoes use the leaf entry
limit, the soft byte limit, the capacity limit of the content, the index child
limit, and the aggregate inline budget. The absorb checks the hard cap
authoritatively. With these vetoes, a new
split cause cannot make nodes split and merge again and again.

Splits have priority over merges. Planner decisions are hints. The worker
checks them again under L's gate and at the absorb.

The underfull cause counts only live entries of a leaf, and children of an index
node. It comes from stored-leaf observations of the leaf coordinator, and from
the parent after a reconciliation. A leaf that holds only cold tombstones
becomes a candidate, and the merge compacts it under L's gate.

The vetoes count both nodes after compaction, because the merge compacts both.
If they counted the tombstones of R, then after a bulk delete each merge would
be vetoed. The last node of a level is never a merge source, so it would also
keep its tombstones.

Underfull thresholds are local to each database instance, like the soft split
thresholds (ADR-072). Benchmarks set their values. Instances with different soft
thresholds can undo each other's splits and merges. The results stay correct,
but structural writes increase while both instances write to the same nodes. The
documentation tells operators to use the same soft thresholds on all instances
of one database.

### Require database protocol v5

Nodes get a low key, a drained marker, and a merge reservation, and structural
intents get a merge variant. An older client would read a drained node as an
empty live node, would not check low keys, and would not advance the generation
at gate installation. The database
metadata version therefore advances to v5, so that older binaries
fail closed. There is no migration; development databases must be recreated.

### Deferred

- Root collapse, which would decrease the tree height.
- Reclamation of drained nodes before the collection is dropped.
- A background pass that shortens right links past drained nodes.
- Merge planning across parents.
- Other merge causes, such as direct commits that miss because their keys are
  in different leaves.

## Consequences

- The topology shrinks after deletes. Scans read fewer leaves, and more
  transactions have all their keys in one leaf for a direct commit.
- Each merge compacts the cold tombstones of its source and target. Leaves that
  are not part of a merge or a split keep their tombstones.
- The tree height never decreases.
- Each merge leaves one drained node until the collection is dropped. Drop and
  reclamation cost grows with them.
- Stale routes and scans take extra right-link hops over drained nodes until the
  links are shortened. The topology of a drained node never changes, so any
  cached copy routes correctly.
- A route that finds a cached copy of a merge target from before the merge
  reads the target again once. Each node stores one more key.
- Every structural gate or merge reservation makes absence reads of that leaf
  fail validation, and scans that cover it use their logical fallback. This also
  happens when the gate is only for compaction or the attempt is cancelled.
- The recovery fence of splits and merges stays sound on backends whose
  revisions depend on content.
- Structural recovery has a second intent kind, and split recovery has a new
  rule.
- Splits, merges, and their recovery share one parent maintenance step. A
  split's parent update also removes the entries that name drained nodes on its
  path.
- Merges skip leaves that have pending holders, so busy leaves merge rarely. A
  membership change of R between the Ready step and the absorb also stops the
  merge until a later sweep.
- A recovered merge that did not absorb can advance R's membership generation,
  so absence reads of R fail validation once more.
- Instances with different soft thresholds can do extra structural work.
- Database protocol v5 requires development databases to be recreated.

## Alternatives considered

### Merge the right node into the left node

L then grows at its right edge, where the high key already detects stale copies,
so nodes need no low key. L also rewrites its own right link, so the drained
node leaves the live right-link chain.

But routing moves right, so routes can reach L's right edge at any time: scans,
and routes from stale parents or stale left neighbors. If L's range grows before
R is drained, these routes read L's copies of R's entries while R is still live,
and R's revocable gate lets writes land in R that the copies do not have. So L's
range must grow at the same moment that R is drained, which changes two nodes.
This needs a gate that blocks writes until the merge ends or recovery runs, or a
pending range on L that each read above `b` resolves through R and the intent.
Routing also needs a rule to move left to L, and to read L again when its copy
does not cover the key.

In the chosen direction, routes reach R below `b` only through L, which the
merge gates and drains. So R can hold L's entries before the drain, and the
drain is the only linearization point.

### Keep separator publication and add a separate parent repoint

Both steps make a parent agree with the right-link chain of its children. Two
steps need two code paths and two recovery actions for the same job.

### Protect R with a structural gate that nobody else can revoke

This keeps one kind of mark on nodes. But it stops data writes to R during the
merge. After a worker crash, it stops them until recovery runs.

### Wait and wound like a split when gating L

This lets merges progress on busy leaves. But an optional merge would then abort
user transactions.

### Reclaim drained nodes in the first phase

Reclamation needs proof that no parent entry or right link names the node,
handling of missing nodes on stale routes, and a fence against a late write that
creates the token again. Each of these is a new correctness risk, while a kept
drained node costs only one small object until the collection is dropped.

### Collapse the root in the first phase

The root has a fixed address and every route starts there, so collapse cannot
drain into a sibling. It needs a new in-place protocol and a routing rule that
goes back to the root. The gain is small: a tall tree after many deletes costs
one extra read per level, and only when the cache is cold.

### Always advance the generation of the merged leaf

`max(g_L, g_R) + 1` is also safe, but it also makes absence reads of L's range
made while L was gated retry without need.

### Read R at a new currentness barrier after every drained node

This needs no new node field. But it does not cover reconciled parent entries,
which name R directly. Also, each route through a stale parent and each scan
through an old right link costs one backend read. Right links to drained nodes
stay until a later structural change, so this cost can stay.

### Record R's generation on the routes that a merge creates

The drained marker and each parent entry that reconciliation changes to name R
would record a membership generation of R after the absorb. A copy of R with a
lower generation is older than the merge. This needs no low key, but the same
evidence must be kept in two kinds of route, reconciliation must copy it, and
each new kind of route, such as shortened right links, must carry it too. A low
key is a property of the node, so one local check covers every route.

### Detect stale index copies by their first separator

A key below the first separator of an index copy shows that the copy is older
than a merge, so index nodes need no new field. But leaves have no first
separator, so stale reads of leaves still need a second rule.

### Fence late writes with a separate node counter

A counter that only gate and reservation installations advance avoids the extra
validation retries. But it adds a second node counter and a second rule for the
same fencing job. Structural gates are rare compared with data traffic, so the
extra retries cost little.

### Accept late writes after a recovery fence

Keep gate installation as it is, without a generation change. Then a late copy
of a gate write can restore the gated revision after a recovery fence. A late
drain or shrink can then land after recovery decided that it did not land:

- For a merge, recovery has already removed R's copies of L's entries, so L's
  keys are lost.
- For a split, recovery has already deleted the created sibling, so the keys at
  and above the split key are lost.

This needs two late writes and a stopped worker, so it is rare. But ADR-043
requires every conditional write to stay safe when it lands late.

### Ignore the tombstones of R in the vetoes without compacting R

This keeps the absorb as a plain copy. But the merged node keeps the tombstones
of R, so it can go above a split threshold at once. A split then compacts it
under a second gate, and each such merge costs two structural rewrites of R.

### Persist the soft thresholds in the database settings

All instances would agree, so splits and merges could not undo each other. This
reopens the ADR-072 decision to keep soft thresholds local, and the problem it
prevents causes only extra work.
