# ADR-032: Node-level locking and coordinated splits

## Status

Proposed.

Supersedes the **"escalate to per-leaf read locks"** membership clause of
[ADR-031](031-dynamic-range-sharding.md). It **refines** — does not replace —
ADR-031's split: the source-shrink CAS stays the split's linearization point and
right-links stay the tolerance mechanism; a node-level **structure lock** is
layered on for priority and mutual exclusion. The B-link object model, encoding,
cached self-correcting descent, right-links, and the in-place root-split shape
carry over unchanged.

## Context

ADR-031 makes leaf and root splits **lock-free**: a background task shrinks the
source in a single CAS, tolerating concurrent access through B-link right-links.
Two problems surface under real contention.

- **Splits starve on the hot leaf.** A lock-free split's shrink CAS competes with
  the write-backs that keep a hot leaf hot. Having no priority, it repeatedly
  loses the CAS precondition, exhausts its retry budget, and re-queues — so the
  hottest leaf, the one that most needs to split, is the least likely to. The
  append-only hotspot ADR-031 itself flags makes this concrete: growth stalls
  exactly where it is needed.
- **The object version conflates concerns.** A node's backend version bumps on
  _any_ write — value overwrite, membership change, or structural split alike —
  so any coordination keyed on it over-conflicts (a listing is disturbed by
  unrelated value writes; a split is indistinguishable from a create). Structure,
  membership, and value are distinct concerns needing distinct coordination.

We want splits to make progress under load by reusing the engine's existing
wound-wait / lease machinery
([ADR-002](002-wound-wait-locking.md)/[ADR-021](021-wound-wait-leases-shard.md)),
and a lock taxonomy that separates structure, membership, and value so range
scans ([ADR-033](033-transactional-key-iteration.md)) are phantom-safe without
conflicting with value writes.

**One constraint bounds the split design.** Cross-object atomicity in this engine
comes from per-key transaction pointers that readers resolve by status
([ADR-020](020-commit-write-back-protocol.md)); structural fields (high-key,
right-sibling, separators, child pointers) have no such status-resolved pointer,
and the transaction log records only key writes. A split therefore **cannot** be
made atomically visible across its objects without introducing structural MVCC.
Rather than do that, this ADR keeps ADR-031's per-CAS linearization and adds
locking only for coordination and priority.

## Decision

### Two node locks: structure and membership (both read/write)

Two node-level locks live in the node object beside the ADR-017 per-key entry
locks — for a leaf, the _same_ object, so leaf-level locking adds no round-trip.
They are the S2PL / escalation layer. Only the **read-only** fast paths (a
read-only scan, a single-key read) take no node lock; every _mutating_ path —
including the single read-write fast path — participates in the structure
protocol (see *Every node-mutating path participates* below).

**Structure lock (R/W)** guards the node's shape (its entry set as a physical
unit, high-key, right-sibling, child pointers).

- Held **R** by every data mutation (overwrite, create, delete) _and_ by an
  escalated scan.
- Held **W** by a split or merge.
- `R∥R`, `R×W`, `W×W`.

A _held_ R is what lets a split, via wound-wait, wound the competing mutations
(and escalated scans) on the node so it can land — the anti-starvation mechanism.
Pure reads hold nothing, so the read hot path stays lock-free and self-corrects
through right-links.

**Membership lock (R/W)** guards the leaf's live key set (the predicate / gap).

- Held **R** by a scan (escalated).
- Held **W** by create and delete — the operations that change the key set.
- Overwrites and reads take **no** membership lock: a value write is not a
  membership change.
- `R∥R`, `R×W`, `W×W`.

The membership-W lock is the create/delete marker, and it is what **distinguishes
a delete from an overwrite**: both take a per-key `Write` lock (ADR-017 has no
distinct delete lock), but only a delete — a membership change — _also_ takes the
membership-W lock, so a scanner conflicts with a delete and not with an
overwrite. It also serializes create/delete within a leaf (`W×W`). This is a deliberate
**simplicity trade-off**, not a fundamental necessity. What serializability
_requires_ is only that create/delete conflict with a concurrent range read
(per-key locks cannot supply this — a phantom key has no entry to lock); it does
**not** strictly require two independent creates to conflict with _each other_. A
more concurrent scheme is possible — a point create could, in a single leaf CAS,
both check the node predicate (membership-R) lock and install a per-key
membership-intent, letting independent creates proceed in parallel — but that is
cross-granularity locking with materially more complexity. We choose the plain
node-level membership-W lock for uniformity and simplicity, accepting that it
serializes create/delete within one leaf. This mirrors v1's collection-level
membership lock, now at **leaf** granularity, so the serialization is confined to
one leaf's range, never the whole collection.

Per-operation lock sets:

| operation         | structure | membership | per-key entry     |
| ----------------- | --------- | ---------- | ----------------- |
| read              | —         | —          | —                 |
| overwrite         | R         | —          | Write             |
| create            | R         | W          | Create            |
| delete            | R         | W          | Write (+tombstone)|
| scan (escalated)  | R         | R          | —                 |
| split / merge     | W         | —          | its structural writes |

Because an escalated scan holds **structure-R**, it conflicts with a split's
**structure-W**: a split and an escalated scan over the same node serialize under
wound-wait. This removes the need to _transfer_ a scanner's lock across a split —
the two cannot overlap. (A non-escalated OCC scan holds nothing; a split that
races it changes the covered leaf set, which OCC validation detects and retries.)

### Membership version: the OCC fast-path token

Each leaf carries a monotonic **membership version**, bumped only by
membership-**write** activity — a create/delete membership-W lock install,
release, or write-back. Scanner membership-**R** acquisition and release do
**not** bump it; otherwise concurrent escalated scanners would needlessly
invalidate each other's optimistic scans. It is an _optimization_, not the
authority.

A read-only or not-yet-escalated scan validates optimistically. The
version-equality shortcut is sound only under this condition: OCC may pass on a
covered leaf iff **(a)** its membership version is unchanged since the scan,
_and_ **(b)** every pending membership-W holder the scan observed in that leaf is
still non-committed at validation. Condition (a) catches any membership activity
_begun_ after the scan (a lock install bumps the version); condition (b) catches
a create/delete that was _already pending_ at scan time and then committed (a
commit flips only the transaction object, so it does not bump the version). When
no pending membership holders were seen — the common case — (a) alone suffices
and validation is a single integer compare; otherwise the scan records those
holders as status dependencies and rechecks them, or escalates.

The **authoritative** membership is always the status-aware resolved key set
([ADR-020] help-forwarding, which honours tx status and excludes the
transaction's own pending writes); the version only lets an uncontended scan skip
recomputing it. A materialized digest of the key set is a hint under the same
(a)+(b) condition, never authoritative — a leaf field that bumped only at
write-back would lag a committed-but-unpublished create and miss the phantom;
one that bumped at lock acquisition would make a transaction's own staged create
invalidate its own scan. Splits do **not** bump the membership version (they
relocate keys without changing the collection's key set); a split is caught,
when relevant, by the covered-leaf-set change, not the version.

### Splits: shrink-CAS linearization, structure lock for coordination

A split keeps ADR-031's protocol and linearization; the structure lock is layered
on for exclusion and priority, not atomicity. Crucially, **it holds at most one
node's structure-W at a time** — never a child-to-parent chain:

1. Acquire the **structure-W** lock on the node to split (wound-wait, ADR-002).
   This excludes concurrent splits/merges and escalated scans on that node and
   lets the split wound/help competing mutations by priority.
2. Create the right sibling (`write_if_not_exists`), then **shrink the source in
   one CAS — the linearization point**, right-linking it to the sibling.
3. **Release the source structure-W immediately.** The split is now linearized;
   right-links make the new topology correct for every reader and writer even
   before the parent knows about it.
4. Insert the separator into the parent as a **separate, independently-locked
   follow-on**: acquire the parent's structure-W, insert, release. If the splitter
   dies first, the next descender (or recovery) performs this idempotently; it is
   never held together with the child lock.

Because a split holds only one structure-W at a time and releases it before
ascending, there is **no multi-level lock chain and therefore no cross-level
deadlock** — the vague "child-then-parent, reconciled with the sorted-order
fallback" ordering is dropped. Concurrent locks a split *does* hold at once (e.g.
during recovery, several created-node tokens) still follow the global
sorted-by-path order of ADR-020.

**Non-root index splits** are the same three-step shape one level up: an over-full
interior node is split under its own structure-W (steps 1–3), and its separator is
inserted into *its* parent as a follow-on (step 4), which may itself overflow and
recurse — each level acquired and released independently. Only the root cannot
move; it splits in place, rewriting `_i` under `_i`'s structure-W.

The earlier framing "commit is the linearization point" is therefore **withdrawn**
as unimplementable with the current machinery (see Context): the shrink CAS
linearizes each split step exactly as in ADR-031.

### Every node-mutating path participates

The anti-starvation argument ("a split wounds every competing mutation") holds
only if there is **no CAS to a node that bypasses the structure lock**. The
starvation ADR-032 fixes is caused specifically by _write-backs_, not user calls,
so the invariant must cover the background and maintenance paths too, not only
foreground overwrites/creates/deletes:

- **Single read-write fast path** ([ADR-027](027-single-rw-parallel-lock-publish.md))
  — a data mutation; it acquires **structure-R** folded into its existing lock CAS
  (no extra round-trip). It takes no membership lock (it overwrites an existing
  key).
- **Asynchronous write-back** ([ADR-020](020-commit-write-back-protocol.md)) — the
  mutation's structure-R is held **from lock acquisition through write-back and
  release**, not dropped at commit, so a write-back CAS is always covered by a
  held structure-R that a split can see.
- **Help-forwarding** — a helper completing another transaction's write-back acts
  under that transaction's still-held structure-R; the holder entry stays in the
  node until the write-back it represents completes.
- **GC pruning, lock/lease reclamation, coordinator maintenance mutations**
  ([ADR-021](021-wound-wait-leases-shard.md)/[ADR-022](022-garbage-collection-mark-sweep.md)/[ADR-028](028-shard-mutation-coordinator.md)) — any of these that CAS-rewrite a
  node take structure-R (or, if they restructure, structure-W) like any mutation.

Given that, a split that holds **structure-W** on a node makes progress as
follows: it **wounds** the younger _uncommitted_ structure-R holders (they abort
and stop retrying); for _committed-but-unpublished_ holders it cannot wound (a
committed transaction is not abortable), so it **help-forwards their write-back to
completion** (a bounded set) and then proceeds; and any _new_ CAS to the node
observes the held structure-W and backs off. There is thus no unprioritized CAS
traffic left to race the shrink. Only the **read-only** fast paths (a read-only
scan, a single-key read) touch a node without any structure lock, and they never
CAS it.

### Structural orphan recovery

A split creates node objects (the sibling, or the root-split children) before they
are referenced, and its structural linearization (the shrink CAS) is **not atomic
with any transaction-status transition** — a split can look aborted while its
shrink CAS actually landed, or look in-progress after it fully completed. Recovery
therefore cannot key off transaction status; it must read the **structural
state**. The lifecycle:

1. **Write-ahead.** Before creating any node, the split writes a **structural log
   record** containing: the source (or root) token and its current version, each
   created-node token, and the intended separator/link (the split key and which
   side moves). The record precedes object creation so no created object can exist
   without a record pointing at it.
2. **Create** the sibling(s) (`write_if_not_exists`, idempotent).
3. **Shrink CAS** on the source — the linearization point — version-guarded by the
   version recorded in step 1.
4. **Publish** the parent separator (the follow-on of the *Splits* section).
5. **Finalize.** Once the parent link is published, the record is marked complete
   and may be deleted.

**Recovery of an in-progress record** determines the outcome from structural
state, not status, by proving **tree-reachability of the created node(s)** — not
by a direct `source.right == sibling` equality (unstable: a later `L → M → R`
split leaves `L.right = M`, yet `R` is still reachable through the chain) and not
by the source's object version (unreliable: lock reclamation or another permitted
rewrite bumps it without touching the link). Right-links are only ever *added*
(merge is deferred), so reachability is monotonic and the search is well-defined:

- **Leaf / interior split.** Search for the created sibling's token by descending
  from `_i` to the recorded separator key and following right-links across the
  covered range. **Reachable** (found via a parent separator or anywhere on the
  right-link chain) ⟹ the shrink CAS landed ⟹ **roll forward**: idempotently
  ensure the parent separator, then finalize and delete the record. **Provably
  unreachable** ⟹ the shrink never landed ⟹ the created node is an **orphan** ⟹
  delete it and finalize the record aborted. (The source is left as-is; it will be
  re-split later by the size trigger — recovery never re-runs the shrink.)
- **In-place root split.** The two children are referenced through `_i`'s index
  entries, not a right-link, so the test is separate: if `_i` is an index pointing
  at both created children, the split landed → finalize; if `_i` still holds keys,
  it did not → delete the orphaned children → finalize aborted.
- **Third state — ambiguous ⟹ retry, never delete.** If reachability cannot be
  decided right now because a concurrent structural operation holds structure-W on
  a node the search must read, recovery **defers and retries later** rather than
  guessing. Deletion happens only on a *proven* unreachable result.

Because create is `write_if_not_exists`, the parent/root separator insert is
idempotent, and deletion is gated on proven unreachability, replaying recovery is
safe.

While a record is live its created tokens count as **reachable** for the GC
reverse-reference check ([ADR-022](022-garbage-collection-mark-sweep.md)), so GC
never races a split; once the record is finalized, ordinary reachability
(parent/right-link) governs. This structural record + forward/abort resolution
replaces ADR-031's split-active registry and reachability sweep, and is the
log-schema extension the current key-writes-only log needs.

### Progress under load: assumptions and a hard cap

Splits run at **normal transaction priority**; wound-wait's
restart-with-original-priority rule ([ADR-002](002-wound-wait-locking.md)) makes a
repeatedly-contended split eventually the oldest contender, so it acquires
structure-W and lands. This guarantee is **conditional**, and the conditions must
be stated rather than assumed:

- priority is retained across background re-queues,
- older conflicting holders are reclaimable (leases expire, ADR-021),
- the node is still **writable**.

The last is not automatic: an append hotspot can push a leaf past the backend
object-size limit _before_ the split wins its lock, after which no rewrite — the
split's own shrink included — can succeed. Because conditional progress is this
ADR's central motivation, the **reservation invariant is settled here** (only its
numeric tuning is deferred):

> **Hard-cap invariant.** A node's admissible content is capped at
> `backend_limit − H`, where the reserved headroom `H` is large enough that, from
> any admissible state, a split can always (a) install its **structure-W** holder
> entry, (b) hold the bounded worst-case set of concurrent **structure-R /
> membership-R holder entries and per-key lock metadata**, and (c) encode the
> **shrink CAS**. A membership-adding mutation (create) is admitted only if the
> post-write encoding still fits under `backend_limit − H`; over the cap it fails
> with a **retryable "leaf full, split pending"** error until the split lands.

Reserving for lock/holder metadata and the structure-W acquisition — not merely
for "one more key" — is what the reviewer's point requires: otherwise lock
contention alone (holder-list growth) could push the object past the limit after
creates are already blocked, deadlocking the very split meant to relieve it. The
shrink itself only *reduces* size, so once `H` guarantees structure-W is
installable and the CAS is encodable, the split always makes progress. Overwrites
(no membership growth) and the split's own shrink are always admissible. The
concrete value of `H` (and whether an at-cap leaf also escalates split priority)
is a tuning question tracked in the design doc.

## Consequences

- **Progress under load, conditionally.** The hot-leaf starvation of the
  lock-free protocol is removed for the common case, given the stated assumptions
  and a hard-cap policy.
- **Splits are coordinated, not atomic.** They remain linearized by the shrink
  CAS (ADR-031); the structure lock adds priority and mutual exclusion, not
  cross-object atomicity — avoiding structural MVCC.
- **create/delete serialize within a leaf** (membership-W is exclusive) — the
  accepted cost of serializable phantom prevention, mirroring v1's membership
  lock at finer (leaf) granularity. Overwrites and reads are unaffected and stay
  invisible to scans.
- **Clean markers.** The membership-W lock distinguishes a delete from an
  overwrite; a scan's structure-R makes splits and escalated scans mutually
  exclusive, so no scanner lock is transferred across a split.
- **Membership is a resolved property**, validated by status-aware resolution;
  the membership version is a sound fast path only under the (a)+(b) condition.
- **Recovery** gains a structural-log entry for created node tokens (a log-schema
  extension) in place of ADR-031's split-active registry.
- **Costs.** A structure-read holder on every mutation (foreground _and_
  write-back/help-forward/GC) grows the leaf's holder list, its logged lock set,
  and CAS-rewrite size, and structure-R is now held through write-back rather than
  released at commit; wounding a mutation aborts its whole (possibly multi-leaf)
  transaction. The split's exclusive interval is bounded to **one node at a time**
  (structure-W is released right after the shrink CAS; the parent insert is a
  separately-locked follow-on), which shortens the hot leaf's lock hold, though
  splits under a shared parent still serialize on that parent's structure-W during
  the follow-on. Accepted for serializability and progress.
- **Format.** Nodes gain the structure/membership lock fields and the membership
  version in their golden-anchored encoding; the transaction log gains structural
  (created-node) entries; golden vectors and DST oracles regenerate (greenfield,
  as ADR-031).
