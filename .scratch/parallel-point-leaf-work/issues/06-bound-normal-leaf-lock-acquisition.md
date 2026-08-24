# Bound normal leaf lock acquisition

Type: grilling
Status: resolved
Blocked by: 01, 03, 04

## Question

How should `KeyLocker` apply `join_all_bounded` to complete per-leaf lock operations, interpret `Locked`, `Conflict`, and `LeafFull` in stable leaf order after every input runs, account for foreign-holder waits that occupy bounded positions, preserve partial receipts and cancellation safety, and leave the existing sorted serial fallback unchanged?

## Answer

### Bounded normal acquisition

Keep `KeyLocker::lock_at` as the domain interface. `KeyLocker` owns a private,
nonzero normal-acquisition limit supplied when it is constructed; callers do
not pass the limit on each call. The exact value remains for
[Choose concurrency limits and verification](09-choose-concurrency-limits-and-verification.md).
The sorted serial path does not use this limit.

Keep the current complete operation for one physical leaf. `build_groups`
already combines point intentions and range-scan membership locks that target
the same leaf. Apply the limit to this complete combined leaf set. This creates
no second range-scan implementation and changes only admission, not range-scan
lock rules or transaction phase order.

For normal parallel acquisition, pass the leaf operations in ascending physical
path order to `join_all_bounded`. One future owns the complete `lock_shard`
loop for one leaf, including coordinator submission, CAS retry, and repeated
foreign-holder waits. A wait keeps that future incomplete and therefore
occupies one bounded position. Do not park it or admit replacement work around
it.

The bounded join runs every supplied leaf future after `Conflict`, `LeafFull`,
or an operational error occurs on another leaf. Its zero-input path returns
directly, and its one-input path awaits that leaf operation directly. Thus a
one-leaf transaction adds no queue, task, or backend operation.

### Stable outcome and receipts

Interpret the complete result vector in the same ascending leaf-path order.
The first item that is not `Locked` determines the aggregate result, regardless
of its category:

- an operational error is returned;
- `Conflict` uses the existing same-identity release, backoff, and retry path;
  and
- `LeafFull` uses the existing same-identity release and capacity-wait path and
  does not count toward serial escalation.

For example, a lower-path `Conflict` suppresses a later operational error even
though the later operation ran. This preserves current behavior and makes the
choice independent of completion order.

Keep successful hold recording inside the leaf future, before it returns
`Locked`. Every completed successful path is therefore present in
`KeyLocker`'s private held-lock bookkeeping even when another path determines
the aggregate result. Retain the successful receipt values until the aggregate
result is known. If every item is `Locked`, require exactly one receipt for each
group and build `LockedTx`. Otherwise discard the receipt values after keeping
the held paths available to retry cleanup. Do not expose `PartialLockedTx` or
partial receipts through the interface.

### Hard-timeout identity renewal

Keep the hard deadlock deadline outside `KeyLocker`. If it expires, dropping
the bounded join can abandon a conditional leaf write after backend dispatch.
That write can still apply remotely before its receipt and held path are
recorded. A same-identity release sweep cannot close this race: it can observe
no holder, return without a CAS, and then lose to the late write. Starting
serial acquisition under that same identity could therefore retain an
out-of-order lock and invalidate the serial progress proof.

Treat this timeout as an unresolved owner operation. Do not mark its
`OwnerOperation` clean. `AttemptDriver`, which owns the coupled transaction
handle and retirement guard, performs the identity transition:

1. Give the old transaction identity a retirement handoff and make its
   `Wounded` status durable before the replacement identity can publish.
2. Renew the transaction identity while preserving its wound-wait priority.
3. Force the renewed identity to start lock acquisition in the existing sorted
   serial mode.
4. For transactions without collection create/drop changes, retain the
   completed transaction body's access set and normal outcome and re-enter the
   commit phases without repeating that body. Rebuild routing and all physical
   lock state under the renewed identity.
5. If later validation rejects a retained read, use the normal transparent
   transaction-body replay. A timeout alone does not cause that replay.
6. For transactions with collection create/drop changes, use the existing
   wound-style transaction-body replay. Do not add collection-resource transfer
   rules to this effort.

A late leaf lock still names the terminal old identity. The renewed transaction
and existing recovery rules treat it as an abort-side holder and can remove it;
it can no longer appear to be the renewed transaction's own out-of-order lock.
Ordinary caller cancellation continues to use the existing abnormal-abandonment
retirement path.

The serial lock implementation itself stays unchanged: rebuild current groups,
visit them one at a time in ascending physical path order, and bypass
`join_all_bounded`. Only the identity used after a hard deadlock timeout changes.
A completed `Conflict` retry can still escalate under the same identity because
all leaf futures and receipts are then accounted for.

### Documentation and verification obligations

The final design needs a minimal ADR that supersedes only ADR-024's same-identity
hard-timeout rule and corrects ADR-025's claim that receipt-based release always
clears a late cancelled acquire. Accepted ADR text otherwise stays frozen.

[Choose concurrency limits and verification](09-choose-concurrency-limits-and-verification.md)
must cover these acquisition cases:

- zero-leaf and one-leaf direct behavior, bounded waves over distinct paths,
  and the maximum number of incomplete futures;
- a foreign-holder wait that occupies one bounded position;
- all-leaf execution and stable selection across mixed `Locked`, `Conflict`,
  `LeafFull`, and operational-error results;
- immediate held-path recording, complete `LockedTx` receipt matching, and
  release of all recorded partial holds after a normal terminal result;
- combined point intentions and range-scan membership locks on one leaf;
- a gated timeout after conditional-write dispatch but before receipt delivery,
  proving a durable old-identity wound, forced renewed-identity serial
  acquisition, safe handling of a late old lock, and no timeout-caused
  transaction-body replay for point/range transactions;
- transaction-body replay for the collection create/drop exception; and
- deterministic admission, result choice, and backend operation streams.
