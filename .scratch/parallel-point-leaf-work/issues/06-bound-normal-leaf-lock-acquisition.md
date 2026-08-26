# Bound normal leaf lock acquisition

Type: grilling
Status: resolved
Blocked by: 01, 03, 04

## Question

How should `KeyLocker` apply `join_all_bounded` to complete per-leaf lock operations, interpret `Locked`, `Conflict`, and `LeafFull` in stable leaf order after every input runs, account for foreign-holder waits that occupy bounded positions, retain locks across normal retries without partial retry state, preserve cancellation safety, and enter the existing sorted serial mechanism under a renewed transaction identity?

## Answer

### Bounded normal acquisition

Keep `KeyLocker::lock_at` as the domain interface. `KeyLocker` owns the nonzero
value as `parallelism`, copied from
`EngineConfig::transaction_leaf_parallelism` when it is constructed. Callers do
not pass the limit on each call. It uses the same value for normal acquisition
and committed write-back. The sorted serial path does not use this limit.

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
- `Conflict` keeps the same transaction identity and all locks that it holds,
  then uses the existing backoff and retry policy. If the conflict threshold
  requires sorted serial acquisition, use the identity transition below; and
- `LeafFull` keeps the same transaction identity and all locks that it holds,
  then uses the existing capacity-wait policy. It does not count toward serial
  escalation.

For example, a lower-path `Conflict` suppresses a later operational error even
though the later operation ran. This preserves current behavior and makes the
choice independent of completion order.

Do not keep an acquisition-phase set of paths that are believed to be locked.
Each `Locked` result carries a private `Installed` or `Observed` receipt for the
current pass. Retain these receipt values only until the aggregate result is
known. If every item is `Locked`, require exactly one receipt for each current
group and build `LockedTx`. Otherwise discard all receipt values but keep the
physical locks. An uncertain lock CAS can also have landed without returning a
receipt. The next retry handles both cases by rebuilding all groups from the
complete `AccessSet` and inspecting each loaded leaf. Do not expose
`PartialLockedTx`, a partial receipt set, or a held-path retry input through the
interface.

`AcquireOperation` performs the retained-lock check inside the existing
coordinator fold. After leaf-scope and structural-gate checks, it returns
`Locked(Observed)` without a CAS when the loaded leaf shows this transaction
identity holding every required entry and membership lock at a sufficient
strength. If any required hold is absent, it stages the normal complete-leaf CAS
and returns `Locked(Installed)` only after that CAS lands. Same-identity staging
is idempotent, so it also reconciles an earlier uncertain CAS.

Use `Requirement::AtLeast(validation_start)` for every retained-lock check. The
operation can return `Observed` without a following CAS, so its loaded leaf must
satisfy the same lower bound that locked logical validation uses. A successful
`Installed` CAS advances its precondition evidence past that bound. A structural
change must still reconcile or wound the pending transaction identity before it
can move the locks. [Define parallel point validation](05-define-parallel-point-validation.md)
sends every locked point read through logical validation, independent of
whether its hold receipt is `Installed` or `Observed`.

### Serial-fallback identity renewal

Keep the hard deadlock deadline outside `KeyLocker`. If it expires, dropping
the bounded join can abandon a conditional leaf write after backend dispatch.
That write can still apply remotely after the future is dropped and before its
result is known. A same-identity release sweep cannot close this race: it can
observe no holder, return without a CAS, and then lose to the late write.
Starting serial acquisition under that same identity could therefore retain an
out-of-order lock and invalidate the serial progress proof.

Treat this timeout as an unresolved owner operation. Do not mark its
`OwnerOperation` clean. A completed `Conflict` has no late acquisition future,
but a conflict threshold that selects sorted serial acquisition uses the same
identity transition. This gives every transition from parallel to serial one
rule and removes the foreground release phase.

`AttemptDriver`, which owns the coupled transaction handle and retirement guard,
performs the identity transition:

1. End the old transaction identity through the general `Engine::end` path and
   make its abort-side terminal status durable before the replacement identity
   can publish. A dropped operation is `Wounded`; a completed conflict episode
   can be `Aborted`.
2. Renew the transaction identity through the general
   `Engine::rebegin_transaction` path while preserving its wound-wait priority.
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
`join_all_bounded`. Every transition from a running parallel acquisition episode
uses a renewed transaction identity. An attempt that starts directly in serial
mode already has no parallel locks under its current identity. A later serial
`Conflict` or `LeafFull` keeps its sorted prefix locks and retries under that
same identity; it is already inside the serial mechanism and does not perform
another fallback.

### Documentation and verification obligations

The final design needs a minimal ADR that supersedes ADR-024's foreground
release-and-reacquire rules and its same-identity transition into sorted serial
acquisition. It must also correct ADR-025's claim that receipt-based release can
always clear a late cancelled acquire. Accepted ADR text otherwise stays frozen.

[Choose concurrency limits and verification](09-choose-concurrency-limits-and-verification.md)
must cover these acquisition cases:

- zero-leaf and one-leaf direct behavior, bounded waves over distinct paths,
  and the maximum number of incomplete futures;
- a foreign-holder wait that occupies one bounded position;
- all-leaf execution and stable selection across mixed `Locked`, `Conflict`,
  `LeafFull`, and operational-error results;
- retained locks after completed `Conflict` and `LeafFull` results, with no
  foreground release CAS;
- a cached complete same-identity hold that returns `Observed` without a CAS,
  an absent or partial hold that runs the complete-leaf CAS, and an uncertain
  landed CAS that the next retry recognizes;
- complete `LockedTx` receipt matching without a partial receipt set or held-path
  state across retries;
- combined point intentions and range-scan membership locks on one leaf;
- a gated timeout after conditional-write dispatch but before result delivery,
  and a completed conflict threshold, each proving a durable abort-side old
  identity, forced renewed-identity serial acquisition, safe handling of old
  locks, and no transition-caused transaction-body replay for point/range
  transactions;
- transaction-body replay for the collection create/drop exception; and
- deterministic admission, result choice, and backend operation streams.
