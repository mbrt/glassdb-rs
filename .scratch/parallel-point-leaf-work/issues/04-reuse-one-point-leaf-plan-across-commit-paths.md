# Define routed leaf-group lifetime across commit paths

Type: grilling
Status: resolved
Blocked by: 02, 03

## Question

Where can a routed leaf group and later protocol-specific leaf state be reused across direct-commit eligibility, fallback to the logged path, lock grouping, validation, and write-back without treating routing evidence as a shared point-leaf plan or current ownership after a split? Decide which interface owns each value, which later phase may reuse it, and where new routing is required.

## Answer

Keep `AccessSet` as the only point-access value shared by direct commit and the
logged path. It owns the normalized logical reads, final writes, and original
read observations for one transaction-body execution. Do not attach routing to
it, and do not retain routing across a body replay that replaces the access set.

Use `TreeRouter` over the shared `NodeStore` cache as the common physical-routing
module. `DirectCommit` and `KeyLocker` depend on `TreeRouter` independently and
call `group_keys_by_leaf` with their own domain payload. Its canonical output is
`RoutedLeafGroup<T>`: one leaf observation and the ordered logical keys, with
their domain payloads, routed to that leaf. It has no separate freshness field
because the observation already carries currentness evidence and the routing
requirements decide whether that evidence is sufficient. A routed leaf group
is a temporary result of one descent, not a shared point-leaf plan or proof of
current ownership. Repeated
`Requirement::Any` descent reuses cached decoded nodes and normally adds no
backend read. Remove the shallow
`KeyResolver::route_one_leaf`; direct commit performs its physical grouping
through `TreeRouter` directly.

`DirectCommit` owns `DirectMember`. Apply the zero-policy and per-value inline
checks before descent, so a member already known to require the logged path does
not route twice. A routed direct member submits one complete operation for its
one candidate leaf. The operation supplies every dependency key as leaf scope,
and the coordinator checks the loaded leaf before it folds. One clean `Reroute`
causes a new complete direct grouping. A later reroute or another locked outcome
discards the direct grouping; `KeyLocker` then groups the same logical
`AccessSet` independently.

`KeyLocker` converts each routed leaf group into its domain-owned lock group.
One lock group can be reused while that leaf waits for a foreign holder because
each coordinator submission reloads the target and checks all keys. A conflict,
leaf-capacity failure, or ownership failure ends the current bounded pass, but
not the normal acquisition episode. Keep every lock held by the same pending
transaction identity. Discard the pass's prospective groups and receipt values,
build current groups from the complete `AccessSet`, and submit the complete set
again.

For each submitted group, `AcquireOperation` examines the leaf already loaded
by `ShardCoordinator`. After the coordinator verifies the leaf scope and the
operation admits ordinary leaf work, return `Locked` without a CAS when that
transaction identity already holds every required entry and membership lock at
a sufficient strength. Otherwise, run the normal idempotent complete-leaf CAS.
This check keeps normal retry state inside the leaf operation. Do not expose or
retain a partial routed plan or a partial receipt set.

A transition from parallel to sorted serial acquisition ends the old identity's
acquisition episode. `AttemptDriver` first uses the general `Engine::end` path
to make that identity terminal on the abort side, then uses the general
`Engine::rebegin_transaction` path and builds all physical lock state again. A
dropped acquisition is pinned as `Wounded`; a completed conflict episode can be
acknowledged as `Aborted`. Do not run a foreground release sweep.

Only a fully successful acquisition pass creates cross-phase physical state.
`LockedTx`, owned by `KeyLocker`, retains each acquired path, `LeafRef`, point
intent group, and private hold receipt. An `Installed` receipt carries the
successful lock-CAS precondition observation. An `Observed` receipt carries the
loaded leaf observation that showed every required lock already held by this
transaction identity. `LockedTx` is the source for the durable lock manifest,
exact range-coverage evidence, and committed write-back. Do not reconstruct
these facts from acquisition-phase held-path bookkeeping; do not add that
bookkeeping.

Keep original point-read observations in `ReadEvidence`. Physical point
validation is the optimistic path before locks are held: combine or remove work
only for observations of the same exact path and revision. Locked point reads
always use the complete logical validation path and treat `Installed` and
`Observed` receipts alike. `KeyResolver` owns a new logical grouping and
resolves the complete point-read set at
`Requirement::AtLeast(validation_start)`, with the validating transaction
identity supplied as the own holder. Keep an exact `Installed` precondition
shortcut only for unchanged range coverage. Do not use direct or lock paths as
current-ownership evidence for logical point resolution.

Do not add separate leaf-load and dependency-resolution requirements to
`ShardCoordinator`. Direct commit seeds leaf work with `Requirement::Any`
because its immediate conditional leaf CAS validates the exact revision and
ownership. Normal `AcquireOperation` is skip-capable, so its first leaf load
uses `Requirement::AtLeast(validation_start)`: an `Observed` result has no
following CAS and must leave the cache ready for locked logical validation. A
successful lock CAS advances the same evidence. A CAS precondition miss, a
target that became an index, or a loaded leaf that excludes a scope key proves
that the seed is stale and triggers reload or regroup. Final transaction
statuses are immutable; a stale `Pending` or `Unknown` status is conservative,
and `Monitor` owns any current poll or mutation needed for wound-wait progress.
Existing range-scan requirements are unchanged.

Committed write-back reuses each `LockedTx` group as its first target. It also
uses that group's `Installed` or `Observed` receipt currentness bound because
write-back can prove the holder already absent and finish without another CAS.
The coordinator still checks the complete key scope. On `Reroute`, `KeyLocker`
performs a new `TreeRouter` grouping of only that affected group's intentions;
one old group can become several current groups. No old path is trusted after
that signal.
