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
leaf-capacity failure, ownership failure, or serial escalation ends that
acquisition attempt. Release the exact paths that actually acquired locks,
clear their process-local bookkeeping, and build new groups from the current
`AccessSet`. Do not reuse the failed attempt's prospective groups. Retry release
uses exact held paths and sweeps for the transaction ID; it does not route keys.

Only a fully successful acquisition creates cross-phase physical state.
`LockedTx`, owned by `KeyLocker`, retains each acquired path, `LeafRef`, point
intent group, and successful lock-CAS receipt. It is the source for the durable
lock manifest, this transaction's physical validation evidence, and committed
write-back. Do not reconstruct these facts from diagnostic held-path
bookkeeping.

Keep original point-read observations in `ReadEvidence`. Physical validation is
optimistic: combine or remove work only for observations of the same exact path
and revision, and accept this transaction's lock receipt only when its CAS
precondition is that same exact state. Physical equality is not sufficient when
the observed entry has an exclusive holder, because the holder's transaction
status can change the effective writer without changing the leaf. When an exact
observation changed or such a holder exists, `KeyResolver` owns a new logical
grouping and resolves the complete point-read set at
`Requirement::AtLeast(validation_start)`. Do not use direct or lock paths as
current-ownership evidence for that resolution.

Do not add separate leaf-load and dependency-resolution requirements to
`ShardCoordinator`. Direct commit and normal point-lock acquisition seed their
leaf work with `Requirement::Any`; their immediate conditional leaf CAS validates
the exact revision and ownership. A CAS precondition miss, a target that became
an index, or a loaded leaf that excludes a scope key proves that the seed is
stale and triggers reload or regroup. Final transaction statuses are immutable;
a stale `Pending` or `Unknown` status is conservative, and `Monitor` owns any
current poll or mutation needed for wound-wait progress. Validation that has no
following CAS keeps `Requirement::AtLeast(validation_start)`. Existing range-scan
requirements are unchanged.

Committed write-back reuses each `LockedTx` group as its first target. It also
uses the lock receipt's currentness bound because write-back can prove the holder
already absent and finish without another CAS. The coordinator still checks the
complete key scope. On `Reroute`, `KeyLocker` performs a new `TreeRouter` grouping
of only that affected group's intentions; one old group can become several
current groups. No old path is trusted after that signal.
