# ADR-065: Renew transaction identity on serial fallback

## Status

Accepted.

This ADR supersedes [ADR-024](024-hold-and-wait-conflict-resolution.md)'s
same-identity foreground-release transition to sorted serial acquisition and
[ADR-025](025-dedup-shard-lock-acquisition.md)'s assumption that receipt-based
release always clears a late cancelled acquire. It narrows
[ADR-026](026-dedup-shard-release-write-back.md): release remains valid for
abort and recovery work, but serial fallback does not use it as foreground
control state.

## Context

Parallel lock acquisition can deadlock between equal-priority transactions.
[ADR-024](024-hold-and-wait-conflict-resolution.md) breaks the cycle with a
deadlock timeout and the sorted serial lock order. Its rule is: release the
out-of-order locks, then acquire again in ascending leaf-path order under the
same transaction identity. The serial order gives progress only if no
contender holds a lock out of that order.

The release step cannot keep that promise. The timeout drops the acquisition
future, and the future can already have dispatched a conditional leaf write.
That write can still apply after the drop and before its result is known. A
same-identity release sweep that runs at that moment observes no holder, returns
without a CAS, and then loses to the late write. Sorted serial acquisition would
then start while the same identity still holds an out-of-order lock, which is
the exact state the sorted order exists to prevent.

[ADR-064](064-bounded-parallel-point-leaf-work.md) also removes the reason to
keep the release sweep for ordinary retries: a completed conflict or capacity
pass keeps its locks and submits the complete group set again. Release is
therefore no longer normal-retry control state, and the only remaining caller in
the acquisition path is this transition.

The two ways to reach sorted serial acquisition from a running parallel episode
differ in what they know. A hard timeout has an unresolved leaf operation. A
completed conflict threshold has accounted for every future. One rule for both
is simpler than two, and the safe rule is the one the timeout needs.

## Decision

Every transition from a running parallel acquisition episode into sorted serial
acquisition renews the transaction identity. There is no foreground release
sweep in this path.

The attempt owner performs the transition with the general engine interfaces
that it already uses for a wound:

1. End the old identity. This closes admission and makes the identity terminal
   on the abort side before any replacement can publish. A dropped or timed-out
   conditional write leaves an unresolved owner operation, so the existing end
   path pins the identity as wounded. A completed conflict episode can be
   acknowledged as aborted. If end fails, no replacement identity is created.
2. Begin again from the ended attempt, preserving wound-wait priority.
3. Enter sorted serial acquisition directly under the renewed identity.

The renewed identity samples a new validation lower bound, acquires the
collection directory locks again, and builds all physical lock state again. A
late leaf write from the old identity names a terminal abort-side holder. The
renewed transaction and the existing recovery rules can remove it, and it can
never look like the renewed transaction's own out-of-order lock.

Renewal replaces the identity, not the work. A point or range transaction keeps
the completed transaction body's access set and normal outcome and re-enters the
commit phases without running the body again. A transaction that creates or
drops a collection keeps its existing wound-style body replay, because its
prepared collection resources belong to the old identity. If later validation
rejects a retained read, the ordinary transparent body replay applies; the
transition alone never causes one.

The sorted serial mechanism itself does not change. It visits current groups one
at a time in ascending leaf-path order, has one incomplete leaf operation at a
time, and arms no timeout. An attempt that starts directly in serial mode holds
no parallel locks under its current identity and needs no transition. A later
serial conflict or capacity failure keeps its sorted prefix locks and retries
under the same identity, because it is already inside the serial mechanism.

## Consequences

- The serial progress proof holds again. A transaction that enters the sorted
  order can no longer hold an out-of-order lock under its current identity,
  even when a leaf write landed after its acquisition future was dropped.
- One transition creates one abort-side transaction object. This is GC debt
  that ADR-024 avoided. The transition is rare, because it needs a deadlock
  timeout or a repeated conflict threshold, and the alternative is an unsound
  progress guarantee.
- The transition costs one durable end before the replacement identity starts.
  In exchange it issues no release CAS for each held leaf, so a wide
  transaction can transition with less backend work than before.
- Old locks are reclaimed as the renewed identity reaches them, or later by
  recovery, instead of by a foreground sweep. A peer that meets one of those
  locks resolves an abort-side holder, which it already knows how to do.
- Retirement has one shape. Wound restart, abnormal abandonment, and the serial
  transition all end the identity through the same path, so there is one place
  where terminal status becomes durable.
- The deadlock timeout stops being an internal control signal that `Algo`
  resolves alone. The attempt owner, which owns the handle and the retirement
  guard, decides the transition. This keeps identity replacement where identity
  ownership already is.
- A transaction that transitions keeps its executed body. Only collection
  create and drop replay it, exactly as a wound does today.

## Alternatives considered

- **Keep ADR-024's same-identity release and reacquire.** It cannot close the
  race with a leaf write that was dispatched before the drop, so the sorted
  order can start with an out-of-order lock still installed.
- **Make the acquisition future cancel-safe, or fence the late write.** This
  needs a durable per-operation fence for every conditional leaf write in the
  acquisition path. It adds protocol state and cost to the hot path to protect a
  rare transition, and the identity renewal already gives the same guarantee
  with existing mechanisms.
- **Wait for the dispatched write to resolve before the serial phase.** The
  timeout exists because that wait has no bound. Waiting for it would remove the
  deadlock bound that the transition serves.
- **Add dedicated retire-for-serial and rebegin-for-serial engine interfaces.**
  A second retirement path would have to keep the same durable guarantees as the
  general one and would drift from it. The forced-serial acquisition mode is
  enough to carry the difference across the general renewal.
- **Renew only after a timeout, and keep the same identity for a completed
  conflict threshold.** Two rules for one transition would require every reader
  to know which episode ended, for no measured gain.
