# Recognize retained leaf locks across normal retries

Type: grilling
Status: resolved
Blocked by: 01, 03, 04, 06

## Question

How should `KeyLocker` retry a completed normal parallel acquisition without
releasing confirmed or uncertain locks, avoid partial retry state, recognize a
complete same-identity hold from the coordinator's loaded leaf without another
CAS, and ensure that every transition to sorted serial acquisition uses a
renewed transaction identity?

## Answer

Do not add a foreground retry-release phase.

After a complete bounded parallel pass returns `Conflict` or `LeafFull`, keep
the pending transaction identity and every leaf lock that it holds. Apply the
existing conflict backoff or capacity wait, rebuild current groups from the
complete `AccessSet`, and submit the complete group set again. Do not pass held
paths or retained partial proofs into the retry. The physical locks and the
leaf state are the retry memory.

Keep `KeyLocker::lock_at` as the deep interface. Put retained-lock recognition
inside `AcquireOperation`, after `ShardCoordinator` has loaded the target leaf,
verified its key scope, and admitted ordinary leaf work. Inspect the complete
group atomically:

- every required entry lock must contain this transaction identity at a
  sufficient strength;
- the required membership lock must contain it at a sufficient strength; and
- a partial match does not succeed.

When the complete match succeeds, return `Locked` without staging a leaf change
or issuing a CAS. When any required hold is absent, stage the ordinary complete
leaf operation. That CAS is idempotent for locks already held by the same
identity and reconciles an earlier uncertain CAS.

Use `Requirement::Any` for the point-only retained-lock check. While the
transaction identity remains pending and normal retry never releases its locks,
another operation cannot remove or move those locks and leave the identity
pending: structural work and reclamation must first reconcile or wound it.
Range-scan groups keep their validation-barrier requirement. If uncertain local
cache knowledge was discarded, the `Any` load reads the leaf before it can find
the hold.

Represent the successful full pass with a private proof per `LockedTx` group:

- `Installed` contains the successful lock-CAS precondition observation; and
- `Observed` contains the loaded leaf observation that showed the complete
  same-identity hold.

Both proofs supply the held strengths and path needed by the lock manifest.
Each proof also supplies the observation bound used by committed write-back.
Only `Installed` can use the exact precondition shortcut during physical point
validation. `Observed` proves the hold but not the state replaced by the earlier
CAS, so it uses logical point validation at the shared lower bound. Do not
expose these proof variants or a partial acquisition type outside `KeyLocker`.

A hard timeout can drop a conditional leaf write before its result is recorded.
A completed `Conflict` threshold has accounted for its futures, but it still
uses the same uniform serial-transition rule. Before either parallel episode
enters sorted serial acquisition, `AttemptDriver` gives the old identity a
retirement handoff and makes its `Wounded` status durable. It then renews the
transaction identity, preserves its wound-wait priority, rebuilds current
groups, and starts sorted serial acquisition. The new identity reclaims any old
abort-side locks as it reaches them. It does not wait for a foreground release
sweep.

Deterministic simulation must cover a cached complete hold with no CAS, a
partial hold with one complete-leaf CAS, an uncertain landed CAS recognized on
retry, stable mixed-result selection, no release operation between normal
passes, and renewed-identity entry into sorted serial acquisition.
