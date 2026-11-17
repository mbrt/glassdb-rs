# Storage evidence rules

Two type modules require stricter correctness review. Paths are relative to
`crates/glassdb-storage/src/`:

| Module | Types | Rules |
| --- | --- | --- |
| `timeline.rs` | `Timeline`, `SequencePoint`, `CurrentnessBarrier`, `Requirement`, `MonotonicFloor` | E1, E2, E5 |
| `cached_store/evidence.rs` | `Observation`, `Revision`, `CasReceipt`, and their result and shared evidence types | E3, E4, E5 |
| `cached_store/cut.rs` | `CutSearch`, `CutTarget` | E5 |

Review every change to these files, including visibility, conversions, tests,
and moves. Changes to their exports must preserve the same restrictions. The
[cache guide](caching.md) explains the currentness model and the constraints
that the cache keeps; [GLOSSARY.md](../../GLOSSARY.md) defines the terms.

## E1: Sequence points belong to one database instance

An operation's point is allocated immediately before backend invocation. It is
a lower bound on when its exact state was current, not a revision or a
completion point. Completion time cannot advance its evidence, and a newer
point cannot order the contents returned by overlapping operations. Points
from different database instances are not comparable.

Raw allocation and representation helpers are storage implementation details.
Their only uses are backend invocation, shared evidence, and persistent-cache
encoding and decoding. They must not derive barriers or requirements from an
observation. The one permitted recovery handoff passes the opaque recovered
point from the opened persistent cache to the new timeline of the same database
identity. That timeline and its approximate staleness cutoffs must follow every
recoverable point.

## E2: Requirements and barriers are opaque

Only `Timeline` constructs a currentness barrier. Capture it after prerequisite
work and before dependent operations. Requirements are constructed only as
`ANY`, `after`, or `within`, and combining two requirements preserves the
stronger one. `within` is an approximate cache policy, not proof that
prerequisite work ended.

Neither barriers nor requirements expose their point, including to other storage
modules. Use predicates to check evidence. Do not add raw-point constructors,
getters, serialization, defaults, or conversions from observations, receipts, or
requirements, including implicit conversions. Copying a barrier does not capture
another barrier. A requirement states what must be proved and must never become
evidence itself.

## E3: An observation retains one exact state

Keep its path, decoded value or absence, revision, and evidence together. The
internal fields and evidence operations stay `pub(super)` to the cached-store
module and its implementation children. Do not widen them to `pub(crate)`, and
do not expose payload mapping, raw watermark accessors, setters, or constructors
to typed stores. Preserving the
cache implementation's existing access does not give other storage modules
permission to manufacture observations.

The shared evidence cell and the backend revision inside a `Revision` remain
private to their module. The cache implementation advances evidence through the
provided operations and borrows that token for conditional operations; neither
can be stored, swapped, or replaced directly. Do not add public constructors,
`Default`, conversion traits, or mutable access to the backend revision. Higher
layers may retain, compare, and serialize a `Revision`, but cannot manufacture
one.

Advance evidence only from a definitive backend result or confirmed evidence
for that exact state. Equal contents at different paths are not the same state.
Independent absence observations must not be assumed to share evidence.
Changed-state and error results cannot advance an old state's evidence.
Eviction removes discoverable knowledge, not the historical facts retained by
an observation. A requirement alone cannot supply evidence for an update.

## E4: A CAS receipt proves one applied CAS

An applied result confirms that one CAS took effect. It does not establish a
transaction commit or promise that the installed state remains current when the
reply arrives.

Only an applied backend CAS may construct a receipt. Keep the expected revision,
exact installed observation, and original invocation point bound to that
mutation. Checking the installed state later cannot renew the precondition
proof.

Reads, failed or in-doubt mutations, and plans with no staged changes cannot
become receipts. Conversion to the installed observation remains explicit, and
that observation does not prove the mutation. Do not build an observation from
the expected revision and the installed body, because they identify different
sides of the transition. Do not add `Deref`, `AsRef`, `From`, payload mapping,
or raw-point extraction. Participation in a coordinator batch is a separate
proof owned by that coordinator.

## E5: A cut point lies inside every currentness interval

The shared evidence of a state also keeps the start of its currentness
interval: the completion point of the first operation that observed the
state. It is allocated after the operation completes, so it can only begin an
interval. It never advances a watermark, and a later completion never moves
the start. The persistent cache stores the start with the watermark, and a
reopened timeline follows both, so a start can come from an earlier open of
the same cache. A state that the persistent cache supplied with no stored
start has no interval. An absence has no interval either: it has no revision,
so a later absence shares its evidence even if the object existed between
them. When the decoded cache evicts a present state, and at
clean shutdown, the persistent cache keeps a newer watermark for the same path
and revision. It writes it only in the index slot, under the slot's integrity
check, and the reopened timeline follows it too.

A cut search reads the starts and the watermarks only inside the cached-store
module. A planned target is the latest start among several states, raised to
the staleness cutoff and to the monotonic floor. One state needs no start.
A state whose watermark is before the target is checked against the barrier
that the plan captures after the target. That barrier follows the target, so
the target never becomes a requirement. The plan runs after the body, so the
same barrier also serves as the validation barrier when a state changed.

Only a stale read-only transaction raises the floor. A completed cut raises
it to the latest start of its states. A fallback to optimistic validation
raises it to the validation barrier, and a locked validation raises it to a
barrier captured after the locked pass.

The floor exposes its point only to storage modules. Do not convert it to a
barrier or a requirement, and do not raise it from a requirement.

## Review

For a change to these modules, identify the affected rules and check every new
export or conversion. Trace the facts used to construct or advance evidence.

The type contract relies on correct backend ordering and reconciliation in
`cached_store`. Those rules remain in the [cache guide](caching.md). The types
do not by themselves make every read-based decision safe.
