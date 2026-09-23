# Storage evidence rules

Two type modules require stricter correctness review. Paths are relative to
`crates/glassdb-storage/src/`:

| Module | Types | Rules |
| --- | --- | --- |
| `timeline.rs` | `Timeline`, `SequencePoint`, `CurrentnessBarrier`, `Requirement` | E1, E2 |
| `cached_store/evidence.rs` | `Observation`, `Revision`, `CasReceipt`, and their result and shared evidence types | E3, E4 |

Review every change to these files, including visibility, conversions, tests,
and moves. Changes to their exports must preserve the same restrictions. The
[cache guide](caching.md) explains the implementation that uses these types;
[CONTEXT.md](../../CONTEXT.md) defines the terms.

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

Neither barriers nor requirements expose their point, including to other
storage modules. Use predicates to check evidence. Do not add raw-point
constructors, getters, serialization, defaults, or conversions from observations
or receipts. Copying a barrier does not capture another barrier. A requirement
states what must be proved and must never become evidence itself.

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

## E4: A CAS receipt proves one definitive mutation

An applied result confirms that one conditional backend mutation took effect. It
does not establish a transaction commit or promise that the installed state
remains current when the reply arrives.

Only a successful backend conditional create or compare-and-swap may construct a
receipt. Keep the expected revision, exact installed observation, and original
invocation point bound to that mutation. Checking the installed state later
cannot renew the precondition proof.

Reads, failed or in-doubt mutations, and plans with no staged changes
cannot become receipts. Conversion to the installed observation remains
explicit. Do not add `Deref`, `AsRef`, `From`, payload mapping, or raw-point
extraction. Participation in a coordinator batch is a separate proof owned by
that coordinator.

## Review

For a change to these modules, identify the affected rules and check every new
export or conversion. Trace the facts used to construct or advance evidence.

The type contract relies on correct backend ordering and reconciliation in
`cached_store`. Those rules remain in the [cache guide](caching.md). The types
do not by themselves make every read-based decision safe.
