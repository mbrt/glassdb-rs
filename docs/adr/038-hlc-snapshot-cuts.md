# ADR-038: Hybrid-logical-clock snapshot cuts

## Status

Proposed.

Constituent decision of the
[snapshot-reads design](../designs/snapshot-reads.md). Requires
[ADR-052](052-backend-server-time-observation.md).

This number previously carried a cooperative sealed-epoch proposal. That model
was rejected before acceptance; see
[Cut definition](../designs/snapshot-reads.md#cut-definition) for the
comparison. Unlike that proposal, this decision leaves ADR-020's commit
sequence and [ADR-027](027-single-rw-parallel-lock-publish.md)'s parallel
single read-write path unchanged.

## Context

GlassDB has no global commit sequence. Transaction identifiers encode
wound-wait priority, and backend content versions order one object. A snapshot
therefore needs some construction that yields a downward-closed prefix of the
existing strict-serializable order.

Building that construction out of a database-wide object makes every commit
write it and every acquisition read it, which contradicts the independence and
scaling properties the rest of the storage layout is built for. The alternative
is a time domain, which used to be unavailable: client clocks cannot be
compared safely. ADR-052 supplies one.

What makes a time domain sufficient here is that the lock protocol already
establishes every serialization edge. Nothing needs to be added to detect
conflicts; only to timestamp them consistently.

## Decision

### Assign a commit timestamp from the backend's clock

Once a transaction holds every lock, it sets its commit timestamp to the
maximum of the server time reported by its own lock-install responses and every
timestamp it observed on the versions and holder records it touched, plus one.
The value is recorded in its holder records as a lower bound while it runs and
frozen into its commit certificate.

Because a server-time observation is generated at or after its operation
applied, the timestamp lands at or after the moment the transaction's intents
became durable. Assignment adds no round trip and no object.

### Propagate across lock conflicts

Every serialization edge passes through a lock, which is what makes the maximum
rule sufficient. A transaction that must acquire a lock another holds either
waits for that holder's outcome, in which case it observes the holder's
timestamp and is pushed above it, or wounds it, in which case the holder aborts
and there is no edge. ADR-020's validate-and-lock takes shared read locks over
the read set, so anti-dependencies are covered by the same argument.

Consequently `ts(T) < ts(U)` for every edge `T -> U`, every prefix of the
timestamp order is downward-closed, and versions of one key strictly increase
because every writer of a key holds its write lock.

### Select a cut from an observation, never from a local clock

A reader takes its cut strictly below a server-time observation it actually
received, discounted by a margin that covers skew within the backend's fleet
and the granularity of its reported time. Any write installing after that
observation carries a strictly greater timestamp and is invisible to the cut;
any write installing before it is visible as a holder on the keys the reader
touches. Local clocks may decide when to resample but must never extrapolate a
cut forward, which would readmit local clock rate into the safety argument.

Admissible cuts are quantized to a fixed grid derived from the policy and
computed identically by every client with no coordination. The grid restores
discrete cuts, which retention coalescing and change logs both need, without
restoring a global sequence.

### Resolve pending holders rather than skipping them

A reader encountering a holder whose timestamp lower bound is at or below its
cut must resolve that holder's outcome; a lower bound above the cut proves the
writer invisible. This reuses the resolution every strict read already
performs, and a holder old enough to matter is also past its lease.

A cached observation of a leaf may serve a cut only if its own watermark is at
or after the cut plus the margin. Otherwise a write could have landed below the
cut after the observation was taken.

### Bound commit age

A transaction must not commit with a timestamp older than a bounded commit age,
and any peer may durably abort one that exceeds it. The bound covers only the
window between lock completion and the commit CAS, not the user body.

This is not required for cut correctness, which readers obtain by resolving
holders. It exists so a grid slot can be declared closed, which retention
coalescing and per-slot change logs require. Its trigger is the transaction's
own age, so it cannot abort a writer because an unrelated transaction is slow.

## Consequences

- Acquiring a cut performs no backend operation beyond having a recent
  observation, which an active client already has. There is no admission
  structure, no fence, no control record, no sealing, and no global frontier.
- No object is written by every commit or read by every acquisition, so
  transactions on disjoint keys never interact through the snapshot mechanism.
- The commit critical path is unchanged, so ADR-027 remains in force and the
  performance question narrows to writing and retaining history.
- Cut safety rests on the backend's clock rather than on a conditional write. A
  sealed frontier could not be corrupted by any clock; this can, if the
  backend's fleet skew exceeds the margin.
- Freshness is asserted from an observation rather than proved by a fence, and
  a cut is no longer an exact set of transactions fixed by CAS ordering. Precise
  incremental change capture between two cuts becomes harder.
- Readers may occasionally resolve a pending holder, which strict reads already
  do, instead of being guaranteed a fully resolved cut by sealing.

## Alternatives considered

Global sealed epochs and scope-limited per-collection epochs are compared
against this decision in
[Cut definition](../designs/snapshot-reads.md#cut-definition). Both were
rejected for making a database-wide object part of every commit and every
acquisition. The alternatives below are within the timestamp family.

### Commit-wait

A writer could wait out the clock uncertainty before releasing its locks, which
makes the timestamp order agree with real time and would let cuts be taken at
the present instant rather than behind a margin. It puts the uncertainty
interval on every commit's critical path, which is the cost this decision
exists to avoid, and it is unnecessary: read-write transactions already obtain
strict serializability from two-phase locking. Timestamps here define cuts for
read-only executions, nothing more.

### Uncertainty-interval read restarts

A reader could treat versions within one uncertainty interval above its cut as
ambiguous and restart at a higher timestamp, which removes the need for a
staleness margin. A snapshot execution is explicitly non-restartable and may
run for an hour, so a restart is a failure rather than a retry, and bounded
staleness already places the uncertainty interval behind the cut.

### Timestamps assigned at commit rather than at lock completion

Stamping at the commit CAS would tie the timestamp to the linearization point
directly. A lost commit acknowledgement then leaves a committed transaction
with no timestamp, and whichever helper resolves it would assign a later one,
which can invert an edge and tear a cut. Assigning after lock completion keeps
the timestamp durable in records that already exist before the outcome does.
