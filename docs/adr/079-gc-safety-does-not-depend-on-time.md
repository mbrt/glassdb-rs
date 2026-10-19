# ADR-079: GC safety does not depend on time

## Status

Proposed.

Refines the safety argument of
[ADR-022](022-garbage-collection-mark-sweep.md) and renames its safety horizon.
The GC rules of [ADR-059](059-pin-foreign-wounds-until-owner-retirement.md) and
[ADR-071](071-gc-skips-pending-and-wounded-transactions.md) do not change.
[ADR-078](078-full-gc-and-gc-write-budget.md) depends on this decision when
several GC checks share one currentness barrier.

## Context

ADR-022 calls the wait before GC checks a final transaction record a safety
horizon. The wait is the pending timeout plus the allowed clock skew, from
the last update of the record. ADR-022 argues that the wait prevents GC from
missing a reference that is still in flight.

A wait cannot do that. A database instance can stop for any length of time, for
example while its process or its machine is paused, and then send a request
that it prepared before the pause. A rule that depends on elapsed time, on the
allowed clock skew, or on a local timer fails in that case.

Two owner decisions in commit recovery depended on the wait.
[ADR-080](080-commit-recovery-does-not-depend-on-gc-retention.md) removed that
dependency. This decision states the same rule for GC: GC must be safe without
the wait.

## Decision

### The rule

GC deletes a transaction record only when no stored object needs it:

- No entry names the transaction as its writer. A reader needs the record of a
  writer to read the value, or the status of the writer for validation.
- No entry names the transaction as a holder of a lock on a key that it wrote.
  A reader needs the record to resolve such a holder to a committed value.
- The transaction has no effects on collections that GC has not removed.

Other locks of the transaction can stay in leaves after GC deletes its record.
They do not hold a value of the transaction, and they cost one grace wait (see
[Stray holders](#stray-holders)).

The proof uses only durable statuses and the order of observations in one
database instance. It does not use a wall clock, the allowed clock skew, the
lease, or a local timer.

The proof assumes that a backend applies a conditional mutation only when its
precondition holds when the mutation applies. It does not assume that a
revision never repeats: on S3, equal contents give the same revision
([ADR-043](043-causally-coordinated-backend-operations.md)).

### GC observes the final status before the leaves

For a candidate T, GC:

1. reads the durable status of T, and continues only if it is `Committed` or
   `Aborted` (ADR-071);
2. after that read completes, captures a currentness barrier;
3. requires that each read that proves the absence of a reference reaches that
   barrier. Other reads, for example the routing through index nodes or the
   load before a CAS, can use cached observations, because a later CAS or a
   later read at the barrier corrects them.

A barrier captured before step 1 completes cannot serve T. If GC read a leaf
before T committed and reused that observation, it could miss a lock that T
took later. Several checks can share one barrier only when GC captured it after
the status reads of all of them complete.

### Committed records

A committed transaction takes every lock of its last commit pass before its
commit point. Each kind of reference to T that the rule needs is then covered:

- **Keys that T wrote.** A write-back or a help-forward replaces the lock of T
  with T as the writer of the key, in one CAS. Any instance can do it, and a
  repeated write-back makes no change. So each leaf state after the commit
  point shows T, either as a holder or as the writer. GC routes each recorded
  key to its current leaf, and reads that leaf at the barrier. Splits and
  merges copy entries with their holders, and right links lead to the copies,
  so routing finds the entry.
- **Membership locks.** A leaf state after the commit point shows them in the
  same way. GC releases them through the coordinator (ADR-029).
- **Directory locks and topology participants.** GC does not depend on a leaf
  read for these. It removes each recorded one, and it reads the collection
  record at the barrier when it finds no holder to remove. It deletes the
  record only after each removal completes.
- **Collections that T created, prepared, or dropped.** The recovery manifest
  lists them. GC deletes or keeps their objects from that list before it
  deletes the record.

A late lock CAS of T can land after GC's check, but it cannot add a reference
that the rule needs. On a key that T wrote, write-back sets T as the writer. A
writer never returns to an entry, so that change fences every earlier CAS of T
on the entry ([ADR-081](081-in-doubt-cas-stays-in-doubt-until-fenced.md)). On
another key, a late CAS can only add a lock. That is a stray holder.

### Aborted records

Only the owner of T writes `Aborted`, and only when its lifecycle shows that no
owner operation is active or unresolved (ADR-059). An in-doubt create of the
committed record counts as unresolved (ADR-080). Every other party writes
`Wounded`, and GC never deletes a `Wounded` record.

The lifecycle does not count an in-doubt lock CAS as unresolved. A commit pass
that ends with an error completes its owner operation. So an aborted record
proves that no record write of T can land. It does not prove that no lock CAS
of T can land. Lock CASes of an aborted T are only stray holders, because an
aborted T publishes no value.

### Stray holders

A stray holder is a lock of T in a leaf after GC deleted the record of T, on a
key whose value T does not hold. It has these causes:

- an aborted T whose locks GC did not record or did not release;
- a committed T that took a lock in an earlier commit pass of the same
  identity, before a body replay, and did not take it again in its last pass.
  The record lists the locks of the last pass only;
- a late lock CAS of T that lands after the write-back, when the revision came
  back.

A stray holder cannot make a reader return a value that was not committed. A
reader treats a holder whose record is missing like a pending holder. After the
missing-record grace of the Monitor, a reader or a writer wounds it and
continues. So a stray holder costs one grace wait and one `Wounded` record that
nobody acknowledges. The grace uses time only to decide liveness.

### Readers do not need the wait

A reader that finds a writer's record missing does not use a stale result:

- If the status is final but the record body is gone, the reader reads the leaf
  again after a new barrier (`ValidateRetry`).
- If the status is missing, the reader treats it as pending and resolves the
  key once more after a new barrier.

In both cases, the fresh leaf names the newer writer that let GC delete the
record.

### The retention period

GC still waits before it checks a final record. Call this wait the **retention
period**. It keeps its value: the pending timeout plus the allowed clock skew,
from the last update of the record. It is not a safety rule. It makes two
outcomes less frequent:

- **False `Error::InDoubt`.** An owner that pauses during commit recovery can
  find its own committed record missing after GC deleted it, and then reports
  `InDoubt` (ADR-080).
- **Leaked wounds.** A reader that resolves a holder whose committed record GC
  deleted wounds that identity. The result is a `Wounded` record that nobody
  acknowledges, which stays stored forever. This needs a leaf observation from
  before the write-back, for example a bounded-staleness read. Stray holders
  have the same cost, and the retention period makes it rarer only for those
  that a later write-back or release would remove.

A short retention period, or clocks with more skew than allowed, makes these
outcomes more frequent. It cannot make GC delete a record that the rule
needs, or make a reader return a value that was not committed.

### Pending leases keep using the clock

A reader decides that a pending lease is dead by comparing its timestamp with
the clock, with the allowed clock skew (ADR-021). It then writes `Wounded`.
This decides liveness: a wound only marks the identity as dead, and GC keeps
the record until the owner retires the identity (ADR-059). This decision does
not change it.

## Consequences

- GC safety does not depend on clocks, timers, or pauses. Reviews of GC safety
  check the rules of this decision, not the retention period.
- Stray holders exist, also with the retention period. Each costs one grace
  wait for the next transaction on its key and one `Wounded` record that
  nobody acknowledges. Two changes can reduce them later: record the locks of
  every commit pass of an identity, and make lock CASes on keys that the
  transaction does not write stay in doubt until fenced.
- Several GC checks can share one currentness barrier when GC captures it after
  their status reads complete. ADR-078 uses this.
- An owner that pauses longer than the retention period can still report
  `Error::InDoubt` for a commit, and a reader can still leak a wound. The
  retention period limits how often this happens.
- Committed and aborted records stay stored for at least 45 s with the default
  timing, as before.
- The glossary term "safety horizon" becomes "retention period". Code comments
  and diagnostics that use the old term change with the next change to the code
  that has them.
- Deterministic tests of GC safety must also pass with a retention period of
  zero. `ProtocolTiming` derives the retention period from the pending timeout
  and the allowed clock skew, so such tests need a separate test setting.

## Alternatives considered

- **Keep committed records until the owner acknowledges them.** The owner would
  write one more object for each locked commit. This doubles the transaction
  record writes for a case that needs a lost acknowledgement and a long pause
  at the same time.
- **Resolve absence from the leaves of the owner's writes.** An owner that
  finds no record could look for its writes in the leaves. A later writer can
  already have replaced them, so the result often stays in doubt.
- **Reclaim final records with no wait.** This is safe, but false in-doubt
  outcomes and leaked wounds become more frequent, and garbage does not need to
  be deleted at once.
- **Keep the name safety horizon.** The name says that GC safety depends on
  clocks. Reviews then check the wrong rule.
