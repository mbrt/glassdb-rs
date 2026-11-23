# ADR-104: A blind wound does not prove that the identity never committed

## Status

Accepted.

Refines [ADR-079](079-gc-safety-does-not-depend-on-time.md): it gives the reader
rule for the leaked wounds and the stray holders of that ADR. Refines
[ADR-024](024-hold-and-wait-conflict-resolution.md): the lazy pending record
stays, and this decision states what a wound at an empty path proves.

## Context

A transaction does not write its record before it takes its locks (ADR-024). A
short transaction writes its record once, as committed. So a database instance
that finds a held lock and no transaction record cannot tell two cases apart:

1. The holder did not create its record yet, or it stopped before it did.
2. The holder reached a final status, and GC deleted its record.

A wound at the empty path is the fence for case 1. It creates a `Wounded`
record, so a late create of the owner fails. In case 2 the same wound states
`Wounded` for an identity that can have committed. ADR-079 calls this a leaked
wound and accepts it as harmless, because "readers do not use stale results".

That argument does not hold. A leaked wound comes from a leaf observation that
still names the holder, and it misleads the instances that resolve the holder
from such an observation later:

- A writer with a cached leaf from before the write-back of a committed holder
  resolves the holder. It finds no record, and it wounds the holder. Its lock
  CAS then fails, but the wound stays.
- A stale view resolves the holder from an earlier leaf state that it keeps
  (ADR-103). The holder now reads as wounded, so a scan drops a key that the
  holder created, at a cut point after the commit point of the holder. A scan
  validates at a fresh barrier only when a membership holder is pending
  (ADR-097), so nothing corrects the result.
- An instance that knows the holder as committed finds the record gone. If a
  current leaf still names the holder as a stray holder (ADR-095), each
  resolution of the key retries without end.
- An owner whose commit write was in doubt reads its record back and finds the
  wound. It takes the wound as a lost commit. If the commit had landed and GC
  had deleted it, the owner replays a committed body, or acknowledges `Aborted`
  for an identity that committed.

No check before the wound removes the cause. The wounder can read the current
leaf at a fresh barrier and see the holder there, but it can pause before its
create lands. Meanwhile the holder can commit, its write-back can remove it from
the leaf, and GC can delete its record. The create then lands at an empty path
again. A create on an empty path cannot tell an object that never existed from
one that GC deleted.

## Decision

### Two kinds of wound

A wound that replaces a pending record proves that the identity never committed.
The pending record shows that the identity had not committed, and the
conditional replace fences it.

A wound that creates the record is a **blind wound**. It proves only that the
identity cannot commit after it. The identity can have committed before, and GC
can have deleted its record.

The two kinds are separate statuses: `Wounded` and `BlindWounded`. Code that
treats them in the same way names both, so each decision is explicit. The
lifecycle allows `BlindWounded` only as a create, and `Wounded` only as a
replace of a pending record. Any other wound is a caller error, not a race. The
wounder chooses the kind from the observation that its write expects.

GC keeps a blind wound until the owner acknowledges it, as for any wound
(ADR-059).

### Who can treat a blind wound as a wound

A decision taken from a current object can treat a blind-wounded identity like a
wounded one. GC deletes a record only when no current object refers to the
identity (ADR-079). So a reference in a current object holds no committed effect
of a blind-wounded identity, and removing it is correct. A decision is taken
from a current object when a CAS on that object applies it, or when it reads the
record itself:

- wound-wait, and the removal of the locks, freezes, gates, and drop intents of
  a holder, by a writer or by GC;
- GC eligibility, which reads the record;
- an owner before its commit write: it has not committed, so a blind wound of its
  identity is a wound.

A decision taken from an observation that can be stale applies the rule of the
next section. The owner that reads its own record back after a commit write
applies the rule of the section after it.

### A leaf observation after a blind wound shows no value of the identity

When a database instance sees a blind wound of an identity, or finds the
committed record of an identity reclaimed, it keeps the first requirement after
that observation. Call it the no-value requirement of the identity. The instance
keeps it with the final status of the identity.

A leaf observation that reaches the no-value requirement cannot name the
identity as a holder of a key that the identity wrote:

- If the identity committed, GC deleted its record only after the write-back
  replaced the lock of the identity with the identity as the writer, on each key
  that it wrote (ADR-079). A writer never returns to an entry, so that change
  fences every later lock CAS of the identity on the entry
  ([ADR-081](081-in-doubt-cas-stays-in-doubt-until-fenced.md)).
- The blind wound landed after that deletion, and the instance read it after it
  landed. The no-value requirement follows that read.

On any other key, the identity holds no value. So the identity holds no value in
such an observation, also when the observation still names it, for example as a
stray holder.

A resolution of a holder from a leaf observation then follows this rule:

- If the observation reaches the no-value requirement, drop the holder.
- If it does not, resolve the key again at the no-value requirement. The
  observation can be from before the write-back, where the holder held its
  value.

A leaf observation that names the identity as the writer of a key is always
older than its no-value requirement, because GC does not delete the record of a
writer that a leaf names. Such an observation is resolved again.

An instance keeps the first no-value requirement of an identity, because it is
the least strict one that is correct. If the instance forgets it, a later read
of the record learns a newer one. That costs a reload, and it is still correct.

### Where the rule applies

The rule applies where a holder is resolved from a leaf observation and the
result is used without a CAS on that leaf: point reads, scans, and the
effective states of keys. A writer applies it too. Its lock CAS also fails on a
stale leaf, so the rule only moves the reload earlier.

It also applies to the drop intent of a node. GC deletes the nodes of a dropped
collection before the record of the drop, so a node observation that reaches
the no-value requirement shows that a blind-wounded holder did not commit its
drop. An older node observation is resolved again.

The check of pending membership holders does not change. The result of a scan
comes from the entries in its range. A key that a holder created or deleted has
the lock of the holder on its own entry, so the resolution of that entry
applies the rule.

### The owner reads a blind wound back

An owner whose commit write did not return success reads its record back. A
`Wounded` record replaced its pending record, so the commit lost. A blind wound
landed at an empty path, and what it proves depends on the attempts of the
owner:

| What the owner knows | What the blind wound proves | Outcome |
|---|---|---|
| It saw its own pending record | GC never deletes a pending record. `Aborted` comes only from the owner, or from GC after the owner retired. Only the reclaimed committed record of the owner can have emptied the path. | committed |
| No pending record, and a create of the committed record was in doubt | The wound came before the create, or the create landed and GC reclaimed it | in doubt, as for a missing record (ADR-080) |
| No pending record, and no create was in doubt | The wound came before the first create | lost |

The owner acknowledges a wound as `Aborted` only when it knows that it did not
commit: its commit write did not start, or the read-back shows a lost commit. So
an acknowledgement states only what the owner knows.

### What does not change

- The pending record stays lazy (ADR-024), and the missing-record grace stays.
- Stray holders can still exist (ADR-095), and blind wounds of committed
  identities can still be written. They no longer mislead a reader or the
  owner.
- A cached committed status stays valid after its record is reclaimed. An
  instance does not drop it.

## Consequences

- A stale scan, a point read, or a lock acquisition no longer drops a committed
  holder because of a blind wound, on any database instance.
- An instance that knows a holder as committed resolves a stray holder of it
  after one reload, and not without end.
- An old leaf observation of a holder with a blind wound, or with a reclaimed
  record, costs one leaf reload. A fresh observation costs nothing more.
- The final-status cache keeps one requirement for each blind wound and each
  reclaimed committed record that it saw.
- The stored transaction record has one more status.
- An owner that replaced its pending record and finds its committed record
  reclaimed reports the commit, instead of an abort.
- An owner whose create of the committed record was in doubt, and that reads a
  blind wound back, reports `Error::InDoubt` instead of a replay of the body. A
  contended commit whose create meets a backend error now surfaces in doubt.
- A change that stops late lock CASes, for example a leaf version that never
  repeats, now only removes the cost of stray holders: one grace wait and one
  wound that GC never deletes. Correctness no longer depends on it.

## Alternatives considered

- **Create the pending record before the first lock.** A missing record would
  then always mean a reclaimed record, and nobody would wound an empty path.
  Each locked commit would pay one more sequential write. Stray holders would
  still need the no-value rule.
- **Check the leaf again at a fresh barrier before a wound.** It stops most
  blind wounds of committed identities, but not one whose wounder pauses
  between the check and the create.
- **A wound intent in the leaf, which GC treats as a reference.** It makes a
  blind wound of a committed identity impossible. It needs a new lock state
  for each kind of lock, and changes to write-back, help-forward, and GC. This
  decision instead makes the wound state only what it proves, with no change to
  the locks.
- **A blind field in the transaction record.** A field would let a record mark
  any status as blind, and the store would have to correct it.
- **One wounded status with a blind mark.** A pattern that matches any wound
  ignores the difference without a sign. Commit recovery and the drop-intent
  check did so, and both were wrong for a blind wound. Separate statuses make
  each site decide.
- **Keep the replay after an in-doubt create.** The read-back has a deadline of
  one pending timeout, and GC waits a retention period, which is longer. So
  within the allowed clock skew, GC cannot reclaim the record before the
  read-back. Beyond that skew, a committed body replays, and correctness would
  depend on time.
- **Validate a stale scan at a fresh barrier for any exclusive membership
  holder.** It fixes the scan, but not the point reads and writers that trust a
  blind wound, and it costs a fresh validation for each scan of a leaf between a
  commit and its write-back.
- **Drop the cached committed status when the record is reclaimed.** The retry
  then resolves the holder as an instance with no knowledge does. That trusts
  the blind wound, so a stale scan drops a committed creation.
