# ADR-111: An owner does not commit while a pending create can land

## Status

Accepted — implemented.

Refines [ADR-080](080-commit-recovery-does-not-depend-on-gc-retention.md): a
create of the pending record that can still land stops the commit. Keeps the
rule of [ADR-104](104-a-blind-wound-does-not-prove-that-the-identity-never-committed.md)
true: a wound that replaces a pending record proves that the identity never
committed.

## Context

A backend can apply a create after it reported `Unavailable` to the caller
([ADR-043](043-causally-coordinated-backend-operations.md)). A create lands
only on an empty path. So a late create of a transaction record can land only
after GC deleted the record.

ADR-080 covers a late create of the committed record, and a late create of the
pending record after GC deleted an acknowledged abort. It does not cover a
late create of the pending record after GC deleted a committed record:

1. The owner sends the first create of its pending record. The backend reports
   `Unavailable`, and it does not apply the create yet.
2. The owner sends the create again, and the second create lands.
3. The owner commits and writes back. After the retention period, GC deletes
   the committed record
   ([ADR-079](079-gc-safety-does-not-depend-on-time.md)).
4. The first create lands on the empty path. Its timestamp is old, so its
   lease has expired.
5. A reader resolves the identity from a leaf observation from before the
   write-back. It finds the expired pending record and replaces it with
   `Wounded`.
6. `Wounded` states that the identity never committed (ADR-104). So the reader
   drops the holder and returns the value from before the commit.

The refresh of the pending record can also send the first create. That create
can still be without an answer when the owner commits.

## Decision

**An identity whose pending create can still land does not commit.** The
owner counts each create of its pending record from the time that it prepares
the create. A create leaves the count only when it landed or when the backend
rejected it. A create that ended with another error, or whose request the
owner dropped, stays in the count.

Before the commit write, the owner stops the refresh and reads the count. If
the count is not zero, the owner does not send the commit write, and the
commit reports that the identity cannot commit, as after a wound. The caller
aborts the identity:

- A transaction renews its identity and replays its body.
- A topology participant ends `Aborted`, as a participant whose join is in
  doubt does ([ADR-106](106-a-collection-record-state-never-repeats.md)).

The refresh counts its create under the same lock as its refresh state. So
after the refresh stops, the count includes each create that the refresh can
still send.

A late create of the pending record can then land only for an identity that
never committed. ADR-080 already accepts that case: the record expires, a peer
wounds it, and the wound is true.

## Limits

A layer below GlassDB can send one create request again, for example an SDK
retry, as in [ADR-109](109-a-created-object-starts-at-a-random-sequence.md).
The owner does not see the second attempt, so it does not count it.

## Consequences

- A wound that replaces a pending record again proves that the identity never
  committed, also after a storage outage during the first pending write.
- A transaction replays its body when a create of its pending record was in
  doubt, or had no answer when the owner committed. The second case needs a
  commit during the round trip of the first refresh create. A topology
  participant ends `Aborted` in the same cases.
- The record format and GC do not change.
- A late create that lands after GC deleted the acknowledged abort leaves an
  expired pending record that nobody acknowledges, as ADR-080 accepts.

## Alternatives considered

- **Keep the committed record.** The committed record states that a pending
  create can still land, and GC never deletes it. It adds a record field, and
  each such record stays stored forever with its writes.
- **Readers treat every wound like a blind wound.** Each reader that drops a
  wounded holder loads the leaf again. It also does not cover an owner that
  pauses longer than the retention period and then reads `Wounded` back.
- **Do not send the create again after `Unavailable`.** The delayed request
  can still land.
- **Make each create of the pending record different, for example with a
  sequence.** The late create lands because the path is empty, not because
  its content is the same as an earlier state.
- **Pin the identity (ADR-059).** A pin only stops the owner from writing
  `Aborted`. It does not stop GC from deleting a committed record.
