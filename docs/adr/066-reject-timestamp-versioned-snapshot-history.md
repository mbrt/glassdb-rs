# ADR-066: Reject timestamp-versioned snapshot history

## Status

Accepted.

This record replaces the discarded "bounded-staleness snapshot reads" design and
its seven proposed records: ADR-037 through ADR-041, ADR-052, and ADR-055. None
of them was accepted. Their numbers stay reserved and must not be used again.

## Context

GlassDB has no read-only transaction that keeps one fixed database state for
minutes. Such an execution would let a reader scan many keys and collections
without locks and without commit validation. Analytics and backup workloads want
it.

The first proposal for that feature was a timestamp-versioned history:

- Each read-write transaction took a commit timestamp from the backend's own
  clock. The value came from responses that the commit protocol already waited
  for, so it added no backend operation.
- A snapshot read selected a cut. A cut was a timestamp on a fixed grid, chosen
  from a recent server-time observation minus a margin. The margin covered clock
  skew in the backend fleet, the granularity of the reported time, and the gap
  between the stamp and the moment the write applied.
- Each logical key kept certified immutable history versions with a sparse
  index. A reader resolved each key at its cut through that history.
- The collection catalog carried the same timestamps, so collection existence,
  membership, and data shared one cut.
- Retention used no reader pins. Garbage collection kept a window derived from
  the maximum staleness plus the maximum read lifetime, and published a history
  floor so that a too-old cut became an error instead of a wrong answer.

Snapshot support was part of the one database format. No database could refuse
the history cost.

## Decision

Do not implement timestamp-versioned snapshot history.

The reasons are:

- **It removes the one-CAS commit.** Mandatory history needs an immutable
  payload and a certificate for each version. One leaf CAS cannot write them, so
  each small overwrite falls back to the logged protocol. Databases that never
  read a snapshot pay that regression.
- **Cut safety needs an undocumented guarantee.** The margin holds only if the
  clocks in the S3 or Cloud Storage fleet agree inside a stated bound. Neither
  provider documents such a bound, so the correctness of a cut rests on an
  environmental assumption.
- **Real-time order is not proved for transactions that do not conflict.**
  Timestamps propagate through locks. Two transactions that touch different keys
  have no lock between them, so a cut can contain the later one and omit the
  earlier one.
- **A cached read is not free.** An execution served fully from cache must still
  refresh a server-time observation at a bounded interval, or expire. A long
  read therefore cannot run with zero backend operations.
- **It assumed a superseded write path.** The design was written against a
  write-back and fallback behavior that ADR-053 and ADR-054 have since replaced.

## Consequences

- GlassDB keeps strict-serializable read-write transactions only. An application
  that needs a stable multi-key view must build one itself.
- The backend trait keeps its current shape. No backend must report a server
  time, and no deployment must trust the clock of the object store.
- The logless single-leaf commit stays as ADR-061 defines it.
- ADR numbers 037 to 041, 052, and 055 stay reserved for link stability.

## Alternatives considered

- **A global sealed epoch.** Each commit joins a database-wide epoch, and a
  sealed epoch is a cut by construction. It needs no clock, but it puts one
  object in the path of every commit and of every snapshot bind, and one slow
  transaction stops snapshot progress for the whole database.
- **One epoch for each collection.** This removes the database-wide choke point
  but keeps a choke point in each hot collection, and a commit that touches
  several collections pays one coordination step for each of them.
- **A creation-time option to disable snapshots.** This makes two database
  formats, and the objections above still apply to the format that has the
  feature.
