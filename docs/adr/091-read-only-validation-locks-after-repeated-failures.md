# ADR-091: Read-only validation locks after repeated failures

## Status

Accepted.

Refines the read-only fast path of
[ADR-020](020-commit-write-back-protocol.md#fast-paths), where one invalidated
read escalated the replays to read locks.

## Context

A read-only transaction validates its reads optimistically. The validation of
the reads of a body that returns an error is the same read-only validation. If
a read is invalidated, the transaction replays its body. Before this decision, the
replay used locked validation: it installed read locks on its keys and kept
them until write-back released them. The escalation bounds the replays of a
reader under sustained churn.

On a hot leaf, one read lock can start a state that does not end:

1. A reader misses by a small margin: a write lands between its read and its
   validation. Its replay installs read locks.
2. A direct commit on the leaf finds the live holder. A direct commit never
   waits for a holder ([ADR-061](061-atomic-logless-single-leaf-commits.md)), so
   the writer falls back to a locked commit and installs write locks.
3. While those locks are live, more direct commits fall back. More readers
   read during a lock window, and fail their validation when the holder
   commits.

When the writers arrive faster than the locks clear, the leaf always has a
holder, and the direct commit does not come back. Every writer then pays a
locked commit, which takes more CAS round trips and more time than a direct
commit.

The first near miss needs only a short gap between the read and the
validation. CPU time gives that gap on any host. In virtual time, CPU work
takes no model time, so the gap is zero and the near miss does not occur. A
workload can then be in the direct-commit state in virtual time and in the
locked state in real time.

## Decision

A transaction uses optimistic validation of its reads for each body execution
until a fixed, small number of these validations have failed. Only the replays
after that use locked validation.

- The count covers every optimistic validation of reads only: the validation
  of a read-only transaction, and the validation of the reads of a body that
  returns an error.
- The count belongs to the transaction handle and survives body replays, like
  the validation mode.
- The bound is small and fixed. It is the same as the count of failed parallel
  lock passes before serial acquisition: the two are the points where a
  transaction gives up on its optimistic path.
- The commit of a read-write transaction does not change. Its locked
  validation comes from its locked commit, not from this count.

## Consequences

- A near miss costs one optimistic replay and leaves no lock, so it cannot
  push the writers of the leaf to locked commits.
- A transaction that never engaged installs read locks only when it fails at
  each attempt up to the bound. Near misses alone no longer start the chain on
  a hot leaf. A transaction that engaged in an earlier pass still uses locked
  validation at once.
- A reader under sustained churn replays up to the bound before it locks.
  Each failed attempt costs a body execution and its validation reads, so the
  worst-case latency of such a reader goes up by a few validations.
- A reader that is still invalidated at the bound uses locked validation as
  before, so the bound on its replays stays.
- Other sources of locks on a hot leaf, such as locked commits of transactions
  that touch more than one leaf, can still push direct commits to locked
  commits. This decision removes only the read-only source.

## Alternatives considered

### Add a model-time gap to virtual time

A short sleep in model time between the body and the validation makes virtual
time show the near misses. It changes only the benchmark, and the locked state
then costs both modes, although the engine can avoid it. The gap also does not
make virtual time match real time: readers replay more often than in real
time.

### Let a direct commit wait for one live holder

A direct commit that one live holder blocks could wait for the holder, then
try the direct path again. This breaks the second link of the chain, and it
also helps when the holder is not a reader. It changes the rule of ADR-061 that
a direct commit never waits for a holder, so it needs its own decision. It
does not stop the read locks that start the chain.

### Wait for a pending holder before the validation or the replay

A reader could wait for the pending holders of its keys, through the monitor,
before it validates or replays.

A wait before the validation saves no replay. A pending holder does not
invalidate a point read: the effective writer stays the previous one, and the
reader is ordered before the holder.

A wait before the replay can save one. If another key of the read set has a
pending holder, and that holder commits between the read and the validation
of the replay, the replay fails again. A wait for that holder avoids the
failure. The wait helps only when the read set has pending holders, which
come from locked commits. A hot leaf with direct-commit writers has none, and
its near misses come from direct commits, which a reader cannot see in
advance. The wait also costs at least one status poll for a holder of another
database instance. It stays a possible follow-up, because the bound alone is
simpler and keeps the direct-commit state.

### Never escalate a reader

Optimistic replays alone can starve a reader under sustained churn. Locked
validation stays as the bound.

### Escalate after a time budget

A time budget depends on the latency of the backend and on the load, and it
behaves differently in real time and in virtual time. A count of failed
validations does not.
