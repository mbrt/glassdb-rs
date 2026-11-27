# ADR-109: A created object starts at a random sequence

## Status

Accepted — implemented.

Refines [ADR-105](105-a-node-state-never-repeats.md) and
[ADR-106](106-a-collection-record-state-never-repeats.md): a node or a
collection record that is created again does not repeat its first state.

## Context

ADR-105 and ADR-106 make each state of a node and of a collection record
unique while the object exists. A create stores the sequence 0. On the memory
backend and on S3, a revision depends only on the content, so two creates of
the same content write the same state. Both ADRs accept this as a limit,
because they assume that only an object that nothing reads is created again.

The prepared collection breaks that assumption. Each commit pass of a
transaction that creates a collection sends the creates of its collection
record and of its tree root again. The simulation shows this sequence:

1. A commit pass creates the collection record. A lock CAS that expects the
   created state is in doubt.
2. A later commit pass sends the create again, and the create is in doubt.
3. The abort reclaims the prepared collection and deletes its record.
4. The create of step 2 lands late, and writes the state of step 1 again.
5. The lock CAS of step 1 lands, and leaves a stray directory holder.

## Decision

**A create starts the sequence at a random value.** The node store and the
collection store take the start value from the entropy source of the process
for each create request. Each replace stores the sequence of the expected
state plus one, and wraps at the end of the range.

Two create requests for the same path then write different states, so a CAS
that expects the state of one create cannot land on the state of another one.

**The sequence is a 32-bit value.** Its varint encoding takes up to 5 bytes, so
the size of a node or a record changes by a few bytes with its sequence. The
split headroom covers this.

A later object on the same path reaches the sequence of a state of an earlier
one after a random number of changes. After n changes of the later object, the
probability that this happened is about `n/2^32`. A late CAS lands only if the
content of that state is also the same as the expected state.

In the simulation, the entropy source is deterministic, so a seed still
replays the same run.

## Limits

A layer below GlassDB can send one create request again, for example an SDK
retry. If the object is deleted between the two attempts, both attempts land
and write the same state.

## Consequences

- A node or a collection record that is created again does not repeat a
  state, so the limits of ADR-105 and ADR-106 do not apply.
- The sequence no longer counts the changes since the creation. Only its
  equality has a meaning.
- A node and a collection record are up to 6 bytes larger at creation. Before,
  a create stored 0, which takes no bytes.
- A size decision near a threshold can depend on the random start, by up to
  6 bytes.

## Alternatives considered

- **Create a prepared collection once per transaction.** It removes this
  cause, but every other path that is created again keeps the limit, and each
  new create path must prove that it is created once.
- **Fence the prepared record before the reclaim.** The fence must wait for
  every in-doubt create, which an outage can make unbounded.
