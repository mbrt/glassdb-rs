# ADR-096: Release and acknowledge wounds from local knowledge

## Status

Proposed.

Refines [ADR-059](059-pin-foreign-wounds-until-owner-retirement.md): GC can
acknowledge a wound for an owner of its own database instance, from the
retirement proof of that owner. Refines
[ADR-071](071-gc-skips-pending-and-wounded-transactions.md): GC acknowledges
some `Wounded` records, and releases the locks of some others once. Refines
[ADR-095](095-the-record-lists-every-lock-of-the-identity.md): a lease-expiry
wound uses the lock list of the pending record.

## Context

A wound pins a transaction record until its owner acknowledges it (ADR-059).
GC skips `Wounded` and `Pending` records, and it never checks or changes their
resources (ADR-071). So when the owner does not acknowledge, the record stays,
and the locks of the identity stay on keys that nobody writes. A reader drops
a holder of a `Wounded` record at once, so these locks cost no wait. But they
take leaf space, and they keep tombstones from reclamation
([ADR-062](062-splitter-driven-tombstone-reclamation.md)).

A simulation count found three sources:

1. **Live owners that do not acknowledge.** A topology participant whose commit
   loses to a wound logged the failure and stopped. A structural worker writes
   no record, so it did not learn that a peer wounded it. In runs without
   faults, these were the only records that leaked.
2. **Owners that pin their own identity.** When an owner operation is
   unresolved, the owner writes `Wounded` instead of `Aborted` (ADR-059). It
   cannot acknowledge later.
3. **Owners that crashed.** A peer wounds the identity after its lease expires.

A time-free rule cannot tell an owner that never returns from a slow owner, and
ADR-059 rejects a rule that depends on time. So some `Wounded` records must
stay. This decision is about the owners that are alive, and about the locks.

## Decision

### Owners acknowledge every wound that they learn of

A topology participant whose commit fails because of a wound acknowledges the
wound, as the transaction algorithm already does.

### A retirement proof outlives the owner state

An identity that retires without a transaction record, and without an active
or unresolved owner operation, has a retirement proof: no record write of it
can land. The `Monitor` keeps these proofs in a bounded set after it removes
the owner state.

A peer can wound such an identity after it retired, through an old view of one
of its holders. When GC of the same database instance finds the `Wounded`
record, it acknowledges the wound for the owner. Then the ordinary GC rules for
`Aborted` apply.

GC does not look for these records. It uses a proof only when it checks a
record for other reasons. The rule applies to every identity that retires this
way, not only to structural workers.

### GC releases a lock set once when nobody will acknowledge

The database instance that writes a wound keeps a lock set, and sends a GC
hint, in two cases:

- The owner pins its own identity. The set is the lock set of the owner, from
  memory.
- A lease-expiry wound changes a pending record. The set is the lock list of
  that record. It can be older than the lock set of the owner, so it can miss
  locks.

GC releases the set inside its write budget. It tries again after a failed
release, and it drops the set after one release completes. The record stays
pinned, and GC does not check its resources again.

A wound-wait preemption keeps no set. A preempted owner is usually alive and
acknowledges, and a release there would add writes on contended paths.

### A lock request sweeps the leaf locks of dead holders

A lock request that removes an aborted or wounded holder from its own keys also
removes, in the same CAS, the other locks of that holder in the leaf. It does
this only when its database instance already knows the final status of the
holder. The CAS changes the leaf anyway, so this adds no request. A committed
holder keeps its other locks, because write-back or help-forward still needs
them.

### What stays as before

- GC does not wound expired pending records (ADR-071). Readers wound the
  holders that they meet, and the simulation count found few pending holders
  left. Revisit this only with numbers from a real workload.
- The `Wounded` record of an owner that never returns stays (ADR-059).
- The identities of the transaction algorithm get no retirement proof, because
  they retire inside an owner operation.

## Consequences

- Live owners no longer leave pinned wounds in runs without faults.
- The locks of dead identities are released in most cases. A lock that stays
  is on a leaf that no lock request touches.
- Every mechanism is best-effort. A restart, an eviction from a bounded set, a
  dropped GC hint, or a wound write that was in doubt and landed loses one
  acknowledgement or one release. A wound of a structural worker that lands
  after a restart of its instance stays pinned. Each loss leaves the state as
  it was before this decision, and none affects correctness.
- A lock that an unresolved operation adds after a release stays until a lock
  request on the same leaf removes it. It costs no grace wait.
- GC acts on a `Wounded` record only from the knowledge of its own instance,
  with at most one acknowledgement or one completed release. The repeated cost
  that ADR-071 rejects does not come back.
- The `Monitor` keeps two more bounded sets in memory, and it reports GC
  candidates.

## Alternatives considered

- **Read the record when a structural worker retires.** This costs one request
  for each worker, most of which nobody wounds. It still misses a wound that
  lands after the read.
- **Write a final record when a structural worker retires.** The record fences
  a later wound, but it costs one write for each worker and a later GC delete.
- **Release the locks at every wound, including wound-wait preemption.** This
  adds writes on contended paths for owners that acknowledge anyway.
- **Release on every encounter of a `Wounded` record.** ADR-071 rejects this
  repeated cost.
- **Keep the proofs and the lock sets in one bounded set.** Structural workers
  retire often, so they would evict the rare lock sets of pinned identities.
- **Keep a lock set on every wound that an instance observes, not only on its
  own confirmed wound write.** This also covers a wound write that was in
  doubt. But several instances that race to wound the same record would each
  release the same locks.
- **Let GC wound expired pending records.** This spends requests on records
  that no workload needs (ADR-071).
