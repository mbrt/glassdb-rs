# ADR-107: A merge absorbs only into a target at its boundary

## Status

Accepted — implemented.

Refines [ADR-073](073-merge-nodes-into-right-sibling.md): it adds a
precondition to the absorb of a merge, and corrects the proof that the merge
keeps every key range in exactly one live node.

## Context

A merge of L into its right sibling R has three steps (ADR-073):

1. The worker gates L and keeps its copy of L.
2. The absorb CAS on R adds the entries of that copy to R, lowers the low key
   of R from the boundary `b = high(L)` to `low(L)`, and sets a merge
   reservation.
3. The drain CAS on L, which expects the gated revision of L, makes L drained
   with its right link to R. If the drain fails, the worker abandons the merge:
   it removes the entries below `b` from R and sets the low key of R back to
   `b`.

The proof of ADR-073 says: "no route reaches R below `b` before a drain". It
assumes that the absorb of a worker that holds a gated copy of L happens before
the range of L is in R.

Other gate acquirers can wound a merge worker and take the gate of L (ADR-073,
polite gate acquisition). The simulation, at a fault intensity with long request
delays, found this sequence:

1. Worker A gates L, and stalls on a delay until its lease expires.
2. Worker B wounds A, gates L, and merges L into R completely. R now has the
   low key `low(L)`, no reservation, and a new membership generation.
3. A continues. It reads R at a fresh barrier and records the new generation
   at Ready. Its absorb CAS passes every check of ADR-073 and absorbs A's old
   copy of L into R a second time.
4. A's drain fails, because L changed. A abandons the merge, which removes the
   entries of L's range from R and sets the low key of R back to `b`.

L is drained into R, and R starts at `b`, so no live node covers
`[low(L), b)`. A wound fences only the drain of A. It fences neither the
absorb nor the abandon. The generation fence does not help either, because A
read the generation after B's merge.

## Decision

The absorb CAS on R stops unless the low key of R is the boundary `b` of the
merge. The worker also cancels the merge before Ready when its target does not
start at `b`.

Each check is enough by itself. The check in the CAS is atomic. The check
before Ready reads the low key and the generation that Ready records from the
same state of R, and each absorb into R advances the generation of R, because
it sets a merge reservation (ADR-073). So when the check before Ready passes,
the generation check of the CAS stops any later stale absorb. The worker keeps
both checks: the early one cancels sooner, and the one in the CAS does not
depend on that argument.

The precondition is sound because the low key of R tells whether R holds the
range of L:

- Before an absorb of L, R starts at `b`. A split of R keeps its low key, and a
  merge of R into its own right sibling gives that node the low key of R.
- After an absorb of L, R starts at `low(L) < b`. Only the abandon of that same
  absorb sets it back to `b`, and the abandon also removes the range of L from
  R.

So an absorb lands only while the range of L is not in R, and the proof of
ADR-073 holds again: no route reaches R below `b` before a drain of the absorb
that lowered its low key.

## Consequences

- A merge worker whose gate was revoked cannot absorb a merge that already
  landed, so its abandon cannot remove the range of L from R.
- Recovery rule 1 of ADR-073 ("L drained with right link R means the merge
  landed") cannot tell which merge drained L. Without stale absorbs, a stale
  intent has no absorb on R, so that rule stays correct.
- A merge whose target does not start at its boundary stops without a change,
  and a later sweep decides again from the current tree.

## Alternatives considered

- **Handle the state in the router**, for example by a retry from the parent or
  a right link. The tree really lost a range, so this would hide data loss.
- **Check in the abandon that L is not drained.** The abandon comes too late:
  the stale absorb has already put an old copy of L into R.
- **Fence the absorb on the gate of L.** It needs a check of L in the same CAS
  as the change of R, which the store cannot do. The low key of R carries the
  same fact in the node that the CAS changes.
