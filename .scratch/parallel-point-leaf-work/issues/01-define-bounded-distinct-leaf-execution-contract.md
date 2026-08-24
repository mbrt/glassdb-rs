# Define the bounded distinct-leaf execution contract

Type: grilling
Status: resolved

## Question

What exact contract must every bounded point-leaf phase obey for admission, maximum active work, stable result and error selection, cancellation, draining started work, stopping work that has not started, and preserving the one-leaf fast path and deterministic simulation behavior?

## Answer

The seam decision in [Place the point-leaf planning and execution seams](03-place-point-leaf-planning-and-execution-seams.md) replaces the earlier active-versus-parked executor contract with bounded `join_all` semantics.

Use a nonzero internal limit `N` for each phase invocation of one transaction. The limit is not shared across the `Database`, so concurrent transactions can each have up to `N` incomplete leaf futures.

The bounded join has this contract:

- Poll futures in the caller's task. Do not spawn one task per leaf.
- Admit futures in stable input order until `N` futures are incomplete. A future counts from its first poll until it completes, including while it waits for time, storage, or foreign transaction progress.
- Admit the next input when an incomplete future completes. Do not expose parking, readmission, replacement, or domain outcome concepts.
- Run every supplied input unless the bounded join itself is dropped. An error or domain terminal outcome does not stop later admission.
- Return every output in input order, independent of completion order. The caller interprets domain outcomes and selects errors from this stable vector.
- Return zero outputs directly. Await one future directly. These paths do not construct the multi-future queue.

Dropping or unwinding the bounded join drops both admitted and stored futures. Each operation keeps its existing cancellation guard and retirement handoff. The bounded join does not add a cleanup system. Panics remain uncaught and propagate.

Domain modules define the input unit before the join. They must combine inputs when physical path sharing, atomic leaf mutation, or routing convergence requires it. Dynamic rerouting stays inside the domain interface that understands it; it is not replacement work in the generic join.

Admission and output order are deterministic. Real backend completion order can vary. Deterministic simulation must reproduce the operation stream for the same schedule and seed.
