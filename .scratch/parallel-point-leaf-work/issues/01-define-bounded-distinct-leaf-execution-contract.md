# Define the bounded distinct-leaf execution contract

Type: grilling
Status: resolved

## Question

What exact contract must every bounded point-leaf phase obey for admission, maximum active work, stable result and error selection, cancellation, draining started work, stopping work that has not started, and preserving the one-leaf fast path and deterministic simulation behavior?

## Answer

Use a nonzero internal limit `N` for each phase invocation of one transaction. The limit is not shared across the `Database`, so concurrent transactions can each have up to `N` active attempts.

Initial work is grouped by current physical leaf path and admitted in stable ascending order. One invocation has at most one ready or active item for a leaf path. A work item becomes **started** when it is first polled. It is **active** while an admitted attempt runs and counts against `N`; it is **parked** while it waits only for time or foreign progress and does not count against `N`; it is **complete** after it produces its leaf outcome. Any physical poll by parked work requires readmission as active work.

An active attempt must complete or park after bounded internal work. It may follow sequential dependencies, but it must not sleep indefinitely, run an unbounded retry loop, or start parallel child work outside the phase limit. Ready started items are admitted before work that has not started, with stable leaf-path order inside each class. A parked item that is not ready cannot block new admission.

Each phase classifies its own outcomes as terminal or continuing. A terminal outcome stops admission of unrelated work that has not started. Every started item must still finish, including parked items after they become ready. The phase receives every started outcome in stable order, and the first terminal outcome in that order controls its result. Retry release can classify all outcomes as continuing so it attempts every held leaf; committed write-back always continues best-effort.

Dynamically discovered replacement work uses the same phase limit and remains part of its parent's started work. It inherits the parent's immutable primary order, with its current leaf path as a stable tie-break. Replacement work that converges on an existing leaf path is combined or queued so the invocation never has two ready or active items for that path.

Normal terminal handling drains started work while the caller still awaits the phase. Cancellation or unwinding is different: it stops admission and drops sibling futures. Each started operation must then finish or perform a retirement handoff through its existing cancellation guard. The bounded execution mechanism does not create a second cleanup system. Panics remain uncaught and propagate.

Leaf futures are polled inside the caller's task. The mechanism does not spawn a task per leaf. Zero work returns directly. One work item is awaited directly without constructing the bounded scheduler or a result queue.

Admission, cutoff, outcome ordering, and result selection are deterministic. Real backend completion order can vary; deterministic simulation must reproduce the operation stream for the same schedule and seed.
