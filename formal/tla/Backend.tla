----------------------------- MODULE Backend ------------------------------
EXTENDS Common

\* A conditional request and its caller are separate actors. In particular,
\* Unresolved still permits a later backend effect after the caller has
\* returned InDoubt or abandoned its future.
BackendMutationStates ==
    {"None", "Dispatched", "Unresolved", "Applied"}

BackendResultClasses ==
    {"Acknowledged", "Precondition", "Unavailable"}

CanStillApply(state) ==
    state \in {"Dispatched", "Unresolved"}

HasApplied(state) ==
    state = "Applied"

IsDefiniteNoEffect(result) ==
    result = "Precondition"

IsAmbiguous(result) ==
    result = "Unavailable"

=============================================================================
