---------------------- MODULE MC_RecoveryLifecycle -----------------------
EXTENDS RecoveryLifecycle

CONSTANTS
    O1,
    O2,
    A1,
    A1R,
    A2,
    K1,
    K2,
    R0,
    R1

ASSUME
    /\ O1 # O2
    /\ A1 # A1R
    /\ A1 # A2
    /\ A1R # A2
    /\ K1 # K2
    /\ R0 # R1

PilotPublicOf ==
    [attempt \in Attempts |-> IF attempt \in {A1, A1R} THEN O1 ELSE O2]

PilotFirstAttempt ==
    [op \in Ops |-> IF op = O1 THEN A1 ELSE A2]

PilotRenewedAttempt ==
    [attempt \in Attempts |-> IF attempt = A1 THEN A1R ELSE NoAttempt]

NoRenewedAttempt ==
    [attempt \in Attempts |-> NoAttempt]

PilotPriority ==
    [attempt \in Attempts |-> IF attempt \in {A1, A1R} THEN 0 ELSE 1]

RenewalPriorityMap ==
    [attempt \in Attempts |-> IF attempt \in {A1, A1R} THEN 1 ELSE 0]

PilotRequiredKey ==
    [op \in Ops |-> IF op = O1 THEN K1 ELSE K1]

PilotNextRevision ==
    [token \in RevisionTokens |-> IF token = R0 THEN R1 ELSE R0]

\* Focused exhaustive relations keep unrelated recovery dimensions from
\* multiplying one another while retaining arbitrary-length executions.
DelayedRequestNext ==
    \/ \E attempt \in Attempts : DispatchAcquire(attempt)
    \/ \E attempt \in Attempts : ApplyAcquire(attempt)
    \/ \E attempt \in Attempts : MarkAcquireUnavailable(attempt)
    \/ \E attempt \in Attempts : AcquirePreconditionFailed(attempt)
    \/ \E attempt \in Attempts : AcknowledgeAcquire(attempt)
    \/ \E attempt \in Attempts : RecoverInstalledAcquire(attempt)
    \/ \E attempt \in Attempts : Crash(attempt)
    \/ \E key \in Keys : RewriteEquivalentLeaf(key)

DelayedRequestSpec == Init /\ [][DelayedRequestNext]_vars

ObserverNext ==
    \* Fixing the observer and holder breaks identity symmetry without removing
    \* either the absent-record or pending-record expiry path.
    \/ DispatchAcquire(A1)
    \/ ApplyAcquire(A1)
    \/ MaterializePending(A1)
    \/ RefreshPending(A1)
    \/ Crash(A1)
    \/ ObserveConflict(A2, A1)
    \/ ObserveProgress(A2, A1)
    \/ AdvanceObserverAge(A2, A1)
    \/ ExpireObserved(A2, A1)
    \/ AdvanceTime

ObserverSpec == Init /\ [][ObserverNext]_vars

RenewalNext ==
    \* One explicit old/fresh path retains the supersession boundary while
    \* acquisition recovery remains owned by DelayedRequestNext.
    \/ DispatchAcquire(A1)
    \/ ApplyAcquire(A1)
    \/ AcknowledgeAcquire(A1)
    \/ Commit(A1)
    \/ Wound(A2, A1)
    \/ RenewPublic(O1)
    \/ ReleaseTerminalLock(A1, K1)
    \/ DispatchAcquire(A1R)
    \/ ApplyAcquire(A1R)
    \/ AcknowledgeAcquire(A1R)
    \/ Commit(A1R)

RenewalSpec == Init /\ [][RenewalNext]_vars

=============================================================================
