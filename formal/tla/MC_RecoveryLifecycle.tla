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
    \/ \E attempt \in Attempts : DispatchAcquire(attempt)
    \/ \E attempt \in Attempts : ApplyAcquire(attempt)
    \/ \E attempt \in Attempts : AcknowledgeAcquire(attempt)
    \/ \E attempt \in Attempts : RecoverInstalledAcquire(attempt)
    \/ \E attempt \in Attempts : MaterializePending(attempt)
    \/ \E attempt \in Attempts : RefreshPending(attempt)
    \/ \E attempt \in Attempts : Crash(attempt)
    \/ \E observer \in Attempts, holder \in Attempts :
           ObserveConflict(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ObserveProgress(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           AdvanceObserverAge(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ExpireObserved(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ObserveFinal(observer, holder)
    \/ \E attempt \in Attempts, key \in Keys :
           ReleaseTerminalLock(attempt, key)
    \/ AdvanceTime

ObserverSpec == Init /\ [][ObserverNext]_vars

RenewalNext ==
    \/ \E attempt \in Attempts : DispatchAcquire(attempt)
    \/ \E attempt \in Attempts : ApplyAcquire(attempt)
    \/ \E attempt \in Attempts : AcknowledgeAcquire(attempt)
    \/ \E attempt \in Attempts : RecoverInstalledAcquire(attempt)
    \/ \E observer \in Attempts, holder \in Attempts :
           Wound(observer, holder)
    \/ \E op \in Ops : RenewPublic(op)
    \/ \E attempt \in Attempts : Commit(attempt)
    \/ \E attempt \in Attempts, key \in Keys :
           ReleaseTerminalLock(attempt, key)

RenewalSpec == Init /\ [][RenewalNext]_vars

=============================================================================
