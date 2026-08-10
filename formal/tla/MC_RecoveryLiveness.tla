----------------------- MODULE MC_RecoveryLiveness -----------------------
EXTENDS MC_RecoveryLifecycle

CONSTANTS Holder, Waiter, WatchedKey

ASSUME
    /\ Holder \in Attempts
    /\ Waiter \in Attempts
    /\ Holder # Waiter
    /\ WatchedKey = Required(Waiter)

\* The environment remains healthy after the selected holder crashes: the
\* waiter is not also crashed, model time is scheduled, and enabled observer
\* and cleanup work is weakly fair.
CrashedPendingNext ==
    \/ \E attempt \in Attempts : DispatchAcquire(attempt)
    \/ \E attempt \in Attempts : ApplyAcquire(attempt)
    \/ \E attempt \in Attempts : AcknowledgeAcquire(attempt)
    \/ \E attempt \in Attempts : RecoverInstalledAcquire(attempt)
    \/ \E attempt \in Attempts : MaterializePending(attempt)
    \/ \E attempt \in Attempts : RefreshPending(attempt)
    \/ Crash(Holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ObserveConflict(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ObserveProgress(observer, holder)
    \/ AdvanceObserverAge(Waiter, Holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ExpireObserved(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ObserveFinal(observer, holder)
    \/ \E attempt \in Attempts, key \in Keys :
           ReleaseTerminalLock(attempt, key)

CrashedPendingFairSpec ==
    /\ Init
    /\ [][CrashedPendingNext]_vars
    /\ WF_vars(ObserveConflict(Waiter, Holder))
    /\ WF_vars(ObserveProgress(Waiter, Holder))
    /\ WF_vars(AdvanceObserverAge(Waiter, Holder))
    /\ WF_vars(ExpireObserved(Waiter, Holder))
    /\ WF_vars(ObserveFinal(Waiter, Holder))
    /\ WF_vars(ReleaseTerminalLock(Holder, WatchedKey))

CrashedPendingConverges ==
    [](/\ phase[Holder] = "Crashed"
       /\ status[Holder] \in {"Absent", "Pending"}
       /\ lock_holder[WatchedKey] = Holder
       => <> /\ status[Holder] = "Aborted"
             /\ lock_holder[WatchedKey] # Holder)

FinalObservationConverges ==
    [](/\ observing[Waiter][Holder]
       /\ status[Holder] \in {"Committed", "Aborted"}
       => <> ~observing[Waiter][Holder])

=============================================================================
