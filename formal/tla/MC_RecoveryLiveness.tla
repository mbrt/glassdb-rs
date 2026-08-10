----------------------- MODULE MC_RecoveryLiveness -----------------------
EXTENDS MC_RecoveryLifecycle

CONSTANTS Holder, Waiter, WatchedKey

ASSUME
    /\ Holder \in Attempts
    /\ Waiter \in Attempts
    /\ Holder # Waiter
    /\ WatchedKey = Required(Waiter)

\* The environment remains healthy after the selected holder crashes: the
\* waiter is not also crashed, observer age advances, and enabled observer and
\* cleanup work is weakly fair.  Fixed roles remove irrelevant identity
\* interleavings from this temporal check.
CrashedPendingNext ==
    \/ DispatchAcquire(Holder)
    \/ ApplyAcquire(Holder)
    \/ MaterializePending(Holder)
    \/ RefreshPending(Holder)
    \/ Crash(Holder)
    \/ ObserveConflict(Waiter, Holder)
    \/ ObserveProgress(Waiter, Holder)
    \/ AdvanceObserverAge(Waiter, Holder)
    \/ ExpireObserved(Waiter, Holder)
    \/ ObserveFinal(Waiter, Holder)
    \/ ReleaseTerminalLock(Holder, WatchedKey)

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
