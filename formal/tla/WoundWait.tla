---------------------------- MODULE WoundWait ----------------------------
EXTENDS TLC

CONSTANTS Older, Younger, OlderKey, YoungerKey, NoTx

Txns == {Older, Younger}
Keys == {OlderKey, YoungerKey}
Statuses == {"Pending", "Aborted"}

ASSUME
    /\ Older # Younger
    /\ OlderKey # YoungerKey
    /\ NoTx \notin Txns

\* Focused decision boundary shared by every TxCore lock-bearing object.
\* Each transaction already owns one key and requests the other's key.  A
\* younger requester records a wait edge; an older requester must wound and
\* clear any edge owned by the now-aborted holder.

VARIABLES status, lock_holder, waiting_on, wounded_by

vars == <<status, lock_holder, waiting_on, wounded_by>>

Wanted(tx) == IF tx = Older THEN YoungerKey ELSE OlderKey

IsOlder(requester, holder) ==
    requester = Older /\ holder = Younger

Init ==
    /\ status = [tx \in Txns |-> "Pending"]
    /\ lock_holder = [key \in Keys |-> IF key = OlderKey THEN Older ELSE Younger]
    /\ waiting_on = [tx \in Txns |-> NoTx]
    /\ wounded_by = [tx \in Txns |-> NoTx]

ResolveConflict(requester) ==
    LET holder == lock_holder[Wanted(requester)]
    IN /\ status[requester] = "Pending"
       /\ status[holder] = "Pending"
       /\ requester # holder
       /\ waiting_on[requester] = NoTx
       /\ IF IsOlder(requester, holder)
          THEN /\ status' = [status EXCEPT ![holder] = "Aborted"]
               /\ waiting_on' = [waiting_on EXCEPT ![holder] = NoTx]
               /\ wounded_by' = [wounded_by EXCEPT ![holder] = requester]
          ELSE /\ waiting_on' = [waiting_on EXCEPT ![requester] = holder]
               /\ UNCHANGED <<status, wounded_by>>
       /\ UNCHANGED lock_holder

ObserveAbort(requester) ==
    LET holder == waiting_on[requester]
    IN /\ holder \in Txns
       /\ status[holder] = "Aborted"
       /\ waiting_on' = [waiting_on EXCEPT ![requester] = NoTx]
       /\ UNCHANGED <<status, lock_holder, wounded_by>>

Next ==
    \/ \E requester \in Txns : ResolveConflict(requester)
    \/ \E requester \in Txns : ObserveAbort(requester)

Spec == Init /\ [][Next]_vars

WW0_TypeOK ==
    /\ status \in [Txns -> Statuses]
    /\ lock_holder \in [Keys -> Txns]
    /\ waiting_on \in [Txns -> Txns \cup {NoTx}]
    /\ wounded_by \in [Txns -> Txns \cup {NoTx}]

WW1_WaitGraphAcyclic ==
    ~(waiting_on[Older] = Younger /\ waiting_on[Younger] = Older)

WW2_OnlyOlderWounds ==
    \A holder \in Txns :
        wounded_by[holder] # NoTx => IsOlder(wounded_by[holder], holder)

\* Negative control: reversing the older branch into a wait creates the cycle
\* once the younger requester has already made its legitimate wait decision.
WaitInsteadOfWound(requester) ==
    LET holder == lock_holder[Wanted(requester)]
    IN /\ status[requester] = "Pending"
       /\ status[holder] = "Pending"
       /\ requester # holder
       /\ waiting_on[requester] = NoTx
       /\ IsOlder(requester, holder)
       /\ waiting_on' = [waiting_on EXCEPT ![requester] = holder]
       /\ UNCHANGED <<status, lock_holder, wounded_by>>

MutantSpec ==
    Init /\ [][Next \/ \E requester \in Txns :
                         WaitInsteadOfWound(requester)]_vars

=============================================================================
