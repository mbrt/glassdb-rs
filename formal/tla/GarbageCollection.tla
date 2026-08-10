----------------------- MODULE GarbageCollection -------------------------
EXTENDS FiniteSets, Naturals, TLC

CONSTANTS Txns, RefKinds, LockRef, ValueRef

Statuses == {"Absent", "Pending", "Committed", "Aborted"}
Ages == {"Recent", "Old"}
FinalStatuses == {"Committed", "Aborted"}

ASSUME
    /\ Txns # {}
    /\ RefKinds = {LockRef, ValueRef}
    /\ LockRef # ValueRef

\* TxCore boundary
\* ----------------
\* FROM TxCore: these are ordinary transaction objects with fresh, unreused
\* IDs; terminal decisions are immutable, a committed object's recorded
\* back-reference set is complete, and references do not reappear after being
\* removed.  Inline/direct writer IDs that intentionally have no transaction
\* object are not GC candidates and are outside this module.  The legitimate
\* lazy-create window in which a lock names a not-yet-created pending object is
\* likewise owned by TxCore's missing-object grace and is reduced away here.
\* TO TxCore: a present referenced object and every recent object survive;
\* stale Pending is durably changed to Aborted with a fresh horizon before GC
\* releases its locks; deletion needs a final, old, unreferenced candidate; and
\* a deleted final object can never be recreated with a guessed decision.

VARIABLES
    status,
    age,
    references,
    ever_created,
    final_decision,
    deleted,
    released_pending

vars ==
    <<status,
      age,
      references,
      ever_created,
      final_decision,
      deleted,
      released_pending>>

Init ==
    /\ status = [tx \in Txns |-> "Absent"]
    /\ age = [tx \in Txns |-> "Recent"]
    /\ references = [tx \in Txns |-> {}]
    /\ ever_created = {}
    /\ final_decision = [tx \in Txns |-> "Absent"]
    /\ deleted = {}
    /\ released_pending = FALSE

CreatePending(tx) ==
    /\ tx \notin ever_created
    /\ status[tx] = "Absent"
    /\ status' = [status EXCEPT ![tx] = "Pending"]
    /\ age' = [age EXCEPT ![tx] = "Recent"]
    /\ ever_created' = ever_created \cup {tx}
    /\ UNCHANGED <<references, final_decision, deleted, released_pending>>

AcquireLockReference(tx) ==
    /\ status[tx] = "Pending"
    /\ references' =
        [references EXCEPT ![tx] = @ \cup {LockRef}]
    /\ UNCHANGED <<status, age, ever_created, final_decision, deleted,
                   released_pending>>

RefreshPending(tx) ==
    /\ status[tx] = "Pending"
    /\ age' = [age EXCEPT ![tx] = "Recent"]
    /\ UNCHANGED <<status, references, ever_created, final_decision,
                   deleted, released_pending>>

Commit(tx) ==
    /\ status[tx] = "Pending"
    /\ status' = [status EXCEPT ![tx] = "Committed"]
    /\ age' = [age EXCEPT ![tx] = "Recent"]
    /\ final_decision' =
        [final_decision EXCEPT ![tx] = "Committed"]
    /\ UNCHANGED <<references, ever_created, deleted, released_pending>>

SelfAbort(tx) ==
    /\ status[tx] = "Pending"
    /\ status' = [status EXCEPT ![tx] = "Aborted"]
    /\ age' = [age EXCEPT ![tx] = "Recent"]
    /\ final_decision' = [final_decision EXCEPT ![tx] = "Aborted"]
    /\ UNCHANGED <<references, ever_created, deleted, released_pending>>

\* Write-back replaces the lock edge with the current-writer edge.  It does
\* not make a committed object transiently unreferenced.
WriteBack(tx) ==
    /\ status[tx] = "Committed"
    /\ LockRef \in references[tx]
    /\ references' =
        [references EXCEPT ![tx] = (@ \ {LockRef}) \cup {ValueRef}]
    /\ UNCHANGED <<status, age, ever_created, final_decision, deleted,
                   released_pending>>

OverwriteValue(tx) ==
    /\ status[tx] = "Committed"
    /\ ValueRef \in references[tx]
    /\ references' =
        [references EXCEPT ![tx] = @ \ {ValueRef}]
    /\ UNCHANGED <<status, age, ever_created, final_decision, deleted,
                   released_pending>>

AdvancePastHorizon(tx) ==
    /\ status[tx] # "Absent"
    /\ age[tx] = "Recent"
    /\ age' = [age EXCEPT ![tx] = "Old"]
    /\ UNCHANGED <<status, references, ever_created, final_decision,
                   deleted, released_pending>>

\* The abort is the synchronization point.  Its new timestamp resets the
\* modeled age class, so the tombstone receives a full post-abort horizon.
ForceAbortExpired(tx) ==
    /\ status[tx] = "Pending"
    /\ age[tx] = "Old"
    /\ status' = [status EXCEPT ![tx] = "Aborted"]
    /\ age' = [age EXCEPT ![tx] = "Recent"]
    /\ final_decision' = [final_decision EXCEPT ![tx] = "Aborted"]
    /\ UNCHANGED <<references, ever_created, deleted, released_pending>>

ReleaseAbortedLocks(tx) ==
    /\ status[tx] = "Aborted"
    /\ LockRef \in references[tx]
    /\ references' =
        [references EXCEPT ![tx] = @ \ {LockRef}]
    /\ UNCHANGED <<status, age, ever_created, final_decision, deleted,
                   released_pending>>

DeleteFinal(tx) ==
    /\ status[tx] \in FinalStatuses
    /\ age[tx] = "Old"
    /\ references[tx] = {}
    /\ status' = [status EXCEPT ![tx] = "Absent"]
    /\ deleted' = deleted \cup {tx}
    /\ UNCHANGED <<age, references, ever_created, final_decision,
                   released_pending>>

Next ==
    \/ \E tx \in Txns : CreatePending(tx)
    \/ \E tx \in Txns : AcquireLockReference(tx)
    \/ \E tx \in Txns : RefreshPending(tx)
    \/ \E tx \in Txns : Commit(tx)
    \/ \E tx \in Txns : SelfAbort(tx)
    \/ \E tx \in Txns : WriteBack(tx)
    \/ \E tx \in Txns : OverwriteValue(tx)
    \/ \E tx \in Txns : AdvancePastHorizon(tx)
    \/ \E tx \in Txns : ForceAbortExpired(tx)
    \/ \E tx \in Txns : ReleaseAbortedLocks(tx)
    \/ \E tx \in Txns : DeleteFinal(tx)

Spec == Init /\ [][Next]_vars

GC0_TypeOK ==
    /\ status \in [Txns -> Statuses]
    /\ age \in [Txns -> Ages]
    /\ references \in [Txns -> SUBSET RefKinds]
    /\ ever_created \subseteq Txns
    /\ final_decision \in [Txns -> Statuses]
    /\ deleted \subseteq Txns
    /\ released_pending \in BOOLEAN

GC1_ReferencedObjectsRemainPresent ==
    \A tx \in Txns : references[tx] # {} => status[tx] # "Absent"

GC2_RecentObjectsRemainPresent ==
    \A tx \in ever_created : age[tx] = "Recent" => status[tx] # "Absent"

GC3_PendingAbortedBeforeRelease == ~released_pending

GC4_DeletedFinalNeverRecreated ==
    \A tx \in deleted :
        /\ status[tx] = "Absent"
        /\ age[tx] = "Old"
        /\ final_decision[tx] \in FinalStatuses

GC5_TerminalDecisionStable ==
    \A tx \in ever_created :
        status[tx] \in FinalStatuses =>
            status[tx] = final_decision[tx]

\* The named guarantee imported by TxCore.
TxCoreGcGuarantee ==
    /\ GC1_ReferencedObjectsRemainPresent
    /\ GC2_RecentObjectsRemainPresent
    /\ GC3_PendingAbortedBeforeRelease
    /\ GC4_DeletedFinalNeverRecreated
    /\ GC5_TerminalDecisionStable

\* Negative control: lock cleanup runs before the pending object is durably
\* aborted, allowing another transaction to pass a still-live attempt.
ReleasePendingLocks(tx) ==
    /\ status[tx] = "Pending"
    /\ LockRef \in references[tx]
    /\ references' =
        [references EXCEPT ![tx] = @ \ {LockRef}]
    /\ released_pending' = TRUE
    /\ UNCHANGED <<status, age, ever_created, final_decision, deleted>>

\* Negative control required by the GC boundary: a final object is deleted
\* even though a physical lock still references its decision.
DeleteReferencedFinal(tx) ==
    /\ status[tx] \in FinalStatuses
    /\ references[tx] # {}
    /\ status' = [status EXCEPT ![tx] = "Absent"]
    /\ deleted' = deleted \cup {tx}
    /\ UNCHANGED <<age, references, ever_created, final_decision,
                   released_pending>>

\* Negative control: recovery sees an absent object after GC and invents a
\* commit result instead of preserving the missing final decision boundary.
GuessDeletedObjectCommitted(tx) ==
    /\ tx \in deleted
    /\ status[tx] = "Absent"
    /\ status' = [status EXCEPT ![tx] = "Committed"]
    /\ UNCHANGED <<age, references, ever_created, final_decision, deleted,
                   released_pending>>

MutantSpec ==
    Init /\ [][Next
                 \/ \E tx \in Txns : GuessDeletedObjectCommitted(tx)]_vars

PendingReleaseMutantSpec ==
    Init /\ [][Next
                 \/ \E tx \in Txns : ReleasePendingLocks(tx)]_vars

ReferencedDeleteMutantSpec ==
    Init /\ [][Next
                 \/ \E tx \in Txns : DeleteReferencedFinal(tx)]_vars

=============================================================================
