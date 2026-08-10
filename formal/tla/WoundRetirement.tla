-------------------- MODULE WoundRetirement --------------------
EXTENDS Naturals, TLC

CONSTANTS Owner, Wounder, Key, NoHolder, OwnerPriority, WounderPriority,
          TombstoneHorizon

Actors == {Owner, Wounder}
Keys == {Key}
OwnerPhases ==
    {"Starting", "Running", "Suspended", "Resumed", "Retiring", "Abandoned", "Done"}
Statuses == {"Absent", "Pending", "Wounded", "Committed", "Aborted"}

ASSUME
    /\ Owner # Wounder
    /\ NoHolder \notin Actors
    /\ WounderPriority < OwnerPriority
    /\ TombstoneHorizon \in Nat \ {0}

\* This focused composition checks ADR-059's durable fence between a lazy
\* holder owner, a foreign wound, owner retirement, and transaction-object GC.
\* Owner denotes one fixed transaction identity; a legitimate retry uses a
\* fresh identity outside this model.
\*
\* wound_history and commit_history are monotonic observational ghosts. They
\* never enable a protocol action and retain decisions after physical GC.
\* ever_materialized, final_decision, and deleted are the corresponding
\* lifecycle observations retained after object reclamation.

VARIABLES
    owner_phase,
    tx_status,
    lock_holder,
    value_referenced,
    owner_retired,
    marker_age,
    ever_materialized,
    final_decision,
    deleted,
    wound_history,
    commit_history

vars ==
    <<owner_phase,
      tx_status,
      lock_holder,
      value_referenced,
      owner_retired,
      marker_age,
      ever_materialized,
      final_decision,
      deleted,
      wound_history,
      commit_history>>

ObjectReferenced ==
    lock_holder[Key] = Owner \/ value_referenced

Init ==
    /\ owner_phase = "Starting"
    /\ tx_status = "Absent"
    /\ lock_holder = [key \in Keys |-> NoHolder]
    /\ value_referenced = FALSE
    /\ owner_retired = FALSE
    /\ marker_age = 0
    /\ ever_materialized = FALSE
    /\ final_decision = "Absent"
    /\ deleted = FALSE
    /\ wound_history = {}
    /\ commit_history = {}

\* Lazy materialization publishes the transaction identity in a holder before
\* any transaction object exists. The owner-operation guard is already active.
PublishLazyLock ==
    /\ owner_phase = "Starting"
    /\ tx_status = "Absent"
    /\ lock_holder[Key] = NoHolder
    /\ owner_phase' = "Running"
    /\ lock_holder' = [lock_holder EXCEPT ![Key] = Owner]
    /\ UNCHANGED <<tx_status, value_referenced, owner_retired, marker_age,
                   ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

SuspendOwner ==
    /\ owner_phase = "Running"
    /\ lock_holder[Key] = Owner
    /\ tx_status \in {"Absent", "Pending"}
    /\ owner_phase' = "Suspended"
    /\ UNCHANGED <<tx_status, lock_holder, value_referenced, owner_retired,
                   marker_age, ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

\* A dropped owner operation cannot later establish the retirement proof.
AbandonOwner ==
    /\ owner_phase = "Suspended"
    /\ owner_phase' = "Abandoned"
    /\ UNCHANGED <<tx_status, lock_holder, value_referenced, owner_retired,
                   marker_age, ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

MaterializePending ==
    /\ owner_phase = "Running"
    /\ lock_holder[Key] = Owner
    /\ tx_status = "Absent"
    /\ tx_status' = "Pending"
    /\ marker_age' = 0
    /\ ever_materialized' = TRUE
    /\ UNCHANGED <<owner_phase, lock_holder, value_referenced, owner_retired,
                   final_decision, deleted, wound_history, commit_history>>

CommitMaterialized ==
    /\ owner_phase = "Running"
    /\ lock_holder[Key] = Owner
    /\ tx_status = "Pending"
    /\ tx_status' = "Committed"
    /\ owner_phase' = "Done"
    /\ owner_retired' = TRUE
    /\ marker_age' = 0
    /\ final_decision' = "Committed"
    /\ commit_history' = commit_history \cup {Owner}
    /\ UNCHANGED <<lock_holder, value_referenced, ever_materialized, deleted,
                   wound_history>>

\* A foreign contender cannot prove owner quiescence. It therefore creates or
\* CASes the pinned Wounded marker before the holder is released.
ForeignWound ==
    /\ owner_phase \in {"Suspended", "Abandoned"}
    /\ lock_holder[Key] = Owner
    /\ tx_status \in {"Absent", "Pending"}
    /\ ~owner_retired
    /\ WounderPriority < OwnerPriority
    /\ tx_status' = "Wounded"
    /\ marker_age' = 0
    /\ ever_materialized' = TRUE
    /\ wound_history' = wound_history \cup {<<Wounder, Owner>>}
    /\ UNCHANGED <<owner_phase, lock_holder, value_referenced, owner_retired,
                   final_decision, deleted, commit_history>>

\* Cleanup may release physical effects while the Wounded marker stays pinned.
ReleaseTerminalLock ==
    /\ tx_status \in {"Wounded", "Aborted"}
    /\ lock_holder[Key] = Owner
    /\ lock_holder' = [lock_holder EXCEPT ![Key] = NoHolder]
    /\ UNCHANGED <<owner_phase, tx_status, value_referenced, owner_retired,
                   marker_age, ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

\* Write-back replaces the lock edge with the current-writer edge atomically,
\* so a committed value never becomes transiently unreferenced.
WriteBackCommitted ==
    /\ tx_status = "Committed"
    /\ lock_holder[Key] = Owner
    /\ ~value_referenced
    /\ lock_holder' = [lock_holder EXCEPT ![Key] = NoHolder]
    /\ value_referenced' = TRUE
    /\ UNCHANGED <<owner_phase, tx_status, owner_retired, marker_age,
                   ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

OverwriteCommittedValue ==
    /\ tx_status = "Committed"
    /\ value_referenced
    /\ value_referenced' = FALSE
    /\ UNCHANGED <<owner_phase, tx_status, lock_holder, owner_retired,
                   marker_age, ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

\* Saturating finite time may pass while an object is referenced. It never
\* makes Wounded deletable, and acknowledgement resets Aborted's horizon.
AdvanceMarkerAge ==
    /\ tx_status # "Absent"
    /\ marker_age < TombstoneHorizon
    /\ marker_age' = marker_age + 1
    /\ UNCHANGED <<owner_phase, tx_status, lock_holder, value_referenced,
                   owner_retired, ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

ResumeOwner ==
    /\ owner_phase = "Suspended"
    /\ owner_phase' = "Resumed"
    /\ UNCHANGED <<tx_status, lock_holder, value_referenced, owner_retired,
                   marker_age, ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

\* A same-identity commit remains possible only when no durable terminal marker
\* won. Wounded is deliberately excluded from this create/CAS transition.
LateOwnerCommit ==
    /\ owner_phase = "Resumed"
    /\ tx_status \in {"Absent", "Pending"}
    /\ ~owner_retired
    /\ tx_status' = "Committed"
    /\ owner_phase' = "Done"
    /\ owner_retired' = TRUE
    /\ marker_age' = 0
    /\ ever_materialized' = TRUE
    /\ final_decision' = "Committed"
    /\ commit_history' = commit_history \cup {Owner}
    /\ UNCHANGED <<lock_holder, value_referenced, deleted, wound_history>>

\* Retirement closes owner admission and proves that no unresolved operation
\* under this identity can still publish. A crash in Retiring leaves Wounded.
RetireWoundedOwner ==
    /\ owner_phase = "Resumed"
    /\ tx_status = "Wounded"
    /\ ~owner_retired
    /\ owner_phase' = "Retiring"
    /\ owner_retired' = TRUE
    /\ UNCHANGED <<tx_status, lock_holder, value_referenced, marker_age,
                   ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

\* The owner acknowledgement is the only Wounded -> Aborted transition. Its
\* fresh timestamp starts the ordinary aborted-record retention horizon.
AcknowledgeRetiredWound ==
    /\ owner_phase = "Retiring"
    /\ tx_status = "Wounded"
    /\ owner_retired
    /\ tx_status' = "Aborted"
    /\ owner_phase' = "Done"
    /\ marker_age' = 0
    /\ final_decision' = "Aborted"
    /\ UNCHANGED <<lock_holder, value_referenced, owner_retired,
                   ever_materialized, deleted, wound_history, commit_history>>

\* Only old, unreferenced acknowledged terminal objects are reclaimable.
DeleteExpiredFinal ==
    /\ tx_status \in {"Committed", "Aborted"}
    /\ ~ObjectReferenced
    /\ marker_age = TombstoneHorizon
    /\ tx_status' = "Absent"
    /\ deleted' = TRUE
    /\ UNCHANGED <<owner_phase, lock_holder, value_referenced, owner_retired,
                   marker_age, ever_materialized, final_decision, wound_history,
                   commit_history>>

Next ==
    \/ PublishLazyLock
    \/ SuspendOwner
    \/ AbandonOwner
    \/ MaterializePending
    \/ CommitMaterialized
    \/ ForeignWound
    \/ ReleaseTerminalLock
    \/ WriteBackCommitted
    \/ OverwriteCommittedValue
    \/ AdvanceMarkerAge
    \/ ResumeOwner
    \/ LateOwnerCommit
    \/ RetireWoundedOwner
    \/ AcknowledgeRetiredWound
    \/ DeleteExpiredFinal

Spec == Init /\ [][Next]_vars

WR0_TypeOK ==
    /\ owner_phase \in OwnerPhases
    /\ tx_status \in Statuses
    /\ lock_holder \in [Keys -> {NoHolder, Owner}]
    /\ value_referenced \in BOOLEAN
    /\ owner_retired \in BOOLEAN
    /\ marker_age \in 0..TombstoneHorizon
    /\ ever_materialized \in BOOLEAN
    /\ final_decision \in Statuses
    /\ deleted \in BOOLEAN
    /\ wound_history \subseteq (Actors \X Actors)
    /\ commit_history \subseteq Actors

WR1_NoResurrectionAfterForeignWound ==
    <<Wounder, Owner>> \in wound_history => Owner \notin commit_history

WR2_ForeignWoundPinnedUntilRetirement ==
    /\ <<Wounder, Owner>> \in wound_history
    /\ ~owner_retired
    => tx_status = "Wounded"

WR3_WoundAcknowledgedBeforeAbort ==
    /\ <<Wounder, Owner>> \in wound_history
    /\ tx_status = "Aborted"
    => owner_retired

\* The lazy holder-before-object window is legitimate only before this exact
\* identity has ever had an object. Once materialized, no reference may outlive
\* the transaction record.
WR4_MaterializedReferencesRemainPresent ==
    /\ ever_materialized
    /\ ObjectReferenced
    => tx_status # "Absent"

WR5_RecentObjectsRemainPresent ==
    /\ ever_materialized
    /\ marker_age < TombstoneHorizon
    => tx_status # "Absent"

WR6_PendingDurablyTerminalBeforeRelease ==
    tx_status = "Pending" => ObjectReferenced

WR7_DeletedFinalNeverRecreated ==
    deleted =>
        /\ tx_status = "Absent"
        /\ marker_age = TombstoneHorizon
        /\ final_decision \in {"Committed", "Aborted"}

WR8_TerminalDecisionStable ==
    tx_status \in {"Committed", "Aborted"} =>
        tx_status = final_decision

WoundGcGuarantee ==
    /\ WR1_NoResurrectionAfterForeignWound
    /\ WR2_ForeignWoundPinnedUntilRetirement
    /\ WR3_WoundAcknowledgedBeforeAbort
    /\ WR4_MaterializedReferencesRemainPresent
    /\ WR5_RecentObjectsRemainPresent
    /\ WR6_PendingDurablyTerminalBeforeRelease
    /\ WR7_DeletedFinalNeverRecreated
    /\ WR8_TerminalDecisionStable

\* Negative control for the superseded four-status protocol: a foreign actor
\* writes GC-eligible Aborted without proving owner retirement.
ForeignAbortWound ==
    /\ owner_phase \in {"Suspended", "Abandoned"}
    /\ lock_holder[Key] = Owner
    /\ tx_status \in {"Absent", "Pending"}
    /\ ~owner_retired
    /\ WounderPriority < OwnerPriority
    /\ tx_status' = "Aborted"
    /\ marker_age' = 0
    /\ ever_materialized' = TRUE
    /\ final_decision' = "Aborted"
    /\ wound_history' = wound_history \cup {<<Wounder, Owner>>}
    /\ UNCHANGED <<owner_phase, lock_holder, value_referenced, owner_retired,
                   deleted, commit_history>>

MutantNext == Next \/ ForeignAbortWound
MutantSpec == Init /\ [][MutantNext]_vars

\* Negative control: cleanup removes the holder before a pending identity has a
\* durable terminal fence.
ReleasePendingLock ==
    /\ tx_status = "Pending"
    /\ lock_holder[Key] = Owner
    /\ lock_holder' = [lock_holder EXCEPT ![Key] = NoHolder]
    /\ UNCHANGED <<owner_phase, tx_status, value_referenced, owner_retired,
                   marker_age, ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

PendingReleaseMutantSpec ==
    Init /\ [][Next \/ ReleasePendingLock]_vars

\* Negative control: GC removes a final object even though a physical holder or
\* value edge still needs its durable decision.
DeleteReferencedFinal ==
    /\ tx_status \in {"Committed", "Aborted"}
    /\ ObjectReferenced
    /\ marker_age = TombstoneHorizon
    /\ tx_status' = "Absent"
    /\ deleted' = TRUE
    /\ UNCHANGED <<owner_phase, lock_holder, value_referenced, owner_retired,
                   marker_age, ever_materialized, final_decision, wound_history,
                   commit_history>>

ReferencedDeleteMutantSpec ==
    Init /\ [][Next \/ DeleteReferencedFinal]_vars

\* Negative control: recovery sees an absent reclaimed record and guesses a
\* commit result instead of respecting the old final decision.
GuessDeletedObjectCommitted ==
    /\ deleted
    /\ tx_status = "Absent"
    /\ tx_status' = "Committed"
    /\ UNCHANGED <<owner_phase, lock_holder, value_referenced, owner_retired,
                   marker_age, ever_materialized, final_decision, deleted,
                   wound_history, commit_history>>

RecreateDeletedMutantSpec ==
    Init /\ [][Next \/ GuessDeletedObjectCommitted]_vars

=============================================================================
