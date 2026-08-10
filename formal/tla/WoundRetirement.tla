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

VARIABLES
    owner_phase,
    tx_status,
    lock_holder,
    owner_retired,
    marker_age,
    wound_history,
    commit_history

vars ==
    <<owner_phase,
      tx_status,
      lock_holder,
      owner_retired,
      marker_age,
      wound_history,
      commit_history>>

Init ==
    /\ owner_phase = "Starting"
    /\ tx_status = "Absent"
    /\ lock_holder = [key \in Keys |-> NoHolder]
    /\ owner_retired = FALSE
    /\ marker_age = 0
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
    /\ UNCHANGED <<tx_status, owner_retired, marker_age, wound_history,
                   commit_history>>

SuspendOwner ==
    /\ owner_phase = "Running"
    /\ lock_holder[Key] = Owner
    /\ tx_status \in {"Absent", "Pending"}
    /\ owner_phase' = "Suspended"
    /\ UNCHANGED <<tx_status, lock_holder, owner_retired, marker_age,
                   wound_history, commit_history>>

\* A dropped owner operation cannot later establish the retirement proof.
AbandonOwner ==
    /\ owner_phase = "Suspended"
    /\ owner_phase' = "Abandoned"
    /\ UNCHANGED <<tx_status, lock_holder, owner_retired, marker_age,
                   wound_history, commit_history>>

MaterializePending ==
    /\ owner_phase = "Running"
    /\ lock_holder[Key] = Owner
    /\ tx_status = "Absent"
    /\ tx_status' = "Pending"
    /\ UNCHANGED <<owner_phase, lock_holder, owner_retired, marker_age,
                   wound_history, commit_history>>

CommitMaterialized ==
    /\ owner_phase = "Running"
    /\ lock_holder[Key] = Owner
    /\ tx_status = "Pending"
    /\ tx_status' = "Committed"
    /\ owner_phase' = "Done"
    /\ owner_retired' = TRUE
    /\ commit_history' = commit_history \cup {Owner}
    /\ UNCHANGED <<lock_holder, marker_age, wound_history>>

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
    /\ wound_history' = wound_history \cup {<<Wounder, Owner>>}
    /\ UNCHANGED <<owner_phase, lock_holder, owner_retired, commit_history>>

\* Cleanup may release physical effects while the Wounded marker stays pinned.
ReleaseTerminalLock ==
    /\ tx_status \in {"Wounded", "Aborted"}
    /\ lock_holder[Key] = Owner
    /\ lock_holder' = [lock_holder EXCEPT ![Key] = NoHolder]
    /\ UNCHANGED <<owner_phase, tx_status, owner_retired, marker_age,
                   wound_history, commit_history>>

\* Saturating finite time can pass around either marker. It never makes an
\* unacknowledged Wounded record deletable.
AdvanceMarkerAge ==
    /\ tx_status \in {"Wounded", "Aborted"}
    /\ lock_holder[Key] = NoHolder
    /\ marker_age < TombstoneHorizon
    /\ marker_age' = marker_age + 1
    /\ UNCHANGED <<owner_phase, tx_status, lock_holder, owner_retired,
                   wound_history, commit_history>>

ResumeOwner ==
    /\ owner_phase = "Suspended"
    /\ owner_phase' = "Resumed"
    /\ UNCHANGED <<tx_status, lock_holder, owner_retired, marker_age,
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
    /\ commit_history' = commit_history \cup {Owner}
    /\ UNCHANGED <<lock_holder, marker_age, wound_history>>

\* Retirement closes owner admission and proves that no unresolved operation
\* under this identity can still publish. A crash in Retiring leaves Wounded.
RetireWoundedOwner ==
    /\ owner_phase = "Resumed"
    /\ tx_status = "Wounded"
    /\ ~owner_retired
    /\ owner_phase' = "Retiring"
    /\ owner_retired' = TRUE
    /\ UNCHANGED <<tx_status, lock_holder, marker_age, wound_history,
                   commit_history>>

\* The owner acknowledgement is the only Wounded -> Aborted transition. Its
\* fresh timestamp starts the ordinary aborted-record retention horizon.
AcknowledgeRetiredWound ==
    /\ owner_phase = "Retiring"
    /\ tx_status = "Wounded"
    /\ owner_retired
    /\ tx_status' = "Aborted"
    /\ owner_phase' = "Done"
    /\ marker_age' = 0
    /\ UNCHANGED <<lock_holder, owner_retired, wound_history, commit_history>>

DeleteExpiredAbort ==
    /\ tx_status = "Aborted"
    /\ lock_holder[Key] = NoHolder
    /\ marker_age = TombstoneHorizon
    /\ tx_status' = "Absent"
    /\ UNCHANGED <<owner_phase, lock_holder, owner_retired, marker_age,
                   wound_history, commit_history>>

Next ==
    \/ PublishLazyLock
    \/ SuspendOwner
    \/ AbandonOwner
    \/ MaterializePending
    \/ CommitMaterialized
    \/ ForeignWound
    \/ ReleaseTerminalLock
    \/ AdvanceMarkerAge
    \/ ResumeOwner
    \/ LateOwnerCommit
    \/ RetireWoundedOwner
    \/ AcknowledgeRetiredWound
    \/ DeleteExpiredAbort

Spec == Init /\ [][Next]_vars

WR0_TypeOK ==
    /\ owner_phase \in OwnerPhases
    /\ tx_status \in Statuses
    /\ lock_holder \in [Keys -> {NoHolder, Owner}]
    /\ owner_retired \in BOOLEAN
    /\ marker_age \in 0..TombstoneHorizon
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
    /\ wound_history' = wound_history \cup {<<Wounder, Owner>>}
    /\ UNCHANGED <<owner_phase, lock_holder, owner_retired, commit_history>>

MutantNext == Next \/ ForeignAbortWound
MutantSpec == Init /\ [][MutantNext]_vars

=============================================================================
