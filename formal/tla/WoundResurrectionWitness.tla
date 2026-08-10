-------------------- MODULE WoundResurrectionWitness --------------------
EXTENDS Naturals, TLC

CONSTANTS Owner, Wounder, Key, NoHolder, OwnerPriority, WounderPriority,
          TombstoneHorizon

Actors == {Owner, Wounder}
Keys == {Key}
OwnerPhases == {"Starting", "Running", "Suspended", "Resumed", "Done"}
Statuses == {"Absent", "Pending", "Committed", "Aborted"}

ASSUME
    /\ Owner # Wounder
    /\ NoHolder \notin Actors
    /\ WounderPriority < OwnerPriority
    /\ TombstoneHorizon \in Nat \ {0}

\* This is an executable witness for the known lazy-holder resurrection bug,
\* not an intended protocol.  The physical object has only the currently
\* implemented Aborted terminal marker; there is deliberately no pinned
\* Wounded status or durable owner-retirement handshake.
\* Owner denotes the original transaction identity, not a process that may
\* later retry under a fresh identity.
\*
\* wound_history and commit_history are monotonic ghost state.  They retain the
\* semantic decisions after GC turns the physical transaction path back into
\* Absent, making resurrection observable at the exact late create-if-absent.

VARIABLES
    owner_phase,
    tx_status,
    lock_holder,
    owner_retired,
    tombstone_age,
    wound_history,
    commit_history

vars ==
    <<owner_phase,
      tx_status,
      lock_holder,
      owner_retired,
      tombstone_age,
      wound_history,
      commit_history>>

Init ==
    /\ owner_phase = "Starting"
    /\ tx_status = "Absent"
    /\ lock_holder = [key \in Keys |-> NoHolder]
    /\ owner_retired = FALSE
    /\ tombstone_age = 0
    /\ wound_history = {}
    /\ commit_history = {}

\* Lazy materialization publishes the transaction identity in a leaf before
\* any transaction object exists.
PublishLazyLock ==
    /\ owner_phase = "Starting"
    /\ tx_status = "Absent"
    /\ lock_holder[Key] = NoHolder
    /\ owner_phase' = "Running"
    /\ lock_holder' = [lock_holder EXCEPT ![Key] = Owner]
    /\ UNCHANGED <<tx_status, owner_retired, tombstone_age, wound_history,
                   commit_history>>

SuspendOwner ==
    /\ owner_phase = "Running"
    /\ lock_holder[Key] = Owner
    /\ tx_status \in {"Absent", "Pending"}
    /\ owner_phase' = "Suspended"
    /\ UNCHANGED <<tx_status, lock_holder, owner_retired, tombstone_age,
                   wound_history, commit_history>>

\* The ordinary slower path may materialize Pending before suspension.  The
\* shortest counterexample deliberately retains the lazy Absent state.
MaterializePending ==
    /\ owner_phase = "Running"
    /\ lock_holder[Key] = Owner
    /\ tx_status = "Absent"
    /\ tx_status' = "Pending"
    /\ UNCHANGED <<owner_phase, lock_holder, owner_retired, tombstone_age,
                   wound_history, commit_history>>

CommitMaterialized ==
    /\ owner_phase = "Running"
    /\ lock_holder[Key] = Owner
    /\ tx_status = "Pending"
    /\ tx_status' = "Committed"
    /\ owner_phase' = "Done"
    /\ owner_retired' = TRUE
    /\ commit_history' = commit_history \cup {Owner}
    /\ UNCHANGED <<lock_holder, tombstone_age, wound_history>>

\* A foreign older contender creates an ordinary Aborted tombstone.  It cannot
\* prove that the suspended owner retired its identity, so owner_retired remains
\* false even though the physical status is terminal for now.
ForeignAbortWound ==
    /\ owner_phase = "Suspended"
    /\ lock_holder[Key] = Owner
    /\ tx_status \in {"Absent", "Pending"}
    /\ ~owner_retired
    /\ WounderPriority < OwnerPriority
    /\ tx_status' = "Aborted"
    /\ tombstone_age' = 0
    /\ wound_history' = wound_history \cup {<<Wounder, Owner>>}
    /\ UNCHANGED <<owner_phase, lock_holder, owner_retired, commit_history>>

ReleaseAbortedLock ==
    /\ tx_status = "Aborted"
    /\ lock_holder[Key] = Owner
    /\ lock_holder' = [lock_holder EXCEPT ![Key] = NoHolder]
    /\ UNCHANGED <<owner_phase, tx_status, owner_retired, tombstone_age,
                   wound_history, commit_history>>

AdvanceTombstoneAge ==
    /\ tx_status = "Aborted"
    /\ lock_holder[Key] = NoHolder
    /\ tombstone_age < TombstoneHorizon
    /\ tombstone_age' = tombstone_age + 1
    /\ UNCHANGED <<owner_phase, tx_status, lock_holder, owner_retired,
                   wound_history, commit_history>>

\* Existing finite-horizon GC treats the foreign wound like an acknowledged
\* abort and removes it even though the old owner has not retired.
DeleteExpiredAbort ==
    /\ tx_status = "Aborted"
    /\ lock_holder[Key] = NoHolder
    /\ tombstone_age = TombstoneHorizon
    /\ tx_status' = "Absent"
    /\ UNCHANGED <<owner_phase, lock_holder, owner_retired, tombstone_age,
                   wound_history, commit_history>>

ResumeOwner ==
    /\ owner_phase = "Suspended"
    /\ owner_phase' = "Resumed"
    /\ UNCHANGED <<tx_status, lock_holder, owner_retired, tombstone_age,
                   wound_history, commit_history>>

\* This is the resumed owner's create-if-absent final write.  It consults only
\* physical state; wound_history is deliberately not an enabling condition.  A
\* pre-wound resume is safe, while the invariant observes a post-deletion one.
LateOwnerCommit ==
    /\ owner_phase = "Resumed"
    /\ tx_status = "Absent"
    /\ ~owner_retired
    /\ tx_status' = "Committed"
    /\ owner_phase' = "Done"
    /\ owner_retired' = TRUE
    /\ commit_history' = commit_history \cup {Owner}
    /\ UNCHANGED <<lock_holder, tombstone_age, wound_history>>

\* If the owner resumes while the tombstone is still present, it can retire
\* safely.  The counterexample instead delays the resume until after deletion.
ObserveAbortAndRetire ==
    /\ owner_phase = "Resumed"
    /\ tx_status = "Aborted"
    /\ owner_phase' = "Done"
    /\ owner_retired' = TRUE
    /\ UNCHANGED <<tx_status, lock_holder, tombstone_age, wound_history,
                   commit_history>>

Next ==
    \/ PublishLazyLock
    \/ SuspendOwner
    \/ MaterializePending
    \/ CommitMaterialized
    \/ ForeignAbortWound
    \/ ReleaseAbortedLock
    \/ AdvanceTombstoneAge
    \/ DeleteExpiredAbort
    \/ ResumeOwner
    \/ LateOwnerCommit
    \/ ObserveAbortAndRetire

Spec == Init /\ [][Next]_vars

WR0_TypeOK ==
    /\ owner_phase \in OwnerPhases
    /\ tx_status \in Statuses
    /\ lock_holder \in [Keys -> {NoHolder, Owner}]
    /\ owner_retired \in BOOLEAN
    /\ tombstone_age \in 0..TombstoneHorizon
    /\ wound_history \subseteq (Actors \X Actors)
    /\ commit_history \subseteq Actors

WR1_NoResurrectionAfterForeignWound ==
    <<Wounder, Owner>> \in wound_history => Owner \notin commit_history

=============================================================================
