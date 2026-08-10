----------------------- MODULE MC_DirectCoreMutants ------------------------
EXTENDS MC_DirectCore

ReverseSequence(sequence) ==
    [index \in 1..Len(sequence) |-> sequence[Len(sequence) - index + 1]]

\* ADR-051/053 regression: a blind direct CAS applied, its marker was
\* superseded before recovery, and the sticky uncertainty is incorrectly
\* ignored. The bad re-fold stages the public operation a second time.
FoldUncertainBlindAsFresh(tx) ==
    /\ coordinator.phase = "Folding"
    /\ coordinator.members = {tx}
    /\ KindOf[tx] = Blind
    /\ clients[tx].uncertain
    /\ clients[tx].direct_count = 1
    /\ ~ExactMarker(tx)
    /\ ~SameUncertainPredecessor(tx)
    /\ coordinator' =
        [coordinator EXCEPT
            !.phase = "Planned",
            !.order = FoldOrder({tx}),
            !.participation =
                [member \in Txns |-> IF member = tx THEN "Staged" ELSE "None"],
            !.decision =
                [member \in Txns |-> IF member = tx THEN "Landed" ELSE "None"],
            !.expected_writer = leaf_writer,
            !.expected_value = leaf_value]
    /\ UNCHANGED
         <<leaf_writer, leaf_value, clients, pending, linearized, environment>>

\* The coordinator forgets its per-key reservation and lets both logless
\* members overwrite one key in the same physical CAS plan.
FoldSameKeyTogether ==
    LET members == coordinator.members
        staged == {tx \in members : DirectCandidate(tx)}
    IN /\ coordinator.phase = "Folding"
       /\ \E first, second \in staged :
             /\ first # second
             /\ KeyOf[first] = KeyOf[second]
       /\ coordinator' =
            [coordinator EXCEPT
                !.phase = "Planned",
                !.order = FoldOrder(members),
                !.participation = PlannedParticipation(members, staged),
                !.decision = PlannedDecisions(members, staged),
                !.expected_writer = leaf_writer,
                !.expected_value = leaf_value]
       /\ UNCHANGED
            <<leaf_writer, leaf_value, clients, pending, linearized, environment>>

\* A shared unavailable result is incorrectly copied to every batch member,
\* including a same-key member that staged no bytes in that CAS.
TransferUncertaintyToSkipped ==
    LET staged == StagedMembers
        skipped == coordinator.members \ staged
    IN /\ coordinator.phase = "Awaiting"
       /\ skipped # {}
       /\ clients' =
            [tx \in Txns |->
                IF tx \in coordinator.members
                THEN [clients[tx] EXCEPT
                        !.uncertain = TRUE,
                        !.uncertain_pre_writer = coordinator.expected_writer,
                        !.uncertain_pre_value = coordinator.expected_value]
                ELSE clients[tx]]
       /\ coordinator' = [coordinator EXCEPT !.phase = "Folding"]
       /\ UNCHANGED <<leaf_writer, leaf_value, pending, linearized, environment>>

\* A read-modify-write whose own CAS is still uncertain is downgraded to a
\* replay after another writer moves the leaf.
ReplayUncertainMember(tx) ==
    /\ coordinator.phase = "Folding"
    /\ coordinator.members = {tx}
    /\ KindOf[tx] = Rmw
    /\ clients[tx].uncertain
    /\ ~ExactMarker(tx)
    /\ ~SameUncertainPredecessor(tx)
    /\ clients' =
        [clients EXCEPT ![tx].phase = "Replay", ![tx].result = "Replay"]
    /\ coordinator' = InitialCoordinator
    /\ UNCHANGED <<leaf_writer, leaf_value, pending, linearized, environment>>

\* A youngest-first fold invalidates the monotonic no-backtracking argument.
FoldYoungestFirst ==
    LET members == coordinator.members
        staged == PlannedStaged(members)
    IN /\ coordinator.phase = "Folding"
       /\ Cardinality(members) > 1
       /\ coordinator' =
            [coordinator EXCEPT
                !.phase = "Planned",
                !.order = ReverseSequence(FoldOrder(members)),
                !.participation = PlannedParticipation(members, staged),
                !.decision = PlannedDecisions(members, staged),
                !.expected_writer = leaf_writer,
                !.expected_value = leaf_value]
       /\ UNCHANGED
            <<leaf_writer, leaf_value, clients, pending, linearized, environment>>

\* A caller incorrectly turns an unresolved direct request into a logged
\* fallback. The backend then applies the old request, and the independently
\* armed fallback commits the same public operation a second time.
StartFallbackWithPending(tx) ==
    /\ pending.state = "Unresolved"
    /\ tx \in pending.members
    /\ clients[tx].phase = "Done"
    /\ clients[tx].result \in {"InDoubt", "Abandoned"}
    /\ clients[tx].direct_count = 0
    /\ clients[tx].fallback_count = 0
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Locked",
            ![tx].result = "Fallback",
            ![tx].uncertain = FALSE,
            ![tx].tx_object = TRUE,
            ![tx].lock_held = TRUE]
    /\ UNCHANGED <<leaf_writer, leaf_value, coordinator, pending,
                   linearized, environment>>

CommitFallbackAfterLateDirect(tx) ==
    LET key == KeyOf[tx]
        value == FallbackValue(tx)
    IN /\ clients[tx].phase = "Locked"
       /\ clients[tx].lock_held
       /\ clients[tx].tx_object
       /\ clients[tx].direct_count = 1
       /\ clients[tx].fallback_count = 0
       /\ leaf_writer' = [leaf_writer EXCEPT ![key] = tx]
       /\ leaf_value' = [leaf_value EXCEPT ![key] = value]
       /\ clients' =
            [clients EXCEPT
                ![tx].phase = "Done",
                ![tx].result = "Success",
                ![tx].fallback_count = @ + 1,
                ![tx].committed_value = value,
                ![tx].lock_held = FALSE,
                ![tx].commit_marker = TRUE]
       /\ linearized' = Append(linearized, tx)
       /\ UNCHANGED <<coordinator, pending, environment>>

BlindRestageSpec ==
    Init /\ [][Next \/ \E tx \in Txns : FoldUncertainBlindAsFresh(tx)]_vars

SameKeyExclusionSpec ==
    Init /\ [][Next \/ FoldSameKeyTogether]_vars

UncertaintyAttributionSpec ==
    Init /\ [][Next \/ TransferUncertaintyToSkipped]_vars

UncertainReplaySpec ==
    Init /\ [][Next \/ \E tx \in Txns : ReplayUncertainMember(tx)]_vars

FoldOrderSpec ==
    Init /\ [][Next \/ FoldYoungestFirst]_vars

PendingFallbackSpec ==
    Init /\ [][Next
               \/ (\E tx \in Txns : StartFallbackWithPending(tx))
               \/ (\E tx \in Txns : CommitFallbackAfterLateDirect(tx))]_vars

=============================================================================
