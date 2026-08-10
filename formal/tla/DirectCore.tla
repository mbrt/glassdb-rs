---------------------------- MODULE DirectCore -----------------------------
EXTENDS Backend, Integers, TLC

CONSTANTS
    Txns,
    Keys,
    Values,
    Blind,
    Rmw,
    NoKey,
    InitWriter,
    EnvWriter,
    KeyOf,
    KindOf,
    Priority,
    TieRank,
    BlindValue,
    RmwNext,
    DirectAllowed,
    InitialDb

Kinds == {Blind, Rmw}
WriterIds == Txns \cup {InitWriter, EnvWriter}

ClientPhases ==
    {"Direct", "InRound", "Replay", "Fallback", "Locked", "Done"}

MemberResults ==
    {"None", "Success", "Replay", "Fallback", "InDoubt", "Abandoned"}
Participation == {"None", "Staged", "Skipped"}
MemberDecisions == {"None", "Landed", "Replay", "Fallback", "InDoubt"}
CoordinatorPhases == {"Idle", "Folding", "Planned", "Awaiting"}
PendingStates == {"None", "Unresolved"}

ClientType ==
    [ phase                : ClientPhases,
      result               : MemberResults,
      observed_writer      : WriterIds,
      observed_value       : Values,
      attempt_value        : Values,
      committed_value      : Values,
      direct_count         : 0..2,
      fallback_count       : 0..2,
      uncertain            : BOOLEAN,
      uncertain_pre_writer : [Keys -> WriterIds],
      uncertain_pre_value  : [Keys -> Values],
      staged_ever          : BOOLEAN,
      tx_object            : BOOLEAN,
      lock_held            : BOOLEAN,
      commit_marker        : BOOLEAN ]

CoordinatorType ==
    [ phase           : CoordinatorPhases,
      members         : SUBSET Txns,
      order           : Seq(Txns),
      participation   : [Txns -> Participation],
      decision        : [Txns -> MemberDecisions],
      expected_writer : [Keys -> WriterIds],
      expected_value  : [Keys -> Values] ]

EnvironmentType ==
    [ healthy : BOOLEAN,
      used  : BOOLEAN,
      key   : Keys \cup {NoKey},
      value : Values ]

PendingType ==
    [ state           : PendingStates,
      members         : SUBSET Txns,
      order           : Seq(Txns),
      expected_writer : [Keys -> WriterIds],
      expected_value  : [Keys -> Values],
      values          : [Txns -> Values] ]

VARIABLES
    leaf_writer,
    leaf_value,
    clients,
    coordinator,
    pending,
    linearized,
    environment

vars ==
    <<leaf_writer, leaf_value, clients, coordinator, pending, linearized,
      environment>>

ASSUME
    /\ Txns # {}
    /\ Keys # {}
    /\ Values # {}
    /\ Blind # Rmw
    /\ NoKey \notin Keys
    /\ InitWriter \notin Txns
    /\ EnvWriter \notin Txns
    /\ InitWriter # EnvWriter
    /\ KeyOf \in [Txns -> Keys]
    /\ KindOf \in [Txns -> Kinds]
    /\ Priority \in [Txns -> Nat]
    /\ TieRank \in [Txns -> Nat]
    /\ \A a, b \in Txns : a # b => TieRank[a] # TieRank[b]
    /\ BlindValue \in [Txns -> Values]
    /\ RmwNext \in [Values -> Values]
    /\ DirectAllowed \in [Txns -> BOOLEAN]
    /\ InitialDb \in [Keys -> Values]

InitialAttemptValue(tx) ==
    IF KindOf[tx] = Blind
    THEN BlindValue[tx]
    ELSE RmwNext[InitialDb[KeyOf[tx]]]

InitialClient(tx) ==
    [ phase                |-> "Direct",
      result               |-> "None",
      observed_writer      |-> InitWriter,
      observed_value       |-> InitialDb[KeyOf[tx]],
      attempt_value        |-> InitialAttemptValue(tx),
      committed_value      |-> InitialDb[KeyOf[tx]],
      direct_count         |-> 0,
      fallback_count       |-> 0,
      uncertain            |-> FALSE,
      uncertain_pre_writer |-> [key \in Keys |-> InitWriter],
      uncertain_pre_value  |-> InitialDb,
      staged_ever          |-> FALSE,
      tx_object            |-> FALSE,
      lock_held            |-> FALSE,
      commit_marker        |-> FALSE ]

InitialCoordinator ==
    [ phase           |-> "Idle",
      members         |-> {},
      order           |-> <<>>,
      participation   |-> [tx \in Txns |-> "None"],
      decision        |-> [tx \in Txns |-> "None"],
      expected_writer |-> [key \in Keys |-> InitWriter],
      expected_value  |-> InitialDb ]

InitialPending ==
    [ state           |-> "None",
      members         |-> {},
      order           |-> <<>>,
      expected_writer |-> [key \in Keys |-> InitWriter],
      expected_value  |-> InitialDb,
      values          |-> [tx \in Txns |-> InitialAttemptValue(tx)] ]

Init ==
    /\ leaf_writer = [key \in Keys |-> InitWriter]
    /\ leaf_value = InitialDb
    /\ clients = [tx \in Txns |-> InitialClient(tx)]
    /\ coordinator = InitialCoordinator
    /\ pending = InitialPending
    /\ linearized = <<>>
    /\ environment =
        [ healthy |-> FALSE,
          used |-> FALSE,
          key |-> NoKey,
          value |-> CHOOSE value \in Values : TRUE ]

Before(a, b) ==
    \/ Priority[a] < Priority[b]
    \/ /\ Priority[a] = Priority[b]
       /\ TieRank[a] < TieRank[b]

Orders(members) ==
    {order \in [1..Cardinality(members) -> members] :
        /\ SetOfSequence(order) = members
        /\ NoDuplicates(order)}

IsOldestFirst(order) ==
    /\ order \in Seq(Txns)
    /\ \A left, right \in 1..Len(order) :
          left < right => Before(order[left], order[right])

FoldOrder(members) ==
    CHOOSE order \in Orders(members) : IsOldestFirst(order)

ExactMarker(tx) ==
    /\ leaf_writer[KeyOf[tx]] = tx
    /\ leaf_value[KeyOf[tx]] = clients[tx].attempt_value

SameUncertainPredecessor(tx) ==
    leaf_writer[KeyOf[tx]] = clients[tx].uncertain_pre_writer[KeyOf[tx]]

ObservationCurrent(tx) ==
    /\ clients[tx].observed_writer = leaf_writer[KeyOf[tx]]
    /\ clients[tx].observed_value = leaf_value[KeyOf[tx]]

DirectCandidate(tx) ==
    /\ DirectAllowed[tx]
    /\ ~ExactMarker(tx)
    /\ (KindOf[tx] = Blind \/ ObservationCurrent(tx))
    /\ (~clients[tx].uncertain \/ SameUncertainPredecessor(tx))

HasEarlierCandidate(tx, members) ==
    \E other \in members :
        /\ other # tx
        /\ KeyOf[other] = KeyOf[tx]
        /\ DirectCandidate(other)
        /\ Before(other, tx)

PlannedStaged(members) ==
    {tx \in members :
        /\ DirectCandidate(tx)
        /\ ~HasEarlierCandidate(tx, members)}

DecisionFor(tx, staged) ==
    IF tx \in staged
    THEN "Landed"
    ELSE IF ExactMarker(tx)
         THEN "Landed"
         ELSE IF clients[tx].uncertain
              THEN "InDoubt"
              ELSE IF ~DirectAllowed[tx]
                   THEN "Fallback"
                   ELSE IF KindOf[tx] = Rmw /\ ~ObservationCurrent(tx)
                        THEN "Replay"
                        ELSE IF KindOf[tx] = Rmw
                             THEN "Replay"
                             ELSE "Fallback"

PlannedParticipation(members, staged) ==
    [tx \in Txns |->
        IF tx \in members
        THEN IF tx \in staged THEN "Staged" ELSE "Skipped"
        ELSE "None"]

PlannedDecisions(members, staged) ==
    [tx \in Txns |->
        IF tx \in members THEN DecisionFor(tx, staged) ELSE "None"]

StagedMembers ==
    {tx \in coordinator.members : coordinator.participation[tx] = "Staged"}

DecisionPhase(decision) ==
    CASE decision = "Landed"   -> "Done"
      [] decision = "Replay"   -> "Replay"
      [] decision = "Fallback" -> "Fallback"
      [] decision = "InDoubt"  -> "Done"

DecisionResult(decision) ==
    IF decision = "Landed" THEN "Success" ELSE decision

FirstForKey(members, key) ==
    CHOOSE tx \in members :
        /\ KeyOf[tx] = key
        /\ ~\E other \in members :
              /\ KeyOf[other] = key
              /\ Before(other, tx)

LastForKey(members, key) ==
    CHOOSE tx \in members :
        /\ KeyOf[tx] = key
        /\ ~\E other \in members :
              /\ KeyOf[other] = key
              /\ Before(tx, other)

ApplyStagedWriters(state, staged) ==
    [key \in Keys |->
        IF \E tx \in staged : KeyOf[tx] = key
        THEN LastForKey(staged, key)
        ELSE state[key]]

ApplyStagedValues(state, staged) ==
    [key \in Keys |->
        IF \E tx \in staged : KeyOf[tx] = key
        THEN clients[LastForKey(staged, key)].attempt_value
        ELSE state[key]]

EventKey(actor) ==
    IF actor = EnvWriter THEN environment.key ELSE KeyOf[actor]

EventValue(actor) ==
    IF actor = EnvWriter
    THEN environment.value
    ELSE clients[actor].committed_value

ApplyValueEvent(state, actor) ==
    [state EXCEPT ![EventKey(actor)] = EventValue(actor)]

ApplyWriterEvent(state, actor) ==
    [state EXCEPT ![EventKey(actor)] = actor]

RECURSIVE ReplayValues(_)
RECURSIVE ReplayWriters(_)

ReplayValues(history) ==
    IF Len(history) = 0
    THEN InitialDb
    ELSE ApplyValueEvent(
            ReplayValues(SubSeq(history, 1, Len(history) - 1)),
            history[Len(history)])

ReplayWriters(history) ==
    IF Len(history) = 0
    THEN [key \in Keys |-> InitWriter]
    ELSE ApplyWriterEvent(
            ReplayWriters(SubSeq(history, 1, Len(history) - 1)),
            history[Len(history)])

ReadyTxns == {tx \in Txns : clients[tx].phase = "Direct"}

StartRound(members) ==
    /\ coordinator.phase = "Idle"
    /\ members \in SUBSET ReadyTxns
    /\ members # {}
    /\ coordinator' =
        [InitialCoordinator EXCEPT
            !.phase = "Folding",
            !.members = members,
            !.order = FoldOrder(members)]
    /\ clients' =
        [tx \in Txns |->
            IF tx \in members
            THEN [clients[tx] EXCEPT !.phase = "InRound", !.result = "None"]
            ELSE clients[tx]]
    /\ UNCHANGED <<leaf_writer, leaf_value, pending, linearized, environment>>

StartAnyRound == \E members \in SUBSET ReadyTxns : StartRound(members)

FoldRound ==
    LET members == coordinator.members
        staged == PlannedStaged(members)
    IN /\ coordinator.phase = "Folding"
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

DispatchRound ==
    /\ coordinator.phase = "Planned"
    /\ StagedMembers # {}
    /\ pending.state = "None"
    /\ coordinator' = [coordinator EXCEPT !.phase = "Awaiting"]
    /\ clients' =
        [tx \in Txns |->
            IF tx \in StagedMembers
            THEN [clients[tx] EXCEPT !.staged_ever = TRUE]
            ELSE clients[tx]]
    /\ UNCHANGED <<leaf_writer, leaf_value, pending, linearized, environment>>

FinishPlannedClients(staged) ==
    [tx \in Txns |->
        IF tx \in coordinator.members
        THEN [clients[tx] EXCEPT
                !.phase = DecisionPhase(coordinator.decision[tx]),
                !.result = DecisionResult(coordinator.decision[tx]),
                !.direct_count =
                    IF tx \in staged THEN @ + 1 ELSE @,
                !.committed_value =
                    IF tx \in staged THEN clients[tx].attempt_value ELSE @]
        ELSE clients[tx]]

DeliverUnchangedRound ==
    /\ coordinator.phase = "Planned"
    /\ StagedMembers = {}
    /\ clients' = FinishPlannedClients({})
    /\ coordinator' = InitialCoordinator
    /\ UNCHANGED <<leaf_writer, leaf_value, pending, linearized, environment>>

ExpectedStateCurrent ==
    /\ leaf_writer = coordinator.expected_writer
    /\ leaf_value = coordinator.expected_value

AcknowledgeRound ==
    LET staged == StagedMembers
    IN /\ coordinator.phase = "Awaiting"
       /\ ExpectedStateCurrent
       /\ leaf_writer' = ApplyStagedWriters(leaf_writer, staged)
       /\ leaf_value' = ApplyStagedValues(leaf_value, staged)
       /\ clients' = FinishPlannedClients(staged)
       /\ linearized' = linearized \o FoldOrder(staged)
       /\ coordinator' = InitialCoordinator
       /\ UNCHANGED <<pending, environment>>

PreconditionMissRound ==
    /\ coordinator.phase = "Awaiting"
    /\ ~ExpectedStateCurrent
    /\ coordinator' = [coordinator EXCEPT !.phase = "Folding"]
    /\ UNCHANGED
         <<leaf_writer, leaf_value, clients, pending, linearized, environment>>

ClientsAfterUncertainty(staged, applied) ==
    [tx \in Txns |->
        IF tx \in staged
        THEN [clients[tx] EXCEPT
                !.uncertain = TRUE,
                !.uncertain_pre_writer = coordinator.expected_writer,
                !.uncertain_pre_value = coordinator.expected_value,
                !.direct_count = IF applied THEN @ + 1 ELSE @,
                !.committed_value =
                    IF applied THEN clients[tx].attempt_value ELSE @]
        ELSE clients[tx]]

UnavailableUnresolved ==
    LET staged == StagedMembers
    IN /\ coordinator.phase = "Awaiting"
       /\ pending.state = "None"
       /\ clients' = ClientsAfterUncertainty(staged, FALSE)
       /\ coordinator' = [coordinator EXCEPT !.phase = "Folding"]
       /\ pending' =
            [ state           |-> "Unresolved",
              members         |-> staged,
              order           |-> FoldOrder(staged),
              expected_writer |-> coordinator.expected_writer,
              expected_value  |-> coordinator.expected_value,
              values          |->
                  [tx \in Txns |-> clients[tx].attempt_value] ]
       /\ UNCHANGED <<leaf_writer, leaf_value, linearized, environment>>

UnavailableAfterEffect ==
    LET staged == StagedMembers
    IN /\ coordinator.phase = "Awaiting"
       /\ pending.state = "None"
       /\ ExpectedStateCurrent
       /\ leaf_writer' = ApplyStagedWriters(leaf_writer, staged)
       /\ leaf_value' = ApplyStagedValues(leaf_value, staged)
       /\ clients' = ClientsAfterUncertainty(staged, TRUE)
       /\ coordinator' = [coordinator EXCEPT !.phase = "Folding"]
       /\ linearized' = linearized \o FoldOrder(staged)
       /\ UNCHANGED <<pending, environment>>

ApplyPendingValues(state) ==
    [key \in Keys |->
        IF \E tx \in pending.members : KeyOf[tx] = key
        THEN pending.values[LastForKey(pending.members, key)]
        ELSE state[key]]

ClientsAfterPendingEffect ==
    [tx \in Txns |->
        IF tx \in pending.members
        THEN [clients[tx] EXCEPT
                !.direct_count = @ + 1,
                !.committed_value = pending.values[tx]]
        ELSE clients[tx]]

\* The backend actor outlives the local unavailable response. It may apply the
\* exact captured CAS even after ExhaustRound or AbandonRound returned locally.
LateApplyPending ==
    /\ pending.state = "Unresolved"
    /\ leaf_writer = pending.expected_writer
    /\ leaf_value = pending.expected_value
    /\ leaf_writer' =
        ApplyStagedWriters(leaf_writer, pending.members)
    /\ leaf_value' = ApplyPendingValues(leaf_value)
    /\ clients' = ClientsAfterPendingEffect
    /\ pending' = InitialPending
    /\ linearized' = linearized \o pending.order
    /\ UNCHANGED <<coordinator, environment>>

ResolvePendingNoEffect ==
    /\ pending.state = "Unresolved"
    /\ pending' = InitialPending
    /\ UNCHANGED
         <<leaf_writer, leaf_value, clients, coordinator, linearized, environment>>

ExhaustionDecision(tx) ==
    IF ExactMarker(tx)
    THEN "Landed"
    ELSE IF clients[tx].uncertain THEN "InDoubt" ELSE "Fallback"

ExhaustRound ==
    /\ coordinator.phase \in {"Folding", "Planned"}
    /\ clients' =
        [tx \in Txns |->
            IF tx \in coordinator.members
            THEN [clients[tx] EXCEPT
                    !.phase = DecisionPhase(ExhaustionDecision(tx)),
                    !.result = DecisionResult(ExhaustionDecision(tx))]
            ELSE clients[tx]]
    /\ coordinator' = InitialCoordinator
    /\ UNCHANGED <<leaf_writer, leaf_value, pending, linearized, environment>>

AbandonRound ==
    /\ coordinator.phase \in {"Folding", "Planned"}
    /\ \E tx \in coordinator.members : clients[tx].uncertain
    /\ clients' =
        [tx \in Txns |->
            IF tx \in coordinator.members
            THEN IF clients[tx].uncertain
                 THEN [clients[tx] EXCEPT
                         !.phase = "Done",
                         !.result = "Abandoned"]
                 ELSE [clients[tx] EXCEPT
                         !.phase = DecisionPhase(ExhaustionDecision(tx)),
                         !.result = DecisionResult(ExhaustionDecision(tx))]
            ELSE clients[tx]]
    /\ coordinator' = InitialCoordinator
    /\ UNCHANGED <<leaf_writer, leaf_value, pending, linearized, environment>>

ExternalOverwrite(key, new_value) ==
    /\ ~environment.used
    /\ key \in Keys
    /\ new_value \in Values
    /\ leaf_writer' = [leaf_writer EXCEPT ![key] = EnvWriter]
    /\ leaf_value' = [leaf_value EXCEPT ![key] = new_value]
    /\ environment' =
        [environment EXCEPT !.used = TRUE, !.key = key, !.value = new_value]
    /\ linearized' = Append(linearized, EnvWriter)
    /\ UNCHANGED <<clients, coordinator, pending>>

ReplayBody(tx) ==
    LET key == KeyOf[tx]
    IN /\ clients[tx].phase = "Replay"
       /\ clients' =
            [clients EXCEPT
                ![tx].phase = "Direct",
                ![tx].result = "None",
                ![tx].observed_writer = leaf_writer[key],
                ![tx].observed_value = leaf_value[key],
                ![tx].attempt_value = RmwNext[leaf_value[key]]]
       /\ UNCHANGED <<leaf_writer, leaf_value, coordinator, pending,
                      linearized, environment>>

EnterFallback(tx) ==
    /\ clients[tx].phase = "Fallback"
    /\ ~clients[tx].uncertain
    /\ clients[tx].direct_count + clients[tx].fallback_count = 0
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Locked",
            ![tx].tx_object = TRUE,
            ![tx].lock_held = TRUE]
    /\ UNCHANGED <<leaf_writer, leaf_value, coordinator, pending,
                   linearized, environment>>

FallbackValue(tx) ==
    IF KindOf[tx] = Blind
    THEN BlindValue[tx]
    ELSE RmwNext[leaf_value[KeyOf[tx]]]

CommitFallback(tx) ==
    LET key == KeyOf[tx]
        value == FallbackValue(tx)
    IN /\ clients[tx].phase = "Locked"
       /\ clients[tx].lock_held
       /\ clients[tx].tx_object
       /\ ~clients[tx].uncertain
       /\ clients[tx].direct_count + clients[tx].fallback_count = 0
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

Next ==
    \/ StartAnyRound
    \/ FoldRound
    \/ DispatchRound
    \/ DeliverUnchangedRound
    \/ AcknowledgeRound
    \/ PreconditionMissRound
    \/ UnavailableUnresolved
    \/ UnavailableAfterEffect
    \/ LateApplyPending
    \/ ResolvePendingNoEffect
    \/ ExhaustRound
    \/ AbandonRound
    \/ \E key \in Keys, value \in Values : ExternalOverwrite(key, value)
    \/ \E tx \in Txns : ReplayBody(tx)
    \/ \E tx \in Txns : EnterFallback(tx)
    \/ \E tx \in Txns : CommitFallback(tx)

Spec == Init /\ [][Next]_vars

HealthyNext ==
    \/ StartAnyRound
    \/ FoldRound
    \/ DispatchRound
    \/ DeliverUnchangedRound
    \/ AcknowledgeRound
    \/ PreconditionMissRound
    \/ LateApplyPending
    \/ ResolvePendingNoEffect
    \/ \E tx \in Txns : ReplayBody(tx)
    \/ \E tx \in Txns : EnterFallback(tx)
    \/ \E tx \in Txns : CommitFallback(tx)

ReplayAny == \E tx \in Txns : ReplayBody(tx)
EnterAnyFallback == \E tx \in Txns : EnterFallback(tx)
CommitAnyFallback == \E tx \in Txns : CommitFallback(tx)
ResolvePending == LateApplyPending \/ ResolvePendingNoEffect

BecomeHealthy ==
    /\ ~environment.healthy
    /\ environment' = [environment EXCEPT !.healthy = TRUE]
    /\ UNCHANGED
         <<leaf_writer, leaf_value, clients, coordinator, pending, linearized>>

LiveStartAnyRound == environment.healthy /\ StartAnyRound
LiveFoldRound == environment.healthy /\ FoldRound
LiveDispatchRound == environment.healthy /\ DispatchRound
LiveDeliverUnchangedRound == environment.healthy /\ DeliverUnchangedRound
LiveAcknowledgeRound == environment.healthy /\ AcknowledgeRound
LivePreconditionMissRound == environment.healthy /\ PreconditionMissRound
LiveResolvePending == environment.healthy /\ ResolvePending
LiveReplayAny == environment.healthy /\ ReplayAny
LiveEnterAnyFallback == environment.healthy /\ EnterAnyFallback
LiveCommitAnyFallback == environment.healthy /\ CommitAnyFallback

EventuallyHealthyNext ==
    \/ /\ ~environment.healthy
       /\ Next
    \/ BecomeHealthy
    \/ /\ environment.healthy
       /\ HealthyNext

EventuallyHealthySpec ==
    /\ Init
    /\ [][EventuallyHealthyNext]_vars
    /\ WF_vars(BecomeHealthy)
    /\ WF_vars(LiveStartAnyRound)
    /\ WF_vars(LiveFoldRound)
    /\ WF_vars(LiveDispatchRound)
    /\ WF_vars(LiveDeliverUnchangedRound)
    /\ WF_vars(LiveAcknowledgeRound)
    /\ WF_vars(LivePreconditionMissRound)
    /\ WF_vars(LiveResolvePending)
    /\ WF_vars(LiveReplayAny)
    /\ WF_vars(LiveEnterAnyFallback)
    /\ WF_vars(LiveCommitAnyFallback)

AllEventuallyComplete == <> (\A tx \in Txns : clients[tx].phase = "Done")

D0_TypeOK ==
    /\ leaf_writer \in [Keys -> WriterIds]
    /\ leaf_value \in [Keys -> Values]
    /\ clients \in [Txns -> ClientType]
    /\ coordinator \in CoordinatorType
    /\ pending \in PendingType
    /\ linearized \in Seq(WriterIds)
    /\ environment \in EnvironmentType
    /\ (environment.used <=> environment.key \in Keys)
    /\ (pending.state = "None" <=> pending.members = {})
    /\ SetOfSequence(pending.order) = pending.members
    /\ NoDuplicates(pending.order)
    /\ IsOldestFirst(pending.order)

S6_DirectAtomicityAndExclusion ==
    /\ \A tx \in Txns :
          /\ clients[tx].direct_count <= 1
          /\ clients[tx].fallback_count <= 1
          /\ clients[tx].direct_count + clients[tx].fallback_count <= 1
    /\ \A left, right \in StagedMembers :
          left # right => KeyOf[left] # KeyOf[right]
    /\ \A left, right \in pending.members :
          left # right => KeyOf[left] # KeyOf[right]
    /\ SetOfSequence(coordinator.order) = coordinator.members
    /\ NoDuplicates(coordinator.order)
    /\ IsOldestFirst(coordinator.order)
    /\ NoDuplicates(linearized)
    /\ SetOfSequence(linearized) =
       {tx \in Txns :
          clients[tx].direct_count + clients[tx].fallback_count = 1}
       \cup (IF environment.used THEN {EnvWriter} ELSE {})
    /\ ReplayValues(linearized) = leaf_value
    /\ ReplayWriters(linearized) = leaf_writer

S7_ReplayIsEffectFree ==
    \A tx \in Txns :
        clients[tx].phase = "Replay" =>
            /\ clients[tx].result = "Replay"
            /\ clients[tx].direct_count = 0
            /\ clients[tx].fallback_count = 0
            /\ ~clients[tx].uncertain
            /\ tx \notin pending.members
            /\ ~clients[tx].tx_object
            /\ ~clients[tx].lock_held
            /\ ~clients[tx].commit_marker

S8_PerMemberUncertainty ==
    \A tx \in Txns :
        /\ clients[tx].uncertain => clients[tx].staged_ever
        /\ tx \in pending.members => clients[tx].uncertain
        /\ tx \in pending.members =>
              pending.values[tx] = clients[tx].attempt_value
        /\ clients[tx].result = "Replay" => ~clients[tx].uncertain
        /\ clients[tx].result = "Fallback" => ~clients[tx].uncertain
        /\ clients[tx].result \in {"Replay", "Fallback"} =>
              tx \notin pending.members
        /\ clients[tx].result = "InDoubt" => clients[tx].uncertain
        /\ clients[tx].result = "Abandoned" => clients[tx].uncertain
        /\ clients[tx].phase = "Locked" => ~clients[tx].uncertain

S6_DirectWritersAreLogless ==
    \A tx \in Txns :
        /\ clients[tx].direct_count = 1 =>
              /\ ~clients[tx].tx_object
              /\ ~clients[tx].lock_held
              /\ ~clients[tx].commit_marker
        /\ clients[tx].fallback_count = 1 =>
              /\ clients[tx].tx_object
              /\ clients[tx].commit_marker

=============================================================================
