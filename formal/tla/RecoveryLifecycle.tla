------------------------ MODULE RecoveryLifecycle ------------------------
EXTENDS Backend, Common

CONSTANTS
    Ops,
    Attempts,
    Keys,
    RevisionTokens,
    NoAttempt,
    NoProgress,
    MaxTime,
    PublicOf,
    FirstAttempt,
    RenewedAttempt,
    Priority,
    RequiredKey,
    InitialRevision,
    NextRevision

ASSUME
    /\ Ops # {}
    /\ Attempts # {}
    /\ Keys # {}
    /\ RevisionTokens # {}
    /\ NoAttempt \notin Attempts
    /\ NoProgress \notin 0..MaxTime
    /\ MaxTime >= 2
    /\ PublicOf \in [Attempts -> Ops]
    /\ FirstAttempt \in [Ops -> Attempts]
    /\ RenewedAttempt \in [Attempts -> Attempts \cup {NoAttempt}]
    /\ Priority \in [Attempts -> Nat]
    /\ RequiredKey \in [Ops -> Keys]
    /\ InitialRevision \in RevisionTokens
    /\ NextRevision \in [RevisionTokens -> RevisionTokens]
    /\ \A op \in Ops : PublicOf[FirstAttempt[op]] = op
    /\ \A attempt \in Attempts :
          RenewedAttempt[attempt] # NoAttempt =>
              /\ PublicOf[RenewedAttempt[attempt]] = PublicOf[attempt]
              /\ RenewedAttempt[RenewedAttempt[attempt]] = NoAttempt

Phases == {"Unused", "Acquiring", "Waiting", "Ready", "Crashed", "Done"}
Statuses == {"Absent", "Pending", "Committed", "Aborted"}
RequestStates == BackendMutationStates
AbortCauses == {"None", "Wound", "Expiry"}

VARIABLES
    current_attempt,
    phase,
    status,
    lease,
    progress,
    effective_priority,
    lock_holder,
    revision,
    request_state,
    request_key,
    request_revision,
    request_expected_holder,
    request_apply_count,
    receipts,
    observing,
    seen_status,
    seen_progress,
    seen_at,
    seen_age,
    abort_cause,
    expired_by,
    committed,
    aborted,
    effect_count,
    now

vars ==
    <<current_attempt, phase, status, lease, progress, effective_priority,
      lock_holder, revision, request_state, request_key, request_revision,
      request_expected_holder, request_apply_count, receipts, observing,
      seen_status, seen_progress, seen_at, seen_age, abort_cause, expired_by,
      committed, aborted, effect_count, now>>

Active(attempt) ==
    current_attempt[PublicOf[attempt]] = attempt

Required(attempt) == RequiredKey[PublicOf[attempt]]

HasPhysicalLock(attempt) ==
    \E key \in Keys : lock_holder[key] = attempt

ObservationChanged(observer, holder) ==
    \/ seen_status[observer][holder] # status[holder]
    \/ status[holder] = "Pending" /\
       seen_progress[observer][holder] # progress[holder]

ExpiryEvidence(observer, holder) ==
    /\ observing[observer][holder]
    /\ \/ /\ status[holder] = "Absent"
           /\ seen_status[observer][holder] = "Absent"
           /\ seen_age[observer][holder] = MaxTime
       \/ /\ status[holder] = "Pending"
           /\ seen_status[observer][holder] = "Pending"
           /\ seen_progress[observer][holder] = progress[holder]
           /\ \/ now > lease[holder]
               \/ seen_age[observer][holder] = MaxTime

Init ==
    /\ current_attempt = FirstAttempt
    /\ phase =
       [attempt \in Attempts |->
           IF attempt \in {FirstAttempt[op] : op \in Ops}
           THEN "Acquiring"
           ELSE "Unused"]
    /\ status = [attempt \in Attempts |-> "Absent"]
    /\ lease = [attempt \in Attempts |-> 0]
    /\ progress = [attempt \in Attempts |-> 0]
    /\ effective_priority = Priority
    /\ lock_holder = [key \in Keys |-> NoAttempt]
    /\ revision = [key \in Keys |-> InitialRevision]
    /\ request_state = [attempt \in Attempts |-> "None"]
    /\ request_key = [attempt \in Attempts |-> Required(attempt)]
    /\ request_revision = [attempt \in Attempts |-> InitialRevision]
    /\ request_expected_holder =
       [attempt \in Attempts |-> NoAttempt]
    /\ request_apply_count = [attempt \in Attempts |-> 0]
    /\ receipts = [attempt \in Attempts |-> {}]
    /\ observing =
       [observer \in Attempts |-> [holder \in Attempts |-> FALSE]]
    /\ seen_status =
       [observer \in Attempts |->
           [holder \in Attempts |-> "Absent"]]
    /\ seen_progress =
       [observer \in Attempts |->
           [holder \in Attempts |-> NoProgress]]
    /\ seen_at =
       [observer \in Attempts |-> [holder \in Attempts |-> 0]]
    /\ seen_age =
       [observer \in Attempts |-> [holder \in Attempts |-> 0]]
    /\ abort_cause = [attempt \in Attempts |-> "None"]
    /\ expired_by = [attempt \in Attempts |-> NoAttempt]
    /\ committed = {}
    /\ aborted = {}
    /\ effect_count = [op \in Ops |-> 0]
    /\ now = 0

MaterializePending(attempt) ==
    /\ Active(attempt)
    /\ phase[attempt] \in {"Acquiring", "Waiting", "Ready"}
    /\ HasPhysicalLock(attempt)
    /\ status[attempt] = "Absent"
    /\ status' = [status EXCEPT ![attempt] = "Pending"]
    /\ lease' = [lease EXCEPT ![attempt] = now]
    /\ progress' = [progress EXCEPT ![attempt] = progress[attempt] + 1]
    /\ UNCHANGED <<current_attempt, phase, effective_priority, lock_holder,
                   revision, request_state, request_key, request_revision,
                   request_expected_holder, request_apply_count, receipts,
                   observing, seen_status, seen_progress, seen_at, seen_age, abort_cause,
                   expired_by, committed, aborted, effect_count, now>>

RefreshPending(attempt) ==
    /\ Active(attempt)
    /\ phase[attempt] \in {"Acquiring", "Waiting", "Ready"}
    /\ status[attempt] = "Pending"
    /\ progress[attempt] < MaxTime
    /\ lease' = [lease EXCEPT ![attempt] = now]
    /\ progress' = [progress EXCEPT ![attempt] = progress[attempt] + 1]
    /\ UNCHANGED <<current_attempt, phase, status, effective_priority,
                   lock_holder, revision, request_state, request_key,
                   request_revision, request_expected_holder,
                   request_apply_count, receipts, observing, seen_status,
                   seen_progress, seen_at, seen_age, abort_cause, expired_by, committed,
                   aborted, effect_count, now>>

DispatchAcquire(attempt) ==
    LET key == Required(attempt)
    IN /\ Active(attempt)
       /\ phase[attempt] \in {"Acquiring", "Waiting"}
       /\ status[attempt] \in {"Absent", "Pending"}
       /\ key \notin receipts[attempt]
       /\ request_state[attempt] = "None"
       /\ lock_holder[key] \in {NoAttempt, attempt}
       /\ request_state' =
          [request_state EXCEPT ![attempt] = "Dispatched"]
       /\ request_key' = [request_key EXCEPT ![attempt] = key]
       /\ request_revision' =
          [request_revision EXCEPT ![attempt] = revision[key]]
       /\ request_expected_holder' =
          [request_expected_holder EXCEPT
              ![attempt] = lock_holder[key]]
       /\ UNCHANGED <<current_attempt, phase, status, lease, progress,
                      effective_priority, lock_holder, revision,
                      request_apply_count, receipts, observing, seen_status,
                      seen_progress, seen_at, seen_age, abort_cause, expired_by,
                      committed, aborted, effect_count, now>>

ApplyAcquire(attempt) ==
    LET key == request_key[attempt]
    IN /\ CanStillApply(request_state[attempt])
       /\ revision[key] = request_revision[attempt]
       /\ lock_holder[key] = request_expected_holder[attempt]
       /\ request_state' =
          [request_state EXCEPT ![attempt] = "Applied"]
       /\ request_apply_count' =
          [request_apply_count EXCEPT ![attempt] = @ + 1]
       /\ lock_holder' = [lock_holder EXCEPT ![key] = attempt]
       /\ revision' = [revision EXCEPT ![key] = NextRevision[@]]
       /\ UNCHANGED <<current_attempt, phase, status, lease, progress,
                      effective_priority, request_key, request_revision,
                      request_expected_holder, receipts, observing,
                      seen_status, seen_progress, seen_at, seen_age, abort_cause,
                      expired_by, committed, aborted, effect_count, now>>

MarkAcquireUnavailable(attempt) ==
    /\ request_state[attempt] = "Dispatched"
    /\ request_state' =
       [request_state EXCEPT ![attempt] = "Unresolved"]
    /\ phase' =
       IF Active(attempt) /\ phase[attempt] # "Crashed"
       THEN [phase EXCEPT ![attempt] = "Waiting"]
       ELSE phase
    /\ UNCHANGED <<current_attempt, status, lease, progress,
                   effective_priority, lock_holder, revision, request_key,
                   request_revision, request_expected_holder,
                   request_apply_count, receipts, observing, seen_status,
                   seen_progress, seen_at, seen_age, abort_cause, expired_by, committed,
                   aborted, effect_count, now>>

AcquirePreconditionFailed(attempt) ==
    LET key == request_key[attempt]
    IN /\ CanStillApply(request_state[attempt])
       /\ \/ revision[key] # request_revision[attempt]
           \/ lock_holder[key] # request_expected_holder[attempt]
       /\ request_state' = [request_state EXCEPT ![attempt] = "None"]
       /\ phase' =
          IF Active(attempt) /\ phase[attempt] # "Crashed"
          THEN [phase EXCEPT ![attempt] = "Acquiring"]
          ELSE phase
       /\ UNCHANGED <<current_attempt, status, lease, progress,
                      effective_priority, lock_holder, revision, request_key,
                      request_revision, request_expected_holder,
                      request_apply_count, receipts, observing, seen_status,
                      seen_progress, seen_at, seen_age, abort_cause, expired_by,
                      committed, aborted, effect_count, now>>

AcknowledgeAcquire(attempt) ==
    LET key == request_key[attempt]
        usable ==
            /\ Active(attempt)
            /\ phase[attempt] \in {"Acquiring", "Waiting"}
            /\ status[attempt] \in {"Absent", "Pending"}
            /\ lock_holder[key] = attempt
    IN /\ request_state[attempt] = "Applied"
       /\ request_state' = [request_state EXCEPT ![attempt] = "None"]
       /\ receipts' =
          IF usable
          THEN [receipts EXCEPT ![attempt] = @ \cup {key}]
          ELSE receipts
       /\ phase' =
          IF usable
          THEN [phase EXCEPT ![attempt] = "Ready"]
          ELSE phase
       /\ UNCHANGED <<current_attempt, status, lease, progress,
                      effective_priority, lock_holder, revision, request_key,
                      request_revision, request_expected_holder,
                      request_apply_count, observing, seen_status,
                      seen_progress, seen_at, seen_age, abort_cause, expired_by,
                      committed, aborted, effect_count, now>>

RecoverInstalledAcquire(attempt) ==
    LET key == Required(attempt)
    IN /\ Active(attempt)
       /\ phase[attempt] \in {"Acquiring", "Waiting"}
       /\ status[attempt] \in {"Absent", "Pending"}
       /\ lock_holder[key] = attempt
       /\ key \notin receipts[attempt]
       /\ receipts' = [receipts EXCEPT ![attempt] = @ \cup {key}]
       /\ phase' = [phase EXCEPT ![attempt] = "Ready"]
       /\ request_state' = [request_state EXCEPT ![attempt] = "None"]
       /\ UNCHANGED <<current_attempt, status, lease, progress,
                      effective_priority, lock_holder, revision, request_key,
                      request_revision, request_expected_holder,
                      request_apply_count, observing, seen_status,
                      seen_progress, seen_at, seen_age, abort_cause, expired_by,
                      committed, aborted, effect_count, now>>

ObserveConflict(observer, holder) ==
    LET key == Required(observer)
    IN /\ Active(observer)
       /\ phase[observer] \in {"Acquiring", "Waiting"}
       /\ lock_holder[key] = holder
       /\ holder # observer
       /\ status[holder] \in {"Absent", "Pending"}
       /\ effective_priority[observer] >= effective_priority[holder]
       /\ ~observing[observer][holder]
       /\ phase' = [phase EXCEPT ![observer] = "Waiting"]
       /\ observing' =
          [observing EXCEPT ![observer][holder] = TRUE]
       /\ seen_status' =
          [seen_status EXCEPT ![observer][holder] = status[holder]]
       /\ seen_progress' =
          [seen_progress EXCEPT
              ![observer][holder] =
                  IF status[holder] = "Pending"
                  THEN progress[holder]
                  ELSE NoProgress]
       /\ seen_at' = [seen_at EXCEPT ![observer][holder] = now]
       /\ seen_age' = [seen_age EXCEPT ![observer][holder] = 0]
       /\ UNCHANGED <<current_attempt, status, lease, progress,
                      effective_priority, lock_holder, revision, request_state,
                      request_key, request_revision, request_expected_holder,
                      request_apply_count, receipts, abort_cause, expired_by,
                      committed, aborted, effect_count, now>>

ObserveProgress(observer, holder) ==
    /\ observing[observer][holder]
    /\ status[holder] \in {"Absent", "Pending"}
    /\ ObservationChanged(observer, holder)
    /\ seen_status' =
       [seen_status EXCEPT ![observer][holder] = status[holder]]
    /\ seen_progress' =
       [seen_progress EXCEPT
           ![observer][holder] =
               IF status[holder] = "Pending"
               THEN progress[holder]
               ELSE NoProgress]
    /\ seen_at' = [seen_at EXCEPT ![observer][holder] = now]
    /\ seen_age' = [seen_age EXCEPT ![observer][holder] = 0]
    /\ UNCHANGED <<current_attempt, phase, status, lease, progress,
                   effective_priority, lock_holder, revision, request_state,
                   request_key, request_revision, request_expected_holder,
                   request_apply_count, receipts, observing, abort_cause,
                   expired_by, committed, aborted, effect_count, now>>

AdvanceObserverAge(observer, holder) ==
    /\ observing[observer][holder]
    /\ status[holder] \in {"Absent", "Pending"}
    /\ ~ObservationChanged(observer, holder)
    /\ seen_age[observer][holder] < MaxTime
    /\ seen_age' =
       [seen_age EXCEPT ![observer][holder] = @ + 1]
    /\ UNCHANGED <<current_attempt, phase, status, lease, progress,
                   effective_priority, lock_holder, revision, request_state,
                   request_key, request_revision, request_expected_holder,
                   request_apply_count, receipts, observing, seen_status,
                   seen_progress, seen_at, abort_cause, expired_by, committed,
                   aborted, effect_count, now>>

ExpireObserved(observer, holder) ==
    /\ ExpiryEvidence(observer, holder)
    /\ status[holder] \in {"Absent", "Pending"}
    /\ status' = [status EXCEPT ![holder] = "Aborted"]
    /\ aborted' = aborted \cup {holder}
    /\ abort_cause' =
       [abort_cause EXCEPT ![holder] = "Expiry"]
    /\ expired_by' = [expired_by EXCEPT ![holder] = observer]
    /\ phase' =
       IF Active(holder)
       THEN [phase EXCEPT ![holder] = "Done"]
       ELSE phase
    /\ UNCHANGED <<current_attempt, lease, progress, effective_priority,
                   lock_holder, revision, request_state, request_key,
                   request_revision, request_expected_holder,
                   request_apply_count, receipts, observing, seen_status,
                   seen_progress, seen_at, seen_age, committed, effect_count, now>>

Wound(observer, holder) ==
    LET key == Required(observer)
    IN /\ Active(observer)
       /\ lock_holder[key] = holder
       /\ holder # observer
       /\ effective_priority[observer] < effective_priority[holder]
       /\ status[holder] \in {"Absent", "Pending"}
       /\ status' = [status EXCEPT ![holder] = "Aborted"]
       /\ aborted' = aborted \cup {holder}
       /\ abort_cause' = [abort_cause EXCEPT ![holder] = "Wound"]
       /\ phase' =
          IF Active(holder)
          THEN [phase EXCEPT ![holder] = "Done"]
          ELSE phase
       /\ UNCHANGED <<current_attempt, lease, progress, effective_priority,
                      lock_holder, revision, request_state, request_key,
                      request_revision, request_expected_holder,
                      request_apply_count, receipts, observing, seen_status,
                      seen_progress, seen_at, seen_age, expired_by, committed,
                      effect_count, now>>

ObserveFinal(observer, holder) ==
    /\ observing[observer][holder]
    /\ status[holder] \in {"Committed", "Aborted"}
    /\ observing' =
       [observing EXCEPT ![observer][holder] = FALSE]
    /\ phase' =
       IF Active(observer) /\ phase[observer] = "Waiting"
       THEN [phase EXCEPT ![observer] = "Acquiring"]
       ELSE phase
    /\ UNCHANGED <<current_attempt, status, lease, progress,
                   effective_priority, lock_holder, revision, request_state,
                   request_key, request_revision, request_expected_holder,
                   request_apply_count, receipts, seen_status, seen_progress,
                   seen_at, seen_age, abort_cause, expired_by, committed, aborted,
                   effect_count, now>>

RenewPublic(op) ==
    LET old == current_attempt[op]
        fresh == RenewedAttempt[old]
    IN /\ status[old] = "Aborted"
       /\ fresh # NoAttempt
       /\ phase[fresh] = "Unused"
       /\ current_attempt' = [current_attempt EXCEPT ![op] = fresh]
       /\ phase' = [phase EXCEPT ![fresh] = "Acquiring"]
       /\ effective_priority' =
          [effective_priority EXCEPT
              ![fresh] = effective_priority[old]]
       /\ UNCHANGED <<status, lease, progress, lock_holder, revision,
                      request_state, request_key, request_revision,
                      request_expected_holder, request_apply_count, receipts,
                      observing, seen_status, seen_progress, seen_at, seen_age,
                      abort_cause, expired_by, committed, aborted,
                      effect_count, now>>

Commit(attempt) ==
    LET op == PublicOf[attempt]
        key == Required(attempt)
    IN /\ Active(attempt)
       /\ phase[attempt] = "Ready"
       /\ status[attempt] \in {"Absent", "Pending"}
       /\ lock_holder[key] = attempt
       /\ key \in receipts[attempt]
       /\ status' = [status EXCEPT ![attempt] = "Committed"]
       /\ phase' = [phase EXCEPT ![attempt] = "Done"]
       /\ committed' = committed \cup {attempt}
       /\ effect_count' = [effect_count EXCEPT ![op] = @ + 1]
       /\ UNCHANGED <<current_attempt, lease, progress, effective_priority,
                      lock_holder, revision, request_state, request_key,
                      request_revision, request_expected_holder,
                      request_apply_count, receipts, observing, seen_status,
                      seen_progress, seen_at, seen_age, abort_cause, expired_by, aborted,
                      now>>

Crash(attempt) ==
    /\ Active(attempt)
    /\ phase[attempt] \in {"Acquiring", "Waiting", "Ready"}
    /\ phase' = [phase EXCEPT ![attempt] = "Crashed"]
    /\ request_state' =
       IF request_state[attempt] = "Dispatched"
       THEN [request_state EXCEPT ![attempt] = "Unresolved"]
       ELSE request_state
    /\ UNCHANGED <<current_attempt, status, lease, progress,
                   effective_priority, lock_holder, revision, request_key,
                   request_revision, request_expected_holder,
                   request_apply_count, receipts, observing, seen_status,
                   seen_progress, seen_at, seen_age, abort_cause, expired_by, committed,
                   aborted, effect_count, now>>

ReleaseTerminalLock(attempt, key) ==
    /\ lock_holder[key] = attempt
    /\ status[attempt] \in {"Committed", "Aborted"}
    /\ lock_holder' = [lock_holder EXCEPT ![key] = NoAttempt]
    /\ revision' = [revision EXCEPT ![key] = NextRevision[@]]
    /\ receipts' = [receipts EXCEPT ![attempt] = @ \ {key}]
    /\ UNCHANGED <<current_attempt, phase, status, lease, progress,
                   effective_priority, request_state, request_key,
                   request_revision, request_expected_holder,
                   request_apply_count, observing, seen_status, seen_progress,
                   seen_at, seen_age, abort_cause, expired_by, committed, aborted,
                   effect_count, now>>

RewriteEquivalentLeaf(key) ==
    /\ revision' = [revision EXCEPT ![key] = NextRevision[@]]
    /\ UNCHANGED <<current_attempt, phase, status, lease, progress,
                   effective_priority, lock_holder, request_state, request_key,
                   request_revision, request_expected_holder,
                   request_apply_count, receipts, observing, seen_status,
                   seen_progress, seen_at, seen_age, abort_cause, expired_by, committed,
                   aborted, effect_count, now>>

AdvanceTime ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<current_attempt, phase, status, lease, progress,
                   effective_priority, lock_holder, revision, request_state,
                   request_key, request_revision, request_expected_holder,
                   request_apply_count, receipts, observing, seen_status,
                   seen_progress, seen_at, seen_age, abort_cause, expired_by, committed,
                   aborted, effect_count>>

Next ==
    \/ \E attempt \in Attempts : MaterializePending(attempt)
    \/ \E attempt \in Attempts : RefreshPending(attempt)
    \/ \E attempt \in Attempts : DispatchAcquire(attempt)
    \/ \E attempt \in Attempts : ApplyAcquire(attempt)
    \/ \E attempt \in Attempts : MarkAcquireUnavailable(attempt)
    \/ \E attempt \in Attempts : AcquirePreconditionFailed(attempt)
    \/ \E attempt \in Attempts : AcknowledgeAcquire(attempt)
    \/ \E attempt \in Attempts : RecoverInstalledAcquire(attempt)
    \/ \E observer \in Attempts, holder \in Attempts :
           ObserveConflict(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ObserveProgress(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           AdvanceObserverAge(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ExpireObserved(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           Wound(observer, holder)
    \/ \E observer \in Attempts, holder \in Attempts :
           ObserveFinal(observer, holder)
    \/ \E op \in Ops : RenewPublic(op)
    \/ \E attempt \in Attempts : Commit(attempt)
    \/ \E attempt \in Attempts : Crash(attempt)
    \/ \E attempt \in Attempts, key \in Keys :
           ReleaseTerminalLock(attempt, key)
    \/ \E key \in Keys : RewriteEquivalentLeaf(key)
    \/ AdvanceTime

Spec == Init /\ [][Next]_vars

R0_TypeOK ==
    /\ current_attempt \in [Ops -> Attempts]
    /\ phase \in [Attempts -> Phases]
    /\ status \in [Attempts -> Statuses]
    /\ lease \in [Attempts -> 0..MaxTime]
    /\ progress \in [Attempts -> 0..MaxTime]
    /\ effective_priority \in [Attempts -> Nat]
    /\ lock_holder \in [Keys -> Attempts \cup {NoAttempt}]
    /\ revision \in [Keys -> RevisionTokens]
    /\ request_state \in [Attempts -> RequestStates]
    /\ request_key \in [Attempts -> Keys]
    /\ request_revision \in [Attempts -> RevisionTokens]
    /\ request_expected_holder \in
       [Attempts -> Attempts \cup {NoAttempt}]
    /\ request_apply_count \in [Attempts -> 0..2]
    /\ receipts \in [Attempts -> SUBSET Keys]
    /\ observing \in [Attempts -> [Attempts -> BOOLEAN]]
    /\ seen_status \in [Attempts -> [Attempts -> Statuses]]
    /\ seen_progress \in
       [Attempts -> [Attempts -> 0..MaxTime \cup {NoProgress}]]
    /\ seen_at \in [Attempts -> [Attempts -> 0..MaxTime]]
    /\ seen_age \in [Attempts -> [Attempts -> 0..MaxTime]]
    /\ abort_cause \in [Attempts -> AbortCauses]
    /\ expired_by \in [Attempts -> Attempts \cup {NoAttempt}]
    /\ committed \subseteq Attempts
    /\ aborted \subseteq Attempts
    /\ effect_count \in [Ops -> 0..2]
    /\ now \in 0..MaxTime

R1_TerminalImmutable ==
    /\ committed \cap aborted = {}
    /\ \A attempt \in Attempts :
          /\ (attempt \in committed) = (status[attempt] = "Committed")
          /\ (attempt \in aborted) = (status[attempt] = "Aborted")

R2_DelayedRequestAtMostOnce ==
    \A attempt \in Attempts : request_apply_count[attempt] <= 1

R3_ReceiptsArePhysical ==
    \A attempt \in Attempts :
        \A key \in receipts[attempt] : lock_holder[key] = attempt

R4_RenewalPreservesPublicPriority ==
    \A attempt \in Attempts :
        effective_priority[attempt] = Priority[FirstAttempt[PublicOf[attempt]]]

R5_ExpiryHasObserverEvidence ==
    \A holder \in Attempts :
        abort_cause[holder] = "Expiry" =>
            LET observer == expired_by[holder]
            IN /\ observer \in Attempts
               /\ \/ /\ seen_status[observer][holder] = "Absent"
                       /\ seen_age[observer][holder] = MaxTime
                   \/ /\ seen_status[observer][holder] = "Pending"
                       /\ seen_progress[observer][holder] = progress[holder]
                       /\ \/ now > lease[holder]
                           \/ seen_age[observer][holder] = MaxTime

R6_PublicEffectAtMostOnce ==
    /\ \A op \in Ops : effect_count[op] <= 1
    /\ \A attempt \in committed :
          /\ Active(attempt)
          /\ effect_count[PublicOf[attempt]] = 1

R7_SupersededAttemptsCannotCommit ==
    \A attempt \in committed :
        current_attempt[PublicOf[attempt]] = attempt

Safety ==
    /\ R0_TypeOK
    /\ R1_TerminalImmutable
    /\ R2_DelayedRequestAtMostOnce
    /\ R3_ReceiptsArePhysical
    /\ R4_RenewalPreservesPublicPriority
    /\ R5_ExpiryHasObserverEvidence
    /\ R6_PublicEffectAtMostOnce
    /\ R7_SupersededAttemptsCannotCommit

=============================================================================
