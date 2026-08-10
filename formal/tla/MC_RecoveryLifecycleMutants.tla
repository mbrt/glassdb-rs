------------------- MODULE MC_RecoveryLifecycleMutants -------------------
EXTENDS MC_RecoveryLifecycle

ApplyDelayedRequestTwice(attempt) ==
    /\ request_state[attempt] = "Applied"
    /\ request_apply_count' =
       [request_apply_count EXCEPT ![attempt] = @ + 1]
    /\ UNCHANGED <<current_attempt, phase, status, lease, progress,
                   effective_priority, lock_holder, revision, request_state,
                   request_key, request_revision, request_expected_holder,
                   receipts, observing, seen_status, seen_progress, seen_at, seen_age,
                   abort_cause, expired_by, committed, aborted, effect_count,
                   now>>

RenewWithNewPriority(op) ==
    LET old == current_attempt[op]
        fresh == RenewedAttempt[old]
    IN /\ status[old] = "Aborted"
       /\ fresh # NoAttempt
       /\ phase[fresh] = "Unused"
       /\ current_attempt' = [current_attempt EXCEPT ![op] = fresh]
       /\ phase' = [phase EXCEPT ![fresh] = "Acquiring"]
       /\ effective_priority' =
          [effective_priority EXCEPT ![fresh] = @ + 1]
       /\ UNCHANGED <<status, lease, progress, lock_holder, revision,
                      request_state, request_key, request_revision,
                      request_expected_holder, request_apply_count, receipts,
                      observing, seen_status, seen_progress, seen_at, seen_age,
                      abort_cause, expired_by, committed, aborted,
                      effect_count, now>>

ExpireWithoutGrace(observer, holder) ==
    /\ observing[observer][holder]
    /\ status[holder] \in {"Absent", "Pending"}
    /\ seen_age[observer][holder] < MaxTime
    /\ ~(status[holder] = "Pending" /\ now > lease[holder])
    /\ status' = [status EXCEPT ![holder] = "Aborted"]
    /\ aborted' = aborted \cup {holder}
    /\ abort_cause' = [abort_cause EXCEPT ![holder] = "Expiry"]
    /\ expired_by' = [expired_by EXCEPT ![holder] = observer]
    /\ UNCHANGED <<current_attempt, phase, lease, progress,
                   effective_priority, lock_holder, revision, request_state,
                   request_key, request_revision, request_expected_holder,
                   request_apply_count, receipts, observing, seen_status,
                   seen_progress, seen_at, seen_age, committed, effect_count, now>>

RequestReplayMutantSpec ==
    Init /\ [][DelayedRequestNext \/ \E attempt \in Attempts :
                         ApplyDelayedRequestTwice(attempt)]_vars

RenewalPriorityMutantSpec ==
    Init /\ [][RenewalNext \/ \E op \in Ops : RenewWithNewPriority(op)]_vars

PrematureExpiryMutantSpec ==
    Init /\ [][ObserverNext \/ \E observer \in Attempts, holder \in Attempts :
                         ExpireWithoutGrace(observer, holder)]_vars

=============================================================================
