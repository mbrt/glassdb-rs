-------------------------- MODULE CacheEvidence --------------------------
EXTENDS FiniteSets, Naturals, TLC

CONSTANTS States, InitialState, Ops, NoState, NoOp, MaxSeq

Kinds == {"None", "Read", "Mutation"}
Phases == {"Idle", "Waiting", "Invoked", "Done", "Uncertain", "Abandoned"}
Results ==
    {"None", "CacheHit", "Definitive", "CleanConflict",
     "Unavailable", "Cancelled", "Abandoned"}

ASSUME
    /\ States # {}
    /\ InitialState \in States
    /\ Ops # {}
    /\ NoState \notin States
    /\ NoOp \notin Ops
    /\ MaxSeq \in Nat \ {0}

\* TxCore boundary
\* ----------------
\* FROM TxCore: a freshness requirement is a SequencePoint allocated before
\* the read request, and retained mutation predicates remain semantically safe
\* under state/revision ABA.  TxCore does not treat invocation order as a
\* backend linearization order.
\* TO TxCore: an accepted observation is backed by a definitive fact at or
\* after its claimed invocation lower bound.  A clean conflict supplies no
\* replacement state.  Unavailable or abandoned mutations invalidate older
\* reusable knowledge and can never publish locally after lane ownership ends.
\*
\* One physical path and one database-local lane are modeled.  A detached
\* mutation may still apply remotely and is then an external writer.  Snapshot
\* reads and persistent L2 policy are deliberately outside this interface.

VARIABLES
    backend_state,
    cache_state,
    cache_after,
    cache_source,
    lane,
    next_seq,
    phase,
    kind,
    required_after,
    invoked_at,
    expected_state,
    target_state,
    remote_pending,
    remote_applied,
    definitive_facts,
    accepted_state,
    accepted_after,
    result,
    invalidated_at

vars ==
    <<backend_state,
      cache_state,
      cache_after,
      cache_source,
      lane,
      next_seq,
      phase,
      kind,
      required_after,
      invoked_at,
      expected_state,
      target_state,
      remote_pending,
      remote_applied,
      definitive_facts,
      accepted_state,
      accepted_after,
      result,
      invalidated_at>>

Init ==
    /\ backend_state = InitialState
    /\ cache_state = NoState
    /\ cache_after = 0
    /\ cache_source = NoOp
    /\ lane = NoOp
    /\ next_seq = 1
    /\ phase = [op \in Ops |-> "Idle"]
    /\ kind = [op \in Ops |-> "None"]
    /\ required_after = [op \in Ops |-> 0]
    /\ invoked_at = [op \in Ops |-> 0]
    /\ expected_state = [op \in Ops |-> NoState]
    /\ target_state = [op \in Ops |-> NoState]
    /\ remote_pending = {}
    /\ remote_applied = {}
    /\ definitive_facts = {}
    /\ accepted_state = [op \in Ops |-> NoState]
    /\ accepted_after = [op \in Ops |-> 0]
    /\ result = [op \in Ops |-> "None"]
    /\ invalidated_at = 0

RequestRead(op, lower_bound) ==
    /\ phase[op] = "Idle"
    /\ lower_bound \in 0..(next_seq - 1)
    /\ phase' = [phase EXCEPT ![op] = "Waiting"]
    /\ kind' = [kind EXCEPT ![op] = "Read"]
    /\ required_after' =
        [required_after EXCEPT ![op] = lower_bound]
    /\ UNCHANGED <<backend_state, cache_state, cache_after, cache_source,
                   lane, next_seq, invoked_at, expected_state, target_state,
                   remote_pending, remote_applied, definitive_facts,
                   accepted_state, accepted_after, result, invalidated_at>>

RequestMutation(op, new_state) ==
    /\ phase[op] = "Idle"
    /\ cache_state \in States
    /\ new_state \in States \ {cache_state}
    /\ phase' = [phase EXCEPT ![op] = "Waiting"]
    /\ kind' = [kind EXCEPT ![op] = "Mutation"]
    /\ expected_state' =
        [expected_state EXCEPT ![op] = cache_state]
    /\ target_state' = [target_state EXCEPT ![op] = new_state]
    /\ UNCHANGED <<backend_state, cache_state, cache_after, cache_source,
                   lane, next_seq, required_after, invoked_at,
                   remote_pending, remote_applied, definitive_facts,
                   accepted_state, accepted_after, result, invalidated_at>>

ServeCache(op) ==
    /\ phase[op] = "Waiting"
    /\ kind[op] = "Read"
    /\ cache_state \in States
    /\ cache_after >= required_after[op]
    /\ phase' = [phase EXCEPT ![op] = "Done"]
    /\ accepted_state' =
        [accepted_state EXCEPT ![op] = cache_state]
    /\ accepted_after' =
        [accepted_after EXCEPT ![op] = cache_after]
    /\ result' = [result EXCEPT ![op] = "CacheHit"]
    /\ UNCHANGED <<backend_state, cache_state, cache_after, cache_source,
                   lane, next_seq, kind, required_after, invoked_at,
                   expected_state, target_state, remote_pending,
                   remote_applied, definitive_facts, invalidated_at>>

CancelQueued(op) ==
    /\ phase[op] = "Waiting"
    /\ phase' = [phase EXCEPT ![op] = "Abandoned"]
    /\ result' = [result EXCEPT ![op] = "Cancelled"]
    /\ UNCHANGED <<backend_state, cache_state, cache_after, cache_source,
                   lane, next_seq, kind, required_after, invoked_at,
                   expected_state, target_state, remote_pending,
                   remote_applied, definitive_facts, accepted_state,
                   accepted_after, invalidated_at>>

InvokeRead(op) ==
    /\ phase[op] = "Waiting"
    /\ kind[op] = "Read"
    /\ \/ cache_state = NoState
       \/ cache_after < required_after[op]
    /\ lane = NoOp
    /\ next_seq <= MaxSeq
    /\ lane' = op
    /\ phase' = [phase EXCEPT ![op] = "Invoked"]
    /\ invoked_at' = [invoked_at EXCEPT ![op] = next_seq]
    /\ next_seq' = next_seq + 1
    /\ UNCHANGED <<backend_state, cache_state, cache_after, cache_source,
                   kind, required_after, expected_state, target_state,
                   remote_pending, remote_applied, definitive_facts,
                   accepted_state, accepted_after, result, invalidated_at>>

InvokeMutation(op) ==
    /\ phase[op] = "Waiting"
    /\ kind[op] = "Mutation"
    /\ lane = NoOp
    /\ next_seq <= MaxSeq
    /\ lane' = op
    /\ phase' = [phase EXCEPT ![op] = "Invoked"]
    /\ invoked_at' = [invoked_at EXCEPT ![op] = next_seq]
    /\ next_seq' = next_seq + 1
    /\ remote_pending' = remote_pending \cup {op}
    /\ UNCHANGED <<backend_state, cache_state, cache_after, cache_source,
                   kind, required_after, expected_state, target_state,
                   remote_applied, definitive_facts, accepted_state,
                   accepted_after, result, invalidated_at>>

CompleteRead(op) ==
    /\ phase[op] = "Invoked"
    /\ kind[op] = "Read"
    /\ lane = op
    /\ cache_state' = backend_state
    /\ cache_after' = invoked_at[op]
    /\ cache_source' = op
    /\ definitive_facts' =
        definitive_facts \cup {<<invoked_at[op], backend_state>>}
    /\ accepted_state' =
        [accepted_state EXCEPT ![op] = backend_state]
    /\ accepted_after' =
        [accepted_after EXCEPT ![op] = invoked_at[op]]
    /\ phase' = [phase EXCEPT ![op] = "Done"]
    /\ result' = [result EXCEPT ![op] = "Definitive"]
    /\ lane' = NoOp
    /\ UNCHANGED <<backend_state, next_seq, kind, required_after,
                   invoked_at, expected_state, target_state, remote_pending,
                   remote_applied, invalidated_at>>

CancelInvokedRead(op) ==
    /\ phase[op] = "Invoked"
    /\ kind[op] = "Read"
    /\ lane = op
    /\ phase' = [phase EXCEPT ![op] = "Abandoned"]
    /\ result' = [result EXCEPT ![op] = "Cancelled"]
    /\ lane' = NoOp
    /\ UNCHANGED <<backend_state, cache_state, cache_after, cache_source,
                   next_seq, kind, required_after, invoked_at,
                   expected_state, target_state, remote_pending,
                   remote_applied, definitive_facts, accepted_state,
                   accepted_after, invalidated_at>>

\* This is the provider's conditional effect.  It may occur after the local
\* caller returned Unavailable or abandoned its future, and ABA may make the
\* original state predicate true again.
ApplyMutation(op) ==
    /\ op \in remote_pending
    /\ op \notin remote_applied
    /\ backend_state = expected_state[op]
    /\ backend_state' = target_state[op]
    /\ remote_applied' = remote_applied \cup {op}
    /\ UNCHANGED <<cache_state, cache_after, cache_source, lane, next_seq,
                   phase, kind, required_after, invoked_at, expected_state,
                   target_state, remote_pending, definitive_facts,
                   accepted_state, accepted_after, result, invalidated_at>>

CompleteMutationSuccess(op) ==
    /\ phase[op] = "Invoked"
    /\ kind[op] = "Mutation"
    /\ lane = op
    /\ op \in remote_pending \cap remote_applied
    /\ cache_state' = target_state[op]
    /\ cache_after' = invoked_at[op]
    /\ cache_source' = op
    /\ definitive_facts' =
        definitive_facts \cup {<<invoked_at[op], target_state[op]>>}
    /\ accepted_state' =
        [accepted_state EXCEPT ![op] = target_state[op]]
    /\ accepted_after' =
        [accepted_after EXCEPT ![op] = invoked_at[op]]
    /\ remote_pending' = remote_pending \ {op}
    /\ phase' = [phase EXCEPT ![op] = "Done"]
    /\ result' = [result EXCEPT ![op] = "Definitive"]
    /\ lane' = NoOp
    /\ UNCHANGED <<backend_state, next_seq, kind, required_after,
                   invoked_at, expected_state, target_state, remote_applied,
                   invalidated_at>>

CompleteCleanConflict(op) ==
    /\ phase[op] = "Invoked"
    /\ kind[op] = "Mutation"
    /\ lane = op
    /\ op \in remote_pending
    /\ op \notin remote_applied
    /\ backend_state # expected_state[op]
    /\ cache_state' = NoState
    /\ cache_after' = 0
    /\ cache_source' = NoOp
    /\ invalidated_at' = invoked_at[op]
    /\ remote_pending' = remote_pending \ {op}
    /\ phase' = [phase EXCEPT ![op] = "Done"]
    /\ result' = [result EXCEPT ![op] = "CleanConflict"]
    /\ lane' = NoOp
    /\ UNCHANGED <<backend_state, next_seq, kind, required_after,
                   invoked_at, expected_state, target_state, remote_applied,
                   definitive_facts, accepted_state, accepted_after>>

ReturnUnavailable(op) ==
    /\ phase[op] = "Invoked"
    /\ kind[op] = "Mutation"
    /\ lane = op
    /\ op \in remote_pending
    /\ cache_state' = NoState
    /\ cache_after' = 0
    /\ cache_source' = NoOp
    /\ invalidated_at' = invoked_at[op]
    /\ phase' = [phase EXCEPT ![op] = "Uncertain"]
    /\ result' = [result EXCEPT ![op] = "Unavailable"]
    /\ lane' = NoOp
    /\ UNCHANGED <<backend_state, next_seq, kind, required_after,
                   invoked_at, expected_state, target_state, remote_pending,
                   remote_applied, definitive_facts, accepted_state,
                   accepted_after>>

AbandonMutation(op) ==
    /\ phase[op] = "Invoked"
    /\ kind[op] = "Mutation"
    /\ lane = op
    /\ op \in remote_pending
    /\ cache_state' = NoState
    /\ cache_after' = 0
    /\ cache_source' = NoOp
    /\ invalidated_at' = invoked_at[op]
    /\ phase' = [phase EXCEPT ![op] = "Abandoned"]
    /\ result' = [result EXCEPT ![op] = "Abandoned"]
    /\ lane' = NoOp
    /\ UNCHANGED <<backend_state, next_seq, kind, required_after,
                   invoked_at, expected_state, target_state, remote_pending,
                   remote_applied, definitive_facts, accepted_state,
                   accepted_after>>

\* Once ownership has ended, the provider may settle with or without an
\* already-modeled effect, but there is no local cache publisher.
SettleDetachedMutation(op) ==
    /\ op \in remote_pending
    /\ phase[op] \in {"Uncertain", "Abandoned"}
    /\ remote_pending' = remote_pending \ {op}
    /\ UNCHANGED <<backend_state, cache_state, cache_after, cache_source,
                   lane, next_seq, phase, kind, required_after, invoked_at,
                   expected_state, target_state, remote_applied,
                   definitive_facts, accepted_state, accepted_after, result,
                   invalidated_at>>

Next ==
    \/ \E op \in Ops, lower_bound \in 0..MaxSeq :
           RequestRead(op, lower_bound)
    \/ \E op \in Ops, new_state \in States :
           RequestMutation(op, new_state)
    \/ \E op \in Ops : ServeCache(op)
    \/ \E op \in Ops : CancelQueued(op)
    \/ \E op \in Ops : InvokeRead(op)
    \/ \E op \in Ops : InvokeMutation(op)
    \/ \E op \in Ops : CompleteRead(op)
    \/ \E op \in Ops : CancelInvokedRead(op)
    \/ \E op \in Ops : ApplyMutation(op)
    \/ \E op \in Ops : CompleteMutationSuccess(op)
    \/ \E op \in Ops : CompleteCleanConflict(op)
    \/ \E op \in Ops : ReturnUnavailable(op)
    \/ \E op \in Ops : AbandonMutation(op)
    \/ \E op \in Ops : SettleDetachedMutation(op)

Spec == Init /\ [][Next]_vars

CE0_TypeOK ==
    /\ backend_state \in States
    /\ cache_state \in States \cup {NoState}
    /\ cache_after \in 0..MaxSeq
    /\ cache_source \in Ops \cup {NoOp}
    /\ lane \in Ops \cup {NoOp}
    /\ next_seq \in 1..(MaxSeq + 1)
    /\ phase \in [Ops -> Phases]
    /\ kind \in [Ops -> Kinds]
    /\ required_after \in [Ops -> 0..MaxSeq]
    /\ invoked_at \in [Ops -> 0..MaxSeq]
    /\ expected_state \in [Ops -> States \cup {NoState}]
    /\ target_state \in [Ops -> States \cup {NoState}]
    /\ remote_pending \subseteq Ops
    /\ remote_applied \subseteq Ops
    /\ definitive_facts \subseteq ((1..MaxSeq) \X States)
    /\ accepted_state \in [Ops -> States \cup {NoState}]
    /\ accepted_after \in [Ops -> 0..MaxSeq]
    /\ result \in [Ops -> Results]
    /\ invalidated_at \in 0..MaxSeq

CE1_ExclusiveLane ==
    /\ (lane # NoOp => phase[lane] = "Invoked")
    /\ \A op \in Ops : phase[op] = "Invoked" => lane = op

CE2_CacheHasDefinitiveEvidence ==
    IF cache_state = NoState
    THEN /\ cache_after = 0
         /\ cache_source = NoOp
    ELSE /\ <<cache_after, cache_state>> \in definitive_facts
         /\ cache_source \in Ops
         /\ phase[cache_source] = "Done"

CE3_UncertainKnowledgeNotReusable ==
    cache_state = NoState \/ cache_after > invalidated_at

CE4_AcceptedEvidenceMeetsBound ==
    \A op \in Ops :
        accepted_state[op] # NoState =>
            /\ <<accepted_after[op], accepted_state[op]>>
               \in definitive_facts
            /\ accepted_after[op] >= required_after[op]

CE5_NoReplacementFromNondefinitiveMutation ==
    \A op \in Ops :
        result[op] \in {"CleanConflict", "Unavailable", "Abandoned"} =>
            /\ invalidated_at >= invoked_at[op]
            /\ cache_source # op
            /\ \A state \in States :
                   <<invoked_at[op], state>> \notin definitive_facts

\* The named guarantee imported by TxCore.
TxCoreCacheGuarantee ==
    /\ CE2_CacheHasDefinitiveEvidence
    /\ CE3_UncertainKnowledgeNotReusable
    /\ CE4_AcceptedEvidenceMeetsBound
    /\ CE5_NoReplacementFromNondefinitiveMutation

\* Negative control: lane release happens, but the pre-mutation cache entry is
\* left reusable even though the detached request can still apply remotely.
AbandonWithoutInvalidation(op) ==
    /\ phase[op] = "Invoked"
    /\ kind[op] = "Mutation"
    /\ lane = op
    /\ op \in remote_pending
    /\ invalidated_at' = invoked_at[op]
    /\ phase' = [phase EXCEPT ![op] = "Abandoned"]
    /\ result' = [result EXCEPT ![op] = "Abandoned"]
    /\ lane' = NoOp
    /\ UNCHANGED <<backend_state, cache_state, cache_after, cache_source,
                   next_seq, kind, required_after, invoked_at,
                   expected_state, target_state, remote_pending,
                   remote_applied, definitive_facts, accepted_state,
                   accepted_after>>

MutantSpec ==
    Init /\ [][Next
                 \/ \E op \in Ops : AbandonWithoutInvalidation(op)]_vars

=============================================================================
