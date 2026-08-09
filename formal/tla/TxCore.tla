------------------------------ MODULE TxCore -------------------------------
EXTENDS Backend, Integers, TLC

CONSTANTS
    Txns,
    Keys,
    Leaves,
    Values,
    Programs,
    NoTx,
    InitWriter,
    NoProgram,
    NoWrite,
    KeyLeaf,
    LeafRank,
    Priority,
    ProgramReads,
    ProgramWrites,
    ProgramResult,
    InitialDb,
    MaxTime

TxStatuses == {"Absent", "Pending", "Committed", "Aborted"}

ClientPhases ==
    { "Idle",
      "Reading",
      "ReadValidating",
      "Acquiring",
      "Validating",
      "Ready",
      "Committing",
      "Done" }

PublicOutcomes ==
    { "None",
      "Success",
      "ValidatedError",
      "DefiniteFailure",
      "InDoubt",
      "Abandoned" }

ProgramResults == {"Commit", "ValidatedError", "Abort"}

DefinitiveLinearizedOutcomes ==
    {"Success", "ValidatedError"}

ActivePhases == ClientPhases \ {"Idle", "Done"}

ClientType ==
    [ phase           : ClientPhases,
      outcome         : PublicOutcomes,
      program         : Programs \cup {NoProgram},
      read_done       : SUBSET Keys,
      observed_writer : [Keys -> Txns \cup {InitWriter, NoTx}],
      observed_value  : [Keys -> Values],
      held            : SUBSET Keys,
      serial_mode     : BOOLEAN,
      validated       : BOOLEAN,
      commit_snapshot : [Keys -> Values],
      commit_writer_snapshot : [Keys -> Txns \cup {InitWriter}],
      commit_post     : [Keys -> Values],
      commit_had_locks: BOOLEAN,
      completed_before: SUBSET Txns,
      mutation        : BackendMutationStates,
      expected_status : TxStatuses,
      expected_lease  : 0..MaxTime ]

VARIABLES
    logical_db,
    base_value,
    base_writer,
    last_writer,
    tx_status,
    tx_lease,
    clients,
    read_locks,
    write_lock,
    committed,
    aborted,
    linearized,
    lin_count,
    now

vars ==
    << logical_db,
       base_value,
       base_writer,
       last_writer,
       tx_status,
       tx_lease,
       clients,
       read_locks,
       write_lock,
       committed,
       aborted,
       linearized,
       lin_count,
       now >>

ASSUME
    /\ Txns # {}
    /\ Keys # {}
    /\ Leaves # {}
    /\ Values # {}
    /\ Programs # {}
    /\ NoTx \notin Txns
    /\ InitWriter \notin Txns
    /\ NoTx # InitWriter
    /\ NoProgram \notin Programs
    /\ NoWrite \notin Values
    /\ KeyLeaf \in [Keys -> Leaves]
    /\ LeafRank \in [Leaves -> Nat]
    /\ Priority \in [Txns -> Nat]
    /\ ProgramReads \in [Programs -> SUBSET Keys]
    /\ ProgramWrites \in [Programs -> [Keys -> Values \cup {NoWrite}]]
    /\ ProgramResult \in [Programs -> ProgramResults]
    /\ InitialDb \in [Keys -> Values]
    /\ MaxTime \in Nat

InitialClient ==
    [ phase            |-> "Idle",
      outcome          |-> "None",
      program          |-> NoProgram,
      read_done        |-> {},
      observed_writer  |-> [key \in Keys |-> NoTx],
      observed_value   |-> InitialDb,
      held             |-> {},
      serial_mode      |-> FALSE,
      validated        |-> FALSE,
      commit_snapshot  |-> InitialDb,
      commit_writer_snapshot |-> [key \in Keys |-> InitWriter],
      commit_post      |-> InitialDb,
      commit_had_locks |-> FALSE,
      completed_before |-> {},
      mutation         |-> "None",
      expected_status  |-> "Absent",
      expected_lease   |-> 0 ]

WriteKeys(tx) ==
    IF clients[tx].program = NoProgram
    THEN {}
    ELSE {key \in Keys :
              ProgramWrites[clients[tx].program][key] # NoWrite}

ReadKeys(tx) ==
    IF clients[tx].program = NoProgram
    THEN {}
    ELSE ProgramReads[clients[tx].program]

RequiredKeys(tx) == ReadKeys(tx) \cup WriteKeys(tx)

RequiredOnLeaf(tx, leaf) ==
    {key \in RequiredKeys(tx) : KeyLeaf[key] = leaf}

LockMode(tx, key) ==
    IF key \in WriteKeys(tx) THEN "Write" ELSE "Read"

PhysicalLocks(tx) ==
    {key \in Keys :
        write_lock[key] = tx \/ tx \in read_locks[key]}

HasAllLocks(tx) ==
    RequiredKeys(tx) \subseteq clients[tx].held

CanInstallLock(tx, key) ==
    IF LockMode(tx, key) = "Write"
    THEN /\ write_lock[key] \in {NoTx, tx}
         /\ read_locks[key] \subseteq {tx}
    ELSE write_lock[key] \in {NoTx, tx}

CanInstallLeaf(tx, leaf) ==
    \A key \in RequiredOnLeaf(tx, leaf) : CanInstallLock(tx, key)

LeafStillNeeded(tx, leaf) ==
    /\ RequiredOnLeaf(tx, leaf) # {}
    /\ ~RequiredOnLeaf(tx, leaf) \subseteq clients[tx].held

AllowedAcquisitionLeaf(tx, leaf) ==
    /\ LeafStillNeeded(tx, leaf)
    /\ \/ ~clients[tx].serial_mode
       \/ \A other \in Leaves :
              LeafStillNeeded(tx, other) => LeafRank[leaf] <= LeafRank[other]

ReadLockAfterInstall(tx, leaf, key) ==
    IF /\ key \in RequiredOnLeaf(tx, leaf)
       /\ LockMode(tx, key) = "Read"
    THEN read_locks[key] \cup {tx}
    ELSE read_locks[key]

WriteLockAfterInstall(tx, leaf, key) ==
    IF /\ key \in RequiredOnLeaf(tx, leaf)
       /\ LockMode(tx, key) = "Write"
    THEN tx
    ELSE write_lock[key]

ConflictsAt(tx, key) ==
    IF LockMode(tx, key) = "Write"
    THEN (read_locks[key] \ {tx})
         \cup (IF write_lock[key] \in {NoTx, tx}
               THEN {}
               ELSE {write_lock[key]})
    ELSE IF write_lock[key] \in {NoTx, tx}
         THEN {}
         ELSE {write_lock[key]}

ConflictingHolders(tx, leaf) ==
    UNION {ConflictsAt(tx, key) : key \in RequiredOnLeaf(tx, leaf)}

ObservationsCurrent(tx) ==
    \A key \in ReadKeys(tx) :
        /\ clients[tx].observed_writer[key] = last_writer[key]
        /\ clients[tx].observed_value[key] = logical_db[key]

ApplyWrites(state, tx) ==
    [key \in Keys |->
        IF key \in WriteKeys(tx)
        THEN ProgramWrites[clients[tx].program][key]
        ELSE state[key]]

LogicalView(key) ==
    IF /\ write_lock[key] # NoTx
       /\ tx_status[write_lock[key]] = "Committed"
       /\ key \in WriteKeys(write_lock[key])
    THEN ProgramWrites[clients[write_lock[key]].program][key]
    ELSE base_value[key]

ApplyLogicalEvent(state, tx) ==
    IF tx \in committed THEN ApplyWrites(state, tx) ELSE state

RECURSIVE Replay(_)

Replay(history) ==
    IF Len(history) = 0
    THEN InitialDb
    ELSE ApplyLogicalEvent(
             Replay(SubSeq(history, 1, Len(history) - 1)),
             history[Len(history)])

Init ==
    /\ logical_db = InitialDb
    /\ base_value = InitialDb
    /\ base_writer = [key \in Keys |-> InitWriter]
    /\ last_writer = [key \in Keys |-> InitWriter]
    /\ tx_status = [tx \in Txns |-> "Absent"]
    /\ tx_lease = [tx \in Txns |-> 0]
    /\ clients = [tx \in Txns |-> InitialClient]
    /\ read_locks = [key \in Keys |-> {}]
    /\ write_lock = [key \in Keys |-> NoTx]
    /\ committed = {}
    /\ aborted = {}
    /\ linearized = <<>>
    /\ lin_count = [tx \in Txns |-> 0]
    /\ now = 0

Invoke(tx, selected_program) ==
    /\ clients[tx].phase = "Idle"
    /\ selected_program \in Programs
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Reading",
            ![tx].program = selected_program,
            ![tx].completed_before =
                {other \in Txns :
                    clients[other].outcome
                        \in DefinitiveLinearizedOutcomes}]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

BodyRead(tx, key) ==
    /\ clients[tx].phase = "Reading"
    /\ key \in ReadKeys(tx) \ clients[tx].read_done
    /\ clients' =
        [clients EXCEPT
            ![tx].read_done = @ \cup {key},
            ![tx].observed_writer[key] = last_writer[key],
            ![tx].observed_value[key] = LogicalView(key)]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

FinishBody(tx) ==
    /\ clients[tx].phase = "Reading"
    /\ clients[tx].read_done = ReadKeys(tx)
    /\ IF ProgramResult[clients[tx].program] = "Abort"
       THEN clients' =
            [clients EXCEPT
                ![tx].phase = "Done",
                ![tx].outcome = "DefiniteFailure"]
       ELSE IF \/ ProgramResult[clients[tx].program] = "ValidatedError"
               \/ WriteKeys(tx) = {}
            THEN clients' =
                 [clients EXCEPT ![tx].phase = "ReadValidating"]
            ELSE clients' =
                 [clients EXCEPT
                    ![tx].phase =
                        IF HasAllLocks(tx)
                        THEN "Validating"
                        ELSE "Acquiring"]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

ValidateReadOnly(tx) ==
    /\ clients[tx].phase = "ReadValidating"
    /\ ObservationsCurrent(tx)
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Done",
            ![tx].outcome =
                IF ProgramResult[clients[tx].program] = "ValidatedError"
                THEN "ValidatedError"
                ELSE "Success",
            ![tx].validated = TRUE,
            ![tx].commit_snapshot = logical_db,
            ![tx].commit_writer_snapshot = last_writer,
            ![tx].commit_post = logical_db]
    /\ linearized' = Append(linearized, tx)
    /\ lin_count' = [lin_count EXCEPT ![tx] = @ + 1]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, now>>

RetryReadOnly(tx) ==
    /\ clients[tx].phase = "ReadValidating"
    /\ ~ObservationsCurrent(tx)
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Reading",
            ![tx].read_done = {},
            ![tx].observed_writer = [key \in Keys |-> NoTx],
            ![tx].observed_value = InitialDb]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

AcquireLeaf(tx, leaf) ==
    LET acquired == clients[tx].held \cup RequiredOnLeaf(tx, leaf)
    IN /\ clients[tx].phase = "Acquiring"
       /\ leaf \in Leaves
       /\ AllowedAcquisitionLeaf(tx, leaf)
       /\ CanInstallLeaf(tx, leaf)
       /\ read_locks' =
           [key \in Keys |-> ReadLockAfterInstall(tx, leaf, key)]
       /\ write_lock' =
           [key \in Keys |-> WriteLockAfterInstall(tx, leaf, key)]
       /\ clients' =
           [clients EXCEPT
               ![tx].held = acquired,
               ![tx].phase =
                   IF RequiredKeys(tx) \subseteq acquired
                   THEN "Validating"
                   ELSE "Acquiring"]
       /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                      tx_status, tx_lease, committed, aborted,
                      linearized, lin_count, now>>

\* This is the reduction of a landed lock CAS whose acknowledgement was lost.
\* Re-reading the leaf later recovers the receipt; cancellation may instead
\* leave the installed lock for wound/expiry cleanup.
AcquireLeafLostAck(tx, leaf) ==
    /\ clients[tx].phase = "Acquiring"
    /\ leaf \in Leaves
    /\ AllowedAcquisitionLeaf(tx, leaf)
    /\ CanInstallLeaf(tx, leaf)
    /\ read_locks' =
        [key \in Keys |-> ReadLockAfterInstall(tx, leaf, key)]
    /\ write_lock' =
        [key \in Keys |-> WriteLockAfterInstall(tx, leaf, key)]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, clients, committed, aborted,
                   linearized, lin_count, now>>

Wound(requester, holder, leaf) ==
    /\ clients[requester].phase = "Acquiring"
    /\ leaf \in Leaves
    /\ AllowedAcquisitionLeaf(requester, leaf)
    /\ holder \in ConflictingHolders(requester, leaf)
    /\ Priority[requester] < Priority[holder]
    /\ tx_status[holder] \in {"Absent", "Pending"}
    /\ tx_status' = [tx_status EXCEPT ![holder] = "Aborted"]
    /\ aborted' = aborted \cup {holder}
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_lease, clients, read_locks, write_lock,
                   committed, linearized, lin_count, now>>

ObserveAbort(tx) ==
    /\ tx_status[tx] = "Aborted"
    /\ clients[tx].phase \in ActivePhases
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Done",
            ![tx].outcome = "Abandoned",
            ![tx].mutation = "None"]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

ReleaseAbortedLock(tx, key) ==
    /\ tx_status[tx] = "Aborted"
    /\ key \in PhysicalLocks(tx)
    /\ read_locks' =
        [read_locks EXCEPT ![key] = @ \ {tx}]
    /\ write_lock' =
        IF write_lock[key] = tx
        THEN [write_lock EXCEPT ![key] = NoTx]
        ELSE write_lock
    /\ clients' =
        [clients EXCEPT ![tx].held = @ \ {key}]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, committed, aborted,
                   linearized, lin_count, now>>

DeadlockTimeout(tx) ==
    /\ clients[tx].phase = "Acquiring"
    /\ clients[tx].held # {}
    /\ \E leaf \in Leaves :
           /\ AllowedAcquisitionLeaf(tx, leaf)
           /\ ConflictingHolders(tx, leaf) # {}
    /\ read_locks' =
        [key \in Keys |-> read_locks[key] \ {tx}]
    /\ write_lock' =
        [key \in Keys |->
            IF write_lock[key] = tx THEN NoTx ELSE write_lock[key]]
    /\ clients' =
        [clients EXCEPT
            ![tx].held = {},
            ![tx].serial_mode = TRUE]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, committed, aborted,
                   linearized, lin_count, now>>

RefreshPending(tx) ==
    /\ clients[tx].phase \in ActivePhases
    /\ PhysicalLocks(tx) # {}
    /\ tx_status[tx] \in {"Absent", "Pending"}
    /\ tx_status' = [tx_status EXCEPT ![tx] = "Pending"]
    /\ tx_lease' = [tx_lease EXCEPT ![tx] = now]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   clients, read_locks, write_lock, committed, aborted,
                   linearized, lin_count, now>>

ValidateAfterLocking(tx) ==
    /\ clients[tx].phase = "Validating"
    /\ HasAllLocks(tx)
    /\ ObservationsCurrent(tx)
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Ready",
            ![tx].validated = TRUE]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

RetryBodyAfterStaleRead(tx) ==
    /\ clients[tx].phase = "Validating"
    /\ HasAllLocks(tx)
    /\ ~ObservationsCurrent(tx)
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Reading",
            ![tx].read_done = {},
            ![tx].observed_writer = [key \in Keys |-> NoTx],
            ![tx].observed_value = InitialDb,
            ![tx].validated = FALSE]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

DispatchCommit(tx) ==
    /\ clients[tx].phase = "Ready"
    /\ clients[tx].validated
    /\ HasAllLocks(tx)
    /\ tx_status[tx] \in {"Absent", "Pending"}
    /\ clients[tx].mutation = "None"
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Committing",
            ![tx].mutation = "Dispatched",
            ![tx].expected_status = tx_status[tx],
            ![tx].expected_lease = tx_lease[tx]]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

CommitPredicateHolds(tx) ==
    /\ tx_status[tx] = clients[tx].expected_status
    /\ tx_lease[tx] = clients[tx].expected_lease
    /\ tx_status[tx] \in {"Absent", "Pending"}

ApplyCommit(tx) ==
    /\ CanStillApply(clients[tx].mutation)
    /\ CommitPredicateHolds(tx)
    /\ tx_status' = [tx_status EXCEPT ![tx] = "Committed"]
    /\ committed' = committed \cup {tx}
    /\ logical_db' = ApplyWrites(logical_db, tx)
    /\ last_writer' =
        [key \in Keys |->
            IF key \in WriteKeys(tx) THEN tx ELSE last_writer[key]]
    /\ clients' =
        [clients EXCEPT
            ![tx].mutation = "Applied",
            ![tx].commit_snapshot = logical_db,
            ![tx].commit_writer_snapshot = last_writer,
            ![tx].commit_post = ApplyWrites(logical_db, tx),
            ![tx].commit_had_locks = HasAllLocks(tx)]
    /\ linearized' = Append(linearized, tx)
    /\ lin_count' = [lin_count EXCEPT ![tx] = @ + 1]
    /\ UNCHANGED <<base_value, base_writer, tx_lease,
                   read_locks, write_lock, aborted, now>>

CommitPreconditionFailed(tx) ==
    /\ CanStillApply(clients[tx].mutation)
    /\ ~CommitPredicateHolds(tx)
    /\ clients' =
        IF clients[tx].phase = "Done"
        THEN [clients EXCEPT ![tx].mutation = "None"]
        ELSE IF tx_status[tx] = "Aborted"
        THEN [clients EXCEPT
                ![tx].phase = "Done",
                ![tx].outcome = "Abandoned",
                ![tx].mutation = "None"]
        ELSE IF clients[tx].phase = "Committing"
             THEN [clients EXCEPT
                    ![tx].phase = "Ready",
                    ![tx].mutation = "None"]
             ELSE [clients EXCEPT ![tx].mutation = "None"]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

ReturnInDoubt(tx) ==
    /\ clients[tx].phase = "Committing"
    /\ clients[tx].mutation \in {"Dispatched", "Applied"}
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Done",
            ![tx].outcome = "InDoubt",
            ![tx].mutation =
                IF clients[tx].mutation = "Dispatched"
                THEN "Unresolved"
                ELSE "Applied"]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

AcknowledgeCommit(tx) ==
    /\ clients[tx].phase = "Committing"
    /\ clients[tx].mutation = "Applied"
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Done",
            ![tx].outcome = "Success",
            ![tx].mutation = "None"]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

ForgetLateCommitResult(tx) ==
    /\ clients[tx].phase = "Done"
    /\ clients[tx].outcome \in {"InDoubt", "Abandoned"}
    /\ clients[tx].mutation = "Applied"
    /\ clients' = [clients EXCEPT ![tx].mutation = "None"]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

Abandon(tx) ==
    /\ clients[tx].phase \in ActivePhases
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Done",
            ![tx].outcome = "Abandoned",
            ![tx].mutation =
                IF @ = "Dispatched" THEN "Unresolved" ELSE @]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock,
                   committed, aborted, linearized, lin_count, now>>

WriteBack(tx, key) ==
    /\ tx_status[tx] = "Committed"
    /\ key \in WriteKeys(tx)
    /\ write_lock[key] = tx
    /\ base_value' =
        [base_value EXCEPT
            ![key] = ProgramWrites[clients[tx].program][key]]
    /\ base_writer' = [base_writer EXCEPT ![key] = tx]
    /\ write_lock' = [write_lock EXCEPT ![key] = NoTx]
    /\ clients' = [clients EXCEPT ![tx].held = @ \ {key}]
    /\ UNCHANGED <<logical_db, last_writer, tx_status, tx_lease,
                   read_locks, committed, aborted, linearized,
                   lin_count, now>>

ReleaseCommittedRead(tx, key) ==
    /\ tx_status[tx] = "Committed"
    /\ tx \in read_locks[key]
    /\ read_locks' = [read_locks EXCEPT ![key] = @ \ {tx}]
    /\ clients' = [clients EXCEPT ![tx].held = @ \ {key}]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, write_lock, committed, aborted,
                   linearized, lin_count, now>>

AdvanceTime ==
    /\ now < MaxTime
    /\ now' = now + 1
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, clients, read_locks, write_lock,
                   committed, aborted, linearized, lin_count>>

ExpirePending(tx) ==
    /\ tx_status[tx] = "Pending"
    /\ now > tx_lease[tx]
    /\ tx_status' = [tx_status EXCEPT ![tx] = "Aborted"]
    /\ aborted' = aborted \cup {tx}
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_lease, clients, read_locks, write_lock,
                   committed, linearized, lin_count, now>>

ExpireAbsentHolder(tx) ==
    /\ tx_status[tx] = "Absent"
    /\ PhysicalLocks(tx) # {}
    /\ now = MaxTime
    /\ tx_status' = [tx_status EXCEPT ![tx] = "Aborted"]
    /\ aborted' = aborted \cup {tx}
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_lease, clients, read_locks, write_lock,
                   committed, linearized, lin_count, now>>

Next ==
    \/ \E tx \in Txns, selected_program \in Programs :
           Invoke(tx, selected_program)
    \/ \E tx \in Txns, key \in Keys : BodyRead(tx, key)
    \/ \E tx \in Txns : FinishBody(tx)
    \/ \E tx \in Txns : ValidateReadOnly(tx)
    \/ \E tx \in Txns : RetryReadOnly(tx)
    \/ \E tx \in Txns, leaf \in Leaves : AcquireLeaf(tx, leaf)
    \/ \E tx \in Txns, leaf \in Leaves : AcquireLeafLostAck(tx, leaf)
    \/ \E requester \in Txns, holder \in Txns, leaf \in Leaves :
           Wound(requester, holder, leaf)
    \/ \E tx \in Txns : ObserveAbort(tx)
    \/ \E tx \in Txns, key \in Keys : ReleaseAbortedLock(tx, key)
    \/ \E tx \in Txns : DeadlockTimeout(tx)
    \/ \E tx \in Txns : RefreshPending(tx)
    \/ \E tx \in Txns : ValidateAfterLocking(tx)
    \/ \E tx \in Txns : RetryBodyAfterStaleRead(tx)
    \/ \E tx \in Txns : DispatchCommit(tx)
    \/ \E tx \in Txns : ApplyCommit(tx)
    \/ \E tx \in Txns : CommitPreconditionFailed(tx)
    \/ \E tx \in Txns : ReturnInDoubt(tx)
    \/ \E tx \in Txns : AcknowledgeCommit(tx)
    \/ \E tx \in Txns : ForgetLateCommitResult(tx)
    \/ \E tx \in Txns : Abandon(tx)
    \/ \E tx \in Txns, key \in Keys : WriteBack(tx, key)
    \/ \E tx \in Txns, key \in Keys : ReleaseCommittedRead(tx, key)
    \/ AdvanceTime
    \/ \E tx \in Txns : ExpirePending(tx)
    \/ \E tx \in Txns : ExpireAbsentHolder(tx)

Spec == Init /\ [][Next]_vars

S0_TypeOK ==
    /\ logical_db \in [Keys -> Values]
    /\ base_value \in [Keys -> Values]
    /\ base_writer \in [Keys -> Txns \cup {InitWriter}]
    /\ last_writer \in [Keys -> Txns \cup {InitWriter}]
    /\ tx_status \in [Txns -> TxStatuses]
    /\ tx_lease \in [Txns -> 0..MaxTime]
    /\ clients \in [Txns -> ClientType]
    /\ read_locks \in [Keys -> SUBSET Txns]
    /\ write_lock \in [Keys -> Txns \cup {NoTx}]
    /\ committed \subseteq Txns
    /\ aborted \subseteq Txns
    /\ linearized \in Seq(Txns)
    /\ lin_count \in [Txns -> 0..2]
    /\ now \in 0..MaxTime
    /\ \A tx \in Txns :
          /\ (clients[tx].phase = "Idle") =
             (clients[tx].program = NoProgram)
          /\ clients[tx].read_done \subseteq ReadKeys(tx)
          /\ clients[tx].held \subseteq PhysicalLocks(tx)

S1_TerminalState ==
    /\ committed \cap aborted = {}
    /\ \A tx \in Txns :
          /\ (tx \in committed) = (tx_status[tx] = "Committed")
          /\ (tx \in aborted) = (tx_status[tx] = "Aborted")

S2_LockCompatibility ==
    /\ \A key \in Keys :
          write_lock[key] # NoTx => read_locks[key] \subseteq {write_lock[key]}
    /\ \A tx \in Txns, key \in Keys :
          key \in clients[tx].held =>
              (write_lock[key] = tx \/ tx \in read_locks[key])

S3_DurableReferences ==
    /\ \A key \in Keys :
          base_writer[key] # InitWriter =>
              /\ tx_status[base_writer[key]] = "Committed"
              /\ key \in WriteKeys(base_writer[key])
              /\ base_value[key] =
                 ProgramWrites[clients[base_writer[key]].program][key]
    /\ \A key \in Keys :
          /\ write_lock[key] # NoTx
          /\ tx_status[write_lock[key]] = "Committed"
          => /\ key \in WriteKeys(write_lock[key])
             /\ ProgramWrites[clients[write_lock[key]].program][key]
                \in Values

S4_Refinement ==
    \A key \in Keys : LogicalView(key) = logical_db[key]

S5_LoggedAtomicity ==
    /\ \A tx \in committed :
          /\ lin_count[tx] = 1
          /\ clients[tx].commit_post =
             ApplyWrites(clients[tx].commit_snapshot, tx)
    /\ \A tx \in Txns : lin_count[tx] <= 1

S9_PostLockValidation ==
    /\ \A tx \in committed :
          /\ clients[tx].validated
          /\ clients[tx].commit_had_locks
    /\ \A tx \in SetOfSequence(linearized) :
          \A key \in ReadKeys(tx) :
              /\ clients[tx].observed_writer[key] =
                 clients[tx].commit_writer_snapshot[key]
              /\ clients[tx].observed_value[key] =
                 clients[tx].commit_snapshot[key]

S10_CommittedCannotAbort ==
    \A tx \in committed :
        /\ tx_status[tx] = "Committed"
        /\ tx \notin aborted

S11_UncertaintyIsConservative ==
    \A tx \in Txns :
        /\ clients[tx].outcome = "DefiniteFailure" =>
              /\ lin_count[tx] = 0
              /\ clients[tx].mutation = "None"
        /\ clients[tx].outcome = "Success" => lin_count[tx] = 1
        /\ clients[tx].outcome = "ValidatedError" => lin_count[tx] = 1
        /\ clients[tx].outcome \in {"InDoubt", "Abandoned"} =>
              lin_count[tx] \in 0..1

S12_StrictSerializableHistory ==
    /\ NoDuplicates(linearized)
    /\ SetOfSequence(linearized) =
       {tx \in Txns : lin_count[tx] = 1}
    /\ Replay(linearized) = logical_db
    /\ \A tx \in SetOfSequence(linearized) :
          /\ clients[tx].commit_snapshot =
             Replay(PrefixBefore(linearized, tx))
          /\ \A key \in ReadKeys(tx) :
                clients[tx].observed_value[key] =
                    clients[tx].commit_snapshot[key]
          /\ clients[tx].completed_before \subseteq
             SetOfSequence(PrefixBefore(linearized, tx))

=============================================================================
