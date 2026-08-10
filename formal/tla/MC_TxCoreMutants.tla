------------------------- MODULE MC_TxCoreMutants --------------------------
EXTENDS MC_TxCore

PublishBeforeCommit(tx, key) ==
    /\ clients[tx].phase \in {"Ready", "Committing"}
    /\ tx_status[tx] \in {"Absent", "Pending"}
    /\ key \in WriteKeys(tx)
    /\ write_lock[key] = tx
    /\ base_value' =
        [base_value EXCEPT
            ![key] = ProgramWrites[clients[tx].program][key]]
    /\ base_writer' = [base_writer EXCEPT ![key] = tx]
    /\ UNCHANGED <<logical_db, last_writer, tx_status, tx_lease, clients,
                   read_locks, write_lock, committed, aborted,
                   linearized, lin_count, now>>

CommitWithoutPostLockValidation(tx) ==
    /\ clients[tx].phase = "Validating"
    /\ HasAllLocks(tx)
    /\ ~ObservationsCurrent(tx)
    /\ tx_status[tx] \in {"Absent", "Pending"}
    /\ tx_status' = [tx_status EXCEPT ![tx] = "Committed"]
    /\ committed' = committed \cup {tx}
    /\ logical_db' = ApplyWrites(logical_db, tx)
    /\ last_writer' =
        [key \in Keys |->
            IF key \in WriteKeys(tx) THEN tx ELSE last_writer[key]]
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Done",
            ![tx].outcome = "Success",
            ![tx].validated = TRUE,
            ![tx].commit_snapshot = logical_db,
            ![tx].commit_writer_snapshot = last_writer,
            ![tx].commit_post = ApplyWrites(logical_db, tx),
            ![tx].commit_had_locks = TRUE]
    /\ linearized' = Append(linearized, tx)
    /\ lin_count' = [lin_count EXCEPT ![tx] = @ + 1]
    /\ UNCHANGED <<base_value, base_writer, tx_lease,
                   read_locks, write_lock, aborted, now>>

ExpireCommitted(tx) ==
    /\ tx \in committed
    /\ tx_status[tx] = "Committed"
    /\ clients[tx].expected_status = "Pending"
    /\ now > clients[tx].expected_lease
    /\ tx_status' = [tx_status EXCEPT ![tx] = "Aborted"]
    /\ aborted' = aborted \cup {tx}
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_lease, clients, read_locks, write_lock, committed,
                   linearized, lin_count, now>>

MisclassifyUncertain(tx) ==
    /\ clients[tx].phase = "Committing"
    /\ clients[tx].mutation = "Dispatched"
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Done",
            ![tx].outcome = "DefiniteFailure",
            ![tx].mutation = "Unresolved"]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock, committed,
                   aborted, linearized, lin_count, now>>

\* A failed optimistic read-only/error validation promises that its retry will
\* validate under shared locks. Returning the retry directly would expose a
\* read-derived result without the lock-carrying validation barrier.
ReturnEscalatedErrorWithoutLocks(tx) ==
    /\ clients[tx].phase = "Reading"
    /\ clients[tx].lock_reads
    /\ clients[tx].read_done = ReadKeys(tx)
    /\ ProgramResult[clients[tx].program] = "ValidatedError"
    /\ ~HasAllLocks(tx)
    /\ clients' =
        [clients EXCEPT
            ![tx].phase = "Done",
            ![tx].outcome = "ValidatedError",
            ![tx].validated = TRUE,
            ![tx].commit_snapshot = logical_db,
            ![tx].commit_writer_snapshot = last_writer,
            ![tx].commit_post = logical_db]
    /\ linearized' = Append(linearized, tx)
    /\ lin_count' = [lin_count EXCEPT ![tx] = @ + 1]
    /\ UNCHANGED <<logical_db, base_value, base_writer, last_writer,
                   tx_status, tx_lease, read_locks, write_lock, committed,
                   aborted, now>>

EarlyPublicationSpec ==
    Init /\ [][Next \/ \E tx \in Txns, key \in Keys :
                         PublishBeforeCommit(tx, key)]_vars

MissingValidationSpec ==
    Init /\ [][Next \/ \E tx \in Txns :
                         CommitWithoutPostLockValidation(tx)]_vars

ExpiryAfterCommitSpec ==
    Init /\ [][Next \/ \E tx \in Txns : ExpireCommitted(tx)]_vars

UncertaintyMisclassificationSpec ==
    Init /\ [][Next \/ \E tx \in Txns : MisclassifyUncertain(tx)]_vars

UnlockedRetryErrorSpec ==
    Init /\ [][Next \/ \E tx \in Txns :
                         ReturnEscalatedErrorWithoutLocks(tx)]_vars

=============================================================================
