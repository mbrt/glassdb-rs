------------------------ MODULE MC_TxCoreLiveness ------------------------
EXTENDS MC_TxCore

CONSTANTS LeftTx, RightTx, WorkProgram

ASSUME
    /\ LeftTx \in Txns
    /\ RightTx \in Txns
    /\ LeftTx # RightTx
    /\ WorkProgram \in Programs
    /\ ProgramResult[WorkProgram] = "Commit"
    /\ \A key \in Keys : ProgramWrites[WorkProgram][key] # NoWrite

\* This wrapper adds only scheduling assumptions.  The backend may remain
\* unreliable before a transaction commits, but an enabled terminal cleanup
\* step is eventually scheduled.  Write-back is the only action that releases
\* a committed write lock, so its fairness checks both publication and release;
\* committed shared locks have their separate release action.
CommittedCleanupFairSpec ==
    /\ Init
    /\ [][Next]_vars
    /\ \A tx \in Txns, key \in Keys : WF_vars(WriteBack(tx, key))
    /\ \A tx \in Txns, key \in Keys :
           WF_vars(ReleaseCommittedRead(tx, key))

CommittedWriteBackConverges ==
    \A tx \in Txns, key \in Keys :
        [](/\ tx_status[tx] = "Committed"
           /\ key \in WriteKeys(tx)
           /\ write_lock[key] = tx
           => <> /\ write_lock[key] # tx
                 /\ base_writer[key] = tx)

CommittedReadReleaseConverges ==
    \A tx \in Txns, key \in Keys :
        [](/\ tx_status[tx] = "Committed"
           /\ tx \in read_locks[key]
           => <> (tx \notin read_locks[key]))

CommittedLocksEventuallyRelease ==
    \A tx \in Txns :
        [](tx_status[tx] = "Committed" => <> (PhysicalLocks(tx) = {}))

WaitsFor(requester, holder) ==
    \E leaf \in Leaves :
        /\ AllowedAcquisitionLeaf(requester, leaf)
        /\ holder \in ConflictingHolders(requester, leaf)

EqualPriorityCycle ==
    /\ Priority[LeftTx] = Priority[RightTx]
    /\ clients[LeftTx].phase = "Acquiring"
    /\ clients[RightTx].phase = "Acquiring"
    /\ clients[LeftTx].held # {}
    /\ clients[RightTx].held # {}
    /\ WaitsFor(LeftTx, RightTx)
    /\ WaitsFor(RightTx, LeftTx)

\* This is the eventually-healthy, bounded-contention suffix for the logged
\* path.  There are exactly two one-shot operations; cancellation, lost
\* acknowledgements, lease expiry, and new arrivals are disabled.  All enabled
\* protocol work is weakly fair.  The ordinary TxCore safety runs cover the
\* excluded failure transitions without fairness assumptions.
SortedFallbackNext ==
    \/ \E tx \in Txns : Invoke(tx, WorkProgram)
    \/ \E tx \in Txns, key \in Keys : BodyRead(tx, key)
    \/ \E tx \in Txns : FinishBody(tx)
    \/ \E tx \in Txns, leaf \in Leaves : AcquireLeaf(tx, leaf)
    \/ \E tx \in Txns : DeadlockTimeout(tx)
    \/ \E tx \in Txns : ValidateAfterLocking(tx)
    \/ \E tx \in Txns : RetryBodyAfterStaleRead(tx)
    \/ \E tx \in Txns : DispatchCommit(tx)
    \/ \E tx \in Txns : ApplyCommit(tx)
    \/ \E tx \in Txns : AcknowledgeCommit(tx)
    \/ \E tx \in Txns, key \in Keys : WriteBack(tx, key)
    \/ \E tx \in Txns, key \in Keys : ReleaseCommittedRead(tx, key)

EqualPriorityFairSpec ==
    /\ Init
    /\ [][SortedFallbackNext]_vars
    /\ \A tx \in Txns : WF_vars(Invoke(tx, WorkProgram))
    /\ \A tx \in Txns, key \in Keys : WF_vars(BodyRead(tx, key))
    /\ \A tx \in Txns : WF_vars(FinishBody(tx))
    /\ \A tx \in Txns, leaf \in Leaves : WF_vars(AcquireLeaf(tx, leaf))
    /\ \A tx \in Txns : WF_vars(DeadlockTimeout(tx))
    /\ \A tx \in Txns : WF_vars(ValidateAfterLocking(tx))
    /\ \A tx \in Txns : WF_vars(RetryBodyAfterStaleRead(tx))
    /\ \A tx \in Txns : WF_vars(DispatchCommit(tx))
    /\ \A tx \in Txns : WF_vars(ApplyCommit(tx))
    /\ \A tx \in Txns : WF_vars(AcknowledgeCommit(tx))
    /\ \A tx \in Txns, key \in Keys : WF_vars(WriteBack(tx, key))
    /\ \A tx \in Txns, key \in Keys :
           WF_vars(ReleaseCommittedRead(tx, key))

CycleEventuallyUsesSortedFallback ==
    [](EqualPriorityCycle =>
         <> (clients[LeftTx].serial_mode \/ clients[RightTx].serial_mode))

\* Weak fairness is sufficient to select sorted fallback, but not to finish it:
\* TLC finds a loop in which the serial contender repeatedly acquires the
\* lowest leaf and times out behind the parallel contender's higher-leaf hold.
\* The parallel acquire/timeout is enabled infinitely often but not
\* continuously.  The completion configuration therefore states the stronger
\* scheduler/backend-admission contract explicitly for those two intermittent
\* actions; all other protocol work retains weak fairness.
EqualPriorityCompletionFairSpec ==
    /\ EqualPriorityFairSpec
    /\ \A tx \in Txns, leaf \in Leaves : SF_vars(AcquireLeaf(tx, leaf))
    /\ \A tx \in Txns : SF_vars(DeadlockTimeout(tx))

AtLeastOneContenderEventuallyCompletes ==
    <> \E tx \in {LeftTx, RightTx} : clients[tx].outcome = "Success"

=============================================================================
