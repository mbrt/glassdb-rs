---------------------------- MODULE MC_TxCore -----------------------------
EXTENDS TxCore

CONSTANTS
    T1,
    T2,
    K1,
    K2,
    L1,
    L2,
    PWrite1,
    PWrite2,
    PReadWrite,
    PReadOnly,
    PValidatedError,
    PAbort,
    VAbsent,
    V1,
    V2,
    VTombstone

ASSUME
    /\ T1 # T2
    /\ K1 # K2
    /\ L1 # L2
    /\ PWrite1 # PWrite2
    /\ PWrite1 # PReadOnly
    /\ PWrite1 # PValidatedError
    /\ PWrite1 # PAbort
    /\ PWrite2 # PReadOnly
    /\ PWrite2 # PValidatedError
    /\ PWrite2 # PAbort
    /\ PReadWrite \notin
       {PWrite1, PWrite2, PReadOnly, PValidatedError, PAbort}
    /\ PReadOnly # PValidatedError
    /\ PReadOnly # PAbort
    /\ PValidatedError # PAbort

SameLeafMap ==
    [key \in Keys |-> L1]

CrossLeafMap ==
    [key \in Keys |-> IF key = K1 THEN L1 ELSE L2]

LeafRankMap ==
    [leaf \in Leaves |-> IF leaf = L1 THEN 0 ELSE 1]

DistinctPriorityMap ==
    [tx \in Txns |-> IF tx = T1 THEN 0 ELSE 1]

EqualPriorityMap ==
    [tx \in Txns |-> 0]

PilotReads ==
    [selected_program \in Programs |->
        IF selected_program = PAbort THEN {} ELSE Keys]

PilotWrites ==
    [selected_program \in Programs |->
        [key \in Keys |->
            IF selected_program = PWrite1
            THEN V1
            ELSE IF selected_program = PWrite2
                 THEN IF key = K2 THEN VTombstone ELSE V2
                 ELSE IF selected_program = PReadWrite
                      THEN IF key = K2 THEN V2 ELSE NoWrite
                      ELSE IF selected_program = PValidatedError
                           THEN V1
                           ELSE NoWrite]]

PilotResults ==
    [selected_program \in Programs |->
        IF selected_program = PValidatedError
        THEN "ValidatedError"
        ELSE IF selected_program = PAbort
             THEN "Abort"
             ELSE "Commit"]

PilotInitialDb ==
    [key \in Keys |-> VAbsent]

SameValueInitialDb ==
    [key \in Keys |-> V1]

=============================================================================
