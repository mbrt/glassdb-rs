--------------------------- MODULE MC_DirectCore ---------------------------
EXTENDS DirectCore

CONSTANTS
    T1,
    T2,
    T3,
    K1,
    K2,
    V0,
    V1,
    V2

ASSUME
    /\ T1 # T2
    /\ T1 # T3
    /\ T2 # T3
    /\ K1 # K2
    /\ V0 # V1
    /\ V0 # V2
    /\ V1 # V2

TwoSameKeyMap == [tx \in Txns |-> K1]

ThreeKeyMap ==
    [tx \in Txns |-> IF tx = T3 THEN K2 ELSE K1]

TwoRmwKinds == [tx \in Txns |-> Rmw]

TwoBlindKinds == [tx \in Txns |-> Blind]

ThreeMixedKinds ==
    [tx \in Txns |-> IF tx = T3 THEN Rmw ELSE Blind]

DistinctPriorities ==
    [tx \in Txns |->
        IF tx = T1 THEN 0 ELSE IF tx = T2 THEN 1 ELSE 2]

EqualPriorities == [tx \in Txns |-> 0]

StableTieRanks ==
    [tx \in Txns |->
        IF tx = T1 THEN 0 ELSE IF tx = T2 THEN 1 ELSE 2]

PilotBlindValues ==
    [tx \in Txns |-> IF tx = T2 THEN V2 ELSE V1]

PilotRmwNext ==
    [value \in Values |->
        IF value = V0 THEN V1 ELSE IF value = V1 THEN V2 ELSE V0]

AllDirectAllowed == [tx \in Txns |-> TRUE]

PilotInitialDb == [key \in Keys |-> V0]

=============================================================================
