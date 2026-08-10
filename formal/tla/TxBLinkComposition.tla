---------------------- MODULE TxBLinkComposition ----------------------
EXTENDS FiniteSets, TLC

CONSTANTS
    Keys,
    LeftKeys,
    RightKeys,
    TargetKey,
    Source,
    Sibling,
    NoNode,
    NoRef,
    TxRef,
    InitialValue,
    TxValue

Nodes == {Source, Sibling}
Refs == {NoRef, TxRef}
Values == {InitialValue, TxValue}
SplitPhases == {"Stable", "Copied", "Linked", "Parented"}
TxPhases == {"Idle", "Acquiring", "Locked", "Committed", "Aborted", "Done"}
TxStatuses == {"Absent", "Pending", "Committed", "Aborted"}

ASSUME
    /\ Keys = LeftKeys \cup RightKeys
    /\ LeftKeys # {}
    /\ RightKeys # {}
    /\ LeftKeys \cap RightKeys = {}
    /\ TargetKey \in RightKeys
    /\ Source # Sibling
    /\ NoNode \notin Nodes
    /\ NoRef # TxRef
    /\ InitialValue # TxValue

\* This is a reduced composition of the TxCore leaf-rewrite boundary and a
\* non-root B-link split.  entry_value and entry_ref are shared protocol state:
\* they are neither duplicated projections nor related only by an assumption.
\*
\* A split owns the structural gate from its copy through the atomic source
\* shrink/right-link rewrite.  TxCore may still decide a transaction by changing
\* its object while that gate is held, but leaf lock installation, cleanup, and
\* write-back wait until the gate is released.

VARIABLES
    split_phase,
    split_gate,
    node_keys,
    entry_value,
    entry_ref,
    source_right,
    parent_has_separator,
    tx_phase,
    tx_status,
    logical_db

vars ==
    <<split_phase,
      split_gate,
      node_keys,
      entry_value,
      entry_ref,
      source_right,
      parent_has_separator,
      tx_phase,
      tx_status,
      logical_db>>

ReachableNodes ==
    {Source}
    \cup (IF source_right = Sibling \/ parent_has_separator
          THEN {Sibling}
          ELSE {})

Owners(key) ==
    {node \in ReachableNodes : key \in node_keys[node]}

RoutedLeaf(key) ==
    IF /\ key \in RightKeys
       /\ parent_has_separator
    THEN Sibling
    ELSE IF /\ key \in RightKeys
            /\ key \notin node_keys[Source]
         THEN IF source_right = Sibling THEN Sibling ELSE Source
         ELSE Source

EntryLogicalValue(node, key) ==
    IF /\ entry_ref[node][key] = TxRef
       /\ tx_status = "Committed"
    THEN TxValue
    ELSE entry_value[node][key]

LogicalView(key) ==
    EntryLogicalValue(RoutedLeaf(key), key)

Init ==
    /\ split_phase = "Stable"
    /\ split_gate = FALSE
    /\ node_keys = [node \in Nodes |-> IF node = Source THEN Keys ELSE {}]
    /\ entry_value =
        [node \in Nodes |-> [key \in Keys |-> InitialValue]]
    /\ entry_ref = [node \in Nodes |-> [key \in Keys |-> NoRef]]
    /\ source_right = NoNode
    /\ parent_has_separator = FALSE
    /\ tx_phase = "Idle"
    /\ tx_status = "Absent"
    /\ logical_db = [key \in Keys |-> InitialValue]

BeginTx ==
    /\ tx_phase = "Idle"
    /\ tx_phase' = "Acquiring"
    /\ tx_status' = "Pending"
    /\ UNCHANGED <<split_phase, split_gate, node_keys, entry_value,
                   entry_ref, source_right, parent_has_separator, logical_db>>

AcquireTarget ==
    LET leaf == RoutedLeaf(TargetKey)
    IN /\ tx_phase = "Acquiring"
       /\ ~split_gate
       /\ leaf \in Owners(TargetKey)
       /\ entry_ref[leaf][TargetKey] = NoRef
       /\ entry_ref' =
           [entry_ref EXCEPT ![leaf][TargetKey] = TxRef]
       /\ tx_phase' = "Locked"
       /\ UNCHANGED <<split_phase, split_gate, node_keys, entry_value,
                      source_right, parent_has_separator, tx_status,
                      logical_db>>

CommitTx ==
    /\ tx_phase = "Locked"
    /\ tx_status = "Pending"
    /\ tx_phase' = "Committed"
    /\ tx_status' = "Committed"
    /\ logical_db' = [logical_db EXCEPT ![TargetKey] = TxValue]
    /\ UNCHANGED <<split_phase, split_gate, node_keys, entry_value,
                   entry_ref, source_right, parent_has_separator>>

AbortTx ==
    /\ tx_phase = "Locked"
    /\ tx_status = "Pending"
    /\ tx_phase' = "Aborted"
    /\ tx_status' = "Aborted"
    /\ UNCHANGED <<split_phase, split_gate, node_keys, entry_value,
                   entry_ref, source_right, parent_has_separator, logical_db>>

WriteBack ==
    LET leaf == RoutedLeaf(TargetKey)
    IN /\ tx_phase = "Committed"
       /\ tx_status = "Committed"
       /\ ~split_gate
       /\ leaf \in Owners(TargetKey)
       /\ entry_ref[leaf][TargetKey] = TxRef
       /\ entry_value' =
           [entry_value EXCEPT ![leaf][TargetKey] = TxValue]
       /\ entry_ref' =
           [entry_ref EXCEPT ![leaf][TargetKey] = NoRef]
       /\ tx_phase' = "Done"
       /\ UNCHANGED <<split_phase, split_gate, node_keys, source_right,
                      parent_has_separator, tx_status, logical_db>>

ReleaseAborted ==
    LET leaf == RoutedLeaf(TargetKey)
    IN /\ tx_phase = "Aborted"
       /\ tx_status = "Aborted"
       /\ ~split_gate
       /\ leaf \in Owners(TargetKey)
       /\ entry_ref[leaf][TargetKey] = TxRef
       /\ entry_ref' =
           [entry_ref EXCEPT ![leaf][TargetKey] = NoRef]
       /\ tx_phase' = "Done"
       /\ UNCHANGED <<split_phase, split_gate, node_keys, entry_value,
                      source_right, parent_has_separator, tx_status,
                      logical_db>>

StartSplit ==
    /\ split_phase = "Stable"
    /\ ~split_gate
    /\ split_phase' = "Copied"
    /\ split_gate' = TRUE
    /\ node_keys' = [node_keys EXCEPT ![Sibling] = RightKeys]
    /\ entry_value' =
        [entry_value EXCEPT
            ![Sibling] =
                [key \in Keys |->
                    IF key \in RightKeys
                    THEN entry_value[Source][key]
                    ELSE entry_value[Sibling][key]]]
    /\ entry_ref' =
        [entry_ref EXCEPT
            ![Sibling] =
                [key \in Keys |->
                    IF key \in RightKeys
                    THEN entry_ref[Source][key]
                    ELSE entry_ref[Sibling][key]]]
    /\ UNCHANGED <<source_right, parent_has_separator, tx_phase, tx_status,
                   logical_db>>

\* Source shrink and right-link publication are the same leaf rewrite.  The
\* sibling becomes authoritative only after the copied image is reachable.
PublishShrink ==
    /\ split_phase = "Copied"
    /\ split_gate
    /\ split_phase' = "Linked"
    /\ split_gate' = FALSE
    /\ node_keys' = [node_keys EXCEPT ![Source] = LeftKeys]
    /\ source_right' = Sibling
    /\ UNCHANGED <<entry_value, entry_ref, parent_has_separator, tx_phase,
                   tx_status, logical_db>>

PublishParent ==
    /\ split_phase = "Linked"
    /\ split_phase' = "Parented"
    /\ parent_has_separator' = TRUE
    /\ UNCHANGED <<split_gate, node_keys, entry_value, entry_ref,
                   source_right, tx_phase, tx_status, logical_db>>

Next ==
    \/ BeginTx
    \/ AcquireTarget
    \/ CommitTx
    \/ AbortTx
    \/ WriteBack
    \/ ReleaseAborted
    \/ StartSplit
    \/ PublishShrink
    \/ PublishParent

Spec == Init /\ [][Next]_vars

TCBS0_TypeOK ==
    /\ split_phase \in SplitPhases
    /\ split_gate \in BOOLEAN
    /\ node_keys \in [Nodes -> SUBSET Keys]
    /\ entry_value \in [Nodes -> [Keys -> Values]]
    /\ entry_ref \in [Nodes -> [Keys -> Refs]]
    /\ source_right \in {NoNode, Sibling}
    /\ parent_has_separator \in BOOLEAN
    /\ tx_phase \in TxPhases
    /\ tx_status \in TxStatuses
    /\ logical_db \in [Keys -> Values]

TCBS1_OneAuthoritativeOwner ==
    \A key \in Keys : Cardinality(Owners(key)) = 1

TCBS2_RoutingReachesOwner ==
    \A key \in Keys : RoutedLeaf(key) \in Owners(key)

TCBS3_LogicalViewRefinesDatabase ==
    [key \in Keys |-> LogicalView(key)] = logical_db

\* This is the joint boundary property: every not-yet-written-back TxCore
\* transaction reference remains in the entry selected by B-link traversal.
TCBS4_LiveTxReferenceReachable ==
    tx_phase \in {"Locked", "Committed", "Aborted"} =>
        entry_ref[RoutedLeaf(TargetKey)][TargetKey] = TxRef

CompositionGuarantee ==
    /\ TCBS1_OneAuthoritativeOwner
    /\ TCBS2_RoutingReachesOwner
    /\ TCBS3_LogicalViewRefinesDatabase
    /\ TCBS4_LiveTxReferenceReachable

\* Negative control: TxCore ignores the split gate and installs its lock in
\* Source after Sibling was copied.  Source still owns the key at this point,
\* so the mismatch becomes observable only when PublishShrink exposes the
\* stale sibling image and loses the live transaction reference.
AcquireWhileSplitGateHeld ==
    /\ split_phase = "Copied"
    /\ split_gate
    /\ tx_phase = "Acquiring"
    /\ entry_ref[Source][TargetKey] = NoRef
    /\ entry_ref' =
        [entry_ref EXCEPT ![Source][TargetKey] = TxRef]
    /\ tx_phase' = "Locked"
    /\ UNCHANGED <<split_phase, split_gate, node_keys, entry_value,
                   source_right, parent_has_separator, tx_status, logical_db>>

MutantSpec ==
    Init /\ [][Next \/ AcquireWhileSplitGateHeld]_vars

=============================================================================
