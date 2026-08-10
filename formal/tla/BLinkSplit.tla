--------------------------- MODULE BLinkSplit ----------------------------
EXTENDS FiniteSets, Naturals, TLC

CONSTANTS
    Keys,
    LeftKeys,
    RightKeys,
    Source,
    Sibling,
    NoNode,
    LowRef,
    HighRef

Nodes == {Source, Sibling}
Phases == {"Stable", "Copied", "Linked", "Parented"}

ASSUME
    /\ Keys = LeftKeys \cup RightKeys
    /\ LeftKeys # {}
    /\ RightKeys # {}
    /\ LeftKeys \cap RightKeys = {}
    /\ Source # Sibling
    /\ NoNode \notin Nodes
    /\ LowRef # HighRef

\* TxCore boundary
\* ----------------
\* FROM TxCore: leaf entries and their transaction references are immutable
\* while the structural gate is held.
\* TO TxCore: the split keeps every entry/reference reachable and gives TxCore
\* one authoritative routed leaf before and after the shrink CAS.
\*
\* This is the reduced non-root split interface.  Root height growth, gate
\* acquisition, parent retry, and merge are outside this finite composition;
\* none changes the shrink/right-link publication order.
EntryRef(key) == IF key \in LeftKeys THEN LowRef ELSE HighRef
EntriesFor(key_set) == {<<key, EntryRef(key)>> : key \in key_set}

VARIABLES
    phase,
    source_entries,
    sibling_entries,
    sibling_exists,
    source_covers_right,
    source_right,
    parent_has_separator

vars ==
    <<phase,
      source_entries,
      sibling_entries,
      sibling_exists,
      source_covers_right,
      source_right,
      parent_has_separator>>

NodeEntries(node) ==
    IF node = Source THEN source_entries ELSE sibling_entries

NodeKeys(node) == {entry[1] : entry \in NodeEntries(node)}

ReachableNodes ==
    {Source}
    \cup (IF source_right = Sibling \/ parent_has_separator
          THEN {Sibling}
          ELSE {})

Owners(key) == {node \in ReachableNodes : key \in NodeKeys(node)}

\* A stale parent first selects Source.  Once the parent separator lands it can
\* select Sibling directly; before then Source's high-key miss follows right.
RoutedLeaf(key) ==
    IF /\ key \in RightKeys
       /\ parent_has_separator
    THEN Sibling
    ELSE IF /\ key \in RightKeys
            /\ ~source_covers_right
         THEN IF source_right = Sibling THEN Sibling ELSE Source
         ELSE Source

Init ==
    /\ phase = "Stable"
    /\ source_entries = EntriesFor(Keys)
    /\ sibling_entries = {}
    /\ sibling_exists = FALSE
    /\ source_covers_right = TRUE
    /\ source_right = NoNode
    /\ parent_has_separator = FALSE

PrepareSibling ==
    /\ phase = "Stable"
    /\ phase' = "Copied"
    /\ sibling_entries' = EntriesFor(RightKeys)
    /\ sibling_exists' = TRUE
    /\ UNCHANGED <<source_entries, source_covers_right, source_right,
                   parent_has_separator>>

\* The source shrink and right-link installation are one conditional rewrite.
PublishShrink ==
    /\ phase = "Copied"
    /\ sibling_exists
    /\ phase' = "Linked"
    /\ source_entries' = EntriesFor(LeftKeys)
    /\ source_covers_right' = FALSE
    /\ source_right' = Sibling
    /\ UNCHANGED <<sibling_entries, sibling_exists, parent_has_separator>>

PublishParent ==
    /\ phase = "Linked"
    /\ phase' = "Parented"
    /\ parent_has_separator' = TRUE
    /\ UNCHANGED <<source_entries, sibling_entries, sibling_exists,
                   source_covers_right, source_right>>

Next == PrepareSibling \/ PublishShrink \/ PublishParent

Spec == Init /\ [][Next]_vars

BS0_TypeOK ==
    /\ phase \in Phases
    /\ source_entries \subseteq EntriesFor(Keys)
    /\ sibling_entries \subseteq EntriesFor(Keys)
    /\ sibling_exists \in BOOLEAN
    /\ source_covers_right \in BOOLEAN
    /\ source_right \in {NoNode, Sibling}
    /\ parent_has_separator \in BOOLEAN

BS1_OneAuthoritativeOwner ==
    \A key \in Keys : Cardinality(Owners(key)) = 1

BS2_RoutingReachesOwner ==
    \A key \in Keys : RoutedLeaf(key) \in Owners(key)

BS3_EntriesAndReferencesPreserved ==
    \A key \in Keys :
        \E node \in ReachableNodes :
            <<key, EntryRef(key)>> \in NodeEntries(node)

BS4_PreparedSiblingInvisible ==
    phase = "Copied" =>
        /\ Sibling \notin ReachableNodes
        /\ source_entries = EntriesFor(Keys)

BS5_HighKeyMatchesSourceRange ==
    source_covers_right <=> RightKeys \subseteq NodeKeys(Source)

\* The named composition boundary consumed by TxCore.
TxCoreSplitGuarantee ==
    /\ BS1_OneAuthoritativeOwner
    /\ BS2_RoutingReachesOwner
    /\ BS3_EntriesAndReferencesPreserved
    /\ BS4_PreparedSiblingInvisible
    /\ BS5_HighKeyMatchesSourceRange

\* Negative control: moving the entries without atomically installing the
\* right-link creates a window in which a stale parent cannot find them.
ShrinkWithoutRightLink ==
    /\ phase = "Copied"
    /\ sibling_exists
    /\ phase' = "Linked"
    /\ source_entries' = EntriesFor(LeftKeys)
    /\ source_covers_right' = FALSE
    /\ UNCHANGED <<sibling_entries, sibling_exists, source_right,
                   parent_has_separator>>

MutantSpec ==
    Init /\ [][Next \/ ShrinkWithoutRightLink]_vars

=============================================================================
