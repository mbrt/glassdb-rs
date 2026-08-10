----------------------- MODULE CollectionDropFence -----------------------
EXTENDS TLC

CONSTANTS Nodes, RootNode

Phases == {"Open", "Frozen", "Fencing", "Dropped"}

ASSUME
    /\ Nodes # {}
    /\ RootNode \in Nodes

\* This focused model owns the collection-drop concurrency seam.  A topology
\* operation admitted before the freeze may still publish a node, so fencing
\* cannot enumerate topology-published nodes until every such participant has
\* settled. Provider objects from abandoned creates that land later without a
\* topology publication are unreachable orphans outside this model; their
\* cleanup remains an implementation contract.
\* Data participants need not stop at the global freeze: each node's fence is
\* the admission barrier, and the fence waits for already-admitted data work.
\*
\* Incarnation allocation, directory rebinding, and stale handles are checked
\* by production-level tests.  Split contents and routing are owned by
\* TxBLinkComposition; only the topology publication of a new node matters here.

VARIABLES
    phase,
    published_nodes,
    live_nodes,
    topology_participants,
    enumerated_nodes,
    data_participants,
    fenced_nodes

vars ==
    <<phase,
      published_nodes,
      live_nodes,
      topology_participants,
      enumerated_nodes,
      data_participants,
      fenced_nodes>>

Init ==
    /\ phase = "Open"
    /\ published_nodes = {RootNode}
    /\ live_nodes = {RootNode}
    /\ topology_participants = {}
    /\ enumerated_nodes = {}
    /\ data_participants = {}
    /\ fenced_nodes = {}

BeginTopology(node) ==
    /\ phase = "Open"
    /\ node \notin published_nodes \cup topology_participants
    /\ topology_participants' = topology_participants \cup {node}
    /\ UNCHANGED <<phase, published_nodes, live_nodes, enumerated_nodes,
                   data_participants, fenced_nodes>>

\* Publication represents the point at which a pre-freeze operation can make
\* another topology node visible to drop's eventual enumeration.
PublishTopology(node) ==
    /\ node \in topology_participants
    /\ topology_participants' = topology_participants \ {node}
    /\ published_nodes' = published_nodes \cup {node}
    /\ live_nodes' = live_nodes \cup {node}
    /\ UNCHANGED <<phase, enumerated_nodes, data_participants, fenced_nodes>>

AbandonTopology(node) ==
    /\ node \in topology_participants
    /\ topology_participants' = topology_participants \ {node}
    /\ UNCHANGED <<phase, published_nodes, live_nodes, enumerated_nodes,
                   data_participants, fenced_nodes>>

BeginFreeze ==
    /\ phase = "Open"
    /\ phase' = "Frozen"
    /\ UNCHANGED <<published_nodes, live_nodes, topology_participants,
                   enumerated_nodes, data_participants, fenced_nodes>>

BeginFencing ==
    /\ phase = "Frozen"
    /\ topology_participants = {}
    /\ phase' = "Fencing"
    /\ enumerated_nodes' = published_nodes
    /\ UNCHANGED <<published_nodes, live_nodes, topology_participants,
                   data_participants, fenced_nodes>>

AdmitData(node) ==
    /\ phase \in {"Open", "Frozen", "Fencing"}
    /\ node \in live_nodes
    /\ node \notin fenced_nodes
    /\ node \notin data_participants
    /\ data_participants' = data_participants \cup {node}
    /\ UNCHANGED <<phase, published_nodes, live_nodes,
                   topology_participants, enumerated_nodes, fenced_nodes>>

CommitData(node) ==
    /\ node \in data_participants
    /\ node \notin fenced_nodes
    /\ data_participants' = data_participants \ {node}
    /\ UNCHANGED <<phase, published_nodes, live_nodes,
                   topology_participants, enumerated_nodes, fenced_nodes>>

AbortData(node) ==
    /\ node \in data_participants
    /\ data_participants' = data_participants \ {node}
    /\ UNCHANGED <<phase, published_nodes, live_nodes,
                   topology_participants, enumerated_nodes, fenced_nodes>>

FenceNode(node) ==
    /\ phase = "Fencing"
    /\ node \in enumerated_nodes \ fenced_nodes
    /\ node \notin data_participants
    /\ fenced_nodes' = fenced_nodes \cup {node}
    /\ UNCHANGED <<phase, published_nodes, live_nodes,
                   topology_participants, enumerated_nodes,
                   data_participants>>

CommitDrop ==
    /\ phase = "Fencing"
    /\ topology_participants = {}
    /\ data_participants = {}
    /\ published_nodes \subseteq enumerated_nodes
    /\ enumerated_nodes \subseteq fenced_nodes
    /\ phase' = "Dropped"
    /\ live_nodes' = {}
    /\ UNCHANGED <<published_nodes, topology_participants,
                   enumerated_nodes, data_participants, fenced_nodes>>

Next ==
    \/ \E node \in Nodes : BeginTopology(node)
    \/ \E node \in Nodes : PublishTopology(node)
    \/ \E node \in Nodes : AbandonTopology(node)
    \/ BeginFreeze
    \/ BeginFencing
    \/ \E node \in Nodes : AdmitData(node)
    \/ \E node \in Nodes : CommitData(node)
    \/ \E node \in Nodes : AbortData(node)
    \/ \E node \in Nodes : FenceNode(node)
    \/ CommitDrop

Spec == Init /\ [][Next]_vars

CDF0_TypeOK ==
    /\ phase \in Phases
    /\ published_nodes \subseteq Nodes
    /\ live_nodes \subseteq Nodes
    /\ topology_participants \subseteq Nodes
    /\ enumerated_nodes \subseteq Nodes
    /\ data_participants \subseteq Nodes
    /\ fenced_nodes \subseteq Nodes

CDF1_EnumerationCoversPublishedNodes ==
    phase \in {"Fencing", "Dropped"} =>
        published_nodes \subseteq enumerated_nodes

CDF2_DataAdmissionRespectsFences ==
    /\ data_participants \subseteq live_nodes
    /\ data_participants \cap fenced_nodes = {}

CDF3_DroppedCollectionIsQuiescent ==
    phase = "Dropped" =>
        /\ live_nodes = {}
        /\ topology_participants = {}
        /\ data_participants = {}
        /\ published_nodes \subseteq fenced_nodes

CollectionDropGuarantee ==
    /\ CDF1_EnumerationCoversPublishedNodes
    /\ CDF2_DataAdmissionRespectsFences
    /\ CDF3_DroppedCollectionIsQuiescent

\* Negative control: fencing snapshots the current node set while a topology
\* participant is still live.  Its later publication creates an extant node
\* outside the snapshot and violates CDF1.
StartFencingBeforeTopologyDrained ==
    /\ phase = "Frozen"
    /\ topology_participants # {}
    /\ phase' = "Fencing"
    /\ enumerated_nodes' = published_nodes
    /\ UNCHANGED <<published_nodes, live_nodes, topology_participants,
                   data_participants, fenced_nodes>>

MutantSpec ==
    Init /\ [][Next \/ StartFencingBeforeTopologyDrained]_vars

=============================================================================
