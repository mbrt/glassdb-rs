----------------------- MODULE CollectionLifecycle -----------------------
EXTENDS FiniteSets, Naturals, TLC

CONSTANTS CollectionIds, Nodes, RootNode, NoCollection

ASSUME
    /\ CollectionIds # {}
    /\ Nodes # {}
    /\ RootNode \in Nodes
    /\ NoCollection \notin CollectionIds

NodeParticipants == CollectionIds \X Nodes

\* TxCore boundary
\* ----------------
\* FROM TxCore: data_participants is the complete set of admitted node
\* rewrites that can still commit; removing one through CommitData or
\* AbortData is definitive.  TxCore also preserves terminal outcomes while a
\* structural gate reconciles the participant.
\* TO TxCore: admission checks the exact incarnation binding and node fence;
\* parent removal occurs only after topology has settled and every extant node
\* and data participant has been fenced.  Fresh collection IDs are never
\* reused, so a retained handle cannot alias a replacement.
\*
\* The model has one parent/name slot and a finite node set.  An abandoned
\* fresh-node create that arrives after topology settlement is reduced away:
\* ADR-043 permits that unreachable orphan, but the frozen topology cannot
\* publish it and no handle can route to it.

VARIABLES
    parent_binding,
    prepared,
    handles,
    objects,
    reachable_nodes,
    topology_participants,
    data_participants,
    fenced_nodes,
    frozen,
    drop_target,
    dropped,
    committed_data,
    stale_commit

vars ==
    <<parent_binding,
      prepared,
      handles,
      objects,
      reachable_nodes,
      topology_participants,
      data_participants,
      fenced_nodes,
      frozen,
      drop_target,
      dropped,
      committed_data,
      stale_commit>>

LogicalCollections ==
    IF parent_binding = NoCollection THEN {} ELSE {parent_binding}

\* reachable_nodes is reachability inside a prepared private tree.  Catalog
\* reachability is LogicalCollections alone, so preparing RootNode does not
\* make the collection discoverable before PublishCollection.

ParticipantsFor(participants, collection) ==
    {participant \in participants : participant[1] = collection}

Init ==
    /\ parent_binding = NoCollection
    /\ prepared = {}
    /\ handles = {}
    /\ objects = [collection \in CollectionIds |-> {}]
    /\ reachable_nodes = [collection \in CollectionIds |-> {}]
    /\ topology_participants = {}
    /\ data_participants = {}
    /\ fenced_nodes = [collection \in CollectionIds |-> {}]
    /\ frozen = {}
    /\ drop_target = NoCollection
    /\ dropped = {}
    /\ committed_data = {}
    /\ stale_commit = FALSE

PrepareCollection(collection) ==
    /\ collection \notin prepared
    /\ collection \notin dropped
    /\ prepared' = prepared \cup {collection}
    /\ objects' =
        [objects EXCEPT ![collection] = {RootNode}]
    /\ reachable_nodes' =
        [reachable_nodes EXCEPT ![collection] = {RootNode}]
    /\ UNCHANGED <<parent_binding, handles, topology_participants,
                   data_participants, fenced_nodes, frozen, drop_target,
                   dropped, committed_data, stale_commit>>

\* The directory-entry commit is the sole logical-existence publication.
PublishCollection(collection) ==
    /\ parent_binding = NoCollection
    /\ collection \in prepared
    /\ collection \notin dropped
    /\ parent_binding' = collection
    /\ UNCHANGED <<prepared, handles, objects, reachable_nodes,
                   topology_participants, data_participants, fenced_nodes,
                   frozen, drop_target, dropped, committed_data, stale_commit>>

OpenHandle(collection) ==
    /\ parent_binding = collection
    /\ handles' = handles \cup {collection}
    /\ UNCHANGED <<parent_binding, prepared, objects, reachable_nodes,
                   topology_participants, data_participants, fenced_nodes,
                   frozen, drop_target, dropped, committed_data, stale_commit>>

BeginData(collection, node) ==
    /\ collection \in handles
    /\ parent_binding = collection
    /\ node \in reachable_nodes[collection]
    /\ node \notin fenced_nodes[collection]
    /\ <<collection, node>> \notin data_participants
    /\ data_participants' =
        data_participants \cup {<<collection, node>>}
    /\ UNCHANGED <<parent_binding, prepared, handles, objects,
                   reachable_nodes, topology_participants, fenced_nodes,
                   frozen, drop_target, dropped, committed_data, stale_commit>>

\* A participant may serialize before the drop reaches its node.  Once the
\* node is fenced, neither this action nor a fresh admission is possible.
CommitData(collection, node) ==
    /\ <<collection, node>> \in data_participants
    /\ collection \notin dropped
    /\ node \notin fenced_nodes[collection]
    /\ data_participants' =
        data_participants \ {<<collection, node>>}
    /\ committed_data' =
        committed_data \cup {<<collection, node>>}
    /\ UNCHANGED <<parent_binding, prepared, handles, objects,
                   reachable_nodes, topology_participants, fenced_nodes,
                   frozen, drop_target, dropped, stale_commit>>

AbortData(collection, node) ==
    /\ <<collection, node>> \in data_participants
    /\ data_participants' =
        data_participants \ {<<collection, node>>}
    /\ UNCHANGED <<parent_binding, prepared, handles, objects,
                   reachable_nodes, topology_participants, fenced_nodes,
                   frozen, drop_target, dropped, committed_data, stale_commit>>

BeginTopology(collection, node) ==
    /\ parent_binding = collection
    /\ collection \notin frozen
    /\ node \notin objects[collection]
    /\ topology_participants' =
        topology_participants \cup {<<collection, node>>}
    /\ objects' = [objects EXCEPT ![collection] = @ \cup {node}]
    /\ UNCHANGED <<parent_binding, prepared, handles, reachable_nodes,
                   data_participants, fenced_nodes, frozen, drop_target,
                   dropped, committed_data, stale_commit>>

\* A participant admitted before the freeze remains joined through its
\* publication.  Drop does not enumerate or fence until all such participants
\* have either published or abandoned their node.
PublishTopology(collection, node) ==
    /\ <<collection, node>> \in topology_participants
    /\ topology_participants' =
        topology_participants \ {<<collection, node>>}
    /\ reachable_nodes' =
        [reachable_nodes EXCEPT ![collection] = @ \cup {node}]
    /\ UNCHANGED <<parent_binding, prepared, handles, objects,
                   data_participants, fenced_nodes, frozen, drop_target,
                   dropped, committed_data, stale_commit>>

AbandonTopology(collection, node) ==
    /\ <<collection, node>> \in topology_participants
    /\ topology_participants' =
        topology_participants \ {<<collection, node>>}
    /\ UNCHANGED <<parent_binding, prepared, handles, objects,
                   reachable_nodes, data_participants, fenced_nodes, frozen,
                   drop_target, dropped, committed_data, stale_commit>>

BeginDrop(collection) ==
    /\ drop_target = NoCollection
    /\ parent_binding = collection
    /\ drop_target' = collection
    /\ frozen' = frozen \cup {collection}
    /\ UNCHANGED <<parent_binding, prepared, handles, objects,
                   reachable_nodes, topology_participants, data_participants,
                   fenced_nodes, dropped, committed_data, stale_commit>>

FenceNode(collection, node) ==
    /\ drop_target = collection
    /\ ParticipantsFor(topology_participants, collection) = {}
    /\ node \in objects[collection]
    /\ node \notin fenced_nodes[collection]
    /\ <<collection, node>> \notin data_participants
    /\ fenced_nodes' =
        [fenced_nodes EXCEPT ![collection] = @ \cup {node}]
    /\ UNCHANGED <<parent_binding, prepared, handles, objects,
                   reachable_nodes, topology_participants, data_participants,
                   frozen, drop_target, dropped, committed_data, stale_commit>>

AbortDrop(collection) ==
    /\ drop_target = collection
    /\ drop_target' = NoCollection
    /\ frozen' = frozen \ {collection}
    /\ fenced_nodes' = [fenced_nodes EXCEPT ![collection] = {}]
    /\ UNCHANGED <<parent_binding, prepared, handles, objects,
                   reachable_nodes, topology_participants, data_participants,
                   dropped, committed_data, stale_commit>>

CommitDrop(collection) ==
    /\ drop_target = collection
    /\ parent_binding = collection
    /\ ParticipantsFor(topology_participants, collection) = {}
    /\ ParticipantsFor(data_participants, collection) = {}
    /\ objects[collection] \subseteq fenced_nodes[collection]
    /\ parent_binding' = NoCollection
    /\ drop_target' = NoCollection
    /\ dropped' = dropped \cup {collection}
    /\ UNCHANGED <<prepared, handles, objects, reachable_nodes,
                   topology_participants, data_participants, fenced_nodes,
                   frozen, committed_data, stale_commit>>

Next ==
    \/ \E collection \in CollectionIds : PrepareCollection(collection)
    \/ \E collection \in CollectionIds : PublishCollection(collection)
    \/ \E collection \in CollectionIds : OpenHandle(collection)
    \/ \E collection \in CollectionIds, node \in Nodes :
           BeginData(collection, node)
    \/ \E collection \in CollectionIds, node \in Nodes :
           CommitData(collection, node)
    \/ \E collection \in CollectionIds, node \in Nodes :
           AbortData(collection, node)
    \/ \E collection \in CollectionIds, node \in Nodes :
           BeginTopology(collection, node)
    \/ \E collection \in CollectionIds, node \in Nodes :
           PublishTopology(collection, node)
    \/ \E collection \in CollectionIds, node \in Nodes :
           AbandonTopology(collection, node)
    \/ \E collection \in CollectionIds : BeginDrop(collection)
    \/ \E collection \in CollectionIds, node \in Nodes :
           FenceNode(collection, node)
    \/ \E collection \in CollectionIds : AbortDrop(collection)
    \/ \E collection \in CollectionIds : CommitDrop(collection)

Spec == Init /\ [][Next]_vars

CL0_TypeOK ==
    /\ parent_binding \in CollectionIds \cup {NoCollection}
    /\ prepared \subseteq CollectionIds
    /\ handles \subseteq CollectionIds
    /\ objects \in [CollectionIds -> SUBSET Nodes]
    /\ reachable_nodes \in [CollectionIds -> SUBSET Nodes]
    /\ topology_participants \subseteq NodeParticipants
    /\ data_participants \subseteq NodeParticipants
    /\ fenced_nodes \in [CollectionIds -> SUBSET Nodes]
    /\ frozen \subseteq CollectionIds
    /\ drop_target \in CollectionIds \cup {NoCollection}
    /\ dropped \subseteq CollectionIds
    /\ committed_data \subseteq NodeParticipants
    /\ stale_commit \in BOOLEAN

CL1_ParentBindingIsAuthority ==
    /\ LogicalCollections \subseteq prepared
    /\ LogicalCollections \cap dropped = {}
    /\ \A collection \in CollectionIds :
           /\ reachable_nodes[collection] \subseteq objects[collection]
           /\ fenced_nodes[collection] \subseteq objects[collection]
    /\ \A collection \in LogicalCollections :
           RootNode \in reachable_nodes[collection]
    /\ \A collection \in prepared \ LogicalCollections :
           parent_binding # collection

CL2_ParticipantsRespectBindingAndFence ==
    \A participant \in data_participants :
        /\ participant[1] = parent_binding
        /\ participant[2] \in reachable_nodes[participant[1]]
        /\ participant[2] \notin fenced_nodes[participant[1]]

CL3_DropFencesEveryParticipant ==
    \A collection \in dropped :
        /\ objects[collection] \subseteq fenced_nodes[collection]
        /\ ParticipantsFor(topology_participants, collection) = {}
        /\ ParticipantsFor(data_participants, collection) = {}

CL4_StaleIncarnationCannotCommit == ~stale_commit

CL5_IncarnationsNeverReused ==
    /\ LogicalCollections \cap dropped = {}
    /\ \A collection \in dropped : collection \in frozen

\* The named guarantee imported by the transaction-core composition.
TxCoreLifecycleGuarantee ==
    /\ CL1_ParentBindingIsAuthority
    /\ CL2_ParticipantsRespectBindingAndFence
    /\ CL3_DropFencesEveryParticipant
    /\ CL4_StaleIncarnationCannotCommit
    /\ CL5_IncarnationsNeverReused

\* Negative control: an implementation that compares only the logical name
\* can use an old handle after that name has been rebound to a fresh ID.
CommitThroughStaleHandle(collection, node) ==
    /\ collection \in handles
    /\ collection \in dropped
    /\ parent_binding \in CollectionIds \ {collection}
    /\ node \in reachable_nodes[collection]
    /\ stale_commit' = TRUE
    /\ committed_data' =
        committed_data \cup {<<collection, node>>}
    /\ UNCHANGED <<parent_binding, prepared, handles, objects,
                   reachable_nodes, topology_participants, data_participants,
                   fenced_nodes, frozen, drop_target, dropped>>

MutantSpec ==
    Init /\ [][Next
                 \/ \E collection \in CollectionIds, node \in Nodes :
                        CommitThroughStaleHandle(collection, node)]_vars

=============================================================================
