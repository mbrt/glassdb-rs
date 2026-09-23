# GlassDB Language

This glossary defines domain language for all parts of GlassDB. It groups terms
by project area.

## Database access

**Database**:
The durable collections and protocol state that one backend stores under one database prefix. Its database ID identifies it: if its metadata is deleted and created again, the result is a different database with the same name.

**Database prefix**:
The top-level object-path component under which one database stores all its objects. It is the validated database name.
_Avoid_: Database root, DB root

**Database instance**:
A local runtime created by one successful database open. Cloned handles share that instance; separate opens create separate instances, including within one process.
_Avoid_: Client

## Data model

**Collection**:
An ordered group of key-value pairs within one database. Its collection ID identifies it, even if its name is removed and later reused, and it can contain named child collections.
_Avoid_: Table, bucket

**Collection ID**:
The identity of one collection within its database. GlassDB never reuses it: a collection created with the name of a dropped collection gets a new collection ID.
_Avoid_: Incarnation, incarnation ID

**Root collection**:
The permanent collection that each database has. It has a reserved collection ID and cannot be dropped.
_Avoid_: Database root

**Binding**:
The entry in a parent collection that maps one child name to one collection ID.

**Collection record**:
The object that holds one collection's child bindings, directory lock, topology participants, and topology freeze. Data-path operations do not read it.

**Collection handle**:
A value that names one collection by its collection ID. It stays bound to that collection and becomes stale when the collection is dropped.

**Drop**:
The removal of one collection and its binding. A drop is not recursive: a collection with child collections cannot be dropped.
_Avoid_: Delete (which applies to logical keys)

**Drop intent**:
A claim on one node of a collection, held by the transaction identity that drops the collection. After the holder commits, every later access through the node reports the collection handle as stale.
_Avoid_: Delete intent, drop fence, deletion fence

**Logical key**:
Raw key bytes interpreted within one collection. Equal bytes in different collections identify different logical keys.
_Avoid_: Object key, object path

## Transaction execution

**Transaction body**:
The caller-supplied computation that stages transaction changes and returns a body outcome when it completes. GlassDB may execute it more than once.
_Avoid_: Callback, user closure

**Body replay**:
An execution of the transaction body that replaces the discarded body outcome of an earlier execution in the same transaction.
_Avoid_: Retry, re-run, transaction retry

**Point access**:
An access to one exact logical key, as distinct from an access to a key range. It can read the key, write it, or do both.
_Avoid_: Point, point item

**Access set**:
The point reads, final key writes, and range scans from one execution of a transaction body.
_Avoid_: Data, transaction data

**Validation barrier**:
The currentness barrier that one transaction allocates after its body and before validation. Validation rechecks the access set at or after it, so transaction body reads can accept any watermark.
_Avoid_: Validation watermark, validation timestamp

**Transaction identity**:
A durable protocol identity that holds one transaction's claims and owns its status and recovery resources.
_Avoid_: Lock owner ID, transaction attempt

**Engaged identity**:
A transaction identity that started a locked commit or locked validation. It can have durable effects, so it must reach a final status.

**Priority**:
The wound-wait rank of a transaction identity. An older identity has priority: it can wound a younger holder, and a younger requester waits for an older holder. Identities with equal priority are not ordered.

**Identity renewal**:
The replacement of a transaction identity with a new identity that keeps the same wound-wait priority. It does not by itself replay the transaction body or discard that body's access set and body outcome.
_Avoid_: Replacement identity, restart

**Commit pass**:
One run of the commit protocol for one body outcome under one transaction identity. It ends when the body outcome can be returned, when the identity must be renewed, when the body must be replayed, or with an error. Identity renewal and body replay each start a new commit pass.
_Avoid_: Attempt, commit attempt

**Body outcome**:
The value returned by one execution of a transaction body. GlassDB can discard the outcome and replay the body.
_Avoid_: Normal outcome

**Commit outcome**:
A body outcome that contains a value and proposes the staged changes for commit.
_Avoid_: Success

**Error outcome**:
A body outcome that contains an error and rejects the staged changes. GlassDB validates its reads before it returns the outcome.
_Avoid_: Body error

**Explicit abort**:
An error outcome that identifies a deliberate request to reject the transaction's staged changes.
_Avoid_: Transaction cancellation

**Transaction interruption**:
A transaction operation that ends without returning because of cancellation, panic, or process failure. It can occur before or after a body outcome exists.
_Avoid_: Abnormal abandonment

**Transaction cancellation**:
A transaction interruption caused by dropping the transaction future before it returns. Cancellation does not prove that the transaction had no effect.
_Avoid_: Explicit abort

**Snapshot-transparent**:
A body outcome that cannot expose an inconsistent snapshot because its reads are validated before it escapes.

**Cancellation-safe**:
A transaction is cancellation-safe when cancellation cannot cause a partial logical commit or leave durable protocol resources without a recovery owner. It does not guarantee rollback.

**Protocol-clean retirement**:
The state in which an interrupted transaction can no longer publish new effects and every remaining durable resource has a recovery owner. Physical reclamation may complete later.
_Avoid_: Immediate cleanup, complete deletion

**Retirement handoff**:
The synchronous transfer of responsibility for an interrupted transaction to managed recovery work before control leaves its owner. Protocol-clean retirement may follow asynchronously.
_Avoid_: Synchronous cleanup

**Owner operation**:
Protocol work that the owner of a transaction identity runs under that identity and that can still publish effects. Each commit pass is one owner operation. While one is active or unresolved, retirement cannot prove that the identity can publish nothing more.

## Commit

**Transaction record**:
The durable record of one transaction identity. It holds the identity's status, lease, and recovery manifest, and the committed values after commit.
_Avoid_: Transaction log, transaction object, tx log, log object

**Recovery manifest**:
The part of a transaction record that lists the claims, collection changes, and prepared collections of its identity. Recovery and GC use it to find the identity's durable effects when the owner cannot.
_Avoid_: Transaction manifest, lock intentions, back-references

**Lease**:
The time during which a pending transaction record shows that its owner is still active. The owner extends it by refreshing the record; after it expires, other transactions can wound the identity.
_Avoid_: Lock lease, heartbeat

**Final status**:
A transaction status that decides whether the identity commits: committed, wounded, or aborted. A wounded status can still change to aborted, but no final status can change to committed.
_Avoid_: Terminal status, finalized status

**Current state**:
The committed state of one logical key in its leaf entry: an inline value, an external value, or a tombstone, each with its writer; or absent, with no writer.
_Avoid_: Pointer

**Inline value**:
A current state that holds the value bytes in the leaf entry. Readers return it without reading the writer's transaction record.

**External value**:
A current state whose value bytes are in the writer's transaction record.
_Avoid_: External pointer, pointer

**Tombstone**:
A current state that records that its writer deleted the key.

**Writer**:
The transaction identity whose commit produced the current state of one logical key.
_Avoid_: Version, writer token, value version

**Direct commit**:
A commit that validates and publishes all point accesses of one transaction with one CAS of one leaf, without locks or a transaction record.
_Avoid_: Logless commit, same-leaf commit

**Locked commit**:
A commit that locks the access set, validates its reads, and then makes the transaction record committed.
_Avoid_: Logged protocol, regular commit protocol, locked path

**Optimistic validation**:
Validation of an access set before the transaction holds any lock.
_Avoid_: Read-only fast path

**Locked validation**:
Validation of an access set while the transaction holds its locks.

**Invalidated read**:
A read in an access set whose observed writer or observed key membership changed before validation. It causes a body replay.
_Avoid_: Validation conflict, read conflict, stale read

**Write-back**:
The publication of a committed transaction's changes into the objects that it locked, together with the release of those locks.
_Avoid_: Cleanup, commit cleanup

**Help-forward**:
A write-back that another transaction or GC does for a committed holder.
_Avoid_: Helping

## Claims and locks

**Claim**:
A durable mark on one stored object that names the transaction identity that holds it. The holder's transaction record decides its meaning: while the holder can still commit, the claim excludes conflicting work, and after that it takes effect or can be removed.

**Holder**:
The transaction identity that a claim names.

**Conflict**:
Concurrent access to the same data by two transactions, where at least one of them writes.

**Lock**:
A claim that a transaction takes on the data that it reads or writes. Wound-wait resolves conflicts between the holders of conflicting locks.

**Key lock**:
A lock on one logical key, recorded in the key's leaf entry. It can lock a key that has no current value.
_Avoid_: Entry lock

**Membership lock**:
A lock on the set of logical keys in one leaf. Range scans hold it shared, and changes to the key set hold it exclusively.
_Avoid_: Membership hold

**Directory lock**:
A lock on the child bindings in one collection record.

**Serial acquisition**:
Lock acquisition that locks the leaves of one transaction one at a time, in ascending leaf path order. This global order cannot deadlock, so a transaction switches to it under a renewed identity when parallel acquisition does not make progress.
_Avoid_: Serial locking, serial validation, serial mode

**Wound**:
The conditional change of a pending transaction record to wounded, usually by another transaction. After it, the identity can never commit.

**Fence**:
A durable change that stops earlier work from publishing more effects, even if that work is still running. A wound, for example, fences the owner of the wounded identity.

## Conditional mutations

**Revision**:
The opaque token that identifies one content state of one stored object. It does not order states.
_Avoid_: Version, backend version, CAS token, generation, ETag

**Conditional mutation**:
A backend change of one stored object that takes effect only if its precondition holds: a CAS or a conditional delete. A conditional delete also succeeds when the object is already absent, so it is not a CAS.

**CAS**:
A conditional mutation that creates or replaces one stored object only if its current state is the expected state: absence or an exact revision. An applied CAS shows that the expected state was current when the CAS took effect.
_Avoid_: Conditional write

**Applied mutation**:
A conditional mutation known to have taken effect on one stored object. This does not establish that the installed state is still current.
_Avoid_: Committed mutation

**CAS receipt**:
The evidence that one applied CAS returns. It shows that the expected state was current at or after the invocation point of the CAS, and it holds an observation of the installed state. It does not show that either state was current at another time.
_Avoid_: Mutation receipt

**Rejected mutation**:
A conditional mutation that did not take effect because its precondition was false.
_Avoid_: Conflict, precondition failure

**In-doubt mutation**:
A conditional mutation whose result does not show whether it took effect.
_Avoid_: Indeterminate, ambiguous, or uncertain mutation

## Currentness

**Sequence point**:
A point on one database-local timeline, which orders currentness evidence within one open database. It is neither wall time nor comparable across database instances.
_Avoid_: Timestamp, epoch, logical clock

**Currentness barrier**:
A sequence point allocated to separate finished work from work not yet started: no operation that definitively completed before the allocation reaches it, and every operation invoked after it does.
_Avoid_: Anchor, epoch, fresh read

**Invocation point**:
The sequence point allocated immediately before one backend operation starts. The operation takes effect at or after it.
_Avoid_: Invocation watermark

**Observation**:
The exact state of one stored object, or its absence, as a read returned it or an applied mutation installed it, with its currentness watermark. It does not prove that the state is current now.

**Currentness watermark**:
The sequence point an observation carries, after which its state was known to be current. It is allocated before the read or mutation that produced the observation, so it states nothing about the state after that operation.
_Avoid_: Anchor, observation timestamp, read watermark

**Freshness requirement**:
The rule a read applies to decide whether existing evidence can serve it: accept any watermark, or only a watermark that reached a stated bound. A reader states that bound as a currentness barrier.
_Avoid_: Consistency level, staleness policy

**Stale read**:
A read outside a transaction that accepts a committed state known to be current within a stated age.

## Point routing

**Collection tree**:
The range-partitioned B-link tree of nodes that holds the logical keys of one collection.
_Avoid_: Coordination directory

**Node**:
One stored object of a collection tree: an index node or a leaf.
_Avoid_: Data node

**Tree root**:
The node at the fixed path of one collection tree, where every routing starts. It is separate from the collection record.
_Avoid_: Root (alone), collection root

**Index node**:
A node that routes key ranges to child nodes through separators.
_Avoid_: Interior node

**Leaf**:
A terminal node of a collection tree. In one exact state, it owns a contiguous logical-key range and is the physical mutation unit for that range.
_Avoid_: Shard, leaf shard

**Membership generation**:
A leaf counter that changes when a transaction changes, or can change, the set of logical keys in the leaf.
_Avoid_: Membership version

**Routing**:
The resolution of a logical key or range endpoint to a leaf by descent through a collection tree. Its result records observed placement; it does not reserve the key or keep that placement current.
_Avoid_: Shard calculation, ownership proof

**Leaf observation**:
An observation of one leaf.
_Avoid_: Fresh leaf, leaf version, freshness observation

**Routed leaf group**:
One leaf observation and the ordered logical keys associated with it by one routing operation. The group records that routing result; it is not a claim.
_Avoid_: Leaf group, owning leaf group, point-leaf plan

**Separator**:
A logical key in a parent index node that bounds one child's range: keys at or above it route to that child. A child split publishes a new separator into its parent.
_Avoid_: Index key, boundary key

## Leaf coordination

**Coordinator round**:
One group of operations coordinated by one database instance for one leaf until the group completes. A round can require multiple CASes.
_Avoid_: Fold round, CAS (when referring to the whole round)

**Round member**:
One operation from one transaction identity in a coordinator round, with its own mutation decision and outcome. Its leaf changes are admitted together or not at all.
_Avoid_: Fold member

**Mutation plan**:
The proposed state of one leaf and the round members' outcomes, which at most one CAS publishes. A plan does not prove that its CAS took effect.
_Avoid_: Fold, fold plan

## Structural changes

**Topology**:
The nodes, separators, and links of one collection tree.
_Avoid_: Tree shape

**Structural change**:
A change of the topology of one collection tree, such as a split.
_Avoid_: Topology change

**Topology participant**:
A transaction identity that a collection record lists while it can make structural changes to the collection tree.
_Avoid_: Topology lock

**Structural intent**:
The durable plan of one structural change, written by one topology participant. It stays until the change is completed or recovered.
_Avoid_: Structural log, structural record, topology intent

**Structural gate**:
An exclusive claim on one node that admits structural changes to that node. A release or a recovery fence must remove it before another structural change starts.
_Avoid_: Structure lock, structure-write lock

**Topology freeze**:
A claim on a collection record, held by the transaction identity that prepares a drop of the collection. It admits no new topology participant, and the existing participants must complete or be recovered before the drop continues.

## Maintenance

**GC hint**:
A local report that a transaction identity can have GC work. It makes the identity a GC candidate without a GC scan.
_Avoid_: Cleanup hint

**GC candidate**:
A transaction identity selected for a GC check. Selection does not prove that its transaction record can be deleted.
_Avoid_: Cleanup candidate

**GC check**:
The check of one GC candidate's recovery manifest and committed writes against the current stored objects. It decides which effects GC can reclaim and whether GC can delete the transaction record.
_Avoid_: Reverse liveness check, reverse check, reference check

**Safety horizon**:
The time after the last refresh of a transaction record during which GC keeps the record and its effects, unless the record is wounded. It is the lease plus the allowed clock skew.
_Avoid_: Cleanup horizon, sweep horizon, retention horizon, lease horizon, safety lease

**Pinned wound**:
A wounded transaction record that GC keeps until the owner proves retirement and changes it to aborted.
_Avoid_: Pinned transaction marker, pinned wound marker

**GC backlog**:
Known GC work that is ready to run but has not completed. Retained live values, pinned wounds, and work awaiting its next permitted check do not by themselves constitute GC backlog.
_Avoid_: Cleanup backlog, transaction-object count, garbage count

**GC scan**:
A traversal of stored transaction records to find GC candidates independently of GC hints. Scans of structural intents belong to structural recovery.
_Avoid_: Recovery scan (when referring to transaction-record GC)

**Transaction prefix**:
One of the fixed listing prefixes that partition the transaction records of one database by transaction identity.
_Avoid_: Transaction shard
