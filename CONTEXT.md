# GlassDB Language

This glossary defines domain language for all parts of GlassDB. It groups terms
by project area.

## Database access

**Backend**:
The object store, such as S3, GCS, or memory, that holds the stored objects of one or more databases. GlassDB reads and lists stored objects by path, and changes them only with conditional mutations.

**Database**:
The durable collections, transaction records, and other protocol state that one backend stores under one database prefix. Its database ID identifies it: a database that is deleted and created again with the same name is a different database.

**Database ID**:
The identity of one database, created with the database. GlassDB never reuses it.

**Database prefix**:
The top-level object-path component under which one database stores all its objects. It is the validated database name.
_Avoid_: Database root, DB root

**Database instance**:
A local runtime created by one successful database open. Cloned database handles share that instance; separate opens create separate instances, including within one process.
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
The stored object that holds one collection's child bindings, directory lock, topology participants, and topology freeze. Reads and writes of logical keys do not read it.

**Collection handle**:
A value that names one collection by its collection ID. It stays bound to that collection and becomes stale when the collection is dropped.

**Drop**:
The removal of one collection and its binding. A drop is not recursive: a collection with child collections cannot be dropped.
_Avoid_: Delete (which applies to logical keys)

**Drop intent**:
A claim on one node of a collection tree, held by the transaction identity that drops the collection. After the holder commits, every later access through the node reports the collection handle as stale.
_Avoid_: Delete intent, drop fence, deletion fence

**Logical key**:
Raw key bytes interpreted within one collection. Equal bytes in different collections identify different logical keys.
_Avoid_: Object key, object path

## Transaction execution

**Transaction**:
A unit of work that a caller runs with one transaction operation, which executes a transaction body. Its staged changes commit together or not at all. It can run under more than one transaction identity and execute its body more than once, but at most one of its identities commits.

**Transaction body**:
The caller-supplied computation that reads data, records the changes to commit (its staged changes), and returns a body outcome when it completes. GlassDB may execute it more than once.
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
The currentness barrier that one transaction allocates after its body and before validation. Validation rechecks the access set at or after it, so transaction body reads can accept any currentness watermark.
_Avoid_: Validation watermark, validation timestamp

**Transaction identity**:
A durable protocol identity that holds one transaction's claims and owns its status and recovery resources.
_Avoid_: Lock owner ID, transaction attempt

**Transaction owner**:
The transaction operation in one database instance that runs a transaction identity. It refreshes the identity's lease and runs its owner operations. Other database instances know the identity only through its transaction record.

**Engaged identity**:
A transaction identity that started a locked commit or locked validation. It can have durable effects, so it must reach a final status.

**Priority**:
The wound-wait rank of a transaction identity, set when its transaction starts. An identity of an older transaction has priority over an identity of a younger one. Identities with equal priority are not ordered.

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
A body outcome that cannot expose an inconsistent snapshot, because GlassDB validates its reads before the caller receives it.

**Cancellation-safe**:
A transaction is cancellation-safe when cancellation cannot cause a partial logical commit or leave a durable protocol resource for which neither its owner nor recovery is responsible. It does not guarantee rollback.

**Protocol-clean retirement**:
The state in which an interrupted transaction can no longer publish new effects, and recovery is responsible for every remaining durable resource. Physical reclamation may complete later.
_Avoid_: Immediate cleanup, complete deletion

**Retirement handoff**:
The synchronous transfer of responsibility for an interrupted transaction from its owner to recovery, before control leaves the owner. Protocol-clean retirement may follow asynchronously.
_Avoid_: Synchronous cleanup

**Owner operation**:
Protocol work that the transaction owner runs under one transaction identity and that can still publish effects. Each commit pass is one owner operation. While one is active or unresolved, the identity can still publish effects, so it cannot reach protocol-clean retirement.

## Commit

**Transaction record**:
The durable record of one transaction identity. It holds the identity's status, lease, and recovery manifest, and the committed values after commit. The owner writes it only when the protocol needs it, so a holder can have no transaction record for a short time.
_Avoid_: Transaction log, transaction object, tx log, log object

**Recovery manifest**:
The part of a transaction record that lists the claims of its identity, the collections that it creates or drops, and the new collections whose stored objects it created before commit. Recovery and GC use it to find the identity's durable effects when the owner cannot.
_Avoid_: Transaction manifest, lock intentions, back-references

**Lease**:
The time during which a pending transaction record shows that its owner is still active. The owner extends it by refreshing the record; after it expires, other transactions can wound the identity.
_Avoid_: Lock lease, heartbeat

**Transaction status**:
The state of one transaction identity in its transaction record: pending, committed, wounded, or aborted. Only a pending identity can still commit.

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

**Effective writer**:
The writer that reads of one logical key observe: the committed holder of a write lock or create lock on the key, if one exists; otherwise the writer of the key's current state. The two are different only until the write-back of a committed transaction completes.

**Direct commit**:
A commit that validates and publishes all point accesses of one transaction with one CAS of one leaf, without locks or a transaction record.
_Avoid_: Logless commit, same-leaf commit

**Locked commit**:
A commit that locks the access set, validates its reads, and then makes the transaction record committed.
_Avoid_: Logged protocol, regular commit protocol, locked path

**Commit point**:
The CAS that decides whether a transaction commits: in a locked commit, the CAS that makes its transaction record committed; in a direct commit, the leaf CAS. After it takes effect, the staged changes are committed, even if write-back has not run.
_Avoid_: Commit flip

**Validation**:
The check that every point read in an access set still observes the same effective writer, and that every read of an absent key and every range scan still observes the same membership generation. GlassDB validates the reads before it commits or returns a body outcome.

**Optimistic validation**:
Validation of an access set before the transaction holds any lock.
_Avoid_: Read-only fast path

**Locked validation**:
Validation of an access set while the transaction holds its locks.

**Invalidated read**:
A read in an access set whose effective writer or membership generation changed before validation. It causes a body replay.
_Avoid_: Validation conflict, read conflict, stale read

**Locked replay**:
A body replay after locked validation, under the same transaction identity, that keeps its key locks and membership locks. Other transactions cannot write the keys that it already locked, so their writes to those keys cannot cause another invalidated read.
_Avoid_: Pessimistic fallback, pessimistic retry

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

**Wound-wait**:
The rule that resolves a lock conflict by priority: a requester with priority over the holder wounds it, and a requester with lower or equal priority waits for the holder.

**Hold-and-wait**:
The way that a lock requester waits for a holder under wound-wait: it keeps all the locks that it already holds, and it does not renew its identity or replay its body.

**Key lock**:
A lock on one logical key, recorded in the key's leaf entry. It can lock a key that has no current value.
_Avoid_: Entry lock

**Read lock**:
A key lock that a transaction takes on a key that it reads. More than one transaction identity can hold it on the same key. Its sole holder can change it to a write lock or a create lock.

**Write lock**:
An exclusive key lock that a transaction takes to delete a key, or to write a value for a key in the key membership.

**Create lock**:
An exclusive key lock that a transaction takes to write a value for a key that is not in the key membership.

**Membership lock**:
A lock on the key membership of one leaf. Range scans hold it shared, and changes to the key membership hold it exclusively.
_Avoid_: Membership hold

**Directory lock**:
A lock on the child bindings in one collection record.

**Serial acquisition**:
Lock acquisition that locks the leaves of one transaction one at a time, in ascending order of their stored-object paths. This global order cannot deadlock. A transaction locks its leaves in parallel by default, and switches to serial acquisition under a renewed identity when parallel acquisition does not make progress.
_Avoid_: Serial locking, serial validation, serial mode

**Wound**:
The conditional change of a transaction record from pending, or from absent, to wounded, usually by another transaction. After it, the identity can never commit.

**Fence**:
A durable change that stops earlier work from publishing more effects, even if that work is still running. A wound, for example, fences the owner of the wounded identity.

## Conditional mutations

**Revision**:
The opaque token that identifies one content state of one stored object. It does not order states.
_Avoid_: Version, backend version, CAS token, generation, ETag

**Conditional mutation**:
A backend change of one stored object that takes effect only if its precondition holds: a CAS or a conditional delete. A conditional delete also succeeds when the object is already absent, so it is not a CAS.

**CAS**:
A conditional mutation that creates or replaces one stored object only if the object is in the expected state: absent, or at an exact revision. An applied CAS shows that the expected state was current when the CAS took effect.
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

**Currentness**:
The property that a known state of one stored object is still its latest state in the backend. It is different from the current state of a logical key, which is the committed state in its leaf entry.

**Sequence point**:
A point on the local timeline of one database instance, which orders currentness evidence within that instance. It is neither wall time nor comparable across database instances.
_Avoid_: Timestamp, epoch, logical clock

**Invocation point**:
The sequence point allocated immediately before one backend operation starts. The operation takes effect at or after it.
_Avoid_: Invocation watermark

**Definitive result**:
A backend result that shows the outcome of one operation: the state or absence that a read found, or an applied or rejected mutation. A result that does not show the outcome, such as an in-doubt mutation or a failed read, is not definitive.

**Path lane**:
The admission rule that lets only one backend read or conditional mutation of one stored object run at a time within one database instance. An operation gets its invocation point after it enters the lane, and updates what the instance knows about the object before it leaves. Thus the order of invocation points agrees with the order of the operations in the backend.

**Currentness barrier**:
A sequence point allocated to separate finished work from work not yet started: every operation that returned a definitive result before the allocation has an earlier invocation point, and every operation invoked after the allocation has an invocation point at or after it.
_Avoid_: Anchor, epoch, fresh read

**Observation**:
The exact state of one stored object, or its absence, as a read returned it or an applied mutation installed it, with its currentness watermark. It does not prove that the state is current now.

**Currentness watermark**:
The sequence point an observation carries, after which its state was known to be current. It is allocated before the read or mutation that produced the observation, so it states nothing about the state after that operation.
_Avoid_: Anchor, observation timestamp, read watermark

**Revision-conditional read**:
A read of one stored object that returns its state only if its revision is different from a known revision. Otherwise it shows that the known state was still current when the read took effect, and it does not transfer the object content.

**Freshness requirement**:
The rule a read applies to decide whether an existing observation can serve it: accept any currentness watermark, or only a watermark that reached a stated bound. A reader states that bound as a currentness barrier.
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

**Node ID**:
The identity of one node other than the tree root within its collection tree. Index nodes and sibling links refer to other nodes by their node IDs.
_Avoid_: Node token

**Index node**:
A node that routes key ranges to child nodes through separators.
_Avoid_: Interior node

**Leaf**:
A terminal node of a collection tree. In one exact state, it owns a contiguous logical-key range, and every change of a key in that range is a CAS of the leaf.
_Avoid_: Shard, leaf shard

**Leaf entry**:
The part of a leaf that holds the current state and key locks of one logical key.

**Key membership**:
The set of logical keys in one leaf whose current state is an inline or external value.

**Membership generation**:
A leaf counter that changes when a transaction or a structural change changes, or can change, the key membership of the leaf.
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
A logical key in a parent index node that bounds one child's range: keys at or above it route to that child. Parent reconciliation adds a separator after a child split, and removes one after a child merge.
_Avoid_: Index key, boundary key

## Leaf coordination

**Coordinator round**:
One group of operations coordinated by one database instance for one leaf until the group completes. A round can require multiple CASes.
_Avoid_: Fold round, CAS (when referring to the whole round)

**Round member**:
One operation from one transaction identity in a coordinator round, with its own proposed leaf changes and outcome. A mutation plan includes all of its leaf changes or none of them.
_Avoid_: Fold member

**Mutation plan**:
The proposed state of one leaf and the round members' outcomes, which at most one CAS publishes. A plan does not prove that its CAS took effect.
_Avoid_: Fold, fold plan

## Structural changes

**Topology**:
The nodes, separators, and sibling links of one collection tree.
_Avoid_: Tree shape

**Structural change**:
A change of the topology of one collection tree, such as a split or a merge.
_Avoid_: Topology change

**Split**:
A structural change that moves the upper part of one node's key range into a new sibling node. A tree root instead splits in place into two new children.

**Merge**:
A structural change that moves the key range and all entries of one node into its right sibling. A tree root never merges.
_Avoid_: Coalesce, join

**Drained node**:
A node after its merge. It owns no key range, and only links to the node that received its entries.
_Avoid_: Retired node, redirect node

**Merge reservation**:
A durable mark on the node that receives a merge, which names the structural intent of that merge. It allows no other structural change to that node, and only that merge or its recovery can remove it.
_Avoid_: Merge gate, merge lock

**Parent reconciliation**:
The step after a split or a merge that makes the separators of one parent index node agree with the sibling links of its children around one key.
_Avoid_: Separator publication, parent repoint

**Topology participant**:
A transaction identity that a collection record lists while it can make structural changes to the collection tree.
_Avoid_: Topology lock

**Structural intent**:
The durable plan of one structural change, written by one topology participant. It stays until the change is completed or recovered.
_Avoid_: Structural log, structural record, topology intent

**Structural gate**:
An exclusive claim on one node that allows structural changes to that node. A release or a recovery fence must remove it before another structural change starts.
_Avoid_: Structure lock, structure-write lock

**Topology freeze**:
A claim on a collection record, held by the transaction identity that prepares a drop of the collection. It allows no new topology participant, and the existing participants must complete or be recovered before the drop continues.

## Maintenance

**Recovery**:
Work that completes or reverts the durable effects of an interrupted operation from durable state alone, for example after a transaction owner stops.

**GC**:
Background work that reclaims the durable effects of transactions, and deletes the transaction records that no stored object names.

**GC hint**:
A report within one database instance that a transaction identity can have GC work. It makes the identity a GC candidate without a GC scan.
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
A wounded transaction record that GC keeps until the owner proves protocol-clean retirement and changes it to aborted.
_Avoid_: Pinned transaction marker, pinned wound marker

**GC backlog**:
Known GC work that is ready to run but has not completed. Transaction records that stored objects still name, pinned wounds, and work that waits for its next permitted check are not GC backlog by themselves.
_Avoid_: Cleanup backlog, transaction-object count, garbage count

**GC scan**:
A traversal of stored transaction records to find GC candidates independently of GC hints. Scans of structural intents belong to structural recovery.
_Avoid_: Recovery scan (when referring to transaction-record GC)

**Transaction prefix**:
One of the fixed listing prefixes that partition the transaction records of one database by transaction identity.
_Avoid_: Transaction shard
