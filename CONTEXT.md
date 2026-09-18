# GlassDB Language

This glossary defines domain language for all parts of GlassDB. It groups terms
by project area.

## Database access

**Database instance**:
A local runtime created by one successful database open. Cloned handles share that instance; separate opens create separate instances, including within one process.

## Data model

**Collection**:
An ordered group of key-value pairs within one database. It has a stable identity, even if its name is removed and later reused, and can contain named child collections.
_Avoid_: Table, bucket

**Logical key**:
Raw key bytes interpreted within one collection. Equal bytes in different collections identify different logical keys.
_Avoid_: Object key, object path

## Transaction execution

**Transaction body**:
The caller-supplied computation that stages transaction changes and returns a body outcome when it completes. GlassDB may execute it more than once.
_Avoid_: Callback, user closure

**Point access**:
An access to one exact logical key, as distinct from an access to a key range. It can read the key, write it, or do both.
_Avoid_: Point, point item

**Access set**:
The point reads, final key writes, and range scans from one execution of a transaction body.
_Avoid_: Data, transaction data

**Validation barrier**:
The currentness barrier one transaction allocates to open validation, and the landmark its key locking, collection locking, and status flip are organized around. The access set's retained observations are rechecked at or after it, which is why transaction body reads may accept any watermark.
_Avoid_: Validation watermark, validation timestamp

**Transaction identity**:
A durable protocol identity that correlates one transaction's locks, status, and recovery resources. Replacing it does not by itself repeat the transaction body or discard that body's access set and body outcome.
_Avoid_: Lock owner ID

**Body outcome**:
The value returned by one execution of a transaction body. GlassDB can discard the outcome and execute the body again.
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

## Currentness

**Applied mutation**:
A conditional backend mutation known to have taken effect on one stored object. This does not establish that the installed state is still current.
_Avoid_: Committed mutation

**Sequence point**:
A point on one database-local timeline, which orders currentness evidence within one open database. It is neither wall time nor comparable across database instances.
_Avoid_: Timestamp, epoch, logical clock

**Currentness barrier**:
A sequence point allocated to separate finished work from work not yet started: no operation that definitively completed before the allocation reaches it, and every operation invoked after it does.
_Avoid_: Anchor, epoch, fresh read

**Currentness watermark**:
The sequence point an observation carries, after which its state was known to be current. It is allocated before the read or mutation that produced the observation, so it states nothing about the state after that operation.
_Avoid_: Anchor, observation timestamp, read watermark

**Freshness requirement**:
The rule a read applies to decide whether existing evidence can serve it: accept any watermark, or only a watermark that reached a stated bound. A reader states that bound as a currentness barrier.
_Avoid_: Consistency level, staleness policy

## Point routing

**Leaf**:
A terminal physical node in one collection's range-partitioned tree. In one exact state, it owns a contiguous logical-key range and is the physical mutation unit for that range.
_Avoid_: Shard, leaf shard

**Routing**:
The resolution of a logical key or range endpoint to a leaf by descent through a collection's tree. Its result records observed placement; it does not reserve the key or keep that placement current.
_Avoid_: Shard calculation, ownership proof

**Leaf observation**:
An exact observed state of one leaf, with a currentness watermark after which that state was known to be current. It does not claim that the state is current now.
_Avoid_: Fresh leaf, leaf version, freshness observation

**Routed leaf group**:
One leaf observation and the ordered logical keys associated with it by one routing operation. The group records that routing result; it is not a durable ownership claim.
_Avoid_: Leaf group, owning leaf group, point-leaf plan

**Separator**:
A logical key in a parent index that bounds one child's range: keys at or above it route to that child. A child split publishes a new separator into its parent.
_Avoid_: Index key, boundary key

## Leaf coordination

**Coordinator round**:
One group of operations coordinated by one database instance for one leaf until the group completes. A round can require multiple mutation attempts.
_Avoid_: Fold round, CAS (when referring to the whole round)

**Round member**:
One operation from one transaction identity in a coordinator round, with its own mutation decision and outcome. Its leaf changes are admitted together or not at all.
_Avoid_: Fold member

**Mutation plan**:
The proposed state of one leaf and the round members' outcomes for one mutation attempt. A plan does not prove that a backend mutation took effect.
_Avoid_: Fold, fold plan

## Topology changes

**Structural intent**:
A durable claim for one planned topology change, owned by a topology participant until the change is completed or recovered.
_Avoid_: Structural log, structural record

**Structural gate**:
An exclusive, durably recorded claim on one node that admits changes to the node's shape. A gate bound to a structural intent remains held until that intent's recovery releases it, even when its transaction owner is final.
_Avoid_: Structure lock, structure-write lock

**Redirect**:
A retired node identity that refers routing to its successor. It retains no authority over key values.
_Avoid_: Alias node, forwarding leaf

## Maintenance

**GC candidate**:
A transaction identity selected for a check of its remaining references and recovery resources. Selection does not prove that its transaction object can be deleted.
_Avoid_: Cleanup candidate

**GC backlog**:
Known GC work that is ready to run but has not completed. Retained live values, pinned transaction markers, and work awaiting its next permitted check do not by themselves constitute GC backlog.
_Avoid_: Cleanup backlog, transaction-object count, garbage count

**GC scan**:
A traversal of stored transaction objects to find GC candidates independently of local hints. Scans of structural intents belong to structural recovery.
_Avoid_: Recovery scan (when referring to transaction-object GC)
