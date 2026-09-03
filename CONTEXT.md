# GlassDB Language

This glossary defines domain language for all parts of GlassDB. It groups terms
by project area.

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
A body outcome that contains an error and rejects the staged changes.
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

## Point routing

**Leaf**:
A terminal physical node in one collection's range-partitioned tree. In one exact state, it owns a contiguous logical-key range and is the physical mutation unit for that range.
_Avoid_: Shard, leaf shard

**Routing**:
The resolution of a logical key or range endpoint to a leaf by descent through a collection's tree. Its result records observed placement; it does not reserve the key or keep that placement current.
_Avoid_: Shard calculation, ownership proof

**Leaf observation**:
An exact observed state of one leaf, with evidence that the state was current after a stated lower bound. It does not claim that the state is current now.
_Avoid_: Fresh leaf, leaf version, freshness observation

**Routed leaf group**:
One leaf observation and the ordered logical keys associated with it by one routing operation. The group records that routing result; it is not a durable ownership claim.
_Avoid_: Leaf group, owning leaf group, point-leaf plan

## Topology changes

**Structural intent**:
A durable claim for one planned topology change, owned by a topology participant until the change is completed or recovered.
_Avoid_: Structural log, structural record
