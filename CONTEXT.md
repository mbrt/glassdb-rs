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
The caller-supplied computation that produces a transaction's staged changes and normal outcome. GlassDB may execute it more than once.
_Avoid_: Callback, user closure

**Point access**:
An access to one exact logical key, as distinct from an access to a key range. It can read the key, write it, or do both.
_Avoid_: Point, point item

**Access set**:
The point reads, final key writes, and range scans from one execution of a transaction body.
_Avoid_: Data, transaction data

**Normal outcome**:
A value or error returned by a transaction body and therefore eligible for snapshot validation and transparent retry.
_Avoid_: Success

**Abnormal abandonment**:
A transaction body ending without a normal outcome because its future is dropped, it unwinds, or its process stops.
_Avoid_: Body error

**Snapshot-transparent**:
A transaction outcome that cannot expose an inconsistent snapshot because its reads are validated before it escapes.

**Protocol-clean retirement**:
The state in which an abandoned transaction can no longer publish new effects and every remaining durable resource has a recovery owner. Physical reclamation may complete later.
_Avoid_: Immediate cleanup, complete deletion

**Retirement handoff**:
The synchronous transfer of responsibility for an abandoned transaction to managed recovery work before control leaves its owner. Protocol-clean retirement may follow asynchronously.
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
