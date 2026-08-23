# GlassDB

Domain language for GlassDB's optimistic transactions and recovery guarantees.

## Language

**Transaction body**:
The caller-supplied computation that produces a transaction's staged changes and normal outcome. GlassDB may execute it more than once.
_Avoid_: Callback, user closure

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
