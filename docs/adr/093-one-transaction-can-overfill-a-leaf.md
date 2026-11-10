# ADR-093: One transaction can overfill a leaf

## Status

Accepted.

Refines [ADR-032](032-node-locking-and-coordinated-splits.md). It changes when
a leaf admits a stage that adds leaf entries, for locked commits and for direct
commits. It also adds a local transaction limit.

## Context

A stage of a locked commit adds a leaf entry for each absent key that it
accesses. A create adds the entry of its new key. A read or a delete of an
absent key also adds an entry, because its lock needs one. A direct commit that
creates a key also adds an entry.

ADR-032 admits a create only if the leaf content after the stage is within the
content limit. The content limit is the hard cap minus the split headroom. The
split headroom is for splits and for the locks on existing entries. Over the
content limit, the create gets `LeafFull` and waits for a capacity split.

A split divides a leaf only at an existing key. So the wait does not help a
transaction whose new entries do not fit in one leaf when no existing key is
between them. A bulk load into a new key range is such a transaction, and the
default transaction limits allow one. The leaf coordinator rejects such a stage
at once with `InvalidInput`, so the transaction cannot commit.

The stage of a read or a delete of an absent key is not a create. Only the hard
cap limits it, so these stages can use the split headroom.

## Decision

### The hard cap has room for one transaction

The default hard cap is large enough for the default create limit plus the
largest stage that the default transaction limits allow. The default create
limit is well above the byte soft cap. So a leaf whose soft cap split waits
(ADR-084) still admits the largest stage. The split headroom stays as it is.

### A leaf admits a stage while the leaf has room for the largest stage

Each database instance computes its **create limit**. It is the content limit
minus the largest stage that the local transaction limits allow. The leaf
coordinator admits a stage that adds entries only if both of these conditions
are true:

- The leaf content before the stage is within the create limit.
- The leaf content after the stage is within the content limit.

The rule applies to every stage that adds entries. That includes the reads and
the deletes of absent keys, and the direct commits that create a key. A stage
of a locked commit that the rule rejects gets `LeafFull`, asks for a capacity
split, and waits with the bounded wait for capacity, as a create does today. A
direct commit whose stage the rule rejects falls back to a locked commit
(ADR-061), and that commit follows the same rule.

The second condition is the check that ADR-032 already makes for creates. It
uses only shared settings. So on every database instance, a stage that adds
entries leaves the split headroom free.

One stage can take a leaf over the create limit. After that stage, the leaf
admits no stage that adds entries until a split makes it smaller. The capacity
split divides the leaf at the median of its entries, and these entries now
include the keys of the large stage.

The largest stage limits one stage, not one transaction identity. A
transaction can lock keys of one leaf in more than one commit pass. A later
stage in a leaf over the create limit gets `LeafFull`, and a split can divide
it, because the keys of the earlier stage are entries in the leaf.

### The create limit stays local

The create limit comes from local transaction limits, so two database instances
can have different create limits. A difference changes only when the stages
that add entries in a leaf start to wait for a split. One instance can reject a
create that another instance admits. Then the instance that rejected the
create asks for a capacity split, and the create fits after the split.
Different soft caps have the same kind of cost.

The create limit is never below the byte soft cap, so that ordinary leaves
keep their creates when the local limits are too large for the hard cap. On
such a database instance, a transaction can make a stage that never fits. The
leaf coordinator then rejects the stage with `InvalidInput`.

### A transaction limits the key bytes of its point accesses

A new limit in `TransactionLimits` caps the total logical-key bytes of the
point accesses of one body execution. It counts reads, writes, and deletes,
because each of them can add an entry with its key. With this limit, a
transaction can use large keys, but it can access fewer of them. The largest
stage comes from this limit and from the operation limit.

## Consequences

- A transaction within the default limits can create all its keys in one leaf,
  also when the leaf is empty or no existing key is between the new keys.
- The first transaction in a new collection can create many keys, because the
  root leaf of the collection follows the same rule.
- A leaf can be several times larger than before. Until its splits finish, every
  read, CAS, and write-back of the leaf moves the large object. The leaf needs
  several splits, one after another, to get below the create limit. This cost
  occurs only after a large stage.
- In an overfilled leaf, the stages of other transactions that add entries wait
  for these splits. The bounded wait for capacity still applies to them.
- The reads and deletes of absent keys no longer use the split headroom. They
  can now get `LeafFull` and wait for a split.
- `TransactionLimits` has one more limit. The database metadata gets no new
  setting.

## Alternatives considered

### Split the leaf before the transaction

The `LeafFull` result could carry a split key from the rejected stage. Then the
restructurer could split at that key until each part fits, with no change to
the hard cap. But the new leaves are underfull, and a merge can join them again
before the transaction locks them. This occurs today with capacity splits. Also,
the tree of a new collection does not exist before the commit, so the
restructurer cannot split it.

### Persist the create limit

A shared create limit would give every database instance the same point at
which creates wait. But the check of the content limit after the stage already
keeps the split headroom free. A shared create limit adds a setting and no
safety.

### Use the soft cap as the create limit

A leaf within its soft caps could admit one large stage. But a leaf can be over
its soft caps while its soft cap split waits (ADR-084). Such a leaf would reject
the large stage, and its creates would force capacity splits that the topology
policy chose to delay.

### Lower the key size limit

A smaller key size limit also makes the largest stage smaller. But it rejects
large keys in every transaction, also in small ones. A limit on the total key
bytes rejects only large transactions.
