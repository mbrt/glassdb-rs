# ADR-077: Scans decide key membership from the leaf

## Status

Proposed.

Refines [ADR-032](032-node-locking-and-coordinated-splits.md): a transaction
that removes the membership lock of a committed holder help-forwards that
holder, and a range scan does not read the transaction record of a holder
that holds no membership lock.

## Context

A range scan returns keys only, so it needs to know only which keys are in
the key membership. Today it resolves the effective writer of each key with a
foreign write lock or create lock. When the holder is new to the database
instance, this reads the transaction record of the holder from the backend.

Each commit of a writer makes a new holder. In the perfbench
`split-merge-fight` scenario on S3, all 8 workers of the scanning instance
reached each new holder at about the same time. They shared one backend read,
but each worker waited for it, about 27 ms. This was about 1 read and 6.6 to
7.3 waits for each commit of a writer. A scan spent 9 ms of 45 ms in these
waits with one leaf, and 41 ms of 119 ms with two leaves. An experiment that
did not read the records gave 57% to 60% more scans in both topologies. A
cache cannot remove this read, because the first read of each record is the
one that the workers wait for.

ADR-032 makes only creates and deletes take the membership lock of the leaf,
and a pending holder of this lock is a dependency that validation checks
again. So a holder that holds no membership lock overwrites its key, and the
scan does not need its transaction record. But a committed holder can lose
its membership lock before its write-back: a create or a direct commit in the
same leaf removes the membership lock of a committed holder, and writes back
only its own keys. The deleted key then stays in the leaf with its earlier
value and a write lock, as after an overwrite. Only the transaction record of
the holder shows the delete. The test
`a_scan_omits_a_committed_delete_whose_write_back_is_held` shows this.

## Decision

### A committed membership holder keeps its effect

A transaction that removes the membership lock of a committed holder from a
leaf also help-forwards that holder in the same leaf CAS: it publishes the
committed state of each key of the leaf that the holder locked, and removes
its key locks. If it cannot help-forward the holder, it keeps the holder in
the membership lock. This applies to locked commits, direct commits, and
structural gates. A structural gate already help-forwards each holder of the
node.

The removal of an aborted or wounded holder does not change the key
membership, so it needs no help-forward.

After this, a write lock or create lock whose holder holds no membership lock
of its leaf never changes whether its key is in the key membership.

### A scan reads only the records of membership holders

A range scan decides from the leaf entry whether a key is in the key
membership when the holder of the key lock holds no membership lock of the
leaf. It resolves the effective writer only when the holder holds the
membership lock, or when the holder is the scanning transaction. This applies
to the body of the scan and to its validation.

Point reads do not change. They need the value of the key, so they still
resolve the effective writer.

## Consequences

- A range scan reads no transaction record for a holder that overwrites its
  key. It still reads the records of membership holders.
- A create or delete that removes the membership lock of a committed holder
  also writes back the entries of that holder in its leaf CAS. It adds no
  CAS.
- Each path that removes a membership holder must keep the help-forward. A
  later path that removes one without it makes scans miss creates and
  deletes, and only the regression tests show it.
- A leaf can no longer hold a committed holder that has no membership lock
  and whose write-back changes the key membership. Tests that build leaves by
  hand must give such a holder its membership lock.
- Preliminary benchmarks: range scans over keys that other instances
  overwrite get faster, and loads with point reads and writes only do not
  change. Faster scans can change the topology that the policy of ADR-076
  holds. Creates and deletes showed no cost above the noise.

## Alternatives considered

### Mark deletes in the key lock

A write lock could record that its holder deletes the key. A scan then knows
the effect of each holder from the leaf, and no path must help-forward. But
this changes the format of leaf entries, and ADR-017 chose one write lock for
overwrites and deletes. The membership lock already marks creates and
deletes.

### Treat a holder that is not in the cache as pending

The experiment did this in the body of a scan. It removed the waits, but its
result depends on what the cache of an instance holds, and it has the same
fault when the membership lock of a committed holder is gone.

### Let the workers share the next read

A worker that comes after a read started could wait for the next read of the
same record. This saves backend reads, but each worker still waits for one
read, so the scans do not get faster.
