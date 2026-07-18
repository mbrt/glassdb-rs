# ADR-042: Optional persistent encoded-body L2 cache

## Status

Proposed.

This extends [ADR-036](036-decoded-object-cache-with-bounded-freshness.md).
Its decoded LRU and currentness protocol remain the L1; this ADR adds a
disposable encoded-body tier beneath it. Database-local `LogicalTime` evidence
remains non-persistable.

## Context

The decoded LRU is bounded by process memory and starts empty when a database is
reopened. GlassDB targets values from 1 KiB to 1 MiB and databases that may be
much larger than memory. Large or infrequently changing objects are repeatedly
fetched from object storage after L1 eviction or process restart even when ample
local SSD is available.

The target deployment has one Linux process, tens to hundreds of gigabytes of
local SSD, and a stable cache-directory assignment to one backend and database.
The cache must survive restarts but remains best-effort: object storage is
authoritative, cache loss is acceptable, and deleted historical values may
linger. Encryption and secure erasure are not required.

A persistent body can avoid more than transferred bytes. `Requirement::Any`
may begin optimistic execution from arbitrarily old state because strong
operations validate observations before returning or committing. Preserving
that benefit across restart requires treating a disk body as unverified stale
state, not merely retaining it for a conditional backend read.

## Decision

### Add an opt-in L2 inside the cached-store boundary

Make the cached object store two-tiered:

```text
typed stores -> decoded L1 -> encoded-body L2 -> Backend
```

The L2 is disabled by default. Enabling it requires an explicit directory and
byte capacity. A directory has one process owner at a time and is assigned by
the operator to one backend and database. An exclusive lock prevents accidental
concurrent use; an operator-supplied identity prevents accidental reuse after
repointing.

The L2 stores present physical objects as their path, opaque backend revision,
and exact encoded body. It stores neither decoded values, negative results,
database-local freshness evidence, nor semantics interpreted above the codec
boundary. All physical object classes share the tier.

Each record has a collision-resistant binding over its format version, path,
revision, and body. Verify it before decoding or using the revision for a
conditional read. Otherwise, a damaged body paired with a still-current
revision could be incorrectly certified by an unchanged backend response.

### Treat a disk hit as unverified local knowledge

Extend the cache evidence model with an explicit `Unverified` state. It is not a
fabricated timestamp and cannot satisfy any `AtLeast` requirement.

On an L1 miss:

- `Any` may return an L2 body and install it in L1 as `Unverified`;
- `AtLeast(T)` may use its saved revision and body for a conditional backend
  read, but only a backend operation started after `T` can verify it; and
- an unchanged response promotes the saved body with that operation's start
  watermark, while a changed or missing response replaces or removes it.

Finite-staleness and strong reads therefore infer no freshness from persistence.
Optimistic execution and CAS loops may start from old state and self-correct
during validation or after a precondition failure. Typed stores may retain
stronger existing invariants, such as indefinitely serving immutable terminal
transaction objects.

### Use a cache-native fixed-capacity segment store

Do not use a general-purpose transactional database. Store immutable records in
a fixed-capacity ring of reusable segments and locate them through a fixed-size,
disk-resident, set-associative hash index.

Publication appends a self-validating record before publishing its
self-validating index pointer. Segment reuse changes the segment generation.
Consequently, a partial record, torn pointer, collision, corrupt body, or pointer
into a reused segment is a miss. An older intact pointer yields only an older
`Unverified` candidate. These permitted outcomes require no write-ahead log,
atomic multi-record transaction, MVCC, or compaction.

Reusing the oldest segment provides byte-bounded FIFO eviction without a
per-hit durable write or an in-memory index proportional to entry count. A
minimum capacity charge per record bounds index occupancy; a record larger than
one segment is not admitted.

#### Fix the v1 container geometry

Use one file named `l2.cache` and take a non-blocking exclusive `flock` on that
file for the lifetime of the database. Use positioned, buffered I/O rather than
`mmap` or direct I/O. Preallocate the complete file on creation. Failure to
preallocate or lock it disables L2 for that open database.

All integers are unsigned and little-endian. Sizes use binary units. The block
size is 4 KiB, the segment size is 64 MiB, and the minimum configured capacity
is 512 MiB. For configured capacity `C`, rounded down to 4 KiB, derive:

```text
superblock_region_bytes = 8 KiB
index_bytes = floor_to_4_KiB((C - superblock_region_bytes) / 32)
data_offset = superblock_region_bytes + index_bytes
segment_count = floor((C - data_offset) / 64 MiB)
file_bytes = data_offset + segment_count * 64 MiB
```

The unused tail is less than one segment, so `file_bytes` never exceeds `C`.
The index occupies approximately 1/32 of the configured capacity. Its 4 KiB
buckets contain 64 slots of 64 bytes each. Records consume at least 4 KiB even
when their content is smaller. The resulting index has slightly more than two
slots for every record that can physically fit in the segment ring, keeping its
maximum load below 50%.

The file contains these regions:

```text
0 KiB .. 4 KiB              superblock A
4 KiB .. 8 KiB              superblock B
8 KiB .. data_offset        fixed hash index
data_offset .. file_bytes   fixed 64 MiB segment ring
```

Changing the byte order, hash, sizes, geometry, or record encoding requires a
new format version. A v1 implementation may discard rather than migrate any
other version or any file whose stored geometry differs from the requested
capacity.

#### Make metadata self-validating

Use SHA-256 throughout v1. The operator supplies a stable, non-empty cache
identity for the backend and database; the superblock stores its SHA-256
digest. Digest inputs have distinct ASCII domain prefixes so one structure
cannot be substituted for another.

Each 4 KiB superblock has this exact layout:

```text
offset   size   field
0        8      magic = "GLDBL2\0\0"
8        4      format_version = 1
12       4      superblock_page_bytes = 4096
16       8      checkpoint_sequence
24       8      file_bytes
32       8      index_offset = 8192
40       8      index_bytes
48       8      data_offset
56       8      segment_bytes = 67108864
64       4      segment_count
68       4      bucket_bytes = 4096
72       4      slot_bytes = 64
76       4      slots_per_bucket = 64
80       4      minimum_record_bytes = 4096
84       4      active_segment
88       4      active_offset
92       4      reserved = 0
96       8      active_segment_generation
104      8      next_record_sequence
112      32     cache_identity_sha256
144      3920   reserved = 0
4064     32     SHA-256 digest
```

The digest covers the prefix `glassdb-l2-superblock-v1` followed by bytes
0..4064. Writers alternate the two pages; readers select the valid page with
the greatest checkpoint sequence.

The first 4 KiB of every segment is its header:

```text
offset   size   field
0        8      magic = "GL2SEG\0\0"
8        4      format_version = 1
12       4      segment_number
16       8      segment_generation
24       8      initialization_checkpoint_sequence
32       4032   reserved = 0
4064     32     SHA-256 digest
```

Its digest covers the prefix `glassdb-l2-segment-v1`, the cache-identity
digest, and bytes 0..4064. Reusing a segment writes a header with a different
generation before writing new records. A missing or invalid header makes all
slots pointing into that segment stale.

#### Use a fixed set-associative index

The index has `index_bytes / 4096` buckets. Compute a path fingerprint as the
first 16 bytes of:

```text
SHA-256("glassdb-l2-path-v1" || cache_identity_sha256 || path)
```

Interpret the first eight fingerprint bytes as a little-endian integer and
take it modulo the bucket count. Each slot has this exact layout:

```text
offset   size   field
0        16     path_fingerprint
16       8      segment_generation
24       8      record_sequence
32       4      segment_number
36       4      record_offset
40       4      record_bytes
44       4      flags; bit 0 means occupied, all other bits are zero
48       16     slot_tag
```

An empty slot is all zero. `slot_tag` is the first 16 bytes of:

```text
SHA-256("glassdb-l2-slot-v1" || cache_identity_sha256 || slot[0..48])
```

A lookup reads one bucket, rejects invalid tags and out-of-range pointers, and
tries matching fingerprints in descending record-sequence order. It accepts a
candidate only after checking the current segment generation and the full path
and digest in the record. Thus a 128-bit fingerprint collision only causes an
extra read or an early eviction; it cannot return another path.

Publication clears every occupied slot with the same fingerprint before
installing one new pointer; invalidation clears all such slots. A path fence is
released only after these writes complete. A 128-bit collision can therefore
evict another path but cannot preserve an older value for the changed path.
When installing a pointer, prefer an empty slot, then a slot whose segment
generation is stale. If none exists, replace the slot with the lowest record
sequence. Segment reuse remains the normal FIFO eviction mechanism; this last
case is reported as index-pressure eviction.

#### Store exact, independently verifiable records

Records start at 8-byte boundaries after the segment header. Their fixed
96-byte header is:

```text
offset   size   field
0        8      magic = "GL2REC\0\0"
8        2      record_version = 1
10       2      header_bytes = 96
12       4      record_bytes
16       8      segment_generation
24       8      record_sequence
32       4      path_bytes
36       4      revision_bytes
40       8      body_bytes
48       4      flags = 0
52       4      reserved = 0
56       8      content_bytes
64       32     SHA-256 digest
```

The content is `path || revision || body`, followed by zero padding.
`content_bytes` must equal the sum of its three lengths. `record_bytes` is the
larger of 4 KiB and the header plus content rounded up to eight bytes. V1 stores
the backend body exactly and does not compress it.

The record digest covers `glassdb-l2-record-v1`, the cache-identity digest,
header bytes 0..64, and the unpadded content. The path and revision use the
canonical byte encodings already used by the backend cache key and conditional
read. A record is admitted only if it fits completely between the segment
header and end of one segment: at most 67,104,768 bytes including its header
and padding. Larger records bypass L2.

Append a complete record before writing its index slot. Clearing a slot or
publishing a newer pointer needs no tombstone: a torn slot fails its tag, while
an older intact record remains a permitted `Unverified` value.

#### Bound checkpoint and recovery work

Checkpoint after either 64 MiB of appended records or five seconds with dirty
state, whichever occurs first, and on graceful shutdown. A checkpoint first
calls `fdatasync` for preceding record, segment-header, and index writes, writes
the alternate superblock with an incremented sequence, then calls `fdatasync`
again. Foreground database operations do not wait for either sync.

On open, validate both superblocks and choose the newest valid one. Then read
only the 4 KiB header of each segment and retain its generation in memory. This
costs one page of startup I/O and eight bytes of heap per 64 MiB of cache data;
neither the records nor the index are scanned. Index slots or records persisted
after the selected checkpoint may be used if they validate, or may be
overwritten as tail data. Either outcome is safe because every recovered value
is `Unverified`.

Checkpoint, segment-generation, and record-sequence counters use checked
increments. Exhaustion discards or disables the cache rather than wrapping a
counter.

Keep the store behind a small internal interface so a purpose-built engine such
as [Foyer](https://github.com/foyer-rs/foyer) can replace it if measurements show
that the simple format cannot meet recovery-time or throughput targets.

### Keep publication outside the foreground durability path

One bounded write-behind worker serializes publication, invalidation, segment
reuse, and checkpoints. Reads never synchronously sync cache data. Graceful
shutdown drains and checkpoints the worker; abrupt termination may lose the
tail without affecting database durability.

Only backend results accepted by L1's non-regression transition are eligible
for publication. Before queueing a path-changing operation, install a bounded
in-memory fence that suppresses L2 lookup for that path until the worker makes
the operation visible. This prevents delayed publication from resurrecting
knowledge that the current process already invalidated.

If the queue, worker, or fence cannot uphold these rules, disable L2 for that
open database and fall back to L1 plus the backend. After restart, an older
surviving candidate is safe under the `Unverified` rules.

### Fail open and expose degradation

L2 initialization and recovery do not gate database availability. An
unavailable, locked, full, corrupt, incompatible, or slow cache is bypassed or
discarded. Cache failures appear in tracing and statistics, not as database
operation errors.

Expose L1 and L2 hits and misses, bytes read and written, conditional
validations, FIFO and index-pressure evictions, rejected admissions, corruption,
discards, errors, disablement, and physical allocation. The feature remains
opt-in until crash-injection tests and working-set benchmarks cover the target
body sizes and capacities.

## Consequences

- Large, low-churn working sets can use local SSD and retain cache warmth across
  restarts while object storage remains authoritative.
- Optimistic reads may avoid their initial object-storage fetch; strong and
  finite-staleness guarantees remain unchanged.
- Cache failures reduce performance but cannot alter data, recovery, or database
  availability.
- Fixed files give an application-level byte ceiling without compaction or
  copy-on-write space amplification. Filesystem metadata and allocation
  granularity remain outside that ceiling.
- FIFO is deliberately weaker than LRU, set associativity can evict entries
  early, and an L2 hit requires an index read plus a record read.
- GlassDB owns a small disk format, allocator, and index. Weak failure semantics
  keep them narrow, but they still require adversarial crash and corruption
  testing.
- A directory cannot be repointed or shared concurrently. Deleted or superseded
  bodies may remain recoverable until segment reuse or wholesale discard.

## Alternatives considered

- **`redb`:** dependency-light and operationally mature, but its copy-on-write
  B-tree, transactions, and MVCC solve guarantees this disposable single-writer
  cache does not need. Eviction would remain GlassDB logic.
- **Foyer:** purpose-built for large hybrid caches, but initially brings a
  broader memory-cache, runtime, compression, and I/O stack that overlaps
  GlassDB's correctness-aware L1 and worker model.
- **One file per entry or `cacache`:** simple atomic publication, but no suitable
  byte-bounded eviction and unacceptable inode and reconciliation costs near the
  1 KiB end of a hundred-gigabyte cache.
- **An LSM tree:** transactions may be optional, but its journal, compaction, and
  background maintenance are more mechanism than segment FIFO while still
  requiring a separate live-entry eviction policy.
