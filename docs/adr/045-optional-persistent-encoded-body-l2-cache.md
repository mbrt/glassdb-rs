# ADR-045: Optional persistent encoded-body L2 cache

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

Treat every index slot as an untrusted hint. Publication appends a
self-validating record before publishing its pointer. Before following a hint,
bound its offset and length; after reading it, verify a digest that binds the
cache identity, requested path, revision, and body. A partial record, torn
pointer, collision, corrupt body, or pointer into a reused segment is therefore
a bounded extra read or a miss. An older intact record yields only an older
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
size and minimum record charge are 4 KiB, and the segment size is 64 MiB. For
configured capacity `C`, rounded down to 4 KiB, derive:

```text
metadata_bytes = 8 KiB
index_bytes = floor_to_4_KiB(C / 64)
data_offset = metadata_bytes + index_bytes
segment_count = floor((C - data_offset) / 64 MiB)
```

A production file must contain at least two segments; 131 MiB is the smallest
whole-MiB capacity satisfying that constraint. Capacities of at least 512 MiB
remain the practical recommendation. The implementation may use an internal
test geometry, such as 256 KiB segments in a 2 MiB file, so rollover and crash
tests exercise the same algorithms without production-sized allocation. Such
files use a test-only format marker and are never accepted by the production
opener.

The file length is exactly `C`; an unused tail of less than one segment follows
the segment ring. The index occupies approximately 1/64 of the configured
capacity. Its 4 KiB buckets contain 128 slots of 32 bytes each. Since records
consume at least 4 KiB, the index has slightly more than two slots for every
record that can physically fit, keeping its maximum load below 50%.

The file contains these regions:

```text
0 KiB .. 4 KiB                         immutable file header
4 KiB .. 8 KiB                         clean-shutdown tail marker
8 KiB .. data_offset                   fixed hash index
data_offset .. end of complete segment fixed 64 MiB segment ring
remaining bytes                        unused tail
```

Changing the byte order, hash, sizes, geometry, or record encoding requires a
new format version. A v1 implementation may discard rather than migrate any
other version or a file whose length differs from the requested capacity.

#### Use an immutable header and clean-shutdown marker

Use SHA-256 for identity, lookup fingerprints, and record binding in v1. The
operator supplies a stable, non-empty cache identity for the backend and
database; the header stores its SHA-256 digest. Digest inputs have distinct
ASCII domain prefixes so one structure cannot be substituted for another.

The first 4 KiB has this layout:

```text
offset   size   field
0        8      magic = "GLDBL2\0\0"
8        8      format_version = 1
16       32     cache_identity_sha256
48       32     header_digest
80       4016   alignment padding, ignored by readers
```

The digest covers `glassdb-l2-header-v1`, bytes 0..48, and `C` encoded as a
little-endian `u64`. The header is written and synced when the file is created
but never updated. An invalid header, identity mismatch, or length mismatch
discards the cache. Padding is zeroed on creation but has no format semantics.

The second 4 KiB page records the append tail of the most recent clean
shutdown:

```text
offset   size   field
0        8      segment_generation
8        8      absolute_append_offset
16       32     marker_digest
48       4048   alignment padding, ignored by readers
```

The marker digest covers `glassdb-l2-clean-tail-v1`, the cache-identity digest,
`C` encoded as a little-endian `u64`, and marker bytes 0..16. A marker is usable
only when its generation is nonzero, is the greatest valid segment generation,
and its aligned offset lies after that segment's header and no later than its
end. A missing, torn, stale, or invalid marker is ignored. Padding is zeroed by
writers and has no format semantics.

The first 4 KiB of every segment is its header:

```text
offset   size   field
0        8      segment_generation
8        8      bitwise complement of segment_generation
16       4080   alignment padding, ignored by readers
```

A generation is valid when it is nonzero and its complement matches. Reusing a
segment writes a greater generation and its complement before writing new
records. A torn or invalid header makes all pointers into that segment stale.
The generation is a reclamation hint rather than a correctness boundary; the
record digest remains authoritative.

#### Use a fixed set-associative index

The index has `index_bytes / 4096` buckets. Compute a path fingerprint as the
first eight bytes, interpreted as a little-endian `u64`, of:

```text
SHA-256(
    "glassdb-l2-path-v1"
    || cache_identity_sha256
    || little_endian_u32(path.len())
    || path
)
```

The fingerprint modulo the bucket count selects a bucket. Each slot consists of
four little-endian `u64` values:

```text
offset   size   field
0        8      path_fingerprint
8        8      segment_generation
16       8      absolute_record_offset
24       8      record_bytes
```

Generation zero denotes an empty slot. A lookup reads one bucket and considers
slots with a matching fingerprint and current segment generation. Before any
record read it rejects an unaligned, undersized, oversized, outside-ring,
segment-header-overlapping, or cross-segment range. It then accepts a candidate
only if its encoded lengths reproduce `record_bytes` and its full digest
verifies against the requested path. Thus a fingerprint collision or torn slot
only causes a bounded extra read or an early eviction; it cannot return another
path.

Publication clears every occupied slot with the same fingerprint before
installing one new pointer; invalidation clears all such slots. A path fence is
released only after these writes complete. A fingerprint collision can
therefore evict another path but cannot preserve an older value for the changed
path. When installing a pointer, prefer an empty slot, then a slot whose
segment generation is stale. If none exists, replace the slot with the lowest
segment generation, breaking ties by slot position. Segment reuse remains the
normal FIFO eviction mechanism; this last case is reported as index-pressure
eviction.

#### Store exact, independently verifiable records

Records start at 8-byte boundaries after the segment header. Their fixed
40-byte header is:

```text
offset   size   field
0        4      revision_bytes
4        4      body_bytes
8        32     record_digest
```

The content is `revision || body`, followed by zero padding. `record_bytes` is
the larger of 4 KiB and the header plus content rounded up to eight bytes. A
reader recomputes that value from the two encoded lengths and requires it to
equal the index slot's value. V1 stores the backend body exactly and does not
compress it.

The record digest covers `glassdb-l2-record-v1`, the cache-identity digest, the
requested path length as a little-endian `u32`, the requested path, header bytes
0..8, and the unpadded revision and body. The path and revision use the
canonical byte encodings already used by the backend cache key and conditional
read. The path need not be stored because every lookup already supplies it and
the digest binds it to the record. This deliberately gives up rebuilding an
index by scanning records.

A record is admitted only if it fits completely between the segment header and
end of one segment: at most 67,104,768 bytes including its header and padding.
The encoded path, revision, and body lengths must each fit in a `u32`. Larger
records bypass L2. Padding is zeroed by writers and ignored by readers.

Append a complete record before writing its index slot. Clearing a slot or
publishing a newer pointer needs no tombstone: an unusable slot is a miss, while
an older intact record remains a permitted `Unverified` value.

#### Recover clean shutdowns exactly

While the cache is open, its active segment, append offset, and next generation
live in memory. On graceful shutdown, drain the worker, sync all preceding
segment, record, and index writes, write the clean-tail marker, and sync again.
A clean reopen resumes at that exact offset, preserving both every completed
cache entry and the unused capacity in the active segment.

On every open, validate the immutable header and read the 16 meaningful bytes
of every segment header. Ignore invalid generations and recover the next
generation as one greater than the maximum valid generation. Resume the active
segment only when the clean-tail marker validates against that maximum;
otherwise ignore it. Neither records nor the index are scanned.

After an unclean shutdown without a usable marker, initialize an unused segment
on the first admission if one exists; otherwise reuse the segment with the
lowest generation. Existing valid records remain readable, but the unused tail
of the previous active segment is abandoned until that segment is reused. A
crash can also lose unsynced records, and resuming from an older clean marker
may later overwrite records appended after that marker. These are cache losses,
not database losses.

During normal operation, sync dirty cache writes after either 64 MiB or five
seconds, whichever occurs first. These syncs do not update the clean marker and
foreground database operations do not wait for them. Generation increments are
checked; exhaustion discards or disables the cache rather than wrapping.

Better crash-tail recovery is future work, justified only if restart-heavy
benchmarks show material loss. A later format can add metadata checkpoints and
two alternating superblocks containing a checkpoint sequence, active segment
and generation, and append offset. It would sync data and index writes before
publishing the inactive superblock, then select the newest valid copy on
recovery. This would reduce crash losses without introducing a WAL or making
cache durability part of database correctness.

Keep the store behind a small internal interface so a purpose-built engine such
as [Foyer](https://github.com/foyer-rs/foyer) can replace it if measurements show
that the simple format cannot meet recovery-time or throughput targets.

### Admit reads and give old hot entries a bounded second chance

Do not admit bodies directly from successful mutations. An ordinary L1
capacity eviction does not affect L2. When L1 accepts knowledge that proves an
L2 value superseded--a mutation, deletion or missing result, or a backend read
with a different revision--fence the path, cancel older queued admissions and
promotions, and clear every matching index slot. The fence makes the old value
logically unreachable immediately; once the worker clears its pointer, its
bytes remain dead until segment reuse. This prevents a write-only or frequently
rewritten path from repeatedly filling L2.

Admit an encoded body only when a backend read returns that body and L1 accepts
it through its non-regression transition. If that read superseded an old L2
value, publish the accepted body under the same fence after clearing the old
pointer. An unchanged conditional read appends nothing. This read-driven policy
may require one backend fetch after a mutation or restart before the new value
becomes L2-resident, which is preferable to letting unproven write utility
displace the read working set.

Pure FIFO can still evict an old entry that remains useful. Approximate a
second-chance policy without per-entry timestamps or per-hit disk writes. Feed
present-value hits from both L1 and L2 into a process-local 4 MiB, two-hash,
two-bit saturating counting filter over path fingerprints. State zero is unseen,
the first hit advances it to seen, and the second advances it to emitted and
queues one promotion probe; further hits emit nothing in that filter epoch.

Clear the filter on open and after either 2^20 present-value hits or half of the
segment ring has been reinitialized, whichever occurs first. The worker
promotes a candidate only if its current L2 pointer is in the older half of
valid segment generations. Hash collisions can suppress or prematurely emit a
probe, but cannot affect the body ultimately returned.

Promotions are asynchronous and coalesced by path within the bounded worker
queue. Before reading or copying a record, the worker checks the age and token
budget, then verifies that the same generation and record offset are still the
published pointer and that no path fence is active; otherwise it drops the
request. A successful promotion appends the verified revision and body,
publishes the new pointer, and leaves the old bytes dead until segment reuse.

Bound promotion write amplification with a token bucket. Seven bytes of
ordinary read admission earn one promotion byte, tokens are capped at one
eighth of a segment's usable capacity, and a promotion must pay its full charged
record size. Tokens are process-local and start empty. Promotions consequently
cannot advance the ring without admission traffic and account for at most one
eighth of steady-state append bytes. If the filter, queue, or token budget
declines a promotion, retain the existing pointer and allow normal FIFO
eviction.

### Keep cache writes outside the foreground durability path

One bounded write-behind worker serializes admission, promotion, invalidation,
segment reuse, and syncs. Reads never synchronously sync cache data. Graceful
shutdown drains and syncs the worker; abrupt termination may lose the tail
without affecting database durability.

Before making a path-changing L1 transition visible or queueing its L2 work,
install a bounded in-memory fence that suppresses L2 lookup for that path until
the worker clears the old pointer or publishes the accepted read body. This
prevents delayed admission or promotion from resurrecting knowledge that the
current process already invalidated.

Queue saturation may drop an optional admission or promotion. If the worker or
fence cannot make a required supersession or invalidation visible, disable L2
for that open database and fall back to L1 plus the backend. After restart, an
older surviving candidate is safe under the `Unverified` rules.

### Fail open and expose degradation

L2 initialization and recovery do not gate database availability. An
unavailable, locked, full, corrupt, incompatible, or slow cache is bypassed or
discarded. Cache failures appear in tracing and statistics, not as database
operation errors.

Expose L1 and L2 hits and misses, bytes read and written, conditional
validations, read admissions, mutation invalidations, dead bytes, FIFO and
index-pressure evictions, second-chance filter hits, promotion attempts and
bytes, budget and queue drops, rejected admissions, corruption, discards,
errors, disablement, and physical allocation. The feature remains opt-in until
crash-injection tests and working-set benchmarks cover the target body sizes
and capacities.

## Consequences

- Large, low-churn working sets can use local SSD and retain cache warmth across
  restarts while object storage remains authoritative.
- Optimistic reads may avoid their initial object-storage fetch; strong and
  finite-staleness guarantees remain unchanged.
- Cache failures reduce performance but cannot alter data, recovery, or database
  availability.
- The fixed preallocated file gives an application-level byte ceiling without
  compaction or copy-on-write space amplification. Filesystem metadata and
  allocation granularity remain outside that ceiling.
- Read-driven admission prevents mutation churn from displacing the working
  set, at the cost of one backend fetch before a newly mutated value becomes
  L2-resident.
- Eviction is not exact LRU. A bounded two-hit second chance refreshes old hot
  entries, but filter resets, false positives, and promotion budgets can still
  produce early or unnecessary eviction and copying.
- Set associativity can evict entries early, and an L2 hit requires an index
  read plus a record read. Promotion adds at most one eighth of steady-state
  append traffic.
- Recovery does not scan the index or records. Clean shutdowns retain the exact
  append tail; an unclean shutdown can abandon one partial-segment tail and
  accelerate eviction.
- GlassDB owns a small disk format, allocator, and index. Weak failure semantics
  keep them narrow, but they still require adversarial crash and corruption
  testing.
- A directory cannot be repointed or shared concurrently. Deleted or superseded
  bodies may remain recoverable until segment reuse or wholesale discard.

## Alternatives considered

- **Protobuf for cache metadata and records:** reuses GlassDB's existing
  toolchain, but its variable-length messages still require a manual fixed-size
  envelope for page boundaries, random-access index slots, padding, and
  integrity checks. Cache upgrades may discard old files, and record bodies are
  already opaque encoded bytes, so schema evolution and another encoding layer
  provide little value.
- **Pure segment FIFO:** has no read-side bookkeeping, but write-only churn and
  scans can evict old entries that are still read frequently.
- **Exact LRU or persistent CLOCK bits:** requires per-entry heap state or
  random per-hit index writes, and still must copy a retained entry out of a
  segment before reclaiming that segment.
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
