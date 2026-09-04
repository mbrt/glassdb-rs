# ADR-048: Simulation-aware disk-cache media

## Status

Accepted.

This extends
[ADR-045](045-optional-persistent-encoded-body-l2-cache.md) and applies the
runtime seam from
[ADR-011](011-guided-interleaving-executor.md) to its persistent cache.

The native `spawn_dedicated` fallback in simulation builds is superseded by
[ADR-069](069-deterministic-only-simulation-builds.md).

## Context

ADR-045 places blocking positioned file I/O on a dedicated worker thread. That
is sound in production, but a native thread and real filesystem bypass the
deterministic executor. The cache is consequently disabled in simulation, so
the on-disk format, recovery scan, ordering fences, and reopened timeline are
absent from end-to-end crash testing.

Redirecting only thread creation is insufficient. A synchronous worker would
run to completion when scheduled by the simulator, leaving no controlled
interleaving or crash point inside media operations. A useful model must run
the same byte-level algorithms while allowing reads, writes, synchronization,
and failures to suspend deterministically.

The cache needs only one exclusively owned, preallocated, random-access
container. A general filesystem abstraction would expose substantially more
surface than the format uses.

## Decision

### Keep the cache engine execution-agnostic

Make cache initialization, recovery, and worker processing asynchronous. The
engine uses the GlassDB runtime seam for task execution and time and performs
all storage access through an internal media interface. It contains no
simulation branch, native-thread creation, direct filesystem access, blocking
queue wait, or host clock.

Add this runtime primitive:

```text
rt::spawn_dedicated(name, future)
    -> Result<DedicatedJoinHandle<future::Output>, SpawnError>
```

Its contract is:

- under production, start one named operating-system thread immediately and
  drive the supplied future there;
- under the active deterministic executor, schedule the future as an ordinary
  simulated task and create no operating-system thread;
- under a simulation build without an active deterministic executor, retain
  the production behavior so ordinary Tokio tests still exercise `FileMedia`;
- reserve one thread per call rather than use a blocking pool or impose a
  concurrency limit;
- detach the worker when its handle is dropped;
- make abort cooperative, dropping the future at its next suspension point;
  an in-progress production syscall remains uninterruptible and observes
  cancellation only after returning; and
- report completion, cancellation, and panic through the runtime's normalized
  join result.

This primitive accepts a future rather than a synchronous closure. That
difference from Tokio's `spawn_blocking` is what lets simulated media operations
be scheduling and cancellation points.

### Abstract one cache container, not a filesystem

Use an internal `CacheMedia` to open one exclusively owned cache container and
return a narrow `CacheFile`:

```text
CacheMedia::open_exclusive(directory) -> CacheFile

CacheFile:
    len
    set_len
    allocate
    read_exact_at
    write_all_at
    sync_data
    sync_all
```

Opening encompasses creation of the configured directory and `l2.cache`, plus
the non-blocking exclusive lock held for the returned handle's lifetime.
Dropping the handle releases the lock. Format validation, initialization,
index scanning, recovery, and all record logic remain in the shared cache
engine.

`FileMedia` implements this interface with the existing Linux filesystem
operations. Its async methods execute blocking calls only while polled on the
dedicated worker.

`SimMedia` models exactly one cache container. It is injected only through
internal simulation/test construction and is not a supported public extension
point or a general-purpose filesystem.

### Model bytes, durability, and media failure

`SimMedia` keeps the bytes visible to the running cache separately from the
last completed synchronization boundary. Successful writes are immediately
visible to later reads but are not thereby durable. A successfully completed
`sync_data` or `sync_all` makes all preceding effects within that operation's
scope durable.

A simulated process crash stops all of that process's cache work and releases
the container lock. On the next open, every byte and metadata effect after the
last completed synchronization may independently retain its old or new value.
There is no write atomicity or persistence-order guarantee: part of one write
may survive, and a later index write may survive while an earlier record write
does not. A synchronization interrupted before success provides no additional
guarantee.

The media model also provides deterministic outcomes for:

- explicit scheduling yields, bounded virtual latency, and an operation that
  remains pending until cancellation or media failure;
- I/O errors after no effect or after a partial write;
- detach, which invalidates open handles and resolves unsynchronized effects
  under the crash rule;
- reattach, which permits a fresh open but does not reactivate a cache already
  disabled for its current database session;
- permanent unavailability; and
- silent corruption of arbitrary durable bytes, independent of crash.

After any runtime media error, the open cache remains disabled until a fresh
open performs exclusive acquisition, validation, recovery, and sequence-point
scanning. Media faults remain performance failures: they must not become
database errors or permit an invalid body to escape cache validation.

### Separate cache exploration from database integration

Give media scheduling, persistence, latency, error, detach, and corruption
choices their own fuzzable input stream. Enabling L2 therefore does not consume
or shift backend-fault choices. Cache tasks still participate normally in the
global scheduling tape because their additional concurrency is real.

Use the same `SimMedia` with progressively narrower fault profiles at three
boundaries:

- exercise `PersistentCache` in isolation with the complete media model and
  small test geometry;
- exercise `CachedStore` with selected media faults to cover candidate
  validation, currentness, path fencing, invalidation, and fail-open behavior;
  and
- exercise a full `Database` only for cache identity, timeline initialization,
  crash/reopen, shutdown, and representative cache-disable scenarios.

The isolated cache harness carries the primary fuzzing burden. Its safety
oracle permits cache loss but rejects fabricated or mixed records, invalid
sequence-point recovery, out-of-bounds media access, and failure to disable.
Focused regressions cover corruption of each format region and crash/reopen at
the synchronization and publication boundaries.

Keep the existing cache-free database workloads. Do not combine the complete
media-fault model with broad transaction schedules and backend faults: that
cross-product spends exploration on semantically invisible cache misses and
reduces depth in every individual fault domain.

## Consequences

- Production retains blocking filesystem calls on one dedicated cache thread;
  it does not consume Tokio's blocking pool.
- Simulation executes the production format, allocation, recovery, and
  currentness algorithms rather than a record-level substitute.
- The strongest unsynchronized-write model validates the self-verifying format
  without relying on filesystem write atomicity or ordering.
- Timeouts, cancellation, detach, and stuck media become deterministic test
  cases.
- The runtime gains a precisely scoped long-lived-worker primitive, and the
  storage crate owns a small byte-level durability model.
- `SimMedia` is deliberately narrower than a filesystem and cannot validate
  platform-specific directory, allocation, locking, or kernel writeback
  behavior. Real-filesystem integration and crash tests remain necessary.
- Isolated cache simulations reach recovery and rollover boundaries more often
  and produce smaller counterexamples than full-database simulations.
- Targeted `CachedStore` and `Database` simulations retain cross-layer coverage
  without multiplying the complete cache, backend, and transaction fault
  spaces. Existing cache-free transaction simulations retain their depth and
  throughput.

## Alternatives considered

- **Continue disabling L2 in simulation:** avoids a media model but leaves the
  cache's format, recovery, and timeline integration outside the principal
  crash-testing environment.
- **Use a Tokio-style synchronous `spawn_blocking`:** a synchronous closure
  cannot suspend inside simulated media operations, and a long-lived cache
  worker is a poor fit for a shared blocking pool.
- **Maintain separate production and simulation workers:** keeps each
  implementation simple locally but duplicates the crash-sensitive state
  machine and permits their behavior to drift.
- **Enable every media fault in broad database simulations:** tests the largest
  composition directly, but makes cache loss mostly invisible to the database
  oracle and dilutes exploration across independent fault domains.
- **Adopt a general simulated filesystem:** provides a broader POSIX-like
  surface than this single-container cache needs. Turmoil's filesystem is
  useful prior art and a possible future independent adapter, but its runtime,
  seeded scheduling, unstable filesystem surface, and durability policy do not
  replace GlassDB's guided executor and chosen crash model.
- **Expose `CacheMedia` publicly:** would commit GlassDB to user-defined media
  semantics and unsupported storage environments for a seam presently needed
  only by production and deterministic testing.
