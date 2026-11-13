# ADR-094: The persistent cache worker is a task over blocking-pool calls

## Status

Proposed.

Refines [ADR-048](048-simulation-aware-disk-cache-media.md). It replaces the
dedicated worker thread and the `rt::spawn_dedicated` primitive of that ADR.

## Context

[ADR-048](048-simulation-aware-disk-cache-media.md) runs the persistent cache
worker on its own operating-system thread. The thread exists for two reasons.
Blocking file calls must not run on the async runtime, and one thread bounds the
blocking work, so an overloaded cache skips L2 and does not queue blocking tasks
without limit.

The thread costs more than it gives:

- `rt::spawn_dedicated` is a second spawn primitive. It has its own join type,
  error type, and abort rules, and a simulation branch that must match the
  native one.
- The file calls are blocking calls inside `async fn`s. They are safe only
  because the whole engine runs inside `block_on` on that thread. Nothing in
  `FileMedia` says so.
- The worker is already one task that does one file operation at a time. That
  bounds the blocking work without a thread. A call on Tokio's blocking pool
  uses at most one pool thread for the cache.

Linux has no non-blocking file I/O that Tokio can poll. A task that reads or
writes the cache file must run the call on a thread that can block. The only
open choice is which thread.

## Decision

Run the cache worker as an ordinary `rt::spawn` task. It runs in the same way
under the native runtime and under the deterministic executor.

`FileMedia` runs each file call with `tokio::task::spawn_blocking`. It keeps the
file in an `Arc<File>` and clones the `Arc` into the closure. A call that the
caller abandons then still holds the file and its lock until the call returns.

The closures own their buffers. A read fills a new buffer and copies it to the
caller. A write copies the caller's slice first. The worker still does one
operation at a time, so the cache uses at most one blocking-pool thread.

Remove `rt::spawn_dedicated`, `DedicatedJoinHandle`, `DedicatedJoinError`, and
`SpawnError`. The native media stays the only code that calls the blocking pool.
The `CacheMedia` and `CacheFile` interfaces, the simulation media, and the
engine do not change.

## Consequences

- One spawn primitive remains. The cache worker has the same cancellation and
  join rules as other tasks.
- The cache engine now runs on the application's runtime. A lookup waits for a
  runtime worker to run the engine task and again after each file call. Another
  task that holds a worker without yielding delays the lookup by about the
  length of the slice that it holds, once for each wait.
- Each file call costs one thread hop and one buffer copy. A lookup does two
  calls: the index bucket, then the record.
- A shutdown that times out aborts the worker task. A file call that already
  runs on the blocking pool cannot be aborted and keeps the file lock until it
  returns. Open retries the lock until `open_timeout`, so a reopen of the same
  directory still works.
- The cache uses the blocking pool of the application's runtime. It needs one
  pool thread and never more.

The host measurements below come from a throwaway test, not from the
repository. They use a release build, a runtime of 4 worker threads, and 3 runs
of each design.

| | dedicated thread | blocking pool |
|---|---|---|
| `lookup` median, idle runtime | 6 to 10 µs | 24 to 27 µs |
| Publish 100 records, then one ordered `lookup` | 0.5 to 0.8 ms | 3 to 5 ms |
| `lookup` median, 4 workers busy with 2 ms CPU slices | 6 to 8 µs | 2.0 ms |
| Same, with 50 µs, 200 µs, and 500 µs slices | not measured | 0.15, 0.6, and 0.5 ms |

A lookup replaces an object-store read, which takes milliseconds or more. The
added cost is small next to that. The `lookup` timeout is 5 s, so a busy runtime
does not disable the cache. Writes are write-behind, so no caller waits for
them.

## Alternatives considered

- **Keep the dedicated thread.** It gives the best latency on a busy runtime. It
  keeps a second spawn primitive and the hidden blocking calls. The benefit
  shows only when other tasks hold runtime workers without yielding, and those
  tasks delay the application's network work in the same way.
- **Run a lookup in one blocking call.** It saves one thread hop. The lookup
  logic would then run synchronously in the closure, so the engine would have a
  synchronous path for native media and an asynchronous path for simulation, or
  the media seam would have to run synchronous closures and lose its
  suspension points inside reads. ADR-048 rejected two copies of the engine
  logic because they drift. The saving is one of three waits. Take this step
  only if measurements of the whole database show that L2 latency matters.
- **Let callers read the file directly.** It skips the engine task. A lookup
  would no longer be ordered with the queued writes of the same path.
- **Use io_uring.** It can cancel in-flight file I/O. It is Linux-only, it needs
  a different runtime, and it is a much larger change.
