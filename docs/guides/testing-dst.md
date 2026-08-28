# DST approach: pros, cons, and comparison

A focused assessment of GlassDB-rs's current deterministic simulation testing
(DST) approach, compared against `madsim`, `turmoil`, and `mad-turmoil`, and
graded against the five stated intents:

1. Easy to maintain (robust to changes in `tokio`, and minimal)
2. Easy to use
3. Fully deterministic and reproducible
4. Fuzz-guided exploration of edge cases
5. Efficient

Sources: ADR-008/010/011/012/013/048, the `rt`/`exec`/`fault` modules, and the
upstream documentation for
[`madsim`](https://github.com/madsim-rs/madsim),
[`turmoil`](https://github.com/tokio-rs/turmoil), and
[`mad-turmoil`](https://github.com/s2-streamstore/mad-turmoil). The Turmoil
assessment includes the filesystem simulation added in 0.7.1.

## TL;DR

The current approach is a **minimal in-repo deterministic executor** (~600 LOC)
that redirects runtime-dependent operations and reuses the runtime-independent
parts of `tokio`. Object-store faults are injected at the `Backend` trait rather
than through a simulated network. ADR-048 applies the same principle to the
optional disk cache through a narrow byte-level media model rather than a
general simulated filesystem. For _this_ system — a **library over object
storage** where clients coordinate only through the store and there is no
peer-to-peer network — it scores best on determinism, fuzz-guidability, and
efficiency. The full media-fault space is explored against `PersistentCache`;
`CachedStore` uses selected faults, and every existing database fuzz target
replays each input both without L2 and with L2 under basic media faults.
This costs ownership of a small executor, media model, and `--cfg sim` seam.

| Criterion                             | Current (in-repo `DetExecutor`)                                                        | madsim                                                                 | turmoil                                                                    | mad-turmoil                                                                 |
| ------------------------------------- | -------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | -------------------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Maintainable / minimal / tokio-robust | **Good** — small and owned; but bespoke executor/media + `tokio_unstable` coupling     | **Partial** — large dependency that re-implements Tokio's runtime      | **Good** — tokio-org maintained; filesystem surface is new and unstable   | **Partial** — small crate but global `libc` interposition + young 3rd-party |
| Easy to use                           | **Good** — distinct `exec`, `rt`, entropy, and media seams + `--cfg sim`; users unaffected | **Partial** — `--cfg madsim` + tokio alias; cloud backends excluded    | **Partial (here)** — useful filesystem shim, mismatched host/runtime model | **Partial** — turmoil structuring + `main()` init incantation               |
| Fully deterministic & reproducible    | **Strong** — verified by byte-identical op-stream + corpus replay; owned input streams | **Good** — but leaks tokio's `select!`/`watch` RNG until tamed         | **Partial** — seeded model does not close application entropy/time leaks   | **Strong** — closes turmoil's `libc` leaks; trace-diff meta-test            |
| Fuzz-guided edge-case exploration     | **Strong** — schedule/backend/media tapes + PCT depth bound                            | **Weak** — own seeded scheduler, not byte-guidable                     | **Weak** — own seeded scheduler and event sampling                         | **Weak** — same as turmoil                                                  |
| Efficient                             | **Strong** — single thread, narrow in-memory models, virtual time                      | **Partial** — full runtime + simulated net + RPC                       | **Partial** — per-host runtimes and general network/filesystem models      | **Partial** — turmoil overhead + cheap overrides                            |

## The four approaches in one paragraph each

- **Current — in-repo `DetExecutor` (ADR-011).** A single-threaded executor with
  a pluggable `Scheduler` controls task **poll order at await points**.
  `exec::{block_on_with, TapeScheduler, PctScheduler}` configures each run;
  `rt::{spawn, sleep, Instant, timeout}` provides services inside it, and the
  entropy facade selects the run's seeded stream. `tokio::sync`,
  `tokio::select!`, and `tokio_util::CancellationToken` are reused as-is. Time
  is virtual, entropy is a seeded RNG, and tokio's own `select!` branch RNG is
  seeded via `RngSeed`.
  Two schedulers provide a **schedule-tape** (libFuzzer bytes choose the next
  task) and **PCT** (randomized priorities + change points). `FaultBackend`
  consumes an independent backend-fault tape. ADR-048 adds a third,
  media-fault stream and a byte-level `SimMedia` for the optional disk cache.
  Active only under `--cfg sim`; production is plain `tokio` plus `FileMedia`.

- **madsim.** A "magical deterministic simulator" that **re-implements** the
  tokio runtime, timer, `tokio::sync`, _and_ a full simulated network stack with
  RPC and node lifecycle (`kill`/`restart`/`pause`). Activated by `--cfg madsim`
  with a `tokio = madsim-tokio` alias across every crate. This was GlassDB-rs's
  original substrate (ADR-008).

- **turmoil.** A tokio-team framework for **distributed-systems** testing: each
  host runs on its own current-thread, time-paused tokio runtime, stepped a
  fixed tick at a time. A seeded RNG drives a simulated network, and its
  unstable filesystem shims model pending and synchronized bytes,
  crash/bounce, and torn-write faults. It controls host events and I/O rather
  than intra-host task poll order, and does not close application entropy and
  host-time leaks by itself.

- **mad-turmoil.** A small crate that adds **madsim-style determinism to
  turmoil** by overriding `libc` symbols (`clock_gettime`, `getrandom`,
  `getentropy`) plus seeding `fastrand`, closing the leaks turmoil has alone.
  Sim-binary-only. It keeps turmoil's host/network/filesystem model; it makes
  that model reproducible rather than adding interleaving control.

## Why the architecture matters here

GlassDB coordinates through **object storage**. Clients never talk to each
other; the shared, contended correctness boundary is the store. The optional
disk cache adds disposable local persistence but no new coordination
authority. Those facts drive the comparison:

- The meaningful fault boundary is **one client's transport to the store**, which
  is exactly the `Backend` trait. The current `FaultBackend` injects delay,
  dropped-request, lost-ack, and sustained per-client outages right there — and,
  being plain middleware, it even runs under ordinary `#[tokio::test]`.
- A **simulated network is largely wasted** here. turmoil/mad-turmoil's core
  value (a network between hosts) has nothing to bite on; you'd be simulating the
  HTTP/socket layer to object storage purely as overhead. madsim had the same
  problem in reverse: ADR-008 had to **manufacture** a network (one DB per node,
  an RPC `NetBackend`) just to make faults meaningful.
- The disk cache is the important exception: a simulated durable byte store is
  directly useful. Turmoil's filesystem is relevant prior art, but its general
  host/filesystem model and durability policy are broader and different from
  the one exclusively owned cache container. ADR-048 keeps the existing guided
  scheduler and adds only that narrow media seam.
- Media durability and corruption are explored primarily at the
  `PersistentCache` boundary. Selected `CachedStore` simulations cover
  currentness and invalidation. Rather than maintain a separate full-database
  cache workload, each existing transaction fuzz target runs its decoded
  workload once cache-free and once with `SimMedia`; that paired run uses only
  delay and pre-effect error injection from its independent media tape. This
  preserves broad identity, timeline, lifecycle, and crash/reopen integration
  without multiplying those workloads by the complete media-fault space.
- The bugs the DST hunts live in the **order of shared-state accesses** (a write
  landing between another tx's read and validate). Catching those needs control
  of _task interleaving_, which only the current executor provides directly.

## Criterion-by-criterion

### 1. Easy to maintain (robust to tokio changes, minimal)

- **Current — Good.** ~600 lines of in-repo executor/scheduler/timer, zero
  external runtime dependencies, trivially forkable and auditable. It
  deliberately redirects only the two seams that need it (`spawn`, `time`) and
  reuses the _stable, runtime-agnostic_ part of tokio (`tokio::sync`) — the very
  surface the others spend most of their code re-implementing. ADR-013 adds a
  source-level guard (`runtime_seam.rs`) plus scheduler/executor unit tests so
  drift fails near its source.
  - _Cons:_ it is still a **bespoke async executor** that must stay faithful to
    tokio's semantics (spawn-from-task, `JoinHandle::abort` as drop-cancel,
    waker routing), and it leans on **`tokio_unstable`** (`RngSeed`) and the
    `coop::unconstrained` + current-thread `block_on` trick to seed the
    `select!` RNG. "Minimal in LOC" is true; "no fidelity burden" is not.

- **madsim — Partial.** A large, deep dependency that re-implements the runtime,
  timer, and network. Its broad compatibility surface must track Tokio and is
  materially harder to audit or fork than the in-repo executor. It also forces a
  Tokio alias across the whole workspace and excludes crates it cannot build,
  such as the cloud SDKs.

- **turmoil — Good.** Maintained by the tokio org and far smaller than madsim, so
  it tracks tokio well. Its filesystem support materially broadens its useful
  scope, but remains explicitly unstable and still leaves application
  determinism and poll-order exploration to the user.

- **mad-turmoil — Partial.** ~10 KB crate, but its mechanism is **global `libc`
  symbol interposition**, which is inherently platform-specific (Linux vs macOS
  differ) and sensitive to _how_ dependencies happen to fetch entropy/time (the
  `getrandom`/`rand` version churn is a live concern). It is young (2025) and
  third-party (S2), and pins `turmoil ^0.7`.

### 2. Easy to use

- **Current — Good.** Simulation harnesses call
  `exec::{block_on_with, TapeScheduler, PctScheduler}` to configure runs;
  engine code calls `rt::{spawn, sleep, Instant, system_now}` for in-run
  services, while entropy callers use the separate entropy facade. The runtime
  seam is enforced by a test. Tests run the _real_ engine suite under
  `--cfg sim`; library users see nothing. Faults are ordinary `Backend`
  middleware. _Cons:_ these seams are a standing discipline, and the sim build
  needs `--cfg sim --cfg tokio_unstable`.

- **madsim — Partial.** Requires `--cfg madsim`, the workspace-wide tokio alias,
  and excluding anything that can't compile against fake-tokio (the s3/gcs
  backends). Invasive at the build level.

- **turmoil — Partial (for this system).** The full framework asks applications
  to run as host/client futures and swap in its I/O shims. Its network remains
  unnecessary for GlassDB, but its positioned-I/O filesystem shim is close
  enough to the disk cache to be useful prior art or a future independent
  adapter. Adopting the full runtime would still replace the guided scheduler,
  while using only the filesystem would require adapting its unstable
  durability and crash controls to GlassDB's media-fault tape.

- **mad-turmoil — Partial.** Inherits turmoil's structuring requirement and adds a
  `main()` init incantation (`set_rng`, `fastrand::seed`, `SimClocksGuard`) that
  must be sim-binary-only. Reasonable for a network service; awkward for a
  library.

### 3. Fully deterministic and reproducible

- **Current — Strong.** Scheduling, time, entropy, _and_ tokio's `select!` branch
  RNG are all pure functions of the input; the HashMap-iteration leak is
  neutralized by path-sorting at the four commit-path sites. Crucially this is
  **verified**, not asserted: `RecordingBackend` checks two same-tape/seed runs
  emit a **byte-identical backend-op stream** (with and without faults, tape and
  PCT), and the committed corpus replays twice and diffs (ADR-008/011/013). A
  failing schedule reproduces exactly from its libFuzzer input.
  - _Honest caveat:_ determinism is **scoped** to this model. By design it does
    not expose real multi-threaded data races, OS scheduling, real cloud-SDK
    behavior, or network partitions outside the `Backend` fault model (ADR-013
    "residual limits").

- **madsim — Good, with a catch GlassDB hit firsthand.** It deterministically
  controls spawn/time/sync/net, but does **not** seed tokio's thread-local
  `select!`/`watch` RNG — ADR-008 §5 found this was the _dominant_ non-determinism
  source once clients talked over the simulated network, and had to fix it with
  `biased` selects and `Notify`-based cancellation. So madsim alone is not "fully
  deterministic" for code that uses non-biased `select!` or `watch`.

- **turmoil — Partial.** A fixed builder seed controls Turmoil's own choices, but
  application `HashMap` randomness, `getrandom`, and host time still require
  discipline or interposition. This is precisely the gap mad-turmoil targets.

- **mad-turmoil — Strong (for its model).** Closes turmoil's `libc`-level leaks;
  S2 reports a CI meta-test that reruns a seed and diffs TRACE logs "down to the
  last bytes on the wire." But note the _granularity_: it makes whatever tokio's
  single-thread scheduler and the network sim produce **reproducible** — it does
  not **control** task interleaving, so the _space explored_ is whatever that
  stack happens to generate.

### 4. Fuzz-guided exploration of edge cases

This is the current approach's clearest win, and the reason it exists.

- **Current — Strong.** ADR-010 established that coverage-guidance has _no
  gradient over schedules_ (`seed → schedule` is chaotic; edge coverage is blind
  to interleaving). ADR-011's answer makes the **interleaving itself** a byte
  string libFuzzer mutates locally — `tape[pos] % ready.len()` chooses the next
  task — so a byte flip is a single, local scheduling perturbation (a real
  gradient). A second **fault tape** extends the same gradient to the fault
  schedule (which ops delay/drop/lose-ack, when clients crash, when outages open).
  **PCT** complements it with a principled seed-breadth sweep that has a provable
  lower bound on catching depth-`d` bugs. All replay byte-for-byte.

- **madsim / turmoil / mad-turmoil — Weak.** All three **seed their own
  schedulers** and expose no "consume _these_ bytes to pick the next task" hook
  (ADR-011 calls this out as a primary reason to build in-repo). They give
  seed-breadth _random sampling_ of schedules and I/O events, not fuzzer-guided
  interleaving search, and none offer a PCT-style depth guarantee out of the box.
  Bending them to a fuzzer tape would mean re-plumbing their scheduler, defeating
  the point of adopting a blessed substrate.

### 5. Efficient

- **Current — Strong.** Single-threaded, in-process, in-memory backend, virtual
  time (sleeps are free; the clock jumps to the next timer only when nothing is
  runnable), and **no network stack, sockets, or serialization** on the hot path.
  A deterministic step-budget catches livelock instead of hanging. This is the
  cheapest of the four _for this engine_, and per-run cheapness is what lets the
  FoundationDB-style "many seeds, long runs" model pay off.

- **madsim — Partial.** A full runtime plus a simulated network and RPC layer; the
  ADR-008 topology added per-op RPC and (de)serialization across simulated links.

- **turmoil / mad-turmoil — Partial.** Each host is its own Tokio runtime stepped
  per tick. The full framework's network and general filesystem are more work
  than GlassDB's `Backend` middleware and single-container `SimMedia`.
  `mad-turmoil`'s `libc` overrides themselves are cheap, but the Turmoil
  substrate cost remains.

## Net assessment of the current approach

**Pros**

- Directly **controls task interleaving**, which is where the target bugs live,
  and is the only one of the four that makes that control **fuzzer-guidable**
  (schedule, backend-fault, and media-fault streams) and **smartly sampled**
  (PCT).
- **Fully and verifiably deterministic** for its model (byte-identical op stream,
  corpus replay), including the tokio `select!`-RNG and HashMap leaks the others
  trip on.
- **Minimal and owned**: no heavy external simulation runtime, trivially forkable,
  production stays on stock tokio behind `--cfg sim`.
- **Efficient and well-matched** to the actual boundaries: object-store faults
  live at `Backend`, while persistent-cache faults live at the narrow
  `CacheMedia` seam. Full media faults run against the isolated cache; broader
  layers retain only the profiles needed for their integration invariants.

**Cons / risks**

- A **bespoke executor** is a standing correctness dependency: it must stay
  faithful to the tokio semantics the engine relies on. This is mitigated, not
  removed, by the suite gate + seam guard.
- Couples to **`tokio_unstable`** (`RngSeed`) and a couple of `block_on`/`coop`
  implementation details — an unstable surface that could shift.
- A standing **`rt` seam discipline** (no direct `tokio::spawn`/`time`/wall-clock
  in engine paths), enforced by a source-level test.
- The byte-level media model is another owned correctness dependency and cannot
  reproduce platform-specific filesystem allocation, locking, or kernel
  writeback behavior. Real-filesystem tests remain necessary.
- **Scoped determinism**: no real multi-thread data races, OS scheduling, real
  cloud-SDK behavior, or true network partitions; the schedule space is sampled,
  not exhausted.

**Where the alternatives would still win.** If GlassDB needed a real SDK client
over a simulated network, or a broad POSIX-like filesystem shared by many files
and hosts, Turmoil — optionally hardened with mad-turmoil — would be the
better-matched tool. ADR-011 parks it as a future network option, and ADR-048
keeps its filesystem usable as prior art or a future independent adapter. For
the current objectives (guided transaction interleavings and deep
single-container cache recovery), the in-repo executor remains the stronger
fit, with ownership of the narrow media model as the conscious trade.
