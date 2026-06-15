# DST approach: pros, cons, and comparison

A focused assessment of GlassDB-rs's current deterministic simulation testing
(DST) approach, compared against `madsim`, `turmoil`, and `mad-turmoil`, and
graded against the five stated intents:

1. Easy to maintain (robust to changes in `tokio`, and minimal)
2. Easy to use
3. Fully deterministic and reproducible
4. Fuzz-guided exploration of edge cases
5. Efficient

Sources: ADR-008/010/011/012/013, `PORTING.md`, the `rt`/`exec`/`fault`
modules, and upstream docs for the alternatives.

## TL;DR

The current approach is a **minimal in-repo deterministic executor** (~600 LOC)
that redirects only `tokio::spawn` and `tokio::time` and reuses the rest of
`tokio` unchanged, with faults injected at the `Backend` trait instead of at a
simulated network. For _this_ system — a **library over object storage** where
clients coordinate only through the store and there is no peer-to-peer network —
it scores best on determinism, fuzz-guidability, and efficiency, at the cost of
owning a small bespoke executor and a `--cfg sim` seam.

| Criterion                             | Current (in-repo `DetExecutor`)                                                     | madsim                                                                 | turmoil                                                           | mad-turmoil                                                                 |
| ------------------------------------- | ----------------------------------------------------------------------------------- | ---------------------------------------------------------------------- | ----------------------------------------------------------------- | --------------------------------------------------------------------------- |
| Maintainable / minimal / tokio-robust | **Good** — tiny, owned, forkable; but bespoke executor + `tokio_unstable` coupling  | **Weak** — large dep, re-implements tokio, primary maintainer moved on | **Good** — tokio-org maintained, smaller; network-only            | **Partial** — small crate but global `libc` interposition + young 3rd-party |
| Easy to use                           | **Good** — `rt` seam + `--cfg sim`; faults are plain middleware; users unaffected   | **Partial** — `--cfg madsim` + tokio alias; cloud backends excluded    | **Weak (here)** — must shim socket I/O the engine doesn't have    | **Partial** — turmoil structuring + `main()` init incantation               |
| Fully deterministic & reproducible    | **Strong** — verified by byte-identical op-stream + corpus replay; all seams closed | **Good** — but leaks tokio's `select!`/`watch` RNG until tamed         | **Partial** — documented leaks (HashMap, `getrandom`, time) alone | **Strong** — closes turmoil's `libc` leaks; trace-diff meta-test            |
| Fuzz-guided edge-case exploration     | **Strong** — schedule-tape + fault-tape gradient + PCT depth bound                  | **Weak** — own seeded scheduler, not byte-guidable                     | **Weak** — own seeded scheduler, network-event sampling           | **Weak** — same as turmoil                                                  |
| Efficient                             | **Strong** — single thread, in-mem, virtual time, no net stack                      | **Partial** — full runtime + simulated net + RPC                       | **Partial** — per-host runtimes, tick stepping, net shims         | **Partial** — turmoil overhead + cheap overrides                            |

## The four approaches in one paragraph each

- **Current — in-repo `DetExecutor` (ADR-011).** A single-threaded executor with
  a pluggable `Scheduler` controls task **poll order at await points**. A `rt`
  seam redirects only `spawn`/`time`; `tokio::sync`, `tokio::select!`, and
  `tokio_util::CancellationToken` are reused as-is. Time is virtual, entropy is
  a seeded RNG, and tokio's own `select!` branch RNG is seeded via `RngSeed`.
  Two schedulers: a **schedule-tape** (libFuzzer bytes choose the next task) and
  **PCT** (randomized priorities + change points). Faults are a per-client
  `Backend` middleware (`FaultBackend`). Active only under `--cfg sim`;
  production is plain `tokio`.

- **madsim.** A "magical deterministic simulator" that **re-implements** the
  tokio runtime, timer, `tokio::sync`, _and_ a full simulated network stack with
  RPC and node lifecycle (`kill`/`restart`/`pause`). Activated by `--cfg madsim`
  with a `tokio = madsim-tokio` alias across every crate. This was GlassDB-rs's
  original substrate (ADR-008).

- **turmoil.** A tokio-team framework for **distributed-systems** testing: each
  host runs on its own current-thread, time-paused tokio runtime, stepped a
  fixed tick at a time; a seeded RNG drives a **simulated network** (latency,
  drop, hold, partition, crash/bounce) through shims mirroring `tokio::net`. It
  controls the _network_, not intra-host task interleaving, and is **not fully
  deterministic on its own** (HashMap, `getrandom`, and `std` time leak).

- **mad-turmoil.** A small crate that adds **madsim-style determinism to
  turmoil** by overriding `libc` symbols (`clock_gettime`, `getrandom`,
  `getentropy`) plus seeding `fastrand`, closing the leaks turmoil has alone.
  Sim-binary-only. It keeps turmoil's host/network model; it makes that model
  reproducible rather than adding interleaving control.

## Why the architecture matters here

GlassDB is a **stateless library over object storage**. Clients never talk to
each other; the only shared, contended resource is the store. That single fact
drives the whole comparison:

- The meaningful fault boundary is **one client's transport to the store**, which
  is exactly the `Backend` trait. The current `FaultBackend` injects delay,
  dropped-request, lost-ack, and sustained per-client outages right there — and,
  being plain middleware, it even runs under ordinary `#[tokio::test]`.
- A **simulated network is largely wasted** here. turmoil/mad-turmoil's core
  value (a network between hosts) has nothing to bite on; you'd be simulating the
  HTTP/socket layer to object storage purely as overhead. madsim had the same
  problem in reverse: ADR-008 had to **manufacture** a network (one DB per node,
  an RPC `NetBackend`) just to make faults meaningful.
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

- **madsim — Weak.** A large, deep dependency that re-implements the runtime,
  timer, and network. ADR-011 flags it explicitly: the primary maintainer has
  moved on, and a core testing substrate that is hard to fork if abandoned is a
  liability. It must also track tokio's evolving surface, and forces a tokio
  alias across the whole workspace (and excludes crates it can't build, e.g. the
  cloud SDKs).

- **turmoil — Good.** Maintained by the tokio org and far smaller than madsim, so
  it tracks tokio well. But it covers only the network dimension, so
  "maintainable" comes with "you still own determinism elsewhere."

- **mad-turmoil — Partial.** ~10 KB crate, but its mechanism is **global `libc`
  symbol interposition**, which is inherently platform-specific (Linux vs macOS
  differ) and sensitive to _how_ dependencies happen to fetch entropy/time (the
  `getrandom`/`rand` version churn is a live concern). It is young (2025) and
  third-party (S2), and pins `turmoil ^0.7`.

### 2. Easy to use

- **Current — Good.** Engine code calls `rt::{spawn, sleep, Instant, system_now}`
  instead of tokio directly — one seam to remember, enforced by a test. Tests
  run the _real_ engine suite under `--cfg sim`; library users see nothing.
  Faults are ordinary `Backend` middleware. _Cons:_ the seam is a standing
  discipline, and the sim build needs `--cfg sim --cfg tokio_unstable`.

- **madsim — Partial.** Requires `--cfg madsim`, the workspace-wide tokio alias,
  and excluding anything that can't compile against fake-tokio (the s3/gcs
  backends). Invasive at the build level.

- **turmoil — Weak (for this system).** Its usage model is "put your socket types
  behind a swappable `mod net` and write host/client futures." GlassDB has no
  sockets to swap — coordination is object-storage calls — so adopting turmoil
  means _inventing_ a network layer to simulate. High impedance mismatch.

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

- **turmoil — Partial.** Documented to leak (`HashMap` `RandomState`, `getrandom`,
  `std` time) unless the application buys in; reproducibility is best-effort
  without help. This is precisely why mad-turmoil exists.

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
  seed-breadth _random sampling_ of schedules/network events, not fuzzer-guided
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

- **turmoil / mad-turmoil — Partial.** Each host is its own tokio runtime stepped
  per tick, with network shims and message (de)serialization; for GlassDB you'd be
  paying to simulate HTTP-to-object-storage that the `Backend` fault middleware
  models for free. mad-turmoil's `libc` overrides themselves are cheap, but the
  turmoil substrate cost remains.

## Net assessment of the current approach

**Pros**

- Directly **controls task interleaving**, which is where the target bugs live,
  and is the only one of the four that makes that control **fuzzer-guidable**
  (schedule-tape + fault-tape) and **smartly sampled** (PCT).
- **Fully and verifiably deterministic** for its model (byte-identical op stream,
  corpus replay), including the tokio `select!`-RNG and HashMap leaks the others
  trip on.
- **Minimal and owned**: no heavy external simulation runtime, trivially forkable,
  production stays on stock tokio behind `--cfg sim`.
- **Efficient and well-matched** to a library-over-object-storage design; faults
  live at the right boundary (`Backend`) and also run under normal tokio tests.

**Cons / risks**

- A **bespoke executor** is a standing correctness dependency: it must stay
  faithful to the tokio semantics the engine relies on. This is mitigated, not
  removed, by the suite gate + seam guard.
- Couples to **`tokio_unstable`** (`RngSeed`) and a couple of `block_on`/`coop`
  implementation details — an unstable surface that could shift.
- A standing **`rt` seam discipline** (no direct `tokio::spawn`/`time`/wall-clock
  in engine paths), enforced by a source-level test.
- **Scoped determinism**: no real multi-thread data races, OS scheduling, real
  cloud-SDK behavior, or true network partitions; the schedule space is sampled,
  not exhausted.

**Where the alternatives would still win.** If GlassDB ever needed to test a
_real SDK client over a genuinely simulated network_ (peer hosts, partitions,
socket-level faults), turmoil — optionally hardened with mad-turmoil — is the
better-matched tool, and ADR-011 explicitly parks it as a future option for that
scenario. For the current objective (serializability under contention, faults,
and crashes against object storage), the in-repo executor is the stronger fit on
four of the five intents and competitive on the fifth (maintainability), with the
fidelity burden being the conscious trade.
