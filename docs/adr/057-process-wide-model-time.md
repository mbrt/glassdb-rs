# ADR-057: Process-wide model time in the runtime seam

## Status

Proposed.

This extends the simulation-aware runtime seam from
[ADR-011](011-guided-interleaving-executor.md) and its enforcement in
[ADR-013](013-deterministic-scheduling-test-coverage.md). If accepted, it
replaces the separate `Clock` abstraction described by
[ADR-002](002-wound-wait-locking.md),
[ADR-021](021-wound-wait-leases-shard.md), and
[ADR-022](022-garbage-collection-mark-sweep.md) without changing their ordering,
lease, or retention decisions.

## Context

Synthetic performance runs compress backend latency so a useful sample takes
less wall time. The delay backend currently owns that compression: it scales
operation sleeps, per-object limiter windows, and per-prefix rates. The
benchmark separately expands measured durations back into production-equivalent
time.

That is incomplete when the database still waits production-sized intervals.
For example, compressing an S3 operation to one fifth of its wall duration while
leaving a 200 ms coordination retry unchanged makes that retry equivalent to one
second in the reported model. Scaling `RetryConfig` separately repairs the
known case, but every new timer, limiter, lease check, or elapsed-time decision
can silently reintroduce the mismatch.

The engine already routes monotonic time, sleeps, and timeouts through `rt` so
the deterministic executor can control them. Wall timestamps take a second
path through `Clock`, even though `rt` already supplies deterministic wall time
under simulation. Adding another accelerated clock would duplicate the runtime
seam and would require every database and simulated service to receive the same
instance.

Every database client in a scaled benchmark must use one time rate. A
process-wide setting enforces that requirement by construction. Not every
duration in the process belongs to the model, however: workload convergence and
the harness's drain deadline still need real watchdogs. The runtime seam must
therefore remain distinct from experiment-control wall time.

## Decision

### Configure model time once per process

`rt` owns one immutable model-time configuration. It is installed before the
first `rt` time observation or wait and cannot change afterward; production
defaults to a real-time rate of one.

The rate is defined in one direction: a speedup of `N` means that one second of
wall time advances model time by `N` seconds, and waiting for a model duration
`d` takes `d / N` wall time. `rt` validates and applies this conversion. Callers
do not invert or multiply the rate themselves.

The model-time configuration governs:

- monotonic instants and elapsed-time comparisons;
- sleeps and timeouts; and
- the wall timestamps returned for transaction identity, leases, and recovery
  horizons.

In real-time mode, wall timestamps continue to come from the system wall clock.
In accelerated mode, `rt` anchors the system wall time once and advances it from
scaled monotonic elapsed time. Under paused Tokio time and the deterministic
executor, an anchored wall epoch advances with their virtual monotonic clock.
Tokio need not virtualize `SystemTime` itself; `rt` derives it from that anchor.

Changing a rate after an instant or persisted timestamp exists would make
elapsed-time and lease comparisons incoherent, so mutable or scoped rate changes
are not supported. Running two model rates requires two processes.

### Make `rt` the only engine time source

Production engine code obtains wall and monotonic time from `rt` and performs
model-time waits through it. The separate `Clock` type and its constructor
plumbing are removed. Deterministic wall time becomes a runtime property rather
than a `DatabaseBuilder` option.

Durations remain expressed in nominal production time. Backend latency
profiles, request rates, retry configuration, lease and deadlock budgets, and
background protocol cadence contain no benchmark scale. Rate limiters measure
refill with model monotonic time, and retries pass their ordinary backoff
duration to `rt`.

Cache sequence-point ordering remains database-local and causal. Any
elapsed-time approximation built on that timeline, such as best-effort stale
reads, uses model monotonic time so a duration keeps its meaning under
acceleration.

Tests that need wall timestamps to follow paused Tokio time use the runtime's
anchored mode. Tests that need a particular old or future persisted timestamp
construct that timestamp relative to the runtime time they observe; they do not
install a different clock into an individual database. The deterministic
executor retains its fixed epoch, so transaction identities and persisted bytes
remain replayable.

### Keep experiment control on unscaled wall time

The benchmark harness deliberately uses an unscaled wall-time source for:

- measurement-window and convergence stopping conditions;
- split-settlement quiet periods and their maximum wait;
- cooldowns between runs; and
- harness deadlines that bound worker drain and database shutdown.

Latency samples and throughput denominators use model elapsed time, while the
decision to stop collecting them uses wall elapsed time. Counts such as backend
operations and retries need no conversion.

### Restrict acceleration to coherent synthetic processes

All database clients, simulated backends, and provider emulators in one scaled
process automatically share the runtime rate. A synthetic provider's
server-time observations and client-side retry timers must use the runtime seam
as well, or be disabled when they cannot be injected.

Real S3 and GCS runs use the real-time configuration. Acceleration is not a
distributed production option: separate processes cannot share an anchor, and
a scaled client clock cannot be compared safely with an unscaled provider
clock.

The runtime-seam check rejects model-time reads or waits that bypass `rt`.
Engine time has no unscaled exception. Benchmark and external lifecycle code
remain outside that model-time seam and use wall time explicitly. A newly added
engine delay therefore fails review or CI rather than silently distorting scaled
benchmarks.

## Consequences

- One process setting controls backend latency, rate limiting, coordination
  retries, liveness timing, background cadence, and reported model time.
- Every database in the process is consistent by construction; no builder or
  backend can accidentally receive a different rate.
- Delay profiles and retry policies become directly comparable between scaled
  and production-time runs because their values no longer change.
- The `Clock` type, deterministic-time builder option, and clock parameters in
  transaction components disappear.
- A process cannot mix real and accelerated databases or sweep several rates
  without restarting. Benchmark invocations already select one backend profile
  and rate, so this is accepted.
- The process has two intentional time domains: engine time is always model
  time, while the benchmark harness controls experiment duration in wall time.
- Acceleration does not remove host timer resolution or CPU distortion. Very
  small compressed sleeps may still be quantized, and a highly accelerated run
  may become CPU-bound. Production-timescale confirmation remains necessary for
  consequential conclusions.
- Default production behavior and all on-disk and backend formats are
  unchanged.

## Alternatives considered

### Inject one accelerated `Clock` everywhere

This follows the historical Go approach and can keep a synthetic environment
coherent when every component receives the same instance. In Rust it duplicates
the existing `rt` seam, adds clock parameters throughout the database and
backend assembly, and still permits accidental use of an unscaled `rt` timer.
The required time domain is process-wide, so dependency injection provides
flexibility that the benchmark must not use.

### Retain `Clock` only for wall timestamps

`rt` could own the rate while `Clock` remained as an injectable wall-time
source. Existing anchored tests would require less change, but production would
retain two time abstractions whose values must advance consistently. Anchoring
wall time to Tokio belongs naturally beside `rt::Instant`, and existing tests do
not require simultaneous database clients with independent clock offsets.

### Scale each timing configuration independently

The benchmark can scale backend options, retry configuration, protocol timing,
and reported samples separately. This is locally simple but has no completeness
property: every duration needs another conversion and every new timer can be
missed. The mismatch that motivated this decision is an example of that failure
mode.

### Scale only backend delays and normalize results

Normalization cannot repair behavior. An unscaled retry suppresses work for too
long and changes contention, batching, and the number of operations before the
benchmark ever measures their latency.

### Accelerate every timer in the process

Applying the scale to raw Tokio or system time would also accelerate
measurement windows, settlement detection, and safety deadlines. Keeping
benchmark control outside model-time `rt` operations preserves the necessary
boundary.

### Run every benchmark at production time

This is the simplest timing model and remains the confirmation tier. Using it
for every sweep makes affinity curves and repeated reference comparisons too
slow, so it does not replace a calibrated accelerated tier.
