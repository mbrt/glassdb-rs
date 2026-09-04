# ADR-069: Deterministic-only simulation builds

## Status

Accepted

## Context

The `cfg(sim)` runtime adapters fell back to Tokio when no deterministic
executor was active. This let the normal test suite run again in a simulation
build, but those tests used Tokio and did not add deterministic scheduling
coverage. The fallback also required every simulated task and `SimWorkload`
future to be `Send`, although the deterministic executor is single-threaded.
This was unnecessary complexity, passed down to the Rust compiler when resolving
the constraints and a test runtime cost.

## Decision

A `cfg(sim)` build is a strict deterministic-execution mode. Its task, time,
dedicated-task, and entropy functions require an active deterministic executor
and do not fall back to Tokio, native threads, host time, or process entropy.
The executor accepts non-`Send` tasks, and `SimWorkload` has no `Send` or `Sync`
bounds. The `glassdb::sim` module is available only with both `cfg(sim)` and the
`sim` feature.

The normal test suite runs once without `cfg(sim)`. A separate lint pass
compiles all simulation branches. The simulation test run selects only tests
that start the deterministic executor; the GlassDB simulation suites form one
integration-test crate.

## Consequences

Production keeps the Tokio runtime interface and its `Send` requirements. The
simulation interface is smaller, matches its single-threaded implementation,
and does not need a higher compiler recursion limit. Code in a simulation build
that uses runtime functions outside `exec::block_on_with` now panics. A future
multi-threaded simulation executor would require new task bounds.

## Alternatives considered

- Increase the recursion limit. This keeps bounds that the deterministic
  executor does not need and makes later type growth harder to detect.
- Keep separate `SimWorkload` bounds for production and simulation builds. This
  exposes two contracts for a harness that is only useful in simulation.
- Keep the Tokio fallback and the duplicate test run. Most of that run does not
  use deterministic scheduling, time, or entropy.
