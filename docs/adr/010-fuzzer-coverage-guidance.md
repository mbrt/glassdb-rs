# ADR-010: Coverage-guidance has limited value for the concurrency DST

## Status

Superseded by [ADR-011](011-guided-interleaving-executor.md)

## Context

The deterministic-simulation fuzzer ([ADR-008](008-deterministic-simulation-fuzzer.md))
is built as a `cargo-fuzz`/libFuzzer target. libFuzzer mutates a byte string,
which `arbitrary` splits into three parts (`fuzz/fuzz_targets/concurrent_tx.rs`):

```rust
let seed: u64 = u.arbitrary().unwrap_or(0);
let workload = Workload::arbitrary(&mut u).unwrap_or_default();
let faults = FaultConfig::arbitrary(&mut u).unwrap_or_default();
```

The implicit premise of choosing a *coverage-guided* engine (libFuzzer) over a
plain random seed loop is that its coverage feedback helps explore the state
space faster than random search. This ADR examines that premise for *this* target
and finds it largely does not hold, so that we set correct expectations and record
why a different mechanism would be needed to change the conclusion.

### How coverage-guided greybox fuzzing works

libFuzzer keeps a corpus, runs each input under SanitizerCoverage instrumentation,
and retains inputs that hit a *new* edge as "interesting", then mutates those.
The loop only beats random search when the input→coverage map has **locality**: a
small mutation of an interesting input is more likely than a random input to be
interesting again, so the search can hill-climb. It excels when valid structure
is *rare* in the input space (magic headers, length-prefix chains, parsers).

### The input has two levers with opposite guidability

- **Structured bytes** (`Workload`, `FaultConfig`) decide *what* runs: client
  count, op mix (`Rmw`/`MultiRmw`/`ReadOnly`), keys, and whether faults fire and
  how hard.
- **The scheduling seed** decides *when* things interleave: one `u64` expanded by
  the runtime PRNG into an entire deterministic schedule.

These respond to coverage feedback in opposite ways.

## Decision

Record the following assessment and adjust how we *use* and *talk about* the
fuzzer accordingly. This is a framing/expectations decision, not a code change to
the harness.

### 1. On the seed, coverage-guidance has no gradient

`seed → schedule` is chaotic by construction: a seed that *almost* triggers a rare
interleaving is not bit-close to one that does, so seed-byte mutations produce
uncorrelated schedules. Along the seed dimension libFuzzer degenerates to **uniform
random sampling of schedules**; the coverage signal is noise.

Underneath that sits a deeper mismatch: **edge/branch coverage is largely blind to
interleaving.** Two runs can hit identical edges yet order shared-state accesses
differently — one serializable, one a lost update. The bugs the DST hunts live in
the *order* of accesses (a write landing between another transaction's read and its
validate), which SanitizerCoverage does not represent. Coverage therefore optimizes
a metric nearly orthogonal to the property under test. Once the contention/error
branches (`Retry`, `Wounded`, `ValidateRetry`, recovery, lock-lease expiry,
dedup-merge) are covered, further interleaving exploration yields no new coverage —
yet that is exactly where the remaining bugs are.

### 2. On the structured bytes, guidance works but the space is shallow

Here byte mutations do map to structured workload changes, and engine coverage
genuinely depends on workload shape, so coverage can reward useful structure. But
the value is capped: the structural space is tiny and almost-always-valid
(`KEY_COUNT = 4`, `MAX_CLIENTS = 4`, `MAX_OPS_PER_CLIENT = 8`, three op types in
[`crates/glassdb/src/sim.rs`](../../crates/glassdb/src/sim.rs)), and `arbitrary` is
total — there is no rare "valid structure" needle for coverage to discover. Random
generation saturates this coverage almost immediately, so guidance buys little over
random here.

### 3. The one real synergy

Because the levers are coupled in one input, coverage does provide a modest,
emergent benefit: when a `(workload, seed)` first reaches a contention-sensitive
branch, libFuzzer keeps it, and later mutations that preserve the
contention-inducing *workload* bytes while perturbing the *seed* bytes effectively
do "given a workload that induces conflicts, keep trying interleavings." Coverage
finds the productive structural *neighborhoods*; random seed search explores
interleavings *within* them. This is better than uniform random over the whole
space, but the interleaving search itself remains unguided and its ceiling is set
by the shallow structure plus interleaving-blind coverage.

### 4. How we therefore treat the fuzzer

For this target libFuzzer functions mainly as a sophisticated **random seed
generator with corpus management and crash/input minimization**, plus modest
structural-workload steering. The "coverage-guided" superpower is largely
neutralized for the concurrency objective. Accordingly:

- **Seed breadth is the primary bug-finding mechanism** — many seeds, long runs,
  parallel trials — not coverage hill-climbing. This is the FoundationDB model,
  which never used coverage-guidance: it relies on volume of randomized scenarios
  plus liberal in-code assertions, with determinism reserved for *reproduction*.
- **Keep `make fuzz` on `cargo-fuzz` for now**, valued for its corpus as a
  regression seed bank, crash + input minimization, and sanitizer integration —
  *not* for coverage-guided search. The libFuzzer-vs-plain-seed-loop choice is
  therefore weaker than it looks and remains revisitable (a seed-loop binary runs
  on stable, without nightly/sanitizer coupling).
- **Do not claim coverage "finds" interleavings** in docs or commit messages; it
  samples them.

## Consequences

- Expectations are set correctly: the DST's power comes from breadth of seeds and
  the strength of its invariants/assertions, not from coverage feedback. Effort is
  better spent on more assertions and more trials than on tuning the corpus.
- No immediate harness change; tooling stays as in ADR-008 (and as proposed for the
  turmoil migration, which keeps the libFuzzer driver).
- A tension is recorded: the main remaining justification for `cargo-fuzz` over an
  FDB-style seed-loop is its tooling, not guidance. If that tooling stops paying
  for its nightly/sanitizer cost, switching to a stable seed-loop is reasonable.

### Open items

A genuinely *guided* exploration of interleavings is out of scope here and taken
up in [ADR-011](011-guided-interleaving-executor.md), which adopts **both**
candidates below: a schedule-tape as the libFuzzer-driven primary policy and PCT
as the seed-breadth complement, on a minimal in-repo deterministic executor (so
the "redirect the scheduler to consume fuzzer bytes" obstacle noted below no
longer applies — the executor *is* in-repo). Two candidate directions:

- **Schedule-tape.** Replace `seed → PRNG → schedule` with a structured,
  byte-mutable decision source: at each scheduling point the harness consumes the
  next fuzzer byte to choose which ready task runs / how long to delay. Byte
  mutations then map *locally* to single scheduling perturbations, giving coverage
  a gradient over orderings. This generalizes the Go `ScheduledBackend`
  byte-sequence scheduler that ADR-008 set aside (which only perturbed backend
  timing). The hard part: madsim and turmoil both seed their *own* schedulers, so
  redirecting them to consume fuzzer bytes — rather than a PRNG — partly defeats
  the "use the blessed substrate as-is" goal.
- **PCT (Probabilistic Concurrency Testing).** Accept that the schedule is
  unguidable and sample it *smartly* instead: randomized task priorities with a
  few random priority-change points, giving a provable lower bound on catching
  depth-`d` bugs. Pairs naturally with the seed-breadth model rather than fighting
  it.
