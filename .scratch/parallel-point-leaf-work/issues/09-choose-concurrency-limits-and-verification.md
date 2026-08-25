# Choose concurrency limits and verification

Type: prototype
Status: resolved
Blocked by: 02, 05, 06, 07, 08

## Question

Which initial internal concurrency limit should each bounded phase use, can one value serve all phases, and what deterministic tests and benchmarks prove the agreed latency-wave, one-leaf, maximum-incomplete-future, all-input, stable-output, retained-lock retry, serial-fallback, and throughput contracts? Include foreign-holder waits that occupy bounded positions, a cached complete same-identity hold that adds no CAS, uncertain-CAS reconciliation, and renewed-identity entry into sorted serial acquisition. Use gated distinct-path tests plus 1, 2, 8, and 32-leaf measurements with warm and cold caches.

## Prototype

[Point-leaf concurrency limit and verification demo](../assets/concurrency-limit-verification-prototype.html)
proposes one common initial limit of sixteen. It selects the smallest limit at
the measured throughput plateau while allowing at most one added backend-wait
wave through 32 leaves. It also makes the bounded-wait, stable-result,
retained-lock retry, renewed-identity fallback, deterministic test, and
benchmark contracts concrete. Human review selected this direction.

The validated primary source is on local throwaway branch
`prototype/point-leaf-limit-16-throughput-knee`, commit `9a69dbeb`, at
`.scratch/parallel-point-leaf-work/assets/concurrency-limit-verification-prototype.html`.

## Answer

### Initial internal limits

Start every bounded point-leaf phase with the same private, nonzero limit of
16. Apply it independently to one invocation for one transaction; do not add a
database-wide semaphore or a public configuration option. The bounded units
are:

- distinct node-path loads during point-key routing;
- distinct observed leaf paths during physical point validation;
- routed leaf groups during logical point revalidation;
- complete combined leaf groups during normal lock acquisition; and
- original `LockedTx` groups during committed write-back.

The sorted serial lock fallback bypasses this limit. A foreign-holder or
transaction-status wait remains incomplete and consumes one position. A
write-back position owns its original group through all rerouting, and split
descendants run serially inside that position.

A transaction does not pay for unused positions. For example, a transaction
with eight independent leaves polls the same eight futures and issues the same
backend operations with limit 8 or limit 16. The higher limit changes only the
admission of additional inputs.

For `L` equal independent backend waits of duration `W`, limit `N` takes about
`ceil(L/N)` waves. Potential added phase latency relative to one all-at-once
wave is therefore about `(ceil(L/N) - 1) * W`. Limit 16 gives one wave through
16 leaves and two waves at 32 leaves. It is the smallest candidate in the
planned sweep that adds at most one backend-wait wave through the required
32-leaf range.
Limit 32 removes that last wave but doubles the maximum per-transaction burst.

### Deterministic verification

Use gates and counters instead of real elapsed time in correctness tests.

`glassdb-concurr` tests must cover zero and one direct inputs, limits smaller
than and larger than the input count, reverse completion, and value errors.
They must prove stable admission, no more than `N` incomplete futures, all
inputs run, outputs return in input order, and dropping the join drops admitted
and stored futures.

Use manually seeded, distinct paths for routing and validation tests. Prove
`ceil(L/16)` cold operation waves at the production value, shared-path
combination, B-link correction, stable routed leaf groups, input-aligned
physical results, and stable logical results and error selection. Cover exact
state combination and evidence propagation, different revisions on one path,
independent absence observations, and committed, not-written, deleted, pending,
unknown, aborted, and wounded holders. An exact `Installed` own-holder proof
can use the physical shortcut. An `Observed` proof, a mismatched proof, or a
foreign exclusive holder must use logical validation.

Normal lock tests must gate at least 17 distinct leaf operations and prove that
the seventeenth is not admitted while 16 are incomplete. A foreign-holder wait
must keep its position while other positions finish. Mixed `Locked`,
`Conflict`, `LeafFull`, and operational-error results must all run and select
the first non-`Locked` result in stable leaf-path order.

Retry tests must prove that completed `Conflict` and `LeafFull` passes keep
physical locks without a foreground release. A cached complete same-identity
hold returns `Observed` without a CAS. A partial hold runs one idempotent
complete-leaf CAS. When a CAS lands but its result is unavailable, the next
full-set retry recognizes the complete hold and adds no second CAS. A complete
pass builds exactly one proof per current group and carries no partial proof or
held-path state into retry.

Serial-transition tests must gate an old-identity conditional write after
dispatch and before result delivery. Both that timeout and a completed conflict
threshold must make the old `Wounded` status durable before identity renewal,
then force the renewed identity into sorted serial acquisition. A late old
write remains abort-side. Point and range transactions keep one execution of
the transaction body for the transition. Collection create or drop keeps the
existing wound-style replay of the transaction body.

Committed write-back tests must cover the 16-position outer bound, split
rerouting inside one position, structural deferral, local failure, all original
groups running, and stable superseded-transaction hints. Normal and simulation
builds must reproduce the same selected results and backend operation streams
for the same seed and schedule. Every phase must also have zero-input and
one-input regression coverage, and the one-input path must add no backend
operation.

### Limit calibration and performance gates

Use a benchmark-only internal setting to sweep limits 8, 16, and 32. Do not
expose the sweep through `DatabaseBuilder`.

Build exact 1, 2, 8, and 32-leaf fixtures with one pre-created key in each of
that many collections. Each collection root is one distinct leaf, so split
timing cannot change the fixture. Measure these regular-protocol workloads with
inline publication disabled:

- point-read transactions for routing and point validation;
- logged blind overwrites for normal lock acquisition and committed write-back;
  and
- logged point read-modify-write transactions for the complete phase sequence.

Run every leaf count with a primed warm cache and with decoded-cache capacity
set to zero. Use memory, simulated GCS, and simulated S3 backends, including
their provider request-rate limits. Measure foreground p50 and p95 latency,
transactions per second, backend reads and writes per transaction, retries,
lock calls, and maximum active backend operations. Also time a fixed transaction
batch followed by graceful shutdown, so committed write-back debt remains in
the throughput result.

Use the existing worker sweep with one and multiple `Database` clients to find
the throughput plateau. A per-transaction limit does not enforce a process-wide
or provider-wide bound, so a single-transaction measurement is not sufficient.
Use at least three converged, paired runs and their cross-run median.

Keep limit 16 only when its median throughput is at least 95% of the best 8,
16, or 32 result in every converged primary cell. Use 32 if it gives more than
5% additional throughput, or if the accepted workload requires its one-wave
32-leaf latency. If one phase reaches backend saturation earlier, give only
that phase a measured lower internal limit. Do not add public configuration.

The candidate must match the parent revision's backend operation sequence and
count for every one-leaf workload. Any repeatable throughput regression greater
than 5% against the parent revision rejects the design. Keep the existing
direct-commit benchmark groups as one-leaf regression gates. Run `make test-all`
after implementation and deterministic regressions are complete.
