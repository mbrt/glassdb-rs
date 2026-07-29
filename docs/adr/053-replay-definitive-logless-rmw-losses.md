# ADR-053: Replay definitive logless RMW losses

## Status

Accepted — implemented (the direct resolver's `Replay` / locked-fallback split,
the coordinator's separate same-key-exclusion outcome, and the removal of
ADR-027's parallel logged path).

This refines
[ADR-051](051-inline-latest-values.md)'s direct-commit fallback policy.
ADR-051 remains otherwise unchanged.

This supersedes
[ADR-027](027-single-rw-parallel-lock-publish.md)'s parallel logged
single-read-write path. Transactions that genuinely require logging use the
regular locked protocol from
[ADR-020](020-commit-write-back-protocol.md).

## Context

ADR-051 allows an eligible single read-modify-write transaction to commit by
publishing its value directly in a leaf. The shard mutation coordinator admits
at most one direct commit for a key in each CAS round, because allowing a later
same-key mutation into the same uncertain write would erase the earlier
transaction's recovery evidence.

The current policy sends another same-key member of that round to the logged
protocol. Under sustained contention, this is not a neutral fallback: it
publishes a holder that makes subsequent direct attempts ineligible. A single
local scheduling loss can therefore move an otherwise logless workload into a
long-lived cycle of locking, resolution, and write-back.

Focused one-key measurements found no corresponding rate of backend CAS
conflicts. The dominant trigger was local same-key exclusion, followed by many
direct-path rejections against the holder created by the fallback. Replaying
the excluded transaction body instead kept the workload logless and materially
improved both throughput and latency.

ADR-027 adds a third commit protocol between direct commit and the regular
locked path. It overlaps publication of a committed transaction object with
installation of its lock, but consequently needs distinct eligibility,
cancellation, orphan-recovery, and in-doubt rules.

Two paired runs with simulated S3 latency compared ADR-027 with going directly
to regular locking. When every small-value attempt landed directly, neither
latency nor throughput changed consistently. For 4 KiB values rejected by the
inline value limit, regular locking used one more backend operation per serial
transaction, increased one-worker median latency by `2.1–2.4x`, and reduced
eight-worker throughput by `33–35%`. A forced leaf-budget rejection increased
one-worker median latency by `1.66–1.73x` and reduced eight-worker throughput by
`11–12%`. Removing ADR-027 is therefore an explicit acceptance of slower
fallback transactions, not a claim that the two logged paths perform equally.

This exposes a distinction hidden by the existing fallback result. Some direct
attempts are known not to have staged any state and only need to reevaluate
their read against a newer version. Others are ineligible because coordination
or durable state requires the logged protocol. An unavailable write whose
outcome cannot be recovered is different from both.

## Decision

### Distinguish body replay from locked fallback

The direct protocol distinguishes four semantic results:

- the direct commit landed;
- no state was staged and the transaction body should be replayed;
- the transaction requires the regular locked protocol; or
- the outcome is in doubt.

These are protocol categories, not prescribed public types or names.

Body replay applies only to read-modify-write attempts for which the engine can
certify that this transaction staged no durable state. It covers a same-key
member excluded from a coordinator round and a direct attempt whose observed
version is definitively superseded before publication.

Replay uses the existing transaction retry contract. It reevaluates the body
against current state while retaining the same transaction ID. The ID remains
unengaged: replay does not create a transaction object, acquire a lock, or
publish any other identity. No new contention backoff is introduced initially;
the coordinator's one-winner-per-key rounds already provide local progress.

The initial scope is read-modify-write transactions. A blind overwrite has no
read-dependent computation to reevaluate and proceeds to regular locking when
it does not commit directly.

### Use one locked fallback for genuine ineligibility

The regular locked protocol is the sole fallback when the direct attempt
encounters state that cannot be resolved by reevaluating its body, including:

- a live or unknown transaction holder;
- a structural gate or collection-deletion fence;
- a missing key or another unsupported transaction shape;
- an inline value or leaf that exceeds direct-admission limits; or
- exhaustion or routing conditions that do not certify the body-replay case.

Do not try ADR-027's parallel logged commit before regular locking. The fallback
acquires and validates through the same general protocol used by every other
read-write transaction, under the same transaction ID while it remains safe to
do so.

This boundary is deliberately narrow. Treating every failed direct attempt as
replay could spin indefinitely around a holder, gate, or admission limit and
would bypass the protocol responsible for resolving that condition.

### Preserve in-doubt semantics

A transaction whose own direct mutation may have reached the backend is never
downgraded to body replay. If recovery cannot prove either its exact commit
marker or the unchanged predicate, ADR-051's `InDoubt` result still applies.

A same-key request excluded before staging does not inherit uncertainty from a
different request that participated in the coordinator CAS. Its own lack of
durable effects is sufficient to replay it.

## Consequences

- Contended eligible read-modify-write transactions can remain on the one-CAS,
  logless path instead of creating a holder merely because they shared a local
  coordinator round.
- Transaction bodies may execute more often. This is already permitted by the
  transaction API's retry contract, and the measured trade-off is substantially
  less backend work and lower tail latency.
- There are two read-write commit protocols rather than three: direct commit
  and regular locking. The parallel committed-object/lock race, its orphaned
  committed objects, and its special recovery and cancellation cases disappear.
- A fallback single-key overwrite loses ADR-027's parallel publication. The
  measured penalty is substantial for workloads dominated by external values
  or exhausted leaf budgets, even though workloads with consistently landing
  direct commits are unaffected.
- Inline admission and direct-commit coverage become more important performance
  boundaries. Lowering the budgets can move traffic onto a meaningfully slower
  path and must be evaluated as such.
- The engine must classify non-landing reasons precisely. A classification that
  is too broad can livelock; one that is too narrow retains avoidable logged
  fallbacks.
- Cross-database contention still relies on backend conditional writes for
  arbitration. This decision removes a local phase transition rather than
  introducing distributed serialization.
- ADR-051's direct-commit in-doubt behavior and user-visible error contract do
  not change. ADR-027's separate ambiguous lock-install outcome disappears.
- Existing aggregate direct-commit metrics remain sufficient. Reason-specific
  counters used to validate this decision need not become permanent telemetry.

## Alternatives considered

### Send every non-landing attempt to the logged protocol

This is simple and guarantees a general path forward, but a transient local
collision creates durable coordination state that suppresses subsequent direct
commits. Measurements showed that this phase transition, rather than backend
CAS contention, dominated the regression.

### Replay every failed direct attempt

This avoids the logged phase transition but conflates a certified logless loss
with genuine ineligibility. A transaction could retry forever against a live
holder, structural gate, or stable admission limit.

### Decline the whole coordinator round on same-key contention

This avoids choosing a local winner, but gives up a safe direct commit and still
pushes useful work toward the slower path. Selecting one member and replaying
the others preserves one unit of progress per successful round.

### Retain ADR-027 as the direct path's first fallback

This preserves materially lower latency and higher throughput for single-key
transactions whose values cannot be inlined. It also retains a complete third
commit protocol, including a parallel committed-object/lock race and recovery
states that the regular locked path does not need. The direct path is the
optimized common case; accepting regular locked performance for the remainder
is preferred to keeping this separate protocol.

### Serialize contending transactions before reaching the backend

Local serialization would reduce collisions but add queueing and duplicate
coordination already provided by the shard mutation coordinator. It also cannot
serialize independent database instances. Conditional backend mutations remain
the appropriate cross-instance arbiter.

### Add a retry limit or contention backoff immediately

A limit eventually recreates the logged-protocol phase transition, while
backoff adds latency despite the coordinator already making local progress.
Either can be reconsidered if multi-instance benchmarks show starvation or
excessive retry amplification.
