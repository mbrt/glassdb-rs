# ADR-078: Full GC and a GC write budget

## Status

Accepted — implemented.

[ADR-079](079-gc-safety-does-not-depend-on-time.md) renames the safety horizon
to retention period, and sets the rule for barriers that several GC checks
share.

Refines GC scheduling in
[ADR-070](070-demand-driven-garbage-collection.md). The reclamation rules of
[ADR-022](022-garbage-collection-mark-sweep.md),
[ADR-059](059-pin-foreign-wounds-until-owner-retirement.md), and
[ADR-071](071-gc-skips-pending-and-wounded-transactions.md) do not change.

## Context

ADR-070 keeps GC behind the load, and accepts that garbage and the cleanup
delay can grow without a limit during sustained overload. Two measurements show
the cost of this decision.

In the perfbench `mixed` scenario, a conditioning load of 120 s at 200 workers
per shape created about 2,800 transaction records in each second. After the
load, 328k records were left. The GC hint queues dropped 1.4 million hints, so
GC scans had to find most records. The 5 client instances and 4 extra instances
needed about 10.5 minutes to delete the records. After a 600 s load, GC did not
finish within 30 minutes. See the
[investigation](../../hack/perf/investigations.md#garbage-collection-after-a-load).

The measurements show two different problems.

**Benchmarks.** A warm-up must end with no garbage. If it does not, the measured
interval also does the GC work of the warm-up, and the result depends on how
much of that work is left. Faster background GC does not solve this problem,
because it also changes GC during the measured interval.

**Production.** GC must not slow down bursty workloads. But under sustained
load, garbage must eventually decrease. Today, fixed limits prevent this.

The limits come from slots that GC candidates hold for a long time, not from
GC parallelism. The 4 extra instances ran with a GC parallelism of 64, but GC
deleted only about 190 records in each second for the first 7 minutes. These
are the limits:

- A hinted candidate holds one of 4,096 slots for at least its safety horizon,
  45 s with the default timing. So the hint path can check at most about 91
  records in each second for each instance.
- A candidate from a GC scan holds one of 2,000 slots until its last check. A
  check that reclaims some effects but keeps the record holds the slot for
  15 s more. A record inside its safety horizon holds the slot until the
  horizon ends. A new LIST starts only when 1,000 slots are free.
- GC starts a new batch of checks only when the slowest check of the previous
  batch is complete.
- The scan cadence slows down when scans find no work that they can reclaim.
  It does not speed up while hints are dropped.

If GC deletes almost nothing, garbage grows by up to about 240 million records
each day at 2,800 records in each second.

GC competes with transactions in two ways: with backend reads and LISTs, and
with conditional mutations. The caches absorb most GC reads of transaction
records and index nodes. GC reads leaves after a fresh currentness barrier, so
these reads usually reach the backend. GC conditional mutations always reach
the backend, and its lock releases join coordinator rounds on the same leaves
as transactions (ADR-029).

## Decision

### A caller can start a full GC

Add the public method `Database::collect_garbage(parallelism)`. It runs one
full GC and returns when the full GC is complete. The name follows the GC
methods of other languages, such as `runtime.GC()` in Go.

A full GC lists all transaction records of the database once. It checks each
listed record with the same rules as background GC. The safety horizon still
applies, and GC still skips missing, pending, and wounded records. The
`parallelism` argument sets the maximum number of GC checks that run at the
same time. The hint queues, the candidate capacities, the scan cadence, and the
GC write budget below do not apply to a full GC.

A full GC processes one LIST page at a time. Its memory follows the page size
and `parallelism`, not the number of records in the database.

The method returns a report with these counts:

- deleted records;
- missing records, which another instance or background GC deleted first;
- records whose GC check reclaimed some effects, but kept the record;
- deferred records, which wait for their safety horizon, and the earliest time
  at which one of them becomes eligible;
- retained records, which other stored objects still name, or which are pending
  or wounded;
- failed GC checks.

A full GC does not wait for safety horizons. To delete deferred records, the
caller waits until the earliest eligible time and starts another full GC. If a
GC check reclaims some effects of a record, the next check can delete the
record. So a caller that wants no garbage repeats full GCs until the report has
no deferred records and no records with reclaimed effects.

A failed GC check does not stop a full GC, and the report counts it. A failed
LIST ends the full GC with an error. A full GC runs in the task of the caller.
If the caller drops the future, the full GC stops, also in the middle of a
check. A background GC check stops in the same way when the instance closes.
This is safe: each step of a check is idempotent, the delete of the record is
the last step, and a later check repeats the steps that remain.

A full GC does not stop transactions or wait for them. LIST is not a snapshot,
so a full GC can miss records that transactions create while it runs.
Background GC and full GCs in other instances can check the same records. This
duplicates work, but it is safe under the existing reclamation rules.

The perfbench `mixed` scenario starts full GCs after the seed and after each
warm-up, when no workers run, until the report has no deferred records and no
records with reclaimed effects. The cost of the full GCs is then outside the
measured interval.

### Background GC has a write budget

Background GC of each database instance has a write budget, which limits the
rate of its conditional mutations. The budget follows the conditional
mutations of the transactions of the instance, with a minimum rate for
instances with few transactions.

**Instance writes** are the conditional mutations that the instance sends to
the backend, counted by one cumulative counter in front of the backend. They
include the writes of transactions, of splits and merges, and lost CASes.

**GC writes** are an estimate that GC keeps from the outcomes of its checks:
one write for each deleted record, one for a check that reclaimed some effects,
and one for each deleted object of a collection. Writes of full GCs count too,
but they spend no budget of background GC.

GC earns budget from the instance writes minus its own estimated writes. This
needs no attribution of each mutation in the write path, which would change the
interfaces of the coordinator and of the stores. The budget limits a rate, so a
rough count is enough:

- Splits, merges, and lost CASes of transactions add budget. They mostly follow
  transaction load. Under heavy contention, GC can write a little more than β
  times the applied transaction writes.
- The estimate of GC writes undercounts a check that releases locks on many
  leaves. That check then earns a little budget from its own writes.

The budget has three values:

- **Earned tokens.** Each instance write that is not a GC write adds β tokens.
  Each GC write of background GC removes one token, but earned tokens never go below zero. Earned tokens that
  GC does not use decrease with the time constant τ_keep.
- **Spend limit.** `(earned tokens) / τ + base rate`, in GC writes in each
  second.
- **Allowance.** It increases at the spend limit, up to the GC writes of one
  second at the spend limit. Each GC write removes one. GC starts no check
  while the allowance is zero or less.

GC cannot know before a check how many writes the check makes. Each check that
starts reserves one write, so a refill cannot start more checks than the
allowance holds. A running check can still make the allowance negative, and GC
then starts no check until it refills. The concurrency does not grow while the
budget holds checks back. A dropped collection is deleted in one check, which
can make the allowance very negative. GC then pauses for as long as the base
rate needs to pay for it.

β is at most 1, and its default is 1. Writers do not wait for the budget. The
budget is local memory, and the rule of ADR-070 that writers do not wait for GC
stays the same.

The budget gives these properties:

- **Limit.** Each GC write while earned tokens are positive removes one
  earned token. While no earned tokens are left, GC writes are limited to
  the base rate. So over any interval, GC writes are at most about β times the
  other instance writes, plus the base rate times the interval, plus the earned
  tokens at its start. Running checks can add a small overshoot to this limit.
- **Sustained load.** At a sustained rate m of other instance writes, GC uses at
  most about the larger of β × m and the base rate.
- **Bursts.** During a burst that is short compared to τ, earned tokens and the
  spend limit increase only a little, so GC competes only a little with the
  burst. After the burst, GC uses the earned tokens. It uses most of them within
  about τ, if it knows about enough GC work.
- **Late discovery.** Earned tokens stay for about τ_keep, so GC can use them
  when scans find records late.
- **Instances with few transactions.** These instances use the base rate. So
  they can delete more garbage than they create, including the garbage of busy
  instances and of instances that crashed or closed.

A public option sets the base rate. A dedicated GC instance can set a large
base rate. Then it can compete with bursts of other instances, but the
operator chooses this with knowledge of the backend capacity. The default base
rate stays small, so that no instance competes at full speed without a choice
by the operator.

### Catch-up under sustained load

All instances together delete garbage faster than they create it when their
total GC rate is larger than the GC rate that new garbage needs:

`Σ max(β × m_i, b) > g`

m_i is the transaction mutation rate of instance i, b is the base rate, and g
is the rate of GC mutations that new garbage needs. Each term is in mutations
in each second.

g does not depend on GC checks that find nothing to do, because these checks
make no mutation. A deleted record needs one delete. A record of an aborted
transaction also needs the release of its locks. A dropped collection needs
the delete of its nodes. When write-back of a committed transaction is
complete, GC needs only the delete of the record.

Each transaction record needs at least one transaction mutation to create it,
and one GC delete to remove it. A locked commit makes more transaction
mutations than this one: its lock CASes and its write-back CASes. So in the
usual case, g is less than m, and β = 1 gives a margin.

In the worst case, g is about equal to m or larger:

- The coordinator puts the leaf CASes of many locked commits into few rounds
  (ADR-028). Then the transactions make about one mutation for each record.
- Transactions abort with locks that GC must release.
- Transactions drop collections with many nodes.

Then garbage can grow under sustained load, and it decreases when the load
decreases. Full GCs also remove it.

Duplicate deletes by other instances use tokens. This is accepted.

### GC capacities follow the budget

The budget is useful only when candidate slots and check scheduling can reach
its spend limit:

- **Barrier epochs and pipelined checks.** Each GC check must make its reads
  that prove the absence of a reference after a currentness barrier that GC
  captured after the status read of that check completed
  ([ADR-079](079-gc-safety-does-not-depend-on-time.md)). Checks that share a
  barrier also share their leaf reads. GC therefore runs status reads without
  pauses, up to its concurrency. At intervals, it captures one barrier and
  starts the leaf checks of every record whose status read completed before
  that barrier, as one epoch. The checks of an epoch run independently, so the
  slowest check holds up neither the next epoch nor the next status reads. A
  full GC also lists the next page while the checks of the current page run.

  Today a full GC captures one barrier for each LIST page, and background GC
  one for each batch of checks. In a full GC after the
  measured load, 98.7% of the 362,114 leaf reads found the leaf unchanged:
  about one read of each of 4,664 leaves for each of 106 pages. These reads
  took 55% of the time of a check. With epochs, each leaf is read about once
  for each epoch that touches it.
- **Candidate capacities.** The hint and scan candidate capacities follow the
  spend limit multiplied by the time that a candidate holds its slot. For hints,
  this time is the retention period. For scans, it includes the deferral after a
  check that reclaims some effects. The scan capacity is at least two LIST
  pages, so that a LIST can always start. `GcLimits` gives a maximum for each
  capacity, which limits memory.
- **Adaptive concurrency.** GC concurrency is the number of GC checks that run
  at the same time. It starts at 1, and increases while ready GC work waits
  and the allowance is positive. From 8 up, it changes once for each round of
  completions, as many as the concurrency: it doubles when at least half of
  the checks of the round wrote, and halves to at least 8 otherwise. While the
  budget holds checks back, it does not change, and it returns to 1 when no GC
  work is left. So checks that only read, for example of records whose values
  are still live, do not raise concurrency above 8.
- **GC parallelism.** It is only a safety limit on concurrency, for memory and
  backend connections. Its default is larger than the concurrency that the
  measured load needs, so it does not limit catch-up at that load.

### Dropped hints make GC scans faster

A dropped hint shows that a transaction record can have GC work that GC does
not know about. The scan cadence counts each dropped hint as a useful scan
result, so scans do not slow down while hints are dropped. This includes hints
that are dropped because a producer finds the hint queue locked. ADR-070 counted
only reclaimed effects and recovery progress.

An instance that runs no transactions drops no hints. Its scan cadence still
follows the progress of its own checks.

Scans still start a LIST only when the scan candidates have free capacity. A
LIST page gives at most 1,000 candidates for one request.

### Initial values

Use these initial values, and test them with representative workloads:

| Value | Initial value | Reason |
| --- | --- | --- |
| β | 1 | GC makes at most about as many writes as the rest of the instance |
| τ | 10 minutes | GC uses the budget of a burst within about 10 minutes after the burst |
| τ_keep | 2 hours | Records that scans find late keep their budget |
| Base rate | 100 writes in each second, set with `GcLimits::base_write_rate` | 5 instances can delete 328k records in about 11 minutes |
| GC parallelism | 256 | About twice the concurrency that the measured load needs |
| Maximum hint candidates | 65,536 | About 10 MB. The measured load needs about 25,000: 560 hints in each second for 45 s |
| Maximum scan candidates | 65,536 | About 10 MB |

The measured load created about 560 records in each second for each instance.
With about 200 ms for each check in the S3 delay model, GC needs about 110
checks at the same time to delete these records as fast as the load creates
them. Pipelined checks and checks that read less make each check shorter, and
then GC needs less concurrency for the same rate.

## Consequences

- Benchmarks can start each measured interval with no garbage from earlier
  loads. A full GC makes at least one LIST request for each 1,000 records, and
  the GC checks of the records that it lists. The records of the last safety
  horizon of a load are deferred, so a benchmark waits at least that long,
  45 s with the default timing, after its workers stop.
- A full GC does not change the write budget. So in the measured interval,
  background GC has the earned tokens of the warm-up. This is the same as in
  production under sustained load, and it is accepted.
- Operators can start a full GC, for example at a time with little load. The
  public API has one more method and one more option, which the crate must
  document and keep compatible.
- Background GC writes have a limit that does not depend on the workload:
  at most about β times the other writes of the instance, plus the base
  rate.
- Under sustained load, garbage decreases when g is less than the total GC
  rate. In the worst case, GC only keeps up, and garbage can grow until the load
  decreases.
- GC reads are not in the budget. Leaf reads usually reach the backend. They
  can affect transaction latency, especially on a backend that limits requests
  for each prefix. Adaptive concurrency limits checks that only read to 8 at
  the same time.
- GC concurrency is high only while GC makes many mutations within the budget.
  With little GC work, or with an empty allowance, it stays small.
- After a burst, GC continues for about τ. After a sustained load, earned tokens
  let GC run faster than the current mutation rate for up to about τ_keep. A
  burst in that period competes with GC at the spend limit of the earned tokens.
- The candidate capacities use up to about 20 MB for each instance, against
  less than 1 MB before.
- Deterministic tests must cover:
  - a full GC with deleted, missing, deferred, retained, and failed records;
  - a full GC that the caller drops;
  - a full GC that runs at the same time as background GC and transactions;
  - the budget under sustained load, after a burst, and with late discovery;
  - checks that make the allowance negative, and a large collection drop;
  - concurrency that increases while checks make mutations, and stays small for
    checks that only read;
  - the scan cadence with dropped hints.

## Alternatives considered

- **Settings that make background GC faster in benchmarks.** These also change
  GC during the measured interval, unless the benchmark turns them off before
  it measures. A switch that the benchmark turns on and off at run time is a
  full GC with a less clear interface.
- **Wait in benchmarks until no candidate is left.** The `mixed` scenario does
  this after the seed. After a conditioning load, it needed about 10.5 minutes,
  and after a 600 s load more than 30 minutes.
- **More GC instances.** Four extra instances still needed about 10 minutes.
  The candidate slots and the scan cadence limit each instance.
- **Accept garbage in benchmarks.** The measured interval then includes GC of
  the warm-up, and the result changes with the length of the warm-up.
- **Raise the fixed limits.** Larger capacities and a faster minimum scan
  cadence help sustained load. But they also let GC compete with a burst at the
  full rate, and a larger load can still exceed them.
- **A budget of GC checks for each created record.** The number of checks for
  each deleted record changes with the garbage density, and the investigation
  measured only a lower limit of 2.8. Most checks that delete nothing make no
  mutation, so a check budget limits the wrong cost.
- **A budget of all backend requests.** The caches absorb many GC reads, and
  reads cost much less than mutations on S3 and GCS. A request budget would
  count reads that do not reach the backend, or that do not slow down
  transactions.
- **A spend limit of β × the moving average of the mutation rate.** When scans
  find the records of a burst late, the average has already decreased, and only
  the base rate is left. Earned tokens keep the budget until GC can use it.
- **A token bucket without a spend limit.** GC could use the tokens of a long
  burst at full parallelism while the burst continues.
- **Base-rate tokens in the earned tokens.** An idle instance would collect
  base-rate tokens for τ_keep, and then spend them far above the base rate.
- **No rate limit in instances without transactions.** The step from an instance
  that makes one mutation to an instance that makes none is then large. Such an
  instance can compete at full parallelism with the bursts of other instances
  on the same backend.
- **A rate limit that follows the GC backlog.** When hints are dropped, GC does
  not know about most garbage, so the GC backlog is a weak signal. The mutations
  of transactions are known exactly, and the instance that makes them pays for
  their GC.
- **Make writers pay for GC.** ADR-070 rejects this, because writers must not
  wait for GC.
