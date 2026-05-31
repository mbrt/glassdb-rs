# ADR-002: Wound-wait deadlock prevention

## Status

Accepted

## Context

GlassDB acquires locks during the validate-and-commit phase of a transaction.
Because keys are locked in parallel (out of order), two transactions can form a
cycle in the wait-for graph and deadlock — e.g. T1 holds `a` and wants `b`, T2
holds `b` and wants `a`.

The only mechanism for breaking such cycles was a timeout: a transaction that
could not make progress for `MAX_DEADLOCK_TIMEOUT` (5 seconds) released all its
locks and restarted, acquiring them one at a time in a globally sorted order
(serial locking). Sorted-order locking cannot deadlock, so this always made
progress — but only after eating the full multi-second timeout. Under sustained
contention on a handful of keys, latency spiked into the tens of seconds.

We wanted deadlocks to be *prevented* proactively and resolved in roughly the
time it takes to do a few backend operations, rather than detected after a long
stall.

## Decision

Adopt the **wound-wait** scheme. Each transaction is assigned a priority; when a
transaction requests a lock that conflicts with current holders:

- If the requester is **older** (higher priority) than a holder, it **wounds**
  that holder — aborts it — and takes the lock.
- If the requester is **younger**, it **waits** for the holder to finish.

Because an older transaction never waits for a younger one, the wait-for graph
stays acyclic and no deadlock can form.

### Priority from the transaction ID

Priority is derived from the transaction ID, so it needs no extra coordination
or backend calls. The ID layout is:

```
[8 bytes random prefix][8 bytes big-endian UnixNano timestamp]
```

- The **timestamp suffix** is the priority: an earlier timestamp is older /
  higher priority (`TxId::older`).
- The **random prefix comes first** so that transaction-log keys (`_t/<tx-id>`)
  keep a high-entropy prefix. Object stores such as S3 partition by key prefix;
  a timestamp prefix would funnel sequential commits into a single hot
  partition, so the entropy must lead.

Priority depends **only on the timestamp** — never on the random prefix.
Transactions sharing a timestamp are not ordered against each other: neither is
`older`, so neither wounds the other and they fall through to the serial-locking
safety net.

### Why equal timestamps are not ordered

Wound-wait needs the priority relation to be both:

1. a **total order** at any instant, so the wait-for graph stays acyclic; and
2. **stable across a transaction's restarts**, so a wounded victim keeps the
   same relative priority.

Property (2) is what rules out breaking ties with the random prefix. A wounded
transaction restarts with a *renewed* ID (`TxId::renew`) that preserves its
timestamp but mints a **fresh** random prefix — the prefix has to change so the
restarted attempt gets a distinct log object (`_t/<tx-id>`), since the aborted
attempt already owns the old one and lock tags reference the specific ID.
Ordering on that prefix would therefore flip the relative order of two
equal-timestamp transactions on every restart, so they could wound each other
indefinitely:

```
T1=(ts=100, prefix=A)  T2=(ts=100, prefix=B), A<B  => T1 older, wounds T2
T2 restarts => (ts=100, prefix=C)               C<A => T2 now older, wounds T1
T1 restarts => (ts=100, prefix=D)               ...               (livelock)
```

This is not hypothetical: it deadlocked `concurrent_multiple_rmw` for minutes
before the timestamp-only rule was adopted.

Ordering equal-timestamp transactions *is* possible, but only with a tiebreak
that is itself stable across restarts. That means splitting the two roles the
prefix plays today — ordering vs. per-attempt log-object uniqueness — into
separate fields, e.g. `[stable nonce][attempt epoch][timestamp]` compared as
`(timestamp, stable nonce)`. We chose not to, because the payoff is small (see
below) and it adds a field and an invariant to keep correct.

### Where the timestamp comes from

The pure `glassdb-data` crate stays free of any clock dependency: `TxId`
exposes only `new_at(unix_nanos)`, `with_priority(unix_nanos, prefix)`, and
`renew`. The timestamp is injected one layer up, in `glassdb-trans`: `Algo::begin`
reads the current time from the `Monitor`'s `Clock` (via `clock_now`) and calls
`TxId::new_at`. This is deliberately **not** a raw `SystemTime::now()` in the
data layer, for two reasons:

- The `Clock` abstraction is anchored to tokio's virtual time under
  `tokio::time::pause`, so priorities are deterministic in tests and consistent
  with the monitor's expiry logic (which already uses the same clock).
- A literal `SystemTime::now()` would ignore paused time, making test ordering
  flaky, and would silently bypass the equal-timestamp serial-locking path that
  wound-wait relies on.

### How likely are timestamp collisions?

In production the `Clock` is `Clock::real()`, i.e. nanosecond units (from
`SystemTime`) in a 64-bit field. The *effective* resolution is set by the host
clock, but two independent `begin` calls are separated by real work
(allocations, a backend round-trip, etc.), so two contending transactions
landing on the exact same nanosecond is improbable in a single process. Across
clients on different machines the wall clocks are not synchronized to nanosecond
precision, so exact collisions are effectively impossible; clock skew there makes
the global order approximate, but wound-wait only needs *some* consistent order,
and any residual tie or skew-induced cycle is still caught by the serial fallback.

The one place collisions are common is the **test environment**: under
`tokio::time::pause` the virtual clock only advances when every task blocks on a
timer, so transactions begun back-to-back share an identical timestamp. That is
precisely what surfaces the livelock above, and why the equal-timestamp case is
handled explicitly rather than assumed away.

### Victim restart

A wounded transaction's log is durably set to `aborted` via a conditional write
(`Monitor::wound_tx`), so both the local victim and any other client observe the
abort. `Algo::commit` surfaces this as `TransError::Wounded` (checked at the top
of each commit round and when the final log write fails, mapped from
`TransError::AlreadyFinalized`). The DB retry loop then restarts the victim with
`Algo::rebegin` (which calls `TxId::renew`), reusing its original priority so it
is not starved on the retry.

### Serial locking as a safety net

Sorted-order serial locking is kept, but is now a backstop rather than the
primary mechanism. Wound-wait resolves priority-ordered conflicts immediately;
the 5-second timeout only fires under sustained contention or for the rare case
of equal-priority transactions deadlocking, at which point serial locking
guarantees progress.

## Consequences

- Deadlocks are prevented proactively. Conflicts resolve in a few backend
  round-trips instead of stalling for `MAX_DEADLOCK_TIMEOUT`, removing the
  tens-of-seconds tail latency under contention.
- Lock acquisition gains a `wound` outcome alongside `wait_for`
  (`glassdb-storage::LockOps`), and the locker durably aborts wounded holders
  before retrying.
- Older transactions are favored, which bounds starvation: a wounded victim
  keeps its priority and eventually becomes the oldest contender.
- Equal-timestamp transactions are intentionally not ordered, so they can still
  deadlock; this is delegated to the serial-locking safety net. This keeps the
  priority order stable across restarts and avoids a wound livelock.
- The transaction-ID layout is now load-bearing: the timestamp encodes priority
  and the random prefix preserves object-store partition spread. Tests that need
  deterministic priorities build IDs with `TxId::with_priority`.
