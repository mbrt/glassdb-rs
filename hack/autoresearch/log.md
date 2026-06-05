# Autoresearch experiment log

The running record of autoresearch experiments. The agent appends one entry per
experiment (kept or discarded) following the format in
[`program.md`](program.md). Autoresearch experiments do not get ADRs; this log
is the record of what was tried and why.

Each entry looks like:

```markdown
## <n>. <short title> - KEPT | DISCARDED
- Hypothesis: <what you expected and why>
- Change: <files / approach>
- Correctness: fast gate <pass/fail>, judge <approved/rejected[: reason]>
- Primary: <best> -> <new> (<+/-%>)
- Secondary: alloc <..>, ns <..>, cpu <..>
- Outcome & why: <why kept or discarded; what was learned>
- Commit: <hash if kept>
```

---

<!-- Experiment entries are appended below. -->

## 0. Baseline - KEPT
- Hypothesis: n/a (establish the starting point).
- Change: none; recorded the baseline correctness and score.
- Correctness: full gate (`check.sh --full`) PASS (build + make test + sim-test + 120s concurrent_tx fuzz, 6327 runs, no crash).
- Primary: baseline score = 434.95 (per-run: 434.83, 434.95, 435.00).
- Secondary: allocBytesPerTx=80409, allocsPerTx=907.2, nsPerTx=41129, cpuNsPerTx=62559.
- Per-workload cost/tx: singleRMW=72.07 (objW 1.01, metaR 0.015, metaW 0.02), multiRMW10=1082.6 (objW 11, metaW 9.8, objR 0.1, metaR 0.1), batchRead10=313.66 (metaR 10, objW 0.05), batchWrite100=20191.38 (objW 199, metaR 200, metaW ~2), readRepeat=31.51 (metaR 1, objW 0.005).
- Outcome & why: starting point. Biggest geomean lever is batchWrite100 (metaR 200/tx = 2 per created key, the double get_metadata called out in program.md), then multiRMW10 (locks+value-applies) and batchRead10 (10 metaR/tx for read validation).
- Commit: d87402c (pre-existing HEAD)

## 1. Skip redundant get_metadata on the create-lock path - KEPT
- Hypothesis: batchWrite100 spends 2 metaReads per created key. The 2nd is the create attempt's `do_lock_op` -> `fetch_lock_info` -> `get_metadata`, which is redundant: a pure Create request applies via `write_if_not_exists` (fails with precondition if the object exists), and `compute_lock_update` ignores the current lock state for a create (never waits/wounds). So the create can skip the metadata read; on precondition the caller already falls back to a write lock.
- Change: `crates/glassdb-trans/src/tlocker.rs` `LockerCore::do_lock_op` - added a fast path for `req.typ == Create && unlockers.empty()` that issues `update_lock` (=> `write_if_not_exists`) directly with a null expected version, skipping `fetch_lock_info`/`fetch_lockers_state`.
- Correctness: fast gate PASS; judge APPROVED; full gate (`check.sh --full`) PASS (sim-test + 120s fuzz, 7185 runs, no crash).
- Primary: 434.95 -> 420.15 (-3.4%). batchWrite100 metaR 200->100/tx, cost 20191->17053.
- Secondary: allocBytes/allocs/ns/cpu ~unchanged (one fewer backend call/key; no extra alloc).
- Outcome & why: clear primary win beyond noise, correctness intact, behavior identical (same precondition fallback). Kept. Round-1 write-lock attempt on a new key still costs 100 metaR/tx (it discovers not-found before the collection lock + create); eliminating that would need a create-before-write reordering that must still take the collection lock first for phantom prevention - a later candidate.
- Commit: 751ddc2

## 2. Route blind writes through create-or-write under a collection lock - KEPT
- Hypothesis: after exp1, batchWrite100 still does 100 metaR/tx = the round-1 wasted write-lock attempts. A blind write (write, no prior read) of unknown existence first tries lock_write (get_metadata -> not-found) then retries under a collection lock to create. Since a create needs the collection lock anyway (phantom prevention), acquire it up front and route blind writes straight to create-or-write, skipping the wasted write-lock round. Expected final lock set + tx-log identical, just fewer ops.
- Change: `crates/glassdb-trans/src/algo.rs` - (1) `collections_locks` also takes a collection WRITE lock for blind-write keys (`write && !read`); (2) `lock_validate_key` routes blind writes to `lock_validate_not_found_key` (conditional create, fallback to write lock on precondition).
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (sim-test + 120s fuzz, 7581 runs, no crash). Concurrent phase of the oracle/fuzzer is RMW (read+write) + read-only, so the only blind writes are single-tx seeding of new keys: same create+collection-lock outcome, fewer ops, deterministic (run-vs-run op stream still identical).
- Primary: 420.15 -> 403.59 (-3.9%). batchWrite100 metaR 100->0/tx, cost 17053->13990.
- Secondary: ~unchanged (one fewer metadata round-trip per created key).
- Outcome & why: clear primary win, identical observable outcome (locks held + log contents unchanged; only the failed write-lock round removed). batchWrite100 now has 0 metaReads (200 at baseline). Kept. No scored workload does blind-writes-to-existing-keys, where create-first would cost an extra failed write_if_not_exists; that case stays correct (precondition -> write-lock fallback).
- Commit: 48617ad

