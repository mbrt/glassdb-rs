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

## 3. Drop redundant per-transaction Data clones on the commit path - KEPT
- Hypothesis: the primary is at the safe protocol floor after exp1/exp2 (batchWrite100 = 2 objW/created key = create-placeholder + materialize; reducing it needs values written under create-locks, a path the fuzzer never exercises since creates only happen during single-tx seeding - too risky for the correctness mandate). So target secondary: the retry loop clones the collected access (reads+writes) every tx (`access.clone()` into begin/reset) only to keep a copy for the rare wound path, and `commit()` clones `tx.data.writes`. Both clones are avoidable -> fewer allocations, no change to backend ops (primary flat).
- Change: `crates/glassdb/src/db.rs` retry loop moves `access` into begin/reset (Ok path: full; Err path: reads-only directly, dropping the old redundant full begin+reset); the wound path recovers the data from the handle via `rebegin(old)`. `crates/glassdb-trans/src/algo.rs`: `rebegin` now consumes the old handle and reuses its `data`; `commit()` passes `&tx.data.writes` to `commit_writes` instead of cloning.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + sim-test + 120s fuzz, 7452 runs, no crash). Pure refactor: handle ends in the same state in every arm (the old Err path's full begin+reset was immediately overwritten by the reads-only reset; the wound path's data is the same data already stored in the handle).
- Primary: 403.59 -> 403.34 (-0.06%, flat/noise; op counts unchanged).
- Secondary (per-workload allocsPerTx, stable across 3 measurements): singleRMW 145.1->136.5 (-5.9%), batchRead10 654.2->633 (-3.2%), batchWrite100 20949.7->20535 (-2.0%); allocBytes singleRMW -2.8%, batchRead10 -1.3%, batchWrite100 -1.0%; aggregate ns/cpu trend down (noisier). No secondary regression.
- Outcome & why: primary within noise + clear, stable, deterministic allocation reduction across every workload with no regression -> meets the secondary tie-breaker keep rule. Kept. Confirms safe primary headroom is exhausted at the protocol floor; remaining wins are allocation/CPU micro-structure.
- Commit: 951953b

## 4. Build the locked-by tag without an intermediate Vec - DISCARDED
- Hypothesis: `apply_lock_tags` builds the `locked-by` tag via `update.lockers.iter().map(tid_to_tag).collect::<Vec<String>>().join(",")`. Every writer lock has exactly one locker, so the common path allocates a throwaway `Vec<String>` plus a separate join buffer. Building the comma-joined string directly (single-locker = just the base64 string) should cut ~2 allocs per lock op, helping the lock-heavy workloads (batchWrite100 ~200 lock ops/tx, multiRMW ~20/tx).
- Change: `crates/glassdb-storage/src/locker.rs` - added `encode_lockers(&[TxId]) -> String` (empty/single/many cases, single allocation) and used it in `apply_lock_tags` in place of the map+collect+join.
- Correctness: fast gate PASS; judge not run (discarded at measurement).
- Primary: 403.34 -> 404.25 (+0.22%, flat/noise; op counts unchanged).
- Secondary: deterministic per-workload allocs DID drop on the lock-heavy workloads (batchWrite100 20513->20345 = -0.8%, multiRMW 3154->3119 = -1.1%), but the aggregate did not clearly improve: allocsPerTx +0.65%, allocBytesPerTx +0.86%, cpuNsPerTx +5.45% (read-workload + cpu noise dominate; readRepeat allocs swing 77-87 run-to-run), nsPerTx -1.44%.
- Outcome & why: the real effect is a tiny (<1.5%) deterministic alloc reduction on 2 of 5 workloads, swamped by noise in the geomean. Does not meet "a secondary axis clearly improves without regressing the others" - allocs/allocBytes/cpu nominally rose. Discarded (correct but too small to register). Confirms single-allocation lock-tag tweaks are below the noise floor; would need a structural change (e.g. fewer/coalesced lock writes) to show.
- Commit: n/a (reverted)

## 5. Share cached metadata via Arc to cut deep tag-map clones - KEPT
- Hypothesis: every backend metadata op deep-clones a Metadata (tag BTreeMap + version string) to populate the write-through cache: `Global::{read,get_metadata,set_tags_if,write,write_if,write_if_not_exists}` all do `local.set_meta/write_with_meta(.., meta.clone())` then return the original. These run ~200x/tx in batchWrite100 (create + unlock per key), 10-20x/tx in multiRMW, 10x/tx in batchRead10 validation. Sharing the metadata via `Arc<Metadata>` turns every such clone (and every cache `get_meta`/`CacheEntry` clone) into a refcount bump, with no change to backend op counts (primary flat). Metadata is immutable once produced, so sharing is trivially safe.
- Change: `glassdb-storage/local.rs` - `CacheMeta.meta` and `LocalMetadata.m` become `Arc<Metadata>`; `set_meta`/`write_with_meta` take `Arc<Metadata>`. `glassdb-storage/global.rs` - the six methods wrap the backend result in `Arc::new(..)` once and share it with the cache and the return value (return type now `Arc<Metadata>`). `glassdb-trans/reader.rs` - `get_metadata` returns `Arc<Metadata>`. Callers read through `Deref`; the 5 sites that moved `Version` out (`tlocker.rs` fetch_lock_info, `tlogger.rs` set/set_if/read_tags x2) now `.clone()` just the version. Returns of `set_tags_if`/`write*` are ignored by production callers, so the type change is transparent there.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + sim-test + 120s fuzz, 8451 runs, no crash). Pure refactor: the cached/returned metadata is identical, only shared instead of copied; nothing mutates a Metadata after creation.
- Primary: 403.34 -> 403.92 (+0.14%, flat/noise; op counts unchanged).
- Secondary (median of 3, re-confirmed stable across 3 measurements): allocsPerTx 846.4 -> 605.5 (-28.5%), allocBytesPerTx 75673 -> 53435 (-29.4%), nsPerTx 39258 -> 32284 (-17.8%), cpuNsPerTx 55458 -> 46973 (-15.3%). Per-workload allocs: singleRMW -30%, multiRMW -33%, batchRead10 -32%, batchWrite100 -19%, readRepeat -27%. No regression on any axis.
- Outcome & why: primary within noise + a large, stable, deterministic drop on every secondary axis and every workload -> easily meets the secondary keep rule. Kept. The win was far bigger than estimated because the Arc also removes deep clones on cache reads (`get_meta`) and on the cache's internal `CacheEntry` clones, not just the write-through path. Lesson vs exp4: structural changes that hit a clone on every op clear the noise floor; per-call micro-tweaks do not.
- Commit: 76da079

## 6. Move value/version out of the owned cache entry on read - KEPT
- Hypothesis: `Cache::get` already returns an owned (cloned) `CacheEntry`, yet `Local::read` then re-clones the value and version (`v.value.clone()`, `v.version.clone()`) and `get_meta` re-clones the metadata. The version clone is itself 2 allocs (token String + writer). Moving the fields out of the owned entry instead removes ~3 allocs per cached read - hits batchRead10 (10 reads/tx) and readRepeat (1/tx).
- Change: `glassdb-storage/local.rs` - in `read`/`get_meta`, compute the `outdated` flag first (it borrows the entry), then move `e.v`/`e.m` out (`let v = e.v?;`) and move the fields into the result instead of cloning.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + sim-test + 120s fuzz, 8687 runs, no crash). Semantically identical: same value/version/outdated returned; only copies elided on an owned temporary.
- Primary: 403.92 -> 404.29 (+0.09%, flat/noise; op counts unchanged).
- Secondary (stable across 3 measurements): allocsPerTx 605.5 -> 586.3 (-3.2%; deterministic per-workload batchRead10 430->400 = -7.1%, readRepeat 56.7->53.9 = -4.9%), nsPerTx -5.6%, cpuNsPerTx -8.2%, allocBytesPerTx flat (-0.1%; the elided value/version are small in these workloads, so byte count barely moves). No axis regressed.
- Outcome & why: primary within noise + a stable, deterministic alloc-count drop on the read workloads plus lower ns/cpu, no regression -> meets the secondary keep rule. Kept. allocBytes staying flat confirms the win is allocation *count* (small objects), which still lowers allocator/CPU pressure.
- Commit: 9be594b

## 7. Avoid cloning the whole paths vector in parallel validation - KEPT
- Hypothesis: `validate_readonly` and `lock_validate` do `let inputs = vstate.paths.clone()` (clones the whole `Vec<PathState>`) and then `inputs[i].clone()` inside `run_indexed`, copying every PathState (path String + read_version) twice per transaction. The first clone exists only to satisfy borrowing; the closure can borrow `vstate.paths` directly and clone each item once (still needed to own the mutable item in each concurrent future). Halves PathState clones; hits every validating/locking tx (batchRead10 10 paths, multiRMW 10, batchWrite100 100).
- Change: `glassdb-trans/algo.rs` - both functions now `let paths = &vstate.paths;` and clone `paths[i]` in the closure; the write-back loop afterwards still mutates `vstate.paths` (NLL ends the borrow when `run_indexed` returns).
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + sim-test + 120s fuzz, 8369 runs, no crash). Identical processing: each item is cloned, validated/locked, and written back exactly as before; only the redundant whole-vector copy is removed.
- Primary: 404.29 -> ~404 (-0.07%, flat/noise; op counts unchanged).
- Secondary: allocsPerTx 586.3 -> ~571 (-2.6%; deterministic per-workload batchRead10 -5.2%, readRepeat -6.1%, multiRMW -2.6%, batchWrite100 -0.6%), allocBytesPerTx -1.3%, ns/cpu flat (one initial measurement spiked +14% on ns/cpu but 3 re-measurements showed flat-to-down; transient machine noise, not the change - removing a clone cannot add CPU work). No axis regressed.
- Outcome & why: primary within noise + stable deterministic alloc-count reduction, no regression -> kept. Lesson: ns/cpu are noisy enough to spike ~15% on a single run; always re-measure a surprising secondary regression before discarding (or keeping).
- Commit: a5efe31

## 8. Back TxId with Arc<[u8]> so clones are refcount bumps - KEPT
- Hypothesis: `TxId(Vec<u8>)` heap-allocates and copies its 16 bytes on every clone, and ids are cloned pervasively - lockers, last-writer of every version, cache entries, wound-wait partitioning, locked_paths, every commit/validation. Storing the bytes as `Arc<[u8]>` makes clones refcount bumps. Construction (new_at/renew/from_bytes) costs slightly more (Vec -> Arc), but clones vastly outnumber constructions, so net allocs should drop. Eq/Ord/Hash on Arc<[u8]> compare contents, so value semantics are preserved.
- Change: `glassdb-data/txid.rs` - `TxId(Vec<u8>)` -> `TxId(Arc<[u8]>)`; constructors wrap the filled `Vec` via `.into()`; `into_bytes` (test-only) returns `self.0.to_vec()`; `Display` iterates `self.0.iter()`. Public API and all derives unchanged.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + sim-test + 120s fuzz, 8817 runs, no crash). Pure representation change; bytes and comparison semantics identical.
- Primary: 404.26 -> 403.92 (-0.08%, flat/noise; op counts unchanged).
- Secondary (stable across 3 measurements): allocsPerTx 570.7 -> 489.4 (-14.3%; per-workload singleRMW -14%, multiRMW -17%, batchRead10 -16%, batchWrite100 -9%, readRepeat -15%), allocBytesPerTx -5.8%, nsPerTx -9%, cpuNsPerTx ~-8% (noisy). No axis regressed.
- Outcome & why: primary within noise + a large, stable, deterministic alloc-count drop on every workload -> easily meets the secondary keep rule. Kept. Same lesson as exp5: a type cloned on nearly every op is a high-leverage target for Arc sharing; the atomic-refcount cost is far cheaper than the per-clone heap alloc it replaces (ns/cpu also fell).
- Commit: e9299a6

## 9. Store transaction access paths as Arc<str> - KEPT
- Hypothesis: `ReadAccess`/`WriteAccess`/`PathState` hold the key path as `String`, so each path is deep-copied through the commit pipeline: `collect_accesses` (staged -> access), `init_validation`'s dedup `HashMap` (clones path for the map key AND the PathState - twice per new key), and the per-item clone in parallel validation/locking. Storing paths as `Arc<str>` turns those into refcount bumps. The `String` sinks (`PathLock`, `TxWrite` protobuf field) materialize a String at the boundary as today, so their cost is unchanged - the win is purely the eliminated intermediate copies, biggest on read-heavy txns (batchRead10 dedups 10 paths twice each).
- Change: `glassdb-trans/algo.rs` - `path: String` -> `Arc<str>` on the three structs; `init_validation` map keyed by `Arc<str>` (cheap entry/clone); `needs_locks`/`to_log` use `self.path.to_string()`/`w.path.to_string()` at the String sinks; in-file test helpers build paths via `.into()`. `glassdb/tx.rs` - `collect_accesses` builds `path: k.as_str().into()` (same single alloc as the previous `String` clone). All `&item.path` borrows work unchanged via `Arc<str>` deref.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + sim-test + 120s fuzz, 8742 runs, no crash). Logical no-op: paths carry identical bytes; only intermediate copies are shared.
- Primary: 403.92 -> 403.60 (-0.08%, flat/noise; op counts unchanged).
- Secondary (stable across 4 measurements): allocsPerTx 489.4 -> ~471 (-4%; deterministic per-workload batchRead10 -9.7%, singleRMW -6.3%, batchWrite100 -2.0%, multiRMW -2.0%), allocBytesPerTx -2.6%, nsPerTx flat, cpuNsPerTx flat (one run spiked +3.7%/cpu=50k but 3 re-runs sat at ~42k; readRepeat first read +1.9% was also noise - re-runs 40-42, flat). No axis regressed.
- Outcome & why: primary within noise + stable deterministic alloc/allocBytes reduction, no regression -> kept. The String sinks cap the upside (still materialized for locks/log), but eliminating the dedup-map double clone is a clean structural win on read paths.
- Commit: c9154fd

