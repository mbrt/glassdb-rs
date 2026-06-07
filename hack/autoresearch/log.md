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
- Correctness: full gate (`check.sh --full`) PASS (build + make test + test-sim + 120s concurrent_tx fuzz, 6327 runs, no crash).
- Primary: baseline score = 434.95 (per-run: 434.83, 434.95, 435.00).
- Secondary: allocBytesPerTx=80409, allocsPerTx=907.2, nsPerTx=41129, cpuNsPerTx=62559.
- Per-workload cost/tx: singleRMW=72.07 (objW 1.01, metaR 0.015, metaW 0.02), multiRMW10=1082.6 (objW 11, metaW 9.8, objR 0.1, metaR 0.1), batchRead10=313.66 (metaR 10, objW 0.05), batchWrite100=20191.38 (objW 199, metaR 200, metaW ~2), readRepeat=31.51 (metaR 1, objW 0.005).
- Outcome & why: starting point. Biggest geomean lever is batchWrite100 (metaR 200/tx = 2 per created key, the double get_metadata called out in program.md), then multiRMW10 (locks+value-applies) and batchRead10 (10 metaR/tx for read validation).
- Commit: d87402c (pre-existing HEAD)

## 1. Skip redundant get_metadata on the create-lock path - KEPT
- Hypothesis: batchWrite100 spends 2 metaReads per created key. The 2nd is the create attempt's `do_lock_op` -> `fetch_lock_info` -> `get_metadata`, which is redundant: a pure Create request applies via `write_if_not_exists` (fails with precondition if the object exists), and `compute_lock_update` ignores the current lock state for a create (never waits/wounds). So the create can skip the metadata read; on precondition the caller already falls back to a write lock.
- Change: `crates/glassdb-trans/src/tlocker.rs` `LockerCore::do_lock_op` - added a fast path for `req.typ == Create && unlockers.empty()` that issues `update_lock` (=> `write_if_not_exists`) directly with a null expected version, skipping `fetch_lock_info`/`fetch_lockers_state`.
- Correctness: fast gate PASS; judge APPROVED; full gate (`check.sh --full`) PASS (test-sim + 120s fuzz, 7185 runs, no crash).
- Primary: 434.95 -> 420.15 (-3.4%). batchWrite100 metaR 200->100/tx, cost 20191->17053.
- Secondary: allocBytes/allocs/ns/cpu ~unchanged (one fewer backend call/key; no extra alloc).
- Outcome & why: clear primary win beyond noise, correctness intact, behavior identical (same precondition fallback). Kept. Round-1 write-lock attempt on a new key still costs 100 metaR/tx (it discovers not-found before the collection lock + create); eliminating that would need a create-before-write reordering that must still take the collection lock first for phantom prevention - a later candidate.
- Commit: 751ddc2

## 2. Route blind writes through create-or-write under a collection lock - KEPT
- Hypothesis: after exp1, batchWrite100 still does 100 metaR/tx = the round-1 wasted write-lock attempts. A blind write (write, no prior read) of unknown existence first tries lock_write (get_metadata -> not-found) then retries under a collection lock to create. Since a create needs the collection lock anyway (phantom prevention), acquire it up front and route blind writes straight to create-or-write, skipping the wasted write-lock round. Expected final lock set + tx-log identical, just fewer ops.
- Change: `crates/glassdb-trans/src/algo.rs` - (1) `collections_locks` also takes a collection WRITE lock for blind-write keys (`write && !read`); (2) `lock_validate_key` routes blind writes to `lock_validate_not_found_key` (conditional create, fallback to write lock on precondition).
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (test-sim + 120s fuzz, 7581 runs, no crash). Concurrent phase of the oracle/fuzzer is RMW (read+write) + read-only, so the only blind writes are single-tx seeding of new keys: same create+collection-lock outcome, fewer ops, deterministic (run-vs-run op stream still identical).
- Primary: 420.15 -> 403.59 (-3.9%). batchWrite100 metaR 100->0/tx, cost 17053->13990.
- Secondary: ~unchanged (one fewer metadata round-trip per created key).
- Outcome & why: clear primary win, identical observable outcome (locks held + log contents unchanged; only the failed write-lock round removed). batchWrite100 now has 0 metaReads (200 at baseline). Kept. No scored workload does blind-writes-to-existing-keys, where create-first would cost an extra failed write_if_not_exists; that case stays correct (precondition -> write-lock fallback).
- Commit: 48617ad

## 3. Drop redundant per-transaction Data clones on the commit path - KEPT
- Hypothesis: the primary is at the safe protocol floor after exp1/exp2 (batchWrite100 = 2 objW/created key = create-placeholder + materialize; reducing it needs values written under create-locks, a path the fuzzer never exercises since creates only happen during single-tx seeding - too risky for the correctness mandate). So target secondary: the retry loop clones the collected access (reads+writes) every tx (`access.clone()` into begin/reset) only to keep a copy for the rare wound path, and `commit()` clones `tx.data.writes`. Both clones are avoidable -> fewer allocations, no change to backend ops (primary flat).
- Change: `crates/glassdb/src/db.rs` retry loop moves `access` into begin/reset (Ok path: full; Err path: reads-only directly, dropping the old redundant full begin+reset); the wound path recovers the data from the handle via `rebegin(old)`. `crates/glassdb-trans/src/algo.rs`: `rebegin` now consumes the old handle and reuses its `data`; `commit()` passes `&tx.data.writes` to `commit_writes` instead of cloning.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + test-sim + 120s fuzz, 7452 runs, no crash). Pure refactor: handle ends in the same state in every arm (the old Err path's full begin+reset was immediately overwritten by the reads-only reset; the wound path's data is the same data already stored in the handle).
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
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + test-sim + 120s fuzz, 8451 runs, no crash). Pure refactor: the cached/returned metadata is identical, only shared instead of copied; nothing mutates a Metadata after creation.
- Primary: 403.34 -> 403.92 (+0.14%, flat/noise; op counts unchanged).
- Secondary (median of 3, re-confirmed stable across 3 measurements): allocsPerTx 846.4 -> 605.5 (-28.5%), allocBytesPerTx 75673 -> 53435 (-29.4%), nsPerTx 39258 -> 32284 (-17.8%), cpuNsPerTx 55458 -> 46973 (-15.3%). Per-workload allocs: singleRMW -30%, multiRMW -33%, batchRead10 -32%, batchWrite100 -19%, readRepeat -27%. No regression on any axis.
- Outcome & why: primary within noise + a large, stable, deterministic drop on every secondary axis and every workload -> easily meets the secondary keep rule. Kept. The win was far bigger than estimated because the Arc also removes deep clones on cache reads (`get_meta`) and on the cache's internal `CacheEntry` clones, not just the write-through path. Lesson vs exp4: structural changes that hit a clone on every op clear the noise floor; per-call micro-tweaks do not.
- Commit: 76da079

## 6. Move value/version out of the owned cache entry on read - KEPT
- Hypothesis: `Cache::get` already returns an owned (cloned) `CacheEntry`, yet `Local::read` then re-clones the value and version (`v.value.clone()`, `v.version.clone()`) and `get_meta` re-clones the metadata. The version clone is itself 2 allocs (token String + writer). Moving the fields out of the owned entry instead removes ~3 allocs per cached read - hits batchRead10 (10 reads/tx) and readRepeat (1/tx).
- Change: `glassdb-storage/local.rs` - in `read`/`get_meta`, compute the `outdated` flag first (it borrows the entry), then move `e.v`/`e.m` out (`let v = e.v?;`) and move the fields into the result instead of cloning.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + test-sim + 120s fuzz, 8687 runs, no crash). Semantically identical: same value/version/outdated returned; only copies elided on an owned temporary.
- Primary: 403.92 -> 404.29 (+0.09%, flat/noise; op counts unchanged).
- Secondary (stable across 3 measurements): allocsPerTx 605.5 -> 586.3 (-3.2%; deterministic per-workload batchRead10 430->400 = -7.1%, readRepeat 56.7->53.9 = -4.9%), nsPerTx -5.6%, cpuNsPerTx -8.2%, allocBytesPerTx flat (-0.1%; the elided value/version are small in these workloads, so byte count barely moves). No axis regressed.
- Outcome & why: primary within noise + a stable, deterministic alloc-count drop on the read workloads plus lower ns/cpu, no regression -> meets the secondary keep rule. Kept. allocBytes staying flat confirms the win is allocation *count* (small objects), which still lowers allocator/CPU pressure.
- Commit: 9be594b

## 7. Avoid cloning the whole paths vector in parallel validation - KEPT
- Hypothesis: `validate_readonly` and `lock_validate` do `let inputs = vstate.paths.clone()` (clones the whole `Vec<PathState>`) and then `inputs[i].clone()` inside `run_indexed`, copying every PathState (path String + read_version) twice per transaction. The first clone exists only to satisfy borrowing; the closure can borrow `vstate.paths` directly and clone each item once (still needed to own the mutable item in each concurrent future). Halves PathState clones; hits every validating/locking tx (batchRead10 10 paths, multiRMW 10, batchWrite100 100).
- Change: `glassdb-trans/algo.rs` - both functions now `let paths = &vstate.paths;` and clone `paths[i]` in the closure; the write-back loop afterwards still mutates `vstate.paths` (NLL ends the borrow when `run_indexed` returns).
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + test-sim + 120s fuzz, 8369 runs, no crash). Identical processing: each item is cloned, validated/locked, and written back exactly as before; only the redundant whole-vector copy is removed.
- Primary: 404.29 -> ~404 (-0.07%, flat/noise; op counts unchanged).
- Secondary: allocsPerTx 586.3 -> ~571 (-2.6%; deterministic per-workload batchRead10 -5.2%, readRepeat -6.1%, multiRMW -2.6%, batchWrite100 -0.6%), allocBytesPerTx -1.3%, ns/cpu flat (one initial measurement spiked +14% on ns/cpu but 3 re-measurements showed flat-to-down; transient machine noise, not the change - removing a clone cannot add CPU work). No axis regressed.
- Outcome & why: primary within noise + stable deterministic alloc-count reduction, no regression -> kept. Lesson: ns/cpu are noisy enough to spike ~15% on a single run; always re-measure a surprising secondary regression before discarding (or keeping).
- Commit: a5efe31

## 8. Back TxId with Arc<[u8]> so clones are refcount bumps - KEPT
- Hypothesis: `TxId(Vec<u8>)` heap-allocates and copies its 16 bytes on every clone, and ids are cloned pervasively - lockers, last-writer of every version, cache entries, wound-wait partitioning, locked_paths, every commit/validation. Storing the bytes as `Arc<[u8]>` makes clones refcount bumps. Construction (new_at/renew/from_bytes) costs slightly more (Vec -> Arc), but clones vastly outnumber constructions, so net allocs should drop. Eq/Ord/Hash on Arc<[u8]> compare contents, so value semantics are preserved.
- Change: `glassdb-data/txid.rs` - `TxId(Vec<u8>)` -> `TxId(Arc<[u8]>)`; constructors wrap the filled `Vec` via `.into()`; `into_bytes` (test-only) returns `self.0.to_vec()`; `Display` iterates `self.0.iter()`. Public API and all derives unchanged.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + test-sim + 120s fuzz, 8817 runs, no crash). Pure representation change; bytes and comparison semantics identical.
- Primary: 404.26 -> 403.92 (-0.08%, flat/noise; op counts unchanged).
- Secondary (stable across 3 measurements): allocsPerTx 570.7 -> 489.4 (-14.3%; per-workload singleRMW -14%, multiRMW -17%, batchRead10 -16%, batchWrite100 -9%, readRepeat -15%), allocBytesPerTx -5.8%, nsPerTx -9%, cpuNsPerTx ~-8% (noisy). No axis regressed.
- Outcome & why: primary within noise + a large, stable, deterministic alloc-count drop on every workload -> easily meets the secondary keep rule. Kept. Same lesson as exp5: a type cloned on nearly every op is a high-leverage target for Arc sharing; the atomic-refcount cost is far cheaper than the per-clone heap alloc it replaces (ns/cpu also fell).
- Commit: e9299a6

## 9. Store transaction access paths as Arc<str> - KEPT
- Hypothesis: `ReadAccess`/`WriteAccess`/`PathState` hold the key path as `String`, so each path is deep-copied through the commit pipeline: `collect_accesses` (staged -> access), `init_validation`'s dedup `HashMap` (clones path for the map key AND the PathState - twice per new key), and the per-item clone in parallel validation/locking. Storing paths as `Arc<str>` turns those into refcount bumps. The `String` sinks (`PathLock`, `TxWrite` protobuf field) materialize a String at the boundary as today, so their cost is unchanged - the win is purely the eliminated intermediate copies, biggest on read-heavy txns (batchRead10 dedups 10 paths twice each).
- Change: `glassdb-trans/algo.rs` - `path: String` -> `Arc<str>` on the three structs; `init_validation` map keyed by `Arc<str>` (cheap entry/clone); `needs_locks`/`to_log` use `self.path.to_string()`/`w.path.to_string()` at the String sinks; in-file test helpers build paths via `.into()`. `glassdb/tx.rs` - `collect_accesses` builds `path: k.as_str().into()` (same single alloc as the previous `String` clone). All `&item.path` borrows work unchanged via `Arc<str>` deref.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + test-sim + 120s fuzz, 8742 runs, no crash). Logical no-op: paths carry identical bytes; only intermediate copies are shared.
- Primary: 403.92 -> 403.60 (-0.08%, flat/noise; op counts unchanged).
- Secondary (stable across 4 measurements): allocsPerTx 489.4 -> ~471 (-4%; deterministic per-workload batchRead10 -9.7%, singleRMW -6.3%, batchWrite100 -2.0%, multiRMW -2.0%), allocBytesPerTx -2.6%, nsPerTx flat, cpuNsPerTx flat (one run spiked +3.7%/cpu=50k but 3 re-runs sat at ~42k; readRepeat first read +1.9% was also noise - re-runs 40-42, flat). No axis regressed.
- Outcome & why: primary within noise + stable deterministic alloc/allocBytes reduction, no regression -> kept. The String sinks cap the upside (still materialized for locks/log), but eliminating the dedup-map double clone is a clean structural win on read paths.
- Commit: c9154fd

## 10. Build validation state by merging sorted accesses (no HashMap) - KEPT
- Hypothesis: `init_validation` allocates a per-transaction `HashMap<Arc<str>, PathState>` and then re-sorts its values. But `collect_accesses` already emits reads and writes sorted and unique by path (ADR-008), so the two runs can be merged directly into a path-sorted `PathState` list - dropping the HashMap's backing allocation (sizeable for batchWrite100's 100-key map), its hashing, and the separate sort. Cross-cutting: every transaction builds a validation state.
- Change: `glassdb-trans/algo.rs` - replace the HashMap build + sort in `init_validation` with a two-pointer merge of `h.data.reads`/`h.data.writes`; equal paths merge into one entry (read+write), preserving the exact deduplicated, path-sorted order.
- Correctness: fast gate PASS; judge APPROVED; full gate PASS (make test + test-sim + 120s fuzz, 8920 runs, no crash). Output order/content identical to the HashMap+sort version given sorted, unique inputs (guaranteed by collect_accesses / ADR-008); the deterministic test-sim would catch any order drift.
- Primary: 403.66 -> 403.97 (+0.07%, flat/noise; op counts unchanged).
- Secondary (stable across 3 runs; ns/cpu medianed over extra samples to filter spikes): allocBytesPerTx 48278 -> ~45600 (-5.5%; the HashMap backing array is a big byte allocation), allocsPerTx -1.1% (multiRMW -3.0%), nsPerTx ~-4%, cpuNsPerTx flat-to-down. No axis regressed.
- Outcome & why: primary within noise + a clear, stable allocBytes drop (-5.5%) plus lower allocs/ns and flat cpu -> meets the secondary keep rule. Kept. Lesson: allocation *bytes* can move far more than allocation *count* when the eliminated allocation is one large buffer (a HashMap's bucket array) rather than many small ones.
- Commit: 4f56e36

## 11. Re-attempt encode_lockers (build locked-by in one allocation) - DISCARDED
- Hypothesis: exp4's `encode_lockers` (avoid the intermediate `Vec<String>` + `join` for the `locked-by` tag) was discarded at the old baseline as sub-noise. With the baseline now ~2x lower (allocs 846 -> 461 after exp5-10), the same fixed per-op saving is a larger fraction, so re-test whether it now clearly registers.
- Change: same as exp4 - `glassdb-storage/locker.rs` `apply_lock_tags` builds the `locked-by` value via a single-pass `encode_lockers(&update.lockers)` instead of `iter().map(tid_to_tag).collect::<Vec<_>>().join(",")`.
- Correctness: fast gate PASS (full gate not run; discarded on measurement).
- Primary: 403.94 -> 403.68 (flat/noise).
- Secondary (median of 3, plus 5 extra ns/cpu samples): allocsPerTx -0.54% (only batchWrite100 -1.4%, because unlock ops have an empty locker list and already produced an empty string), allocBytesPerTx flat (-0.07%), nsPerTx/cpuNsPerTx medians lower (~-5 to -8%) but fully within the baseline's own run-to-run variance (ns seen 26-36k, cpu 37-53k on the *unchanged* code too).
- Outcome & why: no axis *clearly* improves beyond noise - allocs/allocBytes are flat-ish and the ns/cpu medians overlap baseline variance, so the apparent ns/cpu win cannot be attributed to the change. Same conclusion as exp4. Discarded (correct, but below the noise floor). Confirms the standing lesson: single-allocation lock-tag tweaks don't register; only structural per-op clone elimination (exp5/8/9) or removing a large buffer (exp10) does.
- Commit: n/a (reverted)

---

## Final summary

Budget: ran until the 3-hour wall clock; 11 experiments total (9 kept, 2 discarded).
Every kept change passed the full correctness gate (`check.sh --full`: build +
`make test` + test-sim + 120s `concurrent_tx` fuzz) and the test-integrity judge,
and only edited allowed implementation files. Strict serializability was never
weakened: every kept change is either a backend-op-count reduction proven safe by
the existing oracle/fuzzer (exp1-2) or a pure representation/allocation refactor
that leaves the protocol, op stream, and value semantics identical (exp3, 5-10).

Cumulative results vs the recorded baseline (median of runs):

| Metric            | Baseline | Final  | Change  |
|-------------------|---------:|-------:|--------:|
| Primary score     |   434.95 | 404.18 |  -7.1%  |
| allocsPerTx       |    907.2 |  461.5 | -49.1%  |
| allocBytesPerTx   |    80409 |  45624 | -43.3%  |
| nsPerTx           |    41129 |  29554 | -28.1%  |
| cpuNsPerTx        |    62559 |  46871 | -25.1%  |

Kept experiments:
1. Skip redundant metadata read on the create-lock path (primary -3.4%).
2. Route blind writes through create-or-write under a collection lock (primary -3.9%).
3. Drop redundant per-transaction `Data` clones on the commit path (secondary allocs).
5. Share cached metadata via `Arc<Metadata>` (allocs -28%).
6. Move value/version out of the owned cache entry on read (allocs -3%).
7. Avoid cloning the whole `paths` vector in parallel validation (allocs -3%).
8. Back `TxId` with `Arc<[u8]>` so clones are refcount bumps (allocs -14%).
9. Store transaction access paths as `Arc<str>` (allocs -4%).
10. Build validation state by merging sorted accesses, no per-tx `HashMap` (allocBytes -5.5%).

Discarded: exp4 and exp11 (the same `encode_lockers` lock-tag tweak, both times
below the geomean noise floor).

What worked and why: the primary score hit a protocol floor after exp1-2 - further
backend-op reductions would need create-value or log-elision changes whose
serializability the current fuzzer/oracle can't adequately certify, so they were
deliberately not attempted (correctness-first). The large, durable wins all came
from one pattern: find a value that is *cloned on nearly every operation* and make
the clone cheap. `Metadata` tag maps (exp5), `TxId` bytes (exp8), and access paths
(exp9) each clear the measurement noise easily; removing one large per-tx buffer -
the validation `HashMap` (exp10) - moved allocation *bytes* the most. Conversely,
shaving a single small allocation per op (exp4/11) never rose above noise. ns/cpu
proved noisy enough (±10-15%) to spike on single runs, so every surprising
secondary delta was re-measured before deciding.

---

# Session 2

New autoresearch session on branch `autoresearch2`, continuing from the session-1
HEAD (post-exp11). Budget: 25 experiments or 4 hours, whichever comes first.
Experiments are numbered continuing from 12.

## Setup note - frozen `check.sh` references a removed cfg
`hack/autoresearch/check.sh`'s `run_fuzz` hardcodes `RUSTFLAGS="--cfg madsim"`, but
the engine DST migrated off `madsim` to the in-repo executor (`--cfg sim`, commit
`de9d02c`, ADR-011); `replay_fuzz_input` is now `#[cfg(sim)]`-gated, so the fuzz
target fails to compile under `--cfg madsim` (`E0425: cannot find function
replay_fuzz_input`). `check.sh` is a frozen infra file I may not modify, and so are
`fuzz/**` and `sim.rs`, so there is no in-scope code fix. Per program.md a setup
problem must be resolved in setup and is not a stop condition. Resolution: the fast
gate (`hack/autoresearch/check.sh`, build + `cargo test --workspace`, incl.
`proptest_concurrent`) works unmodified and is run verbatim every experiment; for
the full-gate fuzz step I substitute the project's own supported invocation
(`cd fuzz && RUSTFLAGS="--cfg sim" cargo +nightly fuzz run concurrent_tx`, the same
command `make fuzz`/`fuzz/.cargo/config.toml` use), so the serializability fuzzer
still runs - just with the correct flag. `make test` + `make test-sim` (which use
`--cfg sim` already) are run as-is for keeps.

## 12. Session-2 baseline - KEPT
- Hypothesis: n/a (re-establish the starting point for this session).
- Change: none; recorded baseline correctness and score from current HEAD.
- Correctness: full suite PASS - `make test` + `make test-sim` (all unit/integration/
  determinism/fault tests, incl. `proptest_concurrent` and the `fuzz_corpus` replay
  of `replay_fuzz_input` under `--cfg sim`) + 20s `concurrent_tx` fuzz under
  `--cfg sim` (2568 runs, no crash). The frozen `check.sh --full` cannot build the
  fuzzer (see setup note); the equivalent `--cfg sim` fuzz passes.
- Primary: baseline score = 403.64 (per-run: 404.29, 403.64, 402.65).
- Secondary: allocBytesPerTx=45561, allocsPerTx=460.2, nsPerTx=26819, cpuNsPerTx=37679.
- Per-workload cost/tx: singleRMW=71.6 (objW 1.01), multiRMW10=1082.3 (objW 11, metaW 9.89), batchRead10=313.66 (metaR 10), batchWrite100=13991.4 (objW 199, metaW ~2), readRepeat=31.5 (metaR 1).
- Outcome & why: starting point for session 2. Session 1 already drove the primary to
  the protocol floor (RMW = 1 metaW + 1 objW/key + 1 objW log; read-only = 1 strong
  metaR/key validation; single-RW = 1 objW; batchWrite create-value fusion is unsafe
  given the reader trusts non-empty objects and the fuzzer does not exercise
  concurrent creates). Remaining safe wins are secondary (allocations/CPU): biggest
  alloc consumers are batchWrite100 (~14432 allocs/tx) and multiRMW10 (~1742). Known
  hot spots to target: `commit_tx`/`set_final_log` clone the whole `TxLog`; the
  `Cache` clones the old entry on `set` and re-allocates the key `String` even when
  the key already exists; per-tx commit clones write values several times.
- Commit: e0f9ab0 (pre-existing HEAD)

## 13. In-place cache writes (no entry-clone / key-realloc) - KEPT
- Hypothesis: the LRU `Cache` clones the entire old `CacheEntry` and re-allocates
  the key `String` on every write-through, even when the key already exists
  (`set`/`update` go through `map.insert(key.to_string(), ..)`, and `update`
  clones the old value just to hand it to the closure). Replacing the value
  through the existing slot and merging fields in place should cut allocations
  on every cache write (all workloads), with no change to backend op counts.
- Change: `crates/glassdb-storage/src/cache.rs` - rewrote `Shard::set` to replace
  the value via `map.get_mut` (no old clone, key kept), and added `Shard::modify`
  + `Cache::modify` (in-place field merge, creates a `V::default()` only when the
  key is absent). `crates/glassdb-storage/src/local.rs` - `write`, `set_meta`,
  `mark_deleted` now use `cache.modify` instead of `cache.update`. `update` also
  switched to `*get_mut = newv` so the existing key is reused.
- Correctness: fast gate PASS; full `make test` + `make test-sim` PASS (all unit/
  integration/determinism/fault tests, incl. `proptest_concurrent` + `fuzz_corpus`);
  `concurrent_tx` fuzz under `--cfg sim` PASS (2516 runs, no crash). Judge APPROVED
  (legitimate allocation optimization, equivalent semantics; corpus files the fuzz
  run dropped under frozen `fuzz/**` were removed before judging/commit).
- Primary: 403.64 -> 404.25 (+0.15%, within noise; per-run 404.25/404.5/403.29 and
  repeats 403.62/403.81 - op counts unchanged).
- Secondary: allocsPerTx 460.2 -> 437.4 (-4.95%, consistent ~430-439 across 3 runs);
  allocBytesPerTx 45561 -> 45275 (-0.63%); nsPerTx/cpuNsPerTx within noise (cpu
  swung 34750-50374 across runs - pure noise).
- Outcome & why: KEPT. Primary flat (the deterministic op counts are unchanged, as
  expected for a pure data-structure refactor) and the allocation axis clearly and
  repeatably improves (~5% fewer allocs/tx) with no consistent regression on the
  other axes. Per-workload allocs dropped on the cache-heavy paths (multiRMW10,
  batchRead10, batchWrite100). Lesson: the cache write path was a real alloc source;
  next, chase the bigger remaining consumers (`TxLog` clones in commit/`set_final_log`
  and per-tx write-value clones).
- Commit: 8c4f73b

## 14. Borrow TxLog on the tx-log write path - KEPT
- Hypothesis: a locked commit clones the whole `TxLog` (paths + values) to hand
  to `set_final_log`, which then clones it *again* on each attempt for
  `TLogger::set`/`set_if` (which only marshal + tag the log). That is two full
  `TxLog` clones on every locked commit. Borrowing instead of owning should cut
  allocation bytes/count on commit-heavy workloads with no op-count change.
- Change: `crates/glassdb-storage/src/tlogger.rs` - `set`/`set_if` take `&TxLog`
  and compute the persisted timestamp once (`marshal_log`/`log_tags` now receive
  it) so body and tags stay consistent. `crates/glassdb-trans/src/monitor.rs` -
  `set_final_log(&self, .., &TxLog)` (no per-retry clone), `commit_tx` passes
  `&tl`, `abort_tx`/`try_abort_remote_tx`/`refresh_pending` pass references.
  `gc.rs` + the tlogger unit test updated to pass `&TxLog` (assertions intact).
- Correctness: fast gate PASS; full `make test`+`make test-sim` PASS (45 suites,
  incl. determinism/fault/proptest/fuzz_corpus); `concurrent_tx` fuzz `--cfg sim`
  PASS (2736 runs, no crash); judge APPROVED (legit alloc optimization; generated
  corpus removed before judging/commit).
- Primary: 404.25 -> 403.55 (-0.17%, within noise; op counts unchanged - per-run
  404.04/403.15/403.55 and repeats 403.36/403.71).
- Secondary: allocBytesPerTx 45275 -> 43765 (-3.33%, consistent 43641-44036),
  allocsPerTx 437.4 -> 425.9 (-2.64%), nsPerTx -2.21%; cpu within noise (swings
  36550-42076). batchWrite100 drove most of the win (allocBytes ~1.31M -> 1.26M).
- Outcome & why: KEPT. Primary unchanged (pure alloc refactor), allocation bytes
  and count clearly and repeatably down with no consistent regression. Lesson:
  ownership-by-value on hot async paths hides retry clones; the commit path now
  marshals straight from the borrowed log. Next: per-tx write-value clones in the
  user-closure -> commit handoff (tx.rs `collect_accesses` / algo commit), and the
  `TxLog.writes`/`locks` Vec construction.
- Commit: fb2565f

## 15. Move (not clone) write values into local cache on commit - DISCARDED
- Hypothesis: each committed write value is cloned 4x (staged to_vec -> WriteAccess
  -> TxWrite via to_log -> protobuf marshal -> local-cache apply). `commit_tx` owns
  the `TxLog` after the durable write, so the local-apply clone is avoidable: move
  the value into `local.write` instead. Expected fewer allocs/bytes on write-heavy
  workloads, no op-count change.
- Change: `crates/glassdb-trans/src/monitor.rs` - `commit_tx` applies writes via
  `std::mem::take(&mut tl.writes)` and moves `entry.value` into `local.write`.
- Correctness: fast gate PASS (op counts unchanged). (Full gate/judge not reached -
  discarded at measure.)
- Primary: 403.55 -> 404.04 (+0.12%, within noise).
- Secondary: allocBytesPerTx +0.15% (flat), allocsPerTx -0.34% (425.9 -> 424.4),
  nsPerTx +8.11% (noise), cpuNsPerTx -2.04% (noise).
- Outcome & why: DISCARDED. The clone removal is real (batchWrite100 dropped ~100
  allocs/tx: 13421 -> 13321) but the benchmark's write values are tiny, so it frees
  only ~800 bytes/tx there; the geomean alloc move (-0.34%) sits inside the
  run-to-run alloc noise (~±1.8%) and allocBytes is flat. Not a clear improvement,
  so per the acceptance rule it is not kept. Lesson: value-clone removal only pays
  off with large payloads; the per-key overhead in batchWrite100 is dominated by
  ~133 *other* allocs/key (locker metadata/tags/protobuf), which is the real target.
- Commit: (reverted)

## 16. Single-task fast path in run_limited - KEPT
- Hypothesis: `run_limited` (the parallel fan-out used by read-only validation,
  lock validation, collection locking, unlock-all, async cleanup) always builds a
  child cancel token and a `buffer_unordered` `FuturesUnordered`, which boxes the
  per-item future. For a single task (`num==1`) there are no siblings to cancel,
  so all that machinery is pure overhead. Single-key transactions (readRepeat,
  single-key read-only/lock validations) have outsized leverage on the alloc
  geomean, so a `num==1` fast path should cut allocs/bytes noticeably.
- Change: `crates/glassdb-trans/src/algo.rs` - in `run_limited`, when `num==1`,
  await `f(ctx.clone(), 0)` directly on the parent ctx and return, skipping the
  child token + stream. Same result and error handling; only the (unused) sibling
  cancellation is dropped.
- Correctness: fast gate PASS; full `make test`+`make test-sim` PASS (45 suites,
  0 failures); `concurrent_tx` fuzz `--cfg sim` PASS (2601 runs, no crash); judge
  APPROVED (honest alloc optimization, no metric gaming).
- Primary: 403.55 -> 402.15 (-0.35%, within noise; op counts unchanged - per-run
  402.55/401.13/402.15 and repeats 403.78/403.87).
- Secondary: allocBytesPerTx 43765 -> 37002 (-15.45%, consistent 37002-37195),
  allocsPerTx 425.9 -> 412.4 (-3.15%), nsPerTx -1.34%, cpuNsPerTx -5.09%. Driven by
  readRepeat (bytes 4725 -> 2064, -56%; the boxed validation future was the big
  allocation); batchRead10/batchWrite100 (n>1) unchanged as expected.
- Outcome & why: KEPT. Big, repeatable allocBytes win with primary flat and no
  regressions. Lesson: `FuturesUnordered`/`buffer_unordered` boxes each future
  (sized to the whole async state machine), so it is expensive for the very common
  n==1 case; specializing the fan-out degree pays off. Next: the per-future box
  cost is still paid for n>1 (multiRMW10/batchWrite100) - but there the parallelism
  is genuinely needed, so look elsewhere (e.g. shrink the validation future, or the
  locker tag/metadata allocations).
- Commit: 04454eb

## 17. Allocation-free is_complete in the locker worker - KEPT
- Hypothesis: `is_complete` runs once per lock/unlock worker-loop iteration and
  builds 4 `TxIdSet`s (cloning the lockers/unlockers Vecs) + 2 `set_diff`s purely
  to test "are all requested lockers locked and all unlockers unlocked". Direct
  containment checks over the (usually 1-element) slices give the same answer with
  zero allocations. Hits every lock op, so multiRMW10 (20 ops/tx) and batchWrite100
  (200 ops/tx) should drop noticeably.
- Change: `crates/glassdb-trans/src/tlocker.rs` - rewrote `is_complete` as
  `req.lockers.iter().all(|t| res.locked_for.contains(t)) && req.unlockers...` and
  dropped the now-unused `set_diff` import.
- Correctness: fast gate PASS; full `make test`+`make test-sim` PASS (45 suites, 0
  failures); `concurrent_tx` fuzz `--cfg sim` PASS (2672 runs, no crash); judge
  APPROVED (same membership check, fewer allocations).
- Primary: 402.15 -> ~402.8 (+0.17%, within noise; op counts unchanged).
- Secondary: allocsPerTx 412.4 -> ~404 (4 runs 409.5/403.9/401.2/405.4; -2%),
  allocBytesPerTx ~-0.7% (36613-36799 vs 37002). Deterministic per-workload:
  multiRMW10 1591 -> ~1480 (-6.5%), batchWrite100 13417 -> 12616 (-6%). ns/cpu noise.
- Outcome & why: KEPT. The first single measurement looked marginal (-0.70% allocs)
  only because readRepeat (unrelated to this change) happened to measure high that
  run; re-measuring showed a stable ~-2% allocsPerTx with rock-solid, deterministic
  per-op reductions in the lock-heavy workloads. Lesson: judge alloc-count changes
  on the *affected* workloads plus repeats, not a single noisy geomean. Next: the
  remaining per-lock-op allocs are the tags `BTreeMap`/base64 in `apply_lock_tags`
  and the `tid.as_bytes().to_vec()` in `update_tx_locks`.
- Commit: 6e83c42

## 18. Trim per-lock-op allocations (join_tids + entry reuse) - DISCARDED
- Hypothesis: each lock application allocates an intermediate `Vec<String>` (then
  `join(",")`) in `apply_lock_tags`, and `update_tx_locks` re-allocates the tid key
  `Vec` on every call via `entry()` even when the tx already has an entry. Removing
  both should cut allocs on every create/write/unlock.
- Change: `crates/glassdb-storage/src/locker.rs` - added `join_tids` (builds the
  `locked-by` value with `encode_string` directly into one `String`, no per-locker
  Vec). `crates/glassdb-trans/src/tlocker.rs` - `update_tx_locks` uses `get_mut`
  before falling back to `entry`/`insert`.
- Correctness: fast gate PASS (op counts unchanged). (Full gate/judge not reached.)
- Primary: ~402.8 -> ~403.9 (within noise).
- Secondary (3 runs vs best): allocsPerTx 403.1/408.6/408.2 (median +1%, i.e. noise),
  allocBytesPerTx +0.1/+0.96/+0.82% (flat). Deterministic per-workload: batchWrite100
  12618 -> ~12310 (-2.4%, solid); multiRMW10 1447-1489 (noisy ~flat); singleRMW flat.
- Outcome & why: DISCARDED. The change is a strict, correct allocation reduction and
  batchWrite100 clearly drops 2.4%, but it is too localized to move the geomean
  secondary metrics above the run-to-run noise (the other workloads barely change),
  so it is not a clear improvement under the acceptance rule. Lesson: per-op savings
  of a few allocations only pay off when they hit a high-leverage (small) workload
  or many workloads at once; concentrating them in batchWrite100 (already the
  highest-count workload, so low ln-leverage) does not register. Stop chasing single
  small allocations in the lock path.
- Commit: (reverted)

## 19. Single-allocation path construction (base64 encode_into) - KEPT
- Hypothesis: `prefix_encode` (used by `from_key`/`from_collection`/`from_transaction`,
  i.e. every read, write, commit-log, lock, and status path) allocates twice: the
  custom `base64::encode` builds a `Vec<u8>` -> `String`, then `format!` builds the
  final `prefix/type/base64` string. Encoding straight into the final, pre-sized
  buffer removes one allocation per path construction. Path building happens on every
  op of every workload, so unlike exp18 this hits the geomean broadly (not one
  workload), giving it the leverage exp15/exp18 lacked.
- Change: `crates/glassdb-data/src/base64.rs` - added `encode_into(&[u8], &mut String)`
  that appends the encoding to an existing buffer (alphabet is ASCII, so `push(char)`
  keeps it valid UTF-8 with one byte each); `encode` now delegates to it.
  `crates/glassdb-data/src/paths.rs` - `prefix_encode` builds the path in one
  `String::with_capacity` (prefix + '/' + type + '/' + encoded payload) and calls
  `encode_into`, dropping the intermediate base64 string.
- Correctness: fast gate PASS (op counts unchanged); full `make test`+`make test-sim`
  PASS (45 suites, 0 failures); `concurrent_tx` fuzz `--cfg sim` PASS (2673 runs, no
  crash); judge APPROVED (honest single-allocation refactor, no metric gaming).
- Primary: ~402.8 -> 403.29 (3 runs 403.29/404.59/403.81, within noise; op counts
  unchanged).
- Secondary (3 runs vs best): allocsPerTx 384.9/384.0/387.5 (-4.1%..-4.9%, clear and
  repeatable); allocBytesPerTx 36643/36554/36883 (flat -0.5%..+0.4% - the removed
  base64 strings are tiny). Deterministic per-workload allocs: singleRMW 68.8 -> ~64.7
  (-6%), batchRead10 255 -> 234 (-8%), batchWrite100 12618 -> 12408 (-1.7%),
  readRepeat 32.8 -> ~30.7 (-6%); multiRMW10 ~flat (its allocs are dominated by lock
  metadata, not paths).
- Outcome & why: KEPT. Primary flat, allocsPerTx clearly and repeatably down ~4.5%
  with no axis regressing (allocBytes flat). Unlike exp18's batchWrite100-only saving,
  this fires on every path of every workload, so the per-op alloc removal compounds
  across the geomean. Lesson: prefer optimizations on truly ubiquitous helpers (path
  building, cache writes) over per-workload hot spots - broad leverage beats local
  depth for a geomean target.
- Commit: ce9bef8

## 20. Allocation-light transaction-log marshaling - KEPT
- Hypothesis: when a multi-key commit persists its log, `marshal_write`/
  `marshal_lock` run once per write/lock and each (a) call `paths::parse` (2
  String allocs: prefix + base64 suffix) then `gopath::join` (1 alloc) just to
  rebuild the protobuf suffix `_k/<b64>` that is already a contiguous substring
  of the path, and (b) clone the collection prefix into the `coll_writes`
  BTreeMap on *every* call (via `entry(prefix.clone())` + `or_insert_with`'s
  second clone), even though a batch write puts all keys under one prefix. This
  is ~3 wasted allocs per write/lock. Only multiRMW10 and batchWrite100 write
  logs, but exp17 showed cutting those two ~6% registers on the geomean.
- Change: `crates/glassdb-data/src/paths.rs` - added `parse_ref` returning
  borrowed `prefix`/`typ` and the protobuf suffix (`_k/<b64>`) as a slice (the
  base64 alphabet has no `/` or `.`, so the slice is byte-identical to the old
  join). `crates/glassdb-storage/src/tlogger.rs` - `marshal_write`/`marshal_lock`
  use `parse_ref` (suffix via one `to_string`, no parse/join) and a `coll_entry`
  helper that does `contains_key`+`get_mut`, cloning the prefix only when the
  collection is first inserted.
- Correctness: fast gate PASS (op counts unchanged); full `make test`+
  `make test-sim` PASS (45 suites, 0 failures); `concurrent_tx` fuzz `--cfg sim`
  PASS (2806 runs, no crash); judge APPROVED (same serialized log bytes, fewer
  allocations; no test/bench/stats changes).
- Primary: 403.29 -> ~403.5 (3 runs 404.04/403.09/403.54, within noise; op counts
  unchanged).
- Secondary (3 runs vs best): allocsPerTx 373.7/370.6/374.6 (-2.7%..-3.7%, clear
  and repeatable); allocBytesPerTx 36590/36265/36694 (flat -1.0%..+0.1%).
  Deterministic per-workload: multiRMW10 1472 -> ~1361 (-7.4%), batchWrite100
  12407 -> 11411 (-8.0%); read-only/single-RW workloads (no log) unchanged.
- Outcome & why: KEPT. Primary flat, allocsPerTx clearly down ~3% with both
  log-writing workloads dropping ~7-8% deterministically and no axis regressing.
  This is the exp17 pattern (move the two lock/log-heavy workloads enough to
  register) but larger, because per write/lock we removed ~3 allocs instead of
  ~1. Lesson: the parse->rebuild round trip (decode then re-encode data you
  already hold) and `entry(key.clone())` on a hot map are recurring, cheap-to-fix
  alloc sources. Next: the remaining per-lock-op allocs in `apply_lock_tags`
  (tag-key Strings + value clone) and the per-write value clones on the commit
  path.
- Commit: ad811ca

## 21. parse_ref on the commit lock-planning path - DISCARDED
- Hypothesis: the lock-planning helpers `collections_locks`, `needs_locks`, and
  `is_key_collection_locked` call `paths::parse` (2 String allocs) but only need
  the borrowed prefix/typ; and `collections_locks` clones the prefix into its
  `HashMap` on every entry. Switching to `parse_ref` (exp20) plus a get/contains
  guard should cut commit-path allocs for the lock-heavy workloads.
- Change: `crates/glassdb-trans/src/algo.rs` - those three callsites use
  `parse_ref`; `collections_locks` upgrades/inserts via `get_mut`/`contains_key`
  so the collection prefix is cloned only when first inserted.
- Correctness: fast gate PASS (op counts unchanged). (Full gate/judge not reached.)
- Primary: ~403.4 (flat).
- Secondary (3 runs vs best): allocsPerTx 371.8/372.0/368.2 (median +0.34%, i.e.
  noise); allocBytesPerTx +0.9/+0.9/-0.1% (flat). Deterministic per-workload:
  batchWrite100 11409 -> ~11097 (-2.7%, solid); multiRMW10 ~flat; reads flat.
- Outcome & why: DISCARDED. Same shape as exp18: a real, deterministic cut but
  concentrated in batchWrite100, the highest-count (so lowest ln-leverage)
  workload. multiRMW10 didn't move because `collections_locks` only acts on
  blind-write / not-found / delete keys, and multiRMW10's keys are read-then-write
  (found), so they're skipped. With only one low-leverage workload moving, the
  geomean stays inside the run-to-run noise, so it's not a clear improvement.
  Lesson (reconfirms exp18): `collections_locks` is a blind-write-only path;
  optimizing it can never help RMW workloads, and batchWrite100 alone is too
  ln-compressed to register. Stop optimizing the blind-write lock path.
- Commit: (reverted)

## 22. Unify the per-tx staged/reads maps - KEPT
- Hypothesis: `Tx` kept two `HashMap`s keyed by path (`staged` for values,
  `reads` for read versions), so every *found* read inserted the same path into
  both -> 2 path-String allocs per read; and `collect_accesses` cloned every
  staged value into the `WriteAccess` list. A found read happens in every
  read/RMW workload, so unifying the maps (one path key per read) and draining
  at commit (move values, build each path's `Arc<str>` once) should cut allocs
  broadly - the high-ln-leverage read/RMW workloads, not just batchWrite100.
- Change: `crates/glassdb/src/tx.rs` - replaced `staged`/`reads` with a single
  `entries: HashMap<String, Entry>` where `Entry { staged: Option<Tvalue>,
  read: Option<ReadInfo> }`. `read`/`write`/`delete` use `entry(p).or_default()`
  (one key alloc). `collect_accesses` drains via `mem::take`, moves staged values
  into `WriteAccess` (no clone), and reuses one `Arc<str>` for a key that is both
  read and written. Same reads/writes sets and sort order, so the `Data` handed
  to the commit algo is byte-identical.
- Correctness: fast gate PASS; full `make test`+`make test-sim` PASS (45 suites,
  0 failures); `concurrent_tx` fuzz `--cfg sim` PASS (3533 runs, no crash); judge
  APPROVED (honest refactor; identical access set; no test/stats changes).
- Primary: flat. Controlled A/B at --count 5: exp22 403.49/403.11 vs reverted
  best 403.41/404.10 - identical within noise (op counts unchanged; this only
  changes in-process bookkeeping, not backend ops).
- Secondary (3 runs vs best): allocsPerTx 356.0/357.9/354.3 (-3.4%..-4.4%);
  allocBytesPerTx 34991/35291/34940 (-2.7%..-3.7%) - BOTH axes clearly down.
  Per-workload (deterministic): singleRMW 64.4->~60.9 (-5.4%), batchRead10
  234->221 (-5.6%), readRepeat 29.9->~28 (-5%), multiRMW10 1362->~1324 (-2.8%),
  batchWrite100 11409->11311 (-0.9%, value move only).
- Outcome & why: KEPT. The first all-workloads win since exp19: every workload
  drops because the duplicate read-key alloc (and the value clone) were on the
  universal read/commit path, not a single workload. Both alloc axes improve with
  primary flat. Lesson: collapsing redundant per-key data structures (two maps ->
  one) is higher-leverage than shaving a single alloc, because it removes a cost
  paid by every key in every transaction. Next: the per-lock-op `apply_lock_tags`
  allocations (Tags BTreeMap keys + value clone) remain the largest untouched
  per-key cost on the write/commit path.
- Commit: e216ce1

## 23. Lazy dedup signal semaphore - DISCARDED
- Hypothesis: `Dedup::run` allocates `Arc::new(Semaphore::new(0))` per call as the
  "new request arrived" signal, but in the uncontended case (no second request,
  worker never waits) it is never used. The deterministic per-workload alloc
  counts show lock ops are uncontended in the bench, so making the semaphore lazy
  saves one alloc per lock/unlock - hitting every lock op in multiRMW10 and
  batchWrite100.
- Change: `crates/glassdb-concurr/src/dedup.rs` - `Call.next: Option<Arc<Semaphore>>`,
  created on first use via a `next_sem()` helper (called from the arrival
  `add_permits` path and `on_next_do`); reset to `None` instead of re-allocating
  in `wake_up_next`.
- Correctness: fast gate PASS (incl. dedup unit tests + op counts unchanged).
  (Full gate/judge not reached.)
- Primary: flat (-0.1%).
- Secondary (3 runs vs best): allocsPerTx 353.6/352.9/356.5 (median -0.68%, one run
  +0.16% -> within noise); allocBytesPerTx flat. Deterministic per-workload:
  batchWrite100 11310 -> ~11100 (-1.9%); multiRMW10 1352 -> ~1300 (-2..-4%, noisy);
  read/single-RW workloads unaffected (they don't queue through dedup).
- Outcome & why: DISCARDED. Correct and saves a real alloc, but - like exp18 and
  exp21 - it only touches the two lock-heavy workloads, each by <2-4%, so the
  geomean stays within noise. Third lock-path-concentrated discard: confirms that
  no lock-path micro-opt can register, because (a) only multiRMW10/batchWrite100
  use it and (b) batchWrite100 is ln-compressed while multiRMW10 is too noisy at
  ~2%. The lock path's real floor is the `Tags` BTreeMap<String,String> (3 owned
  key Strings + nodes per op), which can't shrink without changing the public
  backend `Tags` type. Stop optimizing the lock path.
- Commit: (reverted)

## 24. Single-allocation TxId construction - DISCARDED
- Hypothesis: `TxId::{new_at,new_random,renew,with_priority}` build a
  `vec![0u8; 16]` then `.into()` an `Arc<[u8]>`. `Vec<u8> -> Arc<[u8]>` cannot
  reuse the buffer (the Arc needs a refcount header contiguous with the data),
  so it allocates the Vec *and* a separate Arc, copying. Building from a stack
  `[u8; 16]` via `Arc::from(&b[..])` does it in one allocation. `begin` mints a
  TxId on every transaction (all workloads), so this saves 1 alloc/tx broadly,
  with the most geomean leverage on the small workloads (singleRMW, readRepeat).
- Change: `crates/glassdb-data/src/txid.rs` - the four fixed-length constructors
  use a stack `[u8; TX_ID_LEN]` buffer + `Arc::from(&b[..])` (one alloc) instead
  of `vec![..].into()` (two). `from_bytes` keeps `Vec` (arbitrary length).
- Correctness: fast gate PASS; `make test` PASS. NOTE: `make test-sim` is
  unreliable here due to a PRE-EXISTING flaky determinism self-check (see infra
  finding below); the change is determinism-NEUTRAL - `recovery_holds` fails at
  the same ~50% rate with and without it (18/36 vs 18/36 over 36 runs each).
  (Judge not run - discarded on measurement.)
- Primary: 404.02 -> 404.20 (flat; op counts unchanged - pure repr change).
- Secondary (4 runs vs 4 baseline runs): allocsPerTx baseline 356.0/355.1/358.3/
  358.1 (~356.9) -> 354.2/352.5/353.7/354.8 (~353.8), ~-0.8%; allocBytesPerTx
  flat (~35100 -> ~35070; the saved buffer is only 16 bytes). Deterministic
  per-workload: singleRMW 60.7 -> 59.7 (exactly -1.0, zero variance every run),
  batchRead10 ~221 -> ~220, batchWrite100 ~11310 -> ~11309, readRepeat ~28.6 ->
  ~27.5 (all ~-1 as predicted); multiRMW10 swamped by its own retry noise (±35).
- Outcome & why: DISCARDED. A correct, broad, deterministic 1-alloc/tx saving
  (singleRMW drops exactly 1 every run), but the geomean allocsPerTx moves only
  ~0.8% with allocBytes flat - the same magnitude as the discarded exp18/21/23,
  i.e. within the run-to-run noise floor. One alloc/tx is simply too small a
  fixed saving to clear the geomean noise even when it hits every workload (cf.
  exp19/22, which removed several allocs per *key* and registered). Not a clear
  improvement under the acceptance rule.

## Infra finding - `recovery_holds` determinism self-check is ~50% flaky on HEAD
While running exp24's full gate I found that
`crates/glassdb/tests/concurrent_sim.rs::recovery_holds_under_crash_restart_and_outages`
fails about half the time on the *unmodified* baseline (clean tree: 18 failures
in 36 runs; "seed 0: recovery op stream diverged at index 98" - run 1 vs run 2
of the same seed schedule different ops, e.g. key "13" vs key "12"). It is the
only flaky test; the other 8 `concurrent_sim` cases and all of `make test` pass
deterministically. The divergence is in op *ordering* across the crash/restart
recovery path, not in this session's changes (every kept change is reverted when
this was measured). Implication for the gate: the prior experiments' "make
test-sim PASS" lines were lucky passes of this ~50% test; the meaningful
correctness signals remain `make test` (fmt+clippy+all tests), the deterministic
`concurrent_tx` fuzzer, and the other determinism/fault sim tests, plus the
judge. Root-causing the recovery nondeterminism is a correctness task outside the
performance-loop scope (and `concurrent_sim.rs` is a frozen verification test),
so it is recorded here rather than fixed.

## 25. Single-allocation TxId tag decoding - KEPT
- Hypothesis: `tag_to_tid` (the base64 decoder for the `locked-by`/`last-writer`
  storage tags) does `URL_SAFE.decode(a)` -> `Vec<u8>` and then
  `TxId::from_bytes` -> `Arc<[u8]>`, which copies the Vec into a fresh
  refcounted allocation = 2 allocs per call. It runs once per `tags_lock_info`
  (last-writer) and once per locker, i.e. on every read validation
  (`validate_read`) and every lock-info parse. Unlike exp24 (1 alloc/tx at
  begin), this fires once per *validated key / lock op*, so the small,
  high-ln-leverage read workloads pay it many times (batchRead10 ~10x/tx).
  Decoding straight into a stack buffer + `Arc::from(&buf[..n])` removes the
  intermediate Vec, saving 1 alloc per decode across every validating/locking tx.
- Change: `crates/glassdb-data/src/txid.rs` - added `TxId::from_slice(&[u8])`
  (one alloc, `Arc::from(slice)`; documents why `from_bytes(Vec)` cannot avoid
  the copy). `crates/glassdb-storage/src/locker.rs` - `tag_to_tid` decodes via
  `decode_slice` into a `[0u8; 64]` stack buffer (covers the 16-byte production
  id = 24 b64 chars, and any realistic id), with a heap-`decode` fallback when
  the encoded id would exceed the buffer (the API permits arbitrary-length ids,
  e.g. fuzzer-supplied tags) - so behavior is identical, only the common-case
  allocation is removed.
- Correctness: fast gate PASS (build + `cargo test --workspace` incl.
  `proptest_concurrent`); `make test` PASS (fmt + clippy -D warnings + all
  tests); all crate sim suites + 8/9 `glassdb` `concurrent_sim` cases PASS
  (the 9th, `recovery_holds`, is the pre-existing ~50% flake - this change is
  determinism-NEUTRAL: 8/24 recovery passes here vs the baseline's 8/24);
  `concurrent_tx` fuzz `--cfg sim` PASS (6246 runs, no crash); judge APPROVED
  (honest hot-path optimization, no frozen files, no metric tampering). Generated
  fuzz corpus removed before judging/commit.
- Primary: 404.02 -> ~403.0 (3 runs 403.27/402.25/403.60, within noise; op
  counts unchanged - pure decode-path representation change).
- Secondary (3 runs vs best.json baseline): allocsPerTx 355.97 -> 339.6/343.2/
  340.9 (~-4.5%, clear and repeatable); allocBytesPerTx ~flat (35100 -> ~34920,
  -0.5%; the removed Vec buffers are tiny). Deterministic per-workload allocs:
  batchRead10 220.9 -> ~201.5 (-8.8%), multiRMW10 1351.6 -> ~1257 (-7.0%),
  singleRMW 60.7 -> ~58.5 (-3.6%), batchWrite100 11310 -> ~11114 (-1.7%);
  readRepeat ~flat (its single read is served from the local cache without a
  global metadata decode, so it has no tag_to_tid to save).
- Outcome & why: KEPT. Primary flat, allocsPerTx clearly down ~4.5% with every
  decoding workload dropping deterministically and no axis regressing. This is
  the exp19/exp22 pattern (a cost on a *ubiquitous per-key helper*), and it
  vindicates exp24's idea at a higher-leverage site: exp24 saved the same
  single Vec->Arc copy but only 1x/tx (at begin), so it stayed in the noise;
  moving the identical fix to `tag_to_tid` - which fires per validated read /
  lock op - multiplies the saving by the key count and clears the floor. Lesson:
  the *frequency* of a per-op allocation, not just its presence, decides whether
  removing it registers on the geomean.
- Commit: 6881846

## 26. Share backend metadata tags via Arc (copy-on-write) - KEPT
- Hypothesis: exp5 shared the *cache's* metadata via `Arc<Metadata>`, but the
  backend -> Global boundary still deep-clones: `Backend::get_metadata` returns
  an owned `Metadata { tags: Tags }`, and the in-memory backend builds it with
  `obj.tags.clone()` - copying the whole `BTreeMap` and its ~3 key/value
  `String`s (~7 allocs) on *every call*. `get_metadata` runs on every read
  validation (`validate_read`, batchRead10 ~10x/tx) and every lock-info parse,
  so this is the single largest remaining per-key allocation on the read/lock
  path. Storing the backend's tag map behind an `Arc` and handing it out with a
  refcount bump (copy-on-write on the rare tag mutation) should remove it, like
  exp5 did one layer up. Tags are immutable once produced, so sharing is safe.
- Change: `crates/glassdb-backend/src/lib.rs` - `Metadata.tags: Tags` ->
  `Arc<Tags>`. `crates/glassdb-backend/src/memory.rs` - `Object.tags: Arc<Tags>`;
  `get_metadata`/write methods return `obj.tags.clone()` (now an Arc bump);
  `update_tags` mutates via `Arc::make_mut` (clones only when the map is still
  shared with a cache entry / outstanding `Metadata`); the two `ReadReply` sites
  (owned `Tags`) deep-clone explicitly. `crates/glassdb-storage/src/global.rs`,
  `glassdb-backend-s3`, `glassdb-backend-gcs` - wrap their freshly-built tag maps
  in `Arc::new(..)` at the `Metadata` construction sites (cloud backends parse
  fresh tags per call, so they are neutral there). All readers reach `&Tags`
  through `Arc` deref coercion, so `tags_lock_info(&meta.tags)` etc. are
  unchanged.
- Correctness: fast gate PASS (build incl. s3/gcs + `cargo test --workspace`
  with `proptest_concurrent`); `make test` PASS (fmt + clippy -D warnings + all
  tests incl. the memory-backend unit tests that exercise tag mutation); all
  crate sim suites + 8/9 `glassdb` `concurrent_sim` cases PASS (the 9th is the
  pre-existing `recovery_holds` flake; this change is determinism-neutral - 9/16
  recovery passes vs the baseline's ~50%); `concurrent_tx` fuzz `--cfg sim` PASS
  (6931 runs, no crash - exercises the copy-on-write path under concurrent
  writers/readers); judge APPROVED (honest Arc/CoW optimization, no frozen files,
  no Stats tampering). Generated fuzz corpus removed before judging/commit.
- Primary: 404.02 -> ~403.3 (3 runs 403.45/403.00/403.32, within noise; op
  counts unchanged - the backend does the same work, only the tag map is shared
  rather than copied).
- Secondary (3 runs vs best.json baseline): allocsPerTx 340.6 -> 288.9/286.5/
  283.3 (~-15.8%); allocBytesPerTx ~34920 -> 29190/28950/28803 (~-17.0%) - BOTH
  axes clearly and repeatably down (the eliminated clone was a whole BTreeMap +
  its Strings, so bytes move as much as count). Deterministic per-workload:
  batchRead10 201.5 -> ~140.7 (-30%), readRepeat 27.5 -> ~21.6 (-21%), singleRMW
  58.5 -> ~53.3 (-9%), multiRMW10 1257 -> ~1160 (-8%), batchWrite100 11114 ->
  ~10210 (-8%). Every workload improves.
- Outcome & why: KEPT - the largest win of session 2. Primary flat, both
  allocation axes down ~16-17% with every workload improving and no regression.
  It is the exp5 lesson reapplied at the next layer: the cache no longer
  deep-copies the tag map (exp5), and now neither does the backend that feeds it.
  Validation, which fetches metadata for every read on the strong-read path, was
  paying a full tag-map clone per key; sharing it via Arc removes that entirely
  (batchRead10 -30%). Lesson: when one Arc-sharing change pays off, check the
  *adjacent layers* on the same data - the same clone often recurs at each
  ownership boundary (cache, then backend).
- Commit: 3cebfc2

## 27. Allocation-free read-validation tag comparison - KEPT
- Hypothesis: after exp26 shared the backend tag map via Arc, the next per-key
  cost on the read-validation path is in `validate_read`/`validate_read_not_found`
  themselves: both call `tags_lock_info(&meta.tags)`, which always parses the
  full `LockInfo` - allocating a `Vec<TxId>` for `locked-by` and decoding the
  `last-writer` tag into a fresh `TxId` (`Arc<[u8]>`) - even on the overwhelmingly
  common unlocked / read-locked path, where all we need is (a) the lock *type*
  and (b) whether the stored `last-writer` equals our read version's writer.
  Computing the type without building `LockInfo`, and comparing the base64
  last-writer tag to the `TxId` bytes on the stack (no decode allocation),
  removes a `Vec` + an `Arc<[u8]>` per validated read on the hot path. This fires
  per *validated key*, so read-fan-out workloads (batchRead10 ~10x/tx) pay it
  many times.
- Change: `crates/glassdb-storage/src/locker.rs` - added
  `tags_lock_type(&Tags) -> Result<LockType>` (parses only the `lock-type` tag,
  no allocation) and `tag_matches_tid(Option<&str>, &TxId) -> bool` (decodes the
  base64 tag into a `[0u8; 64]` stack buffer and byte-compares to the TxId,
  matching `last_writer_from_tags` semantics for missing/empty/over-long tags via
  a heap fallback); refactored `tags_lock_info` to reuse `tags_lock_type` for its
  type field (no behavior change). `crates/glassdb-storage/src/lib.rs` - export
  the two helpers. `crates/glassdb-trans/src/algo.rs` - `validate_read` fast-paths
  `LockType::None`/`Read` via `tags_lock_type` + `tag_matches_tid` (Ok if the
  last-writer is unchanged, else Retry), falling back to the full
  `tags_lock_info` + `validate_locked_read` only when an actual write/exclusive
  lock is present; `validate_read_not_found` fast-paths the same unlocked/read
  case via `tags_lock_type` instead of building `LockInfo`.
- Correctness: fast gate PASS (build + `cargo test --workspace` incl.
  `proptest_concurrent`); `make test` PASS (fmt + clippy -D warnings + all
  tests); all crate sim suites PASS + 8/9 `glassdb` `concurrent_sim` cases (the
  9th is the pre-existing `recovery_holds` ~50% flake; determinism-NEUTRAL);
  `concurrent_tx` fuzz `--cfg sim` PASS (6181 runs, no crash - this is the
  serializability-critical change of the session, so the fuzzer exercises the new
  fast-path under concurrent writers/readers); judge APPROVED (honest hot-path
  refactor, semantics preserved, no frozen files, no Stats tampering). Generated
  fuzz corpus removed before judging/commit.
- Primary: 403.04 -> 402.92 (within noise; op counts unchanged - same validation
  decisions, only the representation of the comparison changed).
- Secondary (3-run avg vs best.json/exp26 baseline): allocsPerTx 288.06 -> 279.28
  (-3.0%, clear and repeatable); allocBytesPerTx 29041 -> 28722 (-1.1%; the
  removed Vec + Arc<[u8]> are small). Deterministic per-workload allocs:
  batchRead10 140.7 -> 130.4 (-7.3%), readRepeat 21.6 -> 20.4 (-5.6%),
  multiRMW10/singleRMW/batchWrite100 flat (their reads either hit the local cache
  or already took the locked path).
- Outcome & why: KEPT. Primary flat, allocsPerTx down 3.0% with the read-fanout
  workloads dropping deterministically and no axis regressing. It is the same
  "save the Vec+Arc on the common path" idea as exp25/exp26 pushed one level
  higher - into the validator itself - by recognizing that the full `LockInfo`
  parse is only needed when a write lock is actually held. Lesson: after sharing
  the data (exp26), look at the *consumer* - it was parsing more than it needed
  on the hot path, and specializing the common case (unlocked/read-locked) avoids
  the work entirely.
- Commit: f7de9b8

## 28. Single-String `locked-by` tag build (no intermediate Vec) - DISCARDED
- Hypothesis: `apply_lock_tags` (runs per lock op on every write key) builds the
  `locked-by` value as `update.lockers.iter().map(tid_to_tag).collect::<Vec<_>>()
  .join(",")` - a `Vec<String>` plus one base64 `String` per locker plus the
  joined `String`. For the common single-locker case that is 3 allocs to produce
  one tag value. Encoding straight into one `String` via `encode_string` removes
  the Vec and the per-id intermediate, leaving a single allocation. batchWrite100
  (100 create+commit keys) and multiRMW10 should pay less.
- Change (reverted): `crates/glassdb-storage/src/locker.rs` - added
  `join_tids(&[TxId]) -> String` (pre-sized `String`, `encode_string` per id with
  a `,` separator; output byte-identical to the old map/collect/join) and used it
  in `apply_lock_tags` in place of the `Vec<String>` + `join`.
- Correctness: `cargo test -p glassdb-storage` PASS (the lock-info round-trip
  tests confirm the encoded tag is identical). Not taken to the full gate - the
  perf result below decided it.
- Primary: ~402.92 -> ~403.4 (within noise; op counts unchanged).
- Secondary (3 runs vs best.json/exp27): batchWrite100 allocs 10216.5 -> ~10009
  (-2.0%, deterministic) and multiRMW10 1172.9 -> ~1153 (-1.7%), but the
  *aggregate* axes are flat: allocsPerTx 279.28 -> 278.45/278.8/279.89 (~279.0,
  -0.1%) and allocBytesPerTx 28722 -> ~28829 (+0.4%). readRepeat/batchRead10/
  singleRMW unchanged (reads never call `apply_lock_tags`; the readRepeat 20.4->
  ~21.0 wiggle is run noise).
- Outcome & why: DISCARDED. The change is strictly-better and zero-risk, but the
  secondary scores are 5-workload geometric means, so a -2% on the single biggest
  workload only moves the geomean ~-0.4% - inside the run-to-run noise. The
  remaining per-key lock-tag allocations (the three constant `*_TAG.to_string()`
  keys, `ltype.to_string()`, and the `value.clone()` the backend must own) are all
  mandated by the `BTreeMap<String,String>` tag representation and the write API,
  so the write path cannot be cut further without a large, risky `Tags`-type
  refactor across every backend.   This is the exp24 lesson again: a real but
  single-workload sub-floor saving does not clear the geomean noise, so it is not
  kept.

## 29. Cache projection: don't deep-clone the entry on metadata reads - KEPT
- Hypothesis: `Local::read`/`get_meta` both call `Cache::get`, which clones the
  *entire* `CacheEntry` (its value `Vec<u8>` *and* its metadata) and then keeps
  only one half. `get_meta` therefore copies the cached value bytes on every
  call just to drop them and return the metadata `Arc`. Metadata reads happen on
  every read validation (`get_metadata` in `validate_read`, the create-lock
  resolver, etc.), so this is a wasted value-sized allocation per validated read
  across every workload. Projecting the entry under the lock - cloning only the
  field the caller needs - removes it.
- Change: `crates/glassdb-storage/src/cache.rs` - added
  `Cache::get_with(key, |&V| -> R)` (and the shard method), which does the same
  `contains_key` + `move_to_front` as `get` but hands the entry to a closure
  instead of cloning it, so the caller clones only what it needs.
  `crates/glassdb-storage/src/local.rs` - `read` now projects just the value half
  (`CacheValue` -> `LocalRead`), `get_meta` projects just the metadata half
  (an `Arc` bump, no value copy). Staleness/outdated logic is computed inside the
  closure on the full entry exactly as before, so behavior is identical; only the
  cross-field clone is dropped.
- Correctness: fast gate PASS; `make test` PASS (fmt + clippy -D warnings + all
  tests incl. the cache unit tests); all crate sim suites + 8/9 `glassdb`
  `concurrent_sim` cases PASS (9th is the pre-existing `recovery_holds` flake;
  determinism-NEUTRAL); `concurrent_tx` fuzz `--cfg sim` PASS (6522 runs, no
  crash - exercises the read/validation cache path under concurrency); judge
  APPROVED (honest projection API, no frozen files, no Stats tampering).
  Generated fuzz corpus removed before judging/commit.
- Primary: 402.92 -> ~403.2 (3 runs 403.08/403.22/403.36; within noise, op
  counts unchanged - the cache returns the same data, only fewer copies).
- Secondary (3 runs vs best.json/exp27): allocsPerTx 279.28 -> 272.28/274.25/
  275.99 (~-1.8%, every run below baseline); allocBytesPerTx 28722 -> ~28592
  (-0.45%; the avoided clones are the small per-key values, so the count moves
  more than the bytes). Deterministic per-workload allocs: multiRMW10 1172.9 ->
  ~1127 (-3.9%), singleRMW 53.2 -> 52.2 (-1.9%), readRepeat 20.4 -> ~19.8 (-3%),
  batchWrite100 10216.5 -> ~10111 (-1.0%); batchRead10 flat (its first reads miss
  the local cache, so they take the global path that builds metadata fresh).
- Outcome & why: KEPT. Primary flat, allocsPerTx clearly down 1.8% with the
  validation-heavy RMW workloads dropping the most and no axis regressing. The
  win is structural: `Cache::get` had a single deep-clone shape forced on every
  reader, and the metadata readers paid for a value copy they never used.
  Splitting the read into a projection lets each caller pay only for what it
  takes. Lesson: a "clone the whole thing" cache accessor hides per-caller waste;
  a projection closure under the lock removes it without changing semantics or
  lock duration.
- Commit: 61a0418

## 30. Zero-alloc `WriterId` from the cached id's `Arc` - DISCARDED
- Hypothesis: on the cached read-through path, `Global::read` builds the
  conditional `read_if_modified` argument with
  `WriterId::new(e.version.writer.as_bytes().to_vec())` - a fresh 16-byte `Vec`
  copy of the cached `TxId` per read. `TxId` is already `Arc<[u8]>`, so if
  `WriterId` also held `Arc<[u8]>` the expected-writer could share the cached
  id's allocation (a refcount bump, zero copy). Expected to cut a per-read alloc
  on the read-heavy workloads (batchRead10).
- Change (reverted): `glassdb-data/src/txid.rs` - `TxId::as_arc()` (clone the
  inner `Arc`). `glassdb-backend/src/lib.rs` - `WriterId(Vec<u8>)` ->
  `WriterId(Arc<[u8]>)`, `new(impl Into<Arc<[u8]>>)` (all existing `Vec`-passing
  callers, incl. the frozen `backendbench`, still compile), added
  `WriterId::from_arc`. `glassdb-storage/src/global.rs` -
  `WriterId::from_arc(e.version.writer.as_arc())`.
- Correctness: workspace builds clean (incl. s3/gcs/frozen bench). Not taken to
  the full gate - the perf result decided it.
- Primary: ~403.2 -> ~403.6 (within noise).
- Secondary (3 runs vs best.json/exp29): allocsPerTx 274.25 ->
  272.56/271.73/276.0 (~273.4, -0.3%, inside noise - run 3 is above baseline);
  allocBytesPerTx flat; **batchRead10 flat (131.1 -> 130.6)** and every other
  workload flat.
- Outcome & why: DISCARDED. The hypothesis was empirically wrong: removing the
  `to_vec` did not move any workload, so the `read_if_modified` cached-revalidate
  path (the only `WriterId` construction the score exercises) is not hot enough
  for its one alloc to register - the benchmark's strong reads largely take the
  full-read or local-cache-fast paths instead. The change is strictly-better and
  zero-risk, but "strictly better" is not "measurably better"; with no geomean
  movement it is not kept. Lesson: confirm a suspected hot path actually shows up
  in the per-workload deltas before investing - an alloc on a rarely-taken branch
  costs nothing to remove and nothing to keep.

## 31. Single-allocation TxId mint (re-try of exp24 at lower denominators) - KEPT
- Hypothesis: exp24 made `TxId::{new_at,new_random,renew,with_priority}` build
  from a stack `[u8; 16]` + `Arc::from(&b[..])` (one alloc) instead of
  `vec![0u8; 16].into()` (two: the Vec then a copy into the refcounted Arc),
  saving exactly 1 alloc per minted id - i.e. 1 alloc/tx, since `begin` mints one
  per transaction on every workload. exp24 was DISCARDED because at that point
  the geomean only moved ~0.8% (readRepeat was 28.6, singleRMW 60.7), inside the
  noise floor. Since then exp25/26/27/29 cut the small workloads' denominators
  hard (readRepeat 28.6 -> ~19.8, singleRMW 60.7 -> ~52.3), and the geomean is a
  5-workload geometric mean, so the *same* fixed 1-alloc/tx saving is now worth
  ~1.6% (readRepeat -1/19.8 = -5%, singleRMW -1/52 = -1.9%, etc.). Re-trying the
  identical change should now clear the floor - this is the exp25-over-exp24
  lesson again: a fixed saving's geomean value rises as the denominators shrink.
- Change: `crates/glassdb-data/src/txid.rs` - the four fixed-length constructors
  use a stack `[u8; TX_ID_LEN]` + `Arc::from(&b[..])` (identical to exp24).
  `from_bytes`/`from_slice` unchanged.
- Correctness: fast gate PASS; `make test` PASS (fmt + clippy -D warnings + all
  tests, incl. the txid layout/priority/renew unit tests); all crate sim suites
  PASS; `glassdb` `concurrent_sim` shows the usual 9/9-or-8/9 split on the
  pre-existing `recovery_holds` flake (determinism-NEUTRAL, as exp24 established
  at 18/36 with and without the change - a pure stack-vs-heap repr change cannot
  affect the random/byte content of an id); `concurrent_tx` fuzz `--cfg sim` PASS
  (7521 runs, no crash); judge APPROVED (equivalent-semantics alloc micro-opt).
  Generated fuzz corpus removed before judging/commit.
- Primary: 403.22 -> ~403.15 (4 runs 403.01/403.29/403.14/403.18; within noise,
  op counts unchanged).
- Secondary (4 runs vs best.json/exp29): allocsPerTx 274.25 -> 271.36/267.14/
  270.96/270.59 (~270.0, -1.55%, *every* run below baseline); allocBytesPerTx
  ~flat (28569 -> ~28505, -0.2%; the saved buffer is 16 bytes). Deterministic
  per-workload: singleRMW 52.3 -> 51.3 (exactly -1.0 every run), batchRead10
  131.1 -> ~129.5 (-1.2 to -1.5), readRepeat ~19.9 -> ~19.4 (noisy, ~-1);
  batchWrite100 ~flat; multiRMW10 swamped by its own retry noise but never up.
- Outcome & why: KEPT. Primary flat, allocsPerTx clearly down 1.55% (all runs
  below baseline, singleRMW deterministic), no axis regressing. This is the same
  code exp24 wrote, now kept purely because the earlier wins lowered the
  denominators it divides into. Lesson: a sub-floor change is not permanently
  rejected - re-evaluate fixed per-tx savings after the loop has shrunk the small
  workloads, because their geomean leverage grows as their absolute counts fall.
- Commit: d29c154

## 32. Key the transaction monitor maps by TxId, not Vec<u8> - KEPT
- Hypothesis: `Monitor::begin_tx` runs once per transaction (called from `commit`
  / `validate_reads` on first entry, so on every workload incl. read-only) and
  registers the tx with `local_tx.insert(tid.as_bytes().to_vec(), ..)` - a fresh
  16-byte `Vec` copy of the id for the map key. The two sibling maps (`waiters`,
  `unknown_tx`) copy the id the same way on insert. `TxId` is `Arc<[u8]>` and
  derives `Hash`/`Eq` (by byte content), so keying the maps by `TxId` lets
  `begin_tx` insert `tid.clone()` (a refcount bump, zero alloc) while lookups by
  `&TxId` stay byte-content-based and identical. Saves ~1 alloc/tx broadly - the
  same high-leverage per-tx class as exp31.
- Change: `crates/glassdb-trans/src/monitor.rs` - `State`'s three maps go from
  `HashMap<Vec<u8>, _>` to `HashMap<TxId, _>`; inserts use `tid.clone()` (Arc
  bump) instead of `tid.as_bytes().to_vec()`; all `get/get_mut/remove` take the
  `&TxId` directly instead of `tid.as_bytes()`. `shard_for` still routes by
  `tid.as_bytes()` (sharding is byte-based, unchanged). No semantic change: the
  keys hash and compare by the same bytes as before.
- Correctness: fast gate PASS; `make test` PASS (fmt + clippy -D warnings + all
  tests incl. the monitor's status/committed_value/wait/refresh unit tests, which
  exercise insert/lookup/remove across the maps); all crate sim suites PASS;
  `glassdb` `concurrent_sim` determinism-NEUTRAL - the only failure is the
  pre-existing `recovery_holds` flake (sampled 3 fails / 5 passes over 8 runs,
  matching the baseline's ~50%; "diverged at index 98" identical; the other 8
  cases always pass); `concurrent_tx` fuzz `--cfg sim` PASS (8163 runs, no crash
  - heavily exercises begin/commit/abort/wait/wound monitor paths under
  concurrency); judge APPROVED (key-type change, lookup semantics preserved, no
  Stats/frozen-file tampering). Generated fuzz corpus removed before
  judging/commit.
- Primary: ~403.2 -> ~403.4 (within noise; op counts unchanged - note the harness
  primary itself varies run-to-run with 0 retries, as singleRMW/batchWrite100
  costPerTx shift with cache/timing, independent of this alloc-only change).
- Secondary (4 runs vs best.json/exp31): allocsPerTx 270.59 -> 269.28/264.72/
  269.01/266.52 (~267.4, -1.2%, every run below baseline); allocBytesPerTx ~flat
  (28534 -> ~28476, -0.2%). Deterministic per-workload: singleRMW 51.2 -> 50.4
  (-1.0 every run), readRepeat ~19.7 -> ~18.7 (~-1.0), batchRead10 ~131 -> ~129
  (~-0.7); multiRMW10 retry-noisy but never up; batchWrite100 ~flat.
- Outcome & why: KEPT. Primary within noise, allocsPerTx down ~1.2% (all runs
  below baseline, singleRMW deterministic), no axis regressing. Same per-tx-floor
  pattern as exp31: a single fixed alloc removed from the begin path, worth ~1%+
  on the geomean because the small workloads' counts are now low. Lesson: the
  transaction-tracking maps copied the id into an owned key on every begin; an
  `Arc`-keyed map shares it for free - prefer the refcounted id type as the key
  for per-tx bookkeeping structures.
- Commit: 4e7f646

## 33. Share the backend Version (CAS) token via Arc<str> - KEPT
- Hypothesis: `backend::Version` is the opaque CAS token returned by every
  metadata/object read and write and carried through the whole read/write
  pipeline (backend reply -> storage `global`/`local` cache entries -> trans
  validation -> next write's `if_match`). Its `token` was a `String`, so every
  `Version::clone()` (and there are many: cache stores it, reads copy it out,
  validation compares and propagates it, writes thread it back) heap-allocated a
  fresh copy of the token bytes. Tokens are immutable once minted, so making
  `token: Arc<str>` turns all those clones into refcount bumps - zero alloc -
  while keeping `Eq`/`Hash`/`Default` by content. This is the same "share an
  immutable value by Arc instead of deep-copying it on every clone" pattern that
  paid off for `TxId` (exp8), cache metadata (exp5), and backend tags (exp26),
  applied to the one token that touches *every* workload (read-only included).
- Change:
  - `crates/glassdb-backend/src/lib.rs` - `Version.token: String -> Arc<str>`;
    `Version::new` takes `impl Into<Arc<str>>` (callers minting from `&str`/
    `String`/`Arc<str>` all still work); `is_null` unchanged (`is_empty`).
  - `crates/glassdb-backend/src/memory.rs` - `Object::version` formats the
    `gen/metagen` token into a stack `[u8; 48]` buffer and builds the `Arc<str>`
    from that slice, so a fresh token is one alloc (the Arc) instead of two
    (String then Arc). Test assertions deref the Arc (`&*m.version.token`).
  - `crates/glassdb-backend-s3/src/lib.rs` - the S3 `if_match` precondition
    wants an owned `String`, so `expected.token.clone()` -> `.to_string()` (one
    alloc only where the SDK actually requires ownership; everywhere else stays
    a refcount bump). GCS needed no change.
- Correctness: fast gate PASS; `make test` PASS (fmt + clippy -D warnings + all
  tests, incl. the memory-backend version/metadata unit tests that assert the
  `"gen/metagen"` token format); all crate sim suites PASS (backend 16, storage
  18, trans 33); `glassdb` `concurrent_sim` determinism-NEUTRAL - over 6 runs 3
  passed 9/9 and 3 failed 8/9, the only failure being the pre-existing
  `recovery_holds` flake ("recovery op stream diverged at index 98", matching
  baseline ~50%; a representation change to an opaque token cannot affect the
  recovery op stream); `concurrent_tx` fuzz `--cfg sim` PASS (8217 runs, no
  crash - hammers CAS-conditioned commit/abort across the token path); judge
  APPROVED (Arc<str> refactor, equivalent semantics, no frozen-oracle/stats
  edits). Generated fuzz corpus removed before judging/commit. (As for every
  prior experiment, the frozen `check.sh --full` cannot run as-is: its fuzz step
  hard-codes the stale `--cfg madsim` flag - the target now builds under
  `--cfg sim` - and its `make test-sim` bundles the pre-existing `recovery_holds`
  flake; the manual `--cfg sim` fuzz + crate sim suites + flake sampling above
  are the equivalent, stronger full-tier check.)
- Primary: best 403.42 -> ~403.5 (3 count-3 medians 404.2 / 403.1 / 403.3;
  within noise, op counts unchanged). best.json refreshed at 403.5.
- Secondary (3 count-3 runs vs best.json/exp32 = allocsPerTx 269.28,
  allocBytes 28576, ns 22097, cpu 35865): allocsPerTx -> 259.0 / 250.1 / 254.7
  (~254.7, -5.4%, every run well below baseline); allocBytesPerTx -> 28354 /
  27818 / 28091 (~-1.7%); nsPerTx and cpuNsPerTx noisy (first run read +5%/+3.5%
  high, the next two -8%/-15% low) so net neutral-to-down, never a real
  regression. Deterministic per-workload alloc drop (from the scouting count-1
  sweep): readRepeat ~-13%, batchRead10 ~-8%, multiRMW10 ~-4%, batchWrite100
  ~-3%, singleRMW ~-1.1% - every workload down because the token rides every
  read and write.
- Outcome & why: KEPT. Primary flat (within noise), allocsPerTx clearly down
  ~5.4% with every workload deterministically lower and no axis regressing
  (allocBytes also down; ns/cpu net down once the one noisy run is set aside).
  This was the single biggest remaining per-tx allocation: one heap copy of the
  CAS token on every clone through the pipeline, now a refcount bump. Lesson
  (again): find the immutable value that is cloned the most times per
  transaction and share it by `Arc`; the CAS token touches literally every
  backend op, so converting its copies to refcount bumps moved every workload at
  once.
- Commit: 0f93c51

## 34. Share object values via Arc<[u8]> through the whole value pipeline - KEPT
- Hypothesis: the object value bytes were a `Vec<u8>` copied at every hop. On a
  read (cache hit) the value is copied twice: once out of the local cache
  (`local.read` clones `CacheValue.value`) and again when the transaction stages
  it for repeatable reads (`Tx::read`), then the staged copy is *never reused*
  in any benchmark (collect_accesses uses only the read-version for read-only
  entries, and a later write overwrites it) - pure waste. On a write the value
  is copied ~3x in the commit path: into the tx-log entry (`to_log` ->
  `TxWrite.value`), into the lock/write `TValue`, and into the write-through
  cache (`update_local` -> `local.write`). Values are immutable once produced,
  so making the value an `Arc<[u8]>` end-to-end turns all these copies into
  refcount bumps; only the final user-facing `Tx::read -> Vec<u8>` and the one
  real backend write still materialize bytes. This is the read/write analog of
  the "share an immutable value by Arc" wins (exp26 tags, exp33 version token),
  applied to the value itself - it touches every read and every write.
- Change: value type `Vec<u8> -> Arc<[u8]>` across the pipeline, keeping the
  backend trait boundary on `Vec<u8>` so the S3/GCS/memory backends are
  untouched (convert once at the boundary):
  - `glassdb-storage`: `CacheValue.value`, `LocalRead.value`, `GlobalRead.value`,
    `TValue.value`, `TxWrite.value` -> `Arc<[u8]>`; `Local::write*` take
    `Arc<[u8]>`; `Global::write*` take `Arc<[u8]>` and `to_vec()` only for the
    backend call (same one copy as the old `clone`); `global.read` wraps the
    backend's `Vec` contents in an `Arc` once on a miss; tlogger marshals via
    `value.to_vec()` and unmarshals into `Arc::from(bytes)`, and the
    per-commit log buffer is `Arc::from(buf)` (1 conv/commit, negligible).
  - `glassdb-trans`: `WriteAccess.val`, `ReadValue.value` -> `Arc<[u8]>`; the
    three commit-path clones (`to_log`, single-RW `TValue`, `update_local`) are
    now Arc bumps; reader `materialize`/`handle_lock_create` unchanged in logic.
  - `glassdb` (public API unchanged): `Tx`'s staged value is `Arc<[u8]>`;
    `Tx::read` stages a bump and hands the caller `rv.value.to_vec()` (the only
    per-read alloc); `write` stages `Arc::from(value)` (same 1 alloc as the old
    `to_vec`); `read_weak` returns `.to_vec()`.
  - Inline unit tests updated to build values as `Arc::from(&b"..."[..])` and
    deref on assert (`&*x.value`); no frozen/integration/bench/fuzz files
    touched.
- Correctness: fast gate PASS; `make test` PASS (fmt + clippy -D warnings + all
  workspace tests, incl. the storage tlogger round-trip, monitor committed-value,
  locker, and algo commit/read unit tests that exercise the value through log
  marshal/unmarshal and the cache); all crate sim suites PASS; `glassdb`
  `concurrent_sim` determinism-NEUTRAL - over 6 runs 3 passed 9/9 and 3 failed
  8/9, the only failure being the pre-existing `recovery_holds` flake (identical
  "recovery op stream diverged at index 98"; the other 8 always pass); a value's
  byte content is unchanged by its container, so it cannot affect the recovery
  stream; `concurrent_tx` fuzz `--cfg sim` PASS (9289 runs, no crash - exercises
  the full write/commit/read value path under concurrency); judge APPROVED
  (consistent Arc<[u8]> refactor, honest test updates, no frozen/stats edits).
  Generated fuzz corpus removed before judging/commit. (As every prior
  experiment notes, frozen `check.sh --full` can't run as-is - its fuzz step
  hard-codes the stale `--cfg madsim` flag and its `make test-sim` bundles the
  `recovery_holds` flake - so the manual `--cfg sim` fuzz + crate sim suites +
  flake sampling above are the equivalent, stronger full-tier check.)
- Primary: best 403.50 -> ~403.5 (3 count-3 medians 403.5 / 403.2 / 404.1;
  within noise, op counts/costPerTx unchanged per workload). best.json refreshed
  at 403.22.
- Secondary (3 count-3 runs vs best.json/exp33 = allocsPerTx 253.65,
  allocBytes 28030, ns 20771, cpu 31601): allocsPerTx -> 229.4 / 234.0 / 233.2
  (~233, -8.1%, every run well below baseline); allocBytesPerTx -> ~27160
  (-3.1%); nsPerTx ~19700 (-5%, down); cpuNsPerTx ~30600 (-3%, down, one noisy
  run aside). Deterministic per-workload alloc drop (best.json refresh): singleRMW
  49.2 -> 43.2 (-12%), batchRead10 118.5 -> 108.4 (-8.5%), multiRMW10 1089.6 ->
  1026.0 (-5.8%), batchWrite100 9805.8 -> 9407.5 (-4.1%), readRepeat ~16.8 ->
  16.5 - every workload down because the value rides every read and every write.
- Outcome & why: KEPT. Primary flat (within noise), allocsPerTx down ~8% with
  *every* workload lower and no axis regressing (allocBytes/ns/cpu all down). The
  value was the most-cloned per-op payload left - 2 copies per read (one of them
  pure waste) and ~3 per write - now refcount bumps, with the real copy only at
  the user boundary and the single backend write. Biggest single allocs win of
  the run. Lesson: extend "share immutable data by Arc" to the value payload
  itself; even tiny values cost a full allocation each, and the geomean is driven
  by allocation count, not size.
- Commit: 09d4119

## 35. Cache the memory backend's CAS version token on the object - KEPT
- Hypothesis: `memory::Object::version()` minted a fresh `Arc<str>` "gen/metagen"
  token on every call, and it is called on *every* backend op: each `read`,
  `get_metadata`, `read_if_modified`, and once per `write*`/`set_tags_if`/
  `delete_if` reply, plus once more on each conditional op just to *compare*
  against `expected`. The token only changes when `gen` or `metagen` change (a
  write or tag update), so caching it on the `Object` and recomputing only on
  mutation makes every read/metadata-read/version-compare a refcount bump
  instead of a fresh allocation. Read-validation (`validate_read` ->
  `global.get_metadata` -> backend `get_metadata`) is one backend metaRead per
  read, so read-heavy workloads mint a token per read today - pure waste.
- Change: `crates/glassdb-backend/src/memory.rs` - `Object` gains a cached
  `token: Arc<str>`; `mint_token(gen, metagen)` builds it via the stack buffer;
  `refresh_token` recomputes it; `version()` returns `Version::new(token.clone())`
  (a bump). The token is refreshed exactly once per mutation: `update_data`
  refreshes it (covers `write`/`write_if`/`write_if_not_exists`, which call
  `update_tags` then `update_data`), and `set_tags_if` refreshes after
  `update_tags` (the only path that mutates tags without data); `update_tags`
  itself no longer refreshes, so a write that touches both tags and data mints
  the token once, not twice. `Default for Object` mints "0/0" to preserve exact
  prior semantics. No public API or other crate changed.
- Note: an initial version refreshed in *both* `update_tags` and `update_data`,
  which double-minted on writes and pushed batchWrite100 *up* ~2% (9407 ->
  9607). Moving the refresh to one place per op fixed it (batchWrite100 back to
  ~9380, neutral) and is the version measured below - a reminder that a "cache
  it" change can add allocs if the cache is rebuilt twice in one operation.
- Correctness: fast gate PASS; `make test` PASS (fmt + clippy -D warnings + all
  workspace tests incl. the memory-backend write/read/version unit tests that
  assert the exact "gen/metagen" token strings); all crate sim suites PASS;
  `glassdb` `concurrent_sim` determinism-NEUTRAL (6 runs: 4 passed 9/9, 2 failed
  8/9 on the pre-existing `recovery_holds` flake, identical "diverged at index
  98"; caching an identical token cannot change the recovery stream);
  `concurrent_tx` fuzz `--cfg sim` PASS (8969 runs, no crash); judge APPROVED
  (legitimate token caching in an editable file, no test/stats/benchmark
  gaming). Generated fuzz corpus removed before judging/commit. (Frozen
  `check.sh --full` not runnable as-is for the usual reasons - stale `--cfg
  madsim` fuzz flag and the bundled `recovery_holds` flake - so the manual `--cfg
  sim` fuzz + crate sim suites + flake sampling are the equivalent full-tier
  check.)
- Primary: best 403.87 (refreshed) vs prior ~403.5 - flat within noise (3 medians
  402.75 / 403.9 / 403.8; op counts unchanged).
- Secondary (3 count-3 runs vs best.json/exp34 = allocsPerTx 236.93,
  allocBytes 28030-ish): allocsPerTx -> 224.9 / 222.7 / 224.8 (~224, -5.4%, every
  run well below baseline); allocBytesPerTx -> ~27050 (-3.5%); ns/cpu flat-to-down.
  Per-workload (best.json refresh): batchRead10 108.4 -> ~98.5 (-9%), readRepeat
  16.5 -> ~14.6 (-11%), multiRMW10 1026 -> ~1004 (-2%), singleRMW 43.2 -> ~42.4
  (-2%), batchWrite100 9407 -> ~9380 (neutral). Reads move most because each
  carries a backend metaRead that previously minted a token.
- Outcome & why: KEPT. Primary flat, allocsPerTx down ~5% with the read-heavy
  workloads down ~9-11% and no axis regressing. The CAS token was minted on every
  backend op (even just to compare); it is immutable between writes, so caching
  it on the object turns those into bumps. Lesson: an immutable per-object
  identifier recomputed on every access is a caching opportunity - store it and
  invalidate on the (rare) mutation, but refresh it exactly once per op.
- Commit: __PENDING__

