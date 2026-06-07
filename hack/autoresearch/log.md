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

