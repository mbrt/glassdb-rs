# Performance investigations

This is the log for manual performance investigations that do not belong to the
autonomous [`autoresearch`](../autoresearch/README.md) loop. Entries may describe
temporary instrumentation, inconclusive variants, and design candidates.

This file is evidence, not a record of accepted behavior:

- [`docs/guides/perf.md`](../../docs/guides/perf.md) records metrics for landed
  performance-affecting changes.
- [`docs/guides/perf-todos.md`](../../docs/guides/perf-todos.md) tracks open
  performance work.
- ADRs record significant decisions once accepted.

## 2026-07-29: Inline admission and structural amplification

Status: logged-publication simplification implemented by
[ADR-054](../../docs/adr/054-reserve-inline-publication-for-logless-commits.md);
inline-pressure splitting implemented and validated by
[ADR-056](../../docs/adr/056-demand-driven-inline-pressure-splits.md); budget
tuning remains open.

Reference: `5c3e5ac6`, after ADR-053. The goal is to isolate ADR-051's
provisional inline budgets from the later contention fix.

### Initial sweep

A temporary role-counting backend wrapper, removed after the experiment,
distinguishes node, transaction-log, and structural-log operations and bytes.
Three runs sweep no inlining, the current 1 KiB / 64 KiB policy, 4 KiB and
16 KiB aggregate budgets, and selected per-value and encoded-object limits.
The workloads cover serial and dense-leaf RMW, batch write, cold and warm read,
cache pressure, and foreground versus background split work under the S3, GCS,
and memory profiles. Every run completes without transaction failures.

#### Direct-path benefit

- On a serial S3-profile RMW, the current policy reduces median latency from
  `614` to `177 ms/transaction` for 128 B values and from `627` to
  `192 ms/transaction` for 1 KiB values. A cold inline read uses one node
  operation instead of a node plus transaction-log read; a warm read gains
  little once the transaction object is cached.
- The GCS-profile latency gain is smaller but the operation saving remains:
  median latency falls from `1367` to `1017 ms/transaction` at 128 B and from
  `1176` to `1052 ms/transaction` at 1 KiB.
- Raising the per-value limit makes a serial 4 KiB S3-profile RMW land directly
  and reduces median latency from `525` to `183 ms/transaction`. This does not
  establish that dense leaves should carry 4 KiB inline values, where every CAS
  would rewrite them.

#### Dense-leaf mixed regime

- A logged 128-key batch transaction performs the same number of backend
  operations with or without inlining. At 1 KiB, however, median node-write
  volume rises from `11.26` to `118.24 KiB/transaction`: write-back adds a
  cached copy of values already durable in the transaction object.
- After that batch, the current policy lands only `50%` of 1 KiB single-key
  RMWs directly. Median latency remains roughly flat (`457` versus
  `451 ms/transaction`) and operations fall only from `2.48` to `2.38`, while
  node-write volume rises from `4.84` to `95.54 KiB/transaction`.
- Smaller aggregate budgets are worse, not safer. The 4 KiB and 16 KiB policies
  land `3.1%` and `12.5%` of 1 KiB direct candidates, respectively, but increase
  median S3-profile RMW latency to `667` and `626 ms/transaction` and operations
  to `3.63` and `3.42`.
- With 900 keys split across five leaves, the current policy lands exactly
  `320/900` 1 KiB candidates: 64 values per leaf, the 64 KiB budget's capacity.
  Compared with no inlining, median mutation work rises from `2.55` to
  `3.45` operations/transaction, node-write volume from `7.89` to
  `108.74 KiB/transaction`, and memory-profile latency by `33%`. At 128 B the
  same leaves have enough budget for every value; direct land rate is `100%`
  and latency improves by `30%`, although node-write bytes still rise by
  `3.9x`.

The aggregate budget is sticky and first-come. Logged write-backs consume it
even though their transaction objects remain authoritative; later direct
commits that require inline storage fall back to locking. Existing inline
payloads must be preserved because a logless writer may have no transaction
object, so every later leaf CAS continues to rewrite those bytes. This partial
coverage can therefore cost more than either full direct coverage or no
inlining.

#### Split attribution

Across three waves of 900 inserts, both policies produce exactly five split
candidates, four completed splits, zero deferrals, and 13 structural-log writes.
Inlining increases foreground node-write bytes by `4.1x` at 128 B and `6.0x` at
1 KiB, and background split node-write bytes by `4.6x` and `5.7x`,
respectively. Inline leaf growth alone therefore does not explain the earlier
`3.8–8.1x` increase in structural-log operation counts, although it clearly
amplifies the bytes moved by each structural operation.

### Write-back suppression and split proxy

A second three-run experiment uses 128 interleaved, existing 1 KiB keys on one
leaf. One logged transaction updates the 64 even keys, then two single-key RMW
passes update the 64 odd and 64 even keys. It compares current behavior with a
temporary variant that suppresses only new logged write-back inlining, while
preserving existing inline states and direct publication. A 64-entry leaf split
threshold is an upper-bound proxy for reacting to inline pressure; it is not a
proposed global default.

- Suppressing write-back inlining reduces the logged batch's node-write volume
  from `73.8` to `9.6 KiB` while retaining the same transaction-log write and
  two node writes. A cold scan adds one `65.7 KiB` transaction-log read, because
  every key names the same logged transaction; median scan latency is
  effectively unchanged in both profiles.
- With the normal 256-entry split threshold, current behavior lands `0/64`
  direct commits on the odd keys and `64/64` on the already-inline even keys.
  Suppressing write-back inlining reverses the result: `64/64`, then `0/64`.
  Total direct capacity does not change; the policy only chooses which keys
  receive it.
- The 64-entry split proxy performs one split and three structural-log writes,
  after which both passes land `64/64` direct commits. Combined median RMW
  latency falls from `570` to `265 ms` in the S3 profile and from `2211` to
  `1435 ms` in GCS. It also makes the logged batch span two leaves: node writes
  rise from two to four and median batch latency rises from `7.2` to `9.8 ms`
  in S3 and from `6.9` to `11.4 ms` in GCS.

As secondary guardrails, five deterministic autoresearch runs move the median
score from `99.72` to `97.36` with write-back inlining suppressed; the
`batchWrite100` median falls from `8.32` to `5.94 ms/transaction`, although the
overall score ranges overlap. Three short paired 128 B `lo/shared` mixbench
runs remain at parity: aggregate-throughput median ratio `0.99`, total backend
operations/transaction ratio `0.99`, and zero failures.

The implemented ADR-054 comparison against `ed590a8c` confirms the deterministic
write benefit: `batchWrite100` cost and operations/transaction both fall to
`0.941`, with the overall score at `0.987`. Its broader adaptive mixbench sweep
also exposes a workload not covered by the original `lo/shared` guardrail. In
`hi/per-shape`, `rwMany` throughput falls to `0.377` and p90 rises from
`0.988 s` to `13.762 s`, while object reads/transaction rise from `1.327` to
`1.764`. The same cell's read-only shapes become over `6x` faster and the other
mixed cells are mostly flat or better, so this is not a uniform slowdown.
The result is consistent with smaller leaf transfers helping cached reads while
cross-client logged-value resolution adds transaction-object lookups and a long
tail to contended multi-key mutations. That attribution needs a repeated,
phase-level run before changing policy.

### Multi-RMW tail follow-up

Status: closed as non-reproducible; the measured representation trade-off
supports ADR-054 and does not justify a tail-specific engine change.

The follow-up first isolates ADR-054 by comparing its accepted parent
`f618e738` with implementation `7bc6fb01`. Three alternating S3-profile
`hi/per-shape` pairs use eight workers per shape, four client Databases per
shape, eight hot keys, a 10% throughput-CI target, and a 30-second cap. Every
shape converges with zero failures.

- `rwMany` throughput ratios are `1.459`, `1.062`, and `1.473`.
- p50 ratios are `0.998`, `0.995`, and `1.014`; p90 ratios are `0.988`,
  `1.009`, and `0.926`.
- Object-read ratios are `0.923`, `1.021`, and `0.968`; object-write ratios are
  `0.967`, `0.997`, and `0.981`.

The original broad comparison (`ed590a8c` to `7bc6fb01`) is also repeated with
the same settings. Its throughput ratios are `0.961`, `1.593`, and `1.390`,
while p90 ratios are `0.984`, `1.022`, and `1.012`. Neither comparison
reproduces the prior `0.377x` throughput and `13.9x` p90 result. That result was
a one-run outlier, not evidence that ADR-054 regressed multi-RMW.

Temporary instrumentation, applied identically to the isolated refs, wraps
each benchmark Database with role- and byte-aware backend counters and brackets
the measured foreground separately from shutdown. It also records decoded L1
hits and misses. The wrapper perturbs scheduling, so its timing is not used;
the operation and byte deltas are stable and all runs still converge without
failures.

For `rwMany` in the cross-client `per-shape` topology:

- transaction-log body reads rise by only `11.3–14.8 B/transaction`; physical
  transaction-log calls are too variable to distinguish because unchanged
  conditional reads transfer no body;
- L1 misses rise by `0.12–0.39/transaction`;
- node reads fall by `57–81 B/transaction`; and
- node writes fall by `2.14–2.15 KiB/transaction`.

The shared-Database topology makes the added transaction-log body transfer
almost disappear (`0.071–0.079 B/transaction` across the whole mixed cell),
confirming that the decoded cache absorbs repeated resolution when clients
share it. Cross-client caches cannot share that entry, but their extra body
transfer remains much smaller than the leaf bytes no longer rewritten.
Shutdown attribution is zero for every per-shape run and negligible in the
shared runs, so this is a foreground representation trade-off rather than
deferred cleanup.

### Demand-driven split validation

A focused scenario in the existing `rtbench` binary fills one 192-entry leaf's
64 KiB aggregate budget with 64 direct 1 KiB mutations. Two distinct external
keys then encounter aggregate rejection. The first requests the root split; the
second, after rerouting, requests a split of the still-saturated child. A final
64-mutation wave alternates between the two leaves with newly available
capacity. The fixture stays below ADR-031's ordinary entry and encoded-byte
thresholds, so pressure is the only possible split cause.

The harness is applied identically to `e88cb819` and `0be65fee`; the historical
copy changes only its statistics-field adapter because ADR-056's pressure
counters do not exist there. Three release-mode pairs per profile are
interleaved by side.

- Every target run processes two pressure candidates, completes two splits,
  and records no deferral or discard. The tree grows from one leaf to three.
  The base remains at one.
- Both discovering mutations still use the locked fallback. The later wave
  changes from `0/64` to `64/64` direct landings and from 64 lock calls to zero.
- S3-profile recovery throughput improves by `3.50–3.65x`, with p50 at
  `0.26–0.27x`, p90 at `0.29–0.32x`, operations/transaction at `0.26–0.29x`,
  and write bytes/transaction at `0.20–0.22x`.
- GCS-profile recovery throughput improves by `3.46–3.90x`, with p50 at
  `0.22–0.25x`, p90 at `0.51–0.66x`, operations/transaction at `0.28–0.29x`,
  and write bytes/transaction at `0.20–0.22x`.
- Including both discovering fallbacks and structural work, total
  operations/transaction fall to `0.53–0.55x` in S3 and `0.53–0.54x` in GCS;
  total write bytes/transaction fall to `0.42–0.45x` and `0.42–0.44x`.

Access order is material under the GCS profile. An initial version grouped 32
recovery mutations on one new leaf before moving to the next. It still doubled
throughput, but target p90 rose to `1.61–1.64x` because the model enforces GCS's
one-write-per-second limit per object and its retry backoff can overshoot the
next token. Alternating the same fixed keys across the two leaves measures the
parallel capacity created by the split and changes p90 to `0.51–0.66x`. This
does not promise relief for a workload that remains concentrated on one leaf;
tree widening helps only when demand spans the new ranges.

### Current conclusion

ADR-054 removes logged write-back amplification, and ADR-056 supplies the
focused inline-capacity fix without globally lowering split thresholds. The
multi-RMW follow-up finds no durable tail regression and shows that the
cross-client transaction-object transfer is small beside the saved leaf bytes.
The focused split result proves that pressure is observed, rerouted, converted
into capacity, and repaid by later mutations. It does not establish that the
current 1 KiB/64 KiB budgets are optimal, nor quantify permanent widening under
a broad workload.

The next investigation is the inline-policy sweep: compare no inlining,
current defaults, and smaller aggregate budgets across value sizes before
treating the defaults as settled.
