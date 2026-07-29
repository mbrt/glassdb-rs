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
inline-pressure splitting and budget tuning remain open.

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

### Current conclusion

Complete write-back suppression is a useful byte-amplification simplification,
but it is not the inline-capacity fix. A global 64-entry split threshold would
solve the focused case by widening every tree, including ones that never need
direct capacity.

The logged-publication simplification is implemented by
[ADR-054](../../docs/adr/054-reserve-inline-publication-for-logless-commits.md).
The next design candidate should instead preserve the suppression and hint a
background split only when direct publication encounters aggregate inline
pressure. Such a split creates more authoritative capacity while retaining the
64 KiB per-object bound. Its trigger rate, first-rejection fallback, and
worst-case tree-width growth need an explicit design before implementation.
