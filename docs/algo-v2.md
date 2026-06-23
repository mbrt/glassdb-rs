# Algorithm v2: object-storage-native layout

## Status

**Tracker** for the in-progress v2 redesign. The architectural decision is
recorded in [ADR-016](adr/016-object-storage-native-layout.md); each sub-decision
becomes its own ADR (see [Planned ADRs](#planned-adrs)). This document is the
living overview and checklist that ties them together — it is mutable, unlike the
ADRs. When v2 lands, its content is absorbed into
[architecture.md](architecture.md) and this file retired.

For the motivation (the S3 metadata-update problem) and the umbrella decision,
see [ADR-016](adr/016-object-storage-native-layout.md).

## Direction at a glance

MVCC for values + S2PL for isolation, with a fixed set of `C` shard objects per
collection acting as a content-CAS coordination directory in place of per-object
tags. No object tags anywhere.

- **Objects** —
  - *Collection root* (`_i`): existence + the constant shard count.
  - *Shard* (`_s/<i>`, `C` per collection): lock table + MVCC version index +
    key directory; the unit of CAS (`If-Match`).
  - *Transaction* (`_t/<txid>`): unified; pending (small: lease + lock
    intentions) → committed (fat: value map) → aborted.
- **Protocol** — execute → validate+lock (one shard GET + one CAS per shard) →
  commit (CAS the transaction object to committed, attaching values) → async
  per-shard write-back (publish current-writer pointers + release locks).
- **Reads** — shard (conditional GET) → current-writer txid → value from the
  immutable transaction object (cacheable indefinitely). Read-only stays
  lock-free.
- **GC** — mark-sweep; live set = `current-writer ∪ locked-by` across shards.
- **Backend trait** — `read / write / write_if / write_if_not_exists / delete /
  list` (tags, nonce, `set_tags_if`, `read_if_modified`, `delete_if` all gone).

## Decided

Rationale lives in [ADR-016](adr/016-object-storage-native-layout.md) and the
per-decision ADRs.

- Full redesign; format **replaced wholesale** (S3 + GCS); Go on-disk
  compatibility dropped.
- Values live **only in unified transaction objects**.
- **Fixed compile-time `C`** shards per collection (not configurable);
  split-resharding deferred to v2.
- **Mark-sweep GC** in the MVP; explicit liveness counter and compaction
  deferred.

## Accepted limitations (MVP)

- Collections larger than `C × keys-per-shard` (needs v2 split-resharding).
- **Within-shard false sharing**: transactions sharing a shard serialize on its
  CAS (~`1 / RTT` per shard). The deliberate trade for removing S3 value-rewrite
  amplification; `C` is the write-parallelism knob.
- No compaction (a cold key can pin a fat transaction blob of otherwise-dead
  values); no explicit GC counter (full mark-sweep only).

## Staging

1. New `Backend` trait + new in-memory backend; re-point the
   serializability / cycle DST oracles at it.
2. Shard directory: lock table + version pointers + key directory; reimplement
   the locker / validate path as per-shard CAS with wound-wait; green under the
   fuzzer.
3. Transaction objects + commit / write-back; read path (shard → blob → cache).
4. Mark-sweep GC.
5. S3 + GCS backends on the shrunk trait; benchmarks to find the false-sharing
   knee and confirm the S3 win.

## Planned ADRs

Each design decision becomes its own ADR (next free number is 017).

- **[ADR-016](adr/016-object-storage-native-layout.md) — Object-storage-native
  layout.** The umbrella decision: move coordination state from tags
  into content; MVCC + S2PL on a sharded directory; the three-object model;
  wholesale format replacement.
- **ADR-017 — Sharded coordination directory.** Shards as lock table + version
  index + key directory; fixed compile-time `C`; key→shard hashing; resharding
  deferred. Phantom prevention via shard CAS (replaces the collection-info lock).
- **ADR-018 — Values in unified transaction objects.** Why values live in the
  transaction object; why it stays unified (status + values) rather than split;
  the pending → committed → aborted lifecycle and the commit point.
- **ADR-019 — Commit & write-back protocol.** Validate+lock as per-shard CAS,
  commit CAS, async per-shard write-back; the cross-shard non-atomicity argument.
- **ADR-020 — Wound-wait & leases at shard granularity.** How wound/expiry
  relocate from log tags to the transaction object; interplay with `locked-by` in
  shards. Re-frames [ADR-002](adr/002-wound-wait-locking.md) for the new layout.
- **ADR-021 — Garbage collection by mark-sweep.** Live set = `current-writer ∪
  locked-by`; the commit→write-back gap; deferral of the explicit counter and
  compaction.
- **ADR-022 — Slimmed `Backend` trait.** The reduced surface; removal of tags /
  nonce / `delete_if`; content CAS as the only coordination primitive. Relates to
  [ADR-009](adr/009-in-doubt-conditional-writes.md) for in-doubt parity at the
  new CAS sites.

## Open points checklist

Group A — layout & encoding:

- [ ] Concrete value of the constant `C` (working proposal: `1024`; ~50 keys/
      shard and a few-KB shards at the 50k-key benchmark; ceiling ≈ 256k keys at
      a ~256-key/shard soft cap). The one number to bikeshed.
- [ ] Key→shard hash function (deterministic & stable across processes and under
      `--cfg sim`; non-crypto, over raw key bytes).
- [ ] On-disk encoding of shards (entry layout, ordering, size budget per shard).
- [ ] On-disk encoding of the unified transaction object (pending vs committed
      forms; value-map representation; reuse `glassdb-proto` or a new schema?).
- [ ] Collection-root format and how the shard count is recorded/validated.
- [ ] Path type markers for shards (`_s`?) and any changes to `paths` encoding.

Group B — protocol details:

- [ ] Exact validate+lock CAS algorithm for multiple keys per shard (validate
      all, lock all, retry-with-locks-held semantics).
- [ ] Resolution of the "effective current writer" when a committed-but-not-
      written-back write-lock holder exists (the relocated `validate_locked_read`
      logic).
- [ ] Deadlock fallback: serial sorted-by-shard locking; equal-priority handling.
- [ ] Lease creation point (pending object at first lock), refresh cadence, and
      the expiry/wound CAS sequence; reuse of existing timeout constants.
- [ ] In-doubt (`Unavailable`) handling parity at the new CAS sites (shard CAS,
      commit CAS, write-once blob) — confirm ADR-009 reasoning carries over.
- [ ] Single-RW and read-only fast-path shapes in the new layout.

Group C — listing, snapshots, phantoms:

- [ ] `list` / iteration: reading the `C` shards and unioning key directories;
      cross-shard snapshot consistency (read-set validation, cf. the cycle
      observer).
- [ ] Phantom prevention: create/delete as shard CAS; interaction with the
      collection root.

Group D — GC & lifecycle:

- [ ] Mark-sweep trigger cadence, batching, and bounds (LIST/read cost).
- [ ] Safety horizon to avoid sweeping in-flight transactions; interaction with
      leases.
- [ ] Defer/spec compaction (v2) and the explicit liveness-counter object.

Group E — backends:

- [ ] Final `Backend` trait signature and error semantics on the reduced surface.
- [ ] S3 mapping (drop nonce/tags; conditional writes; remove `delete_if`).
- [ ] GCS mapping (content CAS via generation `If-Match`; drop metadata patch).
- [ ] In-memory backend semantics for the new trait (and DST fault injection).

Group F — testing & migration:

- [ ] Re-point DST oracles (serializability, cycle ring) at the new layout.
- [ ] Regenerate golden vectors and `RecordingBackend` byte-stream expectations.
- [ ] Benchmark plan to locate the false-sharing knee vs `C` and confirm the S3
      win.
- [ ] Update `README.md`, `architecture.md`, `PORTING.md` once the layout lands.

Group G — open questions to resolve before/within ADRs:

- [ ] Does the unified transaction object ever need a `list`-discoverable pending
      registry, or are shard `locked-by` entries sufficient to discover all live
      transactions for GC and recovery?
- [ ] Behavior under a hot shard hitting S3's per-prefix PUT ceiling — accept as
      a documented limit for the MVP, or spread shard paths to mitigate?
- [ ] Whether the collection root and shards should share a fate (created
      atomically on `collection.create`).
