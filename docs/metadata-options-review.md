# Database metadata and client options review

Date: 2026-09-18

Status: First-pass implementation complete; remaining limits are recorded below.

## Implementation status

- Finding 1: hard limits are stored in v4 metadata and loaded before opening the
  engine. Existing builder setters are retained; their hard limits apply only
  when creating a database. Soft thresholds remain local. Regression tests,
  `make test`, and adversarial review passed.
- Finding 2: both timing fields are stored in the same unmerged v4 metadata.
  Recovery, refresh, and GC receive the stored profile. Invalid creation timing
  is rejected before storage initialization. Regression tests, `make test`,
  and adversarial review passed.
- Finding 3: metadata bootstrap returns one validated identity and settings
  record. Limits must admit at least the empty key. Tests cover missing and
  invalid fields, concurrent creators, invalid proposals when reopening, and
  clients with different inline budgets. Regression tests, `make test`, and
  adversarial review passed.
- Finding 4: capacity rejection now requests a split independently of local soft
  thresholds. Parent separator publication and recovery preserve this split
  reason. The reproduced insertion failure and a full parent below its soft
  limits now pass regression tests, including interrupted parent publication.
  Regression tests, `make test`, and adversarial review passed.

The first pass retains the mixed `SplitPolicy` interface and does not provide
migration or online changes to stored settings. Earlier development databases
must be recreated. The findings below describe the code before these fixes.

Limit validation checks the minimum key shape. It does not certify that every
key distribution or transient lock set fits a chosen limit.

Capacity hints request one split of a divisible node, then the blocked operation
retries admission. They do not retain the blocked operation, so stale hints can
cause extra splits. An operation-aware benefit check is deferred. A node with
fewer than two entries or children cannot split and retains its capacity limit.

Persist transaction timing and hard coordination limits. Keep performance
settings local to each database instance. Settings that are necessary for
correctness must be persisted at creation and loaded when opening the database.

At the time of review, metadata stored only the version and database ID. Opening
used the caller's configuration unchanged. See
[metadata bootstrap](../crates/glassdb/src/version.rs) and
[database open](../crates/glassdb/src/db.rs).

## Prioritized findings and proposed fixes

### 1. P1 — Persist hard coordination limits

Move `SplitPolicy::node_max_bytes` and `split_headroom_bytes` into database
metadata. These determine permitted key sizes, leaf mutations, and
collection-directory capacity. They affect whether clients can operate on
existing data. See
[key validation](../crates/glassdb-trans/src/algo.rs),
[mutation admission](../crates/glassdb-trans/src/leaf_coord.rs), and
[directory limits](../crates/glassdb-trans/src/collection_catalog.rs).

Two failures were reproduced after reopening with a smaller hard limit:

- An existing key became unwritable.
- Inserting beside an existing inline value failed after 30 seconds of capacity
  retries.

**Proposed fix:** Separate hard limits from soft split thresholds. Persist the
hard limits at creation and load them before constructing the engine. Derive
key, entry, and directory limits from that stored configuration.

### 2. P1 — Persist the transaction timing contract

Persist both `ProtocolTiming::pending_timeout` and `max_clock_skew`. The timeout
controls refresh frequency, peer expiry, and ambiguous commit recovery. GC uses
the timeout plus skew allowance for final-record retention. See
[timing](../crates/glassdb-trans/src/monitor.rs),
[recovery](../crates/glassdb-trans/src/monitor.rs), and
[GC eligibility](../crates/glassdb-trans/src/gc.rs).

Different settings can cause premature wounds or remove an unreferenced final
record during another client's recovery window. Current pinned wounds and
`InDoubt` handling still protect safety. This finding does not establish data
corruption.

**Proposed fix:** Load one stored timing profile into Monitor and GC. Derive
refresh and recovery intervals from it. Validate duration ranges before
creation. Clients must still satisfy the stored clock-skew assumption.

### 3. P1 — Enforce one complete configuration at creation and open

Adding protobuf fields alone is insufficient. Existing binaries can ignore
them and continue using local settings.

**Proposed fix:** Separate creation settings from client options. Have metadata
bootstrap return the complete validated database configuration. Publish it
atomically with the database ID. A concurrent creator that loses must load the
winning configuration before starting its engine. Opening must use stored
values without applying local defaults over them.

Gate this contract with a protocol version that older clients reject. Reject
missing required settings rather than guessing historical values. The reviewed
v3 format is [documented as unshipped](adr/070-demand-driven-garbage-collection.md#transaction-paths-permit-broad-and-narrow-scans),
so recreation is a simple transition.

### 4. P2 — Make hard-cap recovery independent of soft split thresholds

Capacity rejection currently requests an ordinary soft-cap split. The splitter
can discard that request because the stored node remains below the soft
threshold. See
[split hints](../crates/glassdb-trans/src/split.rs) and
[split checks](../crates/glassdb-trans/src/split.rs).

This failure was reproduced with one unchanged configuration, including a soft
byte threshold equal to the content limit. Persisting that configuration would
preserve the failure.

**Proposed fix:** Add a capacity-driven split reason that checks whether a split
can admit the blocked operation, independently of soft thresholds. Validate
settings before publishing metadata. Soft thresholds can then remain client
options.

## Proposed ownership

| Setting | Owner | Reason |
| --- | --- | --- |
| Protocol version, database ID | Metadata; already stored | Shared interpretation and identity |
| Pending timeout, clock-skew allowance | Metadata | Shared expiry and recovery contract |
| Hard object limit, reserved coordination space | Metadata | Compatible admission and recovery |
| Inline value and aggregate budgets | Client | Select direct or logged commits; existing inline values remain readable |
| Soft split thresholds | Client, after finding 4 is fixed | Control when topology changes occur |
| Cache sizes, persistent-cache directory and capacity | Client | Local resources |
| Retry delays, leaf parallelism, GC scheduling and scan depth | Client | Local execution policy |
| Active operation, transaction, and GC candidate limits | Client | Bound local work and resource use |
| Explicit stale-read allowance | Read operation | Caller-selected read behavior |

Fixed path layouts, encodings, and protocol rules should remain covered by the
persisted protocol version. They do not need separate configuration fields.

## Initial review verification

Four focused tests ran outside the repository during this review. All passed
their assertions, which reproduced the failures or checked the expected
behavior below. Production code was unchanged. The full `make test` suite was
not run for this review.

| Test | Setup | Observed result |
| --- | --- | --- |
| Existing key after reopening | Write a 256-byte key with default settings, then reopen with a 512-byte hard limit and 128 bytes of reserved space | Reading succeeds; overwriting returns `InvalidInput` |
| Existing inline value after reopening | Write a 900-byte inline value with default settings, then reopen with the same smaller limits | Inserting another key fails after the 30-second capacity retry budget; the original value remains readable |
| Different inline policies | Open two database instances with default inline policy and `InlinePolicy::none()`; alternate writes and reads | Both instances read the expected values |
| Soft threshold blocks capacity recovery | Use a 512-byte hard limit, 128 bytes of reserved space, and either a 256 KiB or 384-byte soft threshold; insert 32-byte keys with inlining disabled | The seventh insertion fails after the capacity retry budget in both cases |

The implementation should add deterministic regression tests for:

- Concurrent creation with different proposed database settings.
- Reopening a database with custom stored settings and default client options.
- Missing, invalid, or incompatible metadata.
- Database instances with different local performance options.
- The capacity failures reproduced above.
- Lease refresh, expiry, and GC retention under the loaded timing profile.

Run `make test` after implementation.
