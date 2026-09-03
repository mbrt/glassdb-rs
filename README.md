# GlassDB (Rust)

[<img alt="crates.io" src="https://img.shields.io/crates/v/glassdb.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/glassdb)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-glassdb-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/glassdb)
[<img alt="build status" src="https://img.shields.io/github/actions/workflow/status/mbrt/glassdb-rs/build.yml?style=for-the-badge" height="20">](https://github.com/mbrt/glassdb-rs/actions?query=branch%3Amain)

Glass DB is a pure Rust key/value store on top of object storage (Amazon S3 or
Google Cloud Storage) that is _stateless_ and supports _ACID transactions_.
Clients import Glass DB as a library and don't need to deploy, nor depend on any
additional services. Everything is built on top of object storage.

This project started as the Rust implementation of the original [Go
project](https://github.com/mbrt/glassdb). The commit protocol and on-disk
format evolved past the original and is no longer compatible.

The interface is inspired by [BoltDB](https://github.com/boltdb/bolt) and
Apple's [FoundationDB](https://github.com/apple/foundationdb).

## Status

> [!WARNING]
> This is still alpha software.

- Runtime: async, built on `tokio`.
- Backends: in-memory (`glassdb::backend::memory`), Amazon S3
  (`glassdb-backend-s3`, behind the `s3` feature), and Google Cloud Storage
  (`glassdb-backend-gcs`, behind the `gcs` feature).

Transactions _should_ be working correctly and performance could definitely
improve. Interfaces and file formats are _not_ stable and can still change at
any point.

For a deep dive into the internals, see the [architecture
doc](docs/architecture.md) and an earlier (now outdated)
[overview](https://blog.mbrt.dev/posts/transactional-object-storage).

We support both [Google GCS](https://cloud.google.com/storage/) and [Amazon
S3](https://aws.amazon.com/s3/). Adding [Azure Blob
Storage](https://azure.microsoft.com/en-us/products/storage/blobs/) should be
very easy.

## Quick start

```rust
use glassdb::Database;
use glassdb::backend::memory::MemoryBackend;

#[tokio::main]
async fn main() -> Result<(), glassdb::Error> {
    let db = Database::open("example", MemoryBackend::new()).await?;

    let users = db.create_collection_if_absent("users").await?;

    // Single-key helpers run in their own transaction.
    users.write(b"alice", b"hello").await?;
    let v = users.read(b"alice").await?.expect("alice exists");
    assert_eq!(v, b"hello");

    // Multi-key serializable transaction with automatic conflict retries.
    // `tx` is an owned handle.
    let users = &users;
    db.tx(|tx| async move {
        let cur = tx
            .read(users, b"counter")
            .await?
            .unwrap_or_else(|| b"0".to_vec());
        let next = String::from_utf8_lossy(&cur).parse::<i64>().unwrap_or(0) + 1;
        tx.write(users, b"counter", next.to_string().as_bytes())
    })
    .await?;

    // Collection lifecycle changes compose with key changes in the same transaction.
    let active = db
        .tx(|tx| async move {
            let root = tx.root_collection();
            let active = tx.create_collection(&root, b"active").await?;
            tx.write(&active, b"alice", b"enabled")?;
            Ok(active)
        })
        .await?;
    active.drop_collection().await?;

    db.shutdown().await;
    Ok(())
}
```

To bound how long a transaction may run, wrap it in `tokio::time::timeout`:
dropping the future is the cancellation mechanism (the commit protocol
recovers any in-flight state).

## Cloud backends

The S3 and GCS backends are gated behind cargo features so their heavy
dependencies are only pulled in when needed:

```toml
glassdb = { version = "0.1", features = ["s3", "gcs"] }
```

Both implement the same `Backend` trait and can be dropped into `Database::open`:

```rust,ignore
// Amazon S3 (feature = "s3"): construct an aws-sdk-s3 client, then:
let backend = glassdb::s3::S3Backend::new(s3_client, "my-bucket");

// Google Cloud Storage (feature = "gcs"): uses Application Default Credentials.
let backend = glassdb::gcs::GcsBackend::new("my-bucket");
```

Each cloud crate is tested against a pure-Rust in-process fake of its API (no
Docker or live credentials required), mirroring the original `gofakes3` /
fake-GCS test setup.

## Why

This project makes the following specific tradeoffs:

- Optimizes for rare conflicts between transactions (optimistic locking).
- Readers are rarely blocked.
- Clients are completely stateless and ephemeral. For example, they can be
  scaled down to zero. We avoid explicit coordination between clients (e.g.
  there's no need for consensus messages).
- Requires access to object storage (the lowest latency the better) with
  requests preconditions (both Google GCS and AWS S3 meet the requirements).
- Assumes that, when transactions race each other, it's better to be slow than
  to be incorrect.
- High throughput is better than low latency.
- Allows stale reads if explicitly requested, but defaults to strong consistency
  in all cases.
- Values are in the range 1KB to 1MB.

Glass DB makes sense in contexts where there are many writers that rarely write
to the same keys or reads are more frequent than writes.

Why rewrite in Rust? Because having proper [DST
tests](#deterministic-simulated-time-in-tests) was proven impossible, and I
found that to be a deal-breaker for a stable database project. With LLM-powered
translation (and lots of review time), I found the porting appealing.

### Example 1: User settings

One example could be storing user settings. Every key is
dedicated to one user and the value contains all the settings. This way we can
update each user independently (and scale horizontally). In the rare case where
two updates for the same user arrive concurrently, we _don't_ produce an
inconsistent result but retry the transaction.

### Example 2: Low frequency updates

The application serves low traffic (e.g. one query per minute). What are the
choices today?

- Single machine / VM mostly idle.
- "Serverless" function with a managed database (for example Google Cloud Run +
  Cloud SQL, or fly.io).

Neither seem cost effective in the scenario. We are talking about $10 a month,
which is not huge, but can we do better?

Yes. With Glass DB you only pay for each query and long term storage. In the
case of GCS (as of 2023) we are talking about:

- $0.020 per GB per month
- $0.05 per 10k write / list ops
- $0.004 per 10k read ops

At a rate of one write per minute this would be around $2 a month. Less usage?
Even less money.

### Example 3: Analytics

Data ingestion can usually be done in parallel and designed in such a way that
different processes write independently.

A compaction process can run in parallel to the ingestion, bringing the data in
a shape better suited for the query layer.

Compaction and ingestion are mostly independent, but we must make sure to be
robust to crashes and restarts (avoiding e.g. double-counting or event
duplicates). This can be ensured with transactions provided by Glass DB. If most
transactions don't conflict with each other, the throughput will scale mostly
linearly (See [Performance](#performance)).

## Performance

We are obviously bound by object storage's latencies which are typically:

| Operation | Size    | Mean (ms) | Std Dev (ms) | Median (ms) | 90th % (ms) |
| --------- | ------- | --------- | ------------ | ----------- | ----------- |
| Download  | 1 KiB   | 57.4      | 6.6          | 56.8        | 64.8        |
| Download  | 100 KiB | 55.4      | 6.7          | 53.3        | 63.1        |
| Download  | 1 MiB   | 56.7      | 3.8          | 57.7        | 59.9        |
| Metadata  | 1 KiB   | 31.5      | 8.0          | 28.1        | 41.3        |
| Upload    | 1 KiB   | 70.4      | 17.3         | 64.7        | 88.8        |
| Upload    | 100 KiB | 88.9      | 14.6         | 83.1        | 105.0       |
| Upload    | 1 MiB   | 117.5     | 12.6         | 115.9       | 131.0       |

This is a lot slower than most databases, but still has a few advantages:

1. Throughput: we can leverage object storage scalability by reading and writing
   many objects in parallel. In this way we can perform many transactions per
   second (scale linearly). We would only be limited by bandwidth (see [GCS
   quotas](https://cloud.google.com/storage/quotas#bandwidth)).

1. Size scalability: object storage scales to petabytes and probably more, as
   cloud providers keep working on making them faster and more scalable.

The benchmark below uses 5,000 keys per collection. It runs single-key and
10-key read-only and read-modify-write transaction shapes together. It varies
each shape from 1 through 200 workers and opens up to five `Database` clients,
each with an independent collection.

### Throughput

GlassDB throughput scales mostly linearly with the number of concurrent
workers:

![](docs/img/tx-throughput.png)

As you can see, median throughput increases almost linearly for single-key
reads. It reaches 2.5k transactions per second with 200 workers per shape (800
workers in total).

The multi-key read-modify-write shape reaches the modeled S3 write limit for its
collection prefixes. More independent collections can add more provider
partitions.

### Latency

Read latency stays mostly flat as concurrency increases. Multi-key write latency
rises when hitting S3 prefix write limits:

![](docs/img/tx-latency.png)

The p50-p90 bands also show tail-latency growth. Transaction retries can add
more delay after a conflict.

## Development

```bash
cargo build --workspace
make test     # fmt --check + clippy -D warnings + cargo test
make test-sim # tests under the deterministic simulation executor (+ fuzz-corpus replay)
make fuzz     # fuzz testing under DST. See Makefile for longer sweeps
```

Updating `glassdb-proto` protos require the Protocol Buffers compiler
(`protoc`).

### Deterministic simulated time in tests

GlassDB uses deterministic time combined with coverage-guided fuzz testing,
inspired by FoundationDB, for stress test the implementation while producing
reproducible failures. See [testing-dst](docs/guides/testing-dst.md) for more
details.

## Design notes

See [architecture.md](docs/architecture.md) for the design decisions behind the
implementation, including the concurrency model, time and determinism, error
handling, and persistent encodings.
