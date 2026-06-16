# GlassDB (Rust)

Glass DB is a pure Rust key/value store on top of object storage (Amazon S3 or
Google Cloud Storage) that is _stateless_ and supports _ACID transactions_.
Clients import Glass DB as a library and don't need to deploy, nor depend on any
additional services. Everything is built on top of object storage.

This is the async (`tokio`-based) Rust implementation, ported from the original
[Go project](https://github.com/mbrt/glassdb). The commit protocol and on-disk
format is compatible with the original project.

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
doc](docs/architecture.md) and the companion [blog
post](https://blog.mbrt.dev/posts/transactional-object-storage) (created for the
Go version, but still relevant).

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

    let users = db.collection(b"users");
    users.create().await?;

    // Single-key helpers run in their own transaction.
    users.write(b"alice", b"hello").await?;
    let v = users.read(b"alice").await?;
    assert_eq!(v, b"hello");

    // Multi-key serializable transaction with automatic conflict retries.
    // `tx` is an owned handle; write the body as `|tx| async move { ... }`.
    let users = &users;
    db.tx(|tx| async move {
        let cur = match tx.read(users, b"counter").await {
            Ok(v) => v,
            Err(e) if e.is_not_found() => b"0".to_vec(),
            Err(e) => return Err(e),
        };
        let next = String::from_utf8_lossy(&cur).parse::<i64>().unwrap_or(0) + 1;
        tx.write(users, b"counter", next.to_string().as_bytes())
    })
    .await?;

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

* Optimizes for rare conflicts between transactions (optimistic locking).
* Readers are rarely blocked.
* Clients are completely stateless and ephemeral. For example, they can be
  scaled down to zero. We avoid explicit coordination between clients (e.g.
  there's no need for consensus messages).
* Requires access to object storage (the lowest latency the better) with
  requests preconditions (both Google GCS and AWS S3 meet the requirements).
* Assumes that, when transactions race each other, it's better to be slow than
  to be incorrect.
* High throughput is better than low latency.
* Allows stale reads if explicitly requested, but defaults to strong consistency
  in all cases.
* Values are in the range 1KB to 1MB.

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

* Single machine / VM mostly idle.
* "Serverless" function with a managed database (for example Google Cloud Run +
  Cloud SQL, or fly.io).

Neither seem cost effective in the scenario. We are talking about $10 a month,
which is not huge, but can we do better?

Yes. With Glass DB you only pay for each query and long term storage. In the
case of GCS (as of 2023) we are talking about:

* $0.020 per GB per month
* $0.05 per 10k write / list ops
* $0.004 per 10k read ops

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

**TODO**

See the [Go version](https://github.com/mbrt/glassdb) for now, which is very
similar.

## Development

```bash
cargo build --workspace
make test     # fmt --check + clippy -D warnings + cargo test
make test-sim # tests under the deterministic simulation executor (+ fuzz-corpus replay)
make test-all # test + test-sim
make format   # cargo fmt --all
make lint     # fmt --check + clippy -D warnings
```

Updating `glassdb-proto` protos require the Protocol Buffers compiler
(`protoc`).

### Deterministic simulated time in tests

GlassDB uses deterministic time combined with coverage-guided fuzz testing,
inspired by FoundationDB, for stress test the implementation while producing
reproducible failures. See [dst-approach](docs/dst-approach.md) for more
details.

## Design notes

See [PORTING.md](PORTING.md) for the design decisions behind the implementation
(concurrency model, time/determinism, error handling, and encoding fidelity).
