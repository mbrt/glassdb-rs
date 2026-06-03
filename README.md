# GlassDB (Rust)

A stateless, ACID, serializable key/value store layered on top of object
storage. Clients use GlassDB as a library and don't need to deploy or depend on
any additional services — everything is built on top of object storage.

This is the async (`tokio`-based) Rust implementation. It ships in-memory,
Amazon S3, and Google Cloud Storage backends, plus latency/scheduler/logging
middleware.

## Status

- Runtime: async, built on `tokio`.
- Backends: in-memory (`glassdb::backend::memory`), Amazon S3
  (`glassdb-backend-s3`, behind the `s3` feature), and Google Cloud Storage
  (`glassdb-backend-gcs`, behind the `gcs` feature).
- Middleware decorators — latency injection, a deterministic scheduler, and a
  `tracing` logger — live in `glassdb::backend::middleware`.
- Encodings (path base64, `TxId` hex, protobuf transaction logs) and the commit
  protocol are wire-compatible with the original GlassDB on-disk format.

## Workspace layout

The code is a Cargo workspace of internal crates. Only the top-level `glassdb`
crate is meant to be used directly.

| Crate | Responsibility |
| --- | --- |
| `glassdb-data` | `TxId`, `TxIdSet`, and order-preserving path encoding. |
| `glassdb-proto` | `prost`-generated transaction-log protobuf messages. |
| `glassdb-concurr` | Concurrency utilities: `Ctx`, `Background`, `Fanout`, `Retry`, `Dedup`. |
| `glassdb-backend` | The `Backend` async trait, in-memory backend, stats decorator, and middleware (delay, scheduler, logger). |
| `glassdb-backend-s3` | Amazon S3 backend (`aws-sdk-s3`), enabled via the `s3` feature. |
| `glassdb-backend-gcs` | Google Cloud Storage backend (GCS JSON API), enabled via the `gcs` feature. |
| `glassdb-storage` | Byte-weighted LRU cache, value versioning, local/global caching, locker, and transaction logger. |
| `glassdb-trans` | The transaction engine: monitor, reader, GC, distributed locker, and commit algorithm. |
| `glassdb` | The public API: `DB`, `Collection`, `Tx`, iterators, and `Stats`. |

## Quick start

```rust
use std::sync::Arc;

use glassdb::backend::memory::MemoryBackend;
use glassdb::{Backend, Ctx, DB};

#[tokio::main]
async fn main() -> Result<(), glassdb::Error> {
    let ctx = Ctx::background();
    let backend: Arc<dyn Backend> = Arc::new(MemoryBackend::new());
    let db = DB::open(&ctx, "example", backend).await?;

    let users = db.collection(b"users");
    users.create(&ctx).await?;

    // Single-key helpers run in their own transaction.
    users.write(&ctx, b"alice", b"hello").await?;
    let v = users.read_strong(&ctx, b"alice").await?;
    assert_eq!(v, b"hello");

    // Multi-key serializable transaction with automatic conflict retries.
    // `tx` is an owned handle; write the body as `|tx| async move { ... }`.
    let users = &users;
    db.tx(&ctx, |tx| async move {
        let cur = match tx.read(users, b"counter").await {
            Ok(v) => v,
            Err(e) if e.is_not_found() => b"0".to_vec(),
            Err(e) => return Err(e),
        };
        let next = String::from_utf8_lossy(&cur).parse::<i64>().unwrap_or(0) + 1;
        tx.write(users, b"counter", next.to_string().as_bytes())
    })
    .await?;

    db.close().await;
    Ok(())
}
```

## Cloud backends

The S3 and GCS backends are gated behind cargo features so their heavy
dependencies are only pulled in when needed:

```toml
glassdb = { version = "0.1", features = ["s3", "gcs"] }
```

Both implement the same `Backend` trait and can be dropped into `DB::open`:

```rust,ignore
// Amazon S3 (feature = "s3"): construct an aws-sdk-s3 client, then:
let backend = glassdb::s3::S3Backend::new(s3_client, "my-bucket");

// Google Cloud Storage (feature = "gcs"): uses Application Default Credentials.
let backend = glassdb::gcs::GcsBackend::new("my-bucket");
```

Each cloud crate is tested against a pure-Rust in-process fake of its API (no
Docker or live credentials required), mirroring the original `gofakes3` /
fake-GCS test setup.

## Development

```bash
cargo build --workspace
cargo test --workspace
cargo fmt --all
cargo clippy --all-targets --all-features -- -D warnings
```

Equivalent `Makefile` targets are also available:

```bash
make test     # fmt --check + clippy -D warnings + cargo test
make format   # cargo fmt --all
make lint     # fmt --check + clippy -D warnings
```

Building `glassdb-proto` requires the Protocol Buffers compiler (`protoc`).

### Deterministic time in tests

Staleness and lock waits are driven by `tokio::time::Instant`, so tests can use
`#[tokio::test(start_paused = true)]` (and `tokio::time::advance`) to make
time-dependent paths deterministic and fast without real sleeps.

## Design notes

See [PORTING.md](PORTING.md) for the design decisions behind the
implementation (concurrency model, time/determinism, error handling, and
encoding fidelity).
