# glassdb-backend-s3

[<img alt="crates.io" src="https://img.shields.io/crates/v/glassdb-backend-s3.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/glassdb-backend-s3)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-glassdb--backend--s3-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/glassdb-backend-s3)

Amazon S3 backend for [GlassDB](https://github.com/mbrt/glassdb-rs), a stateless
ACID key/value store on top of object storage.

Each logical key maps to a single S3 object whose body holds the value.
Coordination is content compare-and-swap only: conditional writes use `If-Match`
and `If-None-Match`, conditional deletion uses `If-Match` on the object ETag, and
conditional reads use `If-None-Match`.

Most users should enable the `s3` feature of the
[`glassdb`](https://crates.io/crates/glassdb) crate instead of depending on this
crate directly:

```toml
glassdb = { version = "0.1", features = ["s3"] }
```

The backend is then reachable as `glassdb::s3`:

```rust,ignore
// Construct an aws-sdk-s3 client, then:
let backend = glassdb::s3::S3Backend::new(s3_client, "my-bucket");
let db = glassdb::Database::open("example", backend).await?;
```

## Features

- `fake-server`: exposes an in-process, pure-Rust fake of the S3 API, so the real
  S3 transport can be driven against an in-memory server in tests and benchmarks
  (no Docker or live credentials needed).

## License

Licensed under the [Apache License, Version 2.0](https://github.com/mbrt/glassdb-rs/blob/main/LICENSE).
