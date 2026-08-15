# glassdb-backend-gcs

[<img alt="crates.io" src="https://img.shields.io/crates/v/glassdb-backend-gcs.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/glassdb-backend-gcs)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-glassdb--backend--gcs-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/glassdb-backend-gcs)

Google Cloud Storage backend for [GlassDB](https://github.com/mbrt/glassdb-rs), a
stateless ACID key/value store on top of object storage.

Each logical key maps to a single GCS object whose body holds the value. GCS
provides native content compare-and-swap through object `generation`
preconditions, so the opaque version token is the object generation: conditional
reads use `ifGenerationNotMatch`, while writes and deletion require an exact
generation condition.

Authentication uses Application Default Credentials by default, and a custom
token provider can be supplied instead.

Most users should enable the `gcs` feature of the
[`glassdb`](https://crates.io/crates/glassdb) crate instead of depending on this
crate directly:

```toml
glassdb = { version = "0.1", features = ["gcs"] }
```

The backend is then reachable as `glassdb::gcs`:

```rust,ignore
let backend = glassdb::gcs::GcsBackend::new("my-bucket");
let db = glassdb::Database::open("example", backend).await?;
```

## License

Licensed under the [Apache License, Version 2.0](https://github.com/mbrt/glassdb-rs/blob/main/LICENSE).
