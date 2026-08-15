# glassdb-backend

[<img alt="crates.io" src="https://img.shields.io/crates/v/glassdb-backend.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/glassdb-backend)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-glassdb--backend-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/glassdb-backend)

Object-storage backend abstraction for [GlassDB](https://github.com/mbrt/glassdb-rs),
a stateless ACID key/value store on top of object storage.

The `Backend` trait is a small, content-CAS-only contract over an object store:
plain and version-conditional reads, conditional writes and deletion, and list.
Every object carries an opaque CAS version (its ETag or generation), which is the
only token used for conditional operations.

This crate also ships an in-memory backend for tests and benchmarks, plus
middleware (latency injection, statistics) that wraps any backend.

Implementations for real object stores live in
[`glassdb-backend-s3`](https://crates.io/crates/glassdb-backend-s3) and
[`glassdb-backend-gcs`](https://crates.io/crates/glassdb-backend-gcs).

Applications normally depend on [`glassdb`](https://crates.io/crates/glassdb),
which re-exports this crate as `glassdb::backend`. Depend on it directly only to
implement a backend for another object store.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/mbrt/glassdb-rs/blob/main/LICENSE).
