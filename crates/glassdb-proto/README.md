# glassdb-proto

[<img alt="crates.io" src="https://img.shields.io/crates/v/glassdb-proto.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/glassdb-proto)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-glassdb--proto-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/glassdb-proto)

Protobuf definitions for the persistent records of
[GlassDB](https://github.com/mbrt/glassdb-rs), a stateless ACID key/value store on
top of object storage.

The `prost` bindings are pre-generated and checked into the repository, so
building this crate does not require `protoc`. The optional `regen` feature
enables the `regen-proto` binary that rebuilds them from the `.proto` sources.

> [!NOTE]
> This is an internal crate of the GlassDB workspace. It is published only
> because the public [`glassdb`](https://crates.io/crates/glassdb) crate depends
> on it, and it carries no API stability guarantees of its own.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/mbrt/glassdb-rs/blob/main/LICENSE).
