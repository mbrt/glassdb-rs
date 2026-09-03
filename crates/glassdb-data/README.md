# glassdb-data

[<img alt="crates.io" src="https://img.shields.io/crates/v/glassdb-data.svg?style=for-the-badge&color=fc8d62&logo=rust" height="20">](https://crates.io/crates/glassdb-data)
[<img alt="docs.rs" src="https://img.shields.io/badge/docs.rs-glassdb--data-66c2a5?style=for-the-badge&labelColor=555555&logo=docs.rs" height="20">](https://docs.rs/glassdb-data)

Core data types for [GlassDB](https://github.com/mbrt/glassdb-rs), a stateless
ACID key/value store on top of object storage: database, collection, and
transaction identities, plus the encoding of the object-storage paths that hold
them.

> [!NOTE]
> This is an internal crate of the GlassDB workspace. It is published only
> because the public [`glassdb`](https://crates.io/crates/glassdb) crate depends
> on it, and it carries no API stability guarantees of its own.

## License

Licensed under the [Apache License, Version 2.0](https://github.com/mbrt/glassdb-rs/blob/main/LICENSE).
