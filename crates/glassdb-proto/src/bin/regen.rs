//! Regenerate `crates/glassdb-proto/src/generated.rs` from
//! `proto/transaction.proto`. Requires `protoc` on `PATH`.
//!
//! Invoke through `hack/regen-proto.sh` or directly:
//!
//! ```sh
//! cargo run -p glassdb-proto --features regen --bin regen-proto
//! ```

use std::path::PathBuf;
use std::process::Command;

fn main() -> std::io::Result<()> {
    let crate_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let src_dir = crate_dir.join("src");
    let proto_dir = crate_dir.join("proto");

    prost_build::Config::new()
        .out_dir(&src_dir)
        .compile_protos(&[proto_dir.join("transaction.proto")], &[&proto_dir])?;

    // `prost-build` derives the file name from the proto package
    // (`package glassdb;`), but we keep a stable file name in source.
    let generated = src_dir.join("generated.rs");
    std::fs::rename(src_dir.join("glassdb.rs"), &generated)?;

    // `prost-build` formats with `prettyplease`, whose style differs from
    // `rustfmt` in a few places. Running `rustfmt` keeps the committed file
    // clean under `cargo fmt --check`.
    let status = Command::new("rustfmt")
        .arg("--edition=2024")
        .arg(&generated)
        .status()?;
    if !status.success() {
        return Err(std::io::Error::other(format!(
            "rustfmt failed with status {status}"
        )));
    }
    Ok(())
}
