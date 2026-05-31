use std::io::Result;

fn main() -> Result<()> {
    println!("cargo:rerun-if-changed=proto/transaction.proto");
    prost_build::compile_protos(&["proto/transaction.proto"], &["proto"])?;
    Ok(())
}
