#!/usr/bin/env bash
# Regenerate the prost-derived bindings in `crates/glassdb-proto/src/generated.rs`
# from `crates/glassdb-proto/proto/transaction.proto`. Run after editing the
# `.proto` file. Requires `protoc` on `PATH` (the only step in the project that
# does); regular `cargo build`/`cargo test` do not.
set -euo pipefail

cd "$(dirname "$0")/.."
cargo run -q -p glassdb-proto --features regen --bin regen-proto
