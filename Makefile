.PHONY: test lint format build sim-test fuzz

build:
	cargo build --workspace

test: lint
	cargo test --workspace

lint:
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings

format:
	cargo fmt --all

# Run the test suite under the madsim deterministic simulator (ADR-008). The
# cloud backend crates (s3/gcs) use real tokio/reqwest/aws-sdk and cannot build
# under madsim, so they are excluded. The `glassdb` run enables the `sim` harness
# feature, which also pulls in the byte-for-byte op-stream determinism
# self-check (tests/concurrent_sim.rs).
sim-test:
	RUSTFLAGS="--cfg madsim" cargo test \
		-p glassdb-data -p glassdb-concurr -p glassdb-backend \
		-p glassdb-storage -p glassdb-trans
	RUSTFLAGS="--cfg madsim" cargo test -p glassdb --features sim

# Run the deterministic concurrency fuzzer. Requires the nightly toolchain and
# cargo-fuzz (`cargo install cargo-fuzz`). `cargo fuzz` sets its own RUSTFLAGS
# (sanitizer + coverage) which overrides the `[build] rustflags` in
# fuzz/.cargo/config.toml, so `--cfg madsim` must be supplied via the
# environment (cargo-fuzz appends its flags to it). With madsim active, any
# crash reproduces exactly:
#   cd fuzz && RUSTFLAGS="--cfg madsim" cargo +nightly fuzz run concurrent_tx <crash-file>
fuzz:
	cd fuzz && RUSTFLAGS="--cfg madsim" cargo +nightly fuzz run concurrent_tx
