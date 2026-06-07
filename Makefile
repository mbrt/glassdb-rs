.PHONY: test lint format build sim-test fuzz fuzz-min bench bench-score flamegraph profile

build:
	cargo build --workspace

test: lint
	cargo test --workspace

lint:
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings

format:
	cargo fmt --all

# Run the Criterion transaction microbenchmarks (memory + simulated S3/GCS).
# The concurrency/throughput "scale" benchmarks (real or simulated cloud
# backends) live in the `glassdb-bench-scale` crate; run them directly, e.g.:
#   cargo run --release -p glassdb-bench-scale --bin rtbench -- --backend memory --test-name simple
#   cargo run --release -p glassdb-bench-scale --bin backendbench -- --backend memory
# See hack/aws-bench/README.md for the real-S3 (EC2 + CloudFormation) harness.
bench:
	cargo bench -p glassdb

# Print the autoresearch performance metric for the current tree: a single-client,
# deterministic op-cost score (lower is better) plus memory/CPU secondary axes.
# Runs the suite a few times and reports the median; this is the basis for the
# CI perf-regression check. Append `-- --json` args for machine-readable output.
bench-score:
	@cargo run --release -p glassdb-bench-score --bin autoresearch -- --count 3

# Record a CPU flamegraph to guide profiling work (a diagnostic aid only -
# excluded from `test`, `bench-score`, and the autoresearch gate; it changes no
# source and is not part of any metric). Tunable via TARGET/COUNT/OUT; see
# hack/perf/README.md.
flamegraph profile:
	hack/perf/profile.sh

# Run the test suite under the in-repo deterministic simulation executor
# (ADR-011, `--cfg sim`). The cloud backend crates (s3/gcs) use real
# tokio/reqwest/aws-sdk and are excluded. The `glassdb` run enables the `sim`
# harness feature, which also pulls in the byte-for-byte op-stream determinism
# self-check (tests/concurrent_sim.rs).
sim-test:
	RUSTFLAGS="--cfg sim" cargo test \
		-p glassdb-data -p glassdb-concurr -p glassdb-backend \
		-p glassdb-storage -p glassdb-trans
	RUSTFLAGS="--cfg sim" cargo test -p glassdb --features sim

# Run the deterministic concurrency fuzzer. Requires the nightly toolchain and
# cargo-fuzz (`cargo install cargo-fuzz`). `cargo fuzz` sets its own RUSTFLAGS
# (sanitizer + coverage) which overrides the `[build] rustflags` in
# fuzz/.cargo/config.toml, so `--cfg sim` must be supplied via the environment
# (cargo-fuzz appends its flags to it). With the deterministic executor active,
# any crash reproduces exactly from its input (schedule tape + seed + workload):
#   cd fuzz && RUSTFLAGS="--cfg sim" cargo +nightly fuzz run concurrent_tx <crash-file>
#
# Each run is single-threaded (for deterministic, reproducible crashes), so
# parallelism comes from libFuzzer's `-fork` mode, which runs FUZZ_JOBS child
# processes that share the corpus. Defaults to all cores; override with e.g.
# `make fuzz FUZZ_JOBS=4`. Fork mode ignores OOMs/timeouts by default but stops
# on the first crash (saving it to fuzz/artifacts/) so bugs surface immediately.
FUZZ_JOBS ?= $(shell nproc)
fuzz:
	cd fuzz && RUSTFLAGS="--cfg sim" cargo +nightly fuzz run concurrent_tx -- \
		-fork=$(FUZZ_JOBS)

# Minimize the fuzz corpus: drop inputs that add no coverage, keeping the
# smallest set that preserves the same reachable behavior. Run after a fuzzing
# session (or before committing the corpus) to keep it small and fast to replay.
# Same `--cfg sim` requirement as `fuzz` (see above).
fuzz-min:
	cd fuzz && RUSTFLAGS="--cfg sim" cargo +nightly fuzz cmin concurrent_tx
