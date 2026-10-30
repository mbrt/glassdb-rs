.PHONY: test test-unit test-sim test-bench lint format build clean fuzz fuzz-min bench bench-all flamegraph

# Flags for every build under the in-repo deterministic simulation executor:
# `--cfg sim` routes spawn/time/randomness through it, and `--cfg tokio_unstable`
# lets it seed tokio's `select!` branch-poll RNG (see crates/glassdb-concurr).
SIM_RUSTFLAGS = --cfg sim --cfg tokio_unstable

build:
	cargo build --workspace

clean:
	cargo clean
	cd fuzz && cargo clean

# Run all database checks. Benchmark checks belong to performance CI.
test: lint test-unit test-sim

test-unit:
	cargo test --workspace --exclude glassdb-bench-scale --all-features

test-bench:
	python3 -m unittest hack.ci.test_perf_report hack.ci.test_perf_compare
	# The plot and comparison tools declare their dependencies as uv scripts.
	cd hack/perf && for test in test_perfbench_results test_compare test_plot_results; do \
		uv run --script $$test.py || exit 1; \
	done
	cargo test -p glassdb-bench-scale
	cargo test --workspace --bench '*'

lint:
	cargo fmt --all -- --check
	cargo clippy --all-targets --all-features -- -D warnings
	# Compile and lint every simulation branch without running ordinary tests under
	# the deterministic runtime adapters.
	RUSTFLAGS="$(SIM_RUSTFLAGS)" cargo clippy --lib --tests \
		-p glassdb-data -p glassdb-concurr -p glassdb-backend \
		-p glassdb-storage -p glassdb-trans -p glassdb \
		--features glassdb-storage/sim,glassdb/sim -- -D warnings

format:
	cargo fmt --all

EXPERIMENTS_DIR = hack/perf/experiments
BENCH_ALL_OUT = $(or $(OUT),target/performance/bench-all-$(or $(TIME),real).json)

# Run perfbench experiments on virtual time, with the latency jitter of the
# provider. The default runs the experiments of the CI comparison. Select other
# experiment files or directories with EXPERIMENT, for example
# `make bench EXPERIMENT=hack/perf/experiments/any-time/contention.toml`.
# Virtual time is fast, but it does not show CPU saturation: use real time for
# the canonical plots (crates/glassdb-bench-scale/README.md explains the
# experiments and the time modes).
bench:
	cargo run --release -p glassdb-bench-scale --bin perfbench -- \
		--time=virtual --experiment=$(or $(EXPERIMENT),$(EXPERIMENTS_DIR)/default)

# Run all experiments one after the other, into one result in OUT. The default
# is one run of each in real time, with the local S3 model of the canonical
# plots (hack/perf/README.md). It takes hours and uses all CPUs: keep the host
# idle. TIME=virtual runs only the experiments that can run on virtual time.
bench-all:
	mkdir -p $(dir $(BENCH_ALL_OUT))
ifeq ($(TIME),virtual)
	cargo run --release -p glassdb-bench-scale --bin perfbench -- \
		--time=virtual --output=$(BENCH_ALL_OUT) \
		--experiment=$(EXPERIMENTS_DIR)/default \
		--experiment=$(EXPERIMENTS_DIR)/any-time
else
	cargo run --release -p glassdb-bench-scale --bin perfbench -- \
		--backend=memory --delays=s3 --delay-scale=0.2 --prefix-depth=3 \
		--drain-timeout=90s --output=$(BENCH_ALL_OUT) \
		--experiment=$(EXPERIMENTS_DIR)/default \
		--experiment=$(EXPERIMENTS_DIR)/any-time \
		--experiment=$(EXPERIMENTS_DIR)/real-time
endif

# Record a CPU flamegraph to guide profiling work (a diagnostic aid only -
# excluded from `test` and benchmarks; it changes no
# source and is not part of any metric). Tunable via
# EXPERIMENT/CASE/SECONDS_PER_CASE/OUT; see hack/perf/README.md.
flamegraph:
	hack/perf/profile.sh

# Run only tests that start the in-repo deterministic simulation executor
# (ADR-011, `--cfg sim`). Storage and `glassdb` enable their simulation
# harnesses so each crate owns its committed corpus replay. The final `sim_test`
# is a substring filter over each full test name. Put every selected test in a
# `sim_tests` module. The stable test policy discovers source roots marked with
# `#![cfg(sim)]` and enforces this.
test-sim:
	RUSTFLAGS="$(SIM_RUSTFLAGS)" cargo test --profile sim-test \
		-p glassdb-concurr -p glassdb-backend \
		-p glassdb-storage -p glassdb \
		--lib --test sim \
		--features glassdb-storage/sim,glassdb/sim \
		sim_test

# Run the deterministic concurrency fuzzers. Requires the nightly toolchain and
# cargo-fuzz (`cargo install cargo-fuzz`). `cargo fuzz` sets its own RUSTFLAGS
# (sanitizer + coverage) which overrides the `[build] rustflags` in
# fuzz/.cargo/config.toml, so `--cfg sim --cfg tokio_unstable` must be supplied
# via the environment (cargo-fuzz appends its flags to it). With the
# deterministic executor active, any crash reproduces exactly from its input
# (schedule tape + seed + workload):
#   cd fuzz && RUSTFLAGS="--cfg sim --cfg tokio_unstable" cargo +nightly fuzz run <target> <crash-file>
#
# `make fuzz` fuzzes every target `cargo fuzz list` reports (the single source of
# truth -- adding a target needs no change here), one after another, each bounded
# by FUZZ_TIME seconds (so the loop terminates) and forked across all cores.
# Sequential runs give each target every core (no oversubscription); the
# trade-off is wall time = sum of the per-target budgets. Override e.g.
# `make fuzz FUZZ_TARGETS=cycle FUZZ_TIME=0` to fuzz one target indefinitely, or
# `make fuzz FUZZ_TIME=300` for a longer sweep.
FUZZ_JOBS ?= $(shell nproc)
FUZZ_TIME ?= 60
FUZZ_TARGETS ?=
fuzz:
	cd fuzz && targets="$(FUZZ_TARGETS)"; \
		[ -n "$$targets" ] || targets=$$(cargo +nightly fuzz list); \
		for t in $$targets; do \
			echo "=== fuzzing $$t for $(FUZZ_TIME)s ==="; \
			RUSTFLAGS="$(SIM_RUSTFLAGS)" cargo +nightly fuzz run $$t -- \
				-fork=$(FUZZ_JOBS) -max_total_time=$(FUZZ_TIME) \
				-timeout=10 || exit $$?; \
		done

# Minimize the fuzz corpora: drop inputs that add no coverage, keeping the
# smallest set that preserves the same reachable behavior. Run after a fuzzing
# session (or before committing the corpus) to keep it small and fast to replay.
# Loops over the same auto-discovered target list as `fuzz`; same
# `--cfg sim --cfg tokio_unstable` requirement (see above).
fuzz-min:
	cd fuzz && targets="$(FUZZ_TARGETS)"; \
		[ -n "$$targets" ] || targets=$$(cargo +nightly fuzz list); \
		for t in $$targets; do \
			RUSTFLAGS="$(SIM_RUSTFLAGS)" cargo +nightly fuzz cmin $$t || exit $$?; \
		done
