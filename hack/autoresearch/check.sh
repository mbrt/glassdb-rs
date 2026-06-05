#!/usr/bin/env bash
#
# Correctness gate for the autoresearch loop. This protects strict
# serializability: an experiment may only be kept if this script passes.
#
# This file is part of the autoresearch fixed infrastructure and must NOT be
# modified by autoresearch experiments.
#
# Usage:
#   hack/autoresearch/check.sh            # fast tier (run every experiment)
#   hack/autoresearch/check.sh --full     # full tier (before keeping a change)
#
# Tiers:
#   fast  build + `cargo test --workspace` (integration tests + the
#         `proptest_concurrent` serializability property test). No nightly
#         toolchain required.
#   full  `make test` (fmt + clippy -D warnings + tests) + `make sim-test`
#         (the madsim determinism / serializability / fault-injection
#         self-checks) + the deterministic concurrency fuzzer
#         (`FuzzConcurrentTx`'s Rust analog) for FULL_FUZZTIME seconds. Needs the
#         nightly toolchain and cargo-fuzz (`cargo install cargo-fuzz`).
#
# Tunables (env):
#   FUZZTIME       fuzz seconds per target in the full tier (default 30) when
#                  the fast tier opts into fuzzing via RUN_FAST_FUZZ=1
#   FULL_FUZZTIME  fuzz seconds per target in the full tier (default 120)
#   RUN_FAST_FUZZ  set to 1 to also run a short fuzz in the fast tier

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

FUZZTIME="${FUZZTIME:-30}"
FULL_FUZZTIME="${FULL_FUZZTIME:-120}"
RUN_FAST_FUZZ="${RUN_FAST_FUZZ:-0}"

mode="fast"
if [[ "${1:-}" == "--full" ]]; then
	mode="full"
fi

# run_fuzz SECONDS - run the deterministic concurrency fuzzer for the given
# budget. madsim must be enabled; cargo-fuzz overrides config.toml rustflags, so
# pass `--cfg madsim` through the environment (cargo-fuzz appends its own flags).
run_fuzz() {
	local secs="$1"
	echo "== serializability fuzz (concurrent_tx, ${secs}s) =="
	(
		cd fuzz
		RUSTFLAGS="--cfg madsim" cargo +nightly fuzz run concurrent_tx -- \
			-max_total_time="${secs}"
	)
}

echo "== build =="
cargo build --workspace

if [[ "$mode" == "full" ]]; then
	echo "== full test suite (make test) =="
	make test

	echo "== deterministic simulator (make sim-test) =="
	make sim-test

	run_fuzz "${FULL_FUZZTIME}"

	echo "== check OK (full) =="
	exit 0
fi

echo "== tests (cargo test --workspace) =="
cargo test --workspace

if [[ "${RUN_FAST_FUZZ}" == "1" ]]; then
	run_fuzz "${FUZZTIME}"
fi

echo "== check OK (fast) =="
