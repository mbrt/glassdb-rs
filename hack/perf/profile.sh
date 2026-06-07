#!/usr/bin/env bash
#
# CPU profiling recipe for glassdb-rs. This is a diagnostic aid only: it is NOT
# part of any test, the benchmark metric, or the autoresearch correctness gate,
# and it never edits source. The profiler attaches to the compiled binary, so it
# can target even the frozen benchmark crates without touching them. See
# hack/perf/README.md for the full story (including the in-memory caveat when
# profiling the `autoresearch` target).
#
# Usage:
#   hack/perf/profile.sh                 # flamegraph of the autoresearch harness
#   TARGET=bench hack/perf/profile.sh    # flamegraph of the transactions bench
#
# Tunables (env):
#   TARGET    autoresearch | bench   what to profile (default autoresearch)
#   COUNT     suite repeats fed to the autoresearch harness (default 50); more
#             repeats => more samples => a less noisy profile
#   OUT       output directory for artifacts (default hack/perf)

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

TARGET="${TARGET:-autoresearch}"
COUNT="${COUNT:-50}"
OUT="${OUT:-hack/perf}"

mkdir -p "$OUT"
svg="$OUT/flamegraph.svg"
folded="$OUT/flamegraph.folded"

# Cargo selector + the args passed to the profiled binary, per target. The
# `transactions` bench wraps the in-memory backend in a latency-injecting
# DelayBackend, so its profile is closer to the real round-trip-bound cost than
# the in-memory `autoresearch` harness (see README).
case "$TARGET" in
autoresearch)
	cargo_sel=(-p glassdb-bench-score --bin autoresearch)
	run_args=(--count "$COUNT")
	;;
bench)
	cargo_sel=(-p glassdb --bench transactions)
	run_args=(--bench)
	;;
*)
	echo "unknown TARGET '$TARGET' (use 'autoresearch' or 'bench')" >&2
	exit 2
	;;
esac

if ! cargo flamegraph --version >/dev/null 2>&1; then
	echo "cargo-flamegraph not found; install it with:" >&2
	echo "  cargo install flamegraph" >&2
	exit 127
fi

# cargo-flamegraph runs the target under perf and renders an SVG. It leaves the
# raw perf.data in the repo root, which we fold into a greppable text file when
# the collapse tool is available.
if ! cargo flamegraph --profile profiling "${cargo_sel[@]}" \
	--output "$svg" -- "${run_args[@]}"; then
	cat >&2 <<'EOF'

profiling failed. On Linux, perf-based profiling needs kernel access:
  sudo sysctl kernel.perf_event_paranoid=1
  sudo sysctl kernel.kptr_restrict=0      # if stacks show only addresses
EOF
	exit 1
fi

echo "wrote $svg"
if [ -f perf.data ] && command -v inferno-collapse-perf >/dev/null 2>&1; then
	perf script -i perf.data | inferno-collapse-perf >"$folded"
	echo "wrote $folded"
else
	echo "note: install inferno (cargo install inferno) for a greppable $folded" >&2
fi
