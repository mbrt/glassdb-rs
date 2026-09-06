#!/usr/bin/env bash
#
# CPU profiling recipe for glassdb-rs. The profiler attaches to the compiled
# binary to measure CPU use. See hack/perf/README.md for the limits of profiling
# the in-memory Criterion target.
#
# Usage:
#   hack/perf/profile.sh
#   FILTER=diagnostic/rmw_inline_1024 hack/perf/profile.sh
#
# Tunables (env):
#   FILTER    Criterion case filter (default diagnostic)
#   SECONDS_PER_CASE  profiling time per case (default 10)
#   OUT       output directory for artifacts (default hack/perf)

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

FILTER="${FILTER:-diagnostic}"
SECONDS_PER_CASE="${SECONDS_PER_CASE:-10}"
OUT="${OUT:-hack/perf}"

mkdir -p "$OUT"
svg="$OUT/flamegraph.svg"
folded="$OUT/flamegraph.folded"

cargo_sel=(-p glassdb --bench diagnostics)
run_args=(--bench "$FILTER" --profile-time "$SECONDS_PER_CASE")

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
