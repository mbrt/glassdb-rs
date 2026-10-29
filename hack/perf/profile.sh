#!/usr/bin/env bash
#
# CPU profiling recipe for glassdb-rs. The profiler attaches to the compiled
# perfbench binary to measure CPU use. See hack/perf/README.md for the limits of
# profiling the in-memory backend.
#
# Usage:
#   hack/perf/profile.sh
#   CASE=rmw_inline_1024 hack/perf/profile.sh
#   EXPERIMENT=hack/perf/experiments/real-time/mixed.toml hack/perf/profile.sh
#
# Tunables (env):
#   EXPERIMENT        perfbench experiment file or directory
#                     (default hack/perf/experiments/default)
#   CASE              one experiment of hack/perf/experiments/default, in place
#                     of EXPERIMENT (default: all)
#   SECONDS_PER_CASE  measured wall time of each default experiment (default 10)
#   OUT               output directory for artifacts (default hack/perf)

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

DEFAULT=hack/perf/experiments/default
EXPERIMENT="${EXPERIMENT:-$DEFAULT}"
CASE="${CASE:-}"
if [[ -n "$CASE" ]]; then
	EXPERIMENT="$DEFAULT/$CASE.toml"
fi
SECONDS_PER_CASE="${SECONDS_PER_CASE:-10}"
OUT="${OUT:-hack/perf}"

mkdir -p "$OUT"
svg="$OUT/flamegraph.svg"
folded="$OUT/flamegraph.folded"

cargo_sel=(-p glassdb-bench-scale --bin perfbench)
run_args=(
	--latency-jitter=false
	--experiment="$EXPERIMENT"
	--output="$OUT/profile.json"
)
if [[ "$(realpath -m "$EXPERIMENT")" == "$(realpath -m "$DEFAULT")"* ]]; then
	# perfbench durations are model time, and the default model clock runs five
	# times as fast as wall time.
	model_seconds=$((SECONDS_PER_CASE * 5))
	run_args+=(
		--set="cell.measure.duration=\"${model_seconds}s\""
		--set="cell.measure.max-duration=\"${model_seconds}s\""
	)
fi

if ! cargo flamegraph --version >/dev/null 2>&1; then
	echo "cargo-flamegraph not found; install it with:" >&2
	echo "  cargo install flamegraph" >&2
	exit 127
fi

# cargo-flamegraph runs the target under perf and renders an SVG. It leaves the
# raw perf.data in the repo root, which we collapse into a searchable folded-stack file when
# the collapse tool is available.
if ! cargo flamegraph --profile profiling "${cargo_sel[@]}" \
	--output "$svg" -- "${run_args[@]}"; then
	cat >&2 <<'MSG'

profiling failed. On Linux, perf-based profiling needs kernel access:
  sudo sysctl kernel.perf_event_paranoid=1
  sudo sysctl kernel.kptr_restrict=0      # if stacks show only addresses
MSG
	exit 1
fi

echo "wrote $svg"
if [ -f perf.data ] && command -v inferno-collapse-perf >/dev/null 2>&1; then
	perf script -i perf.data | inferno-collapse-perf >"$folded"
	echo "wrote $folded"
else
	echo "note: install inferno (cargo install inferno) for a greppable $folded" >&2
fi
