#!/usr/bin/env bash
#
# CPU profiling recipe for glassdb-rs. The profiler attaches to the compiled
# perfbench binary to measure CPU use. See hack/perf/README.md for the limits of
# profiling the in-memory backend.
#
# Usage:
#   hack/perf/profile.sh
#   CASE=rmw-inline hack/perf/profile.sh
#   EXPERIMENT=hack/perf/experiments/mixed.toml hack/perf/profile.sh
#
# Tunables (env):
#   EXPERIMENT        perfbench experiment file or directory (default: the
#                     experiments of hack/perf/experiments with the tag
#                     `ci/timing`)
#   CASE              one experiment of hack/perf/experiments with the tag
#                     `ci/timing`, in place of EXPERIMENT (default: all)
#   SECONDS_PER_CASE  measured wall time of each experiment with the tag
#                     `ci/timing` (default 10)
#   OUT               output directory for artifacts (default hack/perf)

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

EXPERIMENTS=hack/perf/experiments
EXPERIMENT="${EXPERIMENT:-}"
CASE="${CASE:-}"
# The CI experiments have a fixed length, which SECONDS_PER_CASE replaces.
ci=false
if [[ -n "$CASE" ]]; then
	selection=(--experiment="$EXPERIMENTS/$CASE.toml" --tag=ci/timing)
	ci=true
elif [[ -n "$EXPERIMENT" ]]; then
	selection=(--experiment="$EXPERIMENT")
else
	selection=(--experiment="$EXPERIMENTS" --tag=ci/timing)
	ci=true
fi
SECONDS_PER_CASE="${SECONDS_PER_CASE:-10}"
OUT="${OUT:-hack/perf}"

mkdir -p "$OUT"
svg="$OUT/flamegraph.svg"
folded="$OUT/flamegraph.folded"

cargo_sel=(-p glassdb-bench-scale --bin perfbench)
run_args=(
	--latency-jitter=false
	"${selection[@]}"
	--output="$OUT/profile.json"
)
if [[ "$ci" == true ]]; then
	# perfbench durations are model time, which is wall time in real time.
	run_args+=(
		--set="cell.measure.duration=\"${SECONDS_PER_CASE}s\""
		--set="cell.measure.max-duration=\"${SECONDS_PER_CASE}s\""
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
