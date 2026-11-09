#!/usr/bin/env bash
# Compares two revisions with the same steps as performance CI: build both
# binaries with the harness of the candidate, run `make bench` with each, and
# report the changes.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
case "${1:-}" in
  ""|--summary) ;;
  *) echo "usage: BASE=main TARGET=ref OUT=new-directory $0 [--summary]" >&2; exit 2 ;;
esac
out="${OUT:-target/performance/results/$(date -u +%Y%m%dT%H%M%SZ)}"
args=(--base "${BASE:-main}" --output "$out")
if [ -n "${TARGET:-}" ]; then
  args+=(--candidate "$TARGET")
fi
python3 hack/ci/perf_compare.py "${args[@]}"
manifest="$out/manifest.json"
# Prints the value of the manifest at the keys of the arguments.
field() {
  python3 - "$manifest" "$@" <<'PY'
import json, sys
value = json.load(open(sys.argv[1]))
for key in sys.argv[2:]:
    value = value[key]
print(value)
PY
}
# As in CI, a side that fails still gets a report, with a warning.
warnings=()
for side in main pr; do
  if ! make bench PERFBENCH="$(field "$side" perfbench)" \
    EXPERIMENTS_DIR="$(field experiments)" OUT="$out/$side"; then
    warnings+=("--warning=The measurement of $side failed; see its output above.")
    break
  fi
done
# The form of the empty array works on bash before 4.4, as on macOS.
python3 hack/ci/perf_report.py --manifest "$manifest" ${warnings[@]+"${warnings[@]}"} \
  --base "$out/main" --candidate "$out/pr" --output "$out/report.md"
cat "$out/report.md"
[ "${#warnings[@]}" -eq 0 ]
