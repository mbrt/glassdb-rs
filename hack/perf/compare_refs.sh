#!/usr/bin/env bash
# The local entry point uses the same bounded comparison as performance CI.

set -euo pipefail

cd "$(git rev-parse --show-toplevel)"
case "${1:-}" in
  ""|--summary) ;;
  *) echo "usage: BASE=main TARGET=ref OUT=new-directory $0 [--summary]" >&2; exit 2 ;;
esac
args=(--base "${BASE:-main}" --output "${OUT:-target/performance/results/$(date -u +%Y%m%dT%H%M%SZ)}")
if [ -n "${TARGET:-}" ]; then
  args+=(--candidate "$TARGET")
fi
exec python3 hack/ci/perf_compare.py "${args[@]}"
