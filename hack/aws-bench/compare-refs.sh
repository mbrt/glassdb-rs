#!/usr/bin/env bash
#
# Compare GlassDB transaction performance between two engine versions (git
# refs) under the in-memory backend with simulated S3 latency and throttling.
#
# It builds `rtbench` + `autoresearch` from a base ref (default `main`, built in
# a reused detached git worktree) and from the target tree (the current worktree
# by default), runs the same workloads on both into `out-refs/`, and diffs them
# with `compare.py`. Throughput and latency are the primary axes; retries and
# backend round-trips per transaction (object-storage efficiency) are secondary.
#
# Because each ref compiles its own engine (the Backend trait differs across v1
# and v2), the two sides are built from separate source trees and reconciled
# through the CSV/JSON outputs. The cross-version run is only fully
# apples-to-apples once both refs carry the enhanced `rtbench` (e.g. after `main`
# is merged into the v2 branch); against an older target the driver falls back
# to the `balanced` mix only and `compare.py` degrades gracefully.
#
# Usage:
#   hack/aws-bench/compare-refs.sh            # main (v1) vs current worktree
#   BASE=main TARGET=s3-redesign hack/aws-bench/compare-refs.sh
#   DELAY_SCALE=0.02 DB_LIST=1,5 DURATION=5s NUM_RUNS=1 \
#     DEADLOCK_DURATION=3s COUNT=2 RW_MIX=balanced \
#     hack/aws-bench/compare-refs.sh        # quick smoke run
#   hack/aws-bench/compare-refs.sh --clean   # drop the base/target worktrees
#
# Tunables (env, with defaults):
#   BASE=main               base ref (the "v1" side), built in a worktree
#   TARGET=<current>        target ref (the "v2" side); empty = current worktree
#   LABEL_A=v1 LABEL_B=v2   labels for the base / target sides
#   DELAY_SCALE=0.05        compress simulated latency + rate limits (preserves
#                           the throttle shape); 1.0 = real time
#   DB_LIST=1,5,10,20,40    rw9010 concurrency points (number of Databases)
#   NUM_KEYS=5000           rw9010 key count
#   DURATION=15s            rw9010 duration per concurrency step
#   NUM_RUNS=2              repeat each sweep (tighter percentile bands)
#   DEADLOCK_DURATION=8s    deadlock duration per contention configuration
#   COUNT=5                 autoresearch suite repeats (reports the median)
#   RW_MIX="balanced readheavy writeheavy"   rw9010 mixes to run
#   OUT=<script dir>/out-refs                output root
#   BASE_WT, TARGET_WT      worktree paths (defaults are repo-parent siblings)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

BASE="${BASE:-main}"
TARGET="${TARGET:-}"
LABEL_A="${LABEL_A:-v1}"
LABEL_B="${LABEL_B:-v2}"
DELAY_SCALE="${DELAY_SCALE:-0.05}"
DB_LIST="${DB_LIST:-1,5,10,20,40}"
NUM_KEYS="${NUM_KEYS:-5000}"
DURATION="${DURATION:-15s}"
NUM_RUNS="${NUM_RUNS:-2}"
DEADLOCK_DURATION="${DEADLOCK_DURATION:-8s}"
COUNT="${COUNT:-5}"
RW_MIX="${RW_MIX:-balanced readheavy writeheavy}"
OUT="${OUT:-$SCRIPT_DIR/out-refs}"
BASE_WT="${BASE_WT:-$(dirname "$REPO_ROOT")/.glassdb-perf-base}"
TARGET_WT_DEFAULT="$(dirname "$REPO_ROOT")/.glassdb-perf-target"
TARGET_WT="${TARGET_WT:-$TARGET_WT_DEFAULT}"

log() { echo "[compare-refs] $*" >&2; }

# Add or refresh a detached worktree at $1 pinned to ref $2.
ensure_worktree() {
  local path="$1" ref="$2"
  if git -C "$path" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    log "refreshing worktree $path -> $ref"
    git -C "$path" checkout --detach "$ref" >/dev/null 2>&1
  else
    [ -e "$path" ] && rm -rf "$path"
    log "adding worktree $path -> $ref"
    git -C "$REPO_ROOT" worktree add --detach "$path" "$ref" >/dev/null
  fi
}

remove_worktree() {
  local path="$1"
  [ -e "$path" ] || return 0
  git -C "$REPO_ROOT" worktree remove --force "$path" 2>/dev/null || rm -rf "$path"
}

if [ "${1:-}" = "--clean" ]; then
  remove_worktree "$BASE_WT"
  remove_worktree "$TARGET_WT"
  git -C "$REPO_ROOT" worktree prune
  log "removed perf worktrees"
  exit 0
fi

build_bins() {
  local dir="$1"
  log "building rtbench + autoresearch in $dir (release)"
  (cd "$dir" && cargo build --release --bin rtbench --bin autoresearch >&2)
}

# Whether the rtbench binary at $1 understands --rw-mix (post-enhancement).
supports_rw_mix() {
  "$1/rtbench" --help 2>&1 | grep -q -- "--rw-mix"
}

# Run every workload for one side into $OUT/<group>/<label>/.
#   $1 = label (output subdir + report label)
#   $2 = bin dir (… /target/release)
#   $3 = whether this side supports --rw-mix (0/1)
run_side() {
  local label="$1" bindir="$2" has_mix="$3"
  local common=(--backend=memory --delays=s3 --delay-scale="$DELAY_SCALE")

  for mix in $MIXES; do
    local d="$OUT/$mix/$label"
    mkdir -p "$d"
    local mix_args=()
    if [ "$has_mix" = "1" ]; then
      mix_args=(--rw-mix="$mix")
    fi
    log "$label rw9010 mix=$mix"
    "$bindir/rtbench" "${common[@]}" \
      --test-name=rw9010 "${mix_args[@]}" \
      --db-list="$DB_LIST" --num-keys="$NUM_KEYS" \
      --duration="$DURATION" --num-runs="$NUM_RUNS" \
      --samples-out="$d/samples.csv" --stats-out="$d/stats.csv" \
      --throughput-out="$d/throughput.csv" --client-stats-out="$d/client-stats.csv" >&2
  done

  local dd="$OUT/contention/$label"
  mkdir -p "$dd"
  log "$label deadlock"
  "$bindir/rtbench" "${common[@]}" \
    --test-name=deadlock --duration="$DEADLOCK_DURATION" --num-runs="$NUM_RUNS" \
    --deadlock-out="$dd/deadlock.csv" >&2

  local de="$OUT/efficiency/$label"
  mkdir -p "$de"
  log "$label autoresearch (--count $COUNT)"
  "$bindir/autoresearch" --json --count "$COUNT" >"$de/score.json"
}

# --- Build both sides ------------------------------------------------------

ensure_worktree "$BASE_WT" "$BASE"
build_bins "$BASE_WT"
BASE_BIN="$BASE_WT/target/release"

if [ -n "$TARGET" ]; then
  ensure_worktree "$TARGET_WT" "$TARGET"
  build_bins "$TARGET_WT"
  TARGET_BIN="$TARGET_WT/target/release"
  TARGET_DESC="$TARGET"
else
  build_bins "$REPO_ROOT"
  TARGET_BIN="$REPO_ROOT/target/release"
  TARGET_DESC="current worktree"
fi

# Determine the mix set: every requested mix only when both binaries support
# --rw-mix, else fall back to the default balanced mix (run flagless).
A_MIX=0; B_MIX=0
supports_rw_mix "$BASE_BIN" && A_MIX=1
supports_rw_mix "$TARGET_BIN" && B_MIX=1
if [ "$A_MIX" = "1" ] && [ "$B_MIX" = "1" ]; then
  MIXES="$RW_MIX"
else
  log "WARNING: a side lacks --rw-mix (base=$A_MIX target=$B_MIX); running balanced only"
  MIXES="balanced"
fi

log "BASE=$BASE ($LABEL_A) vs TARGET=$TARGET_DESC ($LABEL_B); mixes: $MIXES"
rm -rf "$OUT"

# --- Run both sides back-to-back -------------------------------------------

run_side "$LABEL_A" "$BASE_BIN" "$A_MIX"
run_side "$LABEL_B" "$TARGET_BIN" "$B_MIX"

# --- Compare ---------------------------------------------------------------

for mix in $MIXES; do
  uv run "$SCRIPT_DIR/compare.py" \
    --a "$OUT/$mix/$LABEL_A" --b "$OUT/$mix/$LABEL_B" \
    --label-a "$LABEL_A" --label-b "$LABEL_B" --title "rw9010/$mix"
done

uv run "$SCRIPT_DIR/compare.py" \
  --a "$OUT/contention/$LABEL_A" --b "$OUT/contention/$LABEL_B" \
  --label-a "$LABEL_A" --label-b "$LABEL_B" --title "deadlock"

uv run "$SCRIPT_DIR/compare.py" \
  --a "$OUT/efficiency/$LABEL_A" --b "$OUT/efficiency/$LABEL_B" \
  --label-a "$LABEL_A" --label-b "$LABEL_B" --title "efficiency" --no-plots

log "done. CSVs + overlay PNGs under $OUT/"
