#!/usr/bin/env bash
#
# Compare GlassDB transaction performance between two engine versions (git
# refs) under the in-memory backend with simulated S3 latency and throttling.
#
# It builds `perfbench` + `autoresearch` from current refs, falling back to the
# retired `rtbench`/`mixbench` binaries when comparing historical refs. The base
# ref (default `main`, built in a reused detached git worktree) and from the
# target tree (the current worktree by default), interleaves paired workload
# repetitions into `out-refs/`, and diffs them with `compare.py`. Throughput and
# latency are the primary axes; retries and backend round-trips per transaction
# (object-storage efficiency) are secondary. `perfbench mixed` runs transaction
# shapes across contention mode x home-collection affinity after setup splits
# have converged. `perfbench inline-pressure` checks that inline saturation
# drives structural capacity and restores direct commits.
#
# Because each ref compiles its own engine (the Backend trait differs across v1
# and v2), the two sides are built from separate source trees and reconciled
# through the CSV/JSON outputs. The cross-version run is only fully
# apples-to-apples once both refs carry the affinity-aware mixed scenario. A ref
# that predates it skips that section, while a supported binary must complete
# successfully.
#
# Each run leaves a small, trackable digest at $OUT/summary.md (the per-section
# ratio summaries plus the deterministic autoresearch score). It is the only
# out-refs artifact that is not gitignored, so it can be committed to follow the
# numbers over time. The worktrees built for the base/target refs are removed at
# the end of every run (same as `--clean`).
#
# `--summary` runs every section that feeds summary.md (contention, inline
# pressure, mixed workload, efficiency) with smaller windows and no plots. The
# mixed scenario self-terminates at a looser target CI. Explicit env tunables
# still override the fast defaults.
#
# Usage:
#   hack/aws-bench/compare-refs.sh            # main (v1) vs current worktree
#   BASE=main TARGET=s3-redesign hack/aws-bench/compare-refs.sh
#   hack/aws-bench/compare-refs.sh --summary  # fast full-summary run (no plots)
#   hack/aws-bench/compare-refs.sh --clean    # drop the base/target worktrees
#
# Tunables (env). Defaults marked "full / summary" differ between the full sweep
# and `--summary`; an explicit env var overrides both.
#   BASE=main               base ref (the "v1" side), built in a worktree
#   TARGET=<current>        target ref (the "v2" side); empty = current worktree
#   LABEL_A=v1 LABEL_B=v2   labels for the base / target sides
#   DELAY_SCALE=0.2         compress process-wide model time for simulated
#                           backends; 1.0 = real time
#   NUM_RUNS=1 / 2          paired contention/inline repetitions (order alternates)
#   CONTENTION_DURATION=8s / 3s duration per contention configuration
#   INLINE_PROFILES=s3,gcs  latency profiles for the inline-pressure scenario
#   INLINE_SETTLE_TIMEOUT=5s / 3s  maximum wait for each demanded split
#   COUNT=5 / 3             autoresearch suite repeats (reports the median)
#   MIX_DURATION=2s / 1s    mixed minimum measured window per cell
#   MIX_MAX_DURATION=60s / 20s  mixed per-cell time cap (upper bound)
#   MIX_TARGET_CI=0.1 / 0.2 mixed target throughput 95% CI half-width; the
#                           cell runs until every shape reaches it or the cap
#   MIX_MODES=lo,hi         mixed contention modes to sweep
#   MIX_AFFINITIES=0,25,50,75,100 home-collection affinity percentages
#   MIX_WORKERS=8           mixed workers per shape
#   MIX_DATABASES=4         mixed client Databases and home collections
#   MIX_NUM_KEYS=5000       mixed lo-mode key pool per collection
#   MIX_HOT_KEYS=8          mixed hi-mode hot-key pool
#   MIX_MULTI_KEYS=10       mixed keys per multi-key shape
#   MIX_SPLIT_QUIET=10s     unchanged completed-split interval before measuring
#   MIX_SPLIT_SETTLE_TIMEOUT=60s setup split convergence deadline
#   DRAIN_TIMEOUT=90s / 30s per-cell completion grace for benchmark binaries
#                           that support --drain-timeout
#   COMMAND_TIMEOUT=15m     hard watchdog for each workload command, including
#                           historical binaries without per-cell deadlines
#   OUT=<script dir>/out-refs                output root
#   BASE_WT, TARGET_WT      worktree paths (defaults are repo-parent siblings)
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(git -C "$SCRIPT_DIR" rev-parse --show-toplevel)"

# Parse the mode flag before defaults so `--summary` can pick fast defaults.
SUMMARY=0
DO_CLEAN=0
case "${1:-}" in
  --clean) DO_CLEAN=1 ;;
  --summary) SUMMARY=1 ;;
  "") ;;
  *)
    echo "[compare-refs] unknown argument: $1 (expected --clean or --summary)" >&2
    exit 2
    ;;
esac

BASE="${BASE:-main}"
TARGET="${TARGET:-}"
LABEL_A="${LABEL_A:-v1}"
LABEL_B="${LABEL_B:-v2}"

# Workload sizing. `--summary` swaps in much smaller defaults for the knobs that
# dominate wall time (duration, concurrency points, repeats); everything still
# runs, so every summary.md section is produced.
if [ "$SUMMARY" = "1" ]; then
  DELAY_SCALE="${DELAY_SCALE:-0.2}"
  CONTENTION_DURATION="${CONTENTION_DURATION:-${DEADLOCK_DURATION:-3s}}"
  INLINE_SETTLE_TIMEOUT="${INLINE_SETTLE_TIMEOUT:-3s}"
  # Mixbench self-terminates at its target CI; a looser CI and shorter cap keep
  # the fast path useful without pretending one short fixed window is precise.
  COUNT="${COUNT:-3}"
  NUM_RUNS="${NUM_RUNS:-2}"
  MIX_DURATION="${MIX_DURATION:-1s}"
  MIX_MAX_DURATION="${MIX_MAX_DURATION:-20s}"
  MIX_TARGET_CI="${MIX_TARGET_CI:-0.2}"
  DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-30s}"
else
  DELAY_SCALE="${DELAY_SCALE:-0.2}"
  CONTENTION_DURATION="${CONTENTION_DURATION:-${DEADLOCK_DURATION:-8s}}"
  INLINE_SETTLE_TIMEOUT="${INLINE_SETTLE_TIMEOUT:-5s}"
  COUNT="${COUNT:-5}"
  NUM_RUNS="${NUM_RUNS:-1}"
  MIX_DURATION="${MIX_DURATION:-2s}"
  MIX_MAX_DURATION="${MIX_MAX_DURATION:-60s}"
  MIX_TARGET_CI="${MIX_TARGET_CI:-0.1}"
  DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-90s}"
fi
INLINE_PROFILES="${INLINE_PROFILES:-s3,gcs}"
# Mixed-scenario tunables. Skipped automatically for a ref that predates the affinity
# schema, because comparing different workload layouts would be meaningless.
MIX_MODES="${MIX_MODES:-lo,hi}"
MIX_AFFINITIES="${MIX_AFFINITIES:-0,25,50,75,100}"
MIX_WORKERS="${MIX_WORKERS:-8}"
MIX_DATABASES="${MIX_DATABASES:-4}"
MIX_NUM_KEYS="${MIX_NUM_KEYS:-5000}"
MIX_HOT_KEYS="${MIX_HOT_KEYS:-8}"
MIX_MULTI_KEYS="${MIX_MULTI_KEYS:-10}"
MIX_SPLIT_QUIET="${MIX_SPLIT_QUIET:-10s}"
MIX_SPLIT_SETTLE_TIMEOUT="${MIX_SPLIT_SETTLE_TIMEOUT:-60s}"
COMMAND_TIMEOUT="${COMMAND_TIMEOUT:-15m}"
OUT="${OUT:-$SCRIPT_DIR/out-refs}"
BASE_WT="${BASE_WT:-$(dirname "$REPO_ROOT")/.glassdb-perf-base}"
TARGET_WT_DEFAULT="$(dirname "$REPO_ROOT")/.glassdb-perf-target"
TARGET_WT="${TARGET_WT:-$TARGET_WT_DEFAULT}"

# summary.md never embeds the overlay PNGs, so skip them in --summary mode (the
# mixed/efficiency comparisons already skip plots regardless).
PLOT_ARGS=()
[ "$SUMMARY" = "1" ] && PLOT_ARGS=(--no-plots)

log() { echo "[compare-refs] $*" >&2; }

run_bounded() {
  timeout --foreground --kill-after=10s "$COMMAND_TIMEOUT" "$@"
}

csv_items() {
  local value="$1" items
  IFS=',' read -r -a items <<<"$value"
  echo "${#items[@]}"
}

validate_csv_rows() {
  local path="$1" expected="$2"
  awk -v expected="$expected" '
    NR > 1 { rows++ }
    END {
      if (rows != expected) {
        printf("%s: expected %d rows, found %d\n", FILENAME, expected, rows) > "/dev/stderr"
        exit 1
      }
    }
  ' "$path"
}

validate_csv_rows_one_of() {
  local path="$1" expected_a="$2" expected_b="$3" rows
  rows="$(awk 'NR > 1 { rows++ } END { print rows + 0 }' "$path")"
  if [ "$rows" -ne "$expected_a" ] && [ "$rows" -ne "$expected_b" ]; then
    printf '%s: expected %d or %d rows, found %d\n' \
      "$path" "$expected_a" "$expected_b" "$rows" >&2
    return 1
  fi
}

validate_mixed_results() {
  local path="$1" expected="$2" cells affinities settlements
  cells="$(grep -c '"mode"' "$path" || true)"
  affinities="$(grep -c '"affinityPct"' "$path" || true)"
  settlements="$(grep -c '"splitSettleWallMs"' "$path" || true)"
  if [ "$cells" -ne "$expected" ]; then
    log "ERROR: $path expected $expected cells, found $cells"
    return 1
  fi
  if [ "$affinities" -ne "$expected" ] || [ "$settlements" -ne "$expected" ]; then
    log "ERROR: $path is missing affinity or split-settlement data"
    return 1
  fi
  if grep -Eq '"failures"[[:space:]]*:[[:space:]]*[1-9]' "$path"; then
    log "ERROR: $path contains transaction failures"
    return 1
  fi
}

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

clean_worktrees() {
  remove_worktree "$BASE_WT"
  remove_worktree "$TARGET_WT"
  git -C "$REPO_ROOT" worktree prune
  log "removed perf worktrees"
}

if [ "$DO_CLEAN" = "1" ]; then
  clean_worktrees
  exit 0
fi

if ! command -v timeout >/dev/null 2>&1; then
  log "ERROR: GNU timeout is required for bounded benchmark runs"
  exit 2
fi

build_bins() {
  local dir="$1" perf_source mix_source rt_source
  log "building performance binaries in $dir (release)"
  (cd "$dir" && cargo build --release --bin autoresearch >&2)
  perf_source="$dir/crates/glassdb-bench-scale/src/bin/perfbench/main.rs"
  mix_source="$dir/crates/glassdb-bench-scale/src/bin/mixbench.rs"
  rt_source="$dir/crates/glassdb-bench-scale/src/bin/rtbench/main.rs"
  if [ -f "$perf_source" ]; then
    (cd "$dir" && cargo build --release --bin perfbench >&2)
    rm -f "$dir/target/release/mixbench" "$dir/target/release/rtbench"
    log "built perfbench in $dir"
    return
  fi
  rm -f "$dir/target/release/perfbench"
  if [ -f "$rt_source" ]; then
    (cd "$dir" && cargo build --release --bin rtbench >&2)
  else
    rm -f "$dir/target/release/rtbench"
  fi
  if [ -f "$mix_source" ]; then
    (cd "$dir" && cargo build --release --bin mixbench >&2)
    log "built mixbench in $dir"
  else
    rm -f "$dir/target/release/mixbench"
    log "NOTE: no mixbench binary in $dir (older ref); its mixbench section is skipped"
  fi
}

uses_perfbench() {
  [ -x "$1/perfbench" ]
}

supports_process_model_time() {
  uses_perfbench "$1" \
    && "$1/perfbench" --help 2>&1 | grep -q -- "process-wide model time"
}

supports_drain_timeout() {
  "$1" --help 2>&1 | grep -q -- "--drain-timeout"
}

supports_deadlock_stats() {
  "$1" --help 2>&1 | grep -q -- "--deadlock-stats-out"
}

supports_inline_pressure() {
  "$1" --help 2>&1 | grep -q -- "--inline-pressure-out"
}

supports_mix_affinity() {
  local bindir="$1"
  if uses_perfbench "$bindir"; then
    "$bindir/perfbench" mixed --help 2>&1 | grep -q -- "--affinities"
  else
    "$bindir/mixbench" --help 2>&1 | grep -q -- "--affinities"
  fi
}

# rtbench before e2171c3c applied --delay-scale to the simulated backend but
# reported compressed wall time. Current binaries undo that compression in
# every reported latency and throughput metric.
rtbench_time_factor() {
  local source_root="$1" main
  if [ -f "$source_root/crates/glassdb-bench-scale/src/bin/perfbench/main.rs" ]; then
    printf '1\n'
    return
  fi
  main="$source_root/crates/glassdb-bench-scale/src/bin/rtbench/main.rs"
  if grep -q 'fn report_time_scale' "$main"; then
    printf '1\n'
  else
    awk -v scale="$DELAY_SCALE" 'BEGIN {
      if (scale <= 0) exit 1
      printf "%.17g\n", 1 / scale
    }'
  fi
}

append_csv_with_run() {
  local src="$1" dst="$2" run="$3" header
  header="$(head -n 1 "$src")"
  mkdir -p "$(dirname "$dst")"
  if [[ "$header" == run,* ]]; then
    [ -f "$dst" ] || printf '%s\n' "$header" >"$dst"
    awk -F, -v OFS=, -v run="$run" 'NR > 1 {$1 = run; print}' "$src" >>"$dst"
  else
    [ -f "$dst" ] || printf 'run,%s\n' "$header" >"$dst"
    awk -v run="$run" 'NR > 1 {print run "," $0}' "$src" >>"$dst"
  fi
}

validate_deadlock_results() {
  local path="$1" expected="$2"
  awk -F, -v expected="$expected" '
    NR == 1 {
      for (i = 1; i <= NF; i++) {
        if ($i == "num-keys") keys_col = i
        if ($i == "overlap") overlap_col = i
      }
      next
    }
    {
      key = $keys_col SUBSEP $overlap_col
      if (!seen[key]++) cells++
    }
    END {
      if (!keys_col || !overlap_col) {
        printf("%s: missing deadlock identity columns\n", FILENAME) > "/dev/stderr"
        bad = 1
      }
      if (cells != expected) {
        printf("%s: expected %d deadlock cells, found %d\n", FILENAME, expected, cells) > "/dev/stderr"
        bad = 1
      }
      exit bad
    }
  ' "$path"
}

validate_perfbench_cells() {
  local path="$1" scenario="$2" field="$3" expected="$4" cells
  grep -q '"schemaVersion"[[:space:]]*:[[:space:]]*1' "$path"
  grep -q "\"scenario\"[[:space:]]*:[[:space:]]*\"$scenario\"" "$path"
  cells="$(grep -c "\"$field\"" "$path" || true)"
  if [ "$cells" -ne "$expected" ]; then
    log "ERROR: $path expected $expected $scenario cells, found $cells"
    return 1
  fi
  if grep -Eq '"failures"[[:space:]]*:[[:space:]]*[1-9]' "$path"; then
    log "ERROR: $path contains transaction failures"
    return 1
  fi
}

run_contention_once() {
  local label="$1" bindir="$2" has_drain="$3" repetition="$4"
  local common=(--backend=memory --delays=s3 --delay-scale="$DELAY_SCALE")
  local drain_args=() stats_args=()
  [ "$has_drain" = "1" ] && drain_args=(--drain-timeout="$DRAIN_TIMEOUT")
  local d="$OUT/contention/$label" raw="$OUT/contention/$label/runs/$repetition"
  mkdir -p "$raw"
  if uses_perfbench "$bindir"; then
    log "$label contention paired-run=$repetition/$NUM_RUNS"
    run_bounded "$bindir/perfbench" "${common[@]}" \
      --drain-timeout="$DRAIN_TIMEOUT" --runs=1 \
      --output="$raw/contention.json" contention \
      --duration="$CONTENTION_DURATION" >&2
    validate_perfbench_cells "$raw/contention.json" contention numKeys 21
    return
  fi
  supports_deadlock_stats "$bindir/rtbench" \
    && stats_args=(--deadlock-stats-out="$raw/deadlock-stats.csv")
  log "$label contention via legacy rtbench paired-run=$repetition/$NUM_RUNS"
  run_bounded "$bindir/rtbench" "${common[@]}" "${drain_args[@]}" \
    "${stats_args[@]}" --test-name=deadlock --duration="$CONTENTION_DURATION" \
    --num-runs=1 --deadlock-out="$raw/deadlock.csv" >&2
  validate_deadlock_results "$raw/deadlock.csv" 21
  append_csv_with_run "$raw/deadlock.csv" "$d/deadlock.csv" "$repetition"
  if [ -f "$raw/deadlock-stats.csv" ]; then
    validate_csv_rows "$raw/deadlock-stats.csv" 21
    append_csv_with_run \
      "$raw/deadlock-stats.csv" "$d/deadlock-stats.csv" "$repetition"
  fi
}

run_inline_pressure_once() {
  local label="$1" bindir="$2" has_drain="$3" profile="$4" repetition="$5"
  local drain_args=()
  [ "$has_drain" = "1" ] && drain_args=(--drain-timeout="$DRAIN_TIMEOUT")
  local d="$OUT/inline-pressure/$profile/$label"
  local raw="$OUT/inline-pressure/$profile/$label/runs/$repetition"
  mkdir -p "$raw"
  log "$label inline-pressure profile=$profile paired-run=$repetition/$NUM_RUNS"
  if uses_perfbench "$bindir"; then
    run_bounded "$bindir/perfbench" \
      --backend=memory --delays="$profile" --delay-scale="$DELAY_SCALE" \
      --drain-timeout="$DRAIN_TIMEOUT" --runs=1 \
      --output="$raw/inline-pressure.json" inline-pressure \
      --settle-timeout="$INLINE_SETTLE_TIMEOUT" >&2
    validate_perfbench_cells "$raw/inline-pressure.json" inline-pressure phase 7
    return
  fi
  run_bounded "$bindir/rtbench" \
    --backend=memory --delays="$profile" --delay-scale="$DELAY_SCALE" \
    "${drain_args[@]}" --test-name=inline-pressure --num-runs=1 \
    --inline-pressure-settle-timeout="$INLINE_SETTLE_TIMEOUT" \
    --inline-pressure-out="$raw/inline-pressure.csv" >&2
  validate_csv_rows_one_of "$raw/inline-pressure.csv" 7 8
  append_csv_with_run \
    "$raw/inline-pressure.csv" "$d/inline-pressure.csv" "$repetition"
}

run_aux_side() {
  local label="$1" bindir="$2" run_mix="$3"
  # Only affinity-aware binaries run: an older topology grid is a different
  # workload and cannot be compared honestly.
  if [ "$run_mix" = "1" ]; then
    local dm="$OUT/mixed/$label" result
    mkdir -p "$dm"
    log "$label mixed"
    if uses_perfbench "$bindir"; then
      result="$dm/mixed.json"
      run_bounded "$bindir/perfbench" \
        --backend=memory --delays=s3 --delay-scale="$DELAY_SCALE" \
        --drain-timeout="$DRAIN_TIMEOUT" --runs=1 \
        --output="$result" mixed \
        --duration="$MIX_DURATION" --max-duration="$MIX_MAX_DURATION" \
        --target-ci="$MIX_TARGET_CI" --modes="$MIX_MODES" \
        --affinities="$MIX_AFFINITIES" --workers-per-shape="$MIX_WORKERS" \
        --databases="$MIX_DATABASES" --num-keys="$MIX_NUM_KEYS" \
        --hot-keys="$MIX_HOT_KEYS" --multi-keys="$MIX_MULTI_KEYS" \
        --split-quiet="$MIX_SPLIT_QUIET" \
        --split-settle-timeout="$MIX_SPLIT_SETTLE_TIMEOUT" >&2
    else
      result="$dm/mixbench.json"
      local mix_drain_args=()
      supports_drain_timeout "$bindir/mixbench" \
        && mix_drain_args=(--drain-timeout="$DRAIN_TIMEOUT")
      run_bounded "$bindir/mixbench" --delays=s3 --delay-scale="$DELAY_SCALE" \
        "${mix_drain_args[@]}" \
        --duration="$MIX_DURATION" --max-duration="$MIX_MAX_DURATION" \
        --target-ci="$MIX_TARGET_CI" --modes="$MIX_MODES" \
        --affinities="$MIX_AFFINITIES" --workers-per-shape="$MIX_WORKERS" \
        --databases="$MIX_DATABASES" --num-keys="$MIX_NUM_KEYS" \
        --hot-keys="$MIX_HOT_KEYS" --multi-keys="$MIX_MULTI_KEYS" \
        --split-quiet="$MIX_SPLIT_QUIET" \
        --split-settle-timeout="$MIX_SPLIT_SETTLE_TIMEOUT" \
        --json >"$result"
    fi
    local expected_mix
    expected_mix=$(( $(csv_items "$MIX_MODES") * $(csv_items "$MIX_AFFINITIES") ))
    validate_mixed_results "$result" "$expected_mix"
  else
    log "$label mixed skipped"
  fi

  local de="$OUT/efficiency/$label"
  mkdir -p "$de"
  log "$label autoresearch (--count $COUNT)"
  run_bounded "$bindir/autoresearch" --json --count "$COUNT" >"$de/score.json"
}

# --- Build both sides ------------------------------------------------------

ensure_worktree "$BASE_WT" "$BASE"
build_bins "$BASE_WT"
BASE_BIN="$BASE_WT/target/release"
BASE_TIME_FACTOR="$(rtbench_time_factor "$BASE_WT")"

if [ -n "$TARGET" ]; then
  ensure_worktree "$TARGET_WT" "$TARGET"
  build_bins "$TARGET_WT"
  TARGET_BIN="$TARGET_WT/target/release"
  TARGET_DESC="$TARGET"
  TARGET_SOURCE_ROOT="$TARGET_WT"
else
  build_bins "$REPO_ROOT"
  TARGET_BIN="$REPO_ROOT/target/release"
  TARGET_DESC="current worktree"
  TARGET_SOURCE_ROOT="$REPO_ROOT"
fi
TARGET_TIME_FACTOR="$(rtbench_time_factor "$TARGET_SOURCE_ROOT")"
TIME_FACTOR_ARGS=(
  --rtbench-time-factor-a "$BASE_TIME_FACTOR"
  --rtbench-time-factor-b "$TARGET_TIME_FACTOR"
)
if [ "$BASE_TIME_FACTOR" != "1" ] || [ "$TARGET_TIME_FACTOR" != "1" ]; then
  log "normalizing legacy rtbench time: $LABEL_A=${BASE_TIME_FACTOR}x $LABEL_B=${TARGET_TIME_FACTOR}x"
fi

A_DRAIN=0; B_DRAIN=0; A_INLINE=0; B_INLINE=0; RUN_MIX=0
A_MODEL_TIME=0; B_MODEL_TIME=0
if uses_perfbench "$BASE_BIN"; then
  A_DRAIN=1
  A_INLINE=1
elif [ -x "$BASE_BIN/rtbench" ]; then
  supports_drain_timeout "$BASE_BIN/rtbench" && A_DRAIN=1
  supports_inline_pressure "$BASE_BIN/rtbench" && A_INLINE=1
fi
if uses_perfbench "$TARGET_BIN"; then
  B_DRAIN=1
  B_INLINE=1
elif [ -x "$TARGET_BIN/rtbench" ]; then
  supports_drain_timeout "$TARGET_BIN/rtbench" && B_DRAIN=1
  supports_inline_pressure "$TARGET_BIN/rtbench" && B_INLINE=1
fi
supports_process_model_time "$BASE_BIN" && A_MODEL_TIME=1
supports_process_model_time "$TARGET_BIN" && B_MODEL_TIME=1
if [ "$A_MODEL_TIME" != "$B_MODEL_TIME" ]; then
  case "$DELAY_SCALE" in
    1|1.0) ;;
    *) die "one side predates process-wide model time; rerun with DELAY_SCALE=1" ;;
  esac
  log "NOTE: one side predates process-wide model time; DELAY_SCALE=1 keeps timing aligned"
fi
if { uses_perfbench "$BASE_BIN" || [ -x "$BASE_BIN/mixbench" ]; } \
   && { uses_perfbench "$TARGET_BIN" || [ -x "$TARGET_BIN/mixbench" ]; } \
   && supports_mix_affinity "$BASE_BIN" \
   && supports_mix_affinity "$TARGET_BIN"; then
  RUN_MIX=1
else
  log "NOTE: a side lacks the affinity-aware mixed scenario; skipping the incomparable grid"
fi
RUN_INLINE=0
if [ "$A_INLINE" = "1" ] && [ "$B_INLINE" = "1" ]; then
  RUN_INLINE=1
else
  log "NOTE: a side lacks inline-pressure support (base=$A_INLINE target=$B_INLINE); skipping it"
fi

MODE_DESC="full"
[ "$SUMMARY" = "1" ] && MODE_DESC="summary (fast, no plots)"
log "BASE=$BASE ($LABEL_A) vs TARGET=$TARGET_DESC ($LABEL_B); mode: $MODE_DESC; \
mixed: $RUN_MIX; drain-timeout: $DRAIN_TIMEOUT; inline-pressure: $RUN_INLINE; \
command-timeout: $COMMAND_TIMEOUT"
rm -rf "$OUT"

# --- Run paired repetitions -------------------------------------------------

# Keep every measured pair adjacent and reverse its order on alternating
# repetitions. This removes the systematic warm-host/time drift of running the
# entire baseline suite before the entire target suite.
for repetition in $(seq 1 "$NUM_RUNS"); do
  if (( repetition % 2 == 1 )); then
    run_contention_once "$LABEL_A" "$BASE_BIN" "$A_DRAIN" "$repetition"
    run_contention_once "$LABEL_B" "$TARGET_BIN" "$B_DRAIN" "$repetition"
  else
    run_contention_once "$LABEL_B" "$TARGET_BIN" "$B_DRAIN" "$repetition"
    run_contention_once "$LABEL_A" "$BASE_BIN" "$A_DRAIN" "$repetition"
  fi

  if [ "$RUN_INLINE" = "1" ]; then
    for profile in ${INLINE_PROFILES//,/ }; do
      if (( repetition % 2 == 1 )); then
        run_inline_pressure_once \
          "$LABEL_A" "$BASE_BIN" "$A_DRAIN" "$profile" "$repetition"
        run_inline_pressure_once \
          "$LABEL_B" "$TARGET_BIN" "$B_DRAIN" "$profile" "$repetition"
      else
        run_inline_pressure_once \
          "$LABEL_B" "$TARGET_BIN" "$B_DRAIN" "$profile" "$repetition"
        run_inline_pressure_once \
          "$LABEL_A" "$BASE_BIN" "$A_DRAIN" "$profile" "$repetition"
      fi
    done
  fi
done

if [ "$RUN_INLINE" = "1" ]; then
  expected_inline=$(( 7 * NUM_RUNS ))
  legacy_expected_inline=$(( 8 * NUM_RUNS ))
  for profile in ${INLINE_PROFILES//,/ }; do
    if ! uses_perfbench "$BASE_BIN"; then
      validate_csv_rows_one_of \
        "$OUT/inline-pressure/$profile/$LABEL_A/inline-pressure.csv" \
        "$expected_inline" "$legacy_expected_inline"
    fi
    if ! uses_perfbench "$TARGET_BIN"; then
      validate_csv_rows_one_of \
        "$OUT/inline-pressure/$profile/$LABEL_B/inline-pressure.csv" \
        "$expected_inline" "$legacy_expected_inline"
    fi
  done
fi

# These sections have their own internal statistical repetition/adaptive
# sampling. Run them adjacently after the explicitly paired focused cells.
run_aux_side "$LABEL_A" "$BASE_BIN" "$RUN_MIX"
run_aux_side "$LABEL_B" "$TARGET_BIN" "$RUN_MIX"

# --- Compare ---------------------------------------------------------------
# Every comparison appends a section to $SUMMARY, leaving one small, trackable
# digest of the run in the output dir.

SUMMARY="$OUT/summary.md"
mkdir -p "$OUT"
{
  echo "# compare-refs summary"
  echo
  echo "- base: $BASE ($LABEL_A)"
  echo "- target: $TARGET_DESC ($LABEL_B)"
  echo "- ratio = $LABEL_B / $LABEL_A (throughput >1 good; latency/ops/cost <1 good)"
  echo "- synthetic model-time delay scale: $DELAY_SCALE"
  if [ "$BASE_TIME_FACTOR" != "1" ] || [ "$TARGET_TIME_FACTOR" != "1" ]; then
    echo "- legacy rtbench time normalization: $LABEL_A=${BASE_TIME_FACTOR}x; $LABEL_B=${TARGET_TIME_FACTOR}x"
  fi
  echo "- each line ends in a \`=> better/WORSE/~same\` verdict read in that"
  echo "  metric's own direction, so no axis has to be interpreted by hand"
  echo "- \`autoresearch-*\` is **deterministic** (single-client backend ops/tx,"
  echo "  lower is better) — the most trustworthy signal; \`mix-*\` cells run"
  echo "  until their throughput 95% CI reaches --target-ci, so a converged"
  echo "  ratio is significant — \`[unconverged]\` marks a cell that hit its time"
  echo "  cap first (read as indicative); \`contention-*\` stay **[noisy]**"
  echo
} >"$SUMMARY"

if [ "$RUN_INLINE" = "1" ]; then
  for profile in ${INLINE_PROFILES//,/ }; do
    uv run "$SCRIPT_DIR/compare.py" \
      --a "$OUT/inline-pressure/$profile/$LABEL_A" \
      --b "$OUT/inline-pressure/$profile/$LABEL_B" \
      --label-a "$LABEL_A" --label-b "$LABEL_B" \
      --title "inline-pressure/$profile" "${TIME_FACTOR_ARGS[@]}" \
      --no-plots --summary-out "$SUMMARY"
  done
fi

uv run "$SCRIPT_DIR/compare.py" \
  --a "$OUT/contention/$LABEL_A" --b "$OUT/contention/$LABEL_B" \
  --label-a "$LABEL_A" --label-b "$LABEL_B" --title "contention" \
  "${TIME_FACTOR_ARGS[@]}" "${PLOT_ARGS[@]}" --summary-out "$SUMMARY"

# Only when both sides produced the affinity-aware grid.
if [ "$RUN_MIX" = "1" ]; then
  uv run "$SCRIPT_DIR/compare.py" \
    --a "$OUT/mixed/$LABEL_A" --b "$OUT/mixed/$LABEL_B" \
    --label-a "$LABEL_A" --label-b "$LABEL_B" --title "mixed" --no-plots \
    "${TIME_FACTOR_ARGS[@]}" --summary-out "$SUMMARY"
else
  log "skipping mixed comparison (missing on a side)"
fi

uv run "$SCRIPT_DIR/compare.py" \
  --a "$OUT/efficiency/$LABEL_A" --b "$OUT/efficiency/$LABEL_B" \
  --label-a "$LABEL_A" --label-b "$LABEL_B" --title "efficiency" --no-plots \
  "${TIME_FACTOR_ARGS[@]}" --summary-out "$SUMMARY"

# --- Clean up worktrees ----------------------------------------------------

clean_worktrees

log "done. summary in $SUMMARY; CSVs + overlay PNGs under $OUT/"
