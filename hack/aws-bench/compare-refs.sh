#!/usr/bin/env bash
#
# Compare GlassDB transaction performance between two engine versions (git
# refs) under the in-memory backend with simulated S3 latency and throttling.
#
# It builds `rtbench` + `autoresearch` (+ `mixbench`, when present) from a base
# ref (default `main`, built in a reused detached git worktree) and from the
# target tree (the current worktree by default), runs the same workloads on both
# into `out-refs/`, and diffs them with `compare.py`. Throughput and latency are
# the primary axes; retries and backend round-trips per transaction
# (object-storage efficiency) are secondary. `mixbench` adds a mixed-workload
# contention grid (per-shape throughput and ops/tx across contention mode x
# Database topology) that surfaces the in-process request-dedup efficiency the
# low-contention rw9010 sweep does not.
#
# Because each ref compiles its own engine (the Backend trait differs across v1
# and v2), the two sides are built from separate source trees and reconciled
# through the CSV/JSON outputs. The cross-version run is only fully
# apples-to-apples once both refs carry the enhanced `rtbench` (e.g. after `main`
# is merged into the v2 branch); against an older target the driver falls back
# to the `balanced` mix only and `compare.py` degrades gracefully. Likewise
# `mixbench` runs only on refs that carry it; a ref that predates it skips that
# section, while a present binary must complete successfully.
#
# Each run leaves a small, trackable digest at $OUT/summary.md (the per-section
# ratio summaries plus the deterministic autoresearch score). It is the only
# out-refs artifact that is not gitignored, so it can be committed to follow the
# numbers over time. The worktrees built for the base/target refs are removed at
# the end of every run (same as `--clean`).
#
# `--summary` runs every section that feeds summary.md (rw9010 mixes, deadlock,
# mixbench, efficiency) but with much smaller windows, two concurrency points,
# and no overlay PNGs. It keeps a few repeats for the low-variance signals (the
# deterministic autoresearch score and the rw9010 sweep) so the digest's
# min/median/max are not single-sample false precision; mixbench self-terminates
# at a (looser, faster) target CI so its ratios stay significant. It produces the
# same summary.md sections an order of magnitude faster than the full sweep, at
# the cost of coarser CIs and no plots. Explicit env tunables still override the
# fast defaults.
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
#   DELAY_SCALE=0.05 / 0.02 compress simulated latency + rate limits (preserves
#                           the throttle shape); 1.0 = real time
#   DB_LIST=1,10,20,40 / 1,10   rw9010 concurrency points (number of Databases)
#   NUM_KEYS=5000           rw9010 key count
#   DURATION=15s / 3s       rw9010 duration per concurrency step
#   NUM_RUNS=1 / 2          repeat each rw9010/deadlock sweep (tighter bands)
#   DEADLOCK_DURATION=8s / 3s   deadlock duration per contention configuration
#   COUNT=5 / 3             autoresearch suite repeats (reports the median)
#   RW_MIX="balanced readheavy writeheavy"   rw9010 mixes to run
#   MIX_DURATION=2s / 1s    mixbench minimum measured window per cell
#   MIX_MAX_DURATION=60s / 20s  mixbench per-cell time cap (upper bound)
#   MIX_TARGET_CI=0.1 / 0.2 mixbench target throughput 95% CI half-width; the
#                           cell runs until every shape reaches it or the cap
#   MIX_MODES=lo,hi         mixbench contention modes to sweep
#   MIX_TOPOLOGIES=shared,per-shape          mixbench Database topologies
#   MIX_WORKERS=8           mixbench workers per shape
#   MIX_CLIENTS=4           mixbench client Databases per shape (per-shape topo)
#   MIX_NUM_KEYS=<NUM_KEYS> mixbench lo-mode key pool
#   MIX_HOT_KEYS=8          mixbench hi-mode hot-key pool
#   MIX_MULTI_KEYS=10       mixbench keys per multi-key shape
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
  DELAY_SCALE="${DELAY_SCALE:-0.02}"
  DB_LIST="${DB_LIST:-1,10}"
  DURATION="${DURATION:-3s}"
  DEADLOCK_DURATION="${DEADLOCK_DURATION:-3s}"
  # A few repeats even in the fast path: the autoresearch score and the rw9010
  # sweep are the low-variance signals worth trusting, and a single sample
  # collapses the digest's min/median/max into false precision. Cheap because
  # both are short. mixbench self-terminates at its target CI (no repeats
  # needed), but a looser CI and shorter cap keep the fast path fast.
  COUNT="${COUNT:-3}"
  NUM_RUNS="${NUM_RUNS:-2}"
  MIX_DURATION="${MIX_DURATION:-1s}"
  MIX_MAX_DURATION="${MIX_MAX_DURATION:-20s}"
  MIX_TARGET_CI="${MIX_TARGET_CI:-0.2}"
  DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-30s}"
else
  DELAY_SCALE="${DELAY_SCALE:-0.05}"
  DB_LIST="${DB_LIST:-1,10,20,40}"
  DURATION="${DURATION:-15s}"
  DEADLOCK_DURATION="${DEADLOCK_DURATION:-8s}"
  COUNT="${COUNT:-5}"
  NUM_RUNS="${NUM_RUNS:-1}"
  MIX_DURATION="${MIX_DURATION:-2s}"
  MIX_MAX_DURATION="${MIX_MAX_DURATION:-60s}"
  MIX_TARGET_CI="${MIX_TARGET_CI:-0.1}"
  # The current 40-Database workload has a measured 40+ second transaction
  # tail. Keep the run bounded without rejecting that valid slow sample.
  DRAIN_TIMEOUT="${DRAIN_TIMEOUT:-90s}"
fi
NUM_KEYS="${NUM_KEYS:-5000}"
RW_MIX="${RW_MIX:-balanced readheavy writeheavy}"
# mixbench (mixed-workload contention grid) tunables. Skipped automatically for
# any ref that predates the binary (e.g. an old BASE).
MIX_MODES="${MIX_MODES:-lo,hi}"
MIX_TOPOLOGIES="${MIX_TOPOLOGIES:-shared,per-shape}"
MIX_WORKERS="${MIX_WORKERS:-8}"
MIX_CLIENTS="${MIX_CLIENTS:-4}"
MIX_NUM_KEYS="${MIX_NUM_KEYS:-$NUM_KEYS}"
MIX_HOT_KEYS="${MIX_HOT_KEYS:-8}"
MIX_MULTI_KEYS="${MIX_MULTI_KEYS:-10}"
COMMAND_TIMEOUT="${COMMAND_TIMEOUT:-15m}"
OUT="${OUT:-$SCRIPT_DIR/out-refs}"
BASE_WT="${BASE_WT:-$(dirname "$REPO_ROOT")/.glassdb-perf-base}"
TARGET_WT_DEFAULT="$(dirname "$REPO_ROOT")/.glassdb-perf-target"
TARGET_WT="${TARGET_WT:-$TARGET_WT_DEFAULT}"

# summary.md never embeds the overlay PNGs, so skip them in --summary mode (the
# mixbench/efficiency comparisons already skip plots regardless).
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

validate_rw_results() {
  local path="$1" expected="$2"
  awk -F, -v expected="$expected" '
    NR == 1 {
      for (i = 1; i <= NF; i++) if ($i == "tx-failures") failures_col = i
      next
    }
    { rows++ }
    failures_col && $failures_col + 0 != 0 {
      printf("%s: transaction failures in row %d: %s\n", FILENAME, NR, $failures_col) > "/dev/stderr"
      bad = 1
    }
    END {
      if (!failures_col) {
        printf("%s: missing tx-failures column\n", FILENAME) > "/dev/stderr"
        bad = 1
      }
      if (rows != expected) {
        printf("%s: expected %d cells, found %d\n", FILENAME, expected, rows) > "/dev/stderr"
        bad = 1
      }
      exit bad
    }
  ' "$path"
}

validate_mix_results() {
  local path="$1" expected="$2" cells
  cells="$(grep -c '"mode"' "$path" || true)"
  if [ "$cells" -ne "$expected" ]; then
    log "ERROR: $path expected $expected cells, found $cells"
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
  local dir="$1"
  log "building rtbench + autoresearch in $dir (release)"
  (cd "$dir" && cargo build --release --bin rtbench --bin autoresearch >&2)
  # mixbench is newer than some base refs, so build it best-effort: a ref that
  # predates the binary just skips the mixbench section (like the --rw-mix
  # negotiation).
  if (cd "$dir" && cargo build --release --bin mixbench) >/dev/null 2>&1; then
    log "built mixbench in $dir"
  else
    log "NOTE: no mixbench binary in $dir (older ref); its mixbench section is skipped"
  fi
}

# Whether the rtbench binary at $1 understands --rw-mix (post-enhancement).
supports_rw_mix() {
  "$1/rtbench" --help 2>&1 | grep -q -- "--rw-mix"
}

supports_drain_timeout() {
  "$1" --help 2>&1 | grep -q -- "--drain-timeout"
}

# Run every workload for one side into $OUT/<group>/<label>/.
#   $1 = label (output subdir + report label)
#   $2 = bin dir (… /target/release)
#   $3 = whether this side supports --rw-mix (0/1)
#   $4 = whether this side's rtbench supports --drain-timeout (0/1)
run_side() {
  local label="$1" bindir="$2" has_mix="$3" has_drain="$4"
  local common=(--backend=memory --delays=s3 --delay-scale="$DELAY_SCALE")
  local drain_args=()
  [ "$has_drain" = "1" ] && drain_args=(--drain-timeout="$DRAIN_TIMEOUT")
  local expected_rw
  expected_rw=$(( $(csv_items "$DB_LIST") * NUM_RUNS ))

  for mix in $MIXES; do
    local d="$OUT/$mix/$label"
    mkdir -p "$d"
    local mix_args=()
    if [ "$has_mix" = "1" ]; then
      mix_args=(--rw-mix="$mix")
    fi
    log "$label rw9010 mix=$mix"
    run_bounded "$bindir/rtbench" "${common[@]}" "${drain_args[@]}" \
      --test-name=rw9010 "${mix_args[@]}" \
      --db-list="$DB_LIST" --num-keys="$NUM_KEYS" \
      --duration="$DURATION" --num-runs="$NUM_RUNS" \
      --samples-out="$d/samples.csv" --stats-out="$d/stats.csv" \
      --throughput-out="$d/throughput.csv" --client-stats-out="$d/client-stats.csv" >&2
    validate_rw_results "$d/client-stats.csv" "$expected_rw"
  done

  local dd="$OUT/contention/$label"
  mkdir -p "$dd"
  log "$label deadlock"
  run_bounded "$bindir/rtbench" "${common[@]}" "${drain_args[@]}" \
    --test-name=deadlock --duration="$DEADLOCK_DURATION" --num-runs="$NUM_RUNS" \
    --deadlock-out="$dd/deadlock.csv" >&2

  # mixbench: all shapes together over the contention x topology grid. Only
  # when this side actually built the binary (older refs skip it); progress
  # goes to stderr, the JSON grid to the compared artifact. An available binary
  # is strict: command errors, missing cells, and transaction failures abort the
  # comparison rather than silently dropping its section.
  if [ -x "$bindir/mixbench" ]; then
    local dm="$OUT/mixbench/$label"
    mkdir -p "$dm"
    log "$label mixbench"
    local mix_drain_args=()
    supports_drain_timeout "$bindir/mixbench" \
      && mix_drain_args=(--drain-timeout="$DRAIN_TIMEOUT")
    run_bounded "$bindir/mixbench" --delays=s3 --delay-scale="$DELAY_SCALE" \
      "${mix_drain_args[@]}" \
      --duration="$MIX_DURATION" --max-duration="$MIX_MAX_DURATION" \
      --target-ci="$MIX_TARGET_CI" --modes="$MIX_MODES" --topologies="$MIX_TOPOLOGIES" \
      --workers-per-shape="$MIX_WORKERS" --clients-per-shape="$MIX_CLIENTS" \
      --num-keys="$MIX_NUM_KEYS" --hot-keys="$MIX_HOT_KEYS" --multi-keys="$MIX_MULTI_KEYS" \
      --json >"$dm/mixbench.json"
    local expected_mix
    expected_mix=$(( $(csv_items "$MIX_MODES") * $(csv_items "$MIX_TOPOLOGIES") ))
    validate_mix_results "$dm/mixbench.json" "$expected_mix"
  else
    log "$label has no mixbench binary; skipping mixbench"
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
A_MIX=0; B_MIX=0; A_DRAIN=0; B_DRAIN=0
supports_rw_mix "$BASE_BIN" && A_MIX=1
supports_rw_mix "$TARGET_BIN" && B_MIX=1
supports_drain_timeout "$BASE_BIN/rtbench" && A_DRAIN=1
supports_drain_timeout "$TARGET_BIN/rtbench" && B_DRAIN=1
if [ "$A_MIX" = "1" ] && [ "$B_MIX" = "1" ]; then
  MIXES="$RW_MIX"
else
  log "WARNING: a side lacks --rw-mix (base=$A_MIX target=$B_MIX); running balanced only"
  MIXES="balanced"
fi

MODE_DESC="full"
[ "$SUMMARY" = "1" ] && MODE_DESC="summary (fast, no plots)"
log "BASE=$BASE ($LABEL_A) vs TARGET=$TARGET_DESC ($LABEL_B); mode: $MODE_DESC; \
mixes: $MIXES; drain-timeout: $DRAIN_TIMEOUT; command-timeout: $COMMAND_TIMEOUT"
rm -rf "$OUT"

# --- Run both sides back-to-back -------------------------------------------

run_side "$LABEL_A" "$BASE_BIN" "$A_MIX" "$A_DRAIN"
run_side "$LABEL_B" "$TARGET_BIN" "$B_MIX" "$B_DRAIN"

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
  echo "- each line ends in a \`=> better/WORSE/~same\` verdict read in that"
  echo "  metric's own direction, so no axis has to be interpreted by hand"
  echo "- \`autoresearch-*\` is **deterministic** (single-client backend ops/tx,"
  echo "  lower is better) — the most trustworthy signal; \`mix-*\` cells run"
  echo "  until their throughput 95% CI reaches --target-ci, so a converged"
  echo "  ratio is significant — \`[unconverged]\` marks a cell that hit its time"
  echo "  cap first (read as indicative); \`deadlock-*\` stay **[noisy]**"
  echo
} >"$SUMMARY"

for mix in $MIXES; do
  uv run "$SCRIPT_DIR/compare.py" \
    --a "$OUT/$mix/$LABEL_A" --b "$OUT/$mix/$LABEL_B" \
    --label-a "$LABEL_A" --label-b "$LABEL_B" --title "rw9010/$mix" \
    "${PLOT_ARGS[@]}" --summary-out "$SUMMARY"
done

uv run "$SCRIPT_DIR/compare.py" \
  --a "$OUT/contention/$LABEL_A" --b "$OUT/contention/$LABEL_B" \
  --label-a "$LABEL_A" --label-b "$LABEL_B" --title "deadlock" \
  "${PLOT_ARGS[@]}" --summary-out "$SUMMARY"

# Only when both sides produced a grid (both refs carry mixbench).
if [ -f "$OUT/mixbench/$LABEL_A/mixbench.json" ] \
   && [ -f "$OUT/mixbench/$LABEL_B/mixbench.json" ]; then
  uv run "$SCRIPT_DIR/compare.py" \
    --a "$OUT/mixbench/$LABEL_A" --b "$OUT/mixbench/$LABEL_B" \
    --label-a "$LABEL_A" --label-b "$LABEL_B" --title "mixbench" --no-plots \
    --summary-out "$SUMMARY"
else
  log "skipping mixbench comparison (missing on a side)"
fi

uv run "$SCRIPT_DIR/compare.py" \
  --a "$OUT/efficiency/$LABEL_A" --b "$OUT/efficiency/$LABEL_B" \
  --label-a "$LABEL_A" --label-b "$LABEL_B" --title "efficiency" --no-plots \
  --summary-out "$SUMMARY"

# --- Clean up worktrees ----------------------------------------------------

clean_worktrees

log "done. summary in $SUMMARY; CSVs + overlay PNGs under $OUT/"
