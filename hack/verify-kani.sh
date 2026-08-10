#!/usr/bin/env bash
set -euo pipefail

required_version="0.67.0"
expected_proof_count=7
root="$(git rev-parse --show-toplevel)"
output_dir="$root/target/kani"
build_dir="$output_dir/build"
kani_timeout_seconds="${KANI_TIMEOUT_SECONDS:-300}"

cd "$root"

if [[ -n "${CARGO_HOME:-}" ]]; then
    kani_cargo_home="$CARGO_HOME"
elif [[ -n "${HOME:-}" ]]; then
    kani_cargo_home="$HOME/.cargo"
else
    echo "error: CARGO_HOME or HOME is required to locate cargo-kani" >&2
    exit 1
fi
kani_proxy="$kani_cargo_home/bin/cargo-kani"
kani_registry="$kani_cargo_home/.crates.toml"

if [[ ! -x "$kani_proxy" ]] \
    || [[ ! -f "$kani_registry" ]] \
    || ! grep -Fq "\"kani-verifier $required_version (" "$kani_registry"; then
    cat >&2 <<EOF
Kani $required_version is required. Install it with:
  cargo install --locked kani-verifier --version $required_version
  cargo kani setup
EOF
    exit 1
fi

timeout_command=""
for candidate in timeout gtimeout; do
    if command -v "$candidate" >/dev/null 2>&1 \
        && "$candidate" --version 2>/dev/null | grep -Fq "GNU coreutils"; then
        timeout_command="$candidate"
        break
    fi
done
if [[ -z "$timeout_command" ]]; then
    echo "error: make verify-kani requires GNU timeout (or gtimeout)" >&2
    exit 1
fi
if [[ ! "$kani_timeout_seconds" =~ ^[1-9][0-9]*$ ]]; then
    echo "error: KANI_TIMEOUT_SECONDS must be a positive integer" >&2
    exit 1
fi

if [[ -n "${KANI_HOME:-}" ]]; then
    kani_base_dir="$KANI_HOME"
elif [[ -n "${HOME:-}" ]]; then
    kani_base_dir="$HOME/.kani"
else
    echo "error: KANI_HOME or HOME is required to locate the Kani bundle" >&2
    exit 1
fi
kani_bundle_dir="$kani_base_dir/kani-$required_version"
if [[ ! -x "$kani_bundle_dir/bin/kani-driver" ]] \
    || [[ ! -x "$kani_bundle_dir/bin/cbmc" ]] \
    || [[ ! -f "$kani_bundle_dir/rust-toolchain-version" ]] \
    || [[ ! -e "$kani_bundle_dir/toolchain" ]] \
    || compgen -G "$kani_base_dir/*.tar.gz" >/dev/null; then
    cat >&2 <<EOF
Kani $required_version setup is missing or incomplete. Complete it explicitly with:
  cargo kani setup
EOF
    exit 1
fi

# The package and setup preflights above prevent the proxy's ordinary first-run
# path from downloading a bundle or installing a Rust toolchain here.
actual_version="$("$kani_proxy" --version)"
if [[ "$actual_version" != "cargo-kani $required_version" ]]; then
    cat >&2 <<EOF
Kani $required_version is required, but found: $actual_version
Install the pinned version with:
  cargo install --locked kani-verifier --version $required_version --force
  cargo kani setup
EOF
    exit 1
fi

mkdir -p "$output_dir" "$build_dir"
lock_dir="$output_dir/.verify-kani.lock"
if ! mkdir "$lock_dir"; then
    echo "error: another verify-kani run is active (or $lock_dir is stale)" >&2
    exit 1
fi
cleanup_lock() {
    rmdir "$lock_dir" 2>/dev/null || true
}
trap cleanup_lock EXIT

proof_count="$(
    find crates/glassdb-storage/src -type f -name '*.rs' \
        -exec grep -hF '#[kani::proof]' {} + \
        | wc -l \
        | tr -d ' '
)"
if [[ "$proof_count" -ne "$expected_proof_count" ]]; then
    echo "error: found $proof_count Kani proofs; update the verified catalog of $expected_proof_count" >&2
    exit 1
fi

# Missing logs are preferable to stale evidence after an interrupted run.
rm -f -- \
    "$output_dir/lifecycle.log" \
    "$output_dir/median-policy.log" \
    "$output_dir/node-finish-split.log" \
    "$output_dir/median-split-upper-bias.log" \
    "$output_dir/lifecycle-mutant-wounded-reclamation.log" \
    "$output_dir/median-split-mutant-empty-lower.log" \
    "$output_dir/node-split-mutant-inherited-holders.log"

run_kani() {
    local name="$1"
    local harness="$2"
    local expected_covers="$3"
    shift 3
    local log="$output_dir/$name.log"

    echo "==> Kani: $name"
    if [[ -x /usr/bin/time ]] && /usr/bin/time -v true >/dev/null 2>&1; then
        /usr/bin/time -v \
            "$timeout_command" --kill-after=10s "${kani_timeout_seconds}s" \
            "$kani_proxy" -p glassdb-storage \
            --target-dir "$build_dir" \
            --output-format terse \
            "$@" \
            --exact \
            --harness "$harness" 2>&1 | tee "$log"
    else
        "$timeout_command" --kill-after=10s "${kani_timeout_seconds}s" \
            "$kani_proxy" -p glassdb-storage \
            --target-dir "$build_dir" \
            --output-format terse \
            "$@" \
            --exact \
            --harness "$harness" 2>&1 | tee "$log"
    fi

    grep -Fq "Checking harness $harness..." "$log"
    grep -Fq "Complete - 1 successfully verified harnesses, 0 failures, 1 total." "$log"
    if [[ "$expected_covers" -eq 0 ]]; then
        if grep -Fq "cover properties satisfied" "$log"; then
            echo "error: unexpected cover properties in $name" >&2
            return 1
        fi
    else
        grep -Fq " ** $expected_covers of $expected_covers cover properties satisfied" "$log"
    fi
}

run_positive() {
    local name="$1"
    local harness="$2"
    local expected_covers="$3"
    local log="$output_dir/$name.log"

    run_kani "$name" "$harness" "$expected_covers"
    grep -Fxq "VERIFICATION:- SUCCESSFUL" "$log"
}

run_stubbed_positive() {
    local name="$1"
    local harness="$2"
    local expected_covers="$3"
    local log="$output_dir/$name.log"

    run_kani "$name" "$harness" "$expected_covers" \
        --features proof-mutants \
        -Z stubbing
    grep -Fxq "VERIFICATION:- SUCCESSFUL" "$log"
}

run_expected_panic() {
    local name="$1"
    local harness="$2"
    local expected_covers="$3"
    local expected="$4"
    local log="$output_dir/$name.log"
    local failed_check_count

    run_kani "$name" "$harness" "$expected_covers" \
        --features proof-mutants \
        -Z stubbing
    failed_check_count="$(grep -c '^Failed Checks:' "$log" || true)"
    if [[ "$failed_check_count" -ne 1 ]]; then
        echo "error: expected exactly one failed check in $name, found $failed_check_count" >&2
        return 1
    fi
    grep -Fq "Failed Checks: \"$expected\"" "$log"
    grep -Fxq "VERIFICATION:- SUCCESSFUL (encountered one or more panics as expected)" "$log"
}

run_positive \
    lifecycle \
    tlogger::lifecycle_proofs::lifecycle_transition_validation_matches_policy \
    5
run_positive \
    median-policy \
    shard::kani_proofs::median_split_index_keeps_bounded_halves_balanced \
    3
run_positive \
    node-finish-split \
    node::kani_proofs::node_finish_split_preserves_b_link_bounds_and_lock_ownership \
    6

# This representative policy change keeps a balanced partition but chooses the
# upper median for odd cardinalities. The unchanged contract must still prove.
run_stubbed_positive \
    median-split-upper-bias \
    shard::kani_proofs::median_split_contract_survives_upper_bias \
    3

# Negative controls use Kani stubs to alter the production kernel only for one
# harness. `should_panic` turns the required assertion counterexample into a
# successful verification outcome and fails if the defect becomes invisible.
run_expected_panic \
    lifecycle-mutant-wounded-reclamation \
    tlogger::lifecycle_proofs::lifecycle_rejects_direct_wounded_reclamation_mutant \
    0 \
    "a pinned wound became directly reclaimable"
run_expected_panic \
    median-split-mutant-empty-lower \
    shard::kani_proofs::median_split_contract_rejects_empty_lower_mutant \
    3 \
    "a split produced an empty lower half"
run_expected_panic \
    node-split-mutant-inherited-holders \
    node::kani_proofs::node_finish_split_rejects_inherited_lock_holders_mutant \
    6 \
    "a split sibling inherited transient node-lock holders"

echo "Kani pilot completed: production proofs and seeded negative controls passed"
