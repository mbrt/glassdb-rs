#!/usr/bin/env bash
set -euo pipefail
export LC_ALL=C

repository_root=$(git rev-parse --show-toplevel)
model_dir="$repository_root/formal/tla"
output_dir="$repository_root/target/formal"
tool_dir="$repository_root/target/formal-tools"

tlc_version=1.7.4
tlc_sha256=936a262061c914694dfd669a543be24573c45d5aa0ff20a8b96b23d01e050e88
tlc_url="https://github.com/tlaplus/tlaplus/releases/download/v${tlc_version}/tla2tools.jar"

if ! command -v java >/dev/null 2>&1; then
    echo "error: make verify-formal requires Java 11 or newer" >&2
    exit 1
fi

java_version_line=$(java -version 2>&1 | head -n 1)
if [[ $java_version_line =~ version\ \"([0-9]+)(\.([0-9]+))? ]]; then
    java_major=${BASH_REMATCH[1]}
    if [[ $java_major -eq 1 ]]; then
        java_major=${BASH_REMATCH[3]}
    fi
else
    echo "error: unable to determine the Java version from: $java_version_line" >&2
    exit 1
fi
if [[ $java_major -lt 11 ]]; then
    echo "error: make verify-formal requires Java 11 or newer (found $java_major)" >&2
    exit 1
fi

sha256() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum -- "$1" | cut -d ' ' -f 1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 -- "$1" | cut -d ' ' -f 1
    else
        echo "error: sha256sum or shasum is required" >&2
        exit 1
    fi
}

verify_jar() {
    local jar=$1
    local actual
    actual=$(sha256 "$jar")
    if [[ "$actual" != "$tlc_sha256" ]]; then
        echo "error: unexpected SHA-256 for $jar" >&2
        echo "expected: $tlc_sha256" >&2
        echo "actual:   $actual" >&2
        exit 1
    fi
}

if [[ -n "${TLA2TOOLS_JAR:-}" ]]; then
    tlc_jar=$TLA2TOOLS_JAR
    if [[ ! -f "$tlc_jar" ]]; then
        echo "error: TLA2TOOLS_JAR does not name a file: $tlc_jar" >&2
        exit 1
    fi
    if [[ $tlc_jar != /* ]]; then
        tlc_jar=$(cd "$(dirname "$tlc_jar")" && pwd -P)/$(basename "$tlc_jar")
    fi
    verify_jar "$tlc_jar"
else
    mkdir -p "$tool_dir"
    tlc_jar="$tool_dir/tla2tools-${tlc_version}.jar"
    if [[ ! -f "$tlc_jar" ]]; then
        if ! command -v curl >/dev/null 2>&1; then
            echo "error: curl is required to fetch the pinned TLC artifact" >&2
            echo "set TLA2TOOLS_JAR to an offline copy instead" >&2
            exit 1
        fi

        temporary_jar=$(mktemp "$tool_dir/tla2tools.XXXXXX")
        trap 'rm -f "$temporary_jar"' EXIT
        if ! curl --fail --location --retry 3 --output "$temporary_jar" "$tlc_url"; then
            echo "error: unable to download TLC; set TLA2TOOLS_JAR to an offline copy" >&2
            exit 1
        fi
        verify_jar "$temporary_jar"
        mv "$temporary_jar" "$tlc_jar"
        trap - EXIT
    fi
    verify_jar "$tlc_jar"
fi

tlc() {
    local module=$1
    local configuration=$2
    local metadata=$3
    local metadata_dir="$output_dir/$metadata"

    if [[ ! $metadata =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
        echo "error: invalid TLC metadata name: $metadata" >&2
        return 1
    fi

    rm -rf -- "$metadata_dir"

    (
        cd "$model_dir"
        java -XX:+UseParallelGC -jar "$tlc_jar" \
            -workers 1 \
            -seed 1 \
            -fp 0 \
            -cleanup \
            -metadir "$metadata_dir" \
            -config "$configuration" \
            "$module"
    )
}

run_check() {
    local name=$1
    local kind=$2
    local module=$3
    local configuration=$4
    local log="$output_dir/$name.log"

    echo "==> $kind: $name"
    if ! tlc "$module" "$configuration" "$name" >"$log" 2>&1; then
        cat "$log" >&2
        return 1
    fi
    grep -E 'Model checking completed|states generated|Finished in' "$log"
}

run_expected_counterexample() {
    local kind=$1
    local name=$2
    local module=$3
    local configuration=$4
    local expected_invariant=$5
    local expected_action=$6
    local log="$output_dir/$name.log"
    local status

    echo "==> expected $kind counterexample: $name"
    set +e
    tlc "$module" "$configuration" "$name" >"$log" 2>&1
    status=$?
    set -e

    if [[ $status -ne 12 ]]; then
        echo "error: $kind counterexample $name returned $status instead of TLC's safety-violation status 12" >&2
        cat "$log" >&2
        return 1
    fi
    if ! grep -Fq "Invariant $expected_invariant is violated." "$log"; then
        echo "error: $kind counterexample $name failed for an unexpected reason" >&2
        cat "$log" >&2
        return 1
    fi
    if ! grep -Fq "<$expected_action " "$log"; then
        echo "error: $kind counterexample $name did not exercise $expected_action" >&2
        cat "$log" >&2
        return 1
    fi
    grep -E "Invariant $expected_invariant is violated|states generated|Finished in" "$log"
}

configuration_name() {
    local stem=${1%.cfg}

    printf '%s\n' "$stem" \
        | sed -E 's/([A-Z]+)([A-Z][a-z])/\1-\2/g; s/([a-z0-9])([A-Z])/\1-\2/g' \
        | tr '[:upper:]' '[:lower:]'
}

verify_directive='\* @verify-formal'
identifier_pattern='^[A-Za-z_][A-Za-z0-9_]*$'

configuration_paths=()
configurations=()
run_names=()
run_modes=()
run_modules=()
expected_invariants=()
expected_actions=()

shopt -s nullglob
configuration_paths=("$model_dir"/*.cfg)
shopt -u nullglob

if [[ ${#configuration_paths[@]} -eq 0 ]]; then
    echo "error: no formal configurations found below $model_dir" >&2
    exit 1
fi

# A configuration file is the registration point for one run.  Validate the
# complete catalog before starting TLC so a malformed later entry fails fast.
for configuration_path in "${configuration_paths[@]}"; do
    configuration=$(basename "$configuration_path")
    name=$(configuration_name "$configuration")
    directive_count=$(grep -Ec \
        '^[[:space:]]*\\\*[[:space:]]+@verify-formal([[:space:]]|$)' \
        "$configuration_path" || true)

    if [[ $directive_count -ne 1 ]]; then
        echo "error: $configuration must contain exactly one $verify_directive directive" >&2
        exit 1
    fi

    directive_line=$(head -n 1 "$configuration_path")
    read -r comment directive mode module expected_invariant expected_action extra \
        <<<"$directive_line"

    if [[ $comment != '\*' || $directive != '@verify-formal' ]]; then
        echo "error: $configuration must place its $verify_directive directive on the first line" >&2
        exit 1
    fi
    if [[ ! $name =~ ^[a-z0-9][a-z0-9-]*$ ]]; then
        echo "error: $configuration does not produce a valid run name: $name" >&2
        exit 1
    fi
    # The conditional expansion keeps an empty array safe under `set -u` on
    # Bash 3.2, which is still the system shell on older macOS installations.
    for existing_name in ${run_names[@]+"${run_names[@]}"}; do
        if [[ $name == "$existing_name" ]]; then
            echo "error: duplicate formal run name derived from $configuration: $name" >&2
            exit 1
        fi
    done
    if [[ ! $module =~ $identifier_pattern ]]; then
        echo "error: invalid module in $configuration: $module" >&2
        exit 1
    fi
    if [[ ! -f "$model_dir/$module.tla" ]]; then
        echo "error: module for $configuration does not exist: $module.tla" >&2
        exit 1
    fi

    case "$mode" in
        safety)
            if [[ -n $expected_invariant || -n $expected_action || -n $extra ]]; then
                echo "error: safety directive in $configuration has unexpected fields" >&2
                exit 1
            fi
            if grep -Eq '^[[:space:]]*PROPERT(Y|IES)([[:space:]]|$)' "$configuration_path"; then
                echo "error: safety configuration $configuration declares a temporal property" >&2
                exit 1
            fi
            ;;
        liveness)
            if [[ -n $expected_invariant || -n $expected_action || -n $extra ]]; then
                echo "error: liveness directive in $configuration has unexpected fields" >&2
                exit 1
            fi
            if ! grep -Eq '^[[:space:]]*PROPERT(Y|IES)([[:space:]]|$)' "$configuration_path"; then
                echo "error: liveness configuration $configuration declares no temporal property" >&2
                exit 1
            fi
            ;;
        mutant | known-protocol)
            if [[ ! $expected_invariant =~ $identifier_pattern \
                || ! $expected_action =~ $identifier_pattern \
                || -n $extra ]]; then
                echo "error: counterexample directive in $configuration must name one invariant and action" >&2
                exit 1
            fi
            if ! grep -Eq "^[[:space:]]*(INVARIANTS?[[:space:]]+)?${expected_invariant}[[:space:]]*$" \
                "$configuration_path"; then
                echo "error: $configuration does not check $expected_invariant" >&2
                exit 1
            fi
            if ! grep -Eq "^[[:space:]]*${expected_action}(\([^)]*\))?[[:space:]]*==" \
                "$model_dir/$module.tla"; then
                echo "error: $module.tla does not define $expected_action" >&2
                exit 1
            fi
            ;;
        *)
            echo "error: unknown verification mode in $configuration: $mode" >&2
            exit 1
            ;;
    esac

    configurations+=("$configuration")
    run_names+=("$name")
    run_modes+=("$mode")
    run_modules+=("$module.tla")
    expected_invariants+=("$expected_invariant")
    expected_actions+=("$expected_action")
done

# This directory is owned by this runner.  Starting from an empty directory
# prevents traces for retired, renamed, or undiscovered configurations from
# being mistaken for evidence from the current catalog.
rm -rf -- "$output_dir"
mkdir -p "$output_dir"

for index in "${!configurations[@]}"; do
    mode=${run_modes[$index]}
    if [[ $mode == safety || $mode == liveness ]]; then
        run_check \
            "${run_names[$index]}" \
            "$mode" \
            "${run_modules[$index]}" \
            "${configurations[$index]}"
    else
        kind=${mode//-/ }
        run_expected_counterexample \
            "$kind" \
            "${run_names[$index]}" \
            "${run_modules[$index]}" \
            "${configurations[$index]}" \
            "${expected_invariants[$index]}" \
            "${expected_actions[$index]}"
    fi
done

echo "formal exploration completed: normal checks passed and required counterexamples reproduced"
