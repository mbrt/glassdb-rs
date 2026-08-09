#!/usr/bin/env bash
set -euo pipefail

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

mkdir -p "$output_dir"

tlc() {
    local module=$1
    local configuration=$2
    local metadata=$3
    local metadata_dir="$output_dir/$metadata"

    case "$metadata" in
        same-leaf-* | cross-leaf-* | mutant-*) ;;
        *)
            echo "error: invalid TLC metadata name: $metadata" >&2
            return 1
            ;;
    esac

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

run_safety() {
    local name=$1
    local configuration=$2
    local log="$output_dir/$name.log"

    echo "==> safety: $name"
    if ! tlc MC_TxCore.tla "$configuration" "$name" >"$log" 2>&1; then
        cat "$log" >&2
        return 1
    fi
    grep -E 'Model checking completed|states generated|Finished in' "$log"
}

run_mutant() {
    local name=$1
    local configuration=$2
    local expected_invariant=$3
    local expected_action=$4
    local log="$output_dir/$name.log"
    local status

    echo "==> expected counterexample: $name"
    set +e
    tlc MC_TxCoreMutants.tla "$configuration" "$name" >"$log" 2>&1
    status=$?
    set -e

    if [[ $status -ne 12 ]]; then
        echo "error: mutant $name returned $status instead of TLC's safety-violation status 12" >&2
        cat "$log" >&2
        return 1
    fi
    if ! grep -Fq "Invariant $expected_invariant is violated." "$log"; then
        echo "error: mutant $name failed for an unexpected reason" >&2
        cat "$log" >&2
        return 1
    fi
    if ! grep -Fq "<$expected_action " "$log"; then
        echo "error: mutant $name did not exercise $expected_action" >&2
        cat "$log" >&2
        return 1
    fi
    grep -E "Invariant $expected_invariant is violated|states generated|Finished in" "$log"
}

run_safety same-leaf-distinct TxCoreSameLeafDistinct.cfg
run_safety same-leaf-equal TxCoreSameLeafEqual.cfg
run_safety cross-leaf-distinct TxCoreCrossLeafDistinct.cfg
run_safety cross-leaf-equal TxCoreCrossLeafEqual.cfg

run_mutant mutant-terminal TxCoreMutantTerminal.cfg S1_TerminalState ReverseCommitted
run_mutant mutant-publication TxCoreMutantPublication.cfg S4_Refinement PublishBeforeCommit
run_mutant mutant-validation TxCoreMutantValidation.cfg S9_PostLockValidation CommitWithoutPostLockValidation
run_mutant mutant-writer-token TxCoreMutantWriterToken.cfg S9_PostLockValidation CommitWithoutPostLockValidation
run_mutant mutant-expiry TxCoreMutantExpiry.cfg S10_CommittedCannotAbort ExpireCommitted
run_mutant mutant-uncertainty TxCoreMutantUncertainty.cfg S11_UncertaintyIsConservative MisclassifyUncertain

echo "formal transaction-core pilot passed"
