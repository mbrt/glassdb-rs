#!/usr/bin/env python3
"""Compare one candidate harness against two engine revisions without cloud access."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import time

if __package__:
    from . import perf_report
else:
    import perf_report

# Planned checks bound the cost of repeated statistical decisions. Each count is
# even so both revisions run first equally often at every possible stopping point.
CHECKPOINTS = [8, 16, 32]
# Measured processes take well under a second, because they run on virtual time.
# Guard each process against hangs, such as a CPU livelock that virtual time
# cannot see.
PROCESS_TIMEOUT = 120
CASES = (
    "warm_read",
    "warm_read_external",
    "fresh_client_read",
    "rmw_inline_1024",
    "rmw_external_1025",
    "rmw_five_leaves",
    "read_long_keys_large_collection",
    "rmw_shared_leaf_three_transactions",
)
# The suffix of the file with the counters of a diagnostic case.
COUNTERS_SUFFIX = perf_report.COUNTERS_SUFFIX
# Virtual time charges a long window only as CPU time. A long window averages
# the jitter of the provider latency out of the percentile samples, so that
# changes of a few percent remain visible. At 120 s, the timings of several
# cases needed 16 pairs; at 300 s, 8 pairs resolve all but the mixed workload.
MEASURE_WINDOW = "300s"
# The options of every compared process. The experiments come from the
# built-in configuration of the candidate harness.
COMMON_ARGS = [
    "--backend=memory",
    "--delays=s3",
    # Virtual time makes a run independent of the host.
    "--time=virtual",
    # The delay profile is not part of the copied harness, so pin its prefix
    # partition depth on both sides.
    "--prefix-depth=3",
    f'--set=cell.measure.duration="{MEASURE_WINDOW}"',
    f'--set=cell.measure.max-duration="{MEASURE_WINDOW}"',
    "--runs=1",
    "--drain-timeout=10s",
]
# The provider latency keeps its jitter, so that percentiles are realistic.
MIXED_ARGS = [*COMMON_ARGS, "--experiment=ci"]
# Jitter moves the counts of background operations by one per process, so the
# counters of a case come from a second process without it. They repeat exactly,
# so the default window is enough.
COUNTER_ARGS = [
    *(arg for arg in COMMON_ARGS if not arg.startswith("--set=cell.measure")),
    "--latency-jitter=false",
]
MIXED_COUNTER_ARGS = [*COUNTER_ARGS, "--experiment=ci"]


def capture(command: list[str], cwd: Path) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True)


def snapshot(repo: Path, destination: Path, revision: str | None) -> None:
    destination.mkdir()
    if revision:
        with subprocess.Popen(
            ["git", "archive", revision], cwd=repo, stdout=subprocess.PIPE
        ) as source:
            subprocess.run(
                ["tar", "-x", "-C", str(destination)], stdin=source.stdout, check=True
            )
            if source.wait() != 0:
                raise RuntimeError(f"cannot archive revision {revision}")
    else:
        names = capture(
            ["git", "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
            repo,
        )
        for name in set(names.split("\0")) - {""}:
            source = repo / name
            if source.is_file() or source.is_symlink():
                target = destination / name
                target.parent.mkdir(parents=True, exist_ok=True)
                shutil.copy2(source, target, follow_symlinks=False)


def use_harness(candidate: Path, base: Path) -> str:
    """Copy the benchmark sources, retaining engine dependencies and locks."""
    paths = [Path("crates/glassdb-bench-scale")]
    digest = hashlib.sha256()
    for relative in paths:
        source = candidate / relative
        target = base / relative
        if not target.is_dir():
            raise RuntimeError(f"unsupported comparison: baseline has no {relative}")
        # These directories are disposable snapshots owned by this invocation.
        shutil.rmtree(target)
        shutil.copytree(source, target)
        for path in sorted(source.rglob("*")):
            if path.is_file():
                digest.update(str(path.relative_to(candidate)).encode())
                digest.update(path.read_bytes())
    return digest.hexdigest()


def build(
    source: Path, target: Path, output: Path, side: str, log: str | None = None
) -> dict:
    result = {}
    for package, selector, name in (("glassdb-bench-scale", "--bin", "perfbench"),):
        command = [
            "cargo",
            "build",
            "--release",
            "--target-dir",
            str(target),
            "-p",
            package,
            selector,
            name,
            "--message-format=json-render-diagnostics",
        ]
        try:
            messages = capture(command, source)
        except subprocess.CalledProcessError as error:
            (output / f"{log or side}-build.jsonl").write_text(error.output or "")
            raise RuntimeError(
                f"unsupported comparison: candidate harness does not build against {side}; see build log"
            ) from error
        executable = None
        for line in messages.splitlines():
            message = json.loads(line)
            if (
                message.get("reason") == "compiler-artifact"
                and message["target"]["name"] == name
            ):
                executable = message.get("executable") or executable
        if executable is None:
            raise RuntimeError(f"no executable for {side}/{name}")
        destination = output / "bin" / f"{side}-{name}"
        destination.parent.mkdir(exist_ok=True)
        shutil.copy2(executable, destination)
        result[name] = str(destination)
    result["lockSha256"] = hashlib.sha256(
        (source / "Cargo.lock").read_bytes()
    ).hexdigest()
    result["executableSha256"] = {
        "perfbench": hashlib.sha256(Path(result["perfbench"]).read_bytes()).hexdigest()
    }
    return result


def prepare(repo: Path, output: Path, base: str, candidate: str | None) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schemaVersion": 2,
        "checkpoints": CHECKPOINTS,
        "completedPairs": {name: 0 for name in (*CASES, "mixed")},
        "cases": CASES,
        "mixedArgs": MIXED_ARGS,
        "diagnosticArgs": [*COMMON_ARGS, "--experiment=diagnostics"],
        "counterArgs": [*COUNTER_ARGS, "--experiment=diagnostics"],
        "mixedCounterArgs": MIXED_COUNTER_ARGS,
        "processTimeoutSeconds": PROCESS_TIMEOUT,
        "base": base,
        "candidate": candidate or "working tree",
        "rustc": capture(["rustc", "--version"], repo).strip(),
        "warnings": [],
    }
    manifest["baseCommit"] = capture(
        ["git", "rev-parse", f"{base}^{{commit}}"], repo
    ).strip()
    manifest["candidateCommit"] = capture(
        ["git", "rev-parse", f"{candidate or 'HEAD'}^{{commit}}"], repo
    ).strip()
    with tempfile.TemporaryDirectory(prefix="glassdb-comparison-") as directory:
        snapshots = Path(directory)
        snapshot(repo, snapshots / "main", manifest["baseCommit"])
        snapshot(
            repo, snapshots / "pr", manifest["candidateCommit"] if candidate else None
        )
        # Keep a pristine copy for the fallback below, as use_harness overwrites.
        shutil.copytree(snapshots / "main", snapshots / "main-own")
        manifest["harnessSha256"] = use_harness(snapshots / "pr", snapshots / "main")
        targets = repo / "target/performance"
        try:
            manifest["main"] = build(
                snapshots / "main", targets / "main", output, "main", "main-shared"
            )
        except RuntimeError as error:
            # The candidate harness needs APIs that the base lacks. Measure both
            # engines with the base harness, so that the sides stay comparable.
            print(f"{error}; retrying with the base harness", file=sys.stderr)
            manifest["harnessSha256"] = use_harness(
                snapshots / "main-own", snapshots / "pr"
            )
            manifest["harness"] = "base"
            manifest["warnings"].append(
                f"The candidate harness does not build against {base}; "
                "both revisions were measured with the harness of the base."
            )
            manifest["main"] = build(
                snapshots / "main-own", targets / "main", output, "main"
            )
        manifest["pr"] = build(snapshots / "pr", targets / "pr", output, "pr")
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def measure(output: Path, manifest: dict) -> None:
    perf_report.validate_manifest(manifest)
    if manifest["schemaVersion"] != 2 or any(manifest["completedPairs"].values()):
        raise ValueError("measurement requires a new checkpoint comparison")
    process_timeout = perf_report.number(manifest.get("processTimeoutSeconds"))
    if process_timeout == 0:
        raise ValueError("process timeout must be positive")
    for side in ("main", "pr"):
        if (output / side).exists():
            raise FileExistsError(
                f"measurement directory already exists: {output / side}"
            )
    start = time.monotonic()
    manifest["platform"] = platform.platform()
    manifest["cpuModel"] = platform.processor()
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        manifest["cpuModel"] = next(
            (
                line.split(":", 1)[1].strip()
                for line in cpuinfo.read_text().splitlines()
                if line.startswith("model name")
            ),
            manifest["cpuModel"],
        )

    def run(side, repetition, stem, command):
        directory = output / side / f"{repetition:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        print(f"{side} repetition {repetition}: {stem}", flush=True)
        with (directory / f"{stem}.log").open("w") as log:
            subprocess.run(
                command,
                cwd=directory,
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=process_timeout,
                check=True,
            )
        return directory

    try:
        active = set(manifest["completedPairs"])
        benchmarks = list(enumerate((*manifest["cases"], "mixed")))
        for stage, checkpoint in enumerate(manifest["checkpoints"]):
            # Alternate order so mixed is not always measured after diagnostics.
            benchmark_order = benchmarks if stage % 2 == 0 else reversed(benchmarks)
            for index, name in benchmark_order:
                if name not in active:
                    continue
                for repetition in range(
                    manifest["completedPairs"][name] + 1, checkpoint + 1
                ):
                    # Adjacent pairs limit host drift.
                    sides = (
                        ("main", "pr") if (repetition + index) % 2 else ("pr", "main")
                    )
                    for side in sides:
                        directory = output / side / f"{repetition:02d}"
                        if name == "mixed":
                            processes = {
                                name: manifest["mixedArgs"],
                                f"{name}{COUNTERS_SUFFIX}": manifest["mixedCounterArgs"],
                            }
                        else:
                            only = f"--only=case={name}"
                            processes = {
                                name: [*manifest["diagnosticArgs"], only],
                                f"{name}{COUNTERS_SUFFIX}": [*manifest["counterArgs"], only],
                            }
                        for stem, arguments in processes.items():
                            run(
                                side,
                                repetition,
                                stem,
                                [
                                    manifest[side]["perfbench"],
                                    f"--output={directory / f'{stem}.json'}",
                                    *arguments,
                                ],
                            )
                        _, warnings = perf_report.read_measurement(
                            directory,
                            name,
                            mixed_args=manifest["mixedArgs"],
                        )
                        if warnings:
                            raise perf_report.ReportError("; ".join(warnings))
                    # A timeout during either side leaves this pair uncounted.
                    manifest["completedPairs"][name] = repetition
                result = perf_report.analyze_benchmark(output, manifest, name)
                if result.warnings:
                    raise perf_report.ReportError("; ".join(result.warnings))
                state = "resolved" if result.resolved else "inconclusive"
                print(f"{name}: {checkpoint} pairs, {state}", flush=True)
                if result.resolved:
                    active.remove(name)
                (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
            if not active:
                break
    except subprocess.TimeoutExpired as error:
        manifest["warnings"].append(
            f"Benchmark process timed out after {error.timeout:g} seconds; "
            "results use each benchmark's last completed checkpoint. "
            f"Command: {error.cmd}"
        )
        raise
    except (subprocess.SubprocessError, perf_report.ReportError) as error:
        manifest["warnings"].append(
            f"Measurement failed: {error}. See the per-run logs."
        )
        raise
    finally:
        manifest["runtimeSeconds"] = time.monotonic() - start
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
        (output / "report.md").write_text(
            perf_report.render_report(output, manifest["base"], manifest["candidate"])
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="main")
    parser.add_argument("--candidate")
    parser.add_argument("--output", type=Path, required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--build-only", action="store_true")
    modes.add_argument("--run-only", action="store_true")
    args = parser.parse_args()
    repo = Path(capture(["git", "rev-parse", "--show-toplevel"], Path.cwd()).strip())
    output = args.output.resolve()
    try:
        manifest = (
            json.loads((output / "manifest.json").read_text())
            if args.run_only
            else prepare(repo, output, args.base, args.candidate)
        )
        if not args.build_only:
            measure(output, manifest)
            print((output / "report.md").read_text())
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
