#!/usr/bin/env python3
"""Compare one candidate harness against two engine revisions without cloud access."""

from __future__ import annotations

import argparse
import hashlib
import json
import platform
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

if __package__:
    from . import perf_report
else:
    import perf_report

# Planned checks bound the cost of repeated statistical decisions. Each count is
# even so both revisions run first equally often at every possible stopping point.
CHECKPOINTS = [8, 16, 32]
# A process runs all cases on virtual time and takes a few seconds.
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
    perf_report.SHARED_LEAF_CASE,
)
# Virtual time charges a long window only as CPU time. A long window averages
# the jitter of the provider latency out of the percentile samples, so that
# changes of a few percent remain visible.
MEASURE_WINDOW = "300s"
# The experiments that all comparisons measure, one for each case, and the
# shared files that they include. They are part of the harness.
EXPERIMENTS = Path("hack/perf/experiments/default")
CONFIGS = Path("hack/perf/configs")
# The sources that the base revision takes from the harness. The crate must be
# in the base revision; the experiment files can be new.
HARNESS = (Path("crates/glassdb-bench-scale"), CONFIGS, EXPERIMENTS)
# The options of every compared process. One process runs all cases. The
# driver adds the experiments, from the copy of the harness in the output.
COMMON_ARGS = [
    "--backend=memory",
    "--delays=s3",
    # Virtual time makes a run independent of the host.
    "--time=virtual",
    # The delay profile is not part of the copied harness, so pin its prefix
    # partition depth on both sides.
    "--prefix-depth=3",
    "--runs=1",
    "--drain-timeout=10s",
]
# The latency comes from a process that keeps the jitter of the provider, so
# that percentiles are realistic.
LATENCY_JITTER = True
TIMING_ARGS = [
    *COMMON_ARGS,
    f"--latency-jitter={str(LATENCY_JITTER).lower()}",
    f'--set=cell.measure.duration="{MEASURE_WINDOW}"',
    f'--set=cell.measure.max-duration="{MEASURE_WINDOW}"',
]
# Jitter moves the counts of background operations by one per process, so the
# counters come from a second process without it. They repeat exactly, so the
# default window is enough.
COUNTER_ARGS = [*COMMON_ARGS, "--latency-jitter=false"]


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
    digest = hashlib.sha256()
    for relative in HARNESS:
        source = candidate / relative
        target = base / relative
        if relative == HARNESS[0] and not target.is_dir():
            raise RuntimeError(f"unsupported comparison: baseline has no {relative}")
        # These directories are disposable snapshots owned by this invocation.
        if target.is_dir():
            shutil.rmtree(target)
        if not source.is_dir():
            continue
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
    manifest: dict[str, Any] = {
        "schemaVersion": perf_report.MANIFEST_VERSION,
        "checkpoints": CHECKPOINTS,
        "completedPairs": 0,
        "cases": list(CASES),
        "latencyJitter": LATENCY_JITTER,
        "timingArgs": list(TIMING_ARGS),
        "counterArgs": list(COUNTER_ARGS),
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
        # The candidate snapshot has the harness that both sides use, and the
        # measurement reads its experiment files after the snapshots are gone.
        harness = output.resolve() / "harness"
        if not (snapshots / "pr" / EXPERIMENTS).is_dir():
            raise RuntimeError(
                f"unsupported comparison: the harness has no {EXPERIMENTS}"
            )
        for relative in (CONFIGS, EXPERIMENTS):
            if (snapshots / "pr" / relative).is_dir():
                shutil.copytree(snapshots / "pr" / relative, harness / relative)
        experiments = f"--experiment={harness / EXPERIMENTS}"
        manifest["timingArgs"].append(experiments)
        manifest["counterArgs"].append(experiments)
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def measure(output: Path, manifest: dict) -> None:
    perf_report.validate_manifest(manifest)
    if manifest["completedPairs"]:
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

    def run_experiments(side, repetition):
        """Run the experiments for the latency, and then for the counters."""
        directory = output / side / f"{repetition:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        for stem, arguments in (
            ("timing", manifest["timingArgs"]),
            ("counters", manifest["counterArgs"]),
        ):
            print(f"{side} repetition {repetition}: {stem}", flush=True)
            command = [
                manifest[side]["perfbench"],
                f"--output={directory / f'{stem}.json'}",
                *arguments,
            ]
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
        for checkpoint in manifest["checkpoints"]:
            for repetition in range(manifest["completedPairs"] + 1, checkpoint + 1):
                # Adjacent pairs limit host drift.
                sides = ("main", "pr") if repetition % 2 else ("pr", "main")
                for side in sides:
                    directory = run_experiments(side, repetition)
                    _, warnings = perf_report.read_measurement(
                        directory,
                        manifest["cases"],
                        latency_jitter=manifest["latencyJitter"],
                    )
                    if warnings:
                        raise perf_report.ReportError("; ".join(warnings))
                # A timeout during either side leaves this pair uncounted.
                manifest["completedPairs"] = repetition
            result = perf_report.analyze(output, manifest)
            if result.warnings:
                raise perf_report.ReportError("; ".join(result.warnings))
            state = (
                "resolved"
                if result.resolved
                else f"inconclusive: {len(result.unresolved)} metrics"
            )
            print(f"{checkpoint} pairs, {state}", flush=True)
            (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
            if result.resolved:
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
