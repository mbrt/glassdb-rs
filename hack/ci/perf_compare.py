#!/usr/bin/env python3
"""Compare one candidate harness against two engine revisions without cloud access."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
from pathlib import Path
import re
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
# Measured processes take about 3–11 seconds, including setup and shutdown.
# Guard each process against hangs without cutting short the planned samples.
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
MIXED_ARGS = [
    "--backend=memory",
    "--delays=s3",
    # Random provider delays obscure code changes in short percentile samples.
    # Keep provider means, throttling, and the full measurement window.
    "--latency-jitter=false",
    "--delay-scale=0.2",
    "--runs=1",
    "--drain-timeout=10s",
    "mixed",
    "--modes=lo",
    "--affinities=100",
    "--databases=1",
    "--workers-per-shape=1",
    "--num-keys=128",
    "--multi-keys=10",
    "--duration=10s",
    "--max-duration=10s",
    "--target-ci=0",
    "--split-quiet=250ms",
    "--split-settle-timeout=5s",
]


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
    """Copy benchmark sources and declarations, retaining engine dependencies and locks."""
    paths = [Path("crates/glassdb/benches"), Path("crates/glassdb-bench-scale")]
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
    relative = Path("crates/glassdb/Cargo.toml")
    candidate_manifest = (candidate / relative).read_text()
    base_manifest = (base / relative).read_text()
    pattern = r"(?ms)^\[dev-dependencies\]\n.*?(?=^\[|\Z)"
    section = re.search(pattern, candidate_manifest)
    if section is None or re.search(pattern, base_manifest) is None:
        raise RuntimeError("unsupported comparison: missing benchmark dev dependencies")
    base_manifest = re.sub(pattern, lambda _: section[0], base_manifest)
    digest.update(section[0].encode())
    pattern = r"(?ms)^\[\[bench\]\]\n.*?(?=^\[|\Z)"
    benchmarks = re.findall(pattern, candidate_manifest)
    if not benchmarks:
        raise RuntimeError("unsupported comparison: missing benchmark declarations")
    base_manifest = re.sub(pattern, "", base_manifest).rstrip() + "\n\n"
    for benchmark in benchmarks:
        base_manifest += benchmark.rstrip() + "\n\n"
        digest.update(benchmark.encode())
    (base / relative).write_text(base_manifest)
    return digest.hexdigest()


def build(source: Path, target: Path, output: Path, side: str) -> dict:
    result = {}
    for package, selector, name in (
        ("glassdb", "--bench", "diagnostics"),
        ("glassdb-bench-scale", "--bin", "perfbench"),
    ):
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
            (output / f"{side}-build.jsonl").write_text(error.output or "")
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
        name: hashlib.sha256(Path(result[name]).read_bytes()).hexdigest()
        for name in ("diagnostics", "perfbench")
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
        manifest["harnessSha256"] = use_harness(snapshots / "pr", snapshots / "main")
        for side in ("main", "pr"):
            manifest[side] = build(
                snapshots / side, repo / "target/performance" / side, output, side
            )
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
    affinity = os.sched_getaffinity(0) if hasattr(os, "sched_getaffinity") else None

    def run(side, repetition, name, command):
        if affinity:
            cpus = manifest["cpuAffinity"] if name == "mixed" else [min(affinity)]
            os.sched_setaffinity(0, cpus)
        directory = output / side / f"{repetition:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        print(f"{side} repetition {repetition}: {name}", flush=True)
        with (directory / f"{name}.log").open("w") as log:
            subprocess.run(
                command,
                cwd=directory,
                env={**os.environ, "CRITERION_HOME": str(directory / "criterion")},
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=process_timeout,
                check=True,
            )
        return directory

    try:
        if affinity:
            # CPU diagnostics need no migration. Mixed measurements retain the
            # CI runner's four-CPU width so runtime contention stays comparable.
            manifest["cpuAffinity"] = sorted(affinity)[:4]
            manifest["diagnosticCpu"] = min(affinity)
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
                        if name == "mixed":
                            command = [
                                manifest[side]["perfbench"],
                                "--output",
                                str(output / side / f"{repetition:02d}" / "mixed.json"),
                                *manifest["mixedArgs"],
                            ]
                            log_name = "mixed"
                        else:
                            command = [
                                manifest[side]["diagnostics"],
                                "--bench",
                                "--noplot",
                                f"^diagnostic/{re.escape(name)}$",
                            ]
                            log_name = f"criterion-{name}"
                        directory = run(side, repetition, log_name, command)
                        _, warnings = perf_report.read_measurement(
                            directory, name, mixed_args=manifest["mixedArgs"]
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
        if affinity:
            os.sched_setaffinity(0, affinity)
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
