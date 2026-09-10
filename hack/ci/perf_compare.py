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

REPETITIONS = 4
DIAGNOSTIC_REPETITIONS = 8
# Eight diagnostic pairs preserve sensitivity after multiple-comparison
# correction. Include fixture preparation and shutdown in the runtime budget.
RUNTIME_LIMIT = 600
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
    return result


def prepare(repo: Path, output: Path, base: str, candidate: str | None) -> dict:
    output.mkdir(parents=True, exist_ok=False)
    manifest = {
        "schemaVersion": 1,
        "repetitions": REPETITIONS,
        "diagnosticRepetitions": DIAGNOSTIC_REPETITIONS,
        "cases": CASES,
        "mixedArgs": MIXED_ARGS,
        "runtimeLimitSeconds": RUNTIME_LIMIT,
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
    if hasattr(os, "sched_getaffinity"):
        manifest["cpuAffinity"] = sorted(os.sched_getaffinity(0))

    def run(side, repetition, name, command):
        directory = output / side / f"{repetition:02d}"
        directory.mkdir(parents=True, exist_ok=True)
        remaining = manifest["runtimeLimitSeconds"] - (time.monotonic() - start)
        if remaining <= 0:
            raise TimeoutError("comparison runtime budget exhausted")
        print(f"{side} repetition {repetition}: {name}", flush=True)
        with (directory / f"{name}.log").open("w") as log:
            subprocess.run(
                command,
                cwd=directory,
                env={**os.environ, "CRITERION_HOME": str(directory / "criterion")},
                stdout=log,
                stderr=subprocess.STDOUT,
                timeout=remaining,
                check=True,
            )
        return directory

    def sides(repetition, case_index=0):
        return ("main", "pr") if (repetition + case_index) % 2 else ("pr", "main")

    try:
        # Keep each case's pair close in time. Running whole suites (and mixed
        # workloads) between the two measurements confounds the ratio with host drift.
        for repetition in range(
            1, manifest.get("diagnosticRepetitions", manifest["repetitions"]) + 1
        ):
            costs = {side: [] for side in ("main", "pr")}
            for index, case in enumerate(manifest["cases"]):
                for side in sides(repetition, index):
                    directory = run(
                        side,
                        repetition,
                        f"criterion-{case}",
                        [
                            manifest[side]["diagnostics"],
                            "--bench",
                            "--noplot",
                            f"^diagnostic/{re.escape(case)}$",
                        ],
                    )
                    record = perf_report.read_costs(directory / f"criterion-{case}.log")
                    try:
                        valid = record["schemaVersion"] == 1 and [
                            row["name"] for row in record["cases"]
                        ] == [case]
                    except (KeyError, TypeError) as error:
                        raise perf_report.ReportError(
                            f"invalid cost record for {side}/{case}: {error}"
                        ) from error
                    if not valid:
                        raise perf_report.ReportError(
                            f"unexpected cost case for {side}/{case}"
                        )
                    costs[side].extend(record["cases"])
                    # Preserve completed cost rows even if a later case fails.
                    (directory / "criterion.log").write_text(
                        "diagnostic-costs: "
                        + json.dumps({"schemaVersion": 1, "cases": costs[side]})
                        + "\n"
                    )
        for repetition in range(1, manifest["repetitions"] + 1):
            for side in sides(repetition):
                run(
                    side,
                    repetition,
                    "mixed",
                    [
                        manifest[side]["perfbench"],
                        "--output",
                        str(output / side / f"{repetition:02d}" / "mixed.json"),
                        *manifest["mixedArgs"],
                    ],
                )
    except (subprocess.SubprocessError, TimeoutError, perf_report.ReportError) as error:
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
