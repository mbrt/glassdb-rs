#!/usr/bin/env python3
"""Compare one candidate harness against two engine revisions without cloud access."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
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

# The runs of the timing experiments. Each run is one independent sample of
# the latency jitter, and the runs of a process run in parallel. With 32 runs
# of 300 s, an A/A comparison gave simultaneous intervals of at most about
# +-1% for the 36 timing metrics of the cases, so the report can tell an
# unchanged metric of a case from a change of 2%. With 8 runs, the p90
# intervals of the cases reached +-1.9%, and unchanged metrics were often
# inconclusive.
TIMING_RUNS = 32
# Each process runs on virtual time. On a laptop with 22 hardware threads, one
# run of the timing experiments took about 80 CPU s alone, and about 220 CPU s
# with all runs in parallel. By that estimate (32 runs x 220 CPU s / 4 vCPUs),
# the timing process takes about 30 minutes on a CI runner. Guard it against
# hangs, such as a CPU livelock that virtual time cannot see. The measure step
# of the workflow allows two sides of this timeout.
PROCESS_TIMEOUT = 2400
# The experiments, and the shared files that they include. They are part of
# the harness.
EXPERIMENTS = Path("hack/perf/experiments")
CONFIGS = Path("hack/perf/configs")
# The sources that the base revision takes from the harness. The crate must be
# in the base revision; the experiment files can be new.
HARNESS = (Path("crates/glassdb-bench-scale"), CONFIGS, EXPERIMENTS)
# The options of every compared process. The driver adds the experiments, from
# the copy of the harness in the output.
COMMON_ARGS = [
    "--backend=memory",
    "--delays=s3",
    # Virtual time makes a run independent of the host.
    "--time=virtual",
    # The delay profile is not part of the copied harness, so pin its prefix
    # partition depth on both sides.
    "--prefix-depth=3",
    "--drain-timeout=10s",
]
# The counters repeat exactly without latency jitter, so one run is enough.
COUNTER_ARGS = [*COMMON_ARGS, "--latency-jitter=false", "--runs=1"]
COUNTER_TAG = "--tag=ci/counters"
# The latency keeps the jitter of the provider, so that percentiles are
# realistic.
TIMING_ARGS = [*COMMON_ARGS, "--latency-jitter=true", f"--runs={TIMING_RUNS}"]
TIMING_TAG = "--tag=ci/timing"
# The version of the manifest that the driver writes.
MANIFEST_VERSION = 5


def capture(command: list[str], cwd: Path) -> str:
    return subprocess.check_output(command, cwd=cwd, text=True)


def snapshot(repo: Path, destination: Path, revision: str | None) -> None:
    """Copy the files of `revision`, or of the working tree, to `destination`.

    Each file gets the current time as its modification time. Cargo treats an
    artifact as fresh when it is newer than its sources, so a snapshot that
    kept older times could reuse artifacts that another snapshot built into the
    same target directory.
    """
    destination.mkdir()
    if revision:
        with subprocess.Popen(
            ["git", "archive", revision], cwd=repo, stdout=subprocess.PIPE
        ) as source:
            subprocess.run(
                ["tar", "-x", "-m", "-C", str(destination)],
                stdin=source.stdout,
                check=True,
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
                shutil.copy(source, target, follow_symlinks=False)


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
        "schemaVersion": MANIFEST_VERSION,
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
            # use_harness replaced the harness of the base snapshot. A new
            # snapshot is also newer than the artifacts of the failed build, so
            # cargo does not reuse the candidate harness that they came from.
            shutil.rmtree(snapshots / "main")
            snapshot(repo, snapshots / "main", manifest["baseCommit"])
            manifest["harnessSha256"] = use_harness(snapshots / "main", snapshots / "pr")
            manifest["harness"] = "base"
            manifest["warnings"].append(
                f"The candidate harness does not build against {base}; "
                "both revisions were measured with the harness of the base."
            )
            manifest["main"] = build(snapshots / "main", targets / "main", output, "main")
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
        manifest["experiments"] = str(harness / EXPERIMENTS)
        experiments = f"--experiment={harness / EXPERIMENTS}"
        manifest["timingArgs"] += [experiments, TIMING_TAG]
        manifest["counterArgs"] += [experiments, COUNTER_TAG]
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
    return manifest


def run_side(
    perfbench: str,
    directory: Path,
    counter_args: list[str],
    timing_args: list[str],
    timeout: float,
) -> None:
    """Run the counter and the timing experiments of one revision into
    `directory`. The log and the progress bar of each process go to stderr;
    perfbench draws the bar only on a terminal."""
    directory.mkdir(parents=True)
    start = time.monotonic()
    completed = False
    try:
        for stem, arguments in (
            (Path(perf_report.COUNTERS_FILE).stem, counter_args),
            (Path(perf_report.TIMING_FILE).stem, timing_args),
        ):
            print(f"{directory.name}: {stem}", flush=True)
            command = [perfbench, f"--output={directory / f'{stem}.json'}", *arguments]
            subprocess.run(command, cwd=directory, timeout=timeout, check=True)
        completed = True
    finally:
        # The host and the time help to explain a slow or a stopped run; the
        # results of virtual time do not depend on them.
        (directory / "host.json").write_text(
            json.dumps(
                {
                    "platform": platform.platform(),
                    "cpuModel": cpu_model(),
                    "cpus": os.cpu_count(),
                    "runtimeSeconds": time.monotonic() - start,
                    "completed": completed,
                },
                indent=2,
            )
        )


def cpu_model() -> str:
    cpuinfo = Path("/proc/cpuinfo")
    if cpuinfo.exists():
        for line in cpuinfo.read_text().splitlines():
            if line.startswith("model name"):
                return line.split(":", 1)[1].strip()
    return platform.processor()


def bench(experiments_dir: Path, perfbench: Path, output: Path) -> None:
    """Run one side of the comparison for the experiments of `experiments_dir`,
    and print the summary of its results."""
    experiments = f"--experiment={experiments_dir.resolve()}"
    run_side(
        str(perfbench.resolve()),
        output,
        [*COUNTER_ARGS, experiments, COUNTER_TAG],
        [*TIMING_ARGS, experiments, TIMING_TAG],
        PROCESS_TIMEOUT,
    )
    print(perf_report.summarize(output))
    print(f"Results: {output}")


def measure(output: Path, manifest: dict) -> None:
    """Run the counter and the timing experiments of each side, and report."""
    if manifest.get("schemaVersion") != MANIFEST_VERSION:
        raise ValueError("unsupported comparison manifest")
    process_timeout = perf_report.number(manifest.get("processTimeoutSeconds"))
    if process_timeout == 0:
        raise ValueError("process timeout must be positive")
    for side in ("main", "pr"):
        if (output / side).exists():
            raise FileExistsError(
                f"measurement directory already exists: {output / side}"
            )
    start = time.monotonic()
    try:
        for side in ("main", "pr"):
            run_side(
                manifest[side]["perfbench"],
                output / side,
                manifest["counterArgs"],
                manifest["timingArgs"],
                process_timeout,
            )
    except subprocess.TimeoutExpired as error:
        manifest["warnings"].append(
            f"Benchmark process timed out after {error.timeout:g} seconds. "
            f"Command: {error.cmd}"
        )
        raise
    except subprocess.SubprocessError as error:
        manifest["warnings"].append(
            f"Measurement failed: {error}. See the output of the measurement."
        )
        raise
    finally:
        manifest["runtimeSeconds"] = time.monotonic() - start
        (output / "manifest.json").write_text(json.dumps(manifest, indent=2))
        (output / "report.md").write_text(
            perf_report.render_report(
                output / "main",
                output / "pr",
                manifest["base"],
                manifest["candidate"],
                manifest["warnings"],
            )
        )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--base", default="main")
    parser.add_argument("--candidate")
    parser.add_argument("--output", type=Path, required=True)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--build-only", action="store_true")
    modes.add_argument(
        "--bench",
        type=Path,
        metavar="PERFBENCH",
        help="run one side with this binary, and print a summary of its results",
    )
    parser.add_argument(
        "--experiments",
        type=Path,
        help="the experiments of --bench (default: hack/perf/experiments)",
    )
    args = parser.parse_args()
    if args.experiments and not args.bench:
        parser.error("--experiments needs --bench")
    repo = Path(capture(["git", "rev-parse", "--show-toplevel"], Path.cwd()).strip())
    output = args.output.resolve()
    try:
        if args.bench:
            bench(args.experiments or repo / EXPERIMENTS, args.bench, output)
            return 0
        manifest = prepare(repo, output, args.base, args.candidate)
        if not args.build_only:
            measure(output, manifest)
            print((output / "report.md").read_text())
        return 0
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError) as error:
        print(str(error), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
